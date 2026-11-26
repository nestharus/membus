use rocksdb::{OptimisticTransactionOptions, ReadOptions, WriteOptions};
use sha2::{Digest, Sha256};

use crate::index::validate_index_mutations;
use crate::storage::codec::{
    decode_record_parts, encode_kv_key, encode_log_entry_v4, encode_log_seq, encode_record,
    RECORD_FLAG_COLD_REF,
};
use crate::storage::rocksdb_kv::{
    apply_index_mutations_in_tx, encode_prefixed_key, estimate_index_bytes,
    estimate_search_op_bytes, map_tx_err, verify_expected_version, RocksDbStorage,
};
use crate::storage::{CF_IDX_EDGE, CF_IDX_ORD, CF_IDX_REV, CF_KV, CF_LOG, CF_META};
use crate::types::{
    sanitize_search_log_op, validate_key, validate_namespace, validate_value, Ack, CommitResult,
    ExpectedVersion, LogMutationDelta, MembusError, MutationOp, ReplicatedApplyResult,
    ReplicatedDelta, ReplicationStamp, SearchLogOp, SignalEvent, SignalKind,
};

pub(crate) const META_PREFIX_MESH_LWW: &[u8] = b"mesh:lww:";
pub(crate) const META_PREFIX_MESH_LOCAL_LWW: &[u8] = b"mesh:local_lww:";
pub(crate) const META_PREFIX_MESH_TOMB: &[u8] = b"mesh:tomb:";
pub(crate) const META_PREFIX_MESH_PEER: &[u8] = b"mesh:peer:";
pub(crate) const META_PREFIX_MESH_SEARCH_SEEN: &[u8] = b"mesh:search_seen:";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct StoredLwwRecord {
    local_applied_seq: u64,
    stamp: ReplicationStamp,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct StoredLocalLwwRecord {
    applied_seq: u64,
    logical_seq: u64,
    ts_ms: u64,
    value_sha256: [u8; 32],
}

impl RocksDbStorage {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn tier_swap_record_impl(
        &self,
        namespace: &str,
        key: &[u8],
        new_value: &[u8],
        set_cold_flag: bool,
        logical_value_sha256: [u8; 32],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        validate_namespace(namespace)?;
        validate_key(key)?;
        validate_value(new_value, self.config.max_value_bytes)?;

        let estimated_bytes = namespace
            .len()
            .saturating_add(key.len())
            .saturating_add(new_value.len())
            .saturating_add(128);
        let _permit = self.admission.try_acquire(estimated_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let enc_key = encode_kv_key(namespace, key);
        let lww_key = encode_prefixed_key(META_PREFIX_MESH_LWW, &enc_key);
        let local_lww_key = encode_prefixed_key(META_PREFIX_MESH_LOCAL_LWW, &enc_key);

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(ack == Ack::Flush);
        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let tx = self.db.transaction_opt(&write_opts, &tx_opts);
        let (old_meta, prev_lww, prev_local_lww) = {
            let tx_snap = tx.snapshot();
            let mut read_opts = ReadOptions::default();
            read_opts.set_snapshot(&tx_snap);

            let current = tx
                .get_pinned_for_update_cf_opt(&kv_cf, &enc_key, true, &read_opts)
                .map_err(map_tx_err)?;
            let Some(raw) = current else {
                verify_expected_version(namespace, key, expected, None)?;
                return Err(MembusError::NotFound(
                    "tier_swap_record target key does not exist".to_owned(),
                ));
            };
            let (meta, _old_val) = decode_record_parts(raw.as_ref())?;
            verify_expected_version(namespace, key, expected, Some(meta.version))?;

            let lww = tx
                .get_pinned_for_update_cf_opt(&meta_cf, &lww_key, true, &read_opts)
                .map_err(map_tx_err)?
                .map(|r| {
                    serde_json::from_slice::<StoredLwwRecord>(r.as_ref()).map_err(|err| {
                        MembusError::Corruption(format!("corrupt mesh lww record: {err}"))
                    })
                })
                .transpose()?;
            let local_lww = tx
                .get_pinned_for_update_cf_opt(&meta_cf, &local_lww_key, true, &read_opts)
                .map_err(map_tx_err)?
                .map(|r| {
                    serde_json::from_slice::<StoredLocalLwwRecord>(r.as_ref()).map_err(|err| {
                        MembusError::Corruption(format!("corrupt mesh local_lww record: {err}"))
                    })
                })
                .transpose()?;
            (meta, lww, local_lww)
        };

        let new_flags = if set_cold_flag {
            old_meta.flags | RECORD_FLAG_COLD_REF
        } else {
            old_meta.flags & !RECORD_FLAG_COLD_REF
        };

        let ticket = self.sequencer.reserve();
        let seq = ticket.seq();

        // Preserve original logical timestamp (`old_meta.ts_ms`) and bitwise-update ONLY cold flag.
        let encoded_val = encode_record(seq, new_flags, old_meta.ts_ms, new_value);
        tx.put_cf(&kv_cf, &enc_key, &encoded_val)
            .map_err(map_tx_err)?;

        // Preserve original LWW stamp so tier archival/rehydration NEVER changes LWW precedence.
        if let Some(mut stored) = prev_lww.filter(|r| r.local_applied_seq == old_meta.version) {
            stored.local_applied_seq = seq;
            let bytes = serde_json::to_vec(&stored).map_err(|err| {
                MembusError::Storage(format!("encode mesh lww record failed: {err}"))
            })?;
            tx.put_cf(&meta_cf, &lww_key, &bytes).map_err(map_tx_err)?;
        } else {
            let next_local_lww = match prev_local_lww.filter(|r| r.applied_seq == old_meta.version)
            {
                Some(mut existing) => {
                    existing.applied_seq = seq;
                    existing
                }
                None => StoredLocalLwwRecord {
                    applied_seq: seq,
                    logical_seq: old_meta.version,
                    ts_ms: old_meta.ts_ms,
                    value_sha256: logical_value_sha256,
                },
            };
            let bytes = serde_json::to_vec(&next_local_lww).map_err(|err| {
                MembusError::Storage(format!("encode mesh local_lww record failed: {err}"))
            })?;
            tx.put_cf(&meta_cf, &local_lww_key, &bytes)
                .map_err(map_tx_err)?;
        }

        // Emit ZERO signal events and tag `is_tier_swap = true, preserve_indexes = true`
        // so mesh replication ignores local tiering while backup replay preserves indexes.
        let delta = LogMutationDelta {
            namespace: namespace.to_owned(),
            key: key.to_vec(),
            op: MutationOp::Put {
                value: new_value.to_vec(),
            },
            flags: new_flags,
            preserve_indexes: true,
            is_tier_swap: true,
            indexes: None,
            search_op: None,
            origin_stamp: None,
        };
        let encoded_log = encode_log_entry_v4(
            self.epoch,
            seq,
            old_meta.ts_ms,
            false,
            &[],
            &[],
            &[delta],
            None,
        );
        tx.put_cf(&log_cf, encode_log_seq(seq), &encoded_log)
            .map_err(map_tx_err)?;

        tx.commit().map_err(map_tx_err)?;
        ticket.commit(Vec::new());

        Ok(CommitResult {
            seq,
            epoch: self.epoch,
            versions: vec![seq],
        })
    }

    pub(crate) fn apply_replicated_delta_impl(
        &self,
        local_node_id: &str,
        delta: &ReplicatedDelta,
        ack: Ack,
    ) -> Result<ReplicatedApplyResult, MembusError> {
        if local_node_id.is_empty() || delta.stamp.origin_node_id.is_empty() {
            return Err(MembusError::InvalidArgument(
                "replication node_id must not be empty".to_owned(),
            ));
        }
        validate_namespace(&delta.namespace)?;
        validate_key(&delta.key)?;
        if let Some(idx_muts) = &delta.indexes {
            validate_index_mutations(idx_muts, self.config.max_value_bytes)?;
        }
        let sanitized_search_op = match &delta.search_op {
            Some(op) => Some(sanitize_search_log_op(op)?),
            None => None,
        };
        let payload_bytes = match &delta.op {
            MutationOp::Put { value } => {
                validate_value(value, self.config.max_value_bytes)?;
                let actual_hash: [u8; 32] = Sha256::digest(value).into();
                if actual_hash != delta.stamp.value_sha256 {
                    return Err(MembusError::Corruption(
                        "replicated delta value_sha256 mismatch".to_owned(),
                    ));
                }
                value.len()
            }
            MutationOp::Remove => {
                if delta.stamp.value_sha256 != [0u8; 32] {
                    return Err(MembusError::Corruption(
                        "replicated remove delta must carry zero value_sha256".to_owned(),
                    ));
                }
                0
            }
        };

        // Observe remote timestamp in local monotonic HLC so any subsequent causal local
        // write gets a strictly higher timestamp even under negative clock skew.
        self.hlc.observe(delta.stamp.timestamp_ms);

        let estimated_bytes = delta
            .namespace
            .len()
            .saturating_add(delta.key.len())
            .saturating_add(payload_bytes)
            .saturating_add(delta.indexes.as_ref().map_or(0, estimate_index_bytes))
            .saturating_add(
                sanitized_search_op
                    .as_ref()
                    .map_or(0, estimate_search_op_bytes),
            )
            .saturating_add(128);
        let _permit = self.admission.try_acquire(estimated_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let idx_ord_cf = self.cf(CF_IDX_ORD)?;
        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let idx_rev_cf = self.cf(CF_IDX_REV)?;
        let enc_key = encode_kv_key(&delta.namespace, &delta.key);
        let lww_key = encode_prefixed_key(META_PREFIX_MESH_LWW, &enc_key);
        let local_lww_key = encode_prefixed_key(META_PREFIX_MESH_LOCAL_LWW, &enc_key);
        let tomb_key = encode_prefixed_key(META_PREFIX_MESH_TOMB, &enc_key);

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(ack == Ack::Flush);
        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        for attempt in 0..32 {
            let tx = self.db.transaction_opt(&write_opts, &tx_opts);
            {
                let tx_snap = tx.snapshot();
                let mut read_opts = ReadOptions::default();
                read_opts.set_snapshot(&tx_snap);

                let kv_raw = tx
                    .get_pinned_for_update_cf_opt(&kv_cf, &enc_key, true, &read_opts)
                    .map_err(map_tx_err)?;
                let lww_raw = tx
                    .get_pinned_for_update_cf_opt(&meta_cf, &lww_key, true, &read_opts)
                    .map_err(map_tx_err)?;
                let local_lww_raw = tx
                    .get_pinned_for_update_cf_opt(&meta_cf, &local_lww_key, true, &read_opts)
                    .map_err(map_tx_err)?;
                let tomb_raw = tx
                    .get_pinned_for_update_cf_opt(&meta_cf, &tomb_key, true, &read_opts)
                    .map_err(map_tx_err)?;

                let mut current_local_seq = 0u64;
                let mut current_stamp: Option<ReplicationStamp> = None;

                if let Some(raw) = lww_raw {
                    let stored: StoredLwwRecord =
                        serde_json::from_slice(raw.as_ref()).map_err(|err| {
                            MembusError::Corruption(format!("corrupt mesh lww record: {err}"))
                        })?;
                    current_local_seq = stored.local_applied_seq;
                    current_stamp = Some(stored.stamp);
                }

                let local_lww_parsed = match local_lww_raw {
                    Some(raw) => Some(
                        serde_json::from_slice::<StoredLocalLwwRecord>(raw.as_ref()).map_err(
                            |err| {
                                MembusError::Corruption(format!(
                                    "corrupt mesh local_lww record: {err}"
                                ))
                            },
                        )?,
                    ),
                    None => None,
                };

                if let Some(raw) = kv_raw {
                    let (meta, val) = decode_record_parts(raw.as_ref())?;
                    if meta.version > current_local_seq {
                        current_local_seq = meta.version;
                        if let Some(local_lww) =
                            local_lww_parsed.filter(|r| r.applied_seq == meta.version)
                        {
                            current_stamp = Some(ReplicationStamp {
                                timestamp_ms: local_lww.ts_ms,
                                origin_node_id: local_node_id.to_owned(),
                                origin_seq: local_lww.logical_seq,
                                value_sha256: local_lww.value_sha256,
                            });
                        } else {
                            current_stamp = Some(ReplicationStamp {
                                timestamp_ms: meta.ts_ms,
                                origin_node_id: local_node_id.to_owned(),
                                origin_seq: meta.version,
                                value_sha256: Sha256::digest(val).into(),
                            });
                        }
                    }
                }

                if let Some(raw) = tomb_raw {
                    let bytes = raw.as_ref();
                    if bytes.len() != 16 {
                        return Err(MembusError::Corruption(
                            "corrupt mesh tombstone metadata".to_owned(),
                        ));
                    }
                    let tomb_seq = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
                    let tomb_ts = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
                    if tomb_seq > current_local_seq {
                        current_stamp = Some(ReplicationStamp {
                            timestamp_ms: tomb_ts,
                            origin_node_id: local_node_id.to_owned(),
                            origin_seq: tomb_seq,
                            value_sha256: [0u8; 32],
                        });
                    }
                }

                if let Some(cur) = &current_stamp {
                    if delta.stamp.cmp_lww(cur) != std::cmp::Ordering::Greater {
                        return Ok(ReplicatedApplyResult {
                            applied: false,
                            local_seq: None,
                        });
                    }
                }

                let applied_indexes = match &delta.op {
                    MutationOp::Put { .. } => delta.indexes.as_ref(),
                    MutationOp::Remove => None,
                };
                apply_index_mutations_in_tx(
                    &tx,
                    &read_opts,
                    &idx_ord_cf,
                    &idx_edge_cf,
                    &idx_rev_cf,
                    &delta.namespace,
                    &delta.key,
                    &enc_key,
                    applied_indexes,
                )?;
            }

            let ticket = self.sequencer.reserve();
            let seq = ticket.seq();
            let ts_ms = delta.stamp.timestamp_ms;

            let (kind, value_hint) = match &delta.op {
                MutationOp::Put { value } => {
                    let encoded_val = encode_record(seq, 0, ts_ms, value);
                    tx.put_cf(&kv_cf, &enc_key, &encoded_val)
                        .map_err(map_tx_err)?;
                    (SignalKind::Put, self.build_value_hint(value))
                }
                MutationOp::Remove => {
                    tx.delete_cf(&kv_cf, &enc_key).map_err(map_tx_err)?;
                    (SignalKind::Delete, None)
                }
            };

            let lww_val = serde_json::to_vec(&StoredLwwRecord {
                local_applied_seq: seq,
                stamp: delta.stamp.clone(),
            })
            .map_err(|err| MembusError::Storage(format!("encode mesh lww record failed: {err}")))?;
            tx.put_cf(&meta_cf, &lww_key, &lww_val)
                .map_err(map_tx_err)?;

            let event = SignalEvent {
                seq,
                epoch: self.epoch,
                namespace: delta.namespace.clone(),
                key: delta.key.clone(),
                version: seq,
                kind,
                value_hint,
            };
            let events = vec![event];
            let log_delta = LogMutationDelta {
                namespace: delta.namespace.clone(),
                key: delta.key.clone(),
                op: delta.op.clone(),
                flags: 0,
                preserve_indexes: false,
                is_tier_swap: false,
                indexes: match &delta.op {
                    MutationOp::Put { .. } => delta.indexes.clone(),
                    MutationOp::Remove => None,
                },
                search_op: sanitized_search_op.clone(),
                origin_stamp: Some(delta.stamp.clone()),
            };
            let encoded_log = encode_log_entry_v4(
                self.epoch,
                seq,
                ts_ms,
                true,
                &events,
                &[],
                &[log_delta],
                None,
            );
            tx.put_cf(&log_cf, encode_log_seq(seq), &encoded_log)
                .map_err(map_tx_err)?;

            match tx.commit().map_err(map_tx_err) {
                Ok(()) => {
                    ticket.commit(events);
                    return Ok(ReplicatedApplyResult {
                        applied: true,
                        local_seq: Some(seq),
                    });
                }
                Err(MembusError::TxConflict(_)) if attempt + 1 < 32 => continue,
                Err(err) => return Err(err),
            }
        }

        Err(MembusError::TxConflict(
            "replicated delta exceeded optimistic retry limit under contention".to_owned(),
        ))
    }

    pub(crate) fn append_replicated_search_ops_impl(
        &self,
        origin_stamp: &ReplicationStamp,
        ops: &[SearchLogOp],
        ack: Ack,
    ) -> Result<ReplicatedApplyResult, MembusError> {
        if origin_stamp.origin_node_id.is_empty() {
            return Err(MembusError::InvalidArgument(
                "origin_node_id must not be empty".to_owned(),
            ));
        }
        if ops.is_empty() {
            return Ok(ReplicatedApplyResult {
                applied: false,
                local_seq: None,
            });
        }
        let mut sanitized = Vec::with_capacity(ops.len());
        let mut total_bytes = 64usize;
        for op in ops {
            let s = sanitize_search_log_op(op)?;
            total_bytes = total_bytes.saturating_add(estimate_search_op_bytes(&s));
            sanitized.push(s);
        }
        if total_bytes > self.config.max_value_bytes {
            return Err(MembusError::InvalidArgument(format!(
                "replicated search ops size {total_bytes} exceeds max_value_bytes {}",
                self.config.max_value_bytes
            )));
        }
        self.hlc.observe(origin_stamp.timestamp_ms);
        let _permit = self.admission.try_acquire(total_bytes)?;

        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let dedup_key = encode_prefixed_key(
            META_PREFIX_MESH_SEARCH_SEEN,
            format!(
                "{}:{}:{}",
                origin_stamp.origin_node_id, origin_stamp.origin_seq, origin_stamp.timestamp_ms
            )
            .as_bytes(),
        );

        let mut wo = WriteOptions::default();
        wo.set_sync(ack == Ack::Flush);
        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        for attempt in 0..16 {
            let tx = self.db.transaction_opt(&wo, &tx_opts);
            {
                let tx_snap = tx.snapshot();
                let mut read_opts = ReadOptions::default();
                read_opts.set_snapshot(&tx_snap);
                if tx
                    .get_pinned_for_update_cf_opt(&meta_cf, &dedup_key, true, &read_opts)
                    .map_err(map_tx_err)?
                    .is_some()
                {
                    return Ok(ReplicatedApplyResult {
                        applied: false,
                        local_seq: None,
                    });
                }
            }
            let ticket = self.sequencer.reserve();
            let seq = ticket.seq();
            tx.put_cf(&meta_cf, &dedup_key, seq.to_be_bytes())
                .map_err(map_tx_err)?;
            let encoded_log = encode_log_entry_v4(
                self.epoch,
                seq,
                origin_stamp.timestamp_ms,
                true,
                &[],
                &sanitized,
                &[],
                Some(origin_stamp),
            );
            tx.put_cf(&log_cf, encode_log_seq(seq), &encoded_log)
                .map_err(map_tx_err)?;

            match tx.commit().map_err(map_tx_err) {
                Ok(()) => {
                    ticket.commit(Vec::new());
                    return Ok(ReplicatedApplyResult {
                        applied: true,
                        local_seq: Some(seq),
                    });
                }
                Err(MembusError::TxConflict(_)) if attempt + 1 < 16 => continue,
                Err(err) => return Err(err),
            }
        }

        Err(MembusError::TxConflict(
            "replicated search ops exceeded optimistic retry limit".to_owned(),
        ))
    }
}
