use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, OptimisticTransactionDB, OptimisticTransactionOptions, ReadOptions,
    Transaction, WriteOptions,
};
use sha2::{Digest, Sha256};

use crate::storage::codec::{
    decode_record_parts, encode_kv_key, encode_log_entry_v4, encode_log_seq, encode_record,
    RECORD_FLAG_COLD_REF,
};
use crate::storage::constants::{
    encode_prefixed_key, CF_IDX_EDGE, CF_IDX_ORD, CF_IDX_REV, CF_KV, CF_LOG, CF_META,
};
use crate::storage::mesh_replication::{
    validate_replication_node_id, META_PREFIX_MESH_LOCAL_LWW, META_PREFIX_MESH_LWW,
    META_PREFIX_MESH_SEARCH_SEEN, META_PREFIX_MESH_TOMB,
};
use crate::storage::rocksdb_kv::helpers::{
    estimate_search_op_bytes, map_tx_err, verify_expected_version,
};
use crate::storage::rocksdb_kv::index_apply::{
    apply_index_mutations_in_tx, validate_and_estimate_indexes, IndexCfHandles, IndexTarget,
};
use crate::storage::rocksdb_kv::state::RocksDbStorage;
use crate::types::{
    sanitize_search_log_op, validate_key, validate_namespace, validate_value, Ack, CommitResult,
    ExpectedVersion, LogMutationDelta, MembusError, MutationOp, ReplicatedApplyResult,
    ReplicatedDelta, ReplicationStamp, SearchLogOp, SignalEvent, SignalKind,
};

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

/// Parameters for an atomic hot/cold tier swap preserving logical LWW stamp and indexes.
pub(crate) struct TierSwapReq<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) key: &'a [u8],
    pub(crate) new_value: &'a [u8],
    pub(crate) set_cold_flag: bool,
    pub(crate) logical_value_sha256: [u8; 32],
    pub(crate) ack: Ack,
    pub(crate) expected: ExpectedVersion,
}

struct ValidatedReplicatedDelta {
    sanitized_search_op: Option<SearchLogOp>,
    estimated_bytes: usize,
}

struct ReplicatedKeySet {
    enc_key: Vec<u8>,
    lww_key: Vec<u8>,
    local_lww_key: Vec<u8>,
    tomb_key: Vec<u8>,
}

struct ReplicatedWriteCfs<'a> {
    kv: &'a Arc<BoundColumnFamily<'a>>,
    meta: &'a Arc<BoundColumnFamily<'a>>,
    log: &'a Arc<BoundColumnFamily<'a>>,
}

struct LwwLineageUpdate<'a> {
    lww_key: &'a [u8],
    local_lww_key: &'a [u8],
    old_meta: &'a crate::types::RecordMeta,
    prev_lww: Option<StoredLwwRecord>,
    prev_local_lww: Option<StoredLocalLwwRecord>,
    seq: u64,
    logical_value_sha256: [u8; 32],
}

impl RocksDbStorage {
    pub(crate) fn tier_swap_record_impl(
        &self,
        req: TierSwapReq<'_>,
    ) -> Result<CommitResult, MembusError> {
        validate_namespace(req.namespace)?;
        validate_key(req.key)?;
        validate_value(req.new_value, self.config.max_value_bytes)?;

        let estimated_bytes = req
            .namespace
            .len()
            .saturating_add(req.key.len())
            .saturating_add(req.new_value.len())
            .saturating_add(128);
        let _permit = self.admission.try_acquire(estimated_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let enc_key = encode_kv_key(req.namespace, req.key);
        let lww_key = encode_prefixed_key(META_PREFIX_MESH_LWW, &enc_key);
        let local_lww_key = encode_prefixed_key(META_PREFIX_MESH_LOCAL_LWW, &enc_key);

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(req.ack == Ack::Flush);
        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let tx = self.db.transaction_opt(&write_opts, &tx_opts);
        let (old_meta, prev_lww, prev_local_lww) = read_tier_swap_state(
            &tx,
            &kv_cf,
            &meta_cf,
            &enc_key,
            &lww_key,
            &local_lww_key,
            &req,
        )?;

        let new_flags = if req.set_cold_flag {
            old_meta.flags | RECORD_FLAG_COLD_REF
        } else {
            old_meta.flags & !RECORD_FLAG_COLD_REF
        };

        let ticket = self.sequencer.reserve();
        let seq = ticket.seq();

        let encoded_val = encode_record(seq, new_flags, old_meta.ts_ms, req.new_value);
        tx.put_cf(&kv_cf, &enc_key, &encoded_val)
            .map_err(map_tx_err)?;

        preserve_lww_lineage(
            &tx,
            &meta_cf,
            LwwLineageUpdate {
                lww_key: &lww_key,
                local_lww_key: &local_lww_key,
                old_meta: &old_meta,
                prev_lww,
                prev_local_lww,
                seq,
                logical_value_sha256: req.logical_value_sha256,
            },
        )?;

        let delta = LogMutationDelta {
            namespace: req.namespace.to_owned(),
            key: req.key.to_vec(),
            op: MutationOp::Put {
                value: req.new_value.to_vec(),
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
        validate_replication_node_id(local_node_id, "local_node_id")?;
        validate_replication_node_id(&delta.stamp.origin_node_id, "origin_node_id")?;
        if delta.stamp.origin_node_id == local_node_id {
            return Ok(ReplicatedApplyResult {
                applied: false,
                local_seq: None,
            });
        }
        let validated = self.validate_replicated_delta(delta)?;
        self.hlc.observe(delta.stamp.timestamp_ms);
        let _permit = self.admission.try_acquire(validated.estimated_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let idx_ord_cf = self.cf(CF_IDX_ORD)?;
        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let idx_rev_cf = self.cf(CF_IDX_REV)?;
        let keys = build_replicated_keys(&delta.namespace, &delta.key);

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

                let current_stamp = resolve_current_lww_stamp(
                    &tx,
                    &read_opts,
                    &kv_cf,
                    &meta_cf,
                    &keys,
                    local_node_id,
                )?;
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
                let idx_cfs = IndexCfHandles {
                    ord: &idx_ord_cf,
                    edge: &idx_edge_cf,
                    rev: &idx_rev_cf,
                };
                let target = IndexTarget {
                    namespace: &delta.namespace,
                    pk: &delta.key,
                    enc_pk: &keys.enc_key,
                };
                apply_index_mutations_in_tx(&tx, &read_opts, &idx_cfs, &target, applied_indexes)?;
            }

            let ticket = self.sequencer.reserve();
            let seq = ticket.seq();
            let write_cfs = ReplicatedWriteCfs {
                kv: &kv_cf,
                meta: &meta_cf,
                log: &log_cf,
            };
            let events = self.stage_replicated_delta_writes(
                &tx,
                &write_cfs,
                &keys,
                delta,
                &validated.sanitized_search_op,
                seq,
            )?;

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

    fn validate_replicated_delta(
        &self,
        delta: &ReplicatedDelta,
    ) -> Result<ValidatedReplicatedDelta, MembusError> {
        validate_namespace(&delta.namespace)?;
        validate_key(&delta.key)?;
        let index_bytes = match &delta.indexes {
            Some(idx_muts) => validate_and_estimate_indexes(idx_muts, self.config.max_value_bytes)?,
            None => 0,
        };
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
        let estimated_bytes = delta
            .namespace
            .len()
            .saturating_add(delta.key.len())
            .saturating_add(payload_bytes)
            .saturating_add(index_bytes)
            .saturating_add(
                sanitized_search_op
                    .as_ref()
                    .map_or(0, estimate_search_op_bytes),
            )
            .saturating_add(128);
        Ok(ValidatedReplicatedDelta {
            sanitized_search_op,
            estimated_bytes,
        })
    }

    fn stage_replicated_delta_writes(
        &self,
        tx: &Transaction<'_, OptimisticTransactionDB>,
        cfs: &ReplicatedWriteCfs<'_>,
        keys: &ReplicatedKeySet,
        delta: &ReplicatedDelta,
        sanitized_search_op: &Option<SearchLogOp>,
        seq: u64,
    ) -> Result<Vec<SignalEvent>, MembusError> {
        let ts_ms = delta.stamp.timestamp_ms;
        let (kind, value_hint) = match &delta.op {
            MutationOp::Put { value } => {
                let encoded_val = encode_record(seq, 0, ts_ms, value);
                tx.put_cf(cfs.kv, &keys.enc_key, &encoded_val)
                    .map_err(map_tx_err)?;
                (SignalKind::Put, self.build_value_hint(value))
            }
            MutationOp::Remove => {
                tx.delete_cf(cfs.kv, &keys.enc_key).map_err(map_tx_err)?;
                (SignalKind::Delete, None)
            }
        };

        let lww_val = serde_json::to_vec(&StoredLwwRecord {
            local_applied_seq: seq,
            stamp: delta.stamp.clone(),
        })
        .map_err(|err| MembusError::Storage(format!("encode mesh lww record failed: {err}")))?;
        tx.put_cf(cfs.meta, &keys.lww_key, &lww_val)
            .map_err(map_tx_err)?;

        let events = vec![SignalEvent {
            seq,
            epoch: self.epoch,
            namespace: delta.namespace.clone(),
            key: delta.key.clone(),
            version: seq,
            kind,
            value_hint,
        }];
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
        tx.put_cf(cfs.log, encode_log_seq(seq), &encoded_log)
            .map_err(map_tx_err)?;
        Ok(events)
    }
}

fn build_replicated_keys(namespace: &str, key: &[u8]) -> ReplicatedKeySet {
    let enc_key = encode_kv_key(namespace, key);
    let lww_key = encode_prefixed_key(META_PREFIX_MESH_LWW, &enc_key);
    let local_lww_key = encode_prefixed_key(META_PREFIX_MESH_LOCAL_LWW, &enc_key);
    let tomb_key = encode_prefixed_key(META_PREFIX_MESH_TOMB, &enc_key);
    ReplicatedKeySet {
        enc_key,
        lww_key,
        local_lww_key,
        tomb_key,
    }
}

fn read_tier_swap_state(
    tx: &Transaction<'_, OptimisticTransactionDB>,
    kv_cf: &Arc<BoundColumnFamily<'_>>,
    meta_cf: &Arc<BoundColumnFamily<'_>>,
    enc_key: &[u8],
    lww_key: &[u8],
    local_lww_key: &[u8],
    req: &TierSwapReq<'_>,
) -> Result<(crate::types::RecordMeta, Option<StoredLwwRecord>, Option<StoredLocalLwwRecord>), MembusError> {
    let tx_snap = tx.snapshot();
    let mut read_opts = ReadOptions::default();
    read_opts.set_snapshot(&tx_snap);

    let current = tx
        .get_pinned_for_update_cf_opt(kv_cf, enc_key, true, &read_opts)
        .map_err(map_tx_err)?;
    let Some(raw) = current else {
        verify_expected_version(req.namespace, req.key, req.expected, None)?;
        return Err(MembusError::NotFound(
            "tier_swap_record target key does not exist".to_owned(),
        ));
    };
    let (meta, _old_val) = decode_record_parts(raw.as_ref())?;
    verify_expected_version(req.namespace, req.key, req.expected, Some(meta.version))?;

    let lww = tx
        .get_pinned_for_update_cf_opt(meta_cf, lww_key, true, &read_opts)
        .map_err(map_tx_err)?
        .map(|r| {
            serde_json::from_slice::<StoredLwwRecord>(r.as_ref()).map_err(|err| {
                MembusError::Corruption(format!("corrupt mesh lww record: {err}"))
            })
        })
        .transpose()?;
    let local_lww = tx
        .get_pinned_for_update_cf_opt(meta_cf, local_lww_key, true, &read_opts)
        .map_err(map_tx_err)?
        .map(|r| {
            serde_json::from_slice::<StoredLocalLwwRecord>(r.as_ref()).map_err(|err| {
                MembusError::Corruption(format!("corrupt mesh local_lww record: {err}"))
            })
        })
        .transpose()?;
    Ok((meta, lww, local_lww))
}

fn preserve_lww_lineage(
    tx: &Transaction<'_, OptimisticTransactionDB>,
    meta_cf: &Arc<BoundColumnFamily<'_>>,
    update: LwwLineageUpdate<'_>,
) -> Result<(), MembusError> {
    if let Some(mut stored) = update
        .prev_lww
        .filter(|r| r.local_applied_seq == update.old_meta.version)
    {
        stored.local_applied_seq = update.seq;
        let bytes = serde_json::to_vec(&stored)
            .map_err(|err| MembusError::Storage(format!("encode mesh lww record failed: {err}")))?;
        tx.put_cf(meta_cf, update.lww_key, &bytes)
            .map_err(map_tx_err)?;
    } else {
        let next_local_lww = match update
            .prev_local_lww
            .filter(|r| r.applied_seq == update.old_meta.version)
        {
            Some(mut existing) => {
                existing.applied_seq = update.seq;
                existing
            }
            None => StoredLocalLwwRecord {
                applied_seq: update.seq,
                logical_seq: update.old_meta.version,
                ts_ms: update.old_meta.ts_ms,
                value_sha256: update.logical_value_sha256,
            },
        };
        let bytes = serde_json::to_vec(&next_local_lww).map_err(|err| {
            MembusError::Storage(format!("encode mesh local_lww record failed: {err}"))
        })?;
        tx.put_cf(meta_cf, update.local_lww_key, &bytes)
            .map_err(map_tx_err)?;
    }
    Ok(())
}

fn resolve_current_lww_stamp(
    tx: &Transaction<'_, OptimisticTransactionDB>,
    read_opts: &ReadOptions,
    kv_cf: &Arc<BoundColumnFamily<'_>>,
    meta_cf: &Arc<BoundColumnFamily<'_>>,
    keys: &ReplicatedKeySet,
    local_node_id: &str,
) -> Result<Option<ReplicationStamp>, MembusError> {
    let kv_raw = tx
        .get_pinned_for_update_cf_opt(kv_cf, &keys.enc_key, true, read_opts)
        .map_err(map_tx_err)?;
    let lww_raw = tx
        .get_pinned_for_update_cf_opt(meta_cf, &keys.lww_key, true, read_opts)
        .map_err(map_tx_err)?;
    let local_lww_raw = tx
        .get_pinned_for_update_cf_opt(meta_cf, &keys.local_lww_key, true, read_opts)
        .map_err(map_tx_err)?;
    let tomb_raw = tx
        .get_pinned_for_update_cf_opt(meta_cf, &keys.tomb_key, true, read_opts)
        .map_err(map_tx_err)?;

    let mut current_local_seq = 0u64;
    let mut current_stamp: Option<ReplicationStamp> = None;

    if let Some(raw) = lww_raw {
        let stored: StoredLwwRecord = serde_json::from_slice(raw.as_ref()).map_err(|err| {
            MembusError::Corruption(format!("corrupt mesh lww record: {err}"))
        })?;
        current_local_seq = stored.local_applied_seq;
        current_stamp = Some(stored.stamp);
    }

    let local_lww_parsed = match local_lww_raw {
        Some(raw) => Some(
            serde_json::from_slice::<StoredLocalLwwRecord>(raw.as_ref()).map_err(|err| {
                MembusError::Corruption(format!("corrupt mesh local_lww record: {err}"))
            })?,
        ),
        None => None,
    };

    if let Some(raw) = kv_raw {
        let (meta, val) = decode_record_parts(raw.as_ref())?;
        if meta.version > current_local_seq {
            current_local_seq = meta.version;
            if let Some(local_lww) = local_lww_parsed.filter(|r| r.applied_seq == meta.version) {
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

    Ok(current_stamp)
}
