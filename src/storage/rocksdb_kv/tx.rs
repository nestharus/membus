use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, Direction, IteratorMode, OptimisticTransactionDB,
    OptimisticTransactionOptions, ReadOptions, Transaction, WriteBatchWithTransaction,
    WriteOptions,
};

use crate::storage::codec::{
    decode_record_parts, encode_kv_key, encode_log_entry_v4, encode_log_seq, encode_record,
};
use crate::storage::constants::{
    encode_prefixed_key, CF_IDX_EDGE, CF_IDX_ORD, CF_IDX_REV, CF_KV, CF_LEASE, CF_LOG, CF_META,
};
use crate::storage::mesh_replication::META_PREFIX_MESH_TOMB;
use crate::storage::rocksdb_kv::helpers::{
    encode_idempotency_key, encode_idempotency_ord_key, estimate_search_op_bytes, map_rocksdb_err,
    map_tx_err, validate_idempotency_context, verify_expected_version, verify_lease_guard_bytes,
    StoredIdempotencyRecord, META_PREFIX_IDEM_ORD,
};
use crate::storage::rocksdb_kv::index_apply::{
    apply_index_mutations_in_tx, validate_and_estimate_indexes, IndexCfHandles, IndexTarget,
};
use crate::storage::rocksdb_kv::state::RocksDbStorage;
use crate::types::{
    sanitize_search_log_op, validate_key, validate_namespace, validate_value, Ack, CommitResult,
    ExpectedVersion, IdempotencyContext, IndexMutations, IndexedMutation, LeaseGuard,
    LogMutationDelta, MembusError, MutationOp, SearchLogOp, SignalEvent, SignalKind, VersionGuard,
};

/// Parameters for a single-key transactional mutation.
pub(crate) struct SingleMutationReq<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) key: &'a [u8],
    pub(crate) op: MutationOp,
    pub(crate) flags: u8,
    pub(crate) preserve_indexes: bool,
    pub(crate) ack: Ack,
    pub(crate) expected: ExpectedVersion,
    pub(crate) indexes: Option<&'a IndexMutations>,
    pub(crate) search_op: Option<&'a SearchLogOp>,
    pub(crate) idempotency: Option<&'a IdempotencyContext>,
}

struct ValidatedSingleMutation {
    sanitized_search_op: Option<SearchLogOp>,
    estimated_bytes: usize,
}

struct ValidatedTxBatch {
    sanitized_per_mutation: Vec<Option<SearchLogOp>>,
    total_bytes: usize,
}

struct KvStageCtx<'a> {
    tx: &'a Transaction<'a, OptimisticTransactionDB>,
    kv_cf: &'a Arc<BoundColumnFamily<'a>>,
    meta_cf: &'a Arc<BoundColumnFamily<'a>>,
    flags: u8,
    seq: u64,
    ts_ms: u64,
}

struct StagedTxBatch {
    events: Vec<SignalEvent>,
    versions: Vec<u64>,
    delta_mutations: Vec<LogMutationDelta>,
}

impl RocksDbStorage {
    pub(crate) fn execute_single_mutation(
        &self,
        req: SingleMutationReq<'_>,
    ) -> Result<(CommitResult, bool), MembusError> {
        let validated = self.validate_single_mutation(&req)?;
        let _permit = self.admission.try_acquire(validated.estimated_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let idx_ord_cf = self.cf(CF_IDX_ORD)?;
        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let idx_rev_cf = self.cf(CF_IDX_REV)?;
        let enc_key = encode_kv_key(req.namespace, req.key);
        let idem_lookup_key = req.idempotency.map(encode_idempotency_key);

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(req.ack == Ack::Flush);
        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let needs_check = !matches!(req.expected, ExpectedVersion::Any);
        let max_attempts = if !needs_check || req.idempotency.is_some() {
            32
        } else {
            1
        };

        for attempt in 0..max_attempts {
            let tx = self.db.transaction_opt(&write_opts, &tx_opts);
            {
                let tx_snap = tx.snapshot();
                let mut read_opts = ReadOptions::default();
                read_opts.set_snapshot(&tx_snap);

                if let Some(replayed) = self.check_idempotency_in_tx(
                    &tx,
                    &meta_cf,
                    &read_opts,
                    req.idempotency,
                    idem_lookup_key.as_deref(),
                    req.ack,
                )? {
                    return Ok((replayed, true));
                }

                if needs_check {
                    let current = tx
                        .get_pinned_for_update_cf_opt(&kv_cf, &enc_key, true, &read_opts)
                        .map_err(map_tx_err)?;
                    let actual_version = match current {
                        Some(raw) => Some(decode_record_parts(raw.as_ref())?.0.version),
                        None => None,
                    };
                    verify_expected_version(req.namespace, req.key, req.expected, actual_version)?;
                }

                if !req.preserve_indexes {
                    let applied_indexes = match &req.op {
                        MutationOp::Put { .. } => req.indexes,
                        MutationOp::Remove => None,
                    };
                    let idx_cfs = IndexCfHandles {
                        ord: &idx_ord_cf,
                        edge: &idx_edge_cf,
                        rev: &idx_rev_cf,
                    };
                    let target = IndexTarget {
                        namespace: req.namespace,
                        pk: req.key,
                        enc_pk: &enc_key,
                    };
                    apply_index_mutations_in_tx(&tx, &read_opts, &idx_cfs, &target, applied_indexes)?;
                }
            }

            let ticket = self.sequencer.reserve();
            let seq = ticket.seq();
            let ts_ms = self.hlc.tick();

            let write_ctx = KvStageCtx {
                tx: &tx,
                kv_cf: &kv_cf,
                meta_cf: &meta_cf,
                flags: req.flags,
                seq,
                ts_ms,
            };
            let (kind, value_hint) = self.stage_kv_mutation(&write_ctx, &enc_key, &req.op)?;

            let event = SignalEvent {
                seq,
                epoch: self.epoch,
                namespace: req.namespace.to_owned(),
                key: req.key.to_vec(),
                version: seq,
                kind,
                value_hint,
            };
            let events = vec![event];
            let delta = LogMutationDelta {
                namespace: req.namespace.to_owned(),
                key: req.key.to_vec(),
                op: req.op.clone(),
                flags: req.flags,
                preserve_indexes: req.preserve_indexes,
                is_tier_swap: false,
                indexes: match &req.op {
                    MutationOp::Put { .. } => req.indexes.cloned(),
                    MutationOp::Remove => None,
                },
                search_op: validated.sanitized_search_op.clone(),
                origin_stamp: None,
            };
            let encoded_log =
                encode_log_entry_v4(self.epoch, seq, ts_ms, false, &events, &[], &[delta], None);
            tx.put_cf(&log_cf, encode_log_seq(seq), &encoded_log)
                .map_err(map_tx_err)?;

            self.stage_idempotency_record(
                &tx,
                &meta_cf,
                req.idempotency,
                idem_lookup_key.as_deref(),
                seq,
                vec![seq],
            )?;

            match tx.commit().map_err(map_tx_err) {
                Ok(()) => {
                    ticket.commit(events);
                    self.record_idempotency_commit(req.idempotency.is_some());
                    return Ok((
                        CommitResult {
                            seq,
                            epoch: self.epoch,
                            versions: vec![seq],
                        },
                        false,
                    ));
                }
                Err(MembusError::TxConflict(_))
                    if (!needs_check || req.idempotency.is_some()) && attempt + 1 < max_attempts =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        Err(MembusError::TxConflict(
            "unconditional write exceeded optimistic retry limit under contention".to_owned(),
        ))
    }

    pub(crate) fn commit_indexed_tx_idempotent_impl(
        &self,
        guards: &[VersionGuard],
        mutations: &[IndexedMutation],
        ack: Ack,
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<(CommitResult, bool), MembusError> {
        self.commit_indexed_tx_idempotent_with_leases_impl(guards, &[], mutations, ack, idempotency)
    }

    pub(crate) fn commit_indexed_tx_idempotent_with_leases_impl(
        &self,
        guards: &[VersionGuard],
        lease_guards: &[LeaseGuard],
        mutations: &[IndexedMutation],
        ack: Ack,
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<(CommitResult, bool), MembusError> {
        let validated = self.validate_tx_batch(guards, lease_guards, mutations, idempotency)?;
        let _permit = self.admission.try_acquire(validated.total_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let lease_cf = self.cf(CF_LEASE)?;
        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let idx_ord_cf = self.cf(CF_IDX_ORD)?;
        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let idx_rev_cf = self.cf(CF_IDX_REV)?;
        let idem_lookup_key = idempotency.map(encode_idempotency_key);
        let encoded_lease_keys: Vec<Vec<u8>> = lease_guards
            .iter()
            .map(|lg| encode_kv_key(&lg.namespace, lg.resource.as_bytes()))
            .collect();

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(ack == Ack::Flush);
        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let max_attempts = if idempotency.is_some() { 16 } else { 1 };
        for attempt in 0..max_attempts {
            // Serialize lease-guarded transactions against concurrent acquire/renew/release
            // in canonical ascending stripe order, held through commit or abort.
            let _lease_stripe_guards = if encoded_lease_keys.is_empty() {
                Vec::new()
            } else {
                self.lock_lease_stripes(&encoded_lease_keys)
            };

            let tx = self.db.transaction_opt(&write_opts, &tx_opts);
            {
                let tx_snap = tx.snapshot();
                let mut read_opts = ReadOptions::default();
                read_opts.set_snapshot(&tx_snap);

                if let Some(replayed) = self.check_idempotency_in_tx(
                    &tx,
                    &meta_cf,
                    &read_opts,
                    idempotency,
                    idem_lookup_key.as_deref(),
                    ack,
                )? {
                    return Ok((replayed, true));
                }

                if !lease_guards.is_empty() {
                    let lease_now_ms = self.monotonic_now_ms_impl();
                    for (lg, enc_lease_key) in lease_guards.iter().zip(encoded_lease_keys.iter()) {
                        let current_lease = tx
                            .get_pinned_for_update_cf_opt(&lease_cf, enc_lease_key, true, &read_opts)
                            .map_err(map_tx_err)?;
                        verify_lease_guard_bytes(current_lease.as_deref(), lg, lease_now_ms)?;
                    }
                }

                let idx_cfs = IndexCfHandles {
                    ord: &idx_ord_cf,
                    edge: &idx_edge_cf,
                    rev: &idx_rev_cf,
                };
                verify_tx_guards_and_indexes(&tx, &read_opts, &kv_cf, &idx_cfs, guards, mutations)?;
            }

            let ticket = self.sequencer.reserve();
            let seq = ticket.seq();
            let ts_ms = self.hlc.tick();

            let write_ctx = KvStageCtx {
                tx: &tx,
                kv_cf: &kv_cf,
                meta_cf: &meta_cf,
                flags: 0,
                seq,
                ts_ms,
            };
            let staged =
                self.stage_tx_mutations(&write_ctx, mutations, &validated.sanitized_per_mutation)?;

            let encoded_log = encode_log_entry_v4(
                self.epoch,
                seq,
                ts_ms,
                false,
                &staged.events,
                &[],
                &staged.delta_mutations,
                None,
            );
            tx.put_cf(&log_cf, encode_log_seq(seq), &encoded_log)
                .map_err(map_tx_err)?;

            self.stage_idempotency_record(
                &tx,
                &meta_cf,
                idempotency,
                idem_lookup_key.as_deref(),
                seq,
                staged.versions.clone(),
            )?;

            match tx.commit().map_err(map_tx_err) {
                Ok(()) => {
                    ticket.commit(staged.events);
                    self.record_idempotency_commit(idempotency.is_some());
                    return Ok((
                        CommitResult {
                            seq,
                            epoch: self.epoch,
                            versions: staged.versions,
                        },
                        false,
                    ));
                }
                Err(MembusError::TxConflict(_))
                    if idempotency.is_some() && attempt + 1 < max_attempts =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        Err(MembusError::TxConflict(
            "idempotent transaction exceeded optimistic retry limit under contention".to_owned(),
        ))
    }

    fn validate_single_mutation(
        &self,
        req: &SingleMutationReq<'_>,
    ) -> Result<ValidatedSingleMutation, MembusError> {
        validate_namespace(req.namespace)?;
        validate_key(req.key)?;
        if let Some(idem) = req.idempotency {
            validate_idempotency_context(idem)?;
        }
        let index_bytes = match req.indexes {
            Some(idx_muts) => validate_and_estimate_indexes(idx_muts, self.config.max_value_bytes)?,
            None => 0,
        };
        let sanitized_search_op = match req.search_op {
            Some(op) => Some(sanitize_search_log_op(op)?),
            None => None,
        };
        let payload_bytes = match &req.op {
            MutationOp::Put { value } => {
                validate_value(value, self.config.max_value_bytes)?;
                value.len()
            }
            MutationOp::Remove => 0,
        };
        let search_bytes = sanitized_search_op
            .as_ref()
            .map_or(0, estimate_search_op_bytes);
        let estimated_bytes = req
            .namespace
            .len()
            .saturating_add(req.key.len())
            .saturating_add(payload_bytes)
            .saturating_add(index_bytes)
            .saturating_add(search_bytes)
            .saturating_add(64);
        Ok(ValidatedSingleMutation {
            sanitized_search_op,
            estimated_bytes,
        })
    }

    fn validate_tx_batch(
        &self,
        guards: &[VersionGuard],
        lease_guards: &[LeaseGuard],
        mutations: &[IndexedMutation],
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<ValidatedTxBatch, MembusError> {
        if mutations.is_empty() {
            return Err(MembusError::InvalidArgument(
                "commit_tx requires at least one mutation".to_owned(),
            ));
        }
        if let Some(idem) = idempotency {
            validate_idempotency_context(idem)?;
        }
        let mut total_bytes = 0usize;
        for guard in guards {
            validate_namespace(&guard.namespace)?;
            validate_key(&guard.key)?;
            total_bytes = total_bytes.saturating_add(guard.namespace.len() + guard.key.len());
        }
        for lg in lease_guards {
            validate_namespace(&lg.namespace)?;
            validate_key(lg.resource.as_bytes())?;
            validate_key(lg.holder.as_bytes())?;
            if lg.generation == 0 {
                return Err(MembusError::InvalidArgument(
                    "lease_guard generation must be >= 1".to_owned(),
                ));
            }
            if lg.token.is_empty() {
                return Err(MembusError::InvalidArgument(
                    "lease_guard token must not be empty".to_owned(),
                ));
            }
            total_bytes = total_bytes
                .saturating_add(lg.namespace.len() + lg.resource.len() + lg.holder.len());
        }
        let mut seen_targets: HashSet<(&str, &[u8])> = HashSet::with_capacity(mutations.len());
        let mut sanitized_per_mutation = Vec::with_capacity(mutations.len());
        for item in mutations {
            let mutation = &item.mutation;
            validate_namespace(&mutation.namespace)?;
            validate_key(&mutation.key)?;
            if !seen_targets.insert((mutation.namespace.as_str(), mutation.key.as_slice())) {
                return Err(MembusError::InvalidArgument(format!(
                    "duplicate mutation target in commit_tx for namespace '{}'",
                    mutation.namespace
                )));
            }
            if let Some(idx_muts) = &item.indexes {
                total_bytes = total_bytes.saturating_add(validate_and_estimate_indexes(
                    idx_muts,
                    self.config.max_value_bytes,
                )?);
            }
            let sanitized_op = match &item.search_op {
                Some(op) => {
                    let s = sanitize_search_log_op(op)?;
                    total_bytes = total_bytes.saturating_add(estimate_search_op_bytes(&s));
                    Some(s)
                }
                None => None,
            };
            sanitized_per_mutation.push(sanitized_op);
            let payload_len = match &mutation.op {
                MutationOp::Put { value } => {
                    validate_value(value, self.config.max_value_bytes)?;
                    value.len()
                }
                MutationOp::Remove => 0,
            };
            total_bytes = total_bytes
                .saturating_add(mutation.namespace.len() + mutation.key.len() + payload_len + 64);
        }
        Ok(ValidatedTxBatch {
            sanitized_per_mutation,
            total_bytes,
        })
    }

    fn check_idempotency_in_tx(
        &self,
        tx: &Transaction<'_, OptimisticTransactionDB>,
        meta_cf: &Arc<BoundColumnFamily<'_>>,
        read_opts: &ReadOptions,
        idempotency: Option<&IdempotencyContext>,
        idem_lookup_key: Option<&[u8]>,
        ack: Ack,
    ) -> Result<Option<CommitResult>, MembusError> {
        let (Some(idem), Some(ik)) = (idempotency, idem_lookup_key) else {
            return Ok(None);
        };
        let Some(raw) = tx
            .get_pinned_for_update_cf_opt(meta_cf, ik, true, read_opts)
            .map_err(map_tx_err)?
        else {
            return Ok(None);
        };
        let stored: StoredIdempotencyRecord =
            serde_json::from_slice(raw.as_ref()).map_err(|err| {
                MembusError::Corruption(format!("corrupt idempotency dedup record: {err}"))
            })?;
        if stored.request_fingerprint_sha256 == idem.request_fingerprint_sha256 {
            if ack == Ack::Flush {
                self.db.flush_wal(true).map_err(map_rocksdb_err)?;
            }
            return Ok(Some(CommitResult {
                seq: stored.committed_seq,
                epoch: stored.committed_epoch,
                versions: stored.committed_versions,
            }));
        }
        Err(MembusError::IdempotencyConflict(format!(
            "idempotency key '{}' was already committed for a different payload (seq {})",
            idem.key, stored.committed_seq
        )))
    }

    fn stage_idempotency_record(
        &self,
        tx: &Transaction<'_, OptimisticTransactionDB>,
        meta_cf: &Arc<BoundColumnFamily<'_>>,
        idempotency: Option<&IdempotencyContext>,
        idem_lookup_key: Option<&[u8]>,
        seq: u64,
        versions: Vec<u64>,
    ) -> Result<(), MembusError> {
        let (Some(idem), Some(ik)) = (idempotency, idem_lookup_key) else {
            return Ok(());
        };
        let rec = StoredIdempotencyRecord {
            request_fingerprint_sha256: idem.request_fingerprint_sha256,
            committed_seq: seq,
            committed_epoch: self.epoch,
            committed_version: seq,
            committed_versions: versions,
        };
        let rec_bytes = serde_json::to_vec(&rec)
            .map_err(|err| MembusError::Storage(format!("encode idempotency record failed: {err}")))?;
        tx.put_cf(meta_cf, ik, &rec_bytes).map_err(map_tx_err)?;
        tx.put_cf(meta_cf, encode_idempotency_ord_key(seq), ik)
            .map_err(map_tx_err)?;
        Ok(())
    }

    fn record_idempotency_commit(&self, had_idempotency: bool) {
        if !had_idempotency {
            return;
        }
        let new_count = self
            .idempotency_entry_count
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if new_count > self.config.max_idempotency_entries {
            self.prune_idempotency_records();
        }
    }

    fn prune_idempotency_records(&self) {
        let _guard = self.idem_prune_lock.lock();
        let max_entries = self.config.max_idempotency_entries;
        let current = self.idempotency_entry_count.load(Ordering::Acquire);
        if current <= max_entries {
            return;
        }
        let Ok(meta_cf) = self.cf(CF_META) else {
            return;
        };
        let to_remove = current.saturating_sub(max_entries);
        let mut ro = ReadOptions::default();
        ro.fill_cache(false);
        let iter = self.db.iterator_cf_opt(
            &meta_cf,
            ro,
            IteratorMode::From(META_PREFIX_IDEM_ORD, Direction::Forward),
        );
        let mut batch = WriteBatchWithTransaction::<true>::default();
        let mut removed = 0usize;
        for item in iter {
            if removed >= to_remove {
                break;
            }
            let Ok((ord_k, lookup_k)) = item else {
                break;
            };
            if !ord_k.starts_with(META_PREFIX_IDEM_ORD) {
                break;
            }
            let ord_seq = if ord_k.len() == META_PREFIX_IDEM_ORD.len() + 8 {
                Some(u64::from_be_bytes(
                    ord_k[META_PREFIX_IDEM_ORD.len()..].try_into().unwrap(),
                ))
            } else {
                None
            };
            batch.delete_cf(&meta_cf, &ord_k);
            if let (Some(seq), Ok(Some(raw_rec))) = (ord_seq, self.db.get_cf(&meta_cf, &lookup_k)) {
                if let Ok(rec) = serde_json::from_slice::<StoredIdempotencyRecord>(&raw_rec) {
                    if rec.committed_seq == seq {
                        batch.delete_cf(&meta_cf, &lookup_k);
                    }
                } else {
                    batch.delete_cf(&meta_cf, &lookup_k);
                }
            }
            removed += 1;
        }
        if removed > 0 && self.db.write(batch).is_ok() {
            let mut cur = self.idempotency_entry_count.load(Ordering::Acquire);
            while let Err(next) = self.idempotency_entry_count.compare_exchange_weak(
                cur,
                cur.saturating_sub(removed),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                cur = next;
            }
        }
    }

    fn stage_kv_mutation(
        &self,
        ctx: &KvStageCtx<'_>,
        enc_key: &[u8],
        op: &MutationOp,
    ) -> Result<(SignalKind, Option<Vec<u8>>), MembusError> {
        match op {
            MutationOp::Put { value } => {
                let encoded_val = encode_record(ctx.seq, ctx.flags, ctx.ts_ms, value);
                ctx.tx
                    .put_cf(ctx.kv_cf, enc_key, &encoded_val)
                    .map_err(map_tx_err)?;
                Ok((SignalKind::Put, self.build_value_hint(value)))
            }
            MutationOp::Remove => {
                ctx.tx.delete_cf(ctx.kv_cf, enc_key).map_err(map_tx_err)?;
                let tomb_key = encode_prefixed_key(META_PREFIX_MESH_TOMB, enc_key);
                let mut tomb_val = [0u8; 16];
                tomb_val[..8].copy_from_slice(&ctx.seq.to_be_bytes());
                tomb_val[8..16].copy_from_slice(&ctx.ts_ms.to_be_bytes());
                ctx.tx
                    .put_cf(ctx.meta_cf, &tomb_key, tomb_val)
                    .map_err(map_tx_err)?;
                Ok((SignalKind::Delete, None))
            }
        }
    }

    fn stage_tx_mutations(
        &self,
        write_ctx: &KvStageCtx<'_>,
        mutations: &[IndexedMutation],
        sanitized_per_mutation: &[Option<SearchLogOp>],
    ) -> Result<StagedTxBatch, MembusError> {
        let mut events = Vec::with_capacity(mutations.len());
        let mut versions = Vec::with_capacity(mutations.len());
        let mut delta_mutations = Vec::with_capacity(mutations.len());

        for (item, sanitized_op) in mutations.iter().zip(sanitized_per_mutation.iter()) {
            let mutation = &item.mutation;
            let enc_key = encode_kv_key(&mutation.namespace, &mutation.key);
            let (kind, value_hint) = self.stage_kv_mutation(write_ctx, &enc_key, &mutation.op)?;
            versions.push(write_ctx.seq);
            events.push(SignalEvent {
                seq: write_ctx.seq,
                epoch: self.epoch,
                namespace: mutation.namespace.clone(),
                key: mutation.key.clone(),
                version: write_ctx.seq,
                kind,
                value_hint,
            });
            delta_mutations.push(LogMutationDelta {
                namespace: mutation.namespace.clone(),
                key: mutation.key.clone(),
                op: mutation.op.clone(),
                flags: 0,
                preserve_indexes: false,
                is_tier_swap: false,
                indexes: match &mutation.op {
                    MutationOp::Put { .. } => item.indexes.clone(),
                    MutationOp::Remove => None,
                },
                search_op: sanitized_op.clone(),
                origin_stamp: None,
            });
        }
        Ok(StagedTxBatch {
            events,
            versions,
            delta_mutations,
        })
    }
}

fn verify_tx_guards_and_indexes(
    tx: &Transaction<'_, OptimisticTransactionDB>,
    read_opts: &ReadOptions,
    kv_cf: &Arc<BoundColumnFamily<'_>>,
    idx_cfs: &IndexCfHandles<'_>,
    guards: &[VersionGuard],
    mutations: &[IndexedMutation],
) -> Result<(), MembusError> {
    for guard in guards {
        let enc_key = encode_kv_key(&guard.namespace, &guard.key);
        let current = tx
            .get_pinned_for_update_cf_opt(kv_cf, &enc_key, true, read_opts)
            .map_err(map_tx_err)?;
        let actual_version = match current {
            Some(raw) => Some(decode_record_parts(raw.as_ref())?.0.version),
            None => None,
        };
        verify_expected_version(&guard.namespace, &guard.key, guard.expected, actual_version)?;
    }

    for item in mutations {
        let mutation = &item.mutation;
        let enc_key = encode_kv_key(&mutation.namespace, &mutation.key);
        let applied_indexes = match &mutation.op {
            MutationOp::Put { .. } => item.indexes.as_ref(),
            MutationOp::Remove => None,
        };
        let target = IndexTarget {
            namespace: &mutation.namespace,
            pk: &mutation.key,
            enc_pk: &enc_key,
        };
        apply_index_mutations_in_tx(tx, read_opts, idx_cfs, &target, applied_indexes)?;
    }
    Ok(())
}
