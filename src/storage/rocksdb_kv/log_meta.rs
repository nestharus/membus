use std::path::Path;
use std::sync::atomic::Ordering;

use rocksdb::checkpoint::Checkpoint;
use rocksdb::{Direction, IteratorMode, OptimisticTransactionOptions, ReadOptions, WriteOptions};
use sha2::{Digest, Sha256};

use crate::storage::codec::{
    decode_log_entry, decode_log_entry_delta, decode_log_entry_full, decode_log_seq,
    encode_cas_blob_key, encode_log_entry_v4, encode_log_seq, encode_raw_blob_key,
};
use crate::storage::constants::{CF_BLOB, CF_LEASE, CF_LOG, CF_META};
use crate::storage::contract::{MetaCasGuard, RawKeyValue};
use crate::storage::mesh_replication::META_PREFIX_MESH_PEER;
use crate::storage::rocksdb_kv::helpers::{
    decode_epoch_seq_meta, estimate_search_op_bytes, map_rocksdb_err, map_tx_err,
    META_KEY_BACKUP_LAST_SEQ, META_KEY_LOG_HORIZON, META_KEY_MESH_PUB_SEQ,
    META_KEY_SEARCH_CHECKPOINT,
};
use crate::storage::rocksdb_kv::state::RocksDbStorage;
use crate::types::{
    sanitize_search_log_op, validate_key, validate_namespace, validate_value, Ack, CommitResult,
    DeltaLogPage, ExpectedVersion, LogScanPage, LogWatermark, MembusError, SearchLogOp,
    SearchLogPage, SearchLogRecord, WalBatch,
};

impl RocksDbStorage {
    pub(crate) fn put_blob_impl(
        &self,
        digest: &[u8],
        data: &[u8],
        ack: Ack,
    ) -> Result<(), MembusError> {
        validate_key(digest)?;
        validate_value(data, self.config.max_value_bytes)?;
        let key = encode_raw_blob_key(digest);
        let _permit = self
            .admission
            .try_acquire(key.len().saturating_add(data.len()))?;
        let blob_cf = self.cf(CF_BLOB)?;
        let mut wo = WriteOptions::default();
        wo.set_sync(ack == Ack::Flush);
        self.db
            .put_cf_opt(&blob_cf, &key, data, &wo)
            .map_err(map_rocksdb_err)
    }

    pub(crate) fn get_blob_impl(&self, digest: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        validate_key(digest)?;
        let key = encode_raw_blob_key(digest);
        let blob_cf = self.cf(CF_BLOB)?;
        self.db.get_cf(&blob_cf, &key).map_err(map_rocksdb_err)
    }

    pub(crate) fn put_cas_blob_impl(
        &self,
        namespace: &str,
        data: &[u8],
        ack: Ack,
    ) -> Result<[u8; 32], MembusError> {
        validate_namespace(namespace)?;
        validate_value(data, self.config.max_value_bytes)?;
        let digest: [u8; 32] = Sha256::digest(data).into();
        let key = encode_cas_blob_key(namespace, &digest)?;
        let blob_cf = self.cf(CF_BLOB)?;

        if let Some(existing) = self
            .db
            .get_pinned_cf(&blob_cf, &key)
            .map_err(map_rocksdb_err)?
        {
            let existing_digest: [u8; 32] = Sha256::digest(existing.as_ref()).into();
            if existing_digest != digest {
                return Err(MembusError::Corruption(
                    "existing CAS blob SHA-256 mismatch".to_owned(),
                ));
            }
            if ack == Ack::Flush {
                self.db.flush_wal(true).map_err(map_rocksdb_err)?;
            }
            return Ok(digest);
        }

        let _permit = self
            .admission
            .try_acquire(key.len().saturating_add(data.len()))?;
        let mut wo = WriteOptions::default();
        wo.set_sync(ack == Ack::Flush);
        self.db
            .put_cf_opt(&blob_cf, &key, data, &wo)
            .map_err(map_rocksdb_err)?;
        Ok(digest)
    }

    pub(crate) fn get_cas_blob_impl(
        &self,
        namespace: &str,
        sha256: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, MembusError> {
        let key = encode_cas_blob_key(namespace, sha256)?;
        let blob_cf = self.cf(CF_BLOB)?;
        let Some(data) = self.db.get_cf(&blob_cf, &key).map_err(map_rocksdb_err)? else {
            return Ok(None);
        };
        let actual: [u8; 32] = Sha256::digest(&data).into();
        if actual != *sha256 {
            return Err(MembusError::Corruption(
                "CAS blob SHA-256 verification failed on readback".to_owned(),
            ));
        }
        Ok(Some(data))
    }

    pub(crate) fn has_cas_blob_impl(
        &self,
        namespace: &str,
        sha256: &[u8; 32],
    ) -> Result<bool, MembusError> {
        let key = encode_cas_blob_key(namespace, sha256)?;
        let blob_cf = self.cf(CF_BLOB)?;
        Ok(self
            .db
            .get_pinned_cf(&blob_cf, &key)
            .map_err(map_rocksdb_err)?
            .is_some())
    }

    fn check_log_horizon(&self, after_seq: u64) -> Result<(), MembusError> {
        let horizon = self.log_horizon.load(Ordering::Acquire);
        if after_seq < horizon {
            return Err(MembusError::Gap {
                earliest_available_seq: horizon.saturating_add(1),
                current_epoch: self.epoch,
            });
        }
        Ok(())
    }

    pub(crate) fn scan_log_after_impl(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<LogScanPage, MembusError> {
        if limit == 0 {
            return Err(MembusError::InvalidArgument(
                "scan_log_after limit must be > 0".to_owned(),
            ));
        }
        self.check_log_horizon(after_seq)?;

        let upper_seq = self.sequencer.published_seq();
        if after_seq >= upper_seq {
            return Ok(LogScanPage {
                events: Vec::new(),
                scanned_through_seq: after_seq,
            });
        }

        let log_cf = self.cf(CF_LOG)?;
        let start_key = encode_log_seq(after_seq.saturating_add(1));
        let mut read_opts = ReadOptions::default();
        read_opts.fill_cache(false);

        let iter = self.db.iterator_cf_opt(
            &log_cf,
            read_opts,
            IteratorMode::From(&start_key, Direction::Forward),
        );

        let mut events = Vec::new();
        let mut last_seen_seq = after_seq;
        let mut hit_limit = false;

        for (entries_read, item) in iter.enumerate() {
            let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
            let seq = decode_log_seq(&raw_k)?;
            if seq > upper_seq {
                break;
            }
            if entries_read >= limit {
                hit_limit = true;
                break;
            }
            let mut decoded = decode_log_entry(&raw_v)?;
            events.append(&mut decoded);
            last_seen_seq = seq;
        }

        self.check_log_horizon(after_seq)?;
        let scanned_through_seq = if hit_limit { last_seen_seq } else { upper_seq };
        Ok(LogScanPage {
            events,
            scanned_through_seq,
        })
    }

    pub(crate) fn append_search_log_op_impl(
        &self,
        op: SearchLogOp,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let sanitized = sanitize_search_log_op(&op)?;
        let op_bytes = estimate_search_op_bytes(&sanitized);
        if op_bytes > self.config.max_value_bytes {
            return Err(MembusError::InvalidArgument(format!(
                "search op size {op_bytes} exceeds max_value_bytes {}",
                self.config.max_value_bytes
            )));
        }
        let _permit = self.admission.try_acquire(op_bytes.max(64))?;
        let log_cf = self.cf(CF_LOG)?;
        let mut wo = WriteOptions::default();
        wo.set_sync(ack == Ack::Flush);
        let tx = self
            .db
            .transaction_opt(&wo, &OptimisticTransactionOptions::default());

        let ticket = self.sequencer.reserve();
        let seq = ticket.seq();
        let encoded_log = encode_log_entry_v4(
            self.epoch,
            seq,
            self.hlc.tick(),
            false,
            &[],
            &[sanitized],
            &[],
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

    pub(crate) fn scan_search_log_after_impl(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<SearchLogPage, MembusError> {
        if limit == 0 {
            return Err(MembusError::InvalidArgument(
                "scan_search_log_after limit must be > 0".to_owned(),
            ));
        }
        self.check_log_horizon(after_seq)?;

        let upper_seq = self.sequencer.published_seq();
        if after_seq >= upper_seq {
            return Ok(SearchLogPage {
                records: Vec::new(),
                scanned_through_seq: after_seq,
            });
        }

        let log_cf = self.cf(CF_LOG)?;
        let start_key = encode_log_seq(after_seq.saturating_add(1));
        let mut read_opts = ReadOptions::default();
        read_opts.fill_cache(false);

        let iter = self.db.iterator_cf_opt(
            &log_cf,
            read_opts,
            IteratorMode::From(&start_key, Direction::Forward),
        );

        let mut records = Vec::new();
        let mut last_seen_seq = after_seq;
        let mut hit_limit = false;

        for (entries_read, item) in iter.enumerate() {
            let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
            let seq = decode_log_seq(&raw_k)?;
            if seq > upper_seq {
                break;
            }
            if entries_read >= limit {
                hit_limit = true;
                break;
            }
            let (epoch, entry_seq, _events, ops) = decode_log_entry_full(&raw_v)?;
            for (idx, op) in ops.into_iter().enumerate() {
                records.push(SearchLogRecord {
                    seq: entry_seq,
                    op_idx: u32::try_from(idx).unwrap_or(u32::MAX),
                    epoch,
                    op,
                });
            }
            last_seen_seq = seq;
        }

        self.check_log_horizon(after_seq)?;
        let scanned_through_seq = if hit_limit { last_seen_seq } else { upper_seq };
        Ok(SearchLogPage {
            records,
            scanned_through_seq,
        })
    }

    pub(crate) fn scan_deltas_after_impl(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<DeltaLogPage, MembusError> {
        if limit == 0 {
            return Err(MembusError::InvalidArgument(
                "scan_deltas_after limit must be > 0".to_owned(),
            ));
        }
        self.check_log_horizon(after_seq)?;

        let upper_seq = self.sequencer.published_seq();
        if after_seq >= upper_seq {
            return Ok(DeltaLogPage {
                entries: Vec::new(),
                scanned_through_seq: after_seq,
            });
        }

        let log_cf = self.cf(CF_LOG)?;
        let start_key = encode_log_seq(after_seq.saturating_add(1));
        let mut read_opts = ReadOptions::default();
        read_opts.fill_cache(false);

        let iter = self.db.iterator_cf_opt(
            &log_cf,
            read_opts,
            IteratorMode::From(&start_key, Direction::Forward),
        );

        let mut entries = Vec::new();
        let mut last_seen_seq = after_seq;
        let mut hit_limit = false;

        for (entries_read, item) in iter.enumerate() {
            let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
            let seq = decode_log_seq(&raw_k)?;
            if seq > upper_seq {
                break;
            }
            if entries_read >= limit {
                hit_limit = true;
                break;
            }
            entries.push(decode_log_entry_delta(&raw_v)?);
            last_seen_seq = seq;
        }

        self.check_log_horizon(after_seq)?;
        let scanned_through_seq = if hit_limit { last_seen_seq } else { upper_seq };
        Ok(DeltaLogPage {
            entries,
            scanned_through_seq,
        })
    }

    pub(crate) fn trim_log_before_locked(&self, keep_from_seq: u64) -> Result<(), MembusError> {
        if keep_from_seq <= 1 {
            return Ok(());
        }
        let max_keep_from = self.sequencer.published_seq().saturating_add(1);
        if keep_from_seq > max_keep_from {
            return Err(MembusError::InvalidArgument(format!(
                "keep_from_seq {keep_from_seq} exceeds latest committed horizon {max_keep_from}"
            )));
        }
        let new_horizon = keep_from_seq.saturating_sub(1);
        let current_horizon = self.log_horizon.load(Ordering::Acquire);
        if new_horizon <= current_horizon {
            return Ok(());
        }

        let meta_cf = self.cf(CF_META)?;
        let mut sync_wo = WriteOptions::default();
        sync_wo.set_sync(true);
        self.db
            .put_cf_opt(
                &meta_cf,
                META_KEY_LOG_HORIZON,
                new_horizon.to_be_bytes(),
                &sync_wo,
            )
            .map_err(map_rocksdb_err)?;
        self.log_horizon.store(new_horizon, Ordering::Release);

        let log_cf = self.cf(CF_LOG)?;
        self.db
            .delete_range_cf_opt(
                &log_cf,
                encode_log_seq(0),
                encode_log_seq(keep_from_seq),
                &sync_wo,
            )
            .map_err(map_rocksdb_err)?;
        Ok(())
    }

    pub(crate) fn trim_log_before_impl(&self, keep_from_seq: u64) -> Result<(), MembusError> {
        let _trim_guard = self.trim_lock.lock();
        self.trim_log_before_locked(keep_from_seq)
    }

    pub(crate) fn list_log_watermarks_impl(&self) -> Result<Vec<LogWatermark>, MembusError> {
        let mut watermarks = Vec::new();
        for (name, key) in [
            ("search:checkpoint", META_KEY_SEARCH_CHECKPOINT),
            ("backup:last_seq", META_KEY_BACKUP_LAST_SEQ),
            ("mesh:pub_seq", META_KEY_MESH_PUB_SEQ),
        ] {
            if let Some(raw) = self.meta_get_impl(key)? {
                let (ep, seq) = decode_epoch_seq_meta(&raw).ok_or_else(|| {
                    MembusError::Corruption(format!(
                        "corrupt log watermark '{name}': expected 16 bytes, got {}",
                        raw.len()
                    ))
                })?;
                if ep == self.epoch {
                    watermarks.push(LogWatermark {
                        name: name.to_owned(),
                        seq,
                    });
                }
            }
        }

        for (k, v) in self.meta_scan_prefix_impl(META_PREFIX_MESH_PEER)? {
            let peer_name = String::from_utf8_lossy(&k).into_owned();
            let json: serde_json::Value = serde_json::from_slice(&v).map_err(|err| {
                MembusError::Corruption(format!(
                    "corrupt mesh peer watermark '{peer_name}': {err}"
                ))
            })?;
            if let (Some(local_ep), Some(outbound_seq)) = (
                json.get("local_epoch").and_then(|x| x.as_u64()),
                json.get("outbound_acked_seq").and_then(|x| x.as_u64()),
            ) {
                if local_ep == self.epoch {
                    watermarks.push(LogWatermark {
                        name: peer_name,
                        seq: outbound_seq,
                    });
                }
            }
        }
        Ok(watermarks)
    }

    pub(crate) fn safe_trim_log_before_impl(
        &self,
        keep_from_seq: u64,
        force: bool,
    ) -> Result<(), MembusError> {
        let _trim_guard = self.trim_lock.lock();
        if keep_from_seq <= 1 {
            return Ok(());
        }
        if !force {
            let new_horizon = keep_from_seq.saturating_sub(1);
            let watermarks = self.list_log_watermarks_impl()?;
            if let Some(blocking) = watermarks.iter().find(|wm| wm.seq < new_horizon) {
                return Err(MembusError::Conflict {
                    namespace: "_log_trim".to_owned(),
                    key: blocking.name.as_bytes().to_vec(),
                    expected: ExpectedVersion::Version(new_horizon),
                    actual: Some(blocking.seq),
                });
            }
        }
        self.trim_log_before_locked(keep_from_seq)
    }

    pub(crate) fn meta_get_impl(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        validate_key(key)?;
        let meta_cf = self.cf(CF_META)?;
        self.db.get_cf(&meta_cf, key).map_err(map_rocksdb_err)
    }

    pub(crate) fn meta_put_impl(
        &self,
        key: &[u8],
        value: &[u8],
        sync: bool,
    ) -> Result<(), MembusError> {
        validate_key(key)?;
        validate_value(value, self.config.max_value_bytes)?;
        let meta_cf = self.cf(CF_META)?;
        let mut wo = WriteOptions::default();
        wo.set_sync(sync);
        self.db
            .put_cf_opt(&meta_cf, key, value, &wo)
            .map_err(map_rocksdb_err)
    }

    pub(crate) fn meta_scan_prefix_impl(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<RawKeyValue>, MembusError> {
        let meta_cf = self.cf(CF_META)?;
        let mut read_opts = ReadOptions::default();
        read_opts.fill_cache(false);
        let iter = self.db.iterator_cf_opt(
            &meta_cf,
            read_opts,
            IteratorMode::From(prefix, Direction::Forward),
        );
        let mut out = Vec::new();
        for item in iter {
            let (k, v) = item.map_err(map_rocksdb_err)?;
            if !k.starts_with(prefix) {
                break;
            }
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    pub(crate) fn meta_cas_batch_impl(
        &self,
        guards: &[MetaCasGuard],
        puts: &[(Vec<u8>, Vec<u8>)],
        deletes: &[Vec<u8>],
        sync: bool,
    ) -> Result<(), MembusError> {
        let mut total_bytes = 64usize;
        for guard in guards {
            validate_key(&guard.key)?;
            total_bytes = total_bytes.saturating_add(guard.key.len());
        }
        for (k, v) in puts {
            validate_key(k)?;
            validate_value(v, self.config.max_value_bytes)?;
            total_bytes = total_bytes
                .saturating_add(k.len())
                .saturating_add(v.len());
        }
        for k in deletes {
            validate_key(k)?;
            total_bytes = total_bytes.saturating_add(k.len());
        }
        let _permit = self.admission.try_acquire(total_bytes)?;

        let meta_cf = self.cf(CF_META)?;
        let mut wo = WriteOptions::default();
        wo.set_sync(sync);
        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let tx = self.db.transaction_opt(&wo, &tx_opts);
        {
            let tx_snap = tx.snapshot();
            let mut read_opts = ReadOptions::default();
            read_opts.set_snapshot(&tx_snap);
            for guard in guards {
                let actual = tx
                    .get_pinned_for_update_cf_opt(&meta_cf, &guard.key, true, &read_opts)
                    .map_err(map_tx_err)?;
                let matched = match (&guard.expected, &actual) {
                    (None, None) => true,
                    (Some(exp), Some(act)) => exp.as_slice() == act.as_ref(),
                    _ => false,
                };
                if !matched {
                    return Err(MembusError::TxConflict(
                        "meta CAS precondition mismatch".to_owned(),
                    ));
                }
            }
        }
        for (k, v) in puts {
            tx.put_cf(&meta_cf, k, v).map_err(map_tx_err)?;
        }
        for k in deletes {
            tx.delete_cf(&meta_cf, k).map_err(map_tx_err)?;
        }
        tx.commit().map_err(map_tx_err)
    }

    pub(crate) fn lease_get_impl(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        validate_key(key)?;
        let lease_cf = self.cf(CF_LEASE)?;
        self.db.get_cf(&lease_cf, key).map_err(map_rocksdb_err)
    }

    pub(crate) fn lease_cas_impl(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> Result<(), MembusError> {
        validate_key(key)?;
        validate_value(new_value, self.config.max_value_bytes)?;
        let _stripe_guard = self.lock_lease_stripe(key);
        let lease_cf = self.cf(CF_LEASE)?;
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let tx = self.db.transaction_opt(&wo, &tx_opts);
        {
            let tx_snap = tx.snapshot();
            let mut read_opts = ReadOptions::default();
            read_opts.set_snapshot(&tx_snap);
            let actual = tx
                .get_pinned_for_update_cf_opt(&lease_cf, key, true, &read_opts)
                .map_err(map_tx_err)?;
            let matched = match (expected, &actual) {
                (None, None) => true,
                (Some(exp), Some(act)) => exp == act.as_ref(),
                _ => false,
            };
            if !matched {
                return Err(MembusError::TxConflict(
                    "lease optimistic CAS conflict".to_owned(),
                ));
            }
        }
        tx.put_cf(&lease_cf, key, new_value).map_err(map_tx_err)?;
        tx.commit().map_err(map_tx_err)
    }

    pub(crate) fn create_checkpoint_impl(&self, dest_dir: &Path) -> Result<u64, MembusError> {
        let ckpt_seq = self.sequencer.published_seq();
        let checkpoint = Checkpoint::new(&*self.db)
            .map_err(|err| MembusError::Storage(format!("checkpoint init failed: {err}")))?;
        checkpoint
            .create_checkpoint(dest_dir)
            .map_err(|err| MembusError::Storage(format!("checkpoint creation failed: {err}")))?;
        Ok(ckpt_seq)
    }

    pub(crate) fn read_wal_since_impl(
        &self,
        rocksdb_seq: u64,
        limit: usize,
    ) -> Result<Vec<WalBatch>, MembusError> {
        if limit == 0 {
            return Err(MembusError::InvalidArgument(
                "read_wal_since limit must be > 0".to_owned(),
            ));
        }
        let iter = self
            .db
            .get_updates_since(rocksdb_seq)
            .map_err(map_rocksdb_err)?;
        let mut out = Vec::new();
        for item in iter {
            if out.len() >= limit {
                break;
            }
            let (seq, batch) = item.map_err(map_rocksdb_err)?;
            out.push(WalBatch {
                rocksdb_seq: seq,
                raw_bytes: batch.data().to_vec(),
                op_count: batch.len(),
            });
        }
        Ok(out)
    }

    pub(crate) fn flush_wal_impl(&self, sync: bool) -> Result<(), MembusError> {
        self.db.flush_wal(sync).map_err(map_rocksdb_err)
    }
}
