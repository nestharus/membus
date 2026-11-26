use std::collections::{BTreeMap, HashSet};
use std::mem::ManuallyDrop;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex as FastMutex;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType, Direction, ErrorKind,
    IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, Options, ReadOptions,
    SliceTransform, SnapshotWithThreadMode, WriteBufferManager, WriteOptions,
    DEFAULT_COLUMN_FAMILY_NAME,
};

use crate::storage::admission::AdmissionGate;
use crate::storage::codec::{
    decode_kv_key, decode_log_entry, decode_log_seq, decode_record, decode_record_parts,
    encode_kv_key, encode_log_entry, encode_log_seq, kv_prefix_in_domain, kv_prefix_transform,
    next_key_after,
};
use crate::storage::{
    SignalPublisher, StorageConfig, StorageEngine, ALL_COLUMN_FAMILIES, CF_BLOB, CF_IDX_EDGE,
    CF_IDX_ORD, CF_IDX_REV, CF_KV, CF_LEASE, CF_LOG, CF_META,
};
use crate::types::{
    now_unix_ms, validate_key, validate_namespace, validate_value, Ack, CommitResult,
    ExpectedVersion, LogScanPage, MembusError, MutationOp, ScanOptions, ScanPage, SignalEvent,
    SignalKind, StoredRecord, VersionGuard, WalBatch, WriteMutation,
};

const META_KEY_EPOCH: &[u8] = b"db_epoch";
const META_KEY_LOG_HORIZON: &[u8] = b"log_horizon_seq";
static EPOCH_ENTROPY_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Owned, `'static` handle to an O(1) MVCC RocksDB snapshot.
///
/// The snapshot is released strictly before the underlying database handle drops.
pub struct OwnedSnapshot {
    snapshot: ManuallyDrop<SnapshotWithThreadMode<'static, OptimisticTransactionDB>>,
    _db: Arc<OptimisticTransactionDB>,
    snapshot_seq: u64,
}

impl OwnedSnapshot {
    pub fn snapshot_seq(&self) -> u64 {
        self.snapshot_seq
    }

    pub(crate) fn inner(&self) -> &SnapshotWithThreadMode<'static, OptimisticTransactionDB> {
        &self.snapshot
    }
}

impl Drop for OwnedSnapshot {
    fn drop(&mut self) {
        // SAFETY: `self.snapshot` is never accessed again and is dropped while `self._db`
        // still keeps the underlying `OptimisticTransactionDB` alive at a fixed address.
        unsafe {
            ManuallyDrop::drop(&mut self.snapshot);
        }
    }
}

struct SequencerState {
    next_publish_seq: u64,
    ready: BTreeMap<u64, Option<Vec<SignalEvent>>>,
}

struct PublishSequencer {
    next_seq: AtomicU64,
    published_seq: AtomicU64,
    state: FastMutex<SequencerState>,
    publisher: Arc<dyn SignalPublisher>,
}

impl PublishSequencer {
    fn new(initial_seq: u64, publisher: Arc<dyn SignalPublisher>) -> Self {
        Self {
            next_seq: AtomicU64::new(initial_seq.saturating_add(1)),
            published_seq: AtomicU64::new(initial_seq),
            state: FastMutex::new(SequencerState {
                next_publish_seq: initial_seq.saturating_add(1),
                ready: BTreeMap::new(),
            }),
            publisher,
        }
    }

    fn reserve(self: &Arc<Self>) -> SeqTicket {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        SeqTicket {
            seq,
            sequencer: Arc::clone(self),
            settled: false,
        }
    }

    fn published_seq(&self) -> u64 {
        self.published_seq.load(Ordering::Acquire)
    }

    fn settle(&self, seq: u64, events: Option<Vec<SignalEvent>>) {
        let mut guard = self.state.lock();
        guard.ready.insert(seq, events);
        loop {
            let current = guard.next_publish_seq;
            let Some(entry) = guard.ready.remove(&current) else {
                break;
            };
            guard.next_publish_seq = current.saturating_add(1);
            if let Some(committed_events) = entry {
                self.published_seq.store(current, Ordering::Release);
                self.publisher.publish_ordered(&committed_events);
            } else {
                self.published_seq.store(current, Ordering::Release);
            }
        }
    }
}

struct SeqTicket {
    seq: u64,
    sequencer: Arc<PublishSequencer>,
    settled: bool,
}

impl SeqTicket {
    fn seq(&self) -> u64 {
        self.seq
    }

    fn commit(mut self, events: Vec<SignalEvent>) {
        self.settled = true;
        self.sequencer.settle(self.seq, Some(events));
    }
}

impl Drop for SeqTicket {
    fn drop(&mut self) {
        if !self.settled {
            self.sequencer.settle(self.seq, None);
        }
    }
}

struct BackgroundWalSync {
    stop_flag: Arc<(Mutex<bool>, Condvar)>,
    handle: Option<JoinHandle<()>>,
}

impl BackgroundWalSync {
    fn spawn(db: &Arc<OptimisticTransactionDB>, interval: Duration) -> Result<Self, MembusError> {
        let stop_flag = Arc::new((Mutex::new(false), Condvar::new()));
        let stop_ref = Arc::clone(&stop_flag);
        let weak_db: Weak<OptimisticTransactionDB> = Arc::downgrade(db);

        let handle = thread::Builder::new()
            .name("membus-wal-sync".to_owned())
            .spawn(move || {
                let (lock, condvar) = &*stop_ref;
                let mut stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
                while !*stopped {
                    let (guard, _) = condvar
                        .wait_timeout(stopped, interval)
                        .unwrap_or_else(|e| e.into_inner());
                    stopped = guard;
                    if *stopped {
                        break;
                    }
                    drop(stopped);
                    if let Some(db) = weak_db.upgrade() {
                        let _ = db.flush_wal(true);
                    } else {
                        break;
                    }
                    stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
                }
            })
            .map_err(|err| MembusError::Storage(format!("failed to spawn wal sync thread: {err}")))?;

        Ok(Self {
            stop_flag,
            handle: Some(handle),
        })
    }
}

impl Drop for BackgroundWalSync {
    fn drop(&mut self) {
        let (lock, condvar) = &*self.stop_flag;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
            condvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Multi-threaded RocksDB storage engine backed by `OptimisticTransactionDB`.
pub struct RocksDbStorage {
    db: Arc<OptimisticTransactionDB>,
    config: StorageConfig,
    epoch: u64,
    log_horizon: AtomicU64,
    trim_lock: FastMutex<()>,
    sequencer: Arc<PublishSequencer>,
    admission: AdmissionGate,
    _bg_sync: Option<BackgroundWalSync>,
}

impl RocksDbStorage {
    pub fn open(
        path: &Path,
        config: StorageConfig,
        publisher: Arc<dyn SignalPublisher>,
    ) -> Result<Self, MembusError> {
        validate_config(&config)?;

        let block_cache = Cache::new_lru_cache(config.block_cache_bytes);
        let write_buffer_manager =
            WriteBufferManager::new_write_buffer_manager(config.total_write_buffer_bytes, false);

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_manager(&write_buffer_manager);
        db_opts.set_wal_ttl_seconds(config.wal_ttl_seconds);
        db_opts.set_wal_size_limit_mb(config.wal_size_limit_mb);
        db_opts.increase_parallelism(
            thread::available_parallelism()
                .map(|n| n.get().clamp(2, 16) as i32)
                .unwrap_or(4),
        );
        db_opts.set_max_background_jobs(6);

        let mut descriptors = Vec::with_capacity(ALL_COLUMN_FAMILIES.len() + 1);
        descriptors.push(ColumnFamilyDescriptor::new(
            DEFAULT_COLUMN_FAMILY_NAME,
            base_cf_options(&config, &block_cache, &write_buffer_manager),
        ));
        descriptors.push(ColumnFamilyDescriptor::new(
            CF_KV,
            kv_cf_options(&config, &block_cache, &write_buffer_manager),
        ));
        descriptors.push(ColumnFamilyDescriptor::new(
            CF_LOG,
            base_cf_options(&config, &block_cache, &write_buffer_manager),
        ));
        descriptors.push(ColumnFamilyDescriptor::new(
            CF_BLOB,
            blob_cf_options(&config, &block_cache, &write_buffer_manager),
        ));
        for reserved_cf in [CF_IDX_ORD, CF_IDX_EDGE, CF_IDX_REV, CF_LEASE, CF_META] {
            descriptors.push(ColumnFamilyDescriptor::new(
                reserved_cf,
                base_cf_options(&config, &block_cache, &write_buffer_manager),
            ));
        }

        let db: OptimisticTransactionDB =
            OptimisticTransactionDB::open_cf_descriptors(&db_opts, path, descriptors)
                .map_err(|err| MembusError::Storage(format!("rocksdb open failed: {err}")))?;
        let db = Arc::new(db);

        let (epoch, log_horizon_seq, last_log_seq) = {
            let meta_cf = db
                .cf_handle(CF_META)
                .ok_or_else(|| MembusError::Storage("missing meta column family".to_owned()))?;
            let epoch = match db
                .get_cf(&meta_cf, META_KEY_EPOCH)
                .map_err(map_rocksdb_err)?
            {
                Some(bytes) => decode_u64_meta(&bytes, "db_epoch")?,
                None => {
                    let initial_epoch = generate_unique_epoch();
                    let mut wo = WriteOptions::default();
                    wo.set_sync(true);
                    db.put_cf_opt(&meta_cf, META_KEY_EPOCH, initial_epoch.to_be_bytes(), &wo)
                        .map_err(map_rocksdb_err)?;
                    initial_epoch
                }
            };

            let log_horizon_seq = match db
                .get_cf(&meta_cf, META_KEY_LOG_HORIZON)
                .map_err(map_rocksdb_err)?
            {
                Some(bytes) => decode_u64_meta(&bytes, "log_horizon_seq")?,
                None => 0u64,
            };

            let log_cf = db
                .cf_handle(CF_LOG)
                .ok_or_else(|| MembusError::Storage("missing log column family".to_owned()))?;
            let last_log_seq = read_last_log_seq(&db, &log_cf)?;
            (epoch, log_horizon_seq, last_log_seq)
        };
        let initial_seq = last_log_seq.max(log_horizon_seq);

        let bg_sync = match config.bg_sync_interval {
            Some(interval) if !interval.is_zero() => Some(BackgroundWalSync::spawn(&db, interval)?),
            _ => None,
        };

        let admission = AdmissionGate::new(config.admission);
        let sequencer = Arc::new(PublishSequencer::new(initial_seq, publisher));

        Ok(Self {
            db,
            config,
            epoch,
            log_horizon: AtomicU64::new(log_horizon_seq),
            trim_lock: FastMutex::new(()),
            sequencer,
            admission,
            _bg_sync: bg_sync,
        })
    }

    pub fn column_family_names(&self) -> Vec<&'static str> {
        ALL_COLUMN_FAMILIES
            .iter()
            .copied()
            .filter(|name| self.db.cf_handle(name).is_some())
            .collect()
    }

    fn cf(&self, name: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, MembusError> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| MembusError::Storage(format!("missing column family '{name}'")))
    }

    fn build_value_hint(&self, value: &[u8]) -> Option<Vec<u8>> {
        if value.len() <= self.config.max_signal_hint_bytes {
            Some(value.to_vec())
        } else {
            None
        }
    }

    fn verify_snapshot_owner(&self, snapshot: &OwnedSnapshot) -> Result<(), MembusError> {
        if !Arc::ptr_eq(&self.db, &snapshot._db) {
            return Err(MembusError::InvalidArgument(
                "snapshot belongs to a different RocksDbStorage instance".to_owned(),
            ));
        }
        Ok(())
    }

    fn execute_keyset_scan<F>(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        effective_start: &[u8],
        options: &ScanOptions,
        in_bounds: F,
    ) -> Result<ScanPage, MembusError>
    where
        F: Fn(&[u8]) -> bool,
    {
        let seek_key = encode_kv_key(namespace, effective_start);
        let kv_cf = self.cf(CF_KV)?;
        let mut read_opts = ReadOptions::default();
        read_opts.set_prefix_same_as_start(true);
        read_opts.fill_cache(false);

        let iter = snapshot.inner().iterator_cf_opt(
            &kv_cf,
            read_opts,
            IteratorMode::From(&seek_key, Direction::Forward),
        );

        let mut records = Vec::new();
        let mut accum_bytes = 0usize;
        let mut truncated = false;

        for item in iter {
            let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
            let (rec_ns, rec_key) = decode_kv_key(&raw_k)?;
            if rec_ns != namespace || !in_bounds(rec_key) {
                break;
            }
            let item_bytes = rec_key.len().saturating_add(raw_v.len());
            if records.len() >= options.limit
                || (!records.is_empty()
                    && accum_bytes.saturating_add(item_bytes) > options.max_bytes)
            {
                truncated = true;
                break;
            }
            let record = decode_record(rec_key.to_vec(), &raw_v)?;
            accum_bytes = accum_bytes.saturating_add(item_bytes);
            records.push(record);
        }

        let next_cursor = if truncated {
            records.last().map(|r| r.key.clone())
        } else {
            None
        };

        Ok(ScanPage {
            records,
            next_cursor,
            truncated,
            snapshot_seq: snapshot.snapshot_seq(),
        })
    }

    fn execute_single_mutation(
        &self,
        namespace: &str,
        key: &[u8],
        op: MutationOp,
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        validate_namespace(namespace)?;
        validate_key(key)?;
        let payload_bytes = match &op {
            MutationOp::Put { value } => {
                validate_value(value, self.config.max_value_bytes)?;
                value.len()
            }
            MutationOp::Remove => 0,
        };
        let estimated_bytes = namespace.len() + key.len() + payload_bytes + 64;
        let _permit = self.admission.try_acquire(estimated_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let log_cf = self.cf(CF_LOG)?;
        let enc_key = encode_kv_key(namespace, key);

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(ack == Ack::Flush);

        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let needs_check = !matches!(expected, ExpectedVersion::Any);
        let max_attempts = if needs_check { 1 } else { 32 };

        for attempt in 0..max_attempts {
            let tx = self.db.transaction_opt(&write_opts, &tx_opts);

            if needs_check {
                let tx_snap = tx.snapshot();
                let mut read_opts = ReadOptions::default();
                read_opts.set_snapshot(&tx_snap);
                let current = tx
                    .get_pinned_for_update_cf_opt(&kv_cf, &enc_key, true, &read_opts)
                    .map_err(map_tx_err)?;
                let actual_version = match current {
                    Some(raw) => Some(decode_record_parts(raw.as_ref())?.0.version),
                    None => None,
                };
                verify_expected_version(namespace, key, expected, actual_version)?;
            }

            let ticket = self.sequencer.reserve();
            let seq = ticket.seq();
            let ts_ms = now_unix_ms();

            let (kind, value_hint) = match &op {
                MutationOp::Put { value } => {
                    let encoded_val = crate::storage::codec::encode_record(seq, 0, ts_ms, value);
                    tx.put_cf(&kv_cf, &enc_key, &encoded_val)
                        .map_err(map_tx_err)?;
                    (SignalKind::Put, self.build_value_hint(value))
                }
                MutationOp::Remove => {
                    tx.delete_cf(&kv_cf, &enc_key).map_err(map_tx_err)?;
                    (SignalKind::Delete, None)
                }
            };

            let event = SignalEvent {
                seq,
                epoch: self.epoch,
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                version: seq,
                kind,
                value_hint,
            };
            let events = vec![event];
            let encoded_log = encode_log_entry(self.epoch, seq, &events);
            tx.put_cf(&log_cf, encode_log_seq(seq), &encoded_log)
                .map_err(map_tx_err)?;

            match tx.commit().map_err(map_tx_err) {
                Ok(()) => {
                    ticket.commit(events);
                    return Ok(CommitResult {
                        seq,
                        epoch: self.epoch,
                        versions: vec![seq],
                    });
                }
                Err(MembusError::TxConflict(_)) if !needs_check && attempt + 1 < max_attempts => {
                    // `ticket` drops here and aborts cleanly before retrying.
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        Err(MembusError::TxConflict(
            "unconditional write exceeded optimistic retry limit under contention".to_owned(),
        ))
    }
}

impl StorageEngine for RocksDbStorage {
    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn latest_seq(&self) -> u64 {
        self.sequencer.published_seq()
    }

    fn log_horizon_seq(&self) -> u64 {
        self.log_horizon.load(Ordering::Acquire)
    }

    fn rocksdb_latest_sequence(&self) -> u64 {
        self.db.latest_sequence_number()
    }

    fn in_flight_ops(&self) -> usize {
        self.admission.in_flight_ops()
    }

    fn in_flight_bytes(&self) -> usize {
        self.admission.in_flight_bytes()
    }

    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<StoredRecord>, MembusError> {
        validate_namespace(namespace)?;
        validate_key(key)?;
        let kv_cf = self.cf(CF_KV)?;
        let enc_key = encode_kv_key(namespace, key);
        match self
            .db
            .get_pinned_cf(&kv_cf, &enc_key)
            .map_err(map_rocksdb_err)?
        {
            Some(raw) => Ok(Some(decode_record(key.to_vec(), raw.as_ref())?)),
            None => Ok(None),
        }
    }

    fn get_many(
        &self,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        validate_namespace(namespace)?;
        for key in keys {
            validate_key(key)?;
        }
        let kv_cf = self.cf(CF_KV)?;
        let encoded_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| encode_kv_key(namespace, k))
            .collect();
        let query_pairs: Vec<_> = encoded_keys.iter().map(|k| (&kv_cf, k.as_slice())).collect();
        let raw_results = self.db.multi_get_cf(query_pairs);
        decode_multi_get_results(keys, raw_results)
    }

    fn put(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        self.execute_single_mutation(
            namespace,
            key,
            MutationOp::Put {
                value: value.to_vec(),
            },
            ack,
            expected,
        )
    }

    fn remove(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        self.execute_single_mutation(namespace, key, MutationOp::Remove, ack, expected)
    }

    fn commit_tx(
        &self,
        guards: &[VersionGuard],
        mutations: &[WriteMutation],
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        if mutations.is_empty() {
            return Err(MembusError::InvalidArgument(
                "commit_tx requires at least one mutation".to_owned(),
            ));
        }

        // Validate all guards and mutations cleanly before entering RocksDB.
        let mut total_bytes = 0usize;
        for guard in guards {
            validate_namespace(&guard.namespace)?;
            validate_key(&guard.key)?;
            total_bytes = total_bytes.saturating_add(guard.namespace.len() + guard.key.len());
        }

        let mut seen_targets: HashSet<(&str, &[u8])> = HashSet::with_capacity(mutations.len());
        for mutation in mutations {
            validate_namespace(&mutation.namespace)?;
            validate_key(&mutation.key)?;
            if !seen_targets.insert((mutation.namespace.as_str(), mutation.key.as_slice())) {
                return Err(MembusError::InvalidArgument(format!(
                    "duplicate mutation target in commit_tx for namespace '{}'",
                    mutation.namespace
                )));
            }
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

        let _permit = self.admission.try_acquire(total_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let log_cf = self.cf(CF_LOG)?;

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(ack == Ack::Flush);

        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let tx = self.db.transaction_opt(&write_opts, &tx_opts);

        // Register and verify every nominated guard key against the transaction's snapshot.
        {
            let tx_snap = tx.snapshot();
            let mut read_opts = ReadOptions::default();
            read_opts.set_snapshot(&tx_snap);
            for guard in guards {
                let enc_key = encode_kv_key(&guard.namespace, &guard.key);
                let current = tx
                    .get_pinned_for_update_cf_opt(&kv_cf, &enc_key, true, &read_opts)
                    .map_err(map_tx_err)?;
                let actual_version = match current {
                    Some(raw) => Some(decode_record_parts(raw.as_ref())?.0.version),
                    None => None,
                };
                verify_expected_version(
                    &guard.namespace,
                    &guard.key,
                    guard.expected,
                    actual_version,
                )?;
            }
        }

        let ticket = self.sequencer.reserve();
        let seq = ticket.seq();
        let ts_ms = now_unix_ms();

        let mut events = Vec::with_capacity(mutations.len());
        let mut versions = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            let enc_key = encode_kv_key(&mutation.namespace, &mutation.key);
            let (kind, value_hint) = match &mutation.op {
                MutationOp::Put { value } => {
                    let encoded_val = crate::storage::codec::encode_record(seq, 0, ts_ms, value);
                    tx.put_cf(&kv_cf, &enc_key, &encoded_val)
                        .map_err(map_tx_err)?;
                    (SignalKind::Put, self.build_value_hint(value))
                }
                MutationOp::Remove => {
                    tx.delete_cf(&kv_cf, &enc_key).map_err(map_tx_err)?;
                    (SignalKind::Delete, None)
                }
            };
            versions.push(seq);
            events.push(SignalEvent {
                seq,
                epoch: self.epoch,
                namespace: mutation.namespace.clone(),
                key: mutation.key.clone(),
                version: seq,
                kind,
                value_hint,
            });
        }

        let encoded_log = encode_log_entry(self.epoch, seq, &events);
        tx.put_cf(&log_cf, encode_log_seq(seq), &encoded_log)
            .map_err(map_tx_err)?;

        tx.commit().map_err(map_tx_err)?;
        ticket.commit(events);

        Ok(CommitResult {
            seq,
            epoch: self.epoch,
            versions,
        })
    }

    fn open_snapshot(&self) -> Result<OwnedSnapshot, MembusError> {
        // Capture contiguous published sequence BEFORE pinning the RocksDB snapshot so
        // every `seq <= snapshot_seq` is guaranteed visible in the snapshot and tailing
        // from `snapshot_seq` never skips an in-flight lower sequence number.
        let snapshot_seq = self.sequencer.published_seq();
        let db_arc = Arc::clone(&self.db);
        let db_ptr: *const OptimisticTransactionDB = Arc::as_ptr(&db_arc);
        // SAFETY: `db_arc` is stored in `OwnedSnapshot::_db`, keeping the heap-allocated
        // `OptimisticTransactionDB` alive at a stable address until after `snapshot` drops.
        let db_ref: &'static OptimisticTransactionDB = unsafe { &*db_ptr };
        let snapshot = SnapshotWithThreadMode::new(db_ref);
        Ok(OwnedSnapshot {
            snapshot: ManuallyDrop::new(snapshot),
            _db: db_arc,
            snapshot_seq,
        })
    }

    fn get_in_snapshot(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_key(key)?;
        let kv_cf = self.cf(CF_KV)?;
        let enc_key = encode_kv_key(namespace, key);
        match snapshot
            .inner()
            .get_pinned_cf(&kv_cf, &enc_key)
            .map_err(map_rocksdb_err)?
        {
            Some(raw) => Ok(Some(decode_record(key.to_vec(), raw.as_ref())?)),
            None => Ok(None),
        }
    }

    fn get_many_in_snapshot(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        for key in keys {
            validate_key(key)?;
        }
        let kv_cf = self.cf(CF_KV)?;
        let encoded_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| encode_kv_key(namespace, k))
            .collect();
        let query_pairs: Vec<_> = encoded_keys.iter().map(|k| (&kv_cf, k.as_slice())).collect();
        let raw_results = snapshot.inner().multi_get_cf(query_pairs);
        decode_multi_get_results(keys, raw_results)
    }

    fn scan_prefix(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_scan_options(options)?;
        let effective_start = compute_scan_start(prefix, options.after_key.as_deref());
        self.execute_keyset_scan(snapshot, namespace, &effective_start, options, |key| {
            key.starts_with(prefix)
        })
    }

    fn scan_range(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_scan_options(options)?;

        if !end_key.is_empty() && end_key <= start_key {
            return Ok(ScanPage {
                records: Vec::new(),
                next_cursor: None,
                truncated: false,
                snapshot_seq: snapshot.snapshot_seq(),
            });
        }

        let effective_start = compute_scan_start(start_key, options.after_key.as_deref());
        if !end_key.is_empty() && effective_start.as_slice() >= end_key {
            return Ok(ScanPage {
                records: Vec::new(),
                next_cursor: None,
                truncated: false,
                snapshot_seq: snapshot.snapshot_seq(),
            });
        }

        self.execute_keyset_scan(snapshot, namespace, &effective_start, options, |key| {
            end_key.is_empty() || key < end_key
        })
    }

    fn put_blob(&self, digest: &[u8], data: &[u8], ack: Ack) -> Result<(), MembusError> {
        validate_key(digest)?;
        validate_value(data, self.config.max_value_bytes)?;
        let _permit = self
            .admission
            .try_acquire(digest.len().saturating_add(data.len()))?;
        let blob_cf = self.cf(CF_BLOB)?;
        let mut wo = WriteOptions::default();
        wo.set_sync(ack == Ack::Flush);
        self.db
            .put_cf_opt(&blob_cf, digest, data, &wo)
            .map_err(map_rocksdb_err)
    }

    fn get_blob(&self, digest: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        validate_key(digest)?;
        let blob_cf = self.cf(CF_BLOB)?;
        self.db.get_cf(&blob_cf, digest).map_err(map_rocksdb_err)
    }

    fn scan_log_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<LogScanPage, MembusError> {
        if limit == 0 {
            return Err(MembusError::InvalidArgument(
                "scan_log_after limit must be > 0".to_owned(),
            ));
        }
        let horizon_before = self.log_horizon.load(Ordering::Acquire);
        if after_seq < horizon_before {
            return Err(MembusError::Gap {
                earliest_available_seq: horizon_before.saturating_add(1),
                current_epoch: self.epoch,
            });
        }

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

        let horizon_after = self.log_horizon.load(Ordering::Acquire);
        if after_seq < horizon_after {
            return Err(MembusError::Gap {
                earliest_available_seq: horizon_after.saturating_add(1),
                current_epoch: self.epoch,
            });
        }

        let scanned_through_seq = if hit_limit { last_seen_seq } else { upper_seq };
        Ok(LogScanPage {
            events,
            scanned_through_seq,
        })
    }

    fn trim_log_before(&self, keep_from_seq: u64) -> Result<(), MembusError> {
        let _trim_guard = self.trim_lock.lock();
        if keep_from_seq <= 1 {
            return Ok(());
        }
        let max_keep_from = self.latest_seq().saturating_add(1);
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

        // Persist the new horizon BEFORE removing log records so a crash mid-trim
        // never silently hides trimmed sequence numbers.
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

    fn create_checkpoint(&self, dest_dir: &Path) -> Result<u64, MembusError> {
        // Capture contiguous watermark BEFORE creating the checkpoint so every
        // `seq <= ckpt_seq` is guaranteed present inside `dest_dir`.
        let ckpt_seq = self.sequencer.published_seq();
        let checkpoint = Checkpoint::new(&*self.db)
            .map_err(|err| MembusError::Storage(format!("checkpoint init failed: {err}")))?;
        checkpoint
            .create_checkpoint(dest_dir)
            .map_err(|err| MembusError::Storage(format!("checkpoint creation failed: {err}")))?;
        Ok(ckpt_seq)
    }

    fn read_wal_since(&self, rocksdb_seq: u64, limit: usize) -> Result<Vec<WalBatch>, MembusError> {
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

    fn flush_wal(&self, sync: bool) -> Result<(), MembusError> {
        self.db.flush_wal(sync).map_err(map_rocksdb_err)
    }
}

fn compute_scan_start(base_start: &[u8], after_key: Option<&[u8]>) -> Vec<u8> {
    match after_key {
        Some(after) => {
            let next = next_key_after(after);
            if next.as_slice() > base_start {
                next
            } else {
                base_start.to_vec()
            }
        }
        None => base_start.to_vec(),
    }
}

fn decode_multi_get_results(
    keys: &[Vec<u8>],
    raw_results: Vec<Result<Option<Vec<u8>>, rocksdb::Error>>,
) -> Result<Vec<Option<StoredRecord>>, MembusError> {
    let mut out = Vec::with_capacity(keys.len());
    for (key, res) in keys.iter().zip(raw_results) {
        match res.map_err(map_rocksdb_err)? {
            Some(raw) => out.push(Some(decode_record(key.clone(), &raw)?)),
            None => out.push(None),
        }
    }
    Ok(out)
}

fn validate_config(config: &StorageConfig) -> Result<(), MembusError> {
    if config.write_buffer_size == 0 || config.max_write_buffer_number <= 0 {
        return Err(MembusError::InvalidArgument(
            "write_buffer_size and max_write_buffer_number must be positive".to_owned(),
        ));
    }
    if config.level0_slowdown_writes_trigger <= 0
        || config.level0_stop_writes_trigger < config.level0_slowdown_writes_trigger
    {
        return Err(MembusError::InvalidArgument(
            "invalid level0 write trigger thresholds".to_owned(),
        ));
    }
    if config.admission.max_in_flight_ops == 0 || config.admission.max_in_flight_bytes == 0 {
        return Err(MembusError::InvalidArgument(
            "admission limits must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn validate_scan_options(options: &ScanOptions) -> Result<(), MembusError> {
    if options.limit == 0 {
        return Err(MembusError::InvalidArgument(
            "scan limit must be > 0".to_owned(),
        ));
    }
    if options.max_bytes == 0 {
        return Err(MembusError::InvalidArgument(
            "scan max_bytes must be > 0".to_owned(),
        ));
    }
    Ok(())
}

fn base_cf_options(
    config: &StorageConfig,
    block_cache: &Cache,
    write_buffer_manager: &WriteBufferManager,
) -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(config.write_buffer_size);
    opts.set_max_write_buffer_number(config.max_write_buffer_number);
    opts.set_level_zero_slowdown_writes_trigger(config.level0_slowdown_writes_trigger);
    opts.set_level_zero_stop_writes_trigger(config.level0_stop_writes_trigger);
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_write_buffer_manager(write_buffer_manager);

    let mut table = BlockBasedOptions::default();
    table.set_block_cache(block_cache);
    table.set_bloom_filter(10.0, false);
    table.set_cache_index_and_filter_blocks(true);
    table.set_pin_l0_filter_and_index_blocks_in_cache(true);
    opts.set_block_based_table_factory(&table);
    opts
}

fn kv_cf_options(
    config: &StorageConfig,
    block_cache: &Cache,
    write_buffer_manager: &WriteBufferManager,
) -> Options {
    let mut opts = base_cf_options(config, block_cache, write_buffer_manager);
    opts.set_prefix_extractor(SliceTransform::create(
        "membus_ns_prefix",
        kv_prefix_transform,
        Some(kv_prefix_in_domain),
    ));

    let mut table = BlockBasedOptions::default();
    table.set_block_cache(block_cache);
    table.set_bloom_filter(10.0, false);
    table.set_whole_key_filtering(true);
    table.set_cache_index_and_filter_blocks(true);
    table.set_pin_l0_filter_and_index_blocks_in_cache(true);
    opts.set_block_based_table_factory(&table);

    configure_blob_db(&mut opts, config.blob_min_size);
    opts
}

fn blob_cf_options(
    config: &StorageConfig,
    block_cache: &Cache,
    write_buffer_manager: &WriteBufferManager,
) -> Options {
    let mut opts = base_cf_options(config, block_cache, write_buffer_manager);
    configure_blob_db(&mut opts, config.blob_min_size);
    opts
}

fn configure_blob_db(opts: &mut Options, min_blob_size: u64) {
    opts.set_enable_blob_files(true);
    opts.set_min_blob_size(min_blob_size);
    opts.set_blob_file_size(256 * 1024 * 1024);
    opts.set_blob_compression_type(DBCompressionType::Zstd);
    opts.set_enable_blob_gc(true);
    opts.set_blob_gc_age_cutoff(0.25);
    opts.set_blob_gc_force_threshold(0.8);
}

fn verify_expected_version(
    namespace: &str,
    key: &[u8],
    expected: ExpectedVersion,
    actual: Option<u64>,
) -> Result<(), MembusError> {
    let matched = match expected {
        ExpectedVersion::Any => true,
        ExpectedVersion::Absent => actual.is_none(),
        ExpectedVersion::Version(v) => actual == Some(v),
    };
    if matched {
        Ok(())
    } else {
        Err(MembusError::Conflict {
            namespace: namespace.to_owned(),
            key: key.to_vec(),
            expected,
            actual,
        })
    }
}

fn read_last_log_seq(
    db: &OptimisticTransactionDB,
    log_cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
) -> Result<u64, MembusError> {
    let mut iter = db.iterator_cf(log_cf, IteratorMode::End);
    match iter.next() {
        Some(Ok((key, _))) => decode_log_seq(&key),
        Some(Err(err)) => Err(map_rocksdb_err(err)),
        None => Ok(0),
    }
}

fn generate_unique_epoch() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);
    let pid = u64::from(std::process::id());
    let salt = EPOCH_ENTROPY_COUNTER.fetch_add(1, Ordering::Relaxed);
    // SplitMix64 avalanche mixing for a non-zero unique lineage identifier.
    let mut z = nanos
        .wrapping_add(pid.rotate_left(27))
        .wrapping_add(salt.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    let mixed = z ^ (z >> 31);
    if mixed == 0 {
        1
    } else {
        mixed
    }
}

fn decode_u64_meta(bytes: &[u8], name: &str) -> Result<u64, MembusError> {
    if bytes.len() != 8 {
        return Err(MembusError::Corruption(format!(
            "meta entry '{name}' expected 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn map_rocksdb_err(err: rocksdb::Error) -> MembusError {
    match err.kind() {
        ErrorKind::Corruption => MembusError::Corruption(err.into_string()),
        ErrorKind::InvalidArgument => MembusError::InvalidArgument(err.into_string()),
        ErrorKind::Busy | ErrorKind::TryAgain => MembusError::Busy {
            retry_after_ms: 5,
            reason: err.into_string(),
        },
        _ => MembusError::Storage(err.into_string()),
    }
}

fn map_tx_err(err: rocksdb::Error) -> MembusError {
    match err.kind() {
        ErrorKind::Busy | ErrorKind::TryAgain => MembusError::TxConflict(err.into_string()),
        ErrorKind::Corruption => MembusError::Corruption(err.into_string()),
        ErrorKind::InvalidArgument => MembusError::InvalidArgument(err.into_string()),
        _ => MembusError::Storage(err.into_string()),
    }
}
