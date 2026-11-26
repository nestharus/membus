use std::collections::{BTreeMap, HashSet, VecDeque};
use std::mem::ManuallyDrop;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex as FastMutex;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType, Direction, ErrorKind,
    IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, Options, ReadOptions,
    SliceTransform, SnapshotWithThreadMode, Transaction, WriteBufferManager, WriteOptions,
    DEFAULT_COLUMN_FAMILY_NAME,
};
use sha2::{Digest, Sha256};

use crate::index::{
    decode_edge_suffix, decode_ordered_suffix, encode_edge_key, encode_edge_seek_prefix,
    encode_index_prefix, encode_order_preserving_prefix, encode_ordered_key, validate_bfs_options,
    validate_index_id, validate_index_mutations, BfsOptions, NeighborQueryOptions,
    OrderedKeyFilter, OrderedQueryOptions, ReversePostings, EDGE_DIR_FORWARD, EDGE_DIR_REVERSE,
};
use crate::storage::admission::AdmissionGate;
use crate::storage::codec::{
    decode_kv_key, decode_log_entry, decode_log_entry_delta, decode_log_entry_full, decode_log_seq,
    decode_record, decode_record_parts, encode_cas_blob_key, encode_kv_key, encode_log_entry_v4,
    encode_log_seq, encode_raw_blob_key, kv_prefix_in_domain, kv_prefix_transform, next_key_after,
};
use crate::storage::{
    MetaCasGuard, SignalPublisher, StorageConfig, StorageEngine, ALL_COLUMN_FAMILIES, CF_BLOB,
    CF_IDX_EDGE, CF_IDX_ORD, CF_IDX_REV, CF_KV, CF_LEASE, CF_LOG, CF_META,
};
use crate::types::{
    now_unix_ms, sanitize_search_log_op, validate_key, validate_namespace, validate_value, Ack,
    BfsResult, CommitResult, DeltaLogPage, ExpectedVersion, GraphDirection, GraphEdgeRecord,
    IndexMutations, IndexedMutation, LogMutationDelta, LogScanPage, LogWatermark, MembusError,
    MutationOp, NeighborPage, OrderedIndexHit, OrderedIndexPage, ReplicatedApplyResult,
    ReplicatedDelta, ReplicationStamp, ScanOptions, ScanPage, SearchLogOp, SearchLogPage,
    SearchLogRecord, SignalEvent, SignalKind, StoredRecord, VersionGuard, WalBatch, WriteMutation,
};

const META_KEY_EPOCH: &[u8] = b"db_epoch";
const META_KEY_LOG_HORIZON: &[u8] = b"log_horizon_seq";
const META_KEY_SEARCH_CHECKPOINT: &[u8] = b"search:checkpoint";
const META_KEY_BACKUP_LAST_SEQ: &[u8] = b"backup:last_seq";
const META_KEY_MESH_PUB_SEQ: &[u8] = b"mesh:pub_seq";
const META_PREFIX_MESH_LWW: &[u8] = b"mesh:lww:";
const META_PREFIX_MESH_LOCAL_LWW: &[u8] = b"mesh:local_lww:";
const META_PREFIX_MESH_TOMB: &[u8] = b"mesh:tomb:";
const META_PREFIX_MESH_PEER: &[u8] = b"mesh:peer:";
const META_PREFIX_MESH_SEARCH_SEEN: &[u8] = b"mesh:search_seen:";
static EPOCH_ENTROPY_COUNTER: AtomicU64 = AtomicU64::new(1);

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

/// Monotonic in-process Hybrid Logical Clock (millisecond precision) ensuring causal
/// local writes always advance past observed remote timestamps even under negative clock skew.
struct HlcClock {
    last_ms: AtomicU64,
}

impl HlcClock {
    fn new(initial_ms: u64) -> Self {
        Self {
            last_ms: AtomicU64::new(initial_ms.max(now_unix_ms())),
        }
    }

    fn current(&self) -> u64 {
        self.last_ms.load(Ordering::Acquire).max(now_unix_ms())
    }

    /// Advance the clock for a new local write: `max(wall_ms, prev + 1)`.
    fn tick(&self) -> u64 {
        let wall = now_unix_ms();
        let mut prev = self.last_ms.load(Ordering::Acquire);
        loop {
            let next = wall.max(prev.saturating_add(1));
            match self.last_ms.compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(actual) => prev = actual,
            }
        }
    }

    /// Observe a remote peer's timestamp so any subsequent local `tick()` is strictly greater.
    fn observe(&self, remote_ts_ms: u64) {
        let floor = remote_ts_ms.max(now_unix_ms());
        let mut prev = self.last_ms.load(Ordering::Acquire);
        while floor > prev {
            match self.last_ms.compare_exchange_weak(
                prev,
                floor,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
    }
}

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
    data_dir: PathBuf,
    db: Arc<OptimisticTransactionDB>,
    config: StorageConfig,
    epoch: u64,
    log_horizon: AtomicU64,
    trim_lock: FastMutex<()>,
    hlc: HlcClock,
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

        let (epoch, log_horizon_seq, last_log_seq, last_log_ts) = {
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
            let (last_log_seq, last_log_ts) = read_last_log_seq_and_ts(&db, &log_cf)?;
            (epoch, log_horizon_seq, last_log_seq, last_log_ts)
        };
        let initial_seq = last_log_seq.max(log_horizon_seq);

        let bg_sync = match config.bg_sync_interval {
            Some(interval) if !interval.is_zero() => Some(BackgroundWalSync::spawn(&db, interval)?),
            _ => None,
        };

        let admission = AdmissionGate::new(config.admission);
        let sequencer = Arc::new(PublishSequencer::new(initial_seq, publisher));

        Ok(Self {
            data_dir: path.to_path_buf(),
            db,
            config,
            epoch,
            log_horizon: AtomicU64::new(log_horizon_seq),
            trim_lock: FastMutex::new(()),
            hlc: HlcClock::new(last_log_ts),
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

    #[allow(clippy::too_many_arguments)]
    fn execute_single_mutation(
        &self,
        namespace: &str,
        key: &[u8],
        op: MutationOp,
        flags: u8,
        preserve_indexes: bool,
        ack: Ack,
        expected: ExpectedVersion,
        indexes: Option<&IndexMutations>,
        search_op: Option<&SearchLogOp>,
    ) -> Result<CommitResult, MembusError> {
        validate_namespace(namespace)?;
        validate_key(key)?;
        if let Some(idx_muts) = indexes {
            validate_index_mutations(idx_muts, self.config.max_value_bytes)?;
        }
        let sanitized_search_op = match search_op {
            Some(op) => Some(sanitize_search_log_op(op)?),
            None => None,
        };
        let payload_bytes = match &op {
            MutationOp::Put { value } => {
                validate_value(value, self.config.max_value_bytes)?;
                value.len()
            }
            MutationOp::Remove => 0,
        };
        let index_bytes = indexes.map_or(0, estimate_index_bytes);
        let search_bytes = sanitized_search_op
            .as_ref()
            .map_or(0, estimate_search_op_bytes);
        let estimated_bytes = namespace
            .len()
            .saturating_add(key.len())
            .saturating_add(payload_bytes)
            .saturating_add(index_bytes)
            .saturating_add(search_bytes)
            .saturating_add(64);
        let _permit = self.admission.try_acquire(estimated_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let idx_ord_cf = self.cf(CF_IDX_ORD)?;
        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let idx_rev_cf = self.cf(CF_IDX_REV)?;
        let enc_key = encode_kv_key(namespace, key);

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(ack == Ack::Flush);

        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let needs_check = !matches!(expected, ExpectedVersion::Any);
        let max_attempts = if needs_check { 1 } else { 32 };

        for attempt in 0..max_attempts {
            let tx = self.db.transaction_opt(&write_opts, &tx_opts);

            {
                let tx_snap = tx.snapshot();
                let mut read_opts = ReadOptions::default();
                read_opts.set_snapshot(&tx_snap);

                if needs_check {
                    let current = tx
                        .get_pinned_for_update_cf_opt(&kv_cf, &enc_key, true, &read_opts)
                        .map_err(map_tx_err)?;
                    let actual_version = match current {
                        Some(raw) => Some(decode_record_parts(raw.as_ref())?.0.version),
                        None => None,
                    };
                    verify_expected_version(namespace, key, expected, actual_version)?;
                }

                if !preserve_indexes {
                    let applied_indexes = match &op {
                        MutationOp::Put { .. } => indexes,
                        MutationOp::Remove => None,
                    };
                    apply_index_mutations_in_tx(
                        &tx,
                        &read_opts,
                        &idx_ord_cf,
                        &idx_edge_cf,
                        &idx_rev_cf,
                        namespace,
                        key,
                        &enc_key,
                        applied_indexes,
                    )?;
                }
            }

            let ticket = self.sequencer.reserve();
            let seq = ticket.seq();
            let ts_ms = self.hlc.tick();

            let (kind, value_hint) = match &op {
                MutationOp::Put { value } => {
                    let encoded_val =
                        crate::storage::codec::encode_record(seq, flags, ts_ms, value);
                    tx.put_cf(&kv_cf, &enc_key, &encoded_val)
                        .map_err(map_tx_err)?;
                    (SignalKind::Put, self.build_value_hint(value))
                }
                MutationOp::Remove => {
                    tx.delete_cf(&kv_cf, &enc_key).map_err(map_tx_err)?;
                    let tomb_key = encode_prefixed_key(META_PREFIX_MESH_TOMB, &enc_key);
                    let mut tomb_val = [0u8; 16];
                    tomb_val[..8].copy_from_slice(&seq.to_be_bytes());
                    tomb_val[8..16].copy_from_slice(&ts_ms.to_be_bytes());
                    tx.put_cf(&meta_cf, &tomb_key, tomb_val)
                        .map_err(map_tx_err)?;
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
            let delta = LogMutationDelta {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                op: op.clone(),
                flags,
                preserve_indexes,
                is_tier_swap: false,
                indexes: match &op {
                    MutationOp::Put { .. } => indexes.cloned(),
                    MutationOp::Remove => None,
                },
                search_op: sanitized_search_op.clone(),
                origin_stamp: None,
            };
            let encoded_log = encode_log_entry_v4(
                self.epoch,
                seq,
                ts_ms,
                false,
                &events,
                &[],
                &[delta],
                None,
            );
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
    fn trim_log_before_locked(&self, keep_from_seq: u64) -> Result<(), MembusError> {
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
}

impl StorageEngine for RocksDbStorage {
    fn data_dir(&self) -> Option<&Path> {
        Some(&self.data_dir)
    }

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

    fn hlc_now_ms(&self) -> u64 {
        self.hlc.current()
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
        self.put_indexed(namespace, key, value, ack, expected, None, None)
    }

    fn put_with_flags(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        flags: u8,
        preserve_indexes: bool,
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        self.execute_single_mutation(
            namespace,
            key,
            MutationOp::Put {
                value: value.to_vec(),
            },
            flags,
            preserve_indexes,
            ack,
            expected,
            None,
            None,
        )
    }

    fn tier_swap_record(
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
            old_meta.flags | crate::storage::codec::RECORD_FLAG_COLD_REF
        } else {
            old_meta.flags & !crate::storage::codec::RECORD_FLAG_COLD_REF
        };

        let ticket = self.sequencer.reserve();
        let seq = ticket.seq();

        // Preserve original logical timestamp (`old_meta.ts_ms`) and bitwise-update ONLY cold flag.
        let encoded_val =
            crate::storage::codec::encode_record(seq, new_flags, old_meta.ts_ms, new_value);
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

    fn apply_replicated_delta(
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

    fn append_replicated_search_ops(
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

    fn put_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        indexes: Option<&IndexMutations>,
        search_op: Option<&SearchLogOp>,
    ) -> Result<CommitResult, MembusError> {
        self.execute_single_mutation(
            namespace,
            key,
            MutationOp::Put {
                value: value.to_vec(),
            },
            0,
            false,
            ack,
            expected,
            indexes,
            search_op,
        )
    }

    fn remove(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        self.remove_indexed(namespace, key, ack, expected, None)
    }

    fn remove_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        search_op: Option<&SearchLogOp>,
    ) -> Result<CommitResult, MembusError> {
        self.execute_single_mutation(
            namespace,
            key,
            MutationOp::Remove,
            0,
            false,
            ack,
            expected,
            None,
            search_op,
        )
    }

    fn commit_tx(
        &self,
        guards: &[VersionGuard],
        mutations: &[WriteMutation],
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let wrapped: Vec<IndexedMutation> = mutations
            .iter()
            .cloned()
            .map(|mutation| IndexedMutation {
                mutation,
                indexes: None,
                search_op: None,
            })
            .collect();
        self.commit_indexed_tx(guards, &wrapped, ack)
    }

    fn commit_indexed_tx(
        &self,
        guards: &[VersionGuard],
        mutations: &[IndexedMutation],
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        if mutations.is_empty() {
            return Err(MembusError::InvalidArgument(
                "commit_tx requires at least one mutation".to_owned(),
            ));
        }

        // Validate all guards, mutations, and index entries cleanly before entering RocksDB.
        let mut total_bytes = 0usize;
        for guard in guards {
            validate_namespace(&guard.namespace)?;
            validate_key(&guard.key)?;
            total_bytes = total_bytes.saturating_add(guard.namespace.len() + guard.key.len());
        }

        let mut seen_targets: HashSet<(&str, &[u8])> = HashSet::with_capacity(mutations.len());
        let mut sanitized_per_mutation: Vec<Option<SearchLogOp>> =
            Vec::with_capacity(mutations.len());
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
                validate_index_mutations(idx_muts, self.config.max_value_bytes)?;
                total_bytes = total_bytes.saturating_add(estimate_index_bytes(idx_muts));
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

        let _permit = self.admission.try_acquire(total_bytes)?;

        let kv_cf = self.cf(CF_KV)?;
        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let idx_ord_cf = self.cf(CF_IDX_ORD)?;
        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let idx_rev_cf = self.cf(CF_IDX_REV)?;

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(ack == Ack::Flush);

        let mut tx_opts = OptimisticTransactionOptions::default();
        tx_opts.set_snapshot(true);

        let tx = self.db.transaction_opt(&write_opts, &tx_opts);

        // Register & verify guards and retract/apply secondary indexes against the tx snapshot.
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

            for item in mutations {
                let mutation = &item.mutation;
                let enc_key = encode_kv_key(&mutation.namespace, &mutation.key);
                let applied_indexes = match &mutation.op {
                    MutationOp::Put { .. } => item.indexes.as_ref(),
                    MutationOp::Remove => None,
                };
                apply_index_mutations_in_tx(
                    &tx,
                    &read_opts,
                    &idx_ord_cf,
                    &idx_edge_cf,
                    &idx_rev_cf,
                    &mutation.namespace,
                    &mutation.key,
                    &enc_key,
                    applied_indexes,
                )?;
            }
        }

        let ticket = self.sequencer.reserve();
        let seq = ticket.seq();
        let ts_ms = self.hlc.tick();

        let mut events = Vec::with_capacity(mutations.len());
        let mut versions = Vec::with_capacity(mutations.len());
        let mut delta_mutations = Vec::with_capacity(mutations.len());

        for (item, sanitized_op) in mutations.iter().zip(sanitized_per_mutation) {
            let mutation = &item.mutation;
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
                    let tomb_key = encode_prefixed_key(META_PREFIX_MESH_TOMB, &enc_key);
                    let mut tomb_val = [0u8; 16];
                    tomb_val[..8].copy_from_slice(&seq.to_be_bytes());
                    tomb_val[8..16].copy_from_slice(&ts_ms.to_be_bytes());
                    tx.put_cf(&meta_cf, &tomb_key, tomb_val)
                        .map_err(map_tx_err)?;
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
                search_op: sanitized_op,
                origin_stamp: None,
            });
        }

        let encoded_log = encode_log_entry_v4(
            self.epoch,
            seq,
            ts_ms,
            false,
            &events,
            &[],
            &delta_mutations,
            None,
        );
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

    fn query_ordered_index(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        options: &OrderedQueryOptions,
    ) -> Result<OrderedIndexPage, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_index_id(index_id)?;
        if options.limit == 0 || options.max_bytes == 0 {
            return Err(MembusError::InvalidArgument(
                "ordered index limit and max_bytes must be > 0".to_owned(),
            ));
        }

        let ns_idx_prefix = encode_index_prefix(namespace, index_id);
        let base_suffix: Vec<u8> = match &options.filter {
            OrderedKeyFilter::All => Vec::new(),
            OrderedKeyFilter::Prefix(p) => {
                let mut out = Vec::with_capacity(p.len() + 2);
                encode_order_preserving_prefix(p, &mut out);
                out
            }
            OrderedKeyFilter::Range { start, end } => {
                if !end.is_empty() && end <= start {
                    return Ok(OrderedIndexPage {
                        entries: Vec::new(),
                        next_cursor: None,
                        truncated: false,
                        snapshot_seq: snapshot.snapshot_seq(),
                    });
                }
                let mut out = Vec::with_capacity(start.len() + 2);
                encode_order_preserving_prefix(start, &mut out);
                out
            }
        };

        let effective_suffix = compute_scan_start(&base_suffix, options.after_cursor.as_deref());
        let mut seek_key = Vec::with_capacity(ns_idx_prefix.len() + effective_suffix.len());
        seek_key.extend_from_slice(&ns_idx_prefix);
        seek_key.extend_from_slice(&effective_suffix);

        let idx_ord_cf = self.cf(CF_IDX_ORD)?;
        let mut read_opts = ReadOptions::default();
        read_opts.fill_cache(false);

        let iter = snapshot.inner().iterator_cf_opt(
            &idx_ord_cf,
            read_opts,
            IteratorMode::From(&seek_key, Direction::Forward),
        );

        let mut entries = Vec::new();
        let mut last_suffix: Option<Vec<u8>> = None;
        let mut accum_bytes = 0usize;
        let mut truncated = false;

        for item in iter {
            let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
            if !raw_k.starts_with(&ns_idx_prefix) {
                break;
            }
            let suffix = &raw_k[ns_idx_prefix.len()..];
            let (sort_key, primary_key) = decode_ordered_suffix(suffix)?;
            let in_bounds = match &options.filter {
                OrderedKeyFilter::All => true,
                OrderedKeyFilter::Prefix(p) => sort_key.starts_with(p),
                OrderedKeyFilter::Range { start, end } => {
                    sort_key.as_slice() >= start.as_slice()
                        && (end.is_empty() || sort_key.as_slice() < end.as_slice())
                }
            };
            if !in_bounds {
                break;
            }
            let item_bytes = sort_key
                .len()
                .saturating_add(primary_key.len())
                .saturating_add(raw_v.len());
            if entries.len() >= options.limit
                || (!entries.is_empty()
                    && accum_bytes.saturating_add(item_bytes) > options.max_bytes)
            {
                truncated = true;
                break;
            }
            accum_bytes = accum_bytes.saturating_add(item_bytes);
            last_suffix = Some(suffix.to_vec());
            entries.push(OrderedIndexHit {
                sort_key,
                primary_key,
                summary: raw_v.to_vec(),
            });
        }

        Ok(OrderedIndexPage {
            entries,
            next_cursor: if truncated { last_suffix } else { None },
            truncated,
            snapshot_seq: snapshot.snapshot_seq(),
        })
    }

    fn query_neighbors(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        node_id: &[u8],
        options: &NeighborQueryOptions,
    ) -> Result<NeighborPage, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_index_id(index_id)?;
        validate_key(node_id)?;
        if let Some(rel) = &options.rel_filter {
            validate_key(rel)?;
        }
        if options.limit == 0 {
            return Err(MembusError::InvalidArgument(
                "query_neighbors limit must be > 0".to_owned(),
            ));
        }

        let dirs: &[u8] = match options.direction {
            GraphDirection::Out => &[EDGE_DIR_FORWARD],
            GraphDirection::In => &[EDGE_DIR_REVERSE],
            GraphDirection::Both => &[EDGE_DIR_FORWARD, EDGE_DIR_REVERSE],
        };

        let (resume_dir, resume_suffix): (Option<u8>, Option<&[u8]>) =
            match options.after_cursor.as_deref() {
                Some(cur) if !cur.is_empty() => {
                    if !dirs.contains(&cur[0]) {
                        return Err(MembusError::InvalidArgument(format!(
                            "graph neighbor cursor direction tag 0x{:02x} is invalid for {:?}",
                            cur[0], options.direction
                        )));
                    }
                    (Some(cur[0]), Some(&cur[1..]))
                }
                Some(_) => {
                    return Err(MembusError::InvalidArgument(
                        "invalid empty graph neighbor cursor".to_owned(),
                    ));
                }
                None => (None, None),
            };

        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let ns_idx_prefix = encode_index_prefix(namespace, index_id);
        let mut edges = Vec::new();
        let mut last_cursor: Option<Vec<u8>> = None;
        let mut truncated = false;

        'dir_loop: for &dir_tag in dirs {
            if let Some(rd) = resume_dir {
                if rd == EDGE_DIR_REVERSE && dir_tag == EDGE_DIR_FORWARD {
                    continue;
                }
            }
            let seek_prefix = encode_edge_seek_prefix(
                namespace,
                index_id,
                dir_tag,
                node_id,
                options.rel_filter.as_deref(),
            );
            let dir_base_len = ns_idx_prefix.len() + 1;

            let seek_key = if resume_dir == Some(dir_tag) {
                let after = resume_suffix.unwrap_or(&[]);
                let next_after = next_key_after(after);
                let mut candidate = Vec::with_capacity(dir_base_len + next_after.len());
                candidate.extend_from_slice(&ns_idx_prefix);
                candidate.push(dir_tag);
                candidate.extend_from_slice(&next_after);
                if candidate.as_slice() > seek_prefix.as_slice() {
                    candidate
                } else {
                    seek_prefix.clone()
                }
            } else {
                seek_prefix.clone()
            };

            let mut read_opts = ReadOptions::default();
            read_opts.fill_cache(false);
            let iter = snapshot.inner().iterator_cf_opt(
                &idx_edge_cf,
                read_opts,
                IteratorMode::From(&seek_key, Direction::Forward),
            );

            for item in iter {
                let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
                if !raw_k.starts_with(&seek_prefix) {
                    break;
                }
                if edges.len() >= options.limit {
                    truncated = true;
                    break 'dir_loop;
                }
                let suffix = &raw_k[dir_base_len..];
                let (first, rel_label, second, _owner_pk) = decode_edge_suffix(suffix)?;
                let (src_id, dst_id, direction) = if dir_tag == EDGE_DIR_FORWARD {
                    (first, second, GraphDirection::Out)
                } else {
                    (second, first, GraphDirection::In)
                };
                let mut cursor = Vec::with_capacity(1 + suffix.len());
                cursor.push(dir_tag);
                cursor.extend_from_slice(suffix);
                last_cursor = Some(cursor);
                edges.push(GraphEdgeRecord {
                    src_id,
                    rel_label,
                    dst_id,
                    direction,
                    meta: raw_v.to_vec(),
                });
            }
        }

        Ok(NeighborPage {
            edges,
            next_cursor: if truncated { last_cursor } else { None },
            truncated,
            snapshot_seq: snapshot.snapshot_seq(),
        })
    }

    fn bfs_traverse(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        seed_node: &[u8],
        options: &BfsOptions,
    ) -> Result<BfsResult, MembusError> {
        self.verify_snapshot_owner(snapshot)?;
        validate_namespace(namespace)?;
        validate_index_id(index_id)?;
        validate_key(seed_node)?;
        validate_bfs_options(options)?;
        if let Some(rel) = &options.rel_filter {
            validate_key(rel)?;
        }

        let mut visited_set: HashSet<Vec<u8>> = HashSet::new();
        let mut visited_nodes: Vec<Vec<u8>> = Vec::new();
        let mut traversed_edges: Vec<GraphEdgeRecord> = Vec::new();
        let mut frontier_set: HashSet<Vec<u8>> = HashSet::new();
        let mut frontier_nodes: Vec<Vec<u8>> = Vec::new();
        let mut queue: VecDeque<(Vec<u8>, u8)> = VecDeque::new();
        let mut truncated = false;

        visited_set.insert(seed_node.to_vec());
        visited_nodes.push(seed_node.to_vec());

        if options.max_hops == 0 {
            frontier_nodes.push(seed_node.to_vec());
            return Ok(BfsResult {
                visited_nodes,
                traversed_edges,
                frontier_nodes,
                truncated: false,
                snapshot_seq: snapshot.snapshot_seq(),
            });
        }
        queue.push_back((seed_node.to_vec(), 0));

        let dirs: &[u8] = match options.direction {
            GraphDirection::Out => &[EDGE_DIR_FORWARD],
            GraphDirection::In => &[EDGE_DIR_REVERSE],
            GraphDirection::Both => &[EDGE_DIR_FORWARD, EDGE_DIR_REVERSE],
        };
        let idx_edge_cf = self.cf(CF_IDX_EDGE)?;
        let dir_base_len = encode_index_prefix(namespace, index_id).len() + 1;

        'bfs: while let Some((node, hop)) = queue.pop_front() {
            if hop >= options.max_hops {
                continue;
            }
            for &dir_tag in dirs {
                let seek_prefix = encode_edge_seek_prefix(
                    namespace,
                    index_id,
                    dir_tag,
                    &node,
                    options.rel_filter.as_deref(),
                );
                let mut read_opts = ReadOptions::default();
                read_opts.fill_cache(false);
                let iter = snapshot.inner().iterator_cf_opt(
                    &idx_edge_cf,
                    read_opts,
                    IteratorMode::From(&seek_prefix, Direction::Forward),
                );
                for item in iter {
                    let (raw_k, raw_v) = item.map_err(map_rocksdb_err)?;
                    if !raw_k.starts_with(&seek_prefix) {
                        break;
                    }
                    let (first, rel_label, second, _owner_pk) =
                        decode_edge_suffix(&raw_k[dir_base_len..])?;
                    let (src_id, dst_id, neighbor, direction) = if dir_tag == EDGE_DIR_FORWARD {
                        (first, second.clone(), second, GraphDirection::Out)
                    } else {
                        (second.clone(), first, second, GraphDirection::In)
                    };

                    if visited_set.contains(&neighbor) {
                        traversed_edges.push(GraphEdgeRecord {
                            src_id,
                            rel_label,
                            dst_id,
                            direction,
                            meta: raw_v.to_vec(),
                        });
                        continue;
                    }

                    if visited_set.len() >= options.max_nodes {
                        truncated = true;
                        if frontier_set.insert(node.clone()) {
                            frontier_nodes.push(node.clone());
                        }
                        while let Some((queued_node, _)) = queue.pop_front() {
                            if frontier_set.insert(queued_node.clone()) {
                                frontier_nodes.push(queued_node);
                            }
                        }
                        break 'bfs;
                    }

                    visited_set.insert(neighbor.clone());
                    visited_nodes.push(neighbor.clone());
                    traversed_edges.push(GraphEdgeRecord {
                        src_id,
                        rel_label,
                        dst_id,
                        direction,
                        meta: raw_v.to_vec(),
                    });
                    if hop + 1 < options.max_hops {
                        queue.push_back((neighbor, hop + 1));
                    } else if frontier_set.insert(neighbor.clone()) {
                        frontier_nodes.push(neighbor);
                    }
                }
            }
        }

        Ok(BfsResult {
            visited_nodes,
            traversed_edges,
            frontier_nodes,
            truncated,
            snapshot_seq: snapshot.snapshot_seq(),
        })
    }

    fn put_blob(&self, digest: &[u8], data: &[u8], ack: Ack) -> Result<(), MembusError> {
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

    fn get_blob(&self, digest: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        validate_key(digest)?;
        let key = encode_raw_blob_key(digest);
        let blob_cf = self.cf(CF_BLOB)?;
        self.db.get_cf(&blob_cf, &key).map_err(map_rocksdb_err)
    }

    fn put_cas_blob(
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

        // Idempotent put-if-absent verifying stored integrity if already present.
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

    fn get_cas_blob(
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

    fn has_cas_blob(&self, namespace: &str, sha256: &[u8; 32]) -> Result<bool, MembusError> {
        let key = encode_cas_blob_key(namespace, sha256)?;
        let blob_cf = self.cf(CF_BLOB)?;
        Ok(self
            .db
            .get_pinned_cf(&blob_cf, &key)
            .map_err(map_rocksdb_err)?
            .is_some())
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

    fn append_search_log_op(
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

    fn scan_search_log_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<SearchLogPage, MembusError> {
        if limit == 0 {
            return Err(MembusError::InvalidArgument(
                "scan_search_log_after limit must be > 0".to_owned(),
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

        let horizon_after = self.log_horizon.load(Ordering::Acquire);
        if after_seq < horizon_after {
            return Err(MembusError::Gap {
                earliest_available_seq: horizon_after.saturating_add(1),
                current_epoch: self.epoch,
            });
        }

        let scanned_through_seq = if hit_limit { last_seen_seq } else { upper_seq };
        Ok(SearchLogPage {
            records,
            scanned_through_seq,
        })
    }

    fn scan_deltas_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<DeltaLogPage, MembusError> {
        if limit == 0 {
            return Err(MembusError::InvalidArgument(
                "scan_deltas_after limit must be > 0".to_owned(),
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

        let horizon_after = self.log_horizon.load(Ordering::Acquire);
        if after_seq < horizon_after {
            return Err(MembusError::Gap {
                earliest_available_seq: horizon_after.saturating_add(1),
                current_epoch: self.epoch,
            });
        }

        let scanned_through_seq = if hit_limit { last_seen_seq } else { upper_seq };
        Ok(DeltaLogPage {
            entries,
            scanned_through_seq,
        })
    }

    fn meta_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        validate_key(key)?;
        let meta_cf = self.cf(CF_META)?;
        self.db.get_cf(&meta_cf, key).map_err(map_rocksdb_err)
    }

    fn meta_put(&self, key: &[u8], value: &[u8], sync: bool) -> Result<(), MembusError> {
        validate_key(key)?;
        validate_value(value, self.config.max_value_bytes)?;
        let meta_cf = self.cf(CF_META)?;
        let mut wo = WriteOptions::default();
        wo.set_sync(sync);
        self.db
            .put_cf_opt(&meta_cf, key, value, &wo)
            .map_err(map_rocksdb_err)
    }

    fn meta_scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, MembusError> {
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

    fn meta_cas_batch(
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

    fn lease_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        validate_key(key)?;
        let lease_cf = self.cf(CF_LEASE)?;
        self.db.get_cf(&lease_cf, key).map_err(map_rocksdb_err)
    }

    fn lease_cas(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> Result<(), MembusError> {
        validate_key(key)?;
        validate_value(new_value, self.config.max_value_bytes)?;
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

    fn trim_log_before(&self, keep_from_seq: u64) -> Result<(), MembusError> {
        let _trim_guard = self.trim_lock.lock();
        self.trim_log_before_locked(keep_from_seq)
    }

    fn list_log_watermarks(&self) -> Result<Vec<LogWatermark>, MembusError> {
        let mut watermarks = Vec::new();

        for (name, key) in [
            ("search:checkpoint", META_KEY_SEARCH_CHECKPOINT),
            ("backup:last_seq", META_KEY_BACKUP_LAST_SEQ),
            ("mesh:pub_seq", META_KEY_MESH_PUB_SEQ),
        ] {
            if let Some(raw) = self.meta_get(key)? {
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

        for (k, v) in self.meta_scan_prefix(META_PREFIX_MESH_PEER)? {
            let peer_name = String::from_utf8_lossy(&k).into_owned();
            let json: serde_json::Value = serde_json::from_slice(&v).map_err(|err| {
                MembusError::Corruption(format!(
                    "corrupt mesh peer watermark '{peer_name}': {err}"
                ))
            })?;
            // Only outbound acknowledgments (sequences pulled/acked by the peer from OUR log)
            // constrain trimming of OUR local `CF_LOG`.
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

    fn safe_trim_log_before(&self, keep_from_seq: u64, force: bool) -> Result<(), MembusError> {
        let _trim_guard = self.trim_lock.lock();
        if keep_from_seq <= 1 {
            return Ok(());
        }
        if !force {
            let new_horizon = keep_from_seq.saturating_sub(1);
            let watermarks = self.list_log_watermarks()?;
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

#[allow(clippy::too_many_arguments)]
fn apply_index_mutations_in_tx(
    tx: &Transaction<'_, OptimisticTransactionDB>,
    read_opts: &ReadOptions,
    idx_ord_cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
    idx_edge_cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
    idx_rev_cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
    namespace: &str,
    pk: &[u8],
    enc_pk: &[u8],
    new_indexes: Option<&IndexMutations>,
) -> Result<(), MembusError> {
    let prev_raw = tx
        .get_pinned_for_update_cf_opt(idx_rev_cf, enc_pk, true, read_opts)
        .map_err(map_tx_err)?;

    let had_prev = if let Some(raw) = prev_raw {
        let prev = ReversePostings::decode(raw.as_ref())?;
        for old_ord in &prev.ord_keys {
            tx.delete_cf(idx_ord_cf, old_ord).map_err(map_tx_err)?;
        }
        for old_edge in &prev.edge_keys {
            tx.delete_cf(idx_edge_cf, old_edge).map_err(map_tx_err)?;
        }
        true
    } else {
        false
    };

    if let Some(muts) = new_indexes.filter(|m| !m.is_empty()) {
        let mut new_rev = ReversePostings::default();
        let mut seen_ord: HashSet<Vec<u8>> = HashSet::with_capacity(muts.ordered.len());
        let mut seen_edge: HashSet<Vec<u8>> = HashSet::with_capacity(muts.edges.len() * 2);

        for ord in &muts.ordered {
            let k = encode_ordered_key(namespace, &ord.index_id, &ord.sort_key, pk);
            tx.put_cf(idx_ord_cf, &k, &ord.summary).map_err(map_tx_err)?;
            if seen_ord.insert(k.clone()) {
                new_rev.ord_keys.push(k);
            }
        }
        for edge in &muts.edges {
            let fwd = encode_edge_key(
                namespace,
                &edge.index_id,
                EDGE_DIR_FORWARD,
                &edge.src_id,
                &edge.rel_label,
                &edge.dst_id,
                pk,
            );
            let rev = encode_edge_key(
                namespace,
                &edge.index_id,
                EDGE_DIR_REVERSE,
                &edge.dst_id,
                &edge.rel_label,
                &edge.src_id,
                pk,
            );
            tx.put_cf(idx_edge_cf, &fwd, &edge.meta).map_err(map_tx_err)?;
            tx.put_cf(idx_edge_cf, &rev, &edge.meta).map_err(map_tx_err)?;
            if seen_edge.insert(fwd.clone()) {
                new_rev.edge_keys.push(fwd);
            }
            if seen_edge.insert(rev.clone()) {
                new_rev.edge_keys.push(rev);
            }
        }
        tx.put_cf(idx_rev_cf, enc_pk, new_rev.encode())
            .map_err(map_tx_err)?;
    } else if had_prev {
        tx.delete_cf(idx_rev_cf, enc_pk).map_err(map_tx_err)?;
    }

    Ok(())
}

fn estimate_index_bytes(muts: &IndexMutations) -> usize {
    let ord_bytes: usize = muts
        .ordered
        .iter()
        .map(|o| o.index_id.len() + o.sort_key.len() + o.summary.len() + 16)
        .sum();
    let edge_bytes: usize = muts
        .edges
        .iter()
        .map(|e| (e.index_id.len() + e.src_id.len() + e.rel_label.len() + e.dst_id.len() + e.meta.len() + 24) * 2)
        .sum();
    ord_bytes.saturating_add(edge_bytes)
}

fn estimate_search_op_bytes(op: &SearchLogOp) -> usize {
    match op {
        SearchLogOp::Index(doc) => {
            let tags_len: usize = doc.tags.iter().map(String::len).sum();
            doc.namespace
                .len()
                .saturating_add(doc.doc_id.len())
                .saturating_add(doc.kind.len())
                .saturating_add(doc.title.len())
                .saturating_add(tags_len)
                .saturating_add(doc.body.len())
                .saturating_add(64)
        }
        SearchLogOp::Delete { namespace, doc_id } => {
            namespace.len().saturating_add(doc_id.len()).saturating_add(32)
        }
        SearchLogOp::RevokePrefix { namespace, prefix } => {
            namespace.len().saturating_add(prefix.len()).saturating_add(32)
        }
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

fn read_last_log_seq_and_ts(
    db: &OptimisticTransactionDB,
    log_cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
) -> Result<(u64, u64), MembusError> {
    let mut iter = db.iterator_cf(log_cf, IteratorMode::End);
    match iter.next() {
        Some(Ok((key, val))) => {
            let seq = decode_log_seq(&key)?;
            let ts = decode_log_entry_delta(&val).map(|e| e.ts_ms).unwrap_or(0);
            Ok((seq, ts))
        }
        Some(Err(err)) => Err(map_rocksdb_err(err)),
        None => Ok((0, 0)),
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

fn encode_prefixed_key(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + suffix.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(suffix);
    out
}

fn decode_epoch_seq_meta(raw: &[u8]) -> Option<(u64, u64)> {
    if raw.len() != 16 {
        return None;
    }
    let epoch = u64::from_be_bytes(raw[0..8].try_into().ok()?);
    let seq = u64::from_be_bytes(raw[8..16].try_into().ok()?);
    Some((epoch, seq))
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
