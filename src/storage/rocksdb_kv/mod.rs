pub(crate) mod helpers;
mod index_apply;
mod log_meta;
mod query;
mod replication;
pub(crate) mod state;
mod tx;

use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex as FastMutex;
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor, DBCompressionType,
    Direction, IteratorMode, OptimisticTransactionDB, Options, ReadOptions, SliceTransform,
    WriteBufferManager, WriteOptions, DEFAULT_COLUMN_FAMILY_NAME,
};

use crate::index::{BfsOptions, NeighborQueryOptions, OrderedQueryOptions};
use crate::storage::admission::AdmissionGate;
use crate::storage::codec::{
    decode_log_entry_delta, decode_log_seq, kv_prefix_in_domain, kv_prefix_transform,
};
use crate::storage::constants::{
    ALL_COLUMN_FAMILIES, CF_BLOB, CF_IDX_EDGE, CF_IDX_ORD, CF_IDX_REV, CF_KV, CF_LEASE, CF_LOG,
    CF_META,
};
use crate::storage::contract::{MetaCasGuard, RawKeyValue, StorageConfig, StorageEngine};
use crate::storage::hlc::HlcClock;
use crate::storage::publisher::SignalPublisher;
use crate::storage::rocksdb_kv::helpers::{
    map_rocksdb_err, META_KEY_EPOCH, META_KEY_LOG_HORIZON, META_PREFIX_IDEM_ORD,
};
use crate::storage::rocksdb_kv::replication::TierSwapReq;
use crate::storage::rocksdb_kv::tx::SingleMutationReq;
use crate::storage::sequencer::PublishSequencer;
use crate::storage::snapshot::{OwnedSnapshot, SnapshotAdmissionGate};
use crate::storage::wal_sync::BackgroundWalSync;
use crate::types::{
    Ack, BfsResult, CommitResult, DeltaLogPage, ExpectedVersion, IdempotencyContext,
    IndexMutations, IndexedMutation, LeaseGuard, LogScanPage, LogWatermark, MembusError,
    MutationOp, NeighborPage, OrderedIndexPage, ReplicatedApplyResult, ReplicatedDelta,
    ReplicationStamp, ScanOptions, ScanPage, SearchLogOp, SearchLogPage, StoredRecord,
    VersionGuard, WalBatch, WriteMutation,
};

pub use state::RocksDbStorage;
use state::LEASE_LOCK_STRIPES;

static EPOCH_ENTROPY_COUNTER: AtomicU64 = AtomicU64::new(1);

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

        let descriptors = build_cf_descriptors(&config, &block_cache, &write_buffer_manager);
        let db = Arc::new(
            OptimisticTransactionDB::open_cf_descriptors(&db_opts, path, descriptors)
                .map_err(|err| MembusError::Storage(format!("rocksdb open failed: {err}")))?,
        );

        let (epoch, log_horizon_seq, last_log_seq, last_log_ts, initial_idem_count) =
            bootstrap_metadata(&db)?;
        let initial_seq = last_log_seq.max(log_horizon_seq);

        let bg_sync = match config.bg_sync_interval {
            Some(interval) if !interval.is_zero() => Some(BackgroundWalSync::spawn(&db, interval)?),
            _ => None,
        };

        Ok(Self {
            data_dir: path.to_path_buf(),
            db,
            admission: AdmissionGate::new(config.admission),
            snapshot_gate: SnapshotAdmissionGate::new(config.max_active_snapshots),
            config,
            epoch,
            log_horizon: AtomicU64::new(log_horizon_seq),
            trim_lock: FastMutex::new(()),
            idem_prune_lock: FastMutex::new(()),
            lease_stripes: (0..LEASE_LOCK_STRIPES).map(|_| FastMutex::new(())).collect(),
            boot_wall_ms: crate::types::now_unix_ms(),
            boot_instant: Instant::now(),
            idempotency_entry_count: AtomicUsize::new(initial_idem_count),
            hlc: HlcClock::new(last_log_ts),
            sequencer: Arc::new(PublishSequencer::new(initial_seq, publisher)),
            bg_sync,
        })
    }
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
    if config.max_active_snapshots == 0
        || config.max_scan_limit == 0
        || config.max_idempotency_entries == 0
    {
        return Err(MembusError::InvalidArgument(
            "max_active_snapshots, max_scan_limit, and max_idempotency_entries must be positive"
                .to_owned(),
        ));
    }
    Ok(())
}

fn build_cf_descriptors(
    config: &StorageConfig,
    block_cache: &Cache,
    write_buffer_manager: &WriteBufferManager,
) -> Vec<ColumnFamilyDescriptor> {
    let mut descriptors = Vec::with_capacity(ALL_COLUMN_FAMILIES.len() + 1);
    descriptors.push(ColumnFamilyDescriptor::new(
        DEFAULT_COLUMN_FAMILY_NAME,
        base_cf_options(config, block_cache, write_buffer_manager),
    ));
    descriptors.push(ColumnFamilyDescriptor::new(
        CF_KV,
        kv_cf_options(config, block_cache, write_buffer_manager),
    ));
    descriptors.push(ColumnFamilyDescriptor::new(
        CF_LOG,
        base_cf_options(config, block_cache, write_buffer_manager),
    ));
    descriptors.push(ColumnFamilyDescriptor::new(
        CF_BLOB,
        blob_cf_options(config, block_cache, write_buffer_manager),
    ));
    for reserved_cf in [CF_IDX_ORD, CF_IDX_EDGE, CF_IDX_REV, CF_LEASE, CF_META] {
        descriptors.push(ColumnFamilyDescriptor::new(
            reserved_cf,
            base_cf_options(config, block_cache, write_buffer_manager),
        ));
    }
    descriptors
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

fn bootstrap_metadata(
    db: &OptimisticTransactionDB,
) -> Result<(u64, u64, u64, u64, usize), MembusError> {
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

    let mut idem_count = 0usize;
    let mut idem_ro = ReadOptions::default();
    idem_ro.fill_cache(false);
    for item in db.iterator_cf_opt(
        &meta_cf,
        idem_ro,
        IteratorMode::From(META_PREFIX_IDEM_ORD, Direction::Forward),
    ) {
        let (k, _) = item.map_err(map_rocksdb_err)?;
        if !k.starts_with(META_PREFIX_IDEM_ORD) {
            break;
        }
        idem_count = idem_count.saturating_add(1);
    }

    let log_cf = db
        .cf_handle(CF_LOG)
        .ok_or_else(|| MembusError::Storage("missing log column family".to_owned()))?;
    let (last_log_seq, last_log_ts) = read_last_log_seq_and_ts(db, &log_cf)?;
    Ok((epoch, log_horizon_seq, last_log_seq, last_log_ts, idem_count))
}

fn read_last_log_seq_and_ts(
    db: &OptimisticTransactionDB,
    log_cf: &Arc<BoundColumnFamily<'_>>,
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

    fn active_snapshots(&self) -> usize {
        self.snapshot_gate.active()
    }

    fn hlc_now_ms(&self) -> u64 {
        self.hlc.current()
    }

    fn monotonic_now_ms(&self) -> u64 {
        self.monotonic_now_ms_impl()
    }

    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<StoredRecord>, MembusError> {
        self.get_impl(namespace, key)
    }

    fn get_many(
        &self,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.get_many_impl(namespace, keys)
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
        self.execute_single_mutation(SingleMutationReq {
            namespace,
            key,
            op: MutationOp::Put {
                value: value.to_vec(),
            },
            flags,
            preserve_indexes,
            ack,
            expected,
            indexes: None,
            search_op: None,
            idempotency: None,
        })
        .map(|(res, _)| res)
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
        self.tier_swap_record_impl(TierSwapReq {
            namespace,
            key,
            new_value,
            set_cold_flag,
            logical_value_sha256,
            ack,
            expected,
        })
    }

    fn apply_replicated_delta(
        &self,
        local_node_id: &str,
        delta: &ReplicatedDelta,
        ack: Ack,
    ) -> Result<ReplicatedApplyResult, MembusError> {
        self.apply_replicated_delta_impl(local_node_id, delta, ack)
    }

    fn append_replicated_search_ops(
        &self,
        origin_stamp: &ReplicationStamp,
        ops: &[SearchLogOp],
        ack: Ack,
    ) -> Result<ReplicatedApplyResult, MembusError> {
        self.append_replicated_search_ops_impl(origin_stamp, ops, ack)
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
        self.put_indexed_idempotent(namespace, key, value, ack, expected, indexes, search_op, None)
            .map(|(res, _)| res)
    }

    fn put_indexed_idempotent(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        indexes: Option<&IndexMutations>,
        search_op: Option<&SearchLogOp>,
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<(CommitResult, bool), MembusError> {
        self.execute_single_mutation(SingleMutationReq {
            namespace,
            key,
            op: MutationOp::Put {
                value: value.to_vec(),
            },
            flags: 0,
            preserve_indexes: false,
            ack,
            expected,
            indexes,
            search_op,
            idempotency,
        })
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
        self.remove_indexed_idempotent(namespace, key, ack, expected, search_op, None)
            .map(|(res, _)| res)
    }

    fn remove_indexed_idempotent(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        search_op: Option<&SearchLogOp>,
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<(CommitResult, bool), MembusError> {
        self.execute_single_mutation(SingleMutationReq {
            namespace,
            key,
            op: MutationOp::Remove,
            flags: 0,
            preserve_indexes: false,
            ack,
            expected,
            indexes: None,
            search_op,
            idempotency,
        })
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
        self.commit_indexed_tx_idempotent(guards, mutations, ack, None)
            .map(|(res, _)| res)
    }

    fn commit_indexed_tx_idempotent(
        &self,
        guards: &[VersionGuard],
        mutations: &[IndexedMutation],
        ack: Ack,
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<(CommitResult, bool), MembusError> {
        self.commit_indexed_tx_idempotent_impl(guards, mutations, ack, idempotency)
    }

    fn commit_indexed_tx_idempotent_with_leases(
        &self,
        guards: &[VersionGuard],
        lease_guards: &[LeaseGuard],
        mutations: &[IndexedMutation],
        ack: Ack,
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<(CommitResult, bool), MembusError> {
        self.commit_indexed_tx_idempotent_with_leases_impl(
            guards,
            lease_guards,
            mutations,
            ack,
            idempotency,
        )
    }

    fn open_snapshot(&self) -> Result<OwnedSnapshot, MembusError> {
        self.open_snapshot_impl()
    }

    fn get_in_snapshot(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        self.get_in_snapshot_impl(snapshot, namespace, key)
    }

    fn get_many_in_snapshot(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.get_many_in_snapshot_impl(snapshot, namespace, keys)
    }

    fn scan_prefix(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.scan_prefix_impl(snapshot, namespace, prefix, options)
    }

    fn scan_range(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.scan_range_impl(snapshot, namespace, start_key, end_key, options)
    }

    fn query_ordered_index(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        options: &OrderedQueryOptions,
    ) -> Result<OrderedIndexPage, MembusError> {
        self.query_ordered_index_impl(snapshot, namespace, index_id, options)
    }

    fn query_neighbors(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        node_id: &[u8],
        options: &NeighborQueryOptions,
    ) -> Result<NeighborPage, MembusError> {
        self.query_neighbors_impl(snapshot, namespace, index_id, node_id, options)
    }

    fn bfs_traverse(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        seed_node: &[u8],
        options: &BfsOptions,
    ) -> Result<BfsResult, MembusError> {
        self.bfs_traverse_impl(snapshot, namespace, index_id, seed_node, options)
    }

    fn put_blob(&self, digest: &[u8], data: &[u8], ack: Ack) -> Result<(), MembusError> {
        self.put_blob_impl(digest, data, ack)
    }

    fn get_blob(&self, digest: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        self.get_blob_impl(digest)
    }

    fn put_cas_blob(
        &self,
        namespace: &str,
        data: &[u8],
        ack: Ack,
    ) -> Result<[u8; 32], MembusError> {
        self.put_cas_blob_impl(namespace, data, ack)
    }

    fn get_cas_blob(
        &self,
        namespace: &str,
        sha256: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, MembusError> {
        self.get_cas_blob_impl(namespace, sha256)
    }

    fn has_cas_blob(&self, namespace: &str, sha256: &[u8; 32]) -> Result<bool, MembusError> {
        self.has_cas_blob_impl(namespace, sha256)
    }

    fn scan_log_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<LogScanPage, MembusError> {
        self.scan_log_after_impl(after_seq, limit)
    }

    fn append_search_log_op(
        &self,
        op: SearchLogOp,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.append_search_log_op_impl(op, ack)
    }

    fn scan_search_log_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<SearchLogPage, MembusError> {
        self.scan_search_log_after_impl(after_seq, limit)
    }

    fn scan_deltas_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<DeltaLogPage, MembusError> {
        self.scan_deltas_after_impl(after_seq, limit)
    }

    fn trim_log_before(&self, keep_from_seq: u64) -> Result<(), MembusError> {
        self.trim_log_before_impl(keep_from_seq)
    }

    fn list_log_watermarks(&self) -> Result<Vec<LogWatermark>, MembusError> {
        self.list_log_watermarks_impl()
    }

    fn safe_trim_log_before(&self, keep_from_seq: u64, force: bool) -> Result<(), MembusError> {
        self.safe_trim_log_before_impl(keep_from_seq, force)
    }

    fn meta_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        self.meta_get_impl(key)
    }

    fn meta_put(&self, key: &[u8], value: &[u8], sync: bool) -> Result<(), MembusError> {
        self.meta_put_impl(key, value, sync)
    }

    fn meta_scan_prefix(&self, prefix: &[u8]) -> Result<Vec<RawKeyValue>, MembusError> {
        self.meta_scan_prefix_impl(prefix)
    }

    fn meta_cas_batch(
        &self,
        guards: &[MetaCasGuard],
        puts: &[(Vec<u8>, Vec<u8>)],
        deletes: &[Vec<u8>],
        sync: bool,
    ) -> Result<(), MembusError> {
        self.meta_cas_batch_impl(guards, puts, deletes, sync)
    }

    fn lease_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        self.lease_get_impl(key)
    }

    fn lease_cas(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> Result<(), MembusError> {
        self.lease_cas_impl(key, expected, new_value)
    }

    fn create_checkpoint(&self, dest_dir: &Path) -> Result<u64, MembusError> {
        self.create_checkpoint_impl(dest_dir)
    }

    fn read_wal_since(&self, rocksdb_seq: u64, limit: usize) -> Result<Vec<WalBatch>, MembusError> {
        self.read_wal_since_impl(rocksdb_seq, limit)
    }

    fn flush_wal(&self, sync: bool) -> Result<(), MembusError> {
        self.flush_wal_impl(sync)
    }
}
