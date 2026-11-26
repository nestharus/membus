use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rocksdb::{IteratorMode, Options, DB, DEFAULT_COLUMN_FAMILY_NAME};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::search::{SearchConfig, SearchEngine};
use crate::storage::codec::RECORD_FLAG_COLD_REF;
use crate::storage::{
    NoopPublisher, RocksDbStorage, StorageConfig, StorageEngine, ALL_COLUMN_FAMILIES, CF_BLOB,
    CF_KV, CF_LEASE, CF_LOG, CF_META,
};
use crate::types::{
    now_unix_ms, Ack, ColdRef, ExpectedVersion, IndexedMutation, LogDeltaEntry, MembusError,
    MutationOp, WriteMutation,
};

/// `CF_META` watermark key storing `(db_epoch_be8 || last_backed_up_seq_be8)`.
pub const BACKUP_META_KEY_LAST_SEQ: &[u8] = b"backup:last_seq";

/// Canonical manifest & delta bundle format version.
pub const BACKUP_FORMAT_VERSION: u32 = 1;

const DELTA_SCAN_PAGE_LIMIT: usize = 512;
const META_KEY_SEARCH_CHECKPOINT: &[u8] = b"search:checkpoint";

static BACKUP_TMP_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Checksum record for one file inside a RocksDB checkpoint or Tantivy search backup directory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChecksum {
    pub rel_path: String,
    pub size_bytes: u64,
    pub sha256_hex: String,
}

/// Logical entry counts recorded from the checkpoint at backup time.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupCounts {
    pub kv_entries: u64,
    pub log_entries: u64,
    pub blob_entries: u64,
    pub meta_entries: u64,
    pub lease_entries: u64,
}

/// Canonical manifest written beside every zero-downtime RocksDB checkpoint (+ optional Tantivy `search/`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub format_version: u32,
    pub db_epoch: u64,
    pub checkpoint_seq: u64,
    pub created_at_ms: u64,
    pub files: Vec<FileChecksum>,
    #[serde(default)]
    pub search_files: Vec<FileChecksum>,
    pub logical_counts: BackupCounts,
}

/// Tamper-evident incremental delta archive exported from `CF_LOG`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaBundle {
    pub format_version: u32,
    pub db_epoch: u64,
    pub from_seq: u64,
    pub through_seq: u64,
    pub created_at_ms: u64,
    pub entries: Vec<LogDeltaEntry>,
    pub bundle_sha256_hex: String,
}

/// Verification and replay summary produced by [`restore_clean_room`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreReport {
    pub db_epoch: u64,
    pub checkpoint_seq: u64,
    pub restored_seq: u64,
    pub files_verified: usize,
    pub deltas_replayed: usize,
    pub search_committed_seq: u64,
}

/// Create a zero-downtime online full backup inside `dest_dir`:
/// - `dest_dir/checkpoint/` holds the RocksDB `Checkpoint`
/// - `dest_dir/search/` holds the persisted Tantivy search index (if present beside the DB)
/// - `dest_dir/manifest.json` holds `db_epoch`, `checkpoint_seq`, per-file SHA-256 digests,
///   and exact logical CF counts.
///
/// Updates `backup:last_seq` in `CF_META` so safe log-trimming preserves unbacked log entries.
pub fn create_full_backup(
    storage: &dyn StorageEngine,
    dest_dir: &Path,
) -> Result<BackupManifest, MembusError> {
    create_full_backup_with_search(storage, None, dest_dir)
}

/// Create a zero-downtime online full backup, flushing an active [`SearchEngine`] first if provided
/// so `dest_dir/search/` and `CF_META["search:checkpoint"]` are mutually consistent.
pub fn create_full_backup_with_search(
    storage: &dyn StorageEngine,
    search: Option<&SearchEngine>,
    dest_dir: &Path,
) -> Result<BackupManifest, MembusError> {
    reject_symlink(dest_dir, "backup destination")?;
    if dest_dir.exists() {
        let mut entries = fs::read_dir(dest_dir)?;
        if entries.next().is_some() {
            return Err(MembusError::InvalidArgument(format!(
                "backup destination '{}' must be empty or non-existent",
                dest_dir.display()
            )));
        }
    } else {
        fs::create_dir_all(dest_dir)?;
    }

    if let Some(engine) = search {
        engine.flush_sync()?;
    }

    let checkpoint_dir = dest_dir.join("checkpoint");
    let checkpoint_seq = storage.create_checkpoint(&checkpoint_dir)?;
    let db_epoch = storage.epoch();

    // Count logical entries BEFORE hashing checkpoint files so manifest hashes cover final files.
    let logical_counts = count_checkpoint_entries(&checkpoint_dir)?;
    let files = collect_dir_checksums(&checkpoint_dir)?;

    let mut search_files = Vec::new();
    if let Some(data_dir) = storage.data_dir() {
        let src_search = data_dir.join("search");
        if src_search.exists() {
            reject_symlink(&src_search, "source search directory")?;
            if src_search.is_dir() {
                let dst_search = dest_dir.join("search");
                copy_search_dir(&src_search, &dst_search)?;
                search_files = collect_dir_checksums(&dst_search)?;
            }
        }
    }

    let manifest = BackupManifest {
        format_version: BACKUP_FORMAT_VERSION,
        db_epoch,
        checkpoint_seq,
        created_at_ms: now_unix_ms(),
        files,
        search_files,
        logical_counts,
    };

    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|err| MembusError::Storage(format!("serialize backup manifest failed: {err}")))?;
    write_file_atomic(&dest_dir.join("manifest.json"), &manifest_bytes)?;
    update_backup_watermark(storage, db_epoch, checkpoint_seq)?;

    Ok(manifest)
}

/// Export an ordered, SHA-256-sealed incremental delta bundle from `CF_LOG` strictly after `since_seq`.
///
/// Fails closed with [`MembusError::Gap`] if `CF_LOG` has been trimmed past `since_seq`.
pub fn export_incremental_delta(
    storage: &dyn StorageEngine,
    since_seq: u64,
    dest_file: &Path,
) -> Result<DeltaBundle, MembusError> {
    reject_symlink(dest_file, "delta destination")?;
    let mut entries = Vec::new();
    let mut cursor = since_seq;
    loop {
        let page = storage.scan_deltas_after(cursor, DELTA_SCAN_PAGE_LIMIT)?;
        entries.extend(page.entries);
        if page.scanned_through_seq <= cursor {
            break;
        }
        cursor = page.scanned_through_seq;
    }

    let db_epoch = storage.epoch();
    let created_at_ms = now_unix_ms();
    let bundle_sha256_hex =
        compute_delta_bundle_sha256(BACKUP_FORMAT_VERSION, db_epoch, since_seq, cursor, &entries)?;
    let bundle = DeltaBundle {
        format_version: BACKUP_FORMAT_VERSION,
        db_epoch,
        from_seq: since_seq,
        through_seq: cursor,
        created_at_ms,
        entries,
        bundle_sha256_hex,
    };

    if let Some(parent) = dest_file.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let encoded = serde_json::to_vec_pretty(&bundle)
        .map_err(|err| MembusError::Storage(format!("serialize delta bundle failed: {err}")))?;
    write_file_atomic(dest_file, &encoded)?;
    update_backup_watermark(storage, db_epoch, cursor)?;

    Ok(bundle)
}

/// Verify `backup_dir/manifest.json` and EVERY file inside `backup_dir/checkpoint/` (and
/// `backup_dir/search/` when listed) against its SHA-256.
///
/// Fails closed with [`MembusError::Corruption`] on any missing file, extra unexpected file,
/// path traversal attempt, symlink, size mismatch, or SHA-256 mismatch.
pub fn verify_backup_dir(backup_dir: &Path) -> Result<BackupManifest, MembusError> {
    reject_symlink(backup_dir, "backup directory")?;
    let manifest_path = backup_dir.join("manifest.json");
    reject_symlink(&manifest_path, "backup manifest")?;
    let raw = fs::read(&manifest_path).map_err(|err| {
        MembusError::Corruption(format!(
            "cannot read backup manifest '{}': {err}",
            manifest_path.display()
        ))
    })?;
    let manifest: BackupManifest = serde_json::from_slice(&raw).map_err(|err| {
        MembusError::Corruption(format!("corrupt backup manifest JSON: {err}"))
    })?;
    if manifest.format_version != BACKUP_FORMAT_VERSION {
        return Err(MembusError::Corruption(format!(
            "unsupported backup manifest format_version {}",
            manifest.format_version
        )));
    }

    let checkpoint_dir = backup_dir.join("checkpoint");
    reject_symlink(&checkpoint_dir, "backup checkpoint directory")?;
    if !checkpoint_dir.is_dir() {
        return Err(MembusError::Corruption(
            "backup checkpoint directory is missing".to_owned(),
        ));
    }
    verify_dir_matches_manifest(&checkpoint_dir, &manifest.files, "checkpoint")?;

    let search_dir = backup_dir.join("search");
    if !manifest.search_files.is_empty() {
        reject_symlink(&search_dir, "backup search directory")?;
        if !search_dir.is_dir() {
            return Err(MembusError::Corruption(
                "backup search directory is missing".to_owned(),
            ));
        }
        verify_dir_matches_manifest(&search_dir, &manifest.search_files, "search")?;
    } else if search_dir.exists() {
        reject_symlink(&search_dir, "backup search directory")?;
        let unexpected = collect_dir_checksums(&search_dir)?;
        if !unexpected.is_empty() {
            return Err(MembusError::Corruption(
                "unexpected search files in backup not listed in manifest".to_owned(),
            ));
        }
    }

    Ok(manifest)
}

fn verify_dir_matches_manifest(
    dir: &Path,
    expected_files: &[FileChecksum],
    label: &str,
) -> Result<(), MembusError> {
    let mut expected_map: BTreeMap<String, &FileChecksum> = BTreeMap::new();
    for entry in expected_files {
        validate_safe_rel_path(&entry.rel_path)?;
        if expected_map.insert(entry.rel_path.clone(), entry).is_some() {
            return Err(MembusError::Corruption(format!(
                "duplicate {label} file entry '{}' in backup manifest",
                entry.rel_path
            )));
        }
    }

    let actual_files = collect_dir_checksums(dir)?;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for actual in &actual_files {
        let Some(expected) = expected_map.get(&actual.rel_path) else {
            return Err(MembusError::Corruption(format!(
                "unexpected file '{}' in backup {label} not listed in manifest",
                actual.rel_path
            )));
        };
        if actual.size_bytes != expected.size_bytes {
            return Err(MembusError::Corruption(format!(
                "backup {label} file '{}' size mismatch: expected {} bytes, got {}",
                actual.rel_path, expected.size_bytes, actual.size_bytes
            )));
        }
        if !actual.sha256_hex.eq_ignore_ascii_case(&expected.sha256_hex) {
            return Err(MembusError::Corruption(format!(
                "backup {label} file '{}' SHA-256 mismatch: expected {}, got {}",
                actual.rel_path, expected.sha256_hex, actual.sha256_hex
            )));
        }
        seen.insert(actual.rel_path.clone());
    }

    if seen.len() != expected_map.len() {
        if let Some(missing) = expected_map.keys().find(|k| !seen.contains(*k)) {
            return Err(MembusError::Corruption(format!(
                "backup {label} missing manifest file '{missing}'"
            )));
        }
    }
    Ok(())
}

/// Load and SHA-256 verify an exported incremental [`DeltaBundle`].
pub fn verify_delta_bundle(path: &Path) -> Result<DeltaBundle, MembusError> {
    reject_symlink(path, "delta bundle")?;
    let raw = fs::read(path).map_err(|err| {
        MembusError::Corruption(format!(
            "cannot read delta bundle '{}': {err}",
            path.display()
        ))
    })?;
    let bundle: DeltaBundle = serde_json::from_slice(&raw)
        .map_err(|err| MembusError::Corruption(format!("corrupt delta bundle JSON: {err}")))?;
    if bundle.format_version != BACKUP_FORMAT_VERSION {
        return Err(MembusError::Corruption(format!(
            "unsupported delta bundle format_version {}",
            bundle.format_version
        )));
    }
    let expected_hash = compute_delta_bundle_sha256(
        bundle.format_version,
        bundle.db_epoch,
        bundle.from_seq,
        bundle.through_seq,
        &bundle.entries,
    )?;
    if !bundle
        .bundle_sha256_hex
        .eq_ignore_ascii_case(&expected_hash)
    {
        return Err(MembusError::Corruption(format!(
            "delta bundle '{}' SHA-256 mismatch: expected {}, got {}",
            path.display(),
            expected_hash,
            bundle.bundle_sha256_hex
        )));
    }
    Ok(bundle)
}

/// Clean-room restore from a verified backup directory plus zero or more ordered incremental
/// delta bundles into a fresh `target_dir`:
/// - Stages the entire restore (checkpoint copy, search copy, RocksDB open, epoch/gap checks,
///   strictly increasing delta replay after `manifest.checkpoint_seq`, and Tantivy catch-up)
///   inside a sibling temporary staging directory with automatic rollback cleanup on any error.
/// - Atomically promotes `staging -> target_dir` only after full success.
pub fn restore_clean_room(
    backup_dir: &Path,
    delta_bundle_paths: &[PathBuf],
    target_dir: &Path,
) -> Result<RestoreReport, MembusError> {
    reject_symlink(target_dir, "restore target")?;
    let target_preexisted_empty = if target_dir.exists() {
        if !target_dir.is_dir() {
            return Err(MembusError::InvalidArgument(format!(
                "restore target '{}' is not a directory",
                target_dir.display()
            )));
        }
        let mut entries = fs::read_dir(target_dir)?;
        if entries.next().is_some() {
            return Err(MembusError::InvalidArgument(format!(
                "restore target '{}' must be empty or non-existent",
                target_dir.display()
            )));
        }
        true
    } else {
        false
    };

    // 1. Verify full backup manifest + every checkpoint & search file SHA-256 upfront.
    let manifest = verify_backup_dir(backup_dir)?;

    // 2. Verify all delta bundles and strictly monotonic sequence ordering upfront.
    let mut bundles = Vec::with_capacity(delta_bundle_paths.len());
    for bundle_path in delta_bundle_paths {
        let bundle = verify_delta_bundle(bundle_path)?;
        if bundle.db_epoch != manifest.db_epoch {
            return Err(MembusError::Corruption(format!(
                "delta bundle epoch {} does not match backup epoch {}",
                bundle.db_epoch, manifest.db_epoch
            )));
        }
        let mut prev_entry_seq = 0u64;
        for entry in &bundle.entries {
            if entry.epoch != manifest.db_epoch {
                return Err(MembusError::Corruption(format!(
                    "delta entry seq {} epoch {} mismatches backup epoch {}",
                    entry.seq, entry.epoch, manifest.db_epoch
                )));
            }
            if entry.seq <= prev_entry_seq {
                return Err(MembusError::Corruption(format!(
                    "delta bundle entries are not strictly increasing: seq {} <= {}",
                    entry.seq, prev_entry_seq
                )));
            }
            prev_entry_seq = entry.seq;
        }
        bundles.push(bundle);
    }

    // 3. Create sibling staging directory with automatic drop-rollback guard.
    let staging_parent = target_dir
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(staging_parent)?;
    let target_name = target_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("restore");
    let seq_id = BACKUP_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let staging_dir = staging_parent.join(format!(
        ".{target_name}.restore-staging.{}.{seq_id}",
        std::process::id()
    ));
    let mut staging_guard = StagingDirGuard::create(staging_dir)?;

    // 4. Copy verified checkpoint files (and optional Tantivy search files) into staging.
    let checkpoint_dir = backup_dir.join("checkpoint");
    for file in &manifest.files {
        copy_verified_file(
            &checkpoint_dir.join(&file.rel_path),
            &staging_guard.path().join(&file.rel_path),
            &file.sha256_hex,
            &file.rel_path,
        )?;
    }
    if !manifest.search_files.is_empty() {
        let backup_search_dir = backup_dir.join("search");
        let staging_search_dir = staging_guard.path().join("search");
        for file in &manifest.search_files {
            copy_verified_file(
                &backup_search_dir.join(&file.rel_path),
                &staging_search_dir.join(&file.rel_path),
                &file.sha256_hex,
                &file.rel_path,
            )?;
        }
    }

    // 5. Open staged RocksDB and replay ALL delta entries strictly after `manifest.checkpoint_seq`
    //    (closing any concurrent checkpoint/delta hole safely).
    let (restored_seq, deltas_replayed, search_committed_seq) = {
        let storage_cfg = StorageConfig {
            bg_sync_interval: None,
            ..Default::default()
        };
        let storage = Arc::new(RocksDbStorage::open(
            staging_guard.path(),
            storage_cfg,
            Arc::new(NoopPublisher),
        )?);
        if storage.epoch() != manifest.db_epoch {
            return Err(MembusError::Corruption(format!(
                "restored db_epoch {} does not match manifest epoch {}",
                storage.epoch(),
                manifest.db_epoch
            )));
        }

        let mut covered_seq = manifest.checkpoint_seq;
        let mut last_replayed_origin_seq = manifest.checkpoint_seq;
        let mut deltas_replayed = 0usize;
        for bundle in &bundles {
            if bundle.from_seq > covered_seq {
                return Err(MembusError::Gap {
                    earliest_available_seq: bundle.from_seq.saturating_add(1),
                    current_epoch: manifest.db_epoch,
                });
            }
            for entry in &bundle.entries {
                if entry.seq <= manifest.checkpoint_seq {
                    continue;
                }
                if entry.seq <= last_replayed_origin_seq {
                    return Err(MembusError::Corruption(format!(
                        "out-of-order or duplicate delta seq {} <= {}",
                        entry.seq, last_replayed_origin_seq
                    )));
                }
                replay_delta_entry(storage.as_ref(), entry)?;
                last_replayed_origin_seq = entry.seq;
                deltas_replayed = deltas_replayed.saturating_add(1);
            }
            if bundle.through_seq > covered_seq {
                covered_seq = bundle.through_seq;
            }
        }

        storage.flush_wal(true)?;
        let restored_seq = storage.latest_seq();

        // 6. Catch up Tantivy search cleanly:
        //    - When `search/` was backed up, preserve `CF_META["search:checkpoint"]` so catch-up
        //      succeeds even when `log_horizon_seq > 0`!
        //    - When `search/` was absent: only reset `search:checkpoint` to 0 if `log_horizon_seq == 0`;
        //      otherwise fail closed with `Gap`.
        if manifest.search_files.is_empty() {
            if storage.log_horizon_seq() == 0 {
                let mut zero_cp = [0u8; 16];
                zero_cp[..8].copy_from_slice(&manifest.db_epoch.to_be_bytes());
                zero_cp[8..16].copy_from_slice(&0u64.to_be_bytes());
                storage.meta_put(META_KEY_SEARCH_CHECKPOINT, &zero_cp, true)?;
            } else {
                return Err(MembusError::Gap {
                    earliest_available_seq: storage.log_horizon_seq().saturating_add(1),
                    current_epoch: manifest.db_epoch,
                });
            }
        }

        let search = SearchEngine::open(
            staging_guard.path(),
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            SearchConfig::default(),
        )?;
        let search_committed_seq = search.flush_sync()?;
        drop(search);
        storage.flush_wal(true)?;
        drop(storage);
        (restored_seq, deltas_replayed, search_committed_seq)
    };

    // 7. Promote completed staging dir atomically into `target_dir`.
    if target_preexisted_empty {
        fs::remove_dir(target_dir)?;
    }
    fs::rename(staging_guard.path(), target_dir)?;
    staging_guard.disarm();

    Ok(RestoreReport {
        db_epoch: manifest.db_epoch,
        checkpoint_seq: manifest.checkpoint_seq,
        restored_seq,
        files_verified: manifest.files.len() + manifest.search_files.len(),
        deltas_replayed,
        search_committed_seq,
    })
}

fn replay_delta_entry(storage: &dyn StorageEngine, entry: &LogDeltaEntry) -> Result<(), MembusError> {
    if entry.mutations.is_empty() {
        for op in &entry.search_ops {
            storage.append_search_log_op(op.clone(), Ack::Flush)?;
        }
        return Ok(());
    }

    // Preserve ts_ms, LWW identity, secondary indexes, and mesh exclusion on tier swaps.
    if entry.mutations.len() == 1 && entry.mutations[0].is_tier_swap {
        let m = &entry.mutations[0];
        if let MutationOp::Put { value } = &m.op {
            let set_cold_flag = (m.flags & RECORD_FLAG_COLD_REF) != 0;
            let logical_sha256: [u8; 32] = if set_cold_flag {
                serde_json::from_slice::<ColdRef>(value)
                    .map_err(|err| {
                        MembusError::Corruption(format!(
                            "corrupt ColdRef payload in tier-swap delta: {err}"
                        ))
                    })?
                    .cold_sha256
            } else {
                Sha256::digest(value).into()
            };
            storage.tier_swap_record(
                &m.namespace,
                &m.key,
                value,
                set_cold_flag,
                logical_sha256,
                Ack::Flush,
                ExpectedVersion::Any,
            )?;
            if let Some(op) = &m.search_op {
                storage.append_search_log_op(op.clone(), Ack::Flush)?;
            }
            for op in &entry.search_ops {
                storage.append_search_log_op(op.clone(), Ack::Flush)?;
            }
            return Ok(());
        }
    }

    // Preserve secondary indexes and header flags for non-tier-swap flagged records.
    if entry.mutations.len() == 1
        && (entry.mutations[0].preserve_indexes || entry.mutations[0].flags != 0)
    {
        let m = &entry.mutations[0];
        if let MutationOp::Put { value } = &m.op {
            storage.put_with_flags(
                &m.namespace,
                &m.key,
                value,
                m.flags,
                m.preserve_indexes,
                Ack::Flush,
                ExpectedVersion::Any,
            )?;
            if let Some(op) = &m.search_op {
                storage.append_search_log_op(op.clone(), Ack::Flush)?;
            }
            for op in &entry.search_ops {
                storage.append_search_log_op(op.clone(), Ack::Flush)?;
            }
            return Ok(());
        }
    }

    let indexed: Vec<IndexedMutation> = entry
        .mutations
        .iter()
        .map(|m| IndexedMutation {
            mutation: WriteMutation {
                namespace: m.namespace.clone(),
                key: m.key.clone(),
                op: m.op.clone(),
            },
            indexes: m.indexes.clone(),
            search_op: m.search_op.clone(),
        })
        .collect();
    storage.commit_indexed_tx(&[], &indexed, Ack::Flush)?;

    for standalone_op in &entry.search_ops {
        storage.append_search_log_op(standalone_op.clone(), Ack::Flush)?;
    }
    Ok(())
}

struct StagingDirGuard {
    path: PathBuf,
    armed: bool,
}

impl StagingDirGuard {
    fn create(path: PathBuf) -> Result<Self, MembusError> {
        fs::create_dir_all(&path)?;
        Ok(Self { path, armed: true })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagingDirGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn copy_search_dir(src_root: &Path, dst_root: &Path) -> Result<(), MembusError> {
    fs::create_dir_all(dst_root)?;
    copy_search_recursive(src_root, dst_root)
}

fn copy_search_recursive(src: &Path, dst: &Path) -> Result<(), MembusError> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if ft.is_symlink() {
            return Err(MembusError::Corruption(format!(
                "refusing symlink '{}' inside search directory",
                entry.path().display()
            )));
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        // Skip live Tantivy writer/meta lockfiles; Tantivy recreates them cleanly on open.
        if name_str == ".tantivy-writer.lock" || name_str == ".tantivy-meta.lock" {
            continue;
        }
        let src_path = entry.path();
        let dst_path = dst.join(&name);
        if ft.is_dir() {
            fs::create_dir_all(&dst_path)?;
            copy_search_recursive(&src_path, &dst_path)?;
        } else if ft.is_file() {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

fn copy_verified_file(
    src: &Path,
    dst: &Path,
    expected_sha256_hex: &str,
    rel_label: &str,
) -> Result<(), MembusError> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(src, dst)?;
    let copied_hash = sha256_file_hex(dst)?.0;
    if !copied_hash.eq_ignore_ascii_case(expected_sha256_hex) {
        return Err(MembusError::Corruption(format!(
            "copied file '{rel_label}' failed post-copy SHA-256 check"
        )));
    }
    Ok(())
}

fn reject_symlink(path: &Path, label: &str) -> Result<(), MembusError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(MembusError::Corruption(format!(
            "refusing symlink at {label} '{}'",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(MembusError::Io(err.to_string())),
    }
}

fn update_backup_watermark(
    storage: &dyn StorageEngine,
    epoch: u64,
    seq: u64,
) -> Result<(), MembusError> {
    if let Some(prev) = storage.meta_get(BACKUP_META_KEY_LAST_SEQ)? {
        if prev.len() == 16 {
            let prev_ep = u64::from_be_bytes(prev[0..8].try_into().unwrap());
            let prev_seq = u64::from_be_bytes(prev[8..16].try_into().unwrap());
            if prev_ep == epoch && prev_seq >= seq {
                return Ok(());
            }
        }
    }
    let mut raw = [0u8; 16];
    raw[0..8].copy_from_slice(&epoch.to_be_bytes());
    raw[8..16].copy_from_slice(&seq.to_be_bytes());
    storage.meta_put(BACKUP_META_KEY_LAST_SEQ, &raw, true)
}

fn collect_dir_checksums(root: &Path) -> Result<Vec<FileChecksum>, MembusError> {
    let mut out = Vec::new();
    walk_checksums(root, root, &mut out)?;
    out.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(out)
}

fn walk_checksums(
    root: &Path,
    dir: &Path,
    out: &mut Vec<FileChecksum>,
) -> Result<(), MembusError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(MembusError::Corruption(format!(
                "refusing symlink '{}' inside backup directory",
                entry.path().display()
            )));
        }
        let path = entry.path();
        if file_type.is_dir() {
            walk_checksums(root, &path, out)?;
        } else if file_type.is_file() {
            let rel = path.strip_prefix(root).map_err(|err| {
                MembusError::Storage(format!("strip backup prefix failed: {err}"))
            })?;
            let rel_path = normalize_rel_path(rel)?;
            let (sha256_hex, size_bytes) = sha256_file_hex(&path)?;
            out.push(FileChecksum {
                rel_path,
                size_bytes,
                sha256_hex,
            });
        }
    }
    Ok(())
}

fn normalize_rel_path(rel: &Path) -> Result<String, MembusError> {
    let mut parts = Vec::new();
    for comp in rel.components() {
        match comp {
            Component::Normal(seg) => {
                let s = seg.to_str().ok_or_else(|| {
                    MembusError::Corruption("non-utf8 filename in backup directory".to_owned())
                })?;
                parts.push(s);
            }
            _ => {
                return Err(MembusError::Corruption(
                    "invalid path component in backup directory".to_owned(),
                ));
            }
        }
    }
    if parts.is_empty() {
        return Err(MembusError::Corruption(
            "empty relative path in backup directory".to_owned(),
        ));
    }
    Ok(parts.join("/"))
}

fn validate_safe_rel_path(rel_path: &str) -> Result<(), MembusError> {
    if rel_path.is_empty() || rel_path.starts_with('/') || rel_path.contains('\\') {
        return Err(MembusError::Corruption(format!(
            "unsafe relative path '{rel_path}' in backup manifest"
        )));
    }
    for comp in Path::new(rel_path).components() {
        if !matches!(comp, Component::Normal(_)) {
            return Err(MembusError::Corruption(format!(
                "path traversal component in backup manifest path '{rel_path}'"
            )));
        }
    }
    Ok(())
}

fn count_checkpoint_entries(checkpoint_dir: &Path) -> Result<BackupCounts, MembusError> {
    let opts = Options::default();
    let mut cf_names: Vec<&str> = Vec::with_capacity(ALL_COLUMN_FAMILIES.len() + 1);
    cf_names.push(DEFAULT_COLUMN_FAMILY_NAME);
    cf_names.extend_from_slice(&ALL_COLUMN_FAMILIES);
    let db = DB::open_cf_for_read_only(&opts, checkpoint_dir, cf_names, false)
        .map_err(|err| MembusError::Storage(format!("open checkpoint read-only failed: {err}")))?;

    Ok(BackupCounts {
        kv_entries: count_cf(&db, CF_KV)?,
        log_entries: count_cf(&db, CF_LOG)?,
        blob_entries: count_cf(&db, CF_BLOB)?,
        meta_entries: count_cf(&db, CF_META)?,
        lease_entries: count_cf(&db, CF_LEASE)?,
    })
}

fn count_cf(db: &DB, cf_name: &str) -> Result<u64, MembusError> {
    let cf = db
        .cf_handle(cf_name)
        .ok_or_else(|| MembusError::Storage(format!("checkpoint missing CF '{cf_name}'")))?;
    let mut count = 0u64;
    for item in db.iterator_cf(&cf, IteratorMode::Start) {
        let _ = item.map_err(|err| MembusError::Storage(err.into_string()))?;
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn compute_delta_bundle_sha256(
    format_version: u32,
    db_epoch: u64,
    from_seq: u64,
    through_seq: u64,
    entries: &[LogDeltaEntry],
) -> Result<String, MembusError> {
    let mut hasher = Sha256::new();
    hasher.update(format_version.to_be_bytes());
    hasher.update(db_epoch.to_be_bytes());
    hasher.update(from_seq.to_be_bytes());
    hasher.update(through_seq.to_be_bytes());
    let entries_json = serde_json::to_vec(entries)
        .map_err(|err| MembusError::Storage(format!("hash delta entries failed: {err}")))?;
    hasher.update(&entries_json);
    let digest: [u8; 32] = hasher.finalize().into();
    Ok(encode_hex(&digest))
}

fn sha256_file_hex(path: &Path) -> Result<(String, u64), MembusError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total = total.saturating_add(n as u64);
    }
    let digest: [u8; 32] = hasher.finalize().into();
    Ok((encode_hex(&digest), total))
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<(), MembusError> {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("membus");
    let seq = BACKUP_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = path.with_file_name(format!(
        ".{file_name}.tmp.{}.{seq}",
        std::process::id()
    ));
    {
        let mut f = File::create(&tmp_path)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    if let Err(err) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(MembusError::Io(err.to_string()));
    }
    Ok(())
}

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}
