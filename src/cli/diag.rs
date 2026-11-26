use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rocksdb::{IteratorMode, Options, DB, DEFAULT_COLUMN_FAMILY_NAME};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::backup::{restore_clean_room, RestoreReport, BACKUP_META_KEY_LAST_SEQ};
use crate::index::ReversePostings;
use crate::mesh::{MeshPeerWatermark, META_KEY_MESH_PUB_SEQ, META_PREFIX_MESH_PEER};
use crate::storage::codec::{
    decode_kv_key, decode_log_entry_delta, decode_log_seq, decode_record_parts, BLOB_DOMAIN_CAS,
    RECORD_FLAG_COLD_REF,
};
use crate::storage::{
    ALL_COLUMN_FAMILIES, CF_BLOB, CF_IDX_EDGE, CF_IDX_ORD, CF_IDX_REV, CF_KV, CF_LEASE, CF_LOG,
    CF_META,
};
use crate::types::{
    now_unix_ms, ColdRef, CollectionDescriptor, LeaseInfo, MembusError, MigrationState,
    StoreProfile,
};

const META_KEY_EPOCH: &[u8] = b"db_epoch";
const META_KEY_LOG_HORIZON: &[u8] = b"log_horizon_seq";
const META_KEY_SEARCH_CHECKPOINT: &[u8] = b"search:checkpoint";
const CAT_CURRENT_PREFIX: &[u8] = b"cat:cur:";

static SECONDARY_DIR_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Per-column-family key/byte statistics produced by `membus diag inspect`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CfStat {
    pub name: String,
    pub entry_count: u64,
    pub total_key_bytes: u64,
    pub total_value_bytes: u64,
}

/// Summary of one registered collection in `membus diag inspect`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionSummary {
    pub namespace: String,
    pub store_profile: StoreProfile,
    pub schema_version: u32,
    pub migration_state: MigrationState,
}

/// Read-only / secondary break-glass inspection report (never includes secret lease tokens).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagInspectReport {
    pub db_epoch: u64,
    pub latest_seq: u64,
    pub log_horizon_seq: u64,
    pub cf_stats: Vec<CfStat>,
    pub active_leases: Vec<LeaseInfo>,
    pub collections: Vec<CollectionSummary>,
    pub search_checkpoint_seq: Option<u64>,
    pub search_checkpoint_lag: u64,
    pub backup_last_seq: Option<u64>,
    pub mesh_pub_seq: Option<u64>,
    pub mesh_peers: Vec<MeshPeerWatermark>,
}

/// Offline read-only deep verification report produced by `membus diag verify`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagVerifyReport {
    pub ok: bool,
    pub db_epoch: u64,
    pub log_horizon_seq: u64,
    pub kv_records_checked: u64,
    pub cas_blobs_checked: u64,
    pub log_entries_checked: u64,
    pub idx_rev_keys_checked: u64,
    pub idx_ord_checked: u64,
    pub idx_edge_checked: u64,
    pub errors: Vec<String>,
}

/// Parsed `membus diag ...` subcommand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiagCommand {
    Inspect {
        data_dir: PathBuf,
    },
    Verify {
        data_dir: PathBuf,
    },
    Restore {
        backup_dir: PathBuf,
        target_dir: PathBuf,
        deltas: Vec<PathBuf>,
    },
}

/// Parse arguments after `membus diag`.
pub fn parse_diag_args(args: &[String]) -> Result<DiagCommand, MembusError> {
    if args.is_empty() {
        return Err(MembusError::InvalidArgument(
            "diag requires a subcommand: inspect, verify, or restore".to_owned(),
        ));
    }
    match args[0].as_str() {
        "inspect" => {
            let data_dir = parse_single_data_dir(&args[1..], "diag inspect")?;
            Ok(DiagCommand::Inspect { data_dir })
        }
        "verify" => {
            let data_dir = parse_single_data_dir(&args[1..], "diag verify")?;
            Ok(DiagCommand::Verify { data_dir })
        }
        "restore" => parse_diag_restore(&args[1..]),
        other => Err(MembusError::InvalidArgument(format!(
            "unknown diag subcommand '{other}' (expected inspect, verify, or restore)"
        ))),
    }
}

/// Execute a parsed `membus diag` subcommand and print canonical JSON output.
pub fn run_diag(cmd: DiagCommand) -> Result<(), MembusError> {
    match cmd {
        DiagCommand::Inspect { data_dir } => {
            let report = inspect_data_dir(&data_dir)?;
            let out = serde_json::to_string_pretty(&report)
                .map_err(|err| MembusError::Storage(format!("serialize inspect report: {err}")))?;
            println!("{out}");
            Ok(())
        }
        DiagCommand::Verify { data_dir } => {
            let report = verify_data_dir(&data_dir)?;
            let out = serde_json::to_string_pretty(&report)
                .map_err(|err| MembusError::Storage(format!("serialize verify report: {err}")))?;
            println!("{out}");
            if !report.ok {
                return Err(MembusError::Corruption(format!(
                    "diagnostic verification failed with {} error(s)",
                    report.errors.len()
                )));
            }
            Ok(())
        }
        DiagCommand::Restore {
            backup_dir,
            target_dir,
            deltas,
        } => {
            let report: RestoreReport = restore_clean_room(&backup_dir, &deltas, &target_dir)?;
            let out = serde_json::to_string_pretty(&report)
                .map_err(|err| MembusError::Storage(format!("serialize restore report: {err}")))?;
            println!("{out}");
            Ok(())
        }
    }
}

/// Open RocksDB in secondary / read-only mode (NEVER taking the primary write lock)
/// so `membus diag inspect` works against BOTH closed and live-open primary databases.
pub fn inspect_data_dir(data_dir: &Path) -> Result<DiagInspectReport, MembusError> {
    let reader = SecondaryDbHandle::open(data_dir)?;
    let db = reader.db();

    let meta_cf = cf(db, CF_META)?;
    let db_epoch = read_u64_meta(db, &meta_cf, META_KEY_EPOCH)?.unwrap_or(0);
    let log_horizon_seq = read_u64_meta(db, &meta_cf, META_KEY_LOG_HORIZON)?.unwrap_or(0);

    let log_cf = cf(db, CF_LOG)?;
    let last_log_seq = match db.iterator_cf(&log_cf, IteratorMode::End).next() {
        Some(Ok((k, _))) => decode_log_seq(&k)?,
        Some(Err(err)) => return Err(MembusError::Storage(err.into_string())),
        None => 0,
    };
    let latest_seq = last_log_seq.max(log_horizon_seq);

    let mut cf_stats = Vec::with_capacity(ALL_COLUMN_FAMILIES.len());
    for cf_name in ALL_COLUMN_FAMILIES {
        let handle = cf(db, cf_name)?;
        let mut entry_count = 0u64;
        let mut total_key_bytes = 0u64;
        let mut total_value_bytes = 0u64;
        for item in db.iterator_cf(&handle, IteratorMode::Start) {
            let (k, v) = item.map_err(|err| MembusError::Storage(err.into_string()))?;
            entry_count = entry_count.saturating_add(1);
            total_key_bytes = total_key_bytes.saturating_add(k.len() as u64);
            total_value_bytes = total_value_bytes.saturating_add(v.len() as u64);
        }
        cf_stats.push(CfStat {
            name: cf_name.to_owned(),
            entry_count,
            total_key_bytes,
            total_value_bytes,
        });
    }

    // Active leases stripped strictly to `LeaseInfo` (zero secret `token` exposure!).
    let now_ms = now_unix_ms();
    let lease_cf = cf(db, CF_LEASE)?;
    let mut active_leases = Vec::new();
    for item in db.iterator_cf(&lease_cf, IteratorMode::Start) {
        let (k, v) = item.map_err(|err| MembusError::Storage(err.into_string()))?;
        let Ok((ns, resource_bytes)) = decode_kv_key(&k) else {
            continue;
        };
        let Ok(raw_rec) = serde_json::from_slice::<RawLeaseRecord>(&v) else {
            continue;
        };
        if !raw_rec.released && raw_rec.expires_at_ms > now_ms {
            active_leases.push(LeaseInfo {
                namespace: ns.to_owned(),
                resource: String::from_utf8_lossy(resource_bytes).into_owned(),
                holder: raw_rec.holder,
                generation: raw_rec.generation,
                expires_at_ms: raw_rec.expires_at_ms,
            });
        }
    }

    // Collections, search checkpoint lag, backup watermark, and mesh peer watermarks.
    let mut collections = Vec::new();
    let mut mesh_peers = Vec::new();
    for item in db.iterator_cf(&meta_cf, IteratorMode::Start) {
        let (k, v) = item.map_err(|err| MembusError::Storage(err.into_string()))?;
        if k.starts_with(CAT_CURRENT_PREFIX) {
            if let Ok(desc) = serde_json::from_slice::<CollectionDescriptor>(&v) {
                collections.push(CollectionSummary {
                    namespace: desc.namespace,
                    store_profile: desc.store_profile,
                    schema_version: desc.schema_version,
                    migration_state: desc.migration_state,
                });
            }
        } else if k.starts_with(META_PREFIX_MESH_PEER) {
            if let Ok(wm) = serde_json::from_slice::<MeshPeerWatermark>(&v) {
                mesh_peers.push(wm);
            }
        }
    }

    let search_checkpoint_seq = read_epoch_seq_meta(db, &meta_cf, META_KEY_SEARCH_CHECKPOINT)
        .filter(|(ep, _)| *ep == db_epoch)
        .map(|(_, seq)| seq);
    let search_checkpoint_lag = latest_seq.saturating_sub(search_checkpoint_seq.unwrap_or(0));

    let backup_last_seq = read_epoch_seq_meta(db, &meta_cf, BACKUP_META_KEY_LAST_SEQ)
        .filter(|(ep, _)| *ep == db_epoch)
        .map(|(_, seq)| seq);
    let mesh_pub_seq = read_epoch_seq_meta(db, &meta_cf, META_KEY_MESH_PUB_SEQ)
        .filter(|(ep, _)| *ep == db_epoch)
        .map(|(_, seq)| seq);

    Ok(DiagInspectReport {
        db_epoch,
        latest_seq,
        log_horizon_seq,
        cf_stats,
        active_leases,
        collections,
        search_checkpoint_seq,
        search_checkpoint_lag,
        backup_last_seq,
        mesh_pub_seq,
        mesh_peers,
    })
}

/// Offline read-only deep integrity verification across `CF_KV`, `CF_BLOB` (SHA-256),
/// `CF_LOG` (CRC32 & sequence monotonicity), and `CF_IDX_REV` <-> `CF_IDX_ORD`/`CF_IDX_EDGE`.
pub fn verify_data_dir(data_dir: &Path) -> Result<DiagVerifyReport, MembusError> {
    let reader = SecondaryDbHandle::open(data_dir)?;
    let db = reader.db();
    let meta_cf = cf(db, CF_META)?;
    let db_epoch = read_u64_meta(db, &meta_cf, META_KEY_EPOCH)?.unwrap_or(0);
    let log_horizon_seq = read_u64_meta(db, &meta_cf, META_KEY_LOG_HORIZON)?.unwrap_or(0);

    let mut errors = Vec::new();

    // 1. Verify CF_KV keys, record CRC32 headers, and ColdRef JSON on cold-flagged rows.
    let kv_cf = cf(db, CF_KV)?;
    let mut kv_keys: HashSet<Vec<u8>> = HashSet::new();
    let mut kv_records_checked = 0u64;
    for item in db.iterator_cf(&kv_cf, IteratorMode::Start) {
        let (k, v) = item.map_err(|err| MembusError::Storage(err.into_string()))?;
        kv_records_checked = kv_records_checked.saturating_add(1);
        kv_keys.insert(k.to_vec());
        if let Err(err) = decode_kv_key(&k) {
            errors.push(format!("kv key decode error: {err}"));
        }
        match decode_record_parts(&v) {
            Ok((meta, val)) => {
                if (meta.flags & RECORD_FLAG_COLD_REF) != 0 {
                    if let Err(err) = serde_json::from_slice::<ColdRef>(val) {
                        errors.push(format!("cold-flagged kv record has corrupt ColdRef JSON: {err}"));
                    }
                }
            }
            Err(err) => errors.push(format!("kv record CRC/header error: {err}")),
        }
    }

    // 2. Verify every namespace-scoped CAS blob's SHA-256 in CF_BLOB.
    let blob_cf = cf(db, CF_BLOB)?;
    let mut cas_blobs_checked = 0u64;
    for item in db.iterator_cf(&blob_cf, IteratorMode::Start) {
        let (k, v) = item.map_err(|err| MembusError::Storage(err.into_string()))?;
        if k.first() == Some(&BLOB_DOMAIN_CAS) {
            cas_blobs_checked = cas_blobs_checked.saturating_add(1);
            if k.len() < 1 + 4 + 32 {
                errors.push(format!("cas blob key truncated ({} bytes)", k.len()));
                continue;
            }
            let ns_len = u32::from_be_bytes([k[1], k[2], k[3], k[4]]) as usize;
            let expected_len = 1 + 4 + ns_len + 32;
            if k.len() != expected_len {
                errors.push(format!(
                    "cas blob key length mismatch: expected {expected_len}, got {}",
                    k.len()
                ));
                continue;
            }
            let expected_sha = &k[1 + 4 + ns_len..];
            let actual_sha: [u8; 32] = Sha256::digest(&v).into();
            if actual_sha.as_slice() != expected_sha {
                errors.push("cas blob SHA-256 mismatch detected".to_owned());
            }
        }
    }

    // 3. Verify CF_LOG CRC32, sequence key matching, epoch matching, and monotonic ordering.
    let log_cf = cf(db, CF_LOG)?;
    let mut log_entries_checked = 0u64;
    let mut prev_seq = 0u64;
    for item in db.iterator_cf(&log_cf, IteratorMode::Start) {
        let (k, v) = item.map_err(|err| MembusError::Storage(err.into_string()))?;
        log_entries_checked = log_entries_checked.saturating_add(1);
        let key_seq = match decode_log_seq(&k) {
            Ok(s) => s,
            Err(err) => {
                errors.push(format!("log key decode error: {err}"));
                continue;
            }
        };
        if key_seq <= prev_seq {
            errors.push(format!(
                "non-monotonic log sequence: {key_seq} <= {prev_seq}"
            ));
        }
        prev_seq = key_seq;
        match decode_log_entry_delta(&v) {
            Ok(entry) => {
                if entry.seq != key_seq {
                    errors.push(format!(
                        "log entry seq mismatch: key={key_seq}, body={}",
                        entry.seq
                    ));
                }
                if db_epoch != 0 && entry.epoch != db_epoch {
                    errors.push(format!(
                        "log entry seq {key_seq} epoch {} mismatches db_epoch {db_epoch}",
                        entry.epoch
                    ));
                }
            }
            Err(err) => errors.push(format!("log entry seq {key_seq} decode/CRC error: {err}")),
        }
    }

    // 4. Bidirectional CF_IDX_REV <-> CF_IDX_ORD / CF_IDX_EDGE consistency check.
    let rev_cf = cf(db, CF_IDX_REV)?;
    let ord_cf = cf(db, CF_IDX_ORD)?;
    let edge_cf = cf(db, CF_IDX_EDGE)?;

    let mut expected_ord: HashSet<Vec<u8>> = HashSet::new();
    let mut expected_edge: HashSet<Vec<u8>> = HashSet::new();
    let mut idx_rev_keys_checked = 0u64;

    for item in db.iterator_cf(&rev_cf, IteratorMode::Start) {
        let (k, v) = item.map_err(|err| MembusError::Storage(err.into_string()))?;
        idx_rev_keys_checked = idx_rev_keys_checked.saturating_add(1);
        if !kv_keys.contains(k.as_ref()) {
            errors.push("orphan idx_rev entry whose primary key is missing from kv".to_owned());
        }
        match ReversePostings::decode(&v) {
            Ok(postings) => {
                for ord_k in postings.ord_keys {
                    expected_ord.insert(ord_k);
                }
                for edge_k in postings.edge_keys {
                    expected_edge.insert(edge_k);
                }
            }
            Err(err) => errors.push(format!("idx_rev decode error: {err}")),
        }
    }

    let mut actual_ord: HashSet<Vec<u8>> = HashSet::new();
    let mut idx_ord_checked = 0u64;
    for item in db.iterator_cf(&ord_cf, IteratorMode::Start) {
        let (k, _v) = item.map_err(|err| MembusError::Storage(err.into_string()))?;
        idx_ord_checked = idx_ord_checked.saturating_add(1);
        let key_vec = k.to_vec();
        if !expected_ord.contains(&key_vec) {
            errors.push("orphan idx_ord posting not referenced by idx_rev".to_owned());
        }
        actual_ord.insert(key_vec);
    }
    for missing_ord in expected_ord.difference(&actual_ord) {
        errors.push(format!(
            "idx_rev references missing idx_ord key ({} bytes)",
            missing_ord.len()
        ));
    }

    let mut actual_edge: HashSet<Vec<u8>> = HashSet::new();
    let mut idx_edge_checked = 0u64;
    for item in db.iterator_cf(&edge_cf, IteratorMode::Start) {
        let (k, _v) = item.map_err(|err| MembusError::Storage(err.into_string()))?;
        idx_edge_checked = idx_edge_checked.saturating_add(1);
        let key_vec = k.to_vec();
        if !expected_edge.contains(&key_vec) {
            errors.push("orphan idx_edge posting not referenced by idx_rev".to_owned());
        }
        actual_edge.insert(key_vec);
    }
    for missing_edge in expected_edge.difference(&actual_edge) {
        errors.push(format!(
            "idx_rev references missing idx_edge key ({} bytes)",
            missing_edge.len()
        ));
    }

    if db_epoch == 0
        && (kv_records_checked > 0
            || cas_blobs_checked > 0
            || log_entries_checked > 0
            || idx_rev_keys_checked > 0
            || idx_ord_checked > 0
            || idx_edge_checked > 0)
    {
        errors.push("missing or zero db_epoch on non-empty database".to_owned());
    }

    Ok(DiagVerifyReport {
        ok: errors.is_empty(),
        db_epoch,
        log_horizon_seq,
        kv_records_checked,
        cas_blobs_checked,
        log_entries_checked,
        idx_rev_keys_checked,
        idx_ord_checked,
        idx_edge_checked,
        errors,
    })
}

/// Owned secondary RocksDB reader that never takes the primary write lock and removes
/// its ephemeral secondary metadata directory strictly AFTER closing RocksDB handles.
struct SecondaryDbHandle {
    db: Option<DB>,
    secondary_dir: PathBuf,
}

impl SecondaryDbHandle {
    fn db(&self) -> &DB {
        self.db.as_ref().expect("SecondaryDbHandle db active until drop")
    }

    fn open(primary_dir: &Path) -> Result<Self, MembusError> {
        if !primary_dir.is_dir() {
            return Err(MembusError::NotFound(format!(
                "data directory '{}' does not exist",
                primary_dir.display()
            )));
        }
        let seq = SECONDARY_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let secondary_dir = std::env::temp_dir().join(format!(
            "membus-diag-secondary-{}-{}-{seq}",
            std::process::id(),
            now_unix_ms()
        ));
        fs::create_dir_all(&secondary_dir)?;

        let mut opts = Options::default();
        opts.set_max_open_files(-1);
        let mut cf_names: Vec<&str> = Vec::with_capacity(ALL_COLUMN_FAMILIES.len() + 1);
        cf_names.push(DEFAULT_COLUMN_FAMILY_NAME);
        cf_names.extend_from_slice(&ALL_COLUMN_FAMILIES);

        let db = match DB::open_cf_as_secondary(&opts, primary_dir, &secondary_dir, cf_names) {
            Ok(db) => {
                if let Err(err) = db.try_catch_up_with_primary() {
                    drop(db);
                    let _ = fs::remove_dir_all(&secondary_dir);
                    return Err(MembusError::Storage(format!(
                        "rocksdb secondary catch-up with primary failed: {err}"
                    )));
                }
                db
            }
            Err(err) => {
                let _ = fs::remove_dir_all(&secondary_dir);
                return Err(MembusError::Storage(format!(
                    "open rocksdb secondary read-only failed: {err}"
                )));
            }
        };

        Ok(Self {
            db: Some(db),
            secondary_dir,
        })
    }
}

impl Drop for SecondaryDbHandle {
    fn drop(&mut self) {
        drop(self.db.take());
        let _ = fs::remove_dir_all(&self.secondary_dir);
    }
}

#[derive(Deserialize)]
struct RawLeaseRecord {
    holder: String,
    generation: u64,
    #[allow(dead_code)]
    token: String,
    expires_at_ms: u64,
    released: bool,
}

fn cf<'a>(db: &'a DB, name: &str) -> Result<std::sync::Arc<rocksdb::BoundColumnFamily<'a>>, MembusError> {
    db.cf_handle(name)
        .ok_or_else(|| MembusError::Storage(format!("missing column family '{name}'")))
}

fn read_u64_meta(
    db: &DB,
    meta_cf: &std::sync::Arc<rocksdb::BoundColumnFamily<'_>>,
    key: &[u8],
) -> Result<Option<u64>, MembusError> {
    let Some(bytes) = db
        .get_cf(meta_cf, key)
        .map_err(|err| MembusError::Storage(err.into_string()))?
    else {
        return Ok(None);
    };
    if bytes.len() != 8 {
        return Err(MembusError::Corruption(format!(
            "meta u64 entry expected 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(Some(u64::from_be_bytes(bytes.try_into().unwrap())))
}

fn read_epoch_seq_meta(
    db: &DB,
    meta_cf: &std::sync::Arc<rocksdb::BoundColumnFamily<'_>>,
    key: &[u8],
) -> Option<(u64, u64)> {
    let raw = db.get_cf(meta_cf, key).ok()??;
    if raw.len() != 16 {
        return None;
    }
    let epoch = u64::from_be_bytes(raw[0..8].try_into().ok()?);
    let seq = u64::from_be_bytes(raw[8..16].try_into().ok()?);
    Some((epoch, seq))
}

fn parse_single_data_dir(args: &[String], cmd: &str) -> Result<PathBuf, MembusError> {
    let mut data_dir: Option<PathBuf> = None;
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--data-dir" => {
                idx += 1;
                let val = args.get(idx).ok_or_else(|| {
                    MembusError::InvalidArgument("missing value for '--data-dir'".to_owned())
                })?;
                data_dir = Some(PathBuf::from(val));
            }
            other => {
                return Err(MembusError::InvalidArgument(format!(
                    "unexpected {cmd} option '{other}'"
                )));
            }
        }
        idx += 1;
    }
    data_dir.ok_or_else(|| MembusError::InvalidArgument(format!("{cmd} requires --data-dir <PATH>")))
}

fn parse_diag_restore(args: &[String]) -> Result<DiagCommand, MembusError> {
    let mut backup_dir: Option<PathBuf> = None;
    let mut target_dir: Option<PathBuf> = None;
    let mut deltas: Vec<PathBuf> = Vec::new();
    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--backup-dir" => {
                idx += 1;
                let val = args.get(idx).ok_or_else(|| {
                    MembusError::InvalidArgument("missing value for '--backup-dir'".to_owned())
                })?;
                backup_dir = Some(PathBuf::from(val));
            }
            "--target-dir" => {
                idx += 1;
                let val = args.get(idx).ok_or_else(|| {
                    MembusError::InvalidArgument("missing value for '--target-dir'".to_owned())
                })?;
                target_dir = Some(PathBuf::from(val));
            }
            "--delta" => {
                idx += 1;
                let val = args.get(idx).ok_or_else(|| {
                    MembusError::InvalidArgument("missing value for '--delta'".to_owned())
                })?;
                deltas.push(PathBuf::from(val));
            }
            other => {
                return Err(MembusError::InvalidArgument(format!(
                    "unexpected diag restore option '{other}'"
                )));
            }
        }
        idx += 1;
    }
    let backup_dir = backup_dir.ok_or_else(|| {
        MembusError::InvalidArgument("diag restore requires --backup-dir <DIR>".to_owned())
    })?;
    let target_dir = target_dir.ok_or_else(|| {
        MembusError::InvalidArgument("diag restore requires --target-dir <DIR>".to_owned())
    })?;
    Ok(DiagCommand::Restore {
        backup_dir,
        target_dir,
        deltas,
    })
}
