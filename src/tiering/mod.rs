use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::backup::encode_hex;
use crate::storage::codec::{compute_record_crc32, RECORD_FLAG_COLD_REF};
use crate::storage::StorageEngine;
use crate::types::{
    validate_key, validate_namespace, Ack, ColdRef, CommitResult, ExpectedVersion, MembusError,
    ReadMode, RecordMeta, ScanOptions, StoredRecord,
};

static COLD_TMP_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Content-addressed cold object tier contract.
///
/// Implementations MUST verify SHA-256 integrity on every read and fail closed
/// with [`MembusError::Corruption`] if stored bytes do not match their digest.
pub trait ColdStore: Send + Sync {
    fn tier_uri_for(&self, sha256: &[u8; 32]) -> String;
    fn put_object(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<String, MembusError>;
    fn get_object(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, MembusError>;
    fn has_object(&self, sha256: &[u8; 32]) -> Result<bool, MembusError>;
    fn delete_object(&self, sha256: &[u8; 32]) -> Result<(), MembusError>;
}

/// Content-addressed local filesystem cold tier sharded by SHA-256 prefix `<aa>/<bb>/<sha256>.blob`.
pub struct FileColdStore {
    root: PathBuf,
}

impl FileColdStore {
    pub fn open(root: &Path) -> Result<Self, MembusError> {
        fs::create_dir_all(root)?;
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Deterministic sharded object path for a 32-byte SHA-256 digest.
    pub fn object_path(&self, sha256: &[u8; 32]) -> PathBuf {
        let hex = encode_hex(sha256);
        self.root
            .join(&hex[0..2])
            .join(&hex[2..4])
            .join(format!("{hex}.blob"))
    }
}

impl ColdStore for FileColdStore {
    fn tier_uri_for(&self, sha256: &[u8; 32]) -> String {
        format!("file-cold://{}", encode_hex(sha256))
    }

    fn put_object(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<String, MembusError> {
        let actual: [u8; 32] = Sha256::digest(bytes).into();
        if actual != *sha256 {
            return Err(MembusError::Corruption(
                "cold object SHA-256 mismatch on put".to_owned(),
            ));
        }

        let target = self.object_path(sha256);
        if target.exists() {
            verify_existing_cold_file(&target, sha256)?;
            return Ok(self.tier_uri_for(sha256));
        }

        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        let seq = COLD_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = target.with_extension(format!("blob.tmp.{}.{seq}", std::process::id()));
        {
            let mut f = File::create(&tmp)?;
            f.write_all(bytes)?;
            f.sync_all()?;
        }
        if let Err(err) = fs::rename(&tmp, &target) {
            let _ = fs::remove_file(&tmp);
            if target.exists() {
                verify_existing_cold_file(&target, sha256)?;
                return Ok(self.tier_uri_for(sha256));
            }
            return Err(MembusError::Io(err.to_string()));
        }
        Ok(self.tier_uri_for(sha256))
    }

    fn get_object(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, MembusError> {
        let target = self.object_path(sha256);
        if !target.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&target)?;
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != *sha256 {
            return Err(MembusError::Corruption(format!(
                "cold tier object {} failed SHA-256 verification on read",
                encode_hex(sha256)
            )));
        }
        Ok(Some(bytes))
    }

    fn has_object(&self, sha256: &[u8; 32]) -> Result<bool, MembusError> {
        Ok(self.object_path(sha256).is_file())
    }

    fn delete_object(&self, sha256: &[u8; 32]) -> Result<(), MembusError> {
        let target = self.object_path(sha256);
        if target.exists() {
            fs::remove_file(target)?;
        }
        Ok(())
    }
}

fn verify_existing_cold_file(target: &Path, expected_sha256: &[u8; 32]) -> Result<(), MembusError> {
    let existing = fs::read(target)?;
    let existing_hash: [u8; 32] = Sha256::digest(&existing).into();
    if existing_hash != *expected_sha256 {
        return Err(MembusError::Corruption(
            "existing cold tier object failed SHA-256 verification".to_owned(),
        ));
    }
    Ok(())
}

/// Eligibility filter for off-path cold archival passes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieringPolicy {
    pub namespace: String,
    pub key_prefix: Vec<u8>,
    pub min_age_ms: u64,
    pub min_value_bytes: usize,
    pub max_entries_per_run: usize,
}

/// Summary counters returned by [`TieringManager::archive_cold_entries`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArchiveReport {
    pub scanned: usize,
    pub archived: usize,
    pub skipped_already_cold: usize,
    pub skipped_conflict: usize,
    pub bytes_offloaded: u64,
}

/// Off-path hot/warm/cold tiering coordinator managing compact `ColdRef` tombstones,
/// transparent SHA-256-verified hydration, explicit rehydration, and watermark-safe log trimming.
pub struct TieringManager {
    storage: Arc<dyn StorageEngine>,
    cold_store: Arc<dyn ColdStore>,
}

impl TieringManager {
    pub fn new(storage: Arc<dyn StorageEngine>, cold_store: Arc<dyn ColdStore>) -> Self {
        Self {
            storage,
            cold_store,
        }
    }

    /// True if `record` holds a compact [`ColdRef`] lineage tombstone rather than hot bytes.
    pub fn is_cold_ref(record: &StoredRecord) -> bool {
        (record.meta.flags & RECORD_FLAG_COLD_REF) != 0
    }

    /// Decode the [`ColdRef`] tombstone from a cold-flagged [`StoredRecord`].
    pub fn decode_cold_ref(record: &StoredRecord) -> Result<ColdRef, MembusError> {
        if !Self::is_cold_ref(record) {
            return Err(MembusError::InvalidArgument(
                "record is not a ColdRef tombstone".to_owned(),
            ));
        }
        serde_json::from_slice(&record.value).map_err(|err| {
            MembusError::Corruption(format!("corrupt ColdRef lineage tombstone: {err}"))
        })
    }

    /// Scan eligible hot records matching `policy`, write their payloads to `ColdStore`,
    /// and CAS-swap `CF_KV` to compact `ColdRef` lineage tombstones (preserving secondary indexes,
    /// original logical `timestamp_ms`, LWW stamp, and emitting NO mesh/signal churn).
    pub fn archive_cold_entries(
        &self,
        policy: &TieringPolicy,
        now_ms: u64,
        ack: Ack,
    ) -> Result<ArchiveReport, MembusError> {
        validate_namespace(&policy.namespace)?;
        if policy.max_entries_per_run == 0 {
            return Err(MembusError::InvalidArgument(
                "max_entries_per_run must be > 0".to_owned(),
            ));
        }

        let snap = self.storage.open_snapshot()?;
        let mut after_key: Option<Vec<u8>> = None;
        let mut report = ArchiveReport::default();

        loop {
            if report.archived >= policy.max_entries_per_run {
                break;
            }
            let page = self.storage.scan_prefix(
                &snap,
                &policy.namespace,
                &policy.key_prefix,
                &ScanOptions {
                    after_key: after_key.clone(),
                    limit: 256,
                    max_bytes: 4 * 1024 * 1024,
                },
            )?;
            if page.records.is_empty() {
                break;
            }

            for rec in page.records {
                if report.archived >= policy.max_entries_per_run {
                    break;
                }
                report.scanned = report.scanned.saturating_add(1);
                if Self::is_cold_ref(&rec) {
                    report.skipped_already_cold = report.skipped_already_cold.saturating_add(1);
                    continue;
                }
                if rec.value.len() < policy.min_value_bytes {
                    continue;
                }
                if now_ms.saturating_sub(rec.meta.ts_ms) < policy.min_age_ms {
                    continue;
                }

                let sha256: [u8; 32] = Sha256::digest(&rec.value).into();
                let tier_uri = self.cold_store.put_object(&sha256, &rec.value)?;
                let cold_ref = ColdRef {
                    cold_sha256: sha256,
                    original_bytes: rec.value.len() as u64,
                    archived_at_ms: now_ms,
                    tier_uri,
                    lineage_seq: rec.meta.version,
                };
                let ref_bytes = serde_json::to_vec(&cold_ref).map_err(|err| {
                    MembusError::Storage(format!("encode ColdRef failed: {err}"))
                })?;

                match self.storage.tier_swap_record(
                    &policy.namespace,
                    &rec.key,
                    &ref_bytes,
                    true,
                    sha256,
                    ack,
                    ExpectedVersion::Version(rec.meta.version),
                ) {
                    Ok(_) => {
                        report.archived = report.archived.saturating_add(1);
                        report.bytes_offloaded = report
                            .bytes_offloaded
                            .saturating_add(cold_ref.original_bytes);
                    }
                    Err(MembusError::Conflict { .. } | MembusError::TxConflict(_)) => {
                        report.skipped_conflict = report.skipped_conflict.saturating_add(1);
                    }
                    Err(err) => return Err(err),
                }
            }

            if !page.truncated {
                break;
            }
            after_key = page.next_cursor;
            if after_key.is_none() {
                break;
            }
        }

        Ok(report)
    }

    /// Read a record with explicit [`ReadMode::HotOnly`] or [`ReadMode::HydrateCold`] semantics.
    ///
    /// On `HydrateCold`, fetches cold payloads, verifies SHA-256 + byte length (failing closed
    /// with [`MembusError::Corruption`] if missing or tampered), and recomputes `meta.crc32`
    /// over the hydrated payload bytes.
    pub fn get(
        &self,
        namespace: &str,
        key: &[u8],
        mode: ReadMode,
    ) -> Result<Option<StoredRecord>, MembusError> {
        validate_namespace(namespace)?;
        validate_key(key)?;
        let Some(rec) = self.storage.get(namespace, key)? else {
            return Ok(None);
        };
        if !Self::is_cold_ref(&rec) || mode == ReadMode::HotOnly {
            return Ok(Some(rec));
        }
        let cold_ref = Self::decode_cold_ref(&rec)?;
        let bytes = self.fetch_verified_cold(&cold_ref)?;
        let hydrated_flags = rec.meta.flags & !RECORD_FLAG_COLD_REF;
        let hydrated_crc =
            compute_record_crc32(rec.meta.version, hydrated_flags, rec.meta.ts_ms, &bytes);
        Ok(Some(StoredRecord {
            key: rec.key,
            value: bytes,
            meta: RecordMeta {
                version: rec.meta.version,
                flags: hydrated_flags,
                ts_ms: rec.meta.ts_ms,
                crc32: hydrated_crc,
            },
        }))
    }

    /// Explicitly promote a cold-tiered record back into hot `CF_KV` (clearing `RECORD_FLAG_COLD_REF`
    /// while preserving original `timestamp_ms`, LWW stamp, other flag bits, and secondary indexes;
    /// emitting NO signals or mesh churn). Returns `Ok(None)` if absent or already hot.
    pub fn rehydrate_cold(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
    ) -> Result<Option<CommitResult>, MembusError> {
        validate_namespace(namespace)?;
        validate_key(key)?;
        let Some(rec) = self.storage.get(namespace, key)? else {
            return Ok(None);
        };
        if !Self::is_cold_ref(&rec) {
            return Ok(None);
        }
        let cold_ref = Self::decode_cold_ref(&rec)?;
        let bytes = self.fetch_verified_cold(&cold_ref)?;
        let commit = self.storage.tier_swap_record(
            namespace,
            key,
            &bytes,
            false,
            cold_ref.cold_sha256,
            ack,
            ExpectedVersion::Version(rec.meta.version),
        )?;
        Ok(Some(commit))
    }

    /// Coordinate log trimming against active search, backup, and mesh watermarks unless `force == true`.
    pub fn safe_trim_log_before(&self, keep_from_seq: u64, force: bool) -> Result<(), MembusError> {
        self.storage.safe_trim_log_before(keep_from_seq, force)
    }

    fn fetch_verified_cold(&self, cold_ref: &ColdRef) -> Result<Vec<u8>, MembusError> {
        let Some(bytes) = self.cold_store.get_object(&cold_ref.cold_sha256)? else {
            return Err(MembusError::Corruption(format!(
                "cold object '{}' missing from cold store",
                cold_ref.tier_uri
            )));
        };
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != cold_ref.cold_sha256 {
            return Err(MembusError::Corruption(format!(
                "cold object '{}' SHA-256 verification failed",
                cold_ref.tier_uri
            )));
        }
        if bytes.len() as u64 != cold_ref.original_bytes {
            return Err(MembusError::Corruption(format!(
                "cold object '{}' length mismatch: expected {}, got {}",
                cold_ref.tier_uri,
                cold_ref.original_bytes,
                bytes.len()
            )));
        }
        Ok(bytes)
    }
}
