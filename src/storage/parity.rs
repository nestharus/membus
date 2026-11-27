use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::catalog::CollectionCatalog;
use crate::mesh::DigestResponse;
use crate::storage::contract::StorageEngine;
use crate::tiering::TieringManager;
use crate::types::{
    validate_namespace, MembusError, NamespaceParityDigest, ParityDigestMode, ScanOptions,
    MAX_KEY_BYTES,
};

pub use crate::types::encode_sha256_hex;

/// Compute a deterministic 32-byte XOR support-digest over all keys in `(namespace, key_prefix)`
/// under a single O(1) MVCC snapshot using the selected [`ParityDigestMode`] and optional overall
/// execution deadline.
///
/// - [`ParityDigestMode::Full`] hashes `SHA-256(key_len_be4 || key || value_sha256)` while
///   resolving `ColdRef` tombstones to their original `cold_sha256`.
/// - [`ParityDigestMode::KeysOnly`] hashes `SHA-256(key_len_be4 || key)` for additive `vN+1`
///   expand-phase key-set parity checks.
pub fn compute_namespace_xor_digest_with_mode(
    storage: &dyn StorageEngine,
    node_id: &str,
    namespace: &str,
    key_prefix: &[u8],
    mode: ParityDigestMode,
    deadline: Option<Instant>,
) -> Result<(DigestResponse, u64), MembusError> {
    validate_namespace(namespace)?;
    if key_prefix.len() > MAX_KEY_BYTES {
        return Err(MembusError::InvalidArgument(format!(
            "key_prefix length {} exceeds maximum {MAX_KEY_BYTES}",
            key_prefix.len()
        )));
    }
    check_digest_deadline(deadline)?;
    let snap = storage.open_snapshot()?;
    let log_horizon_seq = storage.log_horizon_seq();
    let mut after_key: Option<Vec<u8>> = None;
    let mut xor_digest = [0u8; 32];
    let mut entry_count = 0u64;

    loop {
        check_digest_deadline(deadline)?;
        let page = storage.scan_prefix(
            &snap,
            namespace,
            key_prefix,
            &ScanOptions {
                after_key: after_key.clone(),
                limit: 512,
                max_bytes: 4 * 1024 * 1024,
            },
        )?;
        if page.records.is_empty() {
            break;
        }
        for rec in &page.records {
            let mut hasher = Sha256::new();
            hasher.update((rec.key.len() as u32).to_be_bytes());
            hasher.update(&rec.key);
            if mode == ParityDigestMode::Full {
                let value_sha256: [u8; 32] = if TieringManager::is_cold_ref(rec) {
                    TieringManager::decode_cold_ref(rec)?.cold_sha256
                } else {
                    Sha256::digest(&rec.value).into()
                };
                hasher.update(value_sha256);
            }
            let item_hash: [u8; 32] = hasher.finalize().into();
            for i in 0..32 {
                xor_digest[i] ^= item_hash[i];
            }
            entry_count = entry_count.saturating_add(1);
        }
        if !page.truncated {
            break;
        }
        after_key = page.next_cursor;
        if after_key.is_none() {
            break;
        }
    }

    Ok((
        DigestResponse {
            node_id: node_id.to_owned(),
            epoch: storage.epoch(),
            latest_seq: snap.snapshot_seq(),
            entry_count,
            xor_digest,
        },
        log_horizon_seq,
    ))
}

/// Compute the full `(key, value_sha256)` XOR anti-entropy digest shared by mesh reconciliation
/// and parity endpoints.
pub fn compute_namespace_xor_digest(
    storage: &dyn StorageEngine,
    node_id: &str,
    namespace: &str,
    key_prefix: &[u8],
) -> Result<DigestResponse, MembusError> {
    compute_namespace_xor_digest_with_mode(
        storage,
        node_id,
        namespace,
        key_prefix,
        ParityDigestMode::Full,
        None,
    )
    .map(|(resp, _horizon)| resp)
}

/// Compute a first-class [`NamespaceParityDigest`] including catalog schema version and
/// `log_horizon_seq` for A/B cutover parity gating.
pub fn compute_namespace_parity_digest(
    storage: &dyn StorageEngine,
    catalog: &CollectionCatalog,
    node_id: &str,
    namespace: &str,
    key_prefix: &[u8],
    mode: ParityDigestMode,
) -> Result<NamespaceParityDigest, MembusError> {
    compute_namespace_parity_digest_with_timeout(
        storage, catalog, node_id, namespace, key_prefix, mode, None,
    )
}

/// Compute a [`NamespaceParityDigest`] enforcing a single overall cooperative timeout across
/// all 512-key scan pages so timed-out requests release their MVCC snapshot and worker promptly.
pub fn compute_namespace_parity_digest_with_timeout(
    storage: &dyn StorageEngine,
    catalog: &CollectionCatalog,
    node_id: &str,
    namespace: &str,
    key_prefix: &[u8],
    mode: ParityDigestMode,
    timeout: Option<Duration>,
) -> Result<NamespaceParityDigest, MembusError> {
    let deadline = timeout.and_then(|d| Instant::now().checked_add(d));
    let (digest, log_horizon_seq) = compute_namespace_xor_digest_with_mode(
        storage, node_id, namespace, key_prefix, mode, deadline,
    )?;
    let catalog_schema_version = catalog
        .get_collection(namespace)?
        .map(|desc| desc.schema_version);
    Ok(NamespaceParityDigest {
        node_id: digest.node_id,
        epoch: digest.epoch,
        latest_seq: digest.latest_seq,
        log_horizon_seq,
        entry_count: digest.entry_count,
        xor_digest_hex: encode_sha256_hex(&digest.xor_digest),
        catalog_schema_version,
    })
}

fn check_digest_deadline(deadline: Option<Instant>) -> Result<(), MembusError> {
    if let Some(dl) = deadline {
        if Instant::now() >= dl {
            return Err(MembusError::Busy {
                retry_after_ms: 50,
                reason: "namespace digest query timeout exceeded".to_owned(),
            });
        }
    }
    Ok(())
}
