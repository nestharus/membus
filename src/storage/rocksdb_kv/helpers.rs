use rocksdb::ErrorKind;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::types::{
    validate_namespace, ExpectedVersion, IdempotencyContext, LeaseGuard, MembusError, ScanOptions,
    SearchLogOp, MAX_IDEMPOTENCY_KEY_BYTES,
};

pub(crate) const META_KEY_EPOCH: &[u8] = b"db_epoch";
pub(crate) const META_KEY_LOG_HORIZON: &[u8] = b"log_horizon_seq";
pub(crate) const META_KEY_SEARCH_CHECKPOINT: &[u8] = b"search:checkpoint";
pub(crate) const META_KEY_BACKUP_LAST_SEQ: &[u8] = b"backup:last_seq";
pub(crate) const META_KEY_MESH_PUB_SEQ: &[u8] = b"mesh:pub_seq";
pub(crate) const META_PREFIX_IDEM_KEY: &[u8] = b"idem:k:";
pub(crate) const META_PREFIX_IDEM_ORD: &[u8] = b"idem:ord:";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredIdempotencyRecord {
    pub(crate) request_fingerprint_sha256: [u8; 32],
    pub(crate) committed_seq: u64,
    pub(crate) committed_epoch: u64,
    pub(crate) committed_version: u64,
    pub(crate) committed_versions: Vec<u64>,
}

pub(crate) fn estimate_search_op_bytes(op: &SearchLogOp) -> usize {
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

pub(crate) fn validate_idempotency_context(ctx: &IdempotencyContext) -> Result<(), MembusError> {
    validate_namespace(&ctx.scope_namespace)?;
    if let Some(pid) = &ctx.principal_id {
        if pid.len() > MAX_IDEMPOTENCY_KEY_BYTES || pid.bytes().any(|b| b < 0x20 || b == 0x7f) {
            return Err(MembusError::InvalidArgument(
                "invalid principal_id for idempotency context".to_owned(),
            ));
        }
    }
    if ctx.key.is_empty() || ctx.key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
        return Err(MembusError::InvalidArgument(format!(
            "idempotency key length must be 1..={MAX_IDEMPOTENCY_KEY_BYTES} bytes"
        )));
    }
    if ctx.key.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(MembusError::InvalidArgument(
            "idempotency key must not contain ASCII control characters".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn encode_idempotency_key(ctx: &IdempotencyContext) -> Vec<u8> {
    let domain_tag: u8 = if ctx.is_tx { 0x02 } else { 0x01 };
    let scope_sha: [u8; 32] = Sha256::digest(ctx.scope_namespace.as_bytes()).into();
    let key_sha: [u8; 32] = Sha256::digest(ctx.key.as_bytes()).into();
    let principal_bytes = ctx.principal_id.as_deref().unwrap_or("").as_bytes();
    let mut out = Vec::with_capacity(
        META_PREFIX_IDEM_KEY.len() + 1 + 1 + 4 + principal_bytes.len() + 32 + 32,
    );
    out.extend_from_slice(META_PREFIX_IDEM_KEY);
    out.push(domain_tag);
    out.push(u8::from(ctx.principal_id.is_some()));
    out.extend_from_slice(&(principal_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(principal_bytes);
    out.extend_from_slice(&scope_sha);
    out.extend_from_slice(&key_sha);
    out
}

pub(crate) fn encode_idempotency_ord_key(seq: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(META_PREFIX_IDEM_ORD.len() + 8);
    out.extend_from_slice(META_PREFIX_IDEM_ORD);
    out.extend_from_slice(&seq.to_be_bytes());
    out
}

pub(crate) fn validate_scan_options(
    options: &ScanOptions,
    max_scan_limit: usize,
) -> Result<(), MembusError> {
    if options.limit == 0 {
        return Err(MembusError::InvalidArgument(
            "scan limit must be > 0".to_owned(),
        ));
    }
    if options.limit > max_scan_limit {
        return Err(MembusError::InvalidArgument(format!(
            "scan limit {} exceeds max_scan_limit {max_scan_limit}",
            options.limit
        )));
    }
    if options.max_bytes == 0 {
        return Err(MembusError::InvalidArgument(
            "scan max_bytes must be > 0".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn verify_expected_version(
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

pub(crate) fn decode_epoch_seq_meta(raw: &[u8]) -> Option<(u64, u64)> {
    if raw.len() != 16 {
        return None;
    }
    let epoch = u64::from_be_bytes(raw[0..8].try_into().ok()?);
    let seq = u64::from_be_bytes(raw[8..16].try_into().ok()?);
    Some((epoch, seq))
}

pub(crate) fn map_rocksdb_err(err: rocksdb::Error) -> MembusError {
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

pub(crate) fn map_tx_err(err: rocksdb::Error) -> MembusError {
    match err.kind() {
        ErrorKind::Busy | ErrorKind::TryAgain => MembusError::TxConflict(err.into_string()),
        ErrorKind::Corruption => MembusError::Corruption(err.into_string()),
        ErrorKind::InvalidArgument => MembusError::InvalidArgument(err.into_string()),
        _ => MembusError::Storage(err.into_string()),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LeaseRecord {
    pub(crate) holder: String,
    pub(crate) generation: u64,
    pub(crate) token: String,
    pub(crate) expires_at_ms: u64,
    pub(crate) released: bool,
}

pub(crate) fn encode_lease_record(rec: &LeaseRecord) -> Result<Vec<u8>, MembusError> {
    serde_json::to_vec(rec)
        .map_err(|err| MembusError::Storage(format!("failed to encode lease record: {err}")))
}

pub(crate) fn decode_lease_record(bytes: &[u8]) -> Result<LeaseRecord, MembusError> {
    serde_json::from_slice(bytes)
        .map_err(|err| MembusError::Corruption(format!("corrupt lease record: {err}")))
}

pub(crate) fn lease_matches_active(
    rec: &LeaseRecord,
    holder: &str,
    generation: u64,
    token: &str,
    now_ms: u64,
) -> bool {
    if rec.released || rec.expires_at_ms <= now_ms {
        return false;
    }
    if rec.holder != holder || rec.generation != generation {
        return false;
    }
    let a: [u8; 32] = Sha256::digest(rec.token.as_bytes()).into();
    let b: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    bool::from(a.ct_eq(&b))
}

/// Verify an atomic [`LeaseGuard`] against raw `CF_LEASE` record bytes inside a transaction.
///
/// Rejects absent, released, or expired leases with [`MembusError::NotFound`] and active
/// mismatched leases (wrong holder, generation, or secret token) with [`MembusError::LeaseHeld`].
/// Never embeds secret token material in error messages.
pub(crate) fn verify_lease_guard_bytes(
    raw: Option<&[u8]>,
    guard: &LeaseGuard,
    now_ms: u64,
) -> Result<(), MembusError> {
    let Some(bytes) = raw else {
        return Err(MembusError::NotFound(format!(
            "lease '{}' does not exist",
            guard.resource
        )));
    };
    let rec = decode_lease_record(bytes)?;
    if rec.released {
        return Err(MembusError::NotFound(format!(
            "lease '{}' is inactive (already released at generation {})",
            guard.resource, rec.generation
        )));
    }
    if rec.expires_at_ms <= now_ms {
        return Err(MembusError::NotFound(format!(
            "lease '{}' is inactive (expired at generation {})",
            guard.resource, rec.generation
        )));
    }
    if !lease_matches_active(&rec, &guard.holder, guard.generation, &guard.token, now_ms) {
        return Err(MembusError::LeaseHeld {
            resource: guard.resource.clone(),
            holder: rec.holder,
            generation: rec.generation,
            expires_at_ms: rec.expires_at_ms,
        });
    }
    Ok(())
}
