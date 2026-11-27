/// Reserved column family names provisioned on open.
pub const CF_KV: &str = "kv";
pub const CF_LOG: &str = "log";
pub const CF_BLOB: &str = "blob";
pub const CF_IDX_ORD: &str = "idx_ord";
pub const CF_IDX_EDGE: &str = "idx_edge";
pub const CF_IDX_REV: &str = "idx_rev";
pub const CF_LEASE: &str = "lease";
pub const CF_META: &str = "meta";

/// All named column families provisioned by Membus v2 storage.
pub const ALL_COLUMN_FAMILIES: [&str; 8] = [
    CF_KV,
    CF_LOG,
    CF_BLOB,
    CF_IDX_ORD,
    CF_IDX_EDGE,
    CF_IDX_REV,
    CF_LEASE,
    CF_META,
];

/// Concatenate a metadata key prefix with a suffix into a single owned buffer.
pub(crate) fn encode_prefixed_key(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + suffix.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(suffix);
    out
}
