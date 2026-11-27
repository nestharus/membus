use crate::types::MembusError;

pub(crate) const META_PREFIX_MESH_LWW: &[u8] = b"mesh:lww:";
pub(crate) const META_PREFIX_MESH_LOCAL_LWW: &[u8] = b"mesh:local_lww:";
pub(crate) const META_PREFIX_MESH_TOMB: &[u8] = b"mesh:tomb:";
pub(crate) const META_PREFIX_MESH_PEER: &[u8] = b"mesh:peer:";
pub(crate) const META_PREFIX_MESH_SEARCH_SEEN: &[u8] = b"mesh:search_seen:";

pub(crate) fn validate_replication_node_id(node_id: &str, field: &str) -> Result<(), MembusError> {
    if node_id.is_empty() || node_id.len() > 128 {
        return Err(MembusError::InvalidArgument(format!(
            "replication {field} length must be 1..=128 bytes"
        )));
    }
    if node_id.trim() != node_id {
        return Err(MembusError::InvalidArgument(format!(
            "replication {field} must not have leading or trailing whitespace"
        )));
    }
    if node_id.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(MembusError::InvalidArgument(format!(
            "replication {field} must not contain ASCII control characters"
        )));
    }
    Ok(())
}
