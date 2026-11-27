use std::sync::Arc;

use parking_lot::Mutex as FastMutex;
use sha2::{Digest, Sha256};

use crate::mesh::protocol::{validate_node_id, MeshDeltaEnvelope, WireReplicatedDelta};
use crate::search::SearchEngine;
use crate::storage::StorageEngine;
use crate::types::{
    now_unix_ms, sanitize_search_log_op, Ack, LogDeltaEntry, MembusError, MutationOp,
    ReplicatedDelta, ReplicationStamp,
};

use super::watermarks::update_peer_inbound_watermark;

/// Convert one settled `CF_LOG` entry into an outbound [`MeshDeltaEnvelope`].
///
/// - Skips local tier-migration swaps (`is_tier_swap == true`) so ColdRef archival and
///   rehydration never enter the mesh.
/// - Relays replicated entries (`entry.replicated == true`) while preserving their original
///   `origin_stamp` so 3+ node topologies (`A <-> B <-> C`) converge cleanly without loops.
/// - Double-sanitizes every `SearchLogOp` so `SearchVisibility::Sealed` plaintext can NEVER
///   leave the node.
pub(super) fn build_envelope_from_log_entry(
    sender_node_id: &str,
    entry: &LogDeltaEntry,
) -> Result<Option<MeshDeltaEnvelope>, MembusError> {
    let mut deltas = Vec::with_capacity(entry.mutations.len());

    for m in &entry.mutations {
        if m.is_tier_swap {
            continue;
        }
        let value_sha256: [u8; 32] = match &m.op {
            MutationOp::Put { value } => Sha256::digest(value).into(),
            MutationOp::Remove => [0u8; 32],
        };
        let stamp = if entry.replicated {
            let Some(orig) = &m.origin_stamp else {
                continue;
            };
            orig.clone()
        } else {
            ReplicationStamp {
                timestamp_ms: entry.ts_ms,
                origin_node_id: sender_node_id.to_owned(),
                origin_seq: entry.seq,
                value_sha256,
            }
        };
        let search_op = match &m.search_op {
            Some(op) => Some(sanitize_search_log_op(op)?),
            None => None,
        };
        deltas.push(WireReplicatedDelta::from(ReplicatedDelta {
            stamp,
            namespace: m.namespace.clone(),
            key: m.key.clone(),
            op: m.op.clone(),
            indexes: m.indexes.clone(),
            search_op,
        }));
    }

    let mut standalone_search_ops = Vec::with_capacity(entry.search_ops.len());
    for op in &entry.search_ops {
        standalone_search_ops.push(sanitize_search_log_op(op)?);
    }

    let standalone_origin = if !standalone_search_ops.is_empty() {
        if entry.replicated {
            entry.standalone_origin.clone()
        } else {
            Some(ReplicationStamp {
                timestamp_ms: entry.ts_ms,
                origin_node_id: sender_node_id.to_owned(),
                origin_seq: entry.seq,
                value_sha256: [0u8; 32],
            })
        }
    } else {
        None
    };

    if deltas.is_empty() && standalone_search_ops.is_empty() {
        return Ok(None);
    }

    Ok(Some(MeshDeltaEnvelope {
        origin_node_id: sender_node_id.to_owned(),
        origin_epoch: entry.epoch,
        origin_seq: entry.seq,
        relayed: entry.replicated,
        deltas,
        standalone_search_ops,
        standalone_origin,
    }))
}

pub(super) fn apply_envelope_blocking(
    storage: &dyn StorageEngine,
    search: Option<&SearchEngine>,
    local_node_id: &str,
    envelope: &MeshDeltaEnvelope,
    watermark_lock: &Arc<FastMutex<()>>,
    require_contiguous_watermark: bool,
) -> Result<(usize, usize), MembusError> {
    if envelope.origin_node_id == local_node_id {
        return Ok((0, envelope.deltas.len()));
    }
    validate_node_id(&envelope.origin_node_id)?;

    let mut applied = 0usize;
    let mut skipped = 0usize;

    for wire_delta in &envelope.deltas {
        let mut clean_delta: ReplicatedDelta = wire_delta.clone().into();
        validate_node_id(&clean_delta.stamp.origin_node_id)?;

        // For non-relayed origin envelopes, enforce strict sender == origin_node_id consistency.
        if !envelope.relayed && clean_delta.stamp.origin_node_id != envelope.origin_node_id {
            return Err(MembusError::Protocol(format!(
                "non-relayed envelope from '{}' carries mismatched delta origin '{}'",
                envelope.origin_node_id, clean_delta.stamp.origin_node_id
            )));
        }
        // Never re-apply our own authored mutation bounced back via a transitive relay.
        if clean_delta.stamp.origin_node_id == local_node_id {
            skipped = skipped.saturating_add(1);
            continue;
        }
        if let Some(op) = &clean_delta.search_op {
            clean_delta.search_op = Some(sanitize_search_log_op(op)?);
        }
        let res = storage.apply_replicated_delta(local_node_id, &clean_delta, Ack::Fast)?;
        if res.applied {
            applied = applied.saturating_add(1);
            if let (Some(engine), Some(local_seq), Some(op)) =
                (search, res.local_seq, clean_delta.search_op)
            {
                if let Err(err) = engine.ingest_committed_op(local_seq, 0, storage.epoch(), op) {
                    engine.record_deferred_ingest(local_seq, &err);
                }
            }
        } else {
            skipped = skipped.saturating_add(1);
        }
    }

    if !envelope.standalone_search_ops.is_empty() {
        let (sa, ss) = apply_standalone_search_ops(storage, search, local_node_id, envelope)?;
        applied = applied.saturating_add(sa);
        skipped = skipped.saturating_add(ss);
    }

    update_peer_inbound_watermark(
        storage,
        &envelope.origin_node_id,
        envelope.origin_epoch,
        envelope.origin_seq,
        require_contiguous_watermark,
        watermark_lock,
    )?;

    Ok((applied, skipped))
}

fn apply_standalone_search_ops(
    storage: &dyn StorageEngine,
    search: Option<&SearchEngine>,
    local_node_id: &str,
    envelope: &MeshDeltaEnvelope,
) -> Result<(usize, usize), MembusError> {
    let origin_stamp = envelope
        .standalone_origin
        .clone()
        .unwrap_or_else(|| ReplicationStamp {
            timestamp_ms: now_unix_ms(),
            origin_node_id: envelope.origin_node_id.clone(),
            origin_seq: envelope.origin_seq,
            value_sha256: [0u8; 32],
        });
    validate_node_id(&origin_stamp.origin_node_id)?;
    if !envelope.relayed && origin_stamp.origin_node_id != envelope.origin_node_id {
        return Err(MembusError::Protocol(format!(
            "non-relayed envelope from '{}' carries mismatched standalone search origin '{}'",
            envelope.origin_node_id, origin_stamp.origin_node_id
        )));
    }
    if origin_stamp.origin_node_id == local_node_id {
        return Ok((0, envelope.standalone_search_ops.len()));
    }

    let mut sanitized_ops = Vec::with_capacity(envelope.standalone_search_ops.len());
    for op in &envelope.standalone_search_ops {
        sanitized_ops.push(sanitize_search_log_op(op)?);
    }
    let res = storage.append_replicated_search_ops(&origin_stamp, &sanitized_ops, Ack::Fast)?;
    if res.applied {
        let count = sanitized_ops.len();
        if let (Some(engine), Some(local_seq)) = (search, res.local_seq) {
            for (idx, op) in sanitized_ops.into_iter().enumerate() {
                if let Err(err) = engine.ingest_committed_op(
                    local_seq,
                    u32::try_from(idx).unwrap_or(u32::MAX),
                    storage.epoch(),
                    op,
                ) {
                    engine.record_deferred_ingest(local_seq, &err);
                }
            }
        }
        Ok((count, 0))
    } else {
        Ok((0, sanitized_ops.len()))
    }
}
