use std::sync::Arc;

use parking_lot::Mutex as FastMutex;

use crate::mesh::protocol::{
    peer_watermark_meta_key, validate_node_id, MeshPeerWatermark, META_KEY_MESH_PUB_SEQ,
};
use crate::storage::{MetaCasGuard, StorageEngine};
use crate::types::{now_unix_ms, MembusError};

pub(super) fn read_or_init_published_seq(storage: &dyn StorageEngine) -> Result<u64, MembusError> {
    let current_epoch = storage.epoch();
    if let Some(raw) = storage.meta_get(META_KEY_MESH_PUB_SEQ)? {
        if raw.len() != 16 {
            return Err(MembusError::Corruption(format!(
                "corrupt mesh:pub_seq watermark: expected 16 bytes, got {}",
                raw.len()
            )));
        }
        let saved_epoch = u64::from_be_bytes(raw[0..8].try_into().unwrap());
        let saved_seq = u64::from_be_bytes(raw[8..16].try_into().unwrap());
        if saved_epoch == current_epoch {
            return Ok(saved_seq);
        }
    }
    // Persist initial `mesh:pub_seq` on startup so safe log-trimming honors an active mesh node.
    persist_published_seq(storage, 0)?;
    Ok(0)
}

pub(super) fn persist_published_seq(
    storage: &dyn StorageEngine,
    seq: u64,
) -> Result<(), MembusError> {
    let mut raw = [0u8; 16];
    raw[0..8].copy_from_slice(&storage.epoch().to_be_bytes());
    raw[8..16].copy_from_slice(&seq.to_be_bytes());
    storage.meta_put(META_KEY_MESH_PUB_SEQ, &raw, false)
}

pub(super) fn read_peer_watermark(
    storage: &dyn StorageEngine,
    peer_id: &str,
) -> Result<Option<MeshPeerWatermark>, MembusError> {
    validate_node_id(peer_id)?;
    let key = peer_watermark_meta_key(peer_id);
    let Some(raw) = storage.meta_get(&key)? else {
        return Ok(None);
    };
    let wm: MeshPeerWatermark = serde_json::from_slice(&raw)
        .map_err(|err| MembusError::Corruption(format!("corrupt peer watermark: {err}")))?;
    Ok(Some(wm))
}

fn cas_peer_watermark<F>(
    storage: &dyn StorageEngine,
    peer_id: &str,
    watermark_lock: &Arc<FastMutex<()>>,
    contention_label: &str,
    build_next: F,
) -> Result<(), MembusError>
where
    F: Fn(Option<&MeshPeerWatermark>) -> Option<MeshPeerWatermark>,
{
    let _guard = watermark_lock.lock();
    let key = peer_watermark_meta_key(peer_id);

    for _ in 0..8 {
        let prev_raw = storage.meta_get(&key)?;
        let prev: Option<MeshPeerWatermark> = match &prev_raw {
            Some(bytes) => Some(serde_json::from_slice(bytes).map_err(|err| {
                MembusError::Corruption(format!("corrupt peer watermark '{peer_id}': {err}"))
            })?),
            None => None,
        };

        let Some(wm) = build_next(prev.as_ref()) else {
            return Ok(());
        };
        let encoded = serde_json::to_vec(&wm)
            .map_err(|err| MembusError::Storage(format!("encode peer watermark failed: {err}")))?;
        match storage.meta_cas_batch(
            &[MetaCasGuard {
                key: key.clone(),
                expected: prev_raw,
            }],
            &[(key.clone(), encoded)],
            &[],
            false,
        ) {
            Ok(()) => return Ok(()),
            Err(MembusError::TxConflict(_)) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(MembusError::TxConflict(format!(
        "{contention_label} contention for '{peer_id}'"
    )))
}

pub(super) fn update_peer_inbound_watermark(
    storage: &dyn StorageEngine,
    peer_id: &str,
    remote_epoch: u64,
    origin_seq: u64,
    require_contiguous: bool,
    watermark_lock: &Arc<FastMutex<()>>,
) -> Result<(), MembusError> {
    cas_peer_watermark(
        storage,
        peer_id,
        watermark_lock,
        "peer watermark update",
        |prev| {
            let (next_epoch, next_seq) = match prev {
                Some(wm) if wm.remote_epoch == remote_epoch => {
                    if require_contiguous {
                        if origin_seq == wm.last_origin_seq.saturating_add(1) {
                            (remote_epoch, origin_seq)
                        } else {
                            // Out-of-order or duplicate live sample: preserve contiguous floor!
                            return None;
                        }
                    } else {
                        (remote_epoch, wm.last_origin_seq.max(origin_seq))
                    }
                }
                _ => {
                    if require_contiguous {
                        if origin_seq == 1 {
                            (remote_epoch, 1)
                        } else {
                            // First observed live sample skipped seq 1..origin_seq-1: hold floor at 0.
                            (remote_epoch, 0)
                        }
                    } else {
                        (remote_epoch, origin_seq)
                    }
                }
            };

            Some(MeshPeerWatermark {
                peer_id: peer_id.to_owned(),
                remote_epoch: next_epoch,
                last_origin_seq: next_seq,
                // Preserve existing outbound ack untouched (NEVER overwrite with local latest_seq!).
                local_epoch: prev.and_then(|p| p.local_epoch),
                outbound_acked_seq: prev.and_then(|p| p.outbound_acked_seq),
                updated_at_ms: now_unix_ms(),
            })
        },
    )
}

pub(super) fn reset_peer_inbound_epoch(
    storage: &dyn StorageEngine,
    peer_id: &str,
    new_remote_epoch: u64,
    watermark_lock: &Arc<FastMutex<()>>,
) -> Result<(), MembusError> {
    cas_peer_watermark(
        storage,
        peer_id,
        watermark_lock,
        "peer watermark epoch reset",
        |prev| {
            Some(MeshPeerWatermark {
                peer_id: peer_id.to_owned(),
                remote_epoch: new_remote_epoch,
                last_origin_seq: 0,
                local_epoch: prev.and_then(|p| p.local_epoch),
                outbound_acked_seq: prev.and_then(|p| p.outbound_acked_seq),
                updated_at_ms: now_unix_ms(),
            })
        },
    )
}

pub(super) fn record_peer_outbound_ack(
    storage: &dyn StorageEngine,
    peer_id: &str,
    local_epoch: u64,
    acked_seq: u64,
    watermark_lock: &Arc<FastMutex<()>>,
) -> Result<(), MembusError> {
    let clamped_ack = acked_seq.min(storage.latest_seq());
    cas_peer_watermark(
        storage,
        peer_id,
        watermark_lock,
        "peer outbound ack",
        |prev| {
            let next_outbound = match prev {
                Some(wm) if wm.local_epoch == Some(local_epoch) => {
                    wm.outbound_acked_seq.unwrap_or(0).max(clamped_ack)
                }
                _ => clamped_ack,
            };
            Some(MeshPeerWatermark {
                peer_id: peer_id.to_owned(),
                remote_epoch: prev.map_or(0, |p| p.remote_epoch),
                last_origin_seq: prev.map_or(0, |p| p.last_origin_seq),
                local_epoch: Some(local_epoch),
                outbound_acked_seq: Some(next_outbound),
                updated_at_ms: now_unix_ms(),
            })
        },
    )
}
