use std::sync::Arc;

use parking_lot::Mutex as FastMutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::mesh::protocol::MeshDeltaEnvelope;
use crate::search::SearchEngine;
use crate::storage::StorageEngine;

use super::envelope::apply_envelope_blocking;
use super::state::wait_shutdown;

pub(super) struct SubscriberContext {
    pub(super) node_id: String,
    pub(super) delta_prefix: String,
    pub(super) max_payload: usize,
    pub(super) max_deltas: usize,
    pub(super) storage: Arc<dyn StorageEngine>,
    pub(super) search: Option<Arc<SearchEngine>>,
    pub(super) watermark_lock: Arc<FastMutex<()>>,
    pub(super) shutdown_rx: watch::Receiver<bool>,
}

pub(super) fn spawn_subscriber_task(
    subscriber: zenoh::pubsub::Subscriber<zenoh::handlers::FifoChannelHandler<zenoh::sample::Sample>>,
    mut ctx: SubscriberContext,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = wait_shutdown(&mut ctx.shutdown_rx) => break,
                sample = subscriber.recv_async() => {
                    let Ok(sample) = sample else { break; };
                    let ke_str = sample.key_expr().as_str().to_owned();
                    let Some(key_sender) = ke_str.strip_prefix(&ctx.delta_prefix) else {
                        continue;
                    };
                    let raw = sample.payload().to_bytes();
                    if raw.len() > ctx.max_payload {
                        continue;
                    }
                    let Ok(envelope) = serde_json::from_slice::<MeshDeltaEnvelope>(&raw) else {
                        continue;
                    };
                    if envelope.origin_node_id != key_sender
                        || envelope.origin_node_id == ctx.node_id
                        || envelope.deltas.len() > ctx.max_deltas
                        || envelope.standalone_search_ops.len() > ctx.max_deltas
                    {
                        continue;
                    }
                    let st = Arc::clone(&ctx.storage);
                    let sr = ctx.search.clone();
                    let nid = ctx.node_id.clone();
                    let wml = Arc::clone(&ctx.watermark_lock);
                    let _ = tokio::task::spawn_blocking(move || {
                        apply_envelope_blocking(
                            st.as_ref(),
                            sr.as_deref(),
                            &nid,
                            &envelope,
                            &wml,
                            true, // live_pubsub => strictly contiguous watermark only
                        )
                    })
                    .await;
                }
            }
        }
    })
}
