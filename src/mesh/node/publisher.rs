use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use crate::storage::StorageEngine;
use crate::types::MembusError;

use super::envelope::build_envelope_from_log_entry;
use super::state::{wait_shutdown, MeshNode};
use super::watermarks::persist_published_seq;

pub(super) struct PublisherContext {
    pub(super) session: zenoh::Session,
    pub(super) pub_ke: String,
    pub(super) node_id: String,
    pub(super) storage: Arc<dyn StorageEngine>,
    pub(super) published_seq: Arc<AtomicU64>,
    pub(super) publish_lock: Arc<AsyncMutex<()>>,
    pub(super) max_batch: usize,
    pub(super) max_payload_bytes: usize,
    pub(super) shutdown_rx: watch::Receiver<bool>,
}

pub(super) fn spawn_publisher_task(
    mut ctx: PublisherContext,
    poll_interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let _ = drain_and_publish_once(&ctx).await;
            tokio::select! {
                biased;
                _ = wait_shutdown(&mut ctx.shutdown_rx) => break,
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }
    })
}

pub(super) async fn drain_and_publish_once(ctx: &PublisherContext) -> Result<(), MembusError> {
    let _guard = ctx.publish_lock.lock().await;
    loop {
        if *ctx.shutdown_rx.borrow() {
            break;
        }
        let after_seq = ctx.published_seq.load(Ordering::Acquire);
        if ctx.storage.latest_seq() <= after_seq {
            break;
        }
        let st = Arc::clone(&ctx.storage);
        let nid = ctx.node_id.clone();
        let max_batch = ctx.max_batch;
        let scan_res = tokio::task::spawn_blocking(move || {
            let page = st.scan_deltas_after(after_seq, max_batch)?;
            let mut envelopes = Vec::new();
            for entry in &page.entries {
                if let Some(env) = build_envelope_from_log_entry(&nid, entry)? {
                    envelopes.push(env);
                }
            }
            Ok::<_, MembusError>((envelopes, page.scanned_through_seq))
        })
        .await
        .map_err(|err| MembusError::Storage(format!("mesh publish scan panicked: {err}")))?;

        let (envelopes, scanned_through) = match scan_res {
            Ok(pair) => pair,
            Err(MembusError::Gap {
                earliest_available_seq,
                ..
            }) => {
                // Forced log trim jumped ahead of publisher cursor: advance to the new horizon
                // so the publisher never wedges permanently.
                let recovered_seq = earliest_available_seq.saturating_sub(1);
                ctx.published_seq.store(recovered_seq, Ordering::Release);
                let st = Arc::clone(&ctx.storage);
                let _ = tokio::task::spawn_blocking(move || {
                    persist_published_seq(st.as_ref(), recovered_seq)
                })
                .await;
                continue;
            }
            Err(err) => return Err(err),
        };

        for env in &envelopes {
            if *ctx.shutdown_rx.borrow() {
                return Ok(());
            }
            let bytes = serde_json::to_vec(env).map_err(|err| {
                MembusError::Protocol(format!("encode mesh envelope failed: {err}"))
            })?;
            if bytes.len() > ctx.max_payload_bytes {
                return Err(MembusError::FrameTooLarge {
                    len: bytes.len(),
                    max_len: ctx.max_payload_bytes,
                });
            }
            ctx.session
                .put(&ctx.pub_ke, bytes)
                .await
                .map_err(|err| MembusError::Io(format!("zenoh publish failed: {err}")))?;
        }

        if scanned_through > after_seq {
            ctx.published_seq.store(scanned_through, Ordering::Release);
            let st = Arc::clone(&ctx.storage);
            let _ = tokio::task::spawn_blocking(move || {
                persist_published_seq(st.as_ref(), scanned_through)
            })
            .await;
        } else {
            break;
        }
    }
    Ok(())
}

impl MeshNode {
    /// Immediately drain and publish any settled `CF_LOG` entries (serialized with background publisher).
    pub async fn flush_outbound(&self) -> Result<u64, MembusError> {
        let ctx = PublisherContext {
            session: self.session.clone(),
            pub_ke: format!("{}/delta/{}", self.cluster_prefix, self.node_id),
            node_id: self.node_id.clone(),
            storage: Arc::clone(&self.storage),
            published_seq: Arc::clone(&self.published_seq),
            publish_lock: Arc::clone(&self.publish_lock),
            max_batch: self.max_batch_entries,
            max_payload_bytes: self.max_mesh_payload_bytes,
            shutdown_rx: self.shutdown_rx.clone(),
        };
        drain_and_publish_once(&ctx).await?;
        Ok(self.published_seq())
    }
}
