mod catchup;
mod envelope;
mod publisher;
mod reconcile;
mod state;
mod subscriber;
mod watermarks;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex as FastMutex;
use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use crate::mesh::protocol::MeshPeerWatermark;
use crate::search::SearchEngine;
use crate::storage::StorageEngine;
use crate::types::MembusError;

/// Compute a deterministic 32-byte XOR support-digest over `(key, value_sha256)` for all
/// keys in `(namespace, key_prefix)` under an O(1) MVCC snapshot.
pub use crate::storage::compute_namespace_xor_digest;
pub use state::{
    sanitize_for_mesh, validate_mesh_listen_endpoints, CatchupStats, MeshConfig, MeshNode,
    ReconcileReport,
};

use catchup::{spawn_catchup_queryable_task, CatchupQueryableContext};
use publisher::{spawn_publisher_task, PublisherContext};
use reconcile::{spawn_digest_queryable_task, DigestQueryableContext};
use state::{build_zenoh_config, validate_mesh_config};
use subscriber::{spawn_subscriber_task, SubscriberContext};
use watermarks::{read_or_init_published_seq, read_peer_watermark};

struct MeshTaskHandles {
    storage: Arc<dyn StorageEngine>,
    search: Option<Arc<SearchEngine>>,
    published_seq: Arc<AtomicU64>,
    publish_lock: Arc<AsyncMutex<()>>,
    watermark_lock: Arc<FastMutex<()>>,
    shutdown_rx: watch::Receiver<bool>,
}

impl MeshNode {
    /// Open an embedded Zenoh session and launch the off-path publisher, subscriber,
    /// gap catch-up queryable, and XOR support-digest queryable tasks.
    pub async fn start(
        config: MeshConfig,
        storage: Arc<dyn StorageEngine>,
        search: Option<Arc<SearchEngine>>,
    ) -> Result<Self, MembusError> {
        validate_mesh_config(&config)?;

        let zenoh_cfg = build_zenoh_config(&config)?;
        let session = zenoh::open(zenoh_cfg)
            .await
            .map_err(|err| MembusError::Storage(format!("zenoh open failed: {err}")))?;

        let initial_pub_seq = read_or_init_published_seq(storage.as_ref())?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handles = MeshTaskHandles {
            storage,
            search,
            published_seq: Arc::new(AtomicU64::new(initial_pub_seq)),
            publish_lock: Arc::new(AsyncMutex::new(())),
            watermark_lock: Arc::new(FastMutex::new(())),
            shutdown_rx,
        };

        let tasks = match spawn_mesh_tasks(&session, &config, &handles).await {
            Ok(tasks) => tasks,
            Err(err) => {
                let _ = shutdown_tx.send(true);
                return Err(err);
            }
        };

        Ok(Self {
            node_id: config.node_id,
            cluster_prefix: config.cluster_prefix,
            query_timeout: Duration::from_millis(config.query_timeout_ms.max(100)),
            max_batch_entries: config.max_batch_entries,
            max_mesh_payload_bytes: config.max_mesh_payload_bytes,
            max_deltas_per_envelope: config.max_deltas_per_envelope,
            max_catchup_rounds: config.max_catchup_rounds,
            storage: handles.storage,
            search: handles.search,
            session,
            published_seq: handles.published_seq,
            publish_lock: handles.publish_lock,
            watermark_lock: handles.watermark_lock,
            shutdown_tx,
            shutdown_rx: handles.shutdown_rx,
            tasks,
        })
    }

    /// Read the persisted peer watermark from `CF_META`.
    pub fn peer_watermark(&self, peer_id: &str) -> Result<Option<MeshPeerWatermark>, MembusError> {
        read_peer_watermark(self.storage.as_ref(), peer_id)
    }

    /// Gracefully stop background tasks and close the embedded Zenoh session.
    pub async fn shutdown(self) -> Result<(), MembusError> {
        let _ = self.shutdown_tx.send(true);
        for task in self.tasks {
            let _ = task.await;
        }
        self.session
            .close()
            .await
            .map_err(|err| MembusError::Io(format!("zenoh close failed: {err}")))?;
        Ok(())
    }
}

async fn spawn_mesh_tasks(
    session: &zenoh::Session,
    config: &MeshConfig,
    handles: &MeshTaskHandles,
) -> Result<Vec<JoinHandle<()>>, MembusError> {
    let delta_sub_key = format!("{}/delta/*", config.cluster_prefix);
    let catchup_key = format!("{}/catchup/{}", config.cluster_prefix, config.node_id);
    let digest_key = format!("{}/digest/{}", config.cluster_prefix, config.node_id);

    // Declare all Zenoh endpoints BEFORE spawning any Tokio background tasks so a partial
    // startup failure never leaves detached background tasks running.
    let subscriber = session
        .declare_subscriber(&delta_sub_key)
        .await
        .map_err(|err| MembusError::Storage(format!("zenoh declare_subscriber failed: {err}")))?;
    let catchup_q = session
        .declare_queryable(&catchup_key)
        .await
        .map_err(|err| MembusError::Storage(format!("zenoh catchup queryable failed: {err}")))?;
    let digest_q = session
        .declare_queryable(&digest_key)
        .await
        .map_err(|err| MembusError::Storage(format!("zenoh digest queryable failed: {err}")))?;

    let sub_task = spawn_subscriber_task(
        subscriber,
        SubscriberContext {
            node_id: config.node_id.clone(),
            delta_prefix: format!("{}/delta/", config.cluster_prefix),
            max_payload: config.max_mesh_payload_bytes,
            max_deltas: config.max_deltas_per_envelope,
            storage: Arc::clone(&handles.storage),
            search: handles.search.clone(),
            watermark_lock: Arc::clone(&handles.watermark_lock),
            shutdown_rx: handles.shutdown_rx.clone(),
        },
    );

    let catchup_task = spawn_catchup_queryable_task(
        catchup_q,
        CatchupQueryableContext {
            node_id: config.node_id.clone(),
            catchup_key,
            max_payload: config.max_mesh_payload_bytes,
            storage: Arc::clone(&handles.storage),
            watermark_lock: Arc::clone(&handles.watermark_lock),
            shutdown_rx: handles.shutdown_rx.clone(),
        },
    );

    let digest_task = spawn_digest_queryable_task(
        digest_q,
        DigestQueryableContext {
            node_id: config.node_id.clone(),
            digest_key,
            max_payload: config.max_mesh_payload_bytes,
            storage: Arc::clone(&handles.storage),
            shutdown_rx: handles.shutdown_rx.clone(),
        },
    );

    let pub_task = spawn_publisher_task(
        PublisherContext {
            session: session.clone(),
            pub_ke: format!("{}/delta/{}", config.cluster_prefix, config.node_id),
            node_id: config.node_id.clone(),
            storage: Arc::clone(&handles.storage),
            published_seq: Arc::clone(&handles.published_seq),
            publish_lock: Arc::clone(&handles.publish_lock),
            max_batch: config.max_batch_entries,
            max_payload_bytes: config.max_mesh_payload_bytes,
            shutdown_rx: handles.shutdown_rx.clone(),
        },
        Duration::from_millis(config.publish_poll_interval_ms.max(5)),
    );

    Ok(vec![sub_task, catchup_task, digest_task, pub_task])
}
