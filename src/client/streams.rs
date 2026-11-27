use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::index::{BfsOptions, NeighborQueryOptions, OrderedQueryOptions};
use crate::protocol::{encode_request, Frame, FrameKind, Request, Response};
use crate::types::{
    BfsResult, MembusError, NeighborPage, OrderedIndexPage, ScanOptions, ScanPage, SignalEvent,
    StoredRecord,
};

use super::core::{enqueue_cleanup_frame, Client, ClientState, StreamDispatchEntry};

impl Client {
    pub async fn open_snapshot(&self) -> Result<ClientSnapshot, MembusError> {
        let resp = self.call(Request::OpenSnapshot).await?;
        match resp {
            Response::SnapshotOpened {
                snapshot_id,
                snapshot_seq,
            } => Ok(ClientSnapshot {
                client: self.clone(),
                snapshot_id,
                snapshot_seq,
                closed: false,
            }),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for open_snapshot: {other:?}"
            ))),
        }
    }

    pub async fn subscribe_each_change(
        &self,
        namespace: &str,
        key_prefix: &[u8],
        epoch: u64,
        since_seq: u64,
    ) -> Result<ClientSubscription, MembusError> {
        self.open_subscription(Request::SubscribeEachChange {
            namespace: namespace.to_owned(),
            key_prefix: key_prefix.to_vec(),
            epoch,
            since_seq,
        })
        .await
    }

    pub async fn subscribe_live(
        &self,
        namespace: &str,
        key_prefix: &[u8],
        max_pending: Option<u32>,
    ) -> Result<ClientSubscription, MembusError> {
        self.open_subscription(Request::SubscribeLive {
            namespace: namespace.to_owned(),
            key_prefix: key_prefix.to_vec(),
            max_pending,
        })
        .await
    }

    async fn open_subscription(&self, req: Request) -> Result<ClientSubscription, MembusError> {
        let encoded = encode_request(&req)?;
        self.check_frame_size(&encoded)?;

        let stream_id = self.alloc_stream_id();
        let (rpc_tx, rpc_rx) = oneshot::channel();
        let (ev_tx, ev_rx) = mpsc::channel(self.inner.subscription_capacity);
        let terminal_error = Arc::new(Mutex::new(None));

        {
            let mut state = self.inner.state.lock();
            if state.closed {
                return Err(MembusError::Closed);
            }
            state.active_streams.insert(
                stream_id,
                StreamDispatchEntry {
                    tx: ev_tx,
                    terminal_error: Arc::clone(&terminal_error),
                },
            );
            state.pending_rpcs.insert(stream_id, rpc_tx);
        }

        let mut handshake_guard = PendingSubGuard {
            state: Arc::clone(&self.inner.state),
            outbound_tx: self.inner.outbound_tx.clone(),
            stream_id,
            active: true,
        };

        let frame = Frame::new(FrameKind::Request, stream_id, encoded);
        if self.inner.outbound_tx.send(frame).await.is_err() {
            return Err(MembusError::Closed);
        }

        let resp = match rpc_rx.await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(MembusError::Closed),
        };

        match resp {
            Response::Subscribed { epoch, start_seq } => {
                handshake_guard.active = false;
                Ok(ClientSubscription {
                    stream_id,
                    epoch,
                    start_seq,
                    rx: ev_rx,
                    terminal_error,
                    outbound_tx: self.inner.outbound_tx.clone(),
                    state: Arc::clone(&self.inner.state),
                    cancelled: false,
                })
            }
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for subscribe: {other:?}"
            ))),
        }
    }
}

struct PendingSubGuard {
    state: Arc<Mutex<ClientState>>,
    outbound_tx: mpsc::Sender<Frame>,
    stream_id: u64,
    active: bool,
}

impl Drop for PendingSubGuard {
    fn drop(&mut self) {
        if self.active {
            {
                let mut state = self.state.lock();
                state.pending_rpcs.remove(&self.stream_id);
                state.active_streams.remove(&self.stream_id);
            }
            enqueue_cleanup_frame(
                &self.outbound_tx,
                Frame::new(FrameKind::Cancel, self.stream_id, Bytes::new()),
            );
        }
    }
}

/// Server-pinned MVCC snapshot handle bound to a [`Client`] connection.
pub struct ClientSnapshot {
    client: Client,
    snapshot_id: u64,
    snapshot_seq: u64,
    closed: bool,
}

impl ClientSnapshot {
    pub fn snapshot_id(&self) -> u64 {
        self.snapshot_id
    }

    pub fn snapshot_seq(&self) -> u64 {
        self.snapshot_seq
    }

    pub async fn get(
        &self,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        self.client
            .get_with_snapshot(namespace, key, Some(self.snapshot_id))
            .await
    }

    pub async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.client
            .get_many_with_snapshot(namespace, keys, Some(self.snapshot_id))
            .await
    }

    pub async fn scan_prefix(
        &self,
        namespace: &str,
        prefix: &[u8],
        options: ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.client
            .scan_prefix(namespace, prefix, options, Some(self.snapshot_id))
            .await
    }

    pub async fn scan_range(
        &self,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.client
            .scan_range(namespace, start_key, end_key, options, Some(self.snapshot_id))
            .await
    }

    pub async fn query_ordered_index(
        &self,
        namespace: &str,
        index_id: &str,
        options: OrderedQueryOptions,
    ) -> Result<OrderedIndexPage, MembusError> {
        self.client
            .query_ordered_index(namespace, index_id, options, Some(self.snapshot_id))
            .await
    }

    pub async fn query_neighbors(
        &self,
        namespace: &str,
        index_id: &str,
        node_id: &[u8],
        options: NeighborQueryOptions,
    ) -> Result<NeighborPage, MembusError> {
        self.client
            .query_neighbors(namespace, index_id, node_id, options, Some(self.snapshot_id))
            .await
    }

    pub async fn bfs_traverse(
        &self,
        namespace: &str,
        index_id: &str,
        seed_node: &[u8],
        options: BfsOptions,
    ) -> Result<BfsResult, MembusError> {
        self.client
            .bfs_traverse(
                namespace,
                index_id,
                seed_node,
                options,
                Some(self.snapshot_id),
            )
            .await
    }

    pub async fn close(mut self) -> Result<(), MembusError> {
        self.closed = true;
        self.client.close_snapshot_id(self.snapshot_id).await
    }
}

impl Drop for ClientSnapshot {
    fn drop(&mut self) {
        if !self.closed && !self.client.is_closed() {
            let stream_id = self.client.alloc_stream_id();
            if let Ok(payload) = encode_request(&Request::CloseSnapshot {
                snapshot_id: self.snapshot_id,
            }) {
                let frame = Frame::new(FrameKind::Request, stream_id, payload);
                enqueue_cleanup_frame(&self.client.inner.outbound_tx, frame);
            }
        }
    }
}

/// Multiplexed client handle to an active server push subscription stream.
pub struct ClientSubscription {
    stream_id: u64,
    epoch: u64,
    start_seq: u64,
    rx: mpsc::Receiver<Result<SignalEvent, MembusError>>,
    terminal_error: Arc<Mutex<Option<MembusError>>>,
    outbound_tx: mpsc::Sender<Frame>,
    state: Arc<Mutex<ClientState>>,
    cancelled: bool,
}

impl ClientSubscription {
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn start_seq(&self) -> u64 {
        self.start_seq
    }

    pub async fn next_event(&mut self) -> Result<Option<SignalEvent>, MembusError> {
        if let Some(err) = self.terminal_error.lock().take() {
            return Err(err);
        }
        match self.rx.recv().await {
            Some(Ok(ev)) => Ok(Some(ev)),
            Some(Err(err)) => Err(err),
            None => {
                if let Some(err) = self.terminal_error.lock().take() {
                    Err(err)
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub async fn cancel(mut self) -> Result<(), MembusError> {
        self.cancelled = true;
        self.state.lock().active_streams.remove(&self.stream_id);
        let frame = Frame::new(FrameKind::Cancel, self.stream_id, Bytes::new());
        let _ = self.outbound_tx.send(frame).await;
        Ok(())
    }
}

impl Drop for ClientSubscription {
    fn drop(&mut self) {
        if !self.cancelled {
            self.state.lock().active_streams.remove(&self.stream_id);
            let frame = Frame::new(FrameKind::Cancel, self.stream_id, Bytes::new());
            enqueue_cleanup_frame(&self.outbound_tx, frame);
        }
    }
}
