use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};

use crate::index::{BfsOptions, NeighborQueryOptions, OrderedQueryOptions};
use crate::protocol::{
    decode_response, decode_signal_event, decode_stream_end, encode_request, read_frame,
    run_batched_frame_writer, Frame, FrameKind, Request, Response, DEFAULT_MAX_FRAME_BODY,
};
use crate::transport::tls::connect_tcp_tls;
use crate::transport::{connect_endpoint, Endpoint, TlsClientConfig};
use crate::types::{
    Ack, BfsResult, CollectionDescriptor, CommitResult, ExpectedVersion, HealthStatus,
    IndexMutations, IndexedMutation, LeaseGrant, LeaseInfo, MembusError, NeighborPage,
    OrderedIndexPage, ScanOptions, ScanPage, SearchDocument, SearchLogOp, SearchPage, SearchQuery,
    SignalEvent, StoredRecord, VersionGuard, WriteMutation,
};

const CLIENT_OUTBOUND_QUEUE: usize = 8192;
const SUBSCRIPTION_CHANNEL_CAPACITY: usize = 1024;

struct StreamDispatchEntry {
    tx: mpsc::Sender<Result<SignalEvent, MembusError>>,
    terminal_error: Arc<Mutex<Option<MembusError>>>,
}

struct ClientState {
    closed: bool,
    pending_rpcs: HashMap<u64, oneshot::Sender<Result<Response, MembusError>>>,
    pending_pings: HashMap<u64, oneshot::Sender<Result<(), MembusError>>>,
    active_streams: HashMap<u64, StreamDispatchEntry>,
}

impl ClientState {
    fn new() -> Self {
        Self {
            closed: false,
            pending_rpcs: HashMap::new(),
            pending_pings: HashMap::new(),
            active_streams: HashMap::new(),
        }
    }

    fn fail_all(&mut self) {
        self.closed = true;
        for (_id, tx) in self.pending_rpcs.drain() {
            let _ = tx.send(Err(MembusError::Closed));
        }
        for (_id, tx) in self.pending_pings.drain() {
            let _ = tx.send(Err(MembusError::Closed));
        }
        for (_id, entry) in self.active_streams.drain() {
            let mut slot = entry.terminal_error.lock();
            if slot.is_none() {
                *slot = Some(MembusError::Closed);
            }
        }
    }
}

struct ClientInner {
    outbound_tx: mpsc::Sender<Frame>,
    next_stream_id: AtomicU64,
    max_frame_body: usize,
    subscription_capacity: usize,
    state: Arc<Mutex<ClientState>>,
}

/// Pipelined, multiplexed async `membus/2` client.
///
/// Cheaply cloneable: hundreds or thousands of concurrent async tasks can share one
/// underlying TCP, TLS/mTLS TCP, or Windows Named Pipe connection with full RPC pipelining
/// and live subscription push streams multiplexed by `stream_id`.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    pub async fn connect(endpoint: &Endpoint) -> Result<Self, MembusError> {
        let stream = connect_endpoint(endpoint).await?;
        Ok(Self::from_stream(stream, DEFAULT_MAX_FRAME_BODY))
    }

    pub async fn connect_tcp(addr: SocketAddr) -> Result<Self, MembusError> {
        Self::connect(&Endpoint::Tcp(addr)).await
    }

    pub async fn connect_tcp_tls(
        addr: SocketAddr,
        tls_cfg: &TlsClientConfig,
    ) -> Result<Self, MembusError> {
        let tls_stream = connect_tcp_tls(addr, tls_cfg).await?;
        Ok(Self::from_stream(tls_stream, DEFAULT_MAX_FRAME_BODY))
    }

    pub async fn connect_named_pipe(pipe_name: &str) -> Result<Self, MembusError> {
        Self::connect(&Endpoint::NamedPipe(pipe_name.to_owned())).await
    }

    pub fn from_stream<S>(stream: S, max_frame_body: usize) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::from_stream_with_capacity(stream, max_frame_body, SUBSCRIPTION_CHANNEL_CAPACITY)
    }

    pub fn from_stream_with_capacity<S>(
        stream: S,
        max_frame_body: usize,
        subscription_capacity: usize,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Frame>(CLIENT_OUTBOUND_QUEUE);

        let state = Arc::new(Mutex::new(ClientState::new()));
        let (shutdown_tx, mut writer_shutdown_rx) = watch::channel(false);
        let mut reader_shutdown_rx = shutdown_tx.subscribe();

        let writer_state = Arc::clone(&state);
        let writer_shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = wait_shutdown(&mut writer_shutdown_rx) => {}
                _ = run_batched_frame_writer(&mut writer, &mut outbound_rx, max_frame_body) => {}
            }
            writer_state.lock().fail_all();
            let _ = writer_shutdown_tx.send(true);
        });

        let reader_state = Arc::clone(&state);
        let reader_shutdown_tx = shutdown_tx;
        let reader_outbound = outbound_tx.downgrade();
        tokio::spawn(async move {
            let mut read_buf = BytesMut::with_capacity(64 * 1024);
            loop {
                let frame_opt = tokio::select! {
                    biased;
                    _ = wait_shutdown(&mut reader_shutdown_rx) => None,
                    res = read_frame(&mut reader, &mut read_buf, max_frame_body) => {
                        res.ok().flatten()
                    }
                };
                let Some(frame) = frame_opt else {
                    break;
                };

                match frame.kind {
                    FrameKind::Response => {
                        let decoded = decode_response(&frame.payload);
                        let rpc_tx = reader_state.lock().pending_rpcs.remove(&frame.stream_id);
                        if let Some(tx) = rpc_tx {
                            let _ = tx.send(decoded);
                        }
                    }
                    FrameKind::Pong => {
                        let ping_tx = reader_state.lock().pending_pings.remove(&frame.stream_id);
                        if let Some(tx) = ping_tx {
                            let _ = tx.send(Ok(()));
                        }
                    }
                    FrameKind::StreamEvent => {
                        let decoded = decode_signal_event(&frame.payload);
                        let lagged_stream = {
                            let mut guard = reader_state.lock();
                            if let Some(entry) = guard.active_streams.get(&frame.stream_id) {
                                match entry.tx.try_send(decoded) {
                                    Ok(()) => false,
                                    Err(TrySendError::Full(_)) => {
                                        if let Some(removed) =
                                            guard.active_streams.remove(&frame.stream_id)
                                        {
                                            *removed.terminal_error.lock() =
                                                Some(MembusError::Busy {
                                                    retry_after_ms: 0,
                                                    reason: format!(
                                                        "client subscription stream {} buffer overflowed",
                                                        frame.stream_id
                                                    ),
                                                });
                                        }
                                        true
                                    }
                                    Err(TrySendError::Closed(_)) => {
                                        guard.active_streams.remove(&frame.stream_id);
                                        true
                                    }
                                }
                            } else {
                                false
                            }
                        };
                        if lagged_stream {
                            if let Some(tx) = reader_outbound.upgrade() {
                                let _ = tx.try_send(Frame::new(
                                    FrameKind::Cancel,
                                    frame.stream_id,
                                    Bytes::new(),
                                ));
                            }
                        }
                    }
                    FrameKind::StreamEnd => {
                        let end_err = decode_stream_end(&frame.payload);
                        let removed = reader_state.lock().active_streams.remove(&frame.stream_id);
                        if let Some(entry) = removed {
                            let err_opt = match end_err {
                                Ok(Some(err)) => Some(err),
                                Err(proto_err) => Some(proto_err),
                                Ok(None) => None,
                            };
                            if let Some(err) = err_opt {
                                if entry.tx.try_send(Err(err.clone())).is_err() {
                                    *entry.terminal_error.lock() = Some(err);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            reader_state.lock().fail_all();
            let _ = reader_shutdown_tx.send(true);
        });

        Self {
            inner: Arc::new(ClientInner {
                outbound_tx,
                next_stream_id: AtomicU64::new(1),
                max_frame_body,
                subscription_capacity: subscription_capacity.max(1),
                state,
            }),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state.lock().closed
    }

    pub fn pending_rpc_count(&self) -> usize {
        self.inner.state.lock().pending_rpcs.len()
    }

    pub async fn authenticate(&self, token: &str) -> Result<String, MembusError> {
        let resp = self
            .call(Request::Authenticate {
                token: token.to_owned(),
            })
            .await?;
        match resp {
            Response::Authenticated { principal_id } => Ok(principal_id),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for authenticate: {other:?}"
            ))),
        }
    }

    pub async fn ping(&self) -> Result<(), MembusError> {
        let stream_id = self.alloc_stream_id();
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.inner.state.lock();
            if state.closed {
                return Err(MembusError::Closed);
            }
            state.pending_pings.insert(stream_id, tx);
        }
        let mut guard = PendingPingGuard {
            state: Arc::clone(&self.inner.state),
            stream_id,
            active: true,
        };

        let frame = Frame::new(FrameKind::Ping, stream_id, Bytes::new());
        if self.inner.outbound_tx.send(frame).await.is_err() {
            return Err(MembusError::Closed);
        }
        let res = rx.await.map_err(|_| MembusError::Closed)?;
        guard.active = false;
        res
    }

    pub async fn get(
        &self,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        self.get_with_snapshot(namespace, key, None).await
    }

    pub async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.get_many_with_snapshot(namespace, keys, None).await
    }

    pub async fn put(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.cas(namespace, key, value, ack, ExpectedVersion::Any)
            .await
    }

    pub async fn cas(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::Put {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                value: value.to_vec(),
                ack,
                expected,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put/cas: {other:?}"
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        indexes: Option<IndexMutations>,
        search_op: Option<SearchLogOp>,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::PutIndexed {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                value: value.to_vec(),
                ack,
                expected,
                indexes,
                search_op,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put_indexed: {other:?}"
            ))),
        }
    }

    pub async fn remove(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::Remove {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                ack,
                expected,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for remove: {other:?}"
            ))),
        }
    }

    pub async fn remove_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        search_op: Option<SearchLogOp>,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::RemoveIndexed {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                ack,
                expected,
                search_op,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for remove_indexed: {other:?}"
            ))),
        }
    }

    pub async fn commit_tx(
        &self,
        guards: Vec<VersionGuard>,
        mutations: Vec<WriteMutation>,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::CommitTx {
                ack,
                guards,
                mutations,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for commit_tx: {other:?}"
            ))),
        }
    }

    pub async fn commit_indexed_tx(
        &self,
        guards: Vec<VersionGuard>,
        mutations: Vec<IndexedMutation>,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::CommitIndexedTx {
                ack,
                guards,
                mutations,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for commit_indexed_tx: {other:?}"
            ))),
        }
    }

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

    pub async fn scan_prefix(
        &self,
        namespace: &str,
        prefix: &[u8],
        options: ScanOptions,
        snapshot_id: Option<u64>,
    ) -> Result<ScanPage, MembusError> {
        let resp = self
            .call(Request::ScanPrefix {
                namespace: namespace.to_owned(),
                prefix: prefix.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Scan(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for scan_prefix: {other:?}"
            ))),
        }
    }

    pub async fn scan_range(
        &self,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: ScanOptions,
        snapshot_id: Option<u64>,
    ) -> Result<ScanPage, MembusError> {
        let resp = self
            .call(Request::ScanRange {
                namespace: namespace.to_owned(),
                start_key: start_key.to_vec(),
                end_key: end_key.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Scan(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for scan_range: {other:?}"
            ))),
        }
    }

    pub async fn query_ordered_index(
        &self,
        namespace: &str,
        index_id: &str,
        options: OrderedQueryOptions,
        snapshot_id: Option<u64>,
    ) -> Result<OrderedIndexPage, MembusError> {
        let resp = self
            .call(Request::QueryOrderedIndex {
                namespace: namespace.to_owned(),
                index_id: index_id.to_owned(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::OrderedIndex(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for query_ordered_index: {other:?}"
            ))),
        }
    }

    pub async fn query_neighbors(
        &self,
        namespace: &str,
        index_id: &str,
        node_id: &[u8],
        options: NeighborQueryOptions,
        snapshot_id: Option<u64>,
    ) -> Result<NeighborPage, MembusError> {
        let resp = self
            .call(Request::QueryNeighbors {
                namespace: namespace.to_owned(),
                index_id: index_id.to_owned(),
                node_id: node_id.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Neighbors(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for query_neighbors: {other:?}"
            ))),
        }
    }

    pub async fn bfs_traverse(
        &self,
        namespace: &str,
        index_id: &str,
        seed_node: &[u8],
        options: BfsOptions,
        snapshot_id: Option<u64>,
    ) -> Result<BfsResult, MembusError> {
        let resp = self
            .call(Request::BfsTraverse {
                namespace: namespace.to_owned(),
                index_id: index_id.to_owned(),
                seed_node: seed_node.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Bfs(res) => Ok(res),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for bfs_traverse: {other:?}"
            ))),
        }
    }

    pub async fn put_blob(&self, digest: &[u8], data: &[u8], ack: Ack) -> Result<(), MembusError> {
        let resp = self
            .call(Request::PutBlob {
                digest: digest.to_vec(),
                data: data.to_vec(),
                ack,
            })
            .await?;
        match resp {
            Response::Unit => Ok(()),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put_blob: {other:?}"
            ))),
        }
    }

    pub async fn get_blob(&self, digest: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        let resp = self
            .call(Request::GetBlob {
                digest: digest.to_vec(),
            })
            .await?;
        match resp {
            Response::Blob(b) => Ok(b),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_blob: {other:?}"
            ))),
        }
    }

    pub async fn put_cas_blob(
        &self,
        namespace: &str,
        data: &[u8],
        ack: Ack,
    ) -> Result<[u8; 32], MembusError> {
        let resp = self
            .call(Request::PutCasBlob {
                namespace: namespace.to_owned(),
                data: data.to_vec(),
                ack,
            })
            .await?;
        match resp {
            Response::CasBlobDigest(digest) => Ok(digest),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put_cas_blob: {other:?}"
            ))),
        }
    }

    pub async fn get_cas_blob(
        &self,
        namespace: &str,
        sha256: [u8; 32],
    ) -> Result<Option<Vec<u8>>, MembusError> {
        let resp = self
            .call(Request::GetCasBlob {
                namespace: namespace.to_owned(),
                sha256,
            })
            .await?;
        match resp {
            Response::Blob(data) => Ok(data),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_cas_blob: {other:?}"
            ))),
        }
    }

    pub async fn has_cas_blob(
        &self,
        namespace: &str,
        sha256: [u8; 32],
    ) -> Result<bool, MembusError> {
        let resp = self
            .call(Request::HasCasBlob {
                namespace: namespace.to_owned(),
                sha256,
            })
            .await?;
        match resp {
            Response::Bool(exists) => Ok(exists),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for has_cas_blob: {other:?}"
            ))),
        }
    }

    pub async fn acquire_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<LeaseGrant, MembusError> {
        let resp = self
            .call(Request::AcquireLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
                holder: holder.to_owned(),
                ttl_ms,
            })
            .await?;
        match resp {
            Response::Lease(g) => Ok(g),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for acquire_lease: {other:?}"
            ))),
        }
    }

    pub async fn renew_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        generation: u64,
        token: &str,
        ttl_ms: u64,
    ) -> Result<LeaseGrant, MembusError> {
        let resp = self
            .call(Request::RenewLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
                holder: holder.to_owned(),
                generation,
                token: token.to_owned(),
                ttl_ms,
            })
            .await?;
        match resp {
            Response::Lease(g) => Ok(g),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for renew_lease: {other:?}"
            ))),
        }
    }

    pub async fn release_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        generation: u64,
        token: &str,
    ) -> Result<(), MembusError> {
        let resp = self
            .call(Request::ReleaseLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
                holder: holder.to_owned(),
                generation,
                token: token.to_owned(),
            })
            .await?;
        match resp {
            Response::Unit => Ok(()),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for release_lease: {other:?}"
            ))),
        }
    }

    pub async fn inspect_lease(
        &self,
        namespace: &str,
        resource: &str,
    ) -> Result<Option<LeaseInfo>, MembusError> {
        let resp = self
            .call(Request::InspectLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
            })
            .await?;
        match resp {
            Response::OptLease(g) => Ok(g),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for inspect_lease: {other:?}"
            ))),
        }
    }

    pub async fn validate_lease(
        &self,
        namespace: &str,
        resource: &str,
        holder: &str,
        generation: u64,
        token: &str,
    ) -> Result<bool, MembusError> {
        let resp = self
            .call(Request::ValidateLease {
                namespace: namespace.to_owned(),
                resource: resource.to_owned(),
                holder: holder.to_owned(),
                generation,
                token: token.to_owned(),
            })
            .await?;
        match resp {
            Response::Bool(v) => Ok(v),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for validate_lease: {other:?}"
            ))),
        }
    }

    pub async fn register_collection(
        &self,
        expected_version: Option<u32>,
        descriptor: CollectionDescriptor,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::RegisterCollection {
                expected_version,
                descriptor,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for register_collection: {other:?}"
            ))),
        }
    }

    pub async fn get_collection(
        &self,
        namespace: &str,
    ) -> Result<Option<CollectionDescriptor>, MembusError> {
        let resp = self
            .call(Request::GetCollection {
                namespace: namespace.to_owned(),
            })
            .await?;
        match resp {
            Response::OptCollection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_collection: {other:?}"
            ))),
        }
    }

    pub async fn list_collections(&self) -> Result<Vec<CollectionDescriptor>, MembusError> {
        let resp = self.call(Request::ListCollections).await?;
        match resp {
            Response::Collections(list) => Ok(list),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for list_collections: {other:?}"
            ))),
        }
    }

    pub async fn get_collection_version(
        &self,
        namespace: &str,
        version: u32,
    ) -> Result<Option<CollectionDescriptor>, MembusError> {
        let resp = self
            .call(Request::GetCollectionVersion {
                namespace: namespace.to_owned(),
                version,
            })
            .await?;
        match resp {
            Response::OptCollection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_collection_version: {other:?}"
            ))),
        }
    }

    pub async fn begin_migration(
        &self,
        namespace: &str,
        expected_version: u32,
        target_version: u32,
        target_sha256: [u8; 32],
        target_schema_bytes: Vec<u8>,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::BeginMigration {
                namespace: namespace.to_owned(),
                expected_version,
                target_version,
                target_sha256,
                target_schema_bytes,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for begin_migration: {other:?}"
            ))),
        }
    }

    pub async fn commit_migration(
        &self,
        namespace: &str,
        target_version: u32,
        verified_through: u64,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::CommitMigration {
                namespace: namespace.to_owned(),
                target_version,
                verified_through,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for commit_migration: {other:?}"
            ))),
        }
    }

    pub async fn abort_migration(
        &self,
        namespace: &str,
        target_version: u32,
    ) -> Result<CollectionDescriptor, MembusError> {
        let resp = self
            .call(Request::AbortMigration {
                namespace: namespace.to_owned(),
                target_version,
            })
            .await?;
        match resp {
            Response::Collection(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for abort_migration: {other:?}"
            ))),
        }
    }

    pub async fn index_document(
        &self,
        doc: SearchDocument,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self.call(Request::IndexDocument { doc, ack }).await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for index_document: {other:?}"
            ))),
        }
    }

    pub async fn delete_search_document(
        &self,
        namespace: &str,
        doc_id: &str,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::DeleteSearchDocument {
                namespace: namespace.to_owned(),
                doc_id: doc_id.to_owned(),
                ack,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for delete_search_document: {other:?}"
            ))),
        }
    }

    pub async fn revoke_search_prefix(
        &self,
        namespace: &str,
        prefix: &str,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::RevokeSearchPrefix {
                namespace: namespace.to_owned(),
                prefix: prefix.to_owned(),
                ack,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for revoke_search_prefix: {other:?}"
            ))),
        }
    }

    pub async fn search(&self, query: SearchQuery) -> Result<SearchPage, MembusError> {
        let resp = self.call(Request::Search(query)).await?;
        match resp {
            Response::Search(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for search: {other:?}"
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

    pub async fn checkpoint(&self, dest_dir: &str) -> Result<u64, MembusError> {
        let resp = self
            .call(Request::Checkpoint {
                dest_dir: dest_dir.to_owned(),
            })
            .await?;
        match resp {
            Response::Checkpoint { seq } => Ok(seq),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for checkpoint: {other:?}"
            ))),
        }
    }

    pub async fn health(&self) -> Result<HealthStatus, MembusError> {
        let resp = self.call(Request::Health).await?;
        match resp {
            Response::Health(status) => Ok(status),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for health: {other:?}"
            ))),
        }
    }

    /// Query extended v2 server health including active snapshots, active streams,
    /// and deferred secondary search synchronization gauges.
    pub async fn health_v2(&self) -> Result<HealthStatus, MembusError> {
        let resp = self.call(Request::HealthV2).await?;
        match resp {
            Response::HealthV2(status) => Ok(status),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for health_v2: {other:?}"
            ))),
        }
    }

    async fn get_with_snapshot(
        &self,
        namespace: &str,
        key: &[u8],
        snapshot_id: Option<u64>,
    ) -> Result<Option<StoredRecord>, MembusError> {
        let resp = self
            .call(Request::Get {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Get(rec) => Ok(rec),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get: {other:?}"
            ))),
        }
    }

    async fn get_many_with_snapshot(
        &self,
        namespace: &str,
        keys: Vec<Vec<u8>>,
        snapshot_id: Option<u64>,
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        let resp = self
            .call(Request::GetMany {
                namespace: namespace.to_owned(),
                keys,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::GetMany(recs) => Ok(recs),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_many: {other:?}"
            ))),
        }
    }

    async fn close_snapshot_id(&self, snapshot_id: u64) -> Result<(), MembusError> {
        let resp = self.call(Request::CloseSnapshot { snapshot_id }).await?;
        match resp {
            Response::Unit => Ok(()),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for close_snapshot: {other:?}"
            ))),
        }
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

    async fn call(&self, req: Request) -> Result<Response, MembusError> {
        let encoded = encode_request(&req)?;
        self.check_frame_size(&encoded)?;

        let stream_id = self.alloc_stream_id();
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.inner.state.lock();
            if state.closed {
                return Err(MembusError::Closed);
            }
            state.pending_rpcs.insert(stream_id, tx);
        }
        let mut guard = PendingRpcGuard {
            state: Arc::clone(&self.inner.state),
            stream_id,
            active: true,
        };

        let frame = Frame::new(FrameKind::Request, stream_id, encoded);
        if self.inner.outbound_tx.send(frame).await.is_err() {
            return Err(MembusError::Closed);
        }

        let res = rx.await.map_err(|_| MembusError::Closed)?;
        guard.active = false;
        res
    }

    fn check_frame_size(&self, payload: &Bytes) -> Result<(), MembusError> {
        if payload.len() > self.inner.max_frame_body {
            return Err(MembusError::FrameTooLarge {
                len: payload.len(),
                max_len: self.inner.max_frame_body,
            });
        }
        Ok(())
    }

    fn alloc_stream_id(&self) -> u64 {
        self.inner.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }
}

struct PendingRpcGuard {
    state: Arc<Mutex<ClientState>>,
    stream_id: u64,
    active: bool,
}

impl Drop for PendingRpcGuard {
    fn drop(&mut self) {
        if self.active {
            self.state.lock().pending_rpcs.remove(&self.stream_id);
        }
    }
}

struct PendingPingGuard {
    state: Arc<Mutex<ClientState>>,
    stream_id: u64,
    active: bool,
}

impl Drop for PendingPingGuard {
    fn drop(&mut self) {
        if self.active {
            self.state.lock().pending_pings.remove(&self.stream_id);
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

async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

fn enqueue_cleanup_frame(tx: &mpsc::Sender<Frame>, frame: Frame) {
    if let Err(TrySendError::Full(unsent)) = tx.try_send(frame) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let tx_clone = tx.clone();
            handle.spawn(async move {
                let _ = tx_clone.send(unsent).await;
            });
        }
    }
}
