use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, watch, Semaphore};

use crate::auth::AuthConfig;
use crate::protocol::message::OP_AUTHENTICATE;
use crate::protocol::{
    decode_request, encode_response, encode_signal_event, encode_stream_end, read_frame,
    run_batched_frame_writer, Frame, FrameFlags, FrameKind, Request, Response,
};
use crate::signals::{EachChangeSubscription, LiveCoalescedSubscription};
use crate::storage::{compute_namespace_parity_digest_with_timeout, OwnedSnapshot};
use crate::transport::write_coord::{
    coordinated_apply_replicated_delta, coordinated_commit_indexed_tx,
    coordinated_delete_search_document, coordinated_index_document, coordinated_put_indexed,
    coordinated_remove_indexed, coordinated_revoke_search_prefix,
};
use crate::transport::ServerContext;
use crate::types::{
    AuthMode, IndexMutations, MembusError, Permission, Principal, SearchLogOp, SignalEvent,
};

pub(crate) struct ConnectionGuard {
    counter: Arc<std::sync::atomic::AtomicUsize>,
}

impl ConnectionGuard {
    pub(crate) fn acquire(counter: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self {
            counter: Arc::clone(counter),
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

struct StreamTable {
    closed: bool,
    auth_epoch: u64,
    authenticated: Option<Principal>,
    streams: HashMap<u64, oneshot::Sender<()>>,
}

struct SnapshotEntry {
    snapshot: Arc<OwnedSnapshot>,
    last_used: Instant,
}

struct SnapshotTable {
    reserved: usize,
    entries: HashMap<u64, SnapshotEntry>,
}

impl SnapshotTable {
    fn reap_expired(&mut self, ttl: Duration) {
        if ttl.is_zero() {
            return;
        }
        let now = Instant::now();
        self.entries
            .retain(|_id, entry| now.saturating_duration_since(entry.last_used) < ttl);
    }
}

struct SnapshotReservationGuard {
    table: Arc<Mutex<SnapshotTable>>,
    committed: bool,
}

impl SnapshotReservationGuard {
    fn commit(mut self, id: u64, snapshot: Arc<OwnedSnapshot>) {
        let mut table = self.table.lock();
        table.reserved = table.reserved.saturating_sub(1);
        table.entries.insert(
            id,
            SnapshotEntry {
                snapshot,
                last_used: Instant::now(),
            },
        );
        self.committed = true;
    }
}

impl Drop for SnapshotReservationGuard {
    fn drop(&mut self) {
        if !self.committed {
            let mut table = self.table.lock();
            table.reserved = table.reserved.saturating_sub(1);
        }
    }
}

struct ConnectionState {
    ctx: ServerContext,
    credit_window: Arc<Semaphore>,
    snapshots: Arc<Mutex<SnapshotTable>>,
    next_snapshot_id: AtomicU64,
    active_streams: Arc<Mutex<StreamTable>>,
    outbound_tx: mpsc::Sender<Frame>,
}

impl ConnectionState {
    fn snapshot_auth(&self) -> (Option<Principal>, u64) {
        let table = self.active_streams.lock();
        (table.authenticated.clone(), table.auth_epoch)
    }

    fn close_and_cancel_all_streams(&self) {
        let mut table = self.active_streams.lock();
        table.closed = true;
        for (_id, cancel_tx) in table.streams.drain() {
            let _ = cancel_tx.send(());
        }
    }
}

/// Serve one multiplexed client connection until EOF, protocol error, or server shutdown.
pub async fn serve_connection<S>(
    stream: S,
    ctx: ServerContext,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let _conn_guard = ConnectionGuard::acquire(&ctx.active_connections);
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Frame>(ctx.config.outbound_queue_capacity);
    let max_frame_body = ctx.config.max_frame_body;

    let writer_task = tokio::spawn(async move {
        run_batched_frame_writer(&mut writer, &mut outbound_rx, max_frame_body).await;
    });

    let conn = Arc::new(ConnectionState {
        credit_window: Arc::new(Semaphore::new(ctx.config.max_in_flight_per_conn.max(1))),
        snapshots: Arc::new(Mutex::new(SnapshotTable {
            reserved: 0,
            entries: HashMap::new(),
        })),
        next_snapshot_id: AtomicU64::new(1),
        active_streams: Arc::new(Mutex::new(StreamTable {
            closed: false,
            auth_epoch: 0,
            authenticated: None,
            streams: HashMap::new(),
        })),
        outbound_tx,
        ctx,
    });

    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let snapshot_ttl = conn.ctx.config.snapshot_ttl;
    let reap_period = if snapshot_ttl.is_zero() {
        Duration::from_secs(3600)
    } else {
        (snapshot_ttl / 2).clamp(Duration::from_millis(25), Duration::from_secs(30))
    };
    let mut reap_interval = tokio::time::interval(reap_period);
    reap_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let frame_opt = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
                continue;
            }
            _ = reap_interval.tick() => {
                conn.snapshots.lock().reap_expired(snapshot_ttl);
                continue;
            }
            res = read_frame(&mut reader, &mut read_buf, max_frame_body) => {
                match res {
                    Ok(frame_opt) => frame_opt,
                    Err(_) => break,
                }
            }
        };

        let Some(frame) = frame_opt else {
            break;
        };

        match frame.kind {
            FrameKind::Ping => {
                let pong = Frame::new(FrameKind::Pong, frame.stream_id, frame.payload);
                if !send_or_shutdown(&conn.outbound_tx, pong, &mut shutdown_rx).await {
                    break;
                }
            }
            FrameKind::Cancel => {
                let cancel = conn.active_streams.lock().streams.remove(&frame.stream_id);
                if let Some(tx) = cancel {
                    let _ = tx.send(());
                }
            }
            FrameKind::Request => {
                // Process `Authenticate` synchronously inline on the reader loop BEFORE
                // dispatching any subsequent pipelined frames so `[Authenticate, Get]`
                // never races and principal switches immediately revoke active streams.
                if frame.payload.first() == Some(&OP_AUTHENTICATE) {
                    let response = match decode_request(&frame.payload) {
                        Ok(Request::Authenticate { token }) => {
                            apply_authenticate_inline(&conn, &token)
                        }
                        Ok(_) => Response::Error(MembusError::Protocol(
                            "malformed authenticate frame".to_owned(),
                        )),
                        Err(err) => Response::Error(err),
                    };
                    let reply = build_response_frame(frame.stream_id, &response, max_frame_body);
                    if !send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await {
                        break;
                    }
                    continue;
                }

                let permit = match Arc::clone(&conn.credit_window).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        let busy = Response::Error(MembusError::Busy {
                            retry_after_ms: 5,
                            reason: "per-connection credit window exhausted".to_owned(),
                        });
                        let reply = build_response_frame(frame.stream_id, &busy, max_frame_body);
                        if !send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                };

                let conn_ref = Arc::clone(&conn);
                let task_shutdown = shutdown_rx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    handle_request_frame(conn_ref, frame.stream_id, frame.payload, task_shutdown)
                        .await;
                });
            }
            _ => {
                let err = Response::Error(MembusError::Protocol(format!(
                    "unexpected client frame kind {:?}",
                    frame.kind
                )));
                let reply = build_response_frame(frame.stream_id, &err, max_frame_body);
                let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
                break;
            }
        }
    }

    conn.close_and_cancel_all_streams();
    drop(conn);
    let _ = writer_task.await;
}

async fn send_or_shutdown(
    tx: &mpsc::Sender<Frame>,
    frame: Frame,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    if *shutdown_rx.borrow() {
        return false;
    }
    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => false,
        res = tx.send(frame) => res.is_ok(),
    }
}

async fn handle_request_frame(
    conn: Arc<ConnectionState>,
    stream_id: u64,
    payload: Bytes,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let max_frame_body = conn.ctx.config.max_frame_body;
    let request = match decode_request(&payload) {
        Ok(req) => req,
        Err(err) => {
            let reply = build_response_frame(stream_id, &Response::Error(err), max_frame_body);
            let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
            return;
        }
    };

    let (principal, captured_auth_epoch) = conn.snapshot_auth();
    if let Err(err) = authorize_request(&conn.ctx.auth, principal.as_ref(), &request) {
        let reply = build_response_frame(stream_id, &Response::Error(err), max_frame_body);
        let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
        return;
    }

    match request {
        Request::SubscribeEachChange {
            namespace,
            key_prefix,
            epoch,
            since_seq,
        } => {
            start_each_change_stream(
                conn,
                stream_id,
                captured_auth_epoch,
                namespace,
                key_prefix,
                epoch,
                since_seq,
                shutdown_rx,
            )
            .await;
            return;
        }
        Request::SubscribeLive {
            namespace,
            key_prefix,
            max_pending,
        } => {
            start_live_stream(
                conn,
                stream_id,
                captured_auth_epoch,
                namespace,
                key_prefix,
                max_pending,
                shutdown_rx,
            )
            .await;
            return;
        }
        _ => {}
    }

    let response = dispatch_unary(&conn, principal.as_ref(), request).await;
    let reply = build_response_frame(stream_id, &response, max_frame_body);
    let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
}

pub(crate) fn authorize_index_mutations(
    auth: &AuthConfig,
    principal: Option<&Principal>,
    namespace: &str,
    indexes: &IndexMutations,
) -> Result<(), MembusError> {
    if auth.mode() == AuthMode::Disabled {
        return Ok(());
    }
    for edge in &indexes.edges {
        auth.authorize(principal, namespace, &edge.src_id, Permission::Write)?;
        auth.authorize(principal, namespace, &edge.dst_id, Permission::Write)?;
    }
    Ok(())
}

pub(crate) fn authorize_search_op(
    auth: &AuthConfig,
    principal: Option<&Principal>,
    op: &SearchLogOp,
) -> Result<(), MembusError> {
    match op {
        SearchLogOp::Index(doc) => {
            auth.authorize(principal, &doc.namespace, doc.doc_id.as_bytes(), Permission::Write)
        }
        SearchLogOp::Delete { namespace, doc_id } => {
            auth.authorize(principal, namespace, doc_id.as_bytes(), Permission::Write)
        }
        SearchLogOp::RevokePrefix { namespace, .. } => {
            auth.authorize_admin(principal, Some(namespace))
        }
    }
}

fn apply_authenticate_inline(conn: &ConnectionState, token: &str) -> Response {
    let auth_res = conn.ctx.auth.authenticate_token(Some(token));
    let mut table = conn.active_streams.lock();
    match auth_res {
        Ok(Some(p)) => {
            let principal_id = p.id.clone();
            let changed = table.authenticated.as_ref() != Some(&p);
            table.authenticated = Some(p);
            if changed {
                table.auth_epoch = table.auth_epoch.saturating_add(1);
                for (_id, cancel_tx) in table.streams.drain() {
                    let _ = cancel_tx.send(());
                }
            }
            Response::Authenticated { principal_id }
        }
        Ok(None) => Response::Authenticated {
            principal_id: "anonymous".to_owned(),
        },
        Err(err) => {
            table.authenticated = None;
            table.auth_epoch = table.auth_epoch.saturating_add(1);
            for (_id, cancel_tx) in table.streams.drain() {
                let _ = cancel_tx.send(());
            }
            Response::Error(err)
        }
    }
}

fn authorize_request(
    auth: &AuthConfig,
    principal: Option<&Principal>,
    req: &Request,
) -> Result<(), MembusError> {
    if auth.mode() == AuthMode::Disabled {
        return Ok(());
    }
    match req {
        Request::Authenticate { .. } => Ok(()),
        Request::Get { namespace, key, .. } => {
            auth.authorize(principal, namespace, key, Permission::Read)
        }
        Request::GetMany {
            namespace, keys, ..
        } => {
            if keys.is_empty() {
                auth.authorize(principal, namespace, b"", Permission::Read)
            } else {
                for key in keys {
                    auth.authorize(principal, namespace, key, Permission::Read)?;
                }
                Ok(())
            }
        }
        Request::Put { namespace, key, .. } | Request::Remove { namespace, key, .. } => {
            auth.authorize(principal, namespace, key, Permission::Write)
        }
        Request::PutIndexed {
            namespace,
            key,
            indexes,
            search_op,
            ..
        } => {
            auth.authorize(principal, namespace, key, Permission::Write)?;
            if let Some(idx) = indexes {
                authorize_index_mutations(auth, principal, namespace, idx)?;
            }
            if let Some(op) = search_op {
                authorize_search_op(auth, principal, op)?;
            }
            Ok(())
        }
        Request::RemoveIndexed {
            namespace,
            key,
            search_op,
            ..
        } => {
            auth.authorize(principal, namespace, key, Permission::Write)?;
            if let Some(op) = search_op {
                authorize_search_op(auth, principal, op)?;
            }
            Ok(())
        }
        Request::CommitTx {
            guards, mutations, ..
        } => {
            auth.require_authenticated(principal)?;
            for g in guards {
                auth.authorize(principal, &g.namespace, &g.key, Permission::Read)?;
            }
            for m in mutations {
                auth.authorize(principal, &m.namespace, &m.key, Permission::Write)?;
            }
            Ok(())
        }
        Request::CommitIndexedTx {
            guards, mutations, ..
        } => {
            auth.require_authenticated(principal)?;
            for g in guards {
                auth.authorize(principal, &g.namespace, &g.key, Permission::Read)?;
            }
            for m in mutations {
                auth.authorize(
                    principal,
                    &m.mutation.namespace,
                    &m.mutation.key,
                    Permission::Write,
                )?;
                if let Some(idx) = &m.indexes {
                    authorize_index_mutations(auth, principal, &m.mutation.namespace, idx)?;
                }
                if let Some(op) = &m.search_op {
                    authorize_search_op(auth, principal, op)?;
                }
            }
            Ok(())
        }
        Request::OpenSnapshot
        | Request::CloseSnapshot { .. }
        | Request::Health
        | Request::HealthV2
        | Request::ListCollections => auth.require_authenticated(principal),
        Request::ScanPrefix {
            namespace, prefix, ..
        } => auth.authorize(principal, namespace, prefix, Permission::Read),
        Request::ScanRange {
            namespace,
            start_key,
            end_key,
            ..
        } => auth.authorize_range(principal, namespace, start_key, end_key, Permission::Read),
        Request::SubscribeEachChange {
            namespace,
            key_prefix,
            ..
        }
        | Request::SubscribeLive {
            namespace,
            key_prefix,
            ..
        } => auth.authorize(principal, namespace, key_prefix, Permission::Read),
        Request::Checkpoint { .. } => auth.authorize_admin(principal, None),
        Request::PutBlob { .. } => auth.authorize(principal, "*", b"", Permission::Write),
        Request::GetBlob { .. } => auth.authorize(principal, "*", b"", Permission::Read),
        Request::QueryOrderedIndex { namespace, .. }
        | Request::QueryNeighbors { namespace, .. }
        | Request::BfsTraverse { namespace, .. }
        | Request::GetCasBlob { namespace, .. }
        | Request::HasCasBlob { namespace, .. }
        | Request::GetCollection { namespace }
        | Request::GetCollectionVersion { namespace, .. } => {
            auth.authorize(principal, namespace, b"", Permission::Read)
        }
        Request::PutCasBlob { namespace, .. } => {
            auth.authorize(principal, namespace, b"", Permission::Write)
        }
        Request::AcquireLease {
            namespace,
            resource,
            ..
        }
        | Request::RenewLease {
            namespace,
            resource,
            ..
        }
        | Request::ReleaseLease {
            namespace,
            resource,
            ..
        } => auth.authorize(principal, namespace, resource.as_bytes(), Permission::Write),
        Request::InspectLease {
            namespace,
            resource,
        }
        | Request::ValidateLease {
            namespace,
            resource,
            ..
        } => auth.authorize(principal, namespace, resource.as_bytes(), Permission::Read),
        Request::RegisterCollection { descriptor, .. } => {
            auth.authorize_admin(principal, Some(&descriptor.namespace))
        }
        Request::BeginMigration { namespace, .. }
        | Request::AdvanceMigrationVerification { namespace, .. }
        | Request::EnterReverseShadow { namespace, .. }
        | Request::RollbackMigration { namespace, .. }
        | Request::ContractMigration { namespace, .. }
        | Request::CommitMigration { namespace, .. }
        | Request::AbortMigration { namespace, .. }
        | Request::RevokeSearchPrefix { namespace, .. } => {
            auth.authorize_admin(principal, Some(namespace))
        }
        Request::NamespaceDigest {
            namespace,
            key_prefix,
            ..
        } => auth.authorize(principal, namespace, key_prefix, Permission::Read),
        Request::TransitionSlotRole { .. } => auth.authorize_admin(principal, None),
        Request::ApplyReplicatedDelta { delta, .. } => {
            auth.authorize(principal, &delta.namespace, &delta.key, Permission::Write)?;
            if let Some(idx) = &delta.indexes {
                authorize_index_mutations(auth, principal, &delta.namespace, idx)?;
            }
            if let Some(op) = &delta.search_op {
                authorize_search_op(auth, principal, op)?;
            }
            Ok(())
        }
        Request::IndexDocument { doc, .. } => {
            auth.authorize(principal, &doc.namespace, doc.doc_id.as_bytes(), Permission::Write)
        }
        Request::DeleteSearchDocument {
            namespace, doc_id, ..
        } => auth.authorize(principal, namespace, doc_id.as_bytes(), Permission::Write),
        Request::Search(query) => {
            let ns = query.namespace.as_deref().unwrap_or("*");
            auth.authorize(principal, ns, b"", Permission::Read)
        }
    }
}

async fn dispatch_unary(
    conn: &Arc<ConnectionState>,
    principal: Option<&Principal>,
    req: Request,
) -> Response {
    let storage = Arc::clone(&conn.ctx.storage);
    match req {
        Request::Authenticate { token } => apply_authenticate_inline(conn, &token),
        Request::Get {
            namespace,
            key,
            snapshot_id,
        } => {
            let snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || match snap {
                Some(s) => storage.get_in_snapshot(&s, &namespace, &key),
                None => storage.get(&namespace, &key),
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("get task failed: {e}"))));
            match result {
                Ok(rec) => Response::Get(rec),
                Err(e) => Response::Error(e),
            }
        }
        Request::GetMany {
            namespace,
            keys,
            snapshot_id,
        } => {
            let snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || match snap {
                Some(s) => storage.get_many_in_snapshot(&s, &namespace, &keys),
                None => storage.get_many(&namespace, &keys),
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("get_many task failed: {e}"))));
            match result {
                Ok(recs) => Response::GetMany(recs),
                Err(e) => Response::Error(e),
            }
        }
        Request::Put {
            namespace,
            key,
            value,
            ack,
            expected,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.put(&namespace, &key, &value, ack, expected)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("put task failed: {e}"))));
            match result {
                Ok(commit) => Response::Commit(commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::PutIndexed {
            namespace,
            key,
            value,
            ack,
            expected,
            indexes,
            search_op,
        } => {
            let ctx = conn.ctx.clone();
            let search = conn.ctx.search.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_put_indexed(
                    &storage,
                    search.as_ref(),
                    &namespace,
                    &key,
                    &value,
                    ack,
                    expected,
                    indexes.as_ref(),
                    search_op,
                )
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("put_indexed failed: {e}"))));
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::Remove {
            namespace,
            key,
            ack,
            expected,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.remove(&namespace, &key, ack, expected)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("remove task failed: {e}"))));
            match result {
                Ok(commit) => Response::Commit(commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::RemoveIndexed {
            namespace,
            key,
            ack,
            expected,
            search_op,
        } => {
            let ctx = conn.ctx.clone();
            let search = conn.ctx.search.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_remove_indexed(
                    &storage,
                    search.as_ref(),
                    &namespace,
                    &key,
                    ack,
                    expected,
                    search_op,
                )
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("remove_indexed failed: {e}"))));
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::CommitTx {
            ack,
            guards,
            mutations,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.commit_tx(&guards, &mutations, ack)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("commit_tx task failed: {e}"))));
            match result {
                Ok(commit) => Response::Commit(commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::CommitIndexedTx {
            ack,
            guards,
            mutations,
        } => {
            let ctx = conn.ctx.clone();
            let search = conn.ctx.search.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_commit_indexed_tx(&storage, search.as_ref(), &guards, mutations, ack)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "commit_indexed_tx failed: {e}"
                )))
            });
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::OpenSnapshot => {
            let reservation = {
                let mut table = conn.snapshots.lock();
                table.reap_expired(conn.ctx.config.snapshot_ttl);
                if table.entries.len().saturating_add(table.reserved)
                    >= conn.ctx.config.max_snapshots_per_conn
                {
                    return Response::Error(MembusError::Busy {
                        retry_after_ms: 10,
                        reason: "per-connection snapshot limit reached".to_owned(),
                    });
                }
                table.reserved += 1;
                SnapshotReservationGuard {
                    table: Arc::clone(&conn.snapshots),
                    committed: false,
                }
            };

            let result = tokio::task::spawn_blocking(move || storage.open_snapshot())
                .await
                .unwrap_or_else(|e| {
                    Err(MembusError::Storage(format!(
                        "open_snapshot task failed: {e}"
                    )))
                });
            match result {
                Ok(snapshot) => {
                    let snapshot_seq = snapshot.snapshot_seq();
                    let snapshot_id = conn.next_snapshot_id.fetch_add(1, Ordering::AcqRel);
                    reservation.commit(snapshot_id, Arc::new(snapshot));
                    Response::SnapshotOpened {
                        snapshot_id,
                        snapshot_seq,
                    }
                }
                Err(e) => Response::Error(e),
            }
        }
        Request::CloseSnapshot { snapshot_id } => {
            if conn.snapshots.lock().entries.remove(&snapshot_id).is_some() {
                Response::Unit
            } else {
                Response::Error(MembusError::NotFound(format!(
                    "unknown snapshot_id {snapshot_id}"
                )))
            }
        }
        Request::ScanPrefix {
            namespace,
            prefix,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "scan_prefix",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.scan_prefix(&snap, &namespace, &prefix, &options)
                },
            )
            .await;
            match result {
                Ok(page) => Response::Scan(page),
                Err(e) => Response::Error(e),
            }
        }
        Request::ScanRange {
            namespace,
            start_key,
            end_key,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "scan_range",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.scan_range(&snap, &namespace, &start_key, &end_key, &options)
                },
            )
            .await;
            match result {
                Ok(page) => Response::Scan(page),
                Err(e) => Response::Error(e),
            }
        }
        Request::QueryOrderedIndex {
            namespace,
            index_id,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "query_ordered_index",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.query_ordered_index(&snap, &namespace, &index_id, &options)
                },
            )
            .await;
            match result {
                Ok(page) => Response::OrderedIndex(page),
                Err(e) => Response::Error(e),
            }
        }
        Request::QueryNeighbors {
            namespace,
            index_id,
            node_id,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "query_neighbors",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.query_neighbors(&snap, &namespace, &index_id, &node_id, &options)
                },
            )
            .await;
            match result {
                Ok(page) => Response::Neighbors(page),
                Err(e) => Response::Error(e),
            }
        }
        Request::BfsTraverse {
            namespace,
            index_id,
            seed_node,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "bfs_traverse",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.bfs_traverse(&snap, &namespace, &index_id, &seed_node, &options)
                },
            )
            .await;
            match result {
                Ok(res) => Response::Bfs(res),
                Err(e) => Response::Error(e),
            }
        }
        Request::Checkpoint { dest_dir } => {
            let safe_dest = match resolve_checkpoint_path(
                conn.ctx.config.checkpoint_root.as_deref(),
                &dest_dir,
            ) {
                Ok(path) => path,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || storage.create_checkpoint(&safe_dest))
                .await
                .unwrap_or_else(|e| Err(MembusError::Storage(format!("checkpoint failed: {e}"))));
            match result {
                Ok(seq) => Response::Checkpoint { seq },
                Err(e) => Response::Error(e),
            }
        }
        Request::Health => Response::Health(conn.ctx.health_status()),
        Request::HealthV2 => Response::HealthV2(conn.ctx.health_status()),
        Request::PutBlob { digest, data, ack } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.put_blob(&digest, &data, ack)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("put_blob failed: {e}"))));
            match result {
                Ok(()) => Response::Unit,
                Err(e) => Response::Error(e),
            }
        }
        Request::GetBlob { digest } => {
            let result = tokio::task::spawn_blocking(move || storage.get_blob(&digest))
                .await
                .unwrap_or_else(|e| Err(MembusError::Storage(format!("get_blob failed: {e}"))));
            match result {
                Ok(data) => Response::Blob(data),
                Err(e) => Response::Error(e),
            }
        }
        Request::PutCasBlob {
            namespace,
            data,
            ack,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.put_cas_blob(&namespace, &data, ack)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("put_cas_blob failed: {e}"))));
            match result {
                Ok(digest) => Response::CasBlobDigest(digest),
                Err(e) => Response::Error(e),
            }
        }
        Request::GetCasBlob { namespace, sha256 } => {
            let result =
                tokio::task::spawn_blocking(move || storage.get_cas_blob(&namespace, &sha256))
                    .await
                    .unwrap_or_else(|e| {
                        Err(MembusError::Storage(format!("get_cas_blob failed: {e}")))
                    });
            match result {
                Ok(data) => Response::Blob(data),
                Err(e) => Response::Error(e),
            }
        }
        Request::HasCasBlob { namespace, sha256 } => {
            let result =
                tokio::task::spawn_blocking(move || storage.has_cas_blob(&namespace, &sha256))
                    .await
                    .unwrap_or_else(|e| {
                        Err(MembusError::Storage(format!("has_cas_blob failed: {e}")))
                    });
            match result {
                Ok(exists) => Response::Bool(exists),
                Err(e) => Response::Error(e),
            }
        }
        Request::AcquireLease {
            namespace,
            resource,
            holder,
            ttl_ms,
        } => {
            let ctx = conn.ctx.clone();
            let leases = Arc::clone(&conn.ctx.leases);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                leases.acquire_lease(&namespace, &resource, &holder, ttl_ms)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("acquire_lease failed: {e}"))));
            match result {
                Ok(grant) => Response::Lease(grant),
                Err(e) => Response::Error(e),
            }
        }
        Request::RenewLease {
            namespace,
            resource,
            holder,
            generation,
            token,
            ttl_ms,
        } => {
            let ctx = conn.ctx.clone();
            let leases = Arc::clone(&conn.ctx.leases);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                leases.renew_lease(&namespace, &resource, &holder, generation, &token, ttl_ms)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("renew_lease failed: {e}"))));
            match result {
                Ok(grant) => Response::Lease(grant),
                Err(e) => Response::Error(e),
            }
        }
        Request::ReleaseLease {
            namespace,
            resource,
            holder,
            generation,
            token,
        } => {
            let ctx = conn.ctx.clone();
            let leases = Arc::clone(&conn.ctx.leases);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                leases.release_lease(&namespace, &resource, &holder, generation, &token)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("release_lease failed: {e}"))));
            match result {
                Ok(()) => Response::Unit,
                Err(e) => Response::Error(e),
            }
        }
        Request::InspectLease {
            namespace,
            resource,
        } => {
            let leases = Arc::clone(&conn.ctx.leases);
            let result =
                tokio::task::spawn_blocking(move || leases.inspect_lease(&namespace, &resource))
                    .await
                    .unwrap_or_else(|e| {
                        Err(MembusError::Storage(format!("inspect_lease failed: {e}")))
                    });
            match result {
                Ok(grant) => Response::OptLease(grant),
                Err(e) => Response::Error(e),
            }
        }
        Request::ValidateLease {
            namespace,
            resource,
            holder,
            generation,
            token,
        } => {
            let leases = Arc::clone(&conn.ctx.leases);
            let result = tokio::task::spawn_blocking(move || {
                leases.validate_lease(&namespace, &resource, &holder, generation, &token)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("validate_lease failed: {e}"))));
            match result {
                Ok(valid) => Response::Bool(valid),
                Err(e) => Response::Error(e),
            }
        }
        Request::RegisterCollection {
            expected_version,
            descriptor,
        } => {
            let ctx = conn.ctx.clone();
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_catalog_write_fence(None)?;
                catalog.register_collection(expected_version, descriptor)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "register_collection failed: {e}"
                )))
            });
            match result {
                Ok(desc) => Response::Collection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::GetCollection { namespace } => {
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || catalog.get_collection(&namespace))
                .await
                .unwrap_or_else(|e| {
                    Err(MembusError::Storage(format!("get_collection failed: {e}")))
                });
            match result {
                Ok(desc) => Response::OptCollection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::ListCollections => {
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || catalog.list_collections())
                .await
                .unwrap_or_else(|e| {
                    Err(MembusError::Storage(format!(
                        "list_collections failed: {e}"
                    )))
                });
            match result {
                Ok(mut list) => {
                    if conn.ctx.auth.mode() == AuthMode::Enforced {
                        list.retain(|desc| {
                            conn.ctx
                                .auth
                                .authorize(principal, &desc.namespace, b"", Permission::Read)
                                .is_ok()
                        });
                    }
                    Response::Collections(list)
                }
                Err(e) => Response::Error(e),
            }
        }
        Request::GetCollectionVersion { namespace, version } => {
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                catalog.get_collection_version(&namespace, version)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "get_collection_version failed: {e}"
                )))
            });
            match result {
                Ok(desc) => Response::OptCollection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::BeginMigration {
            namespace,
            expected_version,
            target_version,
            target_sha256,
            target_schema_bytes,
        } => {
            let ctx = conn.ctx.clone();
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_catalog_write_fence(None)?;
                catalog.begin_migration(
                    &namespace,
                    expected_version,
                    target_version,
                    target_sha256,
                    target_schema_bytes,
                )
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("begin_migration failed: {e}"))));
            match result {
                Ok(desc) => Response::Collection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::CommitMigration {
            namespace,
            target_version,
            verified_through,
        } => {
            let ctx = conn.ctx.clone();
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_catalog_write_fence(None)?;
                catalog.commit_migration(&namespace, target_version, verified_through)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "commit_migration failed: {e}"
                )))
            });
            match result {
                Ok(desc) => Response::Collection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::AbortMigration {
            namespace,
            target_version,
        } => {
            let ctx = conn.ctx.clone();
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_catalog_write_fence(None)?;
                catalog.abort_migration(&namespace, target_version)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("abort_migration failed: {e}"))));
            match result {
                Ok(desc) => Response::Collection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::AdvanceMigrationVerification {
            namespace,
            target_version,
            verified_through,
        } => {
            let ctx = conn.ctx.clone();
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_catalog_write_fence(None)?;
                catalog.advance_migration_verification(&namespace, target_version, verified_through)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "advance_migration_verification failed: {e}"
                )))
            });
            match result {
                Ok(desc) => Response::Collection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::EnterReverseShadow {
            namespace,
            target_version,
            verified_through,
        } => {
            let ctx = conn.ctx.clone();
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_catalog_write_fence(None)?;
                catalog.enter_reverse_shadow(&namespace, target_version, verified_through)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "enter_reverse_shadow failed: {e}"
                )))
            });
            match result {
                Ok(desc) => Response::Collection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::RollbackMigration {
            namespace,
            rollback_version,
        } => {
            let ctx = conn.ctx.clone();
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_catalog_write_fence(None)?;
                catalog.rollback_migration(&namespace, rollback_version)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "rollback_migration failed: {e}"
                )))
            });
            match result {
                Ok(desc) => Response::Collection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::ContractMigration {
            namespace,
            expected_version,
        } => {
            let ctx = conn.ctx.clone();
            let catalog = Arc::clone(&conn.ctx.catalog);
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_catalog_write_fence(None)?;
                catalog.contract_migration(&namespace, expected_version)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "contract_migration failed: {e}"
                )))
            });
            match result {
                Ok(desc) => Response::Collection(desc),
                Err(e) => Response::Error(e),
            }
        }
        Request::NamespaceDigest {
            namespace,
            key_prefix,
            mode,
        } => {
            let catalog = Arc::clone(&conn.ctx.catalog);
            let node_id = conn.ctx.node_id().to_owned();
            let timeout = conn.ctx.config.query_timeout;
            let result = run_blocking_query(timeout, "namespace_digest", move || {
                compute_namespace_parity_digest_with_timeout(
                    storage.as_ref(),
                    catalog.as_ref(),
                    &node_id,
                    &namespace,
                    &key_prefix,
                    mode,
                    timeout,
                )
            })
            .await;
            match result {
                Ok(digest) => Response::NamespaceDigest(digest),
                Err(e) => Response::Error(e),
            }
        }
        Request::TransitionSlotRole {
            expected_role,
            expected_role_epoch,
            new_role,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                ctx.transition_slot_role(expected_role, expected_role_epoch, new_role)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "transition_slot_role failed: {e}"
                )))
            });
            match result {
                Ok(state) => Response::SlotRole(state),
                Err(e) => Response::Error(e),
            }
        }
        Request::ApplyReplicatedDelta {
            delta,
            ack,
            route_origin,
        } => {
            let ctx = conn.ctx.clone();
            let search = conn.ctx.search.clone();
            let local_node_id = conn.ctx.node_id().to_owned();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_replicated_apply_fence(None)?;
                coordinated_apply_replicated_delta(
                    &storage,
                    search.as_ref(),
                    &local_node_id,
                    route_origin.as_deref(),
                    &delta,
                    ack,
                )
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "apply_replicated_delta failed: {e}"
                )))
            });
            match result {
                Ok(coord) => Response::ReplicatedApply(coord.result),
                Err(e) => Response::Error(e),
            }
        }
        Request::IndexDocument { doc, ack } => {
            let ctx = conn.ctx.clone();
            let Some(search) = conn.ctx.search.clone() else {
                return Response::Error(MembusError::InvalidArgument(
                    "search engine is not enabled on this server".to_owned(),
                ));
            };
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_index_document(&search, doc, ack)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("index_document failed: {e}"))));
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::DeleteSearchDocument {
            namespace,
            doc_id,
            ack,
        } => {
            let ctx = conn.ctx.clone();
            let Some(search) = conn.ctx.search.clone() else {
                return Response::Error(MembusError::InvalidArgument(
                    "search engine is not enabled on this server".to_owned(),
                ));
            };
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_delete_search_document(&search, &namespace, &doc_id, ack)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "delete_search_document failed: {e}"
                )))
            });
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::RevokeSearchPrefix {
            namespace,
            prefix,
            ack,
        } => {
            let ctx = conn.ctx.clone();
            let Some(search) = conn.ctx.search.clone() else {
                return Response::Error(MembusError::InvalidArgument(
                    "search engine is not enabled on this server".to_owned(),
                ));
            };
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_revoke_search_prefix(&search, &namespace, &prefix, ack)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "revoke_search_prefix failed: {e}"
                )))
            });
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::Search(query) => {
            let Some(search) = conn.ctx.search.clone() else {
                return Response::Error(MembusError::InvalidArgument(
                    "search engine is not enabled on this server".to_owned(),
                ));
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "search",
                move || search.search(&query),
            )
            .await;
            match result {
                Ok(page) => Response::Search(page),
                Err(e) => Response::Error(e),
            }
        }
        Request::SubscribeEachChange { .. } | Request::SubscribeLive { .. } => {
            Response::Error(MembusError::Protocol(
                "subscription request misrouted to unary dispatch".to_owned(),
            ))
        }
    }
}

enum ActiveSubscription {
    EachChange(EachChangeSubscription),
    Live(LiveCoalescedSubscription),
}

impl ActiveSubscription {
    async fn next_event(&mut self) -> Result<Option<SignalEvent>, MembusError> {
        match self {
            Self::EachChange(s) => s.next_event().await,
            Self::Live(s) => s.next_event().await,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_each_change_stream(
    conn: Arc<ConnectionState>,
    stream_id: u64,
    captured_auth_epoch: u64,
    namespace: String,
    key_prefix: Vec<u8>,
    epoch: u64,
    since_seq: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    match conn
        .ctx
        .broker
        .subscribe_each_change(&namespace, key_prefix, epoch, since_seq)
    {
        Ok(sub) => {
            let epoch = sub.epoch();
            let start_seq = sub.cursor_seq();
            launch_subscription_stream(
                conn,
                stream_id,
                captured_auth_epoch,
                epoch,
                start_seq,
                ActiveSubscription::EachChange(sub),
                shutdown_rx,
            )
            .await;
        }
        Err(err) => {
            let reply = build_response_frame(
                stream_id,
                &Response::Error(err),
                conn.ctx.config.max_frame_body,
            );
            let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_live_stream(
    conn: Arc<ConnectionState>,
    stream_id: u64,
    captured_auth_epoch: u64,
    namespace: String,
    key_prefix: Vec<u8>,
    max_pending: Option<u32>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    match conn.ctx.broker.subscribe_live(
        &namespace,
        key_prefix,
        max_pending.map(|v| v as usize),
    ) {
        Ok(sub) => {
            let epoch = sub.epoch();
            let start_seq = conn.ctx.storage.latest_seq();
            launch_subscription_stream(
                conn,
                stream_id,
                captured_auth_epoch,
                epoch,
                start_seq,
                ActiveSubscription::Live(sub),
                shutdown_rx,
            )
            .await;
        }
        Err(err) => {
            let reply = build_response_frame(
                stream_id,
                &Response::Error(err),
                conn.ctx.config.max_frame_body,
            );
            let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
        }
    }
}

async fn launch_subscription_stream(
    conn: Arc<ConnectionState>,
    stream_id: u64,
    captured_auth_epoch: u64,
    epoch: u64,
    start_seq: u64,
    mut sub: ActiveSubscription,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let max_frame_body = conn.ctx.config.max_frame_body;
    let stream_guard = match conn.ctx.try_acquire_stream_guard() {
        Ok(guard) => guard,
        Err(err) => {
            let reply = build_response_frame(stream_id, &Response::Error(err), max_frame_body);
            let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
            return;
        }
    };
    let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
    let register_result = {
        let mut table = conn.active_streams.lock();
        if table.closed {
            return;
        }
        if table.auth_epoch != captured_auth_epoch {
            Err(MembusError::Unauthorized(
                "connection authentication changed while opening subscription".to_owned(),
            ))
        } else if table.streams.contains_key(&stream_id) {
            Err(MembusError::Protocol(format!(
                "duplicate active subscription stream_id {stream_id}"
            )))
        } else if table.streams.len() >= conn.ctx.config.max_subscriptions_per_conn {
            Err(MembusError::Busy {
                retry_after_ms: 10,
                reason: "per-connection subscription limit reached".to_owned(),
            })
        } else {
            table.streams.insert(stream_id, cancel_tx);
            Ok(())
        }
    };
    if let Err(err) = register_result {
        let reply = build_response_frame(stream_id, &Response::Error(err), max_frame_body);
        let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
        return;
    }

    let ack = Response::Subscribed { epoch, start_seq };
    let ack_frame = build_response_frame(stream_id, &ack, max_frame_body);
    if !send_or_shutdown(&conn.outbound_tx, ack_frame, &mut shutdown_rx).await {
        conn.active_streams.lock().streams.remove(&stream_id);
        return;
    }

    let outbound_tx = conn.outbound_tx.clone();
    let active_streams = Arc::clone(&conn.active_streams);
    tokio::spawn(async move {
        let _stream_guard = stream_guard;
        let end_err = loop {
            tokio::select! {
                biased;
                _ = &mut cancel_rx => break None,
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break None;
                    }
                }
                next = sub.next_event() => match next {
                    Ok(Some(event)) => {
                        let encoded = encode_signal_event(&event);
                        if encoded.len() > max_frame_body {
                            break Some(MembusError::FrameTooLarge {
                                len: encoded.len(),
                                max_len: max_frame_body,
                            });
                        }
                        let frame = Frame::new(FrameKind::StreamEvent, stream_id, encoded);
                        tokio::select! {
                            biased;
                            _ = &mut cancel_rx => break None,
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    break None;
                                }
                            }
                            send_res = outbound_tx.send(frame) => {
                                if send_res.is_err() {
                                    break None;
                                }
                            }
                        }
                    }
                    Ok(None) => break None,
                    Err(err) => break Some(err),
                }
            }
        };
        active_streams.lock().streams.remove(&stream_id);
        let end_frame = Frame::new(
            FrameKind::StreamEnd,
            stream_id,
            encode_stream_end(end_err.as_ref()),
        );
        let _ = outbound_tx.try_send(end_frame);
    });
}

fn resolve_snapshot(
    conn: &ConnectionState,
    snapshot_id: Option<u64>,
) -> Result<Option<Arc<OwnedSnapshot>>, MembusError> {
    match snapshot_id {
        None => Ok(None),
        Some(id) => {
            let mut table = conn.snapshots.lock();
            table.reap_expired(conn.ctx.config.snapshot_ttl);
            let entry = table
                .entries
                .get_mut(&id)
                .ok_or_else(|| {
                    MembusError::NotFound(format!("unknown or expired snapshot_id {id}"))
                })?;
            entry.last_used = Instant::now();
            Ok(Some(Arc::clone(&entry.snapshot)))
        }
    }
}

/// Run a blocking query closure with an optional server-side timeout.
pub(crate) async fn run_blocking_query<T, F>(
    timeout: Option<Duration>,
    op_name: &'static str,
    f: F,
) -> Result<T, MembusError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, MembusError> + Send + 'static,
{
    let handle = tokio::task::spawn_blocking(f);
    match timeout {
        Some(limit) => match tokio::time::timeout(limit, handle).await {
            Ok(Ok(res)) => res,
            Ok(Err(join_err)) => Err(MembusError::Storage(format!(
                "{op_name} task failed: {join_err}"
            ))),
            Err(_) => Err(MembusError::Busy {
                retry_after_ms: 50,
                reason: format!("{op_name} query timeout exceeded ({limit:?})"),
            }),
        },
        None => handle
            .await
            .map_err(|join_err| MembusError::Storage(format!("{op_name} task failed: {join_err}")))?,
    }
}

/// Restrict remote checkpoint destinations strictly inside a configured `checkpoint_root`.
pub(crate) fn resolve_checkpoint_path(
    root: Option<&Path>,
    raw_dest: &str,
) -> Result<PathBuf, MembusError> {
    let root = root.ok_or_else(|| {
        MembusError::InvalidArgument(
            "checkpoint RPC is disabled unless server checkpoint_root is configured".to_owned(),
        )
    })?;
    if raw_dest.is_empty() {
        return Err(MembusError::InvalidArgument(
            "checkpoint destination must not be empty".to_owned(),
        ));
    }
    let rel = Path::new(raw_dest);
    if rel.is_absolute() {
        return Err(MembusError::InvalidArgument(
            "checkpoint destination must be a relative path under checkpoint_root".to_owned(),
        ));
    }

    let mut clean = PathBuf::new();
    for component in rel.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(MembusError::InvalidArgument(format!(
                    "checkpoint destination '{raw_dest}' escapes checkpoint_root"
                )));
            }
        }
    }
    if clean.as_os_str().is_empty() {
        return Err(MembusError::InvalidArgument(
            "checkpoint destination must name a subdirectory under checkpoint_root".to_owned(),
        ));
    }
    Ok(root.join(clean))
}

fn build_response_frame(stream_id: u64, resp: &Response, max_frame_body: usize) -> Frame {
    let encoded = encode_response(resp);
    if encoded.len() > max_frame_body {
        let too_large = Response::Error(MembusError::FrameTooLarge {
            len: encoded.len(),
            max_len: max_frame_body,
        });
        return Frame::with_flags(
            FrameKind::Response,
            FrameFlags::ERROR,
            stream_id,
            encode_response(&too_large),
        );
    }
    let flags = if matches!(resp, Response::Error(_)) {
        FrameFlags::ERROR
    } else {
        FrameFlags::NONE
    };
    Frame::with_flags(FrameKind::Response, flags, stream_id, encoded)
}
