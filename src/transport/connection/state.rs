use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};

use crate::protocol::{encode_response, Frame, FrameFlags, FrameKind, Response};
use crate::storage::OwnedSnapshot;
use crate::transport::ServerContext;
use crate::types::{MembusError, Principal};

pub(super) struct StreamTable {
    pub(super) closed: bool,
    pub(super) auth_epoch: u64,
    pub(super) authenticated: Option<Principal>,
    pub(super) streams: HashMap<u64, oneshot::Sender<()>>,
}

pub(super) struct SnapshotEntry {
    pub(super) snapshot: Arc<OwnedSnapshot>,
    pub(super) last_used: Instant,
}

pub(super) struct SnapshotTable {
    pub(super) reserved: usize,
    pub(super) entries: HashMap<u64, SnapshotEntry>,
}

impl SnapshotTable {
    pub(super) fn reap_expired(&mut self, ttl: Duration) {
        if ttl.is_zero() {
            return;
        }
        let now = Instant::now();
        self.entries
            .retain(|_id, entry| now.saturating_duration_since(entry.last_used) < ttl);
    }
}

pub(super) struct SnapshotReservationGuard {
    pub(super) table: Arc<Mutex<SnapshotTable>>,
    pub(super) committed: bool,
}

impl SnapshotReservationGuard {
    pub(super) fn commit(mut self, id: u64, snapshot: Arc<OwnedSnapshot>) {
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

pub(super) struct ConnectionState {
    pub(super) ctx: ServerContext,
    pub(super) credit_window: Arc<Semaphore>,
    pub(super) snapshots: Arc<Mutex<SnapshotTable>>,
    pub(super) next_snapshot_id: AtomicU64,
    pub(super) active_streams: Arc<Mutex<StreamTable>>,
    pub(super) outbound_tx: mpsc::Sender<Frame>,
}

impl ConnectionState {
    pub(super) fn new(ctx: ServerContext, outbound_tx: mpsc::Sender<Frame>) -> Self {
        let credit_window = Arc::new(Semaphore::new(ctx.config.max_in_flight_per_conn.max(1)));
        Self {
            ctx,
            credit_window,
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
        }
    }

    pub(super) fn snapshot_auth(&self) -> (Option<Principal>, u64) {
        let table = self.active_streams.lock();
        (table.authenticated.clone(), table.auth_epoch)
    }

    pub(super) fn close_and_cancel_all_streams(&self) {
        let mut table = self.active_streams.lock();
        table.closed = true;
        for (_id, cancel_tx) in table.streams.drain() {
            let _ = cancel_tx.send(());
        }
    }

    pub(super) fn resolve_snapshot(
        &self,
        snapshot_id: Option<u64>,
    ) -> Result<Option<Arc<OwnedSnapshot>>, MembusError> {
        match snapshot_id {
            None => Ok(None),
            Some(id) => {
                let mut table = self.snapshots.lock();
                table.reap_expired(self.ctx.config.snapshot_ttl);
                let entry = table.entries.get_mut(&id).ok_or_else(|| {
                    MembusError::NotFound(format!("unknown or expired snapshot_id {id}"))
                })?;
                entry.last_used = Instant::now();
                Ok(Some(Arc::clone(&entry.snapshot)))
            }
        }
    }
}

pub(super) fn apply_authenticate_inline(conn: &ConnectionState, token: &str) -> Response {
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

pub(super) async fn send_or_shutdown(
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

pub(super) fn build_response_frame(stream_id: u64, resp: &Response, max_frame_body: usize) -> Frame {
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
