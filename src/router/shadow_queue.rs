//! Bounded asynchronous shadow write-tee queue, failure isolation worker, `CF_LOG` catch-up
//! engine, startup slot reconciliation, and shared runtime state for `membus-ab-router`.

use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use ring::rand::{SecureRandom, SystemRandom};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{watch, Notify, Semaphore};

use crate::router::config::{RouterConfig, ShadowQueueConfig, SlotId};
use crate::router::upstream::UpstreamClient;
use crate::types::{
    now_unix_ms, MembusError, MutationOp, NamespaceParityDigest, ParityDigestMode,
    ReplicatedDelta, ReplicationStamp, SlotRole,
};

const MAX_CATCHUP_PASSES: usize = 32;
const CATCHUP_PAGE_LIMIT: usize = 256;

/// Header value emitted on `X-Route-Shadow-Sync` after an authoritative Primary commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowEnqueueStatus {
    Queued,
    Deferred,
    Dropped,
    Disabled,
}

impl ShadowEnqueueStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Deferred => "deferred",
            Self::Dropped => "dropped",
            Self::Disabled => "disabled",
        }
    }
}

/// One committed Primary write batch queued for asynchronous shadow delivery.
#[derive(Clone, Debug)]
pub struct ShadowItem {
    pub origin_slot: SlotId,
    pub origin_seq: u64,
    pub approx_bytes: usize,
    pub deltas: Vec<ReplicatedDelta>,
}

/// Snapshot of the last completed parity digest verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VerifiedParityRecord {
    pub namespaces: Vec<String>,
    pub mode: ParityDigestMode,
    pub matched: bool,
    pub verified_at_ms: u64,
    pub primary_slot: SlotId,
    pub shadow_slot: SlotId,
}

/// Per-namespace digest comparison between Slot A and Slot B.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NamespaceParityResult {
    pub namespace: String,
    #[serde(rename = "match")]
    pub matched: bool,
    pub slot_a: NamespaceParityDigest,
    pub slot_b: NamespaceParityDigest,
}

/// Aggregate parity report returned by `POST /admin/parity`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ParityCheckReport {
    #[serde(rename = "match")]
    pub matched: bool,
    pub mode: ParityDigestMode,
    pub primary_slot: SlotId,
    pub shadow_slot: SlotId,
    pub parity_dirty: bool,
    pub namespaces: Vec<NamespaceParityResult>,
}

/// Report returned by `POST /admin/catchup`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CatchupReport {
    pub caught_up: bool,
    pub replayed_entries: u64,
    pub replayed_mutations: u64,
    pub scanned_through_seq: u64,
    pub parity_verified: bool,
    pub parity_dirty: bool,
    pub verified_namespaces: Vec<String>,
}

/// Live router telemetry returned by `GET /admin/status`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RouterMetricsSnapshot {
    pub primary_slot: SlotId,
    pub shadow_slot: SlotId,
    pub primary_role_epoch: u64,
    pub shadow_role_epoch: u64,
    pub shadow_enabled: bool,
    pub parity_dirty: bool,
    pub shadow_queue_depth: usize,
    pub shadow_queue_bytes: usize,
    pub shadow_lag_ops: u64,
    pub shadow_applied_total: u64,
    pub shadow_dropped_total: u64,
    pub shadow_errors_total: u64,
    pub last_shadowed_seq: u64,
    pub last_verified_parity: Option<VerifiedParityRecord>,
}

#[derive(Clone, Debug)]
pub(crate) struct RouteTopologyState {
    pub primary_slot: SlotId,
    pub shadow_enabled: bool,
    pub primary_role_epoch: u64,
    pub shadow_role_epoch: u64,
    pub last_shadowed_seq: u64,
    /// Conservative lowest sequence that was dropped or failed shadow delivery;
    /// catch-up always starts at or before `min_failed_seq - 1`.
    pub min_failed_seq: Option<u64>,
    pub touched_namespaces: BTreeSet<String>,
    pub last_verified_parity: Option<VerifiedParityRecord>,
}

impl RouteTopologyState {
    pub(crate) fn catchup_start_seq(&self) -> u64 {
        match self.min_failed_seq {
            Some(failed_seq) => self
                .last_shadowed_seq
                .min(failed_seq.saturating_sub(1)),
            None => self.last_shadowed_seq,
        }
    }
}

struct BoundedShadowQueue {
    items: VecDeque<ShadowItem>,
    queued_bytes: usize,
    in_flight: usize,
}

impl BoundedShadowQueue {
    fn new() -> Self {
        Self {
            items: VecDeque::new(),
            queued_bytes: 0,
            in_flight: 0,
        }
    }

    fn depth(&self) -> usize {
        self.items.len().saturating_add(self.in_flight)
    }
}

/// Shared runtime state coordinating data-plane routing, shadow delivery, and admin control.
pub struct RouterRuntime {
    slot_a: Arc<UpstreamClient>,
    slot_b: Arc<UpstreamClient>,
    shadow_cfg: ShadowQueueConfig,
    primary_timeout: Duration,
    admin_token: Option<String>,
    boot_nonce: [u8; 16],
    data_concurrency_sem: Arc<Semaphore>,
    topology: RwLock<RouteTopologyState>,
    queue: Mutex<BoundedShadowQueue>,
    queue_notify: Notify,
    drain_notify: Notify,
    parity_dirty: AtomicBool,
    dirty_generation: AtomicU64,
    shadow_lag_ops: AtomicU64,
    shadow_applied_total: AtomicU64,
    shadow_dropped_total: AtomicU64,
    shadow_errors_total: AtomicU64,
    request_counter: AtomicU64,
    cutover_lock: tokio::sync::Mutex<()>,
}

impl RouterRuntime {
    pub fn new(
        slot_a: Arc<UpstreamClient>,
        slot_b: Arc<UpstreamClient>,
        cfg: &RouterConfig,
        admin_token: Option<String>,
    ) -> Result<Self, MembusError> {
        let mut boot_nonce = [0u8; 16];
        SystemRandom::new()
            .fill(&mut boot_nonce)
            .map_err(|_| MembusError::Io("failed to generate CSPRNG boot nonce".to_owned()))?;

        Ok(Self {
            slot_a,
            slot_b,
            shadow_cfg: cfg.shadow,
            primary_timeout: Duration::from_millis(cfg.primary_timeout_ms),
            admin_token,
            boot_nonce,
            data_concurrency_sem: Arc::new(Semaphore::new(cfg.max_in_flight.max(1))),
            topology: RwLock::new(RouteTopologyState {
                primary_slot: cfg.initial_primary,
                shadow_enabled: cfg.shadow_enabled,
                primary_role_epoch: 0,
                shadow_role_epoch: 0,
                last_shadowed_seq: 0,
                min_failed_seq: None,
                touched_namespaces: BTreeSet::new(),
                last_verified_parity: None,
            }),
            queue: Mutex::new(BoundedShadowQueue::new()),
            queue_notify: Notify::new(),
            drain_notify: Notify::new(),
            parity_dirty: AtomicBool::new(false),
            dirty_generation: AtomicU64::new(0),
            shadow_lag_ops: AtomicU64::new(0),
            shadow_applied_total: AtomicU64::new(0),
            shadow_dropped_total: AtomicU64::new(0),
            shadow_errors_total: AtomicU64::new(0),
            request_counter: AtomicU64::new(1),
            cutover_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub fn client_for(&self, slot: SlotId) -> Arc<UpstreamClient> {
        match slot {
            SlotId::SlotA => Arc::clone(&self.slot_a),
            SlotId::SlotB => Arc::clone(&self.slot_b),
        }
    }

    pub fn admin_token(&self) -> Option<&str> {
        self.admin_token.as_deref()
    }

    pub fn boot_nonce(&self) -> &[u8; 16] {
        &self.boot_nonce
    }

    pub fn data_concurrency_sem(&self) -> &Arc<Semaphore> {
        &self.data_concurrency_sem
    }

    pub fn primary_timeout(&self) -> Duration {
        self.primary_timeout
    }

    pub fn shadow_timeout(&self) -> Duration {
        Duration::from_millis(self.shadow_cfg.timeout_ms)
    }

    pub fn next_request_id(&self) -> u64 {
        self.request_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn cutover_lock(&self) -> &tokio::sync::Mutex<()> {
        &self.cutover_lock
    }

    pub(crate) fn topology_snapshot(&self) -> RouteTopologyState {
        self.topology.read().clone()
    }

    pub fn primary_slot(&self) -> SlotId {
        self.topology.read().primary_slot
    }

    pub fn shadow_slot(&self) -> SlotId {
        self.primary_slot().other()
    }

    pub fn is_parity_dirty(&self) -> bool {
        self.parity_dirty.load(Ordering::Acquire)
    }

    /// Mark parity dirty and advance `dirty_generation` so any concurrent catch-up cannot
    /// race and clear `parity_dirty` after a newer unmirrored write or drop occurred.
    pub fn mark_parity_dirty(&self) {
        self.dirty_generation.fetch_add(1, Ordering::AcqRel);
        self.parity_dirty.store(true, Ordering::Release);
    }

    pub fn record_touched_namespace(&self, ns: &str) {
        if !ns.trim().is_empty() {
            let mut topo = self.topology.write();
            topo.touched_namespaces.insert(ns.to_owned());
        }
    }

    pub fn shadow_queue_depth(&self) -> usize {
        self.queue.lock().depth()
    }

    /// Reconcile initial primary slot and role epochs from both slots' `/v2/health`.
    ///
    /// - Under `auto_discover == true` (`--primary auto`): requires both slots reachable and
    ///   EXACTLY ONE slot reporting `SlotRole::Active`, failing closed on 0 or 2 Active slots.
    /// - Under `auto_discover == false`: detects dual-Active split-brain (fail closed),
    ///   reconciles to whichever slot is actually `Active` if roles swapped before restart,
    ///   and marks `parity_dirty = true` if both slots are reachable with divergent `latest_seq`.
    pub async fn reconcile_startup_slots(&self, auto_discover: bool) -> Result<(), MembusError> {
        let probe_timeout = Duration::from_millis(self.shadow_cfg.timeout_ms.min(2000));
        let (res_a, res_b) = tokio::join!(
            self.slot_a.health(probe_timeout),
            self.slot_b.health(probe_timeout),
        );

        match (res_a, res_b) {
            (Ok(ha), Ok(hb)) => {
                let a_active = ha.slot_role == SlotRole::Active;
                let b_active = hb.slot_role == SlotRole::Active;
                if a_active && b_active {
                    return Err(MembusError::Conflict {
                        namespace: "_router:startup".to_owned(),
                        key: b"split_brain_dual_active".to_vec(),
                        expected: crate::types::ExpectedVersion::Any,
                        actual: None,
                    });
                }
                let discovered_primary = match (a_active, b_active) {
                    (true, false) => SlotId::SlotA,
                    (false, true) => SlotId::SlotB,
                    (false, false) => {
                        if auto_discover {
                            return Err(MembusError::InvalidArgument(format!(
                                "startup auto-discovery failed closed: neither slot is Active (slot-a={}, slot-b={})",
                                ha.slot_role.as_str(),
                                hb.slot_role.as_str()
                            )));
                        }
                        self.primary_slot()
                    }
                    (true, true) => unreachable!(),
                };
                let (p_health, s_health) = match discovered_primary {
                    SlotId::SlotA => (ha, hb),
                    SlotId::SlotB => (hb, ha),
                };
                {
                    let mut topo = self.topology.write();
                    topo.primary_slot = discovered_primary;
                    topo.primary_role_epoch = p_health.role_epoch;
                    topo.shadow_role_epoch = s_health.role_epoch;
                }
                if p_health.latest_seq != s_health.latest_seq {
                    self.mark_parity_dirty();
                }
                Ok(())
            }
            (Ok(ha), Err(err_b)) => {
                if auto_discover {
                    return Err(MembusError::Io(format!(
                        "startup auto-discovery failed closed: slot-b unreachable ({err_b})"
                    )));
                }
                let configured = self.primary_slot();
                let mut topo = self.topology.write();
                if configured == SlotId::SlotA {
                    topo.primary_role_epoch = ha.role_epoch;
                } else {
                    topo.shadow_role_epoch = ha.role_epoch;
                }
                Ok(())
            }
            (Err(err_a), Ok(hb)) => {
                if auto_discover {
                    return Err(MembusError::Io(format!(
                        "startup auto-discovery failed closed: slot-a unreachable ({err_a})"
                    )));
                }
                let configured = self.primary_slot();
                let mut topo = self.topology.write();
                if configured == SlotId::SlotB {
                    topo.primary_role_epoch = hb.role_epoch;
                } else {
                    topo.shadow_role_epoch = hb.role_epoch;
                }
                Ok(())
            }
            (Err(err_a), Err(err_b)) => {
                if auto_discover {
                    return Err(MembusError::Io(format!(
                        "startup auto-discovery failed closed: both slots unreachable (slot-a: {err_a}; slot-b: {err_b})"
                    )));
                }
                Ok(())
            }
        }
    }

    /// Update cached role epochs after a live health probe, compensation, or role transition.
    pub(crate) fn update_role_epochs(&self, primary_epoch: u64, shadow_epoch: u64) {
        let mut topo = self.topology.write();
        topo.primary_role_epoch = primary_epoch;
        topo.shadow_role_epoch = shadow_epoch;
    }

    /// Atomically swap the active primary slot and enable reverse-shadowing.
    pub(crate) fn commit_topology_swap(
        &self,
        new_primary: SlotId,
        new_primary_epoch: u64,
        new_shadow_epoch: u64,
        new_last_shadowed_seq: u64,
        shadow_enabled: bool,
    ) {
        let mut topo = self.topology.write();
        topo.primary_slot = new_primary;
        topo.shadow_enabled = shadow_enabled;
        topo.primary_role_epoch = new_primary_epoch;
        topo.shadow_role_epoch = new_shadow_epoch;
        topo.last_shadowed_seq = new_last_shadowed_seq;
        topo.min_failed_seq = None;
        self.shadow_lag_ops.store(0, Ordering::Release);
        self.parity_dirty.store(false, Ordering::Release);
    }

    /// Return a consistent snapshot of all router operational metrics.
    pub fn metrics_snapshot(&self) -> RouterMetricsSnapshot {
        let topo = self.topology.read().clone();
        let (depth, bytes) = {
            let q = self.queue.lock();
            (q.depth(), q.queued_bytes)
        };
        RouterMetricsSnapshot {
            primary_slot: topo.primary_slot,
            shadow_slot: topo.primary_slot.other(),
            primary_role_epoch: topo.primary_role_epoch,
            shadow_role_epoch: topo.shadow_role_epoch,
            shadow_enabled: topo.shadow_enabled,
            parity_dirty: self.parity_dirty.load(Ordering::Acquire),
            shadow_queue_depth: depth,
            shadow_queue_bytes: bytes,
            shadow_lag_ops: self.shadow_lag_ops.load(Ordering::Acquire),
            shadow_applied_total: self.shadow_applied_total.load(Ordering::Acquire),
            shadow_dropped_total: self.shadow_dropped_total.load(Ordering::Acquire),
            shadow_errors_total: self.shadow_errors_total.load(Ordering::Acquire),
            last_shadowed_seq: topo.last_shadowed_seq,
            last_verified_parity: topo.last_verified_parity,
        }
    }

    /// Non-blocking O(1) enqueue into the bounded shadow queue after a Primary `2xx` commit.
    ///
    /// Never awaits shadow I/O and never fails or delays the Primary response.
    pub fn enqueue_shadow(&self, item: ShadowItem) -> ShadowEnqueueStatus {
        let shadow_enabled = self.topology.read().shadow_enabled;
        if !shadow_enabled {
            return ShadowEnqueueStatus::Disabled;
        }
        if item.deltas.is_empty() {
            return ShadowEnqueueStatus::Queued;
        }

        let mut q = self.queue.lock();
        let next_bytes = q.queued_bytes.checked_add(item.approx_bytes);
        let exceeds_ops = q.items.len() >= self.shadow_cfg.max_ops;
        let exceeds_bytes = next_bytes.is_none_or(|b| b > self.shadow_cfg.max_bytes);
        if exceeds_ops || exceeds_bytes {
            drop(q);
            self.record_shadow_failure(item.origin_slot, item.origin_seq, false);
            return ShadowEnqueueStatus::Dropped;
        }

        q.queued_bytes = next_bytes.unwrap_or(self.shadow_cfg.max_bytes);
        q.items.push_back(item);
        drop(q);
        self.queue_notify.notify_one();

        if self.parity_dirty.load(Ordering::Acquire) {
            ShadowEnqueueStatus::Deferred
        } else {
            ShadowEnqueueStatus::Queued
        }
    }

    fn record_shadow_failure(&self, origin_slot: SlotId, origin_seq: u64, count_error: bool) {
        if count_error {
            self.shadow_errors_total.fetch_add(1, Ordering::AcqRel);
        }
        self.shadow_dropped_total.fetch_add(1, Ordering::AcqRel);
        self.shadow_lag_ops.fetch_add(1, Ordering::AcqRel);
        self.mark_parity_dirty();

        if origin_seq > 0 {
            let mut topo = self.topology.write();
            if topo.primary_slot == origin_slot {
                topo.min_failed_seq = Some(match topo.min_failed_seq {
                    Some(prev) => prev.min(origin_seq),
                    None => origin_seq,
                });
            }
        }
    }

    /// Wait up to `timeout` for queued and in-flight shadow items to drain to zero.
    pub async fn wait_shadow_queue_drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let notified = self.drain_notify.notified();
            if self.shadow_queue_depth() == 0 {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return self.shadow_queue_depth() == 0;
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    fn clear_queued_items(&self) {
        let mut q = self.queue.lock();
        q.items.clear();
        q.queued_bytes = 0;
        if q.in_flight == 0 {
            self.drain_notify.notify_waiters();
        }
    }

    /// Discover all namespaces requiring parity verification across explicit arguments,
    /// registered catalog collections, previously verified namespaces, and observed writes.
    pub(crate) async fn discover_namespaces(
        &self,
        explicit: &[String],
        extra: &BTreeSet<String>,
    ) -> Vec<String> {
        let mut set: BTreeSet<String> = extra.clone();
        for ns in explicit {
            if !ns.trim().is_empty() {
                set.insert(ns.clone());
            }
        }
        let topo = self.topology_snapshot();
        for ns in &topo.touched_namespaces {
            set.insert(ns.clone());
        }
        if let Some(prev) = &topo.last_verified_parity {
            for ns in &prev.namespaces {
                set.insert(ns.clone());
            }
        }
        let primary = self.client_for(topo.primary_slot);
        if let Ok(collections) = primary.list_collections(self.primary_timeout).await {
            for desc in collections {
                set.insert(desc.namespace);
            }
        }
        set.into_iter().collect()
    }

    /// Verify parity digests across both Slot A and Slot B for `namespaces`, including
    /// `entry_count`, `xor_digest_hex`, and `catalog_schema_version`.
    pub async fn verify_parity(
        &self,
        namespaces: &[String],
        mode: ParityDigestMode,
        prefix: Option<&str>,
    ) -> Result<ParityCheckReport, MembusError> {
        if namespaces.is_empty() {
            return Err(MembusError::InvalidArgument(
                "at least one namespace is required for parity verification".to_owned(),
            ));
        }

        let timeout = self.primary_timeout;
        let primary_slot = self.primary_slot();
        let shadow_slot = primary_slot.other();
        let mut results = Vec::with_capacity(namespaces.len());
        let mut all_matched = true;

        for ns in namespaces {
            let (dig_a_res, dig_b_res) = tokio::join!(
                self.slot_a.namespace_digest(ns, mode, prefix, timeout),
                self.slot_b.namespace_digest(ns, mode, prefix, timeout),
            );
            let dig_a = dig_a_res?;
            let dig_b = dig_b_res?;
            let matched = dig_a.entry_count == dig_b.entry_count
                && dig_a.xor_digest_hex == dig_b.xor_digest_hex
                && dig_a.catalog_schema_version == dig_b.catalog_schema_version;
            if !matched {
                all_matched = false;
            }
            results.push(NamespaceParityResult {
                namespace: ns.clone(),
                matched,
                slot_a: dig_a,
                slot_b: dig_b,
            });
        }

        let record = VerifiedParityRecord {
            namespaces: namespaces.to_vec(),
            mode,
            matched: all_matched,
            verified_at_ms: now_unix_ms(),
            primary_slot,
            shadow_slot,
        };
        self.topology.write().last_verified_parity = Some(record);

        Ok(ParityCheckReport {
            matched: all_matched,
            mode,
            primary_slot,
            shadow_slot,
            parity_dirty: self.is_parity_dirty(),
            namespaces: results,
        })
    }

    /// Replay `CF_LOG` deltas from Primary (`GET /v2/updates`) to Shadow (`/v2/replication/apply`)
    /// and clear `parity_dirty` ONLY when fully caught up, digest-verified, AND no concurrent
    /// dirty-generation bump occurred during catch-up.
    pub async fn run_catchup(
        &self,
        requested_namespaces: &[String],
        mode: ParityDigestMode,
        from_seq: Option<u64>,
    ) -> Result<CatchupReport, MembusError> {
        let _guard = self.cutover_lock.lock().await;
        self.run_catchup_locked(requested_namespaces, mode, from_seq)
            .await
    }

    pub(crate) async fn run_catchup_locked(
        &self,
        requested_namespaces: &[String],
        mode: ParityDigestMode,
        from_seq: Option<u64>,
    ) -> Result<CatchupReport, MembusError> {
        let topo = self.topology_snapshot();
        let primary_slot = topo.primary_slot;
        let shadow_slot = primary_slot.other();
        let primary = self.client_for(primary_slot);
        let shadow = self.client_for(shadow_slot);
        let timeout = self.primary_timeout;

        let primary_health = primary.health(timeout).await?;
        let shadow_health = shadow.health(timeout).await?;
        self.update_role_epochs(primary_health.role_epoch, shadow_health.role_epoch);

        // Snapshot dirty_generation BEFORE draining/replaying so any concurrent drop or
        // unmirrored write during catch-up prevents clearing parity_dirty.
        let gen_before = self.dirty_generation.load(Ordering::Acquire);

        self.clear_queued_items();
        let _ = self
            .wait_shadow_queue_drain(Duration::from_millis(self.shadow_cfg.timeout_ms))
            .await;

        let start_seq = from_seq.unwrap_or_else(|| topo.catchup_start_seq());
        let (replayed_entries, replayed_mutations, cursor, touched_namespaces, saw_standalone_search) =
            self.replay_updates_until_caught_up(
                &primary,
                &shadow,
                primary_slot,
                shadow_health.role_epoch,
                start_seq,
                timeout,
            )
            .await?;

        // Always persist forward cursor progress so large (>8,192 entry) backlogs never livelock.
        {
            let mut t = self.topology.write();
            if t.primary_slot == primary_slot && cursor > t.last_shadowed_seq {
                t.last_shadowed_seq = cursor;
            }
            if t.primary_slot == primary_slot
                && t.min_failed_seq
                    .is_some_and(|failed| start_seq < failed && cursor >= failed)
            {
                t.min_failed_seq = None;
            }
            for ns in &touched_namespaces {
                t.touched_namespaces.insert(ns.clone());
            }
        }

        if saw_standalone_search {
            self.mark_parity_dirty();
            return Err(MembusError::InvalidArgument(
                "CF_LOG contains standalone search_ops not replicable via /v2/replication/apply; manual search sync required before clearing parity_dirty"
                    .to_owned(),
            ));
        }

        let verify_list = self
            .discover_namespaces(requested_namespaces, &touched_namespaces)
            .await;

        let parity_verified = if verify_list.is_empty() {
            primary.health(timeout).await?.latest_seq <= cursor
        } else {
            let report = self.verify_parity(&verify_list, mode, None).await?;
            report.matched
        };

        let post_primary = primary.health(timeout).await?;
        let queue_empty = self.shadow_queue_depth() == 0;
        let caught_up = post_primary.latest_seq <= cursor && queue_empty;
        let no_concurrent_dirty = self.dirty_generation.load(Ordering::Acquire) == gen_before;

        if caught_up && parity_verified && no_concurrent_dirty {
            self.shadow_lag_ops.store(0, Ordering::Release);
            self.parity_dirty.store(false, Ordering::Release);
        } else {
            self.parity_dirty.store(true, Ordering::Release);
        }

        Ok(CatchupReport {
            caught_up,
            replayed_entries,
            replayed_mutations,
            scanned_through_seq: cursor,
            parity_verified,
            parity_dirty: self.is_parity_dirty(),
            verified_namespaces: verify_list,
        })
    }

    /// Replay `CF_LOG` updates from `primary` to `shadow` starting after `start_seq`.
    /// Skips local tier-swap entries (`is_tier_swap == true`) and flags standalone `search_ops`.
    pub(crate) async fn replay_updates_until_caught_up(
        &self,
        primary: &UpstreamClient,
        shadow: &UpstreamClient,
        primary_slot: SlotId,
        shadow_role_epoch: u64,
        start_seq: u64,
        timeout: Duration,
    ) -> Result<(u64, u64, u64, BTreeSet<String>, bool), MembusError> {
        let mut cursor = start_seq;
        let mut total_entries = 0u64;
        let mut total_mutations = 0u64;
        let mut touched_namespaces = BTreeSet::new();
        let mut saw_standalone_search = false;
        let route_origin = primary_slot.route_origin();
        let epoch_opt = (shadow_role_epoch > 0).then_some(shadow_role_epoch);

        for _ in 0..MAX_CATCHUP_PASSES {
            let primary_health = primary.health(timeout).await?;
            let page = primary
                .fetch_updates(cursor, CATCHUP_PAGE_LIMIT, timeout)
                .await?;
            for entry in &page.entries {
                total_entries = total_entries.saturating_add(1);
                if !entry.search_ops.is_empty() {
                    saw_standalone_search = true;
                }
                for m in &entry.mutations {
                    if m.is_tier_swap {
                        continue;
                    }
                    touched_namespaces.insert(m.namespace.clone());
                    let stamp = match &m.origin_stamp {
                        Some(existing) => existing.clone(),
                        None => ReplicationStamp {
                            timestamp_ms: entry.ts_ms,
                            origin_node_id: primary_slot.as_str().to_owned(),
                            origin_seq: entry.seq,
                            value_sha256: mutation_value_sha256(&m.op),
                        },
                    };
                    let delta = ReplicatedDelta {
                        stamp,
                        namespace: m.namespace.clone(),
                        key: m.key.clone(),
                        op: m.op.clone(),
                        indexes: m.indexes.clone(),
                        search_op: m.search_op.clone(),
                    };
                    let resp = shadow
                        .apply_replicated_delta(&delta, &route_origin, epoch_opt, timeout)
                        .await?;
                    if !resp.status.is_success() {
                        let body_txt = String::from_utf8_lossy(&resp.body);
                        return Err(MembusError::Io(format!(
                            "shadow {} rejected catch-up delta seq {} with HTTP {}: {}",
                            shadow.slot(),
                            entry.seq,
                            resp.status.as_u16(),
                            body_txt.trim()
                        )));
                    }
                    total_mutations = total_mutations.saturating_add(1);
                    self.shadow_applied_total.fetch_add(1, Ordering::AcqRel);
                }
            }

            let advanced = page.scanned_through_seq > cursor;
            if page.scanned_through_seq > cursor {
                cursor = page.scanned_through_seq;
            }
            if page.entries.is_empty() && cursor >= primary_health.latest_seq {
                break;
            }
            if !advanced && page.entries.is_empty() {
                break;
            }
        }

        Ok((
            total_entries,
            total_mutations,
            cursor,
            touched_namespaces,
            saw_standalone_search,
        ))
    }

    fn pop_next_item(&self) -> Option<ShadowItem> {
        let mut q = self.queue.lock();
        let item = q.items.pop_front()?;
        q.queued_bytes = q.queued_bytes.saturating_sub(item.approx_bytes);
        q.in_flight = q.in_flight.saturating_add(1);
        Some(item)
    }

    fn finish_in_flight(&self) {
        let mut q = self.queue.lock();
        q.in_flight = q.in_flight.saturating_sub(1);
        if q.depth() == 0 {
            self.drain_notify.notify_waiters();
        }
    }

    async fn deliver_shadow_item(&self, item: &ShadowItem) -> bool {
        let (current_primary, shadow_epoch) = {
            let topo = self.topology.read();
            (topo.primary_slot, topo.shadow_role_epoch)
        };
        if item.origin_slot != current_primary {
            return true;
        }
        let shadow_slot = current_primary.other();
        let shadow = self.client_for(shadow_slot);
        let route_origin = item.origin_slot.route_origin();
        let timeout = self.shadow_timeout();
        let epoch_opt = (shadow_epoch > 0).then_some(shadow_epoch);

        for delta in &item.deltas {
            let res = shadow
                .apply_replicated_delta(delta, &route_origin, epoch_opt, timeout)
                .await;
            match res {
                Ok(resp) if resp.status.is_success() => {
                    self.shadow_applied_total.fetch_add(1, Ordering::AcqRel);
                }
                Ok(_) | Err(_) => {
                    self.record_shadow_failure(item.origin_slot, item.origin_seq, true);
                    return false;
                }
            }
        }

        if !self.parity_dirty.load(Ordering::Acquire) {
            let mut topo = self.topology.write();
            if topo.primary_slot == item.origin_slot
                && topo.min_failed_seq.is_none()
                && item.origin_seq > topo.last_shadowed_seq
            {
                topo.last_shadowed_seq = item.origin_seq;
            }
        }
        true
    }
}

/// Spawn the background asynchronous shadow delivery + automatic catch-up worker task.
pub fn spawn_shadow_worker(
    runtime: Arc<RouterRuntime>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff_ms = 0u64;
        let max_backoff_ms = runtime.shadow_cfg.max_backoff_ms.max(50);
        let catchup_interval =
            Duration::from_millis(runtime.shadow_cfg.catchup_interval_ms.max(100));
        let mut last_auto_catchup = Instant::now();

        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            if let Some(item) = runtime.pop_next_item() {
                let ok = runtime.deliver_shadow_item(&item).await;
                runtime.finish_in_flight();

                if ok {
                    backoff_ms = 0;
                } else {
                    backoff_ms = if backoff_ms == 0 {
                        50
                    } else {
                        backoff_ms.saturating_mul(2).min(max_backoff_ms)
                    };
                    let sleep = tokio::time::sleep(Duration::from_millis(backoff_ms));
                    tokio::select! {
                        biased;
                        _ = wait_shutdown_ref(&mut shutdown_rx) => break,
                        _ = sleep => {}
                    }
                }
                continue;
            }

            if runtime.is_parity_dirty() && last_auto_catchup.elapsed() >= catchup_interval {
                last_auto_catchup = Instant::now();
                if let Ok(_guard) = runtime.cutover_lock.try_lock() {
                    let _ = runtime
                        .run_catchup_locked(&[], ParityDigestMode::Full, None)
                        .await;
                }
            }

            let wait_dur = if runtime.is_parity_dirty() {
                catchup_interval
            } else {
                Duration::from_secs(30)
            };
            tokio::select! {
                biased;
                _ = wait_shutdown_ref(&mut shutdown_rx) => break,
                _ = runtime.queue_notify.notified() => {}
                _ = tokio::time::sleep(wait_dur) => {}
            }
        }
    })
}

pub(crate) fn mutation_value_sha256(op: &MutationOp) -> [u8; 32] {
    match op {
        MutationOp::Put { value } => Sha256::digest(value).into(),
        MutationOp::Remove => [0u8; 32],
    }
}

pub(crate) async fn wait_shutdown_ref(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}
