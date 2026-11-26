//! Explicit bounded whole populations, paged internally using the existing snapshot registry.
use crate::{
    atomic::{AtomicAck, AtomicRequest, FinalChange, PublicationError as P},
    chunks::Sha256,
    client::{Client, Deadline, Error, Transport},
    codec::Cursor,
    Store, Value,
};
use std::sync::Arc;
pub(crate) const CAPTURE_WORKSPACE_BYTES: usize = 4 * (crate::MAX_FRAME / 3) + 4096;

/// Caller-selected work bounds. They never increase provider resource admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PopulationLimits {
    pub max_keys: u32,
    /// Conservative charge for pinned snapshot plus complete owning view.
    pub max_capture_bytes: u64,
    pub max_changes: u16,
    pub max_original_bytes: u32,
}

#[cfg(test)]
#[path = "../unit_tests/atomic/population_client_tests.rs"]
mod client_tests;
impl PopulationLimits {
    pub fn validate(&self) -> Result<(), P> {
        if self.max_keys == 0
            || self.max_capture_bytes == 0
            || self.max_changes == 0
            || self.max_original_bytes == 0
            || self.max_original_bytes as usize > crate::MAX_FRAME / 3
            || usize::try_from(self.max_capture_bytes).is_err()
        {
            return Err(P::Invalid);
        }
        Ok(())
    }
    pub(crate) fn encode(self, b: &mut Vec<u8>) {
        b.extend(self.max_keys.to_le_bytes());
        b.extend(self.max_capture_bytes.to_le_bytes());
        b.extend(self.max_changes.to_le_bytes());
        b.extend(self.max_original_bytes.to_le_bytes());
    }
    pub(crate) fn decode(c: &mut Cursor<'_>) -> Result<Self, u16> {
        let v = Self {
            max_keys: c.u32()?,
            max_capture_bytes: c.u64()?,
            max_changes: c.u16()?,
            max_original_bytes: c.u32()?,
        };
        v.validate().map_err(|_| 3u16)?;
        Ok(v)
    }
}
#[derive(Clone, Debug)]
pub(crate) struct PopulationGuard {
    pub count: u32,
    pub digest: [u8; 32],
    pub limits: PopulationLimits,
}
impl PopulationGuard {
    pub(crate) fn validate(&self) -> Result<(), P> {
        self.limits.validate()?;
        if self.count > self.limits.max_keys {
            return Err(P::Invalid);
        }
        Ok(())
    }
    pub(crate) fn encode(&self, b: &mut Vec<u8>) {
        b.extend(self.wire_bytes());
    }
    pub(crate) fn wire_bytes(&self) -> [u8; 54] {
        let mut b = [0; 54];
        b[..4].copy_from_slice(&self.count.to_le_bytes());
        b[4..36].copy_from_slice(&self.digest);
        b[36..40].copy_from_slice(&self.limits.max_keys.to_le_bytes());
        b[40..48].copy_from_slice(&self.limits.max_capture_bytes.to_le_bytes());
        b[48..50].copy_from_slice(&self.limits.max_changes.to_le_bytes());
        b[50..].copy_from_slice(&self.limits.max_original_bytes.to_le_bytes());
        b
    }
    pub(crate) fn decode(c: &mut Cursor<'_>) -> Result<Self, u16> {
        let count = c.u32()?;
        let mut digest = [0; 32];
        for b in &mut digest {
            *b = c.u8()?;
        }
        let v = Self {
            count,
            digest,
            limits: PopulationLimits::decode(c)?,
        };
        v.validate().map_err(|_| 3u16)?;
        Ok(v)
    }
}
pub(crate) fn population_digest<'a>(rows: impl Iterator<Item = (&'a str, u64)>) -> (u32, [u8; 32]) {
    let mut h = Sha256::new();
    h.update(b"MAP2");
    let mut count = 0u32;
    for (key, version) in rows {
        h.update(&(key.len() as u32).to_le_bytes());
        h.update(key.as_bytes());
        h.update(&version.to_le_bytes());
        count += 1;
    }
    h.update(&(count as u64).to_le_bytes());
    (count, h.finish())
}
/// Constructible only after complete ordered population validation. No partial-view prepare.
pub struct PopulationView {
    namespace: String,
    epoch: u64,
    expires: u64,
    guard: PopulationGuard,
    rows: Vec<(String, Value)>,
    charged_bytes: u64,
    _snapshot: Option<Arc<crate::snapshots::Snapshot>>,
}
impl PopulationView {
    pub fn charged_bytes(&self) -> u64 {
        self.charged_bytes
    }
    pub fn rows(&self) -> &[(String, Value)] {
        &self.rows
    }
    pub fn prepare(
        &self,
        id: &str,
        ack: AtomicAck,
        changes: Vec<FinalChange>,
    ) -> Result<AtomicRequest, P> {
        AtomicRequest::prepare_bounded(
            &self.namespace,
            id,
            self.epoch,
            self.expires,
            &self.guard,
            ack,
            changes,
        )
    }
}
struct Header {
    namespace: String,
    epoch: u64,
    expires: u64,
    guard: PopulationGuard,
    charged_bytes: u64,
    token: [u8; 16],
}
impl Header {
    fn encode(&self) -> Vec<u8> {
        let mut b = b"MAS2".to_vec();
        crate::lp(&mut b, self.namespace.as_bytes());
        b.extend(self.epoch.to_le_bytes());
        b.extend(self.expires.to_le_bytes());
        self.guard.encode(&mut b);
        b.extend(self.charged_bytes.to_le_bytes());
        b.extend(self.token);
        b
    }
    fn decode(b: &[u8]) -> Result<Self, u16> {
        if b.get(..4) != Some(b"MAS2") {
            return Err(3);
        }
        let mut c = Cursor::new(&b[4..]);
        let namespace = c.text()?;
        let epoch = c.u64()?;
        let expires = c.u64()?;
        let guard = PopulationGuard::decode(&mut c)?;
        let charged_bytes = c.u64()?;
        if charged_bytes > guard.limits.max_capture_bytes {
            return Err(3);
        }
        let mut token = [0; 16];
        for v in &mut token {
            *v = c.u8()?;
        }
        c.end()?;
        if namespace.contains('\0')
            || epoch == 0
            || expires == 0
            || u64::from_le_bytes(token[..8].try_into().unwrap()) != epoch
        {
            return Err(3);
        }
        Ok(Self {
            namespace,
            epoch,
            expires,
            guard,
            charged_bytes,
            token,
        })
    }
}
impl Store {
    fn population_snapshot(
        &self,
        ns: &str,
        limits: PopulationLimits,
    ) -> Result<(Header, Arc<crate::snapshots::Snapshot>), P> {
        limits.validate()?;
        if ns.is_empty() || ns.len() > crate::MAX_ID || ns.contains('\0') {
            return Err(P::Invalid);
        }
        if ns == crate::POLICY_NS || ns == crate::timers::NS {
            return Err(P::Unsupported);
        }
        let _gate = self.admission_gate.write().unwrap();
        {
            let committed = self.committed.read().unwrap();
            let state = self.state.lock().unwrap();
            if state.read_only
                || state.warming
                || self
                    .flags
                    .storage_error
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(P::Unavailable);
            }
            if !self.atomic_compatible(ns, &committed) {
                return Err(P::Unsupported);
            }
        }
        let (token, snapshot) = self
            .snapshot_open_bounded(
                ns,
                Some((limits.max_keys as usize, limits.max_capture_bytes as usize)),
            )
            .map_err(|e| if e == 20 { P::Expired } else { P::Resource(7) })?;
        let (count, digest) = population_digest(
            snapshot
                .rows
                .iter()
                .map(|(k, v)| (k.as_str(), v.entry.version)),
        );
        let expires = snapshot
            .created
            .saturating_add(self.config.snapshot_ttl_ms.min(self.config.retry_ms));
        Ok((
            Header {
                namespace: ns.into(),
                epoch: self.epoch,
                expires,
                guard: PopulationGuard {
                    count,
                    digest,
                    limits,
                },
                charged_bytes: snapshot._charge.bytes as u64,
                token: token.try_into().unwrap(),
            },
            snapshot,
        ))
    }
    pub(crate) fn atomic_population_header(
        &self,
        ns: &str,
        limits: PopulationLimits,
        max: usize,
    ) -> Result<Vec<u8>, P> {
        // Header length is known before allocating a retained capture.
        if 102usize.saturating_add(ns.len()) > max {
            return Err(P::Resource(4));
        }
        let (header, _snapshot) = self.population_snapshot(ns, limits)?;
        Ok(header.encode())
    }
    pub fn capture_namespace_bounded(
        &self,
        ns: &str,
        limits: PopulationLimits,
    ) -> Result<PopulationView, P> {
        let (header, snapshot) = self.population_snapshot(ns, limits)?;
        let result = (|| {
            let mut rows = Vec::with_capacity(header.guard.count as usize);
            for (at, (key, _)) in snapshot.rows.iter().enumerate() {
                rows.push((
                    key.clone(),
                    (*snapshot.load_at(self, at).map_err(|_| P::Uncertain)?).clone(),
                ));
            }
            if self.clock.mono_ms() >= header.expires {
                return Err(P::Expired);
            }
            Ok(PopulationView {
                namespace: header.namespace.clone(),
                epoch: header.epoch,
                expires: header.expires,
                guard: header.guard.clone(),
                charged_bytes: header.charged_bytes,
                rows,
                _snapshot: Some(snapshot.clone()),
            })
        })();
        // The returned in-process view retains its charge; registry custody is no longer needed.
        let released = self.snapshot_release(ns, &header.token);
        match result {
            Err(e) => Err(e),
            Ok(v) => {
                released.map_err(|_| P::Expired)?;
                Ok(v)
            }
        }
    }
}
impl<T: Transport> Client<T> {
    /// All pages, one original boundary, whole population digest. No partial success.
    /// Requires negotiated scans; otherwise refuses locally before opening a capture.
    pub fn capture_atomic_namespace_bounded(
        &mut self,
        ns: &str,
        operation: &str,
        limits: PopulationLimits,
        d: Deadline,
    ) -> Result<PopulationView, Error> {
        self.require_atomic_wire()?;
        limits.validate().map_err(Error::Atomic)?;
        // Paging and release must both be available before retaining a server view.
        if !self.negotiated().scans {
            return Err(Error::Invalid("scans unavailable"));
        }
        let mut p = self.prefix(operation, ns, d)?;
        limits.encode(&mut p);
        let b = self.exchange(crate::ATOMIC_POPULATION, &p, d.local)?;
        let header = match crate::atomic_wire::decode(&b).and_then(|b| {
            Header::decode(b).map_err(|_| Error::Protocol("atomic population header"))
        }) {
            Ok(header) => header,
            Err(e) => {
                if matches!(e, Error::Protocol(_)) {
                    self.usable = false;
                }
                return Err(e);
            }
        };
        if header.namespace != ns
            || header.epoch != self.negotiated().epoch
            || header.guard.limits != limits
        {
            self.usable = false;
            return Err(Error::Protocol("atomic population identity"));
        }
        let result = (|| {
            let mut rows = Vec::new();
            let mut cursor = Vec::new();
            let mut bytes = 256 + ns.len() + CAPTURE_WORKSPACE_BYTES;
            loop {
                let page = self.scan_population_page(ns, &header.token, &cursor, operation, d)?;
                if page.items.is_empty() && !page.done {
                    return Err(Error::Protocol("empty atomic continuation"));
                }
                for (key, v) in page.items {
                    if rows.last().is_some_and(|(k, _)| k >= &key) {
                        return Err(Error::Protocol("atomic population order"));
                    }
                    bytes = bytes
                        .checked_add(2 * key.len() + 640 + 2 * v.bytes.len())
                        .ok_or(Error::Protocol("atomic capture size"))?;
                    if rows.len() >= header.guard.count as usize
                        || bytes > limits.max_capture_bytes as usize
                    {
                        return Err(Error::Protocol("atomic capture bounds"));
                    }
                    rows.push((
                        key,
                        Value {
                            version: v.version,
                            ack: if v.ack == crate::client::Ack::Os {
                                1
                            } else {
                                2
                            },
                            bytes: v.bytes,
                            tombstone: false,
                        },
                    ));
                }
                if page.done {
                    break;
                }
                cursor = page.cursor;
            }
            let (count, digest) =
                population_digest(rows.iter().map(|(k, v)| (k.as_str(), v.version)));
            if count != header.guard.count
                || digest != header.guard.digest
                || bytes as u64 != header.charged_bytes
            {
                return Err(Error::Protocol("incomplete atomic population"));
            }
            Ok(PopulationView {
                namespace: header.namespace,
                epoch: header.epoch,
                expires: header.expires,
                guard: header.guard,
                charged_bytes: header.charged_bytes,
                rows,
                _snapshot: None,
            })
        })();
        // Cleanup is explicit, bounded and uses a fresh read-only deadline; preserve primary error.
        let cleanup = Deadline {
            unix_ms: crate::chunks::Clock::new(None).utc_ms() + 1000,
            local: std::time::Instant::now() + std::time::Duration::from_secs(1),
        };
        let released = self.snapshot_release(ns, &header.token, operation, cleanup);
        match result {
            Err(e) => {
                if matches!(e, Error::Protocol(_)) {
                    self.usable = false;
                }
                Err(e)
            }
            Ok(v) => {
                released?;
                Ok(v)
            }
        }
    }
}
