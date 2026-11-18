// Logical allocation ownership for complete atomic populations.
use std::sync::atomic::AtomicUsize;

/// A finite caller-owned allocation account. Clones share the same allowance.
/// This is managed backing-storage accounting, not a measurement of free RAM.
/// Store capture creates a separate account backed by its actual scan reservation.
#[derive(Clone)]
pub struct OriginalBudget(Arc<Pool>);
struct Pool {
    limit: usize,
    used: AtomicUsize,
    _store: Option<crate::snapshots::ScanCharge>,
}
impl std::fmt::Debug for OriginalBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalBudget")
            .field("limit", &self.0.limit)
            .field("used", &self.used())
            .finish()
    }
}
impl OriginalBudget {
    pub fn new(bytes: usize) -> Self {
        Self::backed(bytes, None)
    }
    pub fn used(&self) -> usize {
        self.0.used.load(Ordering::Acquire)
    }
    pub(crate) fn backed(bytes: usize, store: Option<crate::snapshots::ScanCharge>) -> Self {
        Self(Arc::new(Pool {
            limit: bytes,
            used: AtomicUsize::new(0),
            _store: store,
        }))
    }
    fn reserve(&self, bytes: usize) -> Result<Reservation, PublicationError> {
        self.reserve_available(bytes).map_err(|_|PublicationError::Resource(7))
    }
    /// Preserve the actual refusal observation for another typed resource
    /// consumer; do not reread a concurrently changing counter as its cause.
    pub(crate) fn reserve_available(&self, bytes: usize) -> Result<Reservation, usize> {
        let mut used = self.used();
        loop {
            let next = used
                .checked_add(bytes)
                .filter(|n| *n <= self.0.limit)
                .ok_or(self.0.limit.saturating_sub(used))?;
            match self
                .0
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => used = actual,
            }
        }
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
}
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: OriginalBudget,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.0.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
#[derive(Debug)]
struct Population {
    rows: Vec<(String, u64)>,
    guard: Option<crate::atomic_population::PopulationGuard>,
    _charge: Reservation,
}
#[derive(Debug)]
struct OriginalData {
    namespace: String,
    operation_id: String,
    epoch: u64,
    expires_mono: u64,
    population: Arc<Population>,
    ack: AtomicAck,
    changes: Vec<FinalChange>,
    _charge: Reservation,
}
fn add(n: usize, m: usize) -> Result<usize, PublicationError> {
    n.checked_add(m).ok_or(PublicationError::Resource(7))
}
fn retained_outcome(
    fp: &[u8; 32],
    record: Option<&Dedup>,
    retired: bool,
    mono: u64,
) -> Result<Option<Outcome>, PublicationError> {
    if let Some(d) = record {
        if d.fingerprint != *fp {
            return Err(PublicationError::IdentityMismatch);
        }
        if mono > d.until_mono {
            return Err(PublicationError::RetryExpired);
        }
        return match &d.outcome {
            Some(o) if o.code == 0 => Ok(Some(o.clone())),
            _ => Err(PublicationError::Uncertain),
        };
    }
    if retired {
        return Err(PublicationError::RetryExpired);
    }
    Ok(None)
}
fn valid_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= crate::MAX_ID && !s.contains('\0')
}
fn valid_namespace(s: &str) -> Result<(), PublicationError> {
    if !valid_id(s) {
        return Err(PublicationError::Invalid);
    }
    if s == crate::POLICY_NS || s == crate::timers::NS {
        return Err(PublicationError::Unsupported);
    }
    Ok(())
}
fn request_charge<'a>(
    ns: &str,
    id: &str,
    changes: impl Iterator<Item = (&'a str, Option<&'a [u8]>)>,
) -> Result<usize, PublicationError> {
    let mut bytes = add(512, add(ns.len(), id.len())?)?;
    for (key, value) in changes {
        bytes = add(
            bytes,
            add(128, add(key.len(), value.map_or(0, <[u8]>::len))?)?,
        )?;
    }
    Ok(bytes)
}

/// Validated original borrowing the caller's immutable frame. Validation has
/// constant auxiliary storage; a wire count or budget never creates a grant.
pub struct BorrowedOriginal<'a> {
    bytes: &'a [u8],
    namespace: &'a str,
    operation_id: &'a str,
    epoch: u64,
    expires_mono: u64,
    ack: AtomicAck,
    population: &'a [u8],
    population_count: usize,
    guard: Option<crate::atomic_population::PopulationGuard>,
    changes: &'a [u8],
    change_count: usize,
}
impl<'a> BorrowedOriginal<'a> {
    /// Borrow an exact, enumerable MAR1 original.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self, PublicationError> {
        if bytes.get(..4) != Some(b"MAR1") {
            return Err(PublicationError::Invalid);
        }
        Self::from_canonical_bytes(bytes)
    }
    pub(crate) fn from_canonical_bytes(bytes: &'a [u8]) -> Result<Self, PublicationError> {
        Self::decode(bytes, usize::MAX)
    }
    fn from_wire_bytes(bytes: &'a [u8]) -> Result<Self, PublicationError> {
        Self::decode(bytes, MAX_POPULATION)
    }
    fn decode(bytes: &'a [u8], enumerable_max: usize) -> Result<Self, PublicationError> {
        let wide = match bytes.get(..4) {
            Some(b"MAR1") => false,
            Some(b"MAR2") => true,
            _ => return Err(PublicationError::Invalid),
        };
        if bytes
            .len()
            .checked_add(if wide { 0 } else { 29 })
            .is_none_or(|n| n > crate::MAX_FRAME / 3)
        {
            return Err(PublicationError::Invalid);
        }
        fn parse(bytes: &[u8], wide: bool, enumerable_max: usize) -> Result<BorrowedOriginal<'_>, u16> {
            let mut c = Cursor::new(&bytes[4..]);
            let namespace = c.bound_ref()?;
            let operation_id = c.bound_ref()?;
            if !valid_id(namespace) || !valid_id(operation_id) {
                return Err(3);
            }
            let epoch = c.u64()?;
            let expires_mono = c.u64()?;
            let ack = match c.u8()? {
                1 => AtomicAck::Os,
                2 => AtomicAck::Flush,
                _ => return Err(3),
            };
            let guard = if wide {
                let guard = crate::atomic_population::PopulationGuard::decode(&mut c)?;
                if epoch == 0 || expires_mono == 0 || bytes.len() > guard.limits.max_original_bytes as usize {
                    return Err(3);
                }
                Some(guard)
            } else {
                None
            };
            let population_count = if wide { 0 } else { c.u32()? as usize };
            // Every nonempty key/revision requires at least 13 literal bytes.
            if population_count > enumerable_max || population_count > c.remainder().len() / 13 {
                return Err(3);
            }
            let population = c.remainder();
            let mut previous = None;
            for _ in 0..population_count {
                let key = c.bound_ref()?;
                if !valid_id(key) || previous.is_some_and(|p| p >= key) || c.u64()? == 0 {
                    return Err(3);
                }
                previous = Some(key);
            }
            let population = &population[..population.len() - c.remainder().len()];
            let change_count = c.u16()? as usize;
            if change_count == 0
                || change_count > guard.as_ref().map_or(MAX_CHANGES, |g| usize::from(g.limits.max_changes))
                || change_count > c.remainder().len() / 10
            {
                return Err(3);
            }
            let changes = c.remainder();
            if change_count <= MAX_CHANGES {
                validate_change_keys(changes, &mut [0; MAX_CHANGES][..change_count])?;
            } else {
                validate_wide_change_keys(changes, change_count)?;
            }
            Ok(BorrowedOriginal {
                bytes,
                namespace,
                operation_id,
                epoch,
                expires_mono,
                ack,
                population,
                population_count,
                guard,
                changes,
                change_count,
            })
        }
        let parsed = parse(bytes, wide, enumerable_max).map_err(|_| PublicationError::Invalid)?;
        valid_namespace(parsed.namespace)?;
        Ok(parsed)
    }
    pub fn namespace(&self) -> &'a str {
        self.namespace
    }
    pub fn operation_id(&self) -> &'a str {
        self.operation_id
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn expires_mono(&self) -> u64 {
        self.expires_mono
    }
    pub fn requested_ack(&self) -> AtomicAck {
        self.ack
    }
    pub fn change_count(&self) -> usize {
        self.change_count
    }
    /// Managed backing bytes required by to_owned, measured without allocation.
    pub fn allocation_bytes(&self) -> Result<usize, PublicationError> {
        let population = self
            .population()
            .try_fold(512, |n, (k, _)| add(n, add(64, k.len())?))?;
        add(
            population,
            request_charge(self.namespace, self.operation_id, self.changes())?,
        )
    }
    pub fn population(&self) -> impl Iterator<Item = (&'a str, u64)> {
        let mut c = Cursor::new(self.population);
        (0..self.population_count).map(move |_| {
            (
                c.bound_ref().expect("validated key"),
                c.u64().expect("validated revision"),
            )
        })
    }
    pub fn changes(&self) -> impl Iterator<Item = (&'a str, Option<&'a [u8]>)> {
        let mut c = Cursor::new(self.changes);
        (0..self.change_count).map(move |_| {
            let key = c.bound_ref().expect("validated target");
            let remove = c.u8().expect("validated kind");
            let value = c.bytes(crate::MAX_VALUE).expect("validated value");
            (key, if remove == 1 { None } else { Some(value) })
        })
    }
    fn matches_current<'b>(&self, current: impl Iterator<Item = (&'b str, u64)>) -> bool
    where
        'a: 'b,
    {
        if let Some(guard) = &self.guard {
            return crate::atomic_population::population_digest(current) == (guard.count, guard.digest);
        }
        current.eq(self.population())
    }
    pub fn to_owned(&self, budget: &OriginalBudget) -> Result<AtomicRequest, PublicationError> {
        let population_bytes = self
            .population()
            .try_fold(512, |n, (k, _)| add(n, add(64, k.len())?))?;
        let population_charge = budget.reserve(population_bytes)?;
        let charge = budget.reserve(request_charge(
            self.namespace,
            self.operation_id,
            self.changes(),
        )?)?;
        // Both reservations precede every growing allocation.
        let mut population = Vec::new();
        population
            .try_reserve_exact(self.population_count)
            .map_err(|_| PublicationError::Resource(7))?;
        population.extend(self.population().map(|(k, v)| (k.to_owned(), v)));
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(self.change_count)
            .map_err(|_| PublicationError::Resource(7))?;
        changes.extend(self.changes().map(|(k, v)| FinalChange {
            key: k.to_owned(),
            value: v.map(<[u8]>::to_vec),
        }));
        Ok(AtomicRequest {
            data: Arc::new(OriginalData {
                namespace: self.namespace.into(),
                operation_id: self.operation_id.into(),
                epoch: self.epoch,
                expires_mono: self.expires_mono,
                population: Arc::new(Population {
                    rows: population,
                    guard: self.guard.clone(),
                    _charge: population_charge,
                }),
                ack: self.ack,
                changes,
                _charge: charge,
            }),
        })
    }
    pub(crate) fn fingerprint(&self) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(self.bytes);
        h.finish()
    }
    fn mutation(&self, key: &str, value: Option<&[u8]>) -> Mutation {
        Mutation {
            ns: self.namespace.into(),
            key: key.into(),
            op_id: self.operation_id.into(),
            deadline: 0,
            ack: self.ack.byte(),
            expected: None,
            bytes: value.unwrap_or_default().to_vec(),
            remove: value.is_none(),
        }
    }
    pub(crate) fn mutations(&self) -> impl Iterator<Item = Mutation> + '_ {
        self.changes().map(|(k, v)| self.mutation(k, v))
    }
}

fn live_population<'a>(
    namespace: &'a str, committed: &'a store::Committed,
) -> impl Iterator<Item = (&'a str, u64)> {
    committed.range((namespace.to_owned(), String::new())..)
        .take_while(move |((ns, _), _)| ns == namespace)
        .filter(|(_, entry)| !entry.tombstone)
        .map(|((_, key), entry)| (key.as_str(), entry.version))
}

// Sort validated frame offsets, leaving the immutable original in caller order.
// The wider stack frame is used only for MAR2 requests above the MAR1 limit.
#[inline(never)]
fn validate_wide_change_keys(bytes: &[u8], count: usize) -> Result<(), u16> {
    let mut offsets = [0; u16::MAX as usize];
    validate_change_keys(bytes, &mut offsets[..count])
}

fn validate_change_keys(bytes: &[u8], offsets: &mut [u32]) -> Result<(), u16> {
    let mut c = Cursor::new(bytes);
    for offset in offsets.iter_mut() {
        *offset = (bytes.len() - c.remainder().len()) as u32;
        let key = c.bound_ref()?;
        let remove = c.u8()?;
        let value = c.bytes(crate::MAX_VALUE)?;
        if !valid_id(key) || remove > 1 || remove == 1 && !value.is_empty() {
            return Err(3);
        }
    }
    c.end()?;
    let key_at = |offset: u32| Cursor::new(&bytes[offset as usize..]).bound_ref().unwrap();
    offsets.sort_unstable_by(|left, right| key_at(*left).cmp(key_at(*right)));
    if offsets.windows(2).any(|pair| key_at(pair[0]) == key_at(pair[1])) {
        return Err(3);
    }
    Ok(())
}

// The caller holds the same complete committed-index interval for measurement
// and materialization. Tests provide an authored immutable interval, never a Store.
fn capture_complete<'a, T: Copy>(
    namespace: &str,
    epoch: u64,
    expires_mono: u64,
    budget: OriginalBudget,
    entries: impl Iterator<Item = (&'a str, usize, T)> + Clone,
    mut read: impl FnMut(&str, T) -> Result<Value, PublicationError>,
) -> Result<NamespaceView, PublicationError> {
    valid_namespace(namespace)?;
    let (count, rows_bytes, population_bytes) =
        capture_measure(namespace, entries.clone().map(|(k, n, _)| (k, n)))?;
    let row_charge = budget.reserve(rows_bytes)?;
    let population_charge = budget.reserve(population_bytes)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| PublicationError::Resource(7))?;
    let mut population = Vec::new();
    population
        .try_reserve_exact(count)
        .map_err(|_| PublicationError::Resource(7))?;
    for (key, len, entry) in entries {
        let value = read(key, entry)?;
        if value.version == 0 || value.tombstone || value.bytes.len() > len {
            return Err(PublicationError::Uncertain);
        }
        population.push((key.to_owned(), value.version));
        rows.push((key.to_owned(), value));
    }
    Ok(NamespaceView {
        namespace: namespace.into(),
        epoch,
        expires_mono,
        rows,
        population: Arc::new(Population {
            rows: population,
            guard: None,
            _charge: population_charge,
        }),
        budget,
        _charge: row_charge,
    })
}
fn capture_measure<'a>(
    namespace: &str,
    entries: impl Iterator<Item = (&'a str, usize)>,
) -> Result<(usize, usize, usize), PublicationError> {
    let mut count = 0usize;
    let mut rows = add(512, namespace.len())?;
    let mut population = 512usize;
    let mut encoded = add(64, namespace.len())?;
    let mut previous = None;
    for (key, len) in entries {
        if !valid_id(key) || previous.is_some_and(|p| p >= key) {
            return Err(PublicationError::Invalid);
        }
        previous = Some(key);
        count = add(count, 1)?;
        encoded = add(encoded, add(12, key.len())?)?;
        if encoded > crate::MAX_FRAME / 3 || count > u32::MAX as usize {
            return Err(PublicationError::Resource(4));
        }
        rows = add(
            rows,
            add(
                128,
                add(
                    key.len(),
                    len.checked_mul(2).ok_or(PublicationError::Resource(7))?,
                )?,
            )?,
        )?;
        population = add(population, add(64, key.len())?)?;
    }
    Ok((count, rows, population))
}
