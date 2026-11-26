//! Supported resident KV1 client. Owns one connection, never a Store.
//! Operation identity, clock conversion, retry policy and reconciliation belong to the caller.
use crate::{
    frame, lp, Cursor, CODES, CONDITIONAL_PUT, GET, GET_MANY, HELLO, MAX_BATCH, MAX_FRAME, MAX_ID,
    MAX_VALUE, PUT, REMOVE,
};
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    time::Instant,
};

/// A transport must enforce the supplied whole-call deadline on every I/O.
pub trait Transport: Read + Write {
    fn deadline(&mut self, until: Instant) -> io::Result<()>;
}
impl Transport for TcpStream {
    fn deadline(&mut self, until: Instant) -> io::Result<()> {
        let remaining = until
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "client deadline"))?;
        self.set_read_timeout(Some(remaining))?;
        self.set_write_timeout(Some(remaining))
    }
}
#[cfg(windows)]
impl Transport for crate::pipe_io::Pipe {
    fn deadline(&mut self, until: Instant) -> io::Result<()> {
        self.set_deadline(until)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ack {
    Os = 1,
    Flush = 2,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedValue {
    pub version: u64,
    pub ack: Ack,
    pub bytes: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationReply {
    pub epoch: u64,
    pub version: u64,
    pub achieved: Ack,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub code: u16,
    pub name: String,
    pub transient: bool,
}
/// Transport/association/payload failure after mutation entry can hide a committed write.
/// Refusals 16 and 17 also retain this request's uncertainty.
#[derive(Debug)]
pub enum Error {
    Invalid(&'static str),
    Atomic(crate::atomic::PublicationError),
    Transport(io::Error),
    Protocol(&'static str),
    Refused(Refusal),
    Uncertain {
        epoch: u64,
        operation_id: String,
        cause: Box<Error>,
    },
    EpochChanged {
        expected: u64,
        actual: u64,
    },
    RetryWindowExpired,
    /// No stronger contract was selected; creating a legacy registration is not a fallback.
    SubscriptionContractUnavailable(crate::subscription_contract::Unavailable),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
type Result<T> = std::result::Result<T, Error>;
mod m3;
pub use m3::{Change, ScanPage, Subscription, SubscriptionStatus};
#[path = "policy.rs"]
mod policy_client;
pub use policy_client::{Policy, PolicySetAttempt, PolicySetRequest, PolicyTable};
#[path = "selected.rs"]
mod selected_client;
#[path = "timers.rs"]
mod timer_client;
pub use timer_client::*;
#[cfg(windows)]
mod listener;
#[cfg(windows)]
pub use listener::{ChangeListener, ReceiveError};

/// Explicit additive opt-ins. Existing Options struct literals remain supported.
#[derive(Clone, Copy, Debug, Default)]
pub struct AdditionalFeatures {
    pub timers: bool,
}
#[derive(Clone, Debug)]
pub struct SignalLimits {
    pub max_frame: usize,
    pub history_items: u64,
    pub history_bytes: u64,
    pub subscriptions: u16,
    pub queued_items: u16,
    pub subscription_bytes: u64,
    pub queue_bytes: u64,
    pub write_ms: u32,
}
#[derive(Clone, Debug, Default)]
pub struct FeatureLimits {
    pub signals: Option<SignalLimits>,
    pub timers: Option<TimerLimits>,
}
#[derive(Clone, Debug)]
pub struct SubscribeOptions {
    pub include_values: bool,
    pub resume: Option<[u8; 16]>,
    pub max_frame: usize,
}
fn parse<T>(v: std::result::Result<T, u16>) -> Result<T> {
    v.map_err(|_| Error::Protocol("malformed reply"))
}
pub(crate) fn identifier(s: &str) -> Result<()> {
    if s.is_empty() || s.len() > MAX_ID {
        Err(Error::Invalid("identifier length"))
    } else {
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct Options {
    pub identity: String,
    pub max_frame: usize,
    pub scans: bool,
    pub subscriptions: bool,
}
impl Options {
    pub fn new(identity: impl Into<String>, max_frame: usize) -> Self {
        Self {
            identity: identity.into(),
            max_frame,
            scans: false,
            subscriptions: false,
        }
    }
}
#[derive(Clone, Debug)]
pub struct Negotiated {
    pub epoch: u64,
    pub max_frame: usize,
    pub max_value: usize,
    pub max_batch: usize,
    pub server_mem_enabled: bool,
    pub retry_window_ms: u64,
    pub scans: bool,
    pub subscriptions: bool,
    pub refusals: BTreeMap<u16, Refusal>,
}
/// Both deadlines are supplied explicitly: server Unix milliseconds and local monotonic budget.
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    pub unix_ms: u64,
    pub local: Instant,
}
/// Stable caller-owned full mutation. Reuse this exact value for an explicit retry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationRequest {
    pub operation_id: String,
    pub namespace: String,
    pub key: String,
    pub value: Vec<u8>,
    pub ack: Ack,
    pub expected: Option<u64>,
}
/// Conditional tombstone, using the existing REMOVE opcode. Zero requires absence;
/// a nonzero expectation is an opaque live revision, never an application ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoveRequest {
    pub operation_id: String,
    pub namespace: String,
    pub key: String,
    pub ack: Ack,
    pub expected: u64,
}
/// Caller-owned removal identity and original retry context. Like Attempt, this
/// is in memory, not a durable journal or authority across clone/restore.
#[derive(Debug)]
pub struct RemoveAttempt {
    request: RemoveRequest,
    inner: Attempt,
}
impl RemoveAttempt {
    pub fn request(&self) -> &RemoveRequest {
        &self.request
    }
    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }
}
/// Retains the exact request and a conservative retry horizon from BEFORE its first send.
/// Keep this object after every uncertain result. It is an in-memory token, not a journal.
#[derive(Debug)]
pub struct Attempt<R = MutationRequest> {
    request: R,
    epoch: u64,
    first_send: Option<Instant>,
    retry_window_ms: u64,
}
impl<R> Attempt<R> {
    pub fn request(&self) -> &R {
        &self.request
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    fn check_horizon(&self, epoch: u64) -> Result<()> {
        if self.epoch != epoch {
            return Err(Error::EpochChanged {
                expected: self.epoch,
                actual: epoch,
            });
        }
        if self
            .first_send
            .is_some_and(|sent| sent.elapsed().as_millis() >= self.retry_window_ms as u128)
        {
            return Err(Error::RetryWindowExpired);
        }
        Ok(())
    }
}
pub struct Client<T: Transport> {
    io: Option<T>,
    request_id: u64,
    negotiated: Negotiated,
    pub(crate) usable: bool,
    feature_limits: FeatureLimits,
    finite_hints: bool,
}
// Own the bounded reply body through typed decoding. Constructed only after
// header association and status validation; decoders borrow past that header.
pub(crate) struct ReplyPayload(Vec<u8>);
impl ReplyPayload {
    // Call only after validating that this value ends at the reply boundary.
    fn into_tail(mut self, len: usize) -> Vec<u8> {
        let start = self.0.len() - len;
        if self.0.capacity() <= len.saturating_mul(2) {
            self.0.copy_within(start.., 0);
            self.0.truncate(len);
            self.0
        } else {
            // Tiny tails must not retain the rest of a large reply.
            self.0[start..].to_vec()
        }
    }
}
impl std::ops::Deref for ReplyPayload {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0[15..]
    }
}
impl<T: Transport> Client<T> {
    pub fn handshake(io: T, options: Options, until: Instant) -> Result<Self> {
        Self::handshake_with_features(io, options, AdditionalFeatures::default(), until)
    }
    pub fn handshake_with_features(
        io: T,
        options: Options,
        additional: AdditionalFeatures,
        until: Instant,
    ) -> Result<Self> {
        Self::handshake_inner(io, options, additional, until, 0)
    }
    /// Require the stronger process-owned subscription contract during HELLO.
    /// Version-one discovery cannot select the finite-hint profile. Use
    /// handshake_finite_hints for that explicit agreement; no fallback is made.
    pub fn handshake_requiring_owned_subscriptions(
        io: T,
        options: Options,
        additional: AdditionalFeatures,
        until: Instant,
    ) -> Result<Self> {
        Self::handshake_inner(io, options, additional, until, 1)
    }
    fn handshake_inner(
        io: T,
        options: Options,
        additional: AdditionalFeatures,
        until: Instant,
        contract_version: u8,
    ) -> Result<Self> {
        identifier(&options.identity)?;
        if options.identity.contains(';') {
            return Err(Error::Invalid("identity contains feature delimiter"));
        }
        if !(1024..=MAX_FRAME).contains(&options.max_frame) {
            return Err(Error::Invalid("max_frame"));
        }
        let mut this = Self {
            io: Some(io),
            request_id: 0,
            usable: true,
            feature_limits: FeatureLimits::default(),
            finite_hints:false,
            negotiated: Negotiated {
                epoch: 0,
                max_frame: options.max_frame,
                max_value: MAX_VALUE,
                max_batch: MAX_BATCH,
                server_mem_enabled: false,
                retry_window_ms: 0,
                scans: false,
                subscriptions: false,
                // No negotiated vocabulary exists before a successful HELLO.
                refusals: BTreeMap::new(),
            },
        };
        // Borrow static suffixes in wire order instead of growing the identity.
        // The current server recognizes scans only as the final feature.
        let features = [
            if additional.timers { ";timers=1" } else { "" },
            if options.subscriptions { ";subscribe=1" } else { "" },
            if contract_version==2 {crate::subscription_contract::FINITE_HELLO_FEATURE} else if contract_version==1 { crate::subscription_contract::HELLO_FEATURE } else { "" },
            if options.scans { ";scans=1" } else { "" },
        ];
        let identity_len = options.identity.len()
            + features.iter().map(|feature| feature.len()).sum::<usize>();
        if identity_len > MAX_ID {
            return Err(Error::Invalid("identifier length"));
        }
        // The complete identity is bounded before reserving the final frame.
        let payload_len = 4 + 1 + 4 + identity_len;
        let mut p = crate::codec::frame_header(HELLO, 0, payload_len);
        p.extend((options.max_frame as u32).to_le_bytes());
        p.push(0);
        p.extend((identity_len as u32).to_le_bytes());
        p.extend_from_slice(options.identity.as_bytes());
        for feature in features {
            p.extend_from_slice(feature.as_bytes());
        }
        // Preserve admission after encoding; local refusals perform no I/O.
        let id = this.begin_exchange(payload_len, until)?;
        p[9..17].copy_from_slice(&id.to_le_bytes());
        let b = this.exchange_bytes(HELLO, id, &p, until)?;
        let mut c = Cursor::new(&b);
        let epoch = parse(c.u64())?;
        let max_frame = parse(c.u32())? as usize;
        let max_value = parse(c.u32())? as usize;
        let max_batch = parse(c.u16())? as usize;
        let mem = parse(c.u8())?;
        let retry_window_ms = parse(c.u64())?;
        if !(1024..=options.max_frame).contains(&max_frame)
            || max_value == 0
            || max_value > MAX_VALUE
            || max_batch == 0
            || max_batch > MAX_BATCH
            || mem > 1
        {
            return Err(Error::Protocol("HELLO limits"));
        }
        let count = parse(c.u16())? as usize;
        // Expected codes are bounded protocol metadata, not owned reply data.
        let codes = match count {
            19 => 1..=19,
            22 if options.scans => 1..=22,
            24 if options.subscriptions => 1..=24,
            23 if additional.timers && !options.subscriptions => 1..=22,
            25 if additional.timers && options.subscriptions => 1..=25,
            _ => return Err(Error::Protocol("HELLO refusal count")),
        };
        let mut codes = codes.chain((count == 23).then_some(25));
        let mut refusals = BTreeMap::new();
        for _ in 0..count {
            let code = parse(c.u16())?;
            let transient = parse(c.u8())?;
            let name = parse(c.text())?;
            if transient > 1
                || CODES.iter().find(|v| v.0 == code).map(|v| (v.1, v.2))
                    != Some((name.as_str(), transient == 1))
                || refusals
                    .insert(
                        code,
                        Refusal {
                            code,
                            name,
                            transient: transient == 1,
                        },
                    )
                    .is_some()
            {
                return Err(Error::Protocol("HELLO refusal vocabulary"));
            }
        }
        if codes.any(|code| !refusals.contains_key(&code)) {
            return Err(Error::Protocol("HELLO missing code"));
        }
        if refusals.contains_key(&24) {
            if parse(c.u16())? != 1 {
                return Err(Error::Protocol("change version"));
            }
            let frame = parse(c.u32())?;
            let history = parse(c.u64())?;
            let history_bytes = parse(c.u64())?;
            let subscriptions = parse(c.u16())?;
            let items = parse(c.u16())?;
            let bytes = parse(c.u64())?;
            let queue = parse(c.u64())?;
            let write_ms = parse(c.u32())?;
            if frame < 1024
                || frame as usize > MAX_FRAME
                || subscriptions == 0
                || items == 0
                || bytes == 0
                || queue == 0
                || write_ms == 0
            {
                return Err(Error::Protocol("change limits"));
            }
            this.feature_limits.signals = Some(SignalLimits {
                max_frame: frame as usize,
                history_items: history,
                history_bytes,
                subscriptions,
                queued_items: items,
                subscription_bytes: bytes,
                queue_bytes: queue,
                write_ms,
            });
        }
        if refusals.contains_key(&25) {
            this.feature_limits.timers = Some(TimerLimits::parse(&mut c)?);
        }
        if contract_version==1 {
            let reason = crate::subscription_contract::decode_discovery(c.remainder())
                .map_err(|_| Error::Protocol("subscription contract discovery"))?;
            return Err(Error::SubscriptionContractUnavailable(reason));
        }
        if contract_version==2 {
            if !refusals.contains_key(&24) {return Err(Error::Protocol("finite hint refusal vocabulary"));}
            if c.remainder()!=crate::subscription_contract::FINITE_DISCOVERY {
                if let Ok(reason)=crate::subscription_contract::decode_discovery(c.remainder()) {return Err(Error::SubscriptionContractUnavailable(reason));}
                return Err(Error::Protocol("finite hint profile discovery"));
            }
            this.finite_hints=true;
        }else{parse(c.end())?;}
        this.negotiated = Negotiated {
            epoch,
            max_frame,
            max_value,
            max_batch,
            server_mem_enabled: mem == 1,
            retry_window_ms,
            scans: count >= 22,
            subscriptions: refusals.contains_key(&24),
            refusals,
        };
        Ok(this)
    }
    pub(crate) fn require_atomic_wire(&self) -> Result<()> {
        if self.finite_hints {
            return Err(Error::Invalid("atomic wire operations unavailable on a finite hint connection"));
        }
        Ok(())
    }

    pub fn negotiated(&self) -> &Negotiated {
        &self.negotiated
    }
    pub fn feature_limits(&self) -> &FeatureLimits {
        &self.feature_limits
    }
    pub fn is_usable(&self) -> bool {
        self.usable
    }
    pub(crate) fn retire_transport(&mut self) {
        self.usable = false;
        drop(self.io.take());
    }
    pub(crate) fn exchange(&mut self, op: u8, p: &[u8], until: Instant) -> Result<ReplyPayload> {
        let id = self.begin_exchange(p.len(), until)?;
        let bytes = frame(op, id, p);
        self.exchange_bytes(op, id, &bytes, until)
    }
    pub(crate) fn begin_exchange(&mut self, payload_len: usize, until: Instant) -> Result<u64> {
        if !self.usable {
            return Err(Error::Invalid("connection requires reconnect"));
        }
        if Instant::now() >= until {
            return Err(Error::Invalid("local deadline elapsed before send"));
        }
        if payload_len
            .checked_add(13)
            .filter(|n| *n <= self.negotiated.max_frame)
            .is_none()
        {
            return Err(Error::Invalid("frame limit"));
        }
        self.request_id = self
            .request_id
            .checked_add(1)
            .ok_or(Error::Invalid("request id exhausted"))?;
        Ok(self.request_id)
    }
    pub(crate) fn exchange_bytes(
        &mut self,
        op: u8,
        id: u64,
        bytes: &[u8],
        until: Instant,
    ) -> Result<ReplyPayload> {
        self.usable = false;
        let io = self
            .io
            .as_mut()
            .ok_or(Error::Invalid("connection requires reconnect"))?;
        let b = (|| -> Result<Vec<u8>> {
            let mut rest = bytes;
            while !rest.is_empty() {
                io.deadline(until).map_err(Error::Transport)?;
                let n = io.write(rest).map_err(Error::Transport)?;
                if n == 0 {
                    return Err(Error::Transport(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "zero write",
                    )));
                }
                rest = &rest[n..];
            }
            let mut size = [0; 4];
            Self::read_exact_before(io, &mut size, until)?;
            let size = u32::from_le_bytes(size) as usize;
            if !(15..=self.negotiated.max_frame).contains(&size) {
                return Err(Error::Protocol("reply frame bound"));
            }
            let mut b = vec![0; size];
            Self::read_exact_before(io, &mut b, until)?;
            Ok(b)
        })()?;
        let mut c = Cursor::new(&b);
        if (
            parse(c.u16())?,
            parse(c.u16())?,
            parse(c.u8())?,
            parse(c.u64())?,
        ) != (1, 0, op | 128, id)
        {
            return Err(Error::Protocol("reply association"));
        }
        let code = parse(c.u16())?;
        if code != 0 {
            parse(c.end())?;
            let refusal = self
                .negotiated
                .refusals
                .get(&code)
                .cloned()
                .or_else(|| {
                    // HELLO may refuse before negotiation. Materialize only
                    // that actual legacy refusal, preserving its vocabulary.
                    if op != HELLO {
                        return None;
                    }
                    CODES[..19].iter().find(|entry| entry.0 == code)
                        .map(|&(code, name, transient)| Refusal {
                            code,
                            name: name.into(),
                            transient,
                        })
                })
                .ok_or(Error::Protocol("unknown refusal"))?;
            self.usable = true;
            return Err(Error::Refused(refusal));
        }
        self.usable = true;
        Ok(ReplyPayload(b))
    }
    fn read_exact_before(io: &mut T, mut b: &mut [u8], until: Instant) -> Result<()> {
        while !b.is_empty() {
            io.deadline(until).map_err(Error::Transport)?;
            let n = io.read(b).map_err(Error::Transport)?;
            if n == 0 {
                return Err(Error::Transport(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "partial reply",
                )));
            }
            b = &mut b[n..];
        }
        Ok(())
    }
    pub(crate) fn prefix(&self, operation_id: &str, namespace: &str, d: Deadline) -> Result<Vec<u8>> {
        identifier(operation_id)?;
        identifier(namespace)?;
        let mut p = d.unix_ms.to_le_bytes().to_vec();
        lp(&mut p, operation_id.as_bytes());
        lp(&mut p, namespace.as_bytes());
        Ok(p)
    }
    fn borrowed_value<'a>(&self, c: &mut Cursor<'a>) -> Result<Option<(u64, Ack, &'a [u8])>> {
        match parse(c.u8())? {
            0 => Ok(None),
            1 => {
                let version = parse(c.u64())?;
                let ack = match parse(c.u8())? {
                    1 => Ack::Os,
                    2 => Ack::Flush,
                    _ => return Err(Error::Protocol("committed ack")),
                };
                if version == 0 {
                    return Err(Error::Protocol("zero version"));
                }
                Ok(Some((
                    version,
                    ack,
                    parse(c.bytes(self.negotiated.max_value))?,
                )))
            }
            _ => Err(Error::Protocol("presence flag")),
        }
    }
    fn value(&self, c: &mut Cursor<'_>) -> Result<Option<CommittedValue>> {
        self.borrowed_value(c).map(|value| {
            value.map(|(version, ack, bytes)| CommittedValue {
                version,
                ack,
                bytes: bytes.to_vec(),
            })
        })
    }
    pub fn get(
        &mut self,
        namespace: &str,
        key: &str,
        operation_id: &str,
        d: Deadline,
    ) -> Result<Option<CommittedValue>> {
        identifier(key)?;
        identifier(operation_id)?;
        identifier(namespace)?;
        // All three identifiers are bounded before reserving the final frame.
        let payload_len = 8 + 3 * 4 + operation_id.len() + namespace.len() + key.len();
        let mut p = crate::codec::frame_header(GET, 0, payload_len);
        p.extend(d.unix_ms.to_le_bytes());
        lp(&mut p, operation_id.as_bytes());
        lp(&mut p, namespace.as_bytes());
        lp(&mut p, key.as_bytes());
        // Keep admission after encoding; rejected reads consume no request ID.
        let id = self.begin_exchange(payload_len, d.local)?;
        p[9..17].copy_from_slice(&id.to_le_bytes());
        let b = self.exchange_bytes(GET, id, &p, d.local)?;
        let mut c = Cursor::new(&b);
        let result = (|| {
            let value = self.borrowed_value(&mut c)?;
            parse(c.end())?;
            // Only a fully validated GET has one value at the end of its body.
            Ok(value.map(|(version, ack, bytes)| (version, ack, bytes.len())))
        })();
        if result.is_err() {
            self.usable = false;
        }
        result.map(|value| {
            value.map(|(version, ack, len)| {
                CommittedValue {
                    version,
                    ack,
                    bytes: b.into_tail(len),
                }
            })
        })
    }
    pub fn get_many(
        &mut self,
        namespace: &str,
        keys: &[&str],
        operation_id: &str,
        d: Deadline,
    ) -> Result<Vec<Option<CommittedValue>>> {
        if keys.is_empty() || keys.len() > self.negotiated.max_batch {
            return Err(Error::Invalid("get_many count"));
        }
        identifier(operation_id)?;
        identifier(namespace)?;
        for key in keys {
            identifier(key)?;
        }
        // Negotiated count and identifier bounds cap every input before reserve.
        let payload_len = 8 + 2 * 4 + operation_id.len() + namespace.len() + 2
            + keys.iter().map(|key| 4 + key.len()).sum::<usize>();
        let mut p = crate::codec::frame_header(GET_MANY, 0, payload_len);
        p.extend(d.unix_ms.to_le_bytes());
        lp(&mut p, operation_id.as_bytes());
        lp(&mut p, namespace.as_bytes());
        p.extend((keys.len() as u16).to_le_bytes());
        for key in keys {
            lp(&mut p, key.as_bytes());
        }
        // Retain connection/deadline/frame admission and request-ID continuity.
        let id = self.begin_exchange(payload_len, d.local)?;
        p[9..17].copy_from_slice(&id.to_le_bytes());
        let b = self.exchange_bytes(GET_MANY, id, &p, d.local)?;
        let mut c = Cursor::new(&b);
        let result = (|| {
            if parse(c.u16())? as usize != keys.len() {
                return Err(Error::Protocol("get_many count association"));
            }
            let mut out = Vec::with_capacity(keys.len());
            let mut tail = None;
            for (index, _) in keys.iter().enumerate() {
                if index + 1 == keys.len() {
                    // Only a present final value can end at the reply boundary.
                    out.push(self.borrowed_value(&mut c)?.map(|(version, ack, bytes)| {
                        tail = Some(bytes.len());
                        CommittedValue { version, ack, bytes: Vec::new() }
                    }));
                } else {
                    out.push(self.value(&mut c)?);
                }
            }
            parse(c.end())?;
            Ok((out, tail))
        })();
        if result.is_err() {
            self.usable = false;
        }
        result.map(|(mut out, tail)| {
            if let Some(len) = tail {
                out.last_mut().expect("validated nonempty batch")
                    .as_mut().expect("validated present final value").bytes = b.into_tail(len);
            }
            out
        })
    }
    /// Prepare a NEW intended operation. Do not prepare again to reset an uncertain
    /// Attempt's epoch/horizon. After loss of the token, reconcile the deterministic
    /// key and embedded caller identity before deciding whether another send is safe.
    pub fn prepare(&self, request: MutationRequest) -> Result<Attempt> {
        identifier(&request.operation_id)?;
        identifier(&request.namespace)?;
        identifier(&request.key)?;
        if request.value.len() > self.negotiated.max_value {
            return Err(Error::Invalid("value limit"));
        }
        Ok(Attempt {
            request,
            epoch: self.negotiated.epoch,
            first_send: None,
            retry_window_ms: self.negotiated.retry_window_ms,
        })
    }
    /// Prepare a NEW removal. Retain this token after an uncertain send; do not
    /// prepare again to erase its original epoch, retry window or uncertainty.
    pub fn prepare_remove(&self, request: RemoveRequest) -> Result<RemoveAttempt> {
        self.validate_remove(&request)?;
        let inner = self.prepare(MutationRequest {
            operation_id: request.operation_id.clone(),
            namespace: request.namespace.clone(),
            key: request.key.clone(),
            value: Vec::new(),
            ack: request.ack,
            expected: Some(request.expected),
        })?;
        Ok(RemoveAttempt { request, inner })
    }
    /// Pure REMOVE frame preflight; performs no transport I/O.
    pub fn validate_remove(&self, request: &RemoveRequest) -> Result<usize> {
        identifier(&request.operation_id)?;
        identifier(&request.namespace)?;
        identifier(&request.key)?;
        let size = 13 + 8 + 3 * 4 + request.operation_id.len()
            + request.namespace.len() + request.key.len() + 1 + 8;
        if size > self.negotiated.max_frame {
            return Err(Error::Invalid("frame limit"));
        }
        Ok(size)
    }
    fn mutation_payload_len(
        &self,
        operation_id: &str,
        namespace: &str,
        key: &str,
        value_len: usize,
        conditional: bool,
    ) -> Result<usize> {
        identifier(key)?;
        if value_len > self.negotiated.max_value {
            return Err(Error::Invalid("value limit"));
        }
        identifier(operation_id)?;
        identifier(namespace)?;
        // All variable lengths are validated above. The payload contains the
        // deadline, three identifiers, acknowledgment, optional CAS comparator
        // and the length-prefixed value; the frame header is separate.
        Ok(8 + 3 * 4
            + operation_id.len()
            + namespace.len()
            + key.len()
            + 1
            + if conditional { 8 } else { 0 }
            + 4
            + value_len)
    }
    fn mutation_frame(&self, op: u8, m: &MutationRequest, d: Deadline) -> Result<Vec<u8>> {
        // Fill the request ID only after admission; refused sends consume none.
        let payload_len = self.mutation_payload_len(
            &m.operation_id,
            &m.namespace,
            &m.key,
            m.value.len(),
            m.expected.is_some(),
        )? - if op == REMOVE { 4 + m.value.len() } else { 0 };
        let mut p = crate::codec::frame_header(op, 0, payload_len);
        p.extend(d.unix_ms.to_le_bytes());
        lp(&mut p, m.operation_id.as_bytes());
        lp(&mut p, m.namespace.as_bytes());
        lp(&mut p, m.key.as_bytes());
        p.push(m.ack as u8);
        if let Some(expected) = m.expected {
            p.extend(expected.to_le_bytes());
        }
        if op != REMOVE {
            lp(&mut p, &m.value);
        }
        Ok(p)
    }
    /// Pure preflight sharing the encoder's validated size. No send or readiness claim.
    pub fn validate_mutation(&self, request: &MutationRequest) -> Result<usize> {
        self.validate_mutation_size(
            &request.operation_id,
            &request.namespace,
            &request.key,
            request.value.len(),
            request.expected.is_some(),
        )
    }
    pub(crate) fn validate_mutation_size(
        &self,
        operation_id: &str,
        namespace: &str,
        key: &str,
        value_len: usize,
        conditional: bool,
    ) -> Result<usize> {
        let size = self.mutation_payload_len(operation_id, namespace, key, value_len, conditional)? + 13;
        if size > self.negotiated.max_frame {
            return Err(Error::Invalid("frame limit"));
        }
        Ok(size)
    }
    /// No retry, reconnect, new identity or backing-state fallback is performed.
    /// Reusing an Attempt is allowed only in its original epoch and conservative retry window.
    pub fn mutate(&mut self, attempt: &mut Attempt, d: Deadline) -> Result<MutationReply> {
        let op = if attempt.request.expected.is_some() {
            CONDITIONAL_PUT
        } else {
            PUT
        };
        self.send_mutation(attempt, op, d)
    }
    /// Publish one conditional tombstone with explicit achieved Ack. This is
    /// not an atomic multi-key operation, archive, or cancellation of a prior send.
    pub fn remove(&mut self, attempt: &mut RemoveAttempt, d: Deadline) -> Result<MutationReply> {
        self.send_mutation(&mut attempt.inner, REMOVE, d)
    }
    fn send_mutation(&mut self, attempt: &mut Attempt, op: u8, d: Deadline) -> Result<MutationReply> {
        attempt.check_horizon(self.negotiated.epoch)?;
        let m = &attempt.request;
        let mut bytes = self.mutation_frame(op, m, d)?;
        if !self.usable {
            return Err(Error::Invalid("connection requires reconnect"));
        }
        if Instant::now() >= d.local {
            return Err(Error::Invalid("local deadline elapsed before send"));
        }
        if bytes.len() - 4 > self.negotiated.max_frame {
            return Err(Error::Invalid("frame limit"));
        }
        attempt.first_send.get_or_insert_with(Instant::now);
        let result = self
            .begin_exchange(bytes.len() - 17, d.local)
            .and_then(|id| {
                // The complete frame starts with length/version/opcode, then ID.
                bytes[9..17].copy_from_slice(&id.to_le_bytes());
                self.exchange_bytes(op, id, &bytes, d.local)
            })
            .and_then(|b| {
                let mut c = Cursor::new(&b);
                let version = parse(c.u64())?;
                let ack = parse(c.u8())?;
                parse(c.end())?;
                if version == 0 || ack != m.ack as u8 {
                    return Err(Error::Protocol("mutation achieved acknowledgement"));
                }
                Ok(MutationReply {
                    epoch: self.negotiated.epoch,
                    version,
                    achieved: m.ack,
                })
            });
        match result {
            Ok(v) => Ok(v),
            Err(e @ Error::Invalid(_)) => Err(e),
            Err(Error::Refused(r)) if !matches!(r.code, 16 | 17) => Err(Error::Refused(r)),
            Err(e) => {
                self.usable = false;
                Err(Error::Uncertain {
                    epoch: attempt.epoch,
                    operation_id: m.operation_id.clone(),
                    cause: Box::new(e),
                })
            }
        }
    }
}
impl Client<TcpStream> {
    /// Explicit foreground/test TCP endpoint; no service or account discovery.
    pub fn connect_tcp(address: SocketAddr, options: Options, until: Instant) -> Result<Self> {
        Self::connect_tcp_with_features(address, options, AdditionalFeatures::default(), until)
    }
    pub fn connect_tcp_with_features(
        address: SocketAddr,
        options: Options,
        additional: AdditionalFeatures,
        until: Instant,
    ) -> Result<Self> {
        let budget = until
            .checked_duration_since(Instant::now())
            .ok_or(Error::Invalid("connect deadline"))?;
        let io = TcpStream::connect_timeout(&address, budget).map_err(Error::Transport)?;
        Self::handshake_with_features(io, options, additional, until)
    }
}
#[cfg(windows)]
impl Client<crate::pipe_io::Pipe> {
    pub fn connect_pipe(path: &std::path::Path, options: Options, until: Instant) -> Result<Self> {
        Self::connect_pipe_with_features(path, options, AdditionalFeatures::default(), until)
    }
    pub fn connect_pipe_with_features(
        path: &std::path::Path,
        options: Options,
        additional: AdditionalFeatures,
        until: Instant,
    ) -> Result<Self> {
        let raw = path.to_string_lossy();
        if !crate::transport::local_pipe::is_valid_local_pipe_name(&raw, false) {
            return Err(Error::Invalid("explicit local named pipe required"));
        }
        let file = loop {
            if Instant::now() >= until {
                return Err(Error::Invalid("connect deadline"));
            }
            match crate::platform::open_overlapped_pipe(path, true) {
                Ok(file) => break file,
                Err(e) if matches!(e.raw_os_error(), Some(2 | 231)) => {
                    std::thread::sleep(
                        std::time::Duration::from_millis(2)
                            .min(until.saturating_duration_since(Instant::now())),
                    );
                }
                Err(e) => return Err(Error::Transport(e)),
            }
        };
        if !crate::platform::is_pipe_handle(&file) {
            return Err(Error::Invalid("explicit local named pipe required"));
        }
        Self::handshake_with_features(
            crate::pipe_io::Pipe::new(file, 1),
            options,
            additional,
            until,
        )
    }
}

#[path = "finite.rs"]
mod finite_client;
pub use finite_client::{HintAttempt,HintCreationState};

#[cfg(test)]
mod client_validation_tests {
    use super::*;
    #[cfg(windows)]
    #[test]
    fn connect_pipe_rejects_traversal_and_invalid_local_pipe_names() {
        let until = Instant::now() + std::time::Duration::from_millis(50);
        for bad in [
            r"\\.\pipe\",
            r"\\.\pipe\..\other",
            r"\\.\pipe\.",
            r"\\.\pipe\a/b",
            r"\\.\pipe\a:b",
            r"\\.\pipe\trailing.",
            r"\\.\pipe\trailing ",
            r"\\server\pipe\name",
            "\\\\.\\pipe\\nul\0byte",
        ] {
            let err = Client::connect_pipe(std::path::Path::new(bad), Options::new("test", 4096), until)
                .err()
                .unwrap_or_else(|| panic!("should reject {bad:?}"));
            assert!(
                matches!(err, Error::Invalid("explicit local named pipe required")),
                "unexpected error for {bad:?}: {err:?}"
            );
        }
    }
    #[test]
    fn scan_replies_enforce_header_inclusive_max_bytes() {
        struct MockReply {
            reply: io::Cursor<Vec<u8>>,
        }
        impl Read for MockReply {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                self.reply.read(buf)
            }
        }
        impl Write for MockReply {
            fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                Ok(b.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl Transport for MockReply {
            fn deadline(&mut self, _: Instant) -> io::Result<()> {
                Ok(())
            }
        }
        let make_client = |opcode: u8, payload_len: usize| {
            let body = vec![0u8; payload_len];
            let wire = crate::reply(opcode, 1, 0, &body);
            Client {
                io: Some(MockReply {
                    reply: io::Cursor::new(wire),
                }),
                request_id: 0,
                negotiated: Negotiated {
                    epoch: 1,
                    max_frame: 1024,
                    max_value: 1024,
                    max_batch: 8,
                    server_mem_enabled: false,
                    retry_window_ms: 1000,
                    scans: true,
                    subscriptions: false,
                    refusals: BTreeMap::new(),
                },
                usable: true,
                feature_limits: FeatureLimits::default(),
                finite_hints: false,
            }
        };
        let d = Deadline {
            unix_ms: crate::now_ms() + 10_000,
            local: Instant::now() + std::time::Duration::from_secs(10),
        };
        let snap = [7u8; 16];
        // payload_len = 55 <= max_bytes (64), but 55 + 15 = 70 > 64.
        let mut c1 = make_client(crate::SCAN_PREFIX, 55);
        let err1 = c1
            .scan_prefix("n", "", &snap, &[], 1, 64, "op1", d)
            .unwrap_err();
        assert!(
            matches!(err1, Error::Protocol("scan page bytes")),
            "expected Protocol(scan page bytes), got {err1:?}"
        );
        let mut c2 = make_client(crate::SCAN_RANGE, 55);
        let err2 = c2
            .scan_range("n", "a", "z", &snap, &[], 1, 64, "op2", d)
            .unwrap_err();
        assert!(
            matches!(err2, Error::Protocol("scan page bytes")),
            "expected Protocol(scan page bytes), got {err2:?}"
        );
    }
}
