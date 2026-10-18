//! Supported resident KV1 client. Owns one connection, never a Store.
//! Operation identity, clock conversion, retry policy and reconciliation belong to the caller.
use crate::{
    frame, lp, Cursor, CODES, CONDITIONAL_PUT, GET, GET_MANY, HELLO, MAX_BATCH, MAX_FRAME, MAX_ID,
    MAX_VALUE, PUT,
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
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
type Result<T> = std::result::Result<T, Error>;
fn parse<T>(v: std::result::Result<T, u16>) -> Result<T> {
    v.map_err(|_| Error::Protocol("malformed reply"))
}
fn identifier(s: &str) -> Result<()> {
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
}
impl Options {
    pub fn new(identity: impl Into<String>, max_frame: usize) -> Self {
        Self {
            identity: identity.into(),
            max_frame,
            scans: false,
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
/// Retains the exact request and a conservative retry horizon from BEFORE its first send.
/// Keep this object after every uncertain result. It is an in-memory token, not a journal.
#[derive(Debug)]
pub struct Attempt {
    request: MutationRequest,
    epoch: u64,
    first_send: Option<Instant>,
    retry_window_ms: u64,
}
impl Attempt {
    pub fn request(&self) -> &MutationRequest {
        &self.request
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}
pub struct Client<T: Transport> {
    io: T,
    request_id: u64,
    negotiated: Negotiated,
    usable: bool,
}
impl<T: Transport> Client<T> {
    pub fn handshake(io: T, options: Options, until: Instant) -> Result<Self> {
        identifier(&options.identity)?;
        if options.identity.contains(';') {
            return Err(Error::Invalid("identity contains feature delimiter"));
        }
        if !(1024..=MAX_FRAME).contains(&options.max_frame) {
            return Err(Error::Invalid("max_frame"));
        }
        let mut this = Self {
            io,
            request_id: 0,
            usable: true,
            negotiated: Negotiated {
                epoch: 0,
                max_frame: options.max_frame,
                max_value: MAX_VALUE,
                max_batch: MAX_BATCH,
                server_mem_enabled: false,
                retry_window_ms: 0,
                scans: false,
                refusals: CODES[..19]
                    .iter()
                    .map(|&(code, name, transient)| {
                        (
                            code,
                            Refusal {
                                code,
                                name: name.into(),
                                transient,
                            },
                        )
                    })
                    .collect(),
            },
        };
        let identity = if options.scans {
            format!("{};scans=1", options.identity)
        } else {
            options.identity
        };
        identifier(&identity)?;
        let mut p = (options.max_frame as u32).to_le_bytes().to_vec();
        p.push(0);
        lp(&mut p, identity.as_bytes());
        let b = this.exchange(HELLO, &p, until)?;
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
        // This slice deliberately negotiates only legacy19 or scan22, without extension tails.
        if count != 19 && !(options.scans && count == 22) {
            return Err(Error::Protocol("HELLO refusal count"));
        }
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
        if (1..=count as u16).any(|code| !refusals.contains_key(&code)) {
            return Err(Error::Protocol("HELLO missing code"));
        }
        parse(c.end())?;
        this.negotiated = Negotiated {
            epoch,
            max_frame,
            max_value,
            max_batch,
            server_mem_enabled: mem == 1,
            retry_window_ms,
            scans: count == 22,
            refusals,
        };
        Ok(this)
    }
    pub fn negotiated(&self) -> &Negotiated {
        &self.negotiated
    }
    pub fn is_usable(&self) -> bool {
        self.usable
    }
    fn exchange(&mut self, op: u8, p: &[u8], until: Instant) -> Result<Vec<u8>> {
        if !self.usable {
            return Err(Error::Invalid("connection requires reconnect"));
        }
        if Instant::now() >= until {
            return Err(Error::Invalid("local deadline elapsed before send"));
        }
        if p.len()
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
        let id = self.request_id;
        let bytes = frame(op, id, p);
        self.usable = false;
        let b = (|| -> Result<Vec<u8>> {
            let mut rest = bytes.as_slice();
            while !rest.is_empty() {
                self.io.deadline(until).map_err(Error::Transport)?;
                let n = self.io.write(rest).map_err(Error::Transport)?;
                if n == 0 {
                    return Err(Error::Transport(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "zero write",
                    )));
                }
                rest = &rest[n..];
            }
            let mut size = [0; 4];
            self.read_exact_before(&mut size, until)?;
            let size = u32::from_le_bytes(size) as usize;
            if !(15..=self.negotiated.max_frame).contains(&size) {
                return Err(Error::Protocol("reply frame bound"));
            }
            let mut b = vec![0; size];
            self.read_exact_before(&mut b, until)?;
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
                .ok_or(Error::Protocol("unknown refusal"))?;
            self.usable = true;
            return Err(Error::Refused(refusal));
        }
        self.usable = true;
        Ok(b[15..].to_vec())
    }
    fn read_exact_before(&mut self, mut b: &mut [u8], until: Instant) -> Result<()> {
        while !b.is_empty() {
            self.io.deadline(until).map_err(Error::Transport)?;
            let n = self.io.read(b).map_err(Error::Transport)?;
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
    fn prefix(&self, operation_id: &str, namespace: &str, d: Deadline) -> Result<Vec<u8>> {
        identifier(operation_id)?;
        identifier(namespace)?;
        let mut p = d.unix_ms.to_le_bytes().to_vec();
        lp(&mut p, operation_id.as_bytes());
        lp(&mut p, namespace.as_bytes());
        Ok(p)
    }
    fn value(&self, c: &mut Cursor<'_>) -> Result<Option<CommittedValue>> {
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
                Ok(Some(CommittedValue {
                    version,
                    ack,
                    bytes: parse(c.bytes(self.negotiated.max_value))?.to_vec(),
                }))
            }
            _ => Err(Error::Protocol("presence flag")),
        }
    }
    pub fn get(
        &mut self,
        namespace: &str,
        key: &str,
        operation_id: &str,
        d: Deadline,
    ) -> Result<Option<CommittedValue>> {
        identifier(key)?;
        let mut p = self.prefix(operation_id, namespace, d)?;
        lp(&mut p, key.as_bytes());
        let b = self.exchange(GET, &p, d.local)?;
        let mut c = Cursor::new(&b);
        let result = (|| {
            let v = self.value(&mut c)?;
            parse(c.end())?;
            Ok(v)
        })();
        if result.is_err() {
            self.usable = false;
        }
        result
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
        let mut p = self.prefix(operation_id, namespace, d)?;
        p.extend((keys.len() as u16).to_le_bytes());
        for key in keys {
            identifier(key)?;
            lp(&mut p, key.as_bytes());
        }
        let b = self.exchange(GET_MANY, &p, d.local)?;
        let mut c = Cursor::new(&b);
        let result = (|| {
            if parse(c.u16())? as usize != keys.len() {
                return Err(Error::Protocol("get_many count association"));
            }
            let mut out = Vec::with_capacity(keys.len());
            for _ in keys {
                out.push(self.value(&mut c)?);
            }
            parse(c.end())?;
            Ok(out)
        })();
        if result.is_err() {
            self.usable = false;
        }
        result
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
    /// No retry, reconnect, new identity or backing-state fallback is performed.
    /// Reusing an Attempt is allowed only in its original epoch and conservative retry window.
    pub fn mutate(&mut self, attempt: &mut Attempt, d: Deadline) -> Result<MutationReply> {
        if attempt.epoch != self.negotiated.epoch {
            return Err(Error::EpochChanged {
                expected: attempt.epoch,
                actual: self.negotiated.epoch,
            });
        }
        if attempt
            .first_send
            .is_some_and(|sent| sent.elapsed().as_millis() >= attempt.retry_window_ms as u128)
        {
            return Err(Error::RetryWindowExpired);
        }
        let m = &attempt.request;
        let mut p = self.prefix(&m.operation_id, &m.namespace, d)?;
        lp(&mut p, m.key.as_bytes());
        p.push(m.ack as u8);
        if let Some(expected) = m.expected {
            p.extend(expected.to_le_bytes());
        }
        lp(&mut p, &m.value);
        let op = if m.expected.is_some() {
            CONDITIONAL_PUT
        } else {
            PUT
        };
        if !self.usable {
            return Err(Error::Invalid("connection requires reconnect"));
        }
        if Instant::now() >= d.local {
            return Err(Error::Invalid("local deadline elapsed before send"));
        }
        if p.len() + 13 > self.negotiated.max_frame {
            return Err(Error::Invalid("frame limit"));
        }
        attempt.first_send.get_or_insert_with(Instant::now);
        let result = self.exchange(op, &p, d.local).and_then(|b| {
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
        let budget = until
            .checked_duration_since(Instant::now())
            .ok_or(Error::Invalid("connect deadline"))?;
        let io = TcpStream::connect_timeout(&address, budget).map_err(Error::Transport)?;
        Self::handshake(io, options, until)
    }
}
#[cfg(windows)]
impl Client<crate::pipe_io::Pipe> {
    pub fn connect_pipe(path: &std::path::Path, options: Options, until: Instant) -> Result<Self> {
        use std::os::windows::fs::OpenOptionsExt;
        if !path.to_string_lossy().starts_with(r"\\.\pipe\") {
            return Err(Error::Invalid("explicit local named pipe required"));
        }
        let file = loop {
            if Instant::now() >= until {
                return Err(Error::Invalid("connect deadline"));
            }
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .share_mode(0)
                .custom_flags(0x40000000)
                .open(path)
            {
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
        Self::handshake(crate::pipe_io::Pipe::new(file, 1), options, until)
    }
}
