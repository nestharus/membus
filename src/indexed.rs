//! Declared append-only collections on the supported resident KV client.
//!
//! Rows are opaque caller bytes. Field/link extraction belongs to the owning
//! codec. This module adds no store, connection implementation or JSON parser.
//! A row and its declared postings precede the committed ordinal boundary.
//! Queries use one caller-owned resident snapshot, including schema and boundary.
use crate::{
    client::{self, Ack, Client, Deadline, MutationRequest, Transport},
    crc32, lp, Cursor, MAX_ID, MAX_VALUE,
};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    /// Ordered exact field names. Each field can have zero or more byte values.
    pub fields: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub ordinal: u64,
    /// One value list per declared field; authored bytes are not interpreted.
    pub fields: Vec<Vec<Vec<u8>>>,
    pub payload: Vec<u8>,
}
/// The version-one query language. Ordering is always original ordinal order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Query {
    All,
    /// Inclusive ordinal interval; zero and reversed intervals are invalid.
    Ordinals {
        first: u64,
        last: u64,
    },
    Equal {
        field: String,
        value: Vec<u8>,
    },
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_rows: usize,
    /// Encoded records read, including collision candidates, not only results.
    pub max_bytes: usize,
}
#[derive(Debug)]
pub enum Error {
    Client(client::Error),
    Invalid(&'static str),
    Conflict(&'static str),
    Corrupt(&'static str),
    Limit,
}
impl From<client::Error> for Error {
    fn from(e: client::Error) -> Self {
        Self::Client(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Debug)]
pub struct Collection {
    name: String,
    schema: Schema,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendReceipt {
    pub through: u64,
    pub already_committed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Read {
    pub through: u64,
    pub records: Vec<Record>,
    pub encoded_bytes_read: usize,
}
fn malformed<T>(x: std::result::Result<T, u16>) -> Result<T> {
    x.map_err(|_| Error::Corrupt("indexed encoding"))
}
fn head(n: u64) -> Vec<u8> {
    [b"MBIX1H".as_slice(), &n.to_le_bytes()].concat()
}
fn count(b: &[u8]) -> Result<u64> {
    if b.len() != 14 || &b[..6] != b"MBIX1H" {
        return Err(Error::Corrupt("boundary"));
    }
    let n = u64::from_le_bytes(b[6..].try_into().unwrap());
    if n == u64::MAX {
        return Err(Error::Corrupt("ordinal bound"));
    }
    Ok(n)
}
impl Schema {
    fn encode(&self) -> Result<Vec<u8>> {
        if self.fields.len() > 64 {
            return Err(Error::Invalid("at most 64 indexed fields"));
        }
        let mut names = BTreeSet::new();
        let mut b = b"MBIX1S".to_vec();
        b.extend((self.fields.len() as u16).to_le_bytes());
        for name in &self.fields {
            if name.is_empty() || name.len() > MAX_ID || !names.insert(name) {
                return Err(Error::Invalid(
                    "field names must be nonempty, distinct and bounded",
                ));
            }
            lp(&mut b, name.as_bytes());
        }
        Ok(b)
    }
}
impl Collection {
    pub fn new(name: impl Into<String>, schema: Schema) -> Result<Self> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        {
            return Err(Error::Invalid("collection name"));
        }
        schema.encode()?;
        Ok(Self { name, schema })
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    /// Validate and measure the exact encoded record before publication.
    pub fn encoded_len(&self, record: &Record) -> Result<usize> {
        Ok(self.encode(record)?.len())
    }
    fn key(&self, tail: &str) -> String {
        format!("x/{}/{tail}", self.name)
    }
    fn row_key(&self, n: u64) -> String {
        self.key(&format!("r/{n:020}"))
    }
    fn encode(&self, row: &Record) -> Result<Vec<u8>> {
        if row.ordinal == 0
            || row.ordinal == u64::MAX
            || row.fields.len() != self.schema.fields.len()
        {
            return Err(Error::Invalid("row ordinal/declared fields"));
        }
        let mut b = b"MBIX1R".to_vec();
        b.extend(row.ordinal.to_le_bytes());
        b.extend((row.fields.len() as u16).to_le_bytes());
        for values in &row.fields {
            if values.len() > 1024 {
                return Err(Error::Invalid("field value count"));
            }
            if b.len() > MAX_VALUE - 2 {
                return Err(Error::Invalid("encoded row bound"));
            }
            b.extend((values.len() as u16).to_le_bytes());
            for value in values {
                if value.len() > MAX_VALUE
                    || b.len()
                        .checked_add(4)
                        .and_then(|n| n.checked_add(value.len()))
                        .is_none_or(|n| n > MAX_VALUE)
                {
                    return Err(Error::Invalid("field bytes"));
                }
                lp(&mut b, value);
            }
        }
        if row.payload.len() > MAX_VALUE
            || b.len()
                .checked_add(4)
                .and_then(|n| n.checked_add(row.payload.len()))
                .is_none_or(|n| n > MAX_VALUE)
        {
            return Err(Error::Invalid("payload bytes"));
        }
        lp(&mut b, &row.payload);
        if b.len() > MAX_VALUE {
            return Err(Error::Invalid("encoded row exceeds resident value bound"));
        }
        Ok(b)
    }
    fn decode(&self, b: &[u8]) -> Result<Record> {
        if b.len() < 16 || &b[..6] != b"MBIX1R" {
            return Err(Error::Corrupt("row header"));
        }
        let mut c = Cursor::new(&b[6..]);
        let ordinal = malformed(c.u64())?;
        if ordinal == 0 || malformed(c.u16())? as usize != self.schema.fields.len() {
            return Err(Error::Corrupt("row schema/ordinal"));
        }
        let mut fields = Vec::new();
        for _ in &self.schema.fields {
            let n = malformed(c.u16())? as usize;
            if n > 1024 {
                return Err(Error::Corrupt("field count"));
            }
            let mut values = Vec::new();
            for _ in 0..n {
                values.push(malformed(c.bytes(MAX_VALUE))?.to_vec());
            }
            fields.push(values);
        }
        let payload = malformed(c.bytes(MAX_VALUE))?.to_vec();
        malformed(c.end())?;
        Ok(Record {
            ordinal,
            fields,
            payload,
        })
    }
    /// Establish exactly this immutable declaration. No implicit schema upgrade.
    pub fn declare<T: Transport>(
        &self,
        c: &mut Client<T>,
        ns: &str,
        op: &str,
        ack: Ack,
        d: Deadline,
    ) -> Result<()> {
        immutable(
            c,
            ns,
            &self.key("schema"),
            self.schema.encode()?,
            &format!("{op}:schema"),
            ack,
            d,
        )?;
        let key = self.key("head");
        match c.get(ns, &key, &format!("{op}:head-read"), d)? {
            Some(v) => {
                count(&v.bytes)?;
            }
            None => {
                let mut a = c.prepare(MutationRequest {
                    operation_id: format!("{op}:head-init"),
                    namespace: ns.into(),
                    key: key.clone(),
                    value: head(0),
                    ack,
                    expected: Some(0),
                })?;
                match c.mutate(&mut a, d) {
                    Ok(_) => (),
                    Err(client::Error::Refused(r)) if r.code == 12 => {
                        count(
                            &c.get(ns, &key, &format!("{op}:head-reconcile"), d)?
                                .ok_or(Error::Corrupt("lost head"))?
                                .bytes,
                        )?;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
    }
    /// Explicit idempotent append/reconciliation; never retries a mutation.
    /// A transport uncertainty is returned with the shared client's actual cause.
    /// Caller keeps the original operation base and immutable record for revisit.
    #[allow(clippy::too_many_arguments)]
    pub fn append<T: Transport>(
        &self,
        c: &mut Client<T>,
        ns: &str,
        row: &Record,
        op: &str,
        ack: Ack,
        d: Deadline,
    ) -> Result<AppendReceipt> {
        let body = self.encode(row)?;
        self.declare(c, ns, &format!("{op}:declare"), ack, d)?;
        let key = self.key("head");
        let h = c
            .get(ns, &key, &format!("{op}:boundary"), d)?
            .ok_or(Error::Corrupt("missing head"))?;
        let through = count(&h.bytes)?;
        if row.ordinal <= through {
            let v = c
                .get(ns, &self.row_key(row.ordinal), &format!("{op}:existing"), d)?
                .ok_or(Error::Corrupt("committed row missing"))?;
            if v.bytes != body || v.ack != ack {
                return Err(Error::Conflict("committed row differs"));
            }
            return Ok(AppendReceipt {
                through,
                already_committed: true,
            });
        }
        if through.checked_add(1) != Some(row.ordinal) {
            return Err(Error::Conflict("ordinal gap"));
        }
        // Reserve the immutable row before any postings: a different writer at
        // this ordinal cannot leave postings referring to somebody else's row.
        immutable(
            c,
            ns,
            &self.row_key(row.ordinal),
            body,
            &format!("{op}:row"),
            ack,
            d,
        )?;
        for (i, values) in row.fields.iter().enumerate() {
            let buckets: BTreeSet<u32> = values.iter().map(|v| crc32(v)).collect();
            for bucket in buckets {
                immutable(
                    c,
                    ns,
                    &self.key(&format!("i/{i:02x}/{bucket:08x}/{:020}", row.ordinal)),
                    row.ordinal.to_le_bytes().to_vec(),
                    &format!("{op}:i{i}-{bucket:08x}"),
                    ack,
                    d,
                )?;
            }
        }
        let mut a = c.prepare(MutationRequest {
            operation_id: format!("{op}:commit"),
            namespace: ns.into(),
            key: key.clone(),
            value: head(row.ordinal),
            ack,
            expected: Some(h.version),
        })?;
        match c.mutate(&mut a, d) {
            Ok(_) => Ok(AppendReceipt {
                through: row.ordinal,
                already_committed: false,
            }),
            Err(client::Error::Refused(r)) if r.code == 12 => {
                let current = c
                    .get(ns, &key, &format!("{op}:commit-reconcile"), d)?
                    .ok_or(Error::Corrupt("head missing after conflict"))?;
                let n = count(&current.bytes)?;
                if n < row.ordinal {
                    return Err(Error::Conflict("boundary changed"));
                }
                Ok(AppendReceipt {
                    through: n,
                    already_committed: true,
                })
            }
            Err(e) => Err(e.into()),
        }
    }
    /// Query one pinned snapshot. A missing/malformed declaration, gap or limit
    /// refuses the complete query; it never becomes an empty successful result.
    #[allow(clippy::too_many_arguments)]
    pub fn query<T: Transport>(
        &self,
        c: &mut Client<T>,
        ns: &str,
        snapshot: &[u8; 16],
        query: &Query,
        limits: Limits,
        op: &str,
        d: Deadline,
    ) -> Result<Read> {
        if limits.max_rows == 0 || limits.max_bytes == 0 {
            return Err(Error::Invalid("query limits"));
        }
        let declaration = exact(
            c,
            ns,
            snapshot,
            &self.key("schema"),
            &format!("{op}:schema"),
            d,
        )?;
        if declaration != self.schema.encode()? {
            return Err(Error::Conflict("schema differs"));
        }
        let through = count(&exact(
            c,
            ns,
            snapshot,
            &self.key("head"),
            &format!("{op}:head"),
            d,
        )?)?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        match query {
            Query::All | Query::Ordinals { .. } => {
                let (first, last) = match query {
                    Query::Ordinals { first, last } => {
                        if *first == 0 || first > last {
                            return Err(Error::Invalid("ordinal query"));
                        }
                        (*first, (*last).min(through))
                    }
                    _ => (1, through),
                };
                if first <= last {
                    if last - first >= limits.max_rows as u64 {
                        return Err(Error::Limit);
                    }
                    let upper = last
                        .checked_add(1)
                        .map(|n| self.row_key(n))
                        .unwrap_or_else(|| self.key("r/~"));
                    let mut cursor = Vec::new();
                    let mut expected = first;
                    loop {
                        let p = c.scan_range(
                            ns,
                            &self.row_key(first),
                            &upper,
                            snapshot,
                            &cursor,
                            c.negotiated().max_batch as u16,
                            c.negotiated().max_frame,
                            &format!("{op}:rows"),
                            d,
                        )?;
                        for (key, value) in p.items {
                            bytes = bytes.checked_add(value.bytes.len()).ok_or(Error::Limit)?;
                            if bytes > limits.max_bytes {
                                return Err(Error::Limit);
                            }
                            let row = self.decode(&value.bytes)?;
                            if row.ordinal != expected || key != self.row_key(expected) {
                                return Err(Error::Corrupt("ordered row gap/identity"));
                            }
                            records.push(row);
                            expected = expected
                                .checked_add(1)
                                .ok_or(Error::Corrupt("ordinal overflow"))?;
                        }
                        if p.done {
                            break;
                        }
                        cursor = p.cursor;
                    }
                    if expected.checked_sub(1) != Some(last) {
                        return Err(Error::Corrupt("incomplete committed range"));
                    }
                }
            }
            Query::Equal { field, value } => {
                let index = self
                    .schema
                    .fields
                    .iter()
                    .position(|v| v == field)
                    .ok_or(Error::Invalid("field is not declared indexed"))?;
                let prefix = self.key(&format!("i/{index:02x}/{:08x}/", crc32(value)));
                let mut cursor = Vec::new();
                let mut examined = 0usize;
                let mut previous = 0;
                loop {
                    let page = c.scan_prefix(
                        ns,
                        &prefix,
                        snapshot,
                        &cursor,
                        c.negotiated().max_batch as u16,
                        c.negotiated().max_frame,
                        &format!("{op}:postings"),
                        d,
                    )?;
                    for (key, v) in page.items {
                        if v.bytes.len() != 8 {
                            return Err(Error::Corrupt("posting"));
                        }
                        let n = u64::from_le_bytes(v.bytes.try_into().unwrap());
                        if n == 0 || n <= previous || key != format!("{prefix}{n:020}") {
                            return Err(Error::Corrupt("posting order/identity"));
                        }
                        previous = n;
                        examined = examined.checked_add(1).ok_or(Error::Limit)?;
                        if examined > limits.max_rows {
                            return Err(Error::Limit);
                        }
                        if n > through {
                            continue;
                        } // staged row, not committed in this snapshot
                        let b =
                            exact(c, ns, snapshot, &self.row_key(n), &format!("{op}:value"), d)?;
                        bytes = bytes.checked_add(b.len()).ok_or(Error::Limit)?;
                        if bytes > limits.max_bytes {
                            return Err(Error::Limit);
                        }
                        let row = self.decode(&b)?;
                        if row.ordinal != n {
                            return Err(Error::Corrupt("posting row identity"));
                        }
                        // CRC buckets are only candidate indexes. Full original
                        // values decide equality, including deliberate collisions.
                        if row.fields[index].iter().any(|v| v == value) {
                            records.push(row);
                        }
                    }
                    if page.done {
                        break;
                    }
                    cursor = page.cursor;
                }
            }
        }
        Ok(Read {
            through,
            records,
            encoded_bytes_read: bytes,
        })
    }
}
#[allow(clippy::too_many_arguments)]
fn immutable<T: Transport>(
    c: &mut Client<T>,
    ns: &str,
    key: &str,
    bytes: Vec<u8>,
    op: &str,
    ack: Ack,
    d: Deadline,
) -> Result<()> {
    if let Some(v) = c.get(ns, key, &format!("{op}:read"), d)? {
        return if v.bytes == bytes && v.ack == ack {
            Ok(())
        } else {
            Err(Error::Conflict("immutable bytes/ack differ"))
        };
    }
    let mut a = c.prepare(MutationRequest {
        operation_id: op.into(),
        namespace: ns.into(),
        key: key.into(),
        value: bytes.clone(),
        ack,
        expected: Some(0),
    })?;
    match c.mutate(&mut a, d) {
        Ok(_) => Ok(()),
        Err(client::Error::Refused(r)) if r.code == 12 => {
            let v = c
                .get(ns, key, &format!("{op}:reconcile"), d)?
                .ok_or(Error::Corrupt("immutable conflict absent"))?;
            if v.bytes == bytes && v.ack == ack {
                Ok(())
            } else {
                Err(Error::Conflict("immutable collision"))
            }
        }
        Err(e) => Err(e.into()),
    }
}
fn exact<T: Transport>(
    c: &mut Client<T>,
    ns: &str,
    snapshot: &[u8; 16],
    key: &str,
    op: &str,
    d: Deadline,
) -> Result<Vec<u8>> {
    let p = c.scan_prefix(ns, key, snapshot, &[], 1, c.negotiated().max_frame, op, d)?;
    if !p.done || p.items.len() != 1 || p.items[0].0 != key {
        return Err(Error::Corrupt("required snapshot key absent/ambiguous"));
    }
    Ok(p.items.into_iter().next().unwrap().1.bytes)
}
