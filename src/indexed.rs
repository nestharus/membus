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
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt::Write as _;

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
#[derive(Clone)]
pub struct Collection {
    name: String,
    schema: Schema,
    declaration: Box<[u8]>,
    schema_key: Box<str>,
    head_key: Box<str>,
}
impl std::fmt::Debug for Collection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Collection")
            .field("name", &self.name)
            .field("schema", &self.schema)
            .finish()
    }
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
// Twenty ASCII digits identify exactly one zero-padded u64 decimal spelling.
fn ordinal_suffix_matches(text: &str, ordinal: u64) -> bool {
    text.len() == 20
        && text.bytes().all(|byte| byte.is_ascii_digit())
        && text.parse::<u64>() == Ok(ordinal)
}
fn range_page_budget(max_frame: usize, remaining: usize, limit: u16) -> usize {
    // Body header/token/count, a maximum cursor, and each bounded key plus
    // value envelope. Encoded row bytes are charged separately by the caller.
    remaining
        .saturating_add(42 + 2 * MAX_ID + 48)
        .saturating_add(usize::from(limit) * (4 + MAX_ID + 14))
        .min(max_frame)
}
impl Schema {
    fn encode(&self) -> Result<Vec<u8>> {
        if self.fields.len() > 64 {
            return Err(Error::Invalid("at most 64 indexed fields"));
        }
        // The field bound permits borrowed scratch slots instead of heap nodes.
        let mut names = [""; 64];
        let mut encoded_len = 8usize;
        for (index, name) in self.fields.iter().enumerate() {
            if name.is_empty() || name.len() > MAX_ID {
                return Err(Error::Invalid(
                    "field names must be nonempty, distinct and bounded",
                ));
            }
            names[index] = name.as_str();
            // At most 64 bounded names keep the complete declaration bounded.
            encoded_len += 4 + name.len();
        }
        let names = &mut names[..self.fields.len()];
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::Invalid(
                "field names must be nonempty, distinct and bounded",
            ));
        }
        let mut b = Vec::with_capacity(encoded_len);
        b.extend_from_slice(b"MBIX1S");
        b.extend((self.fields.len() as u16).to_le_bytes());
        for name in &self.fields {
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
        let declaration = schema.encode()?.into_boxed_slice();
        // Name and schema are immutable. Each control key has one exact owned
        // allocation, independent of caller strings, namespaces and snapshots.
        let key = |tail: &str| {
            let mut key = String::with_capacity(3 + name.len() + tail.len());
            key.push_str("x/");
            key.push_str(&name);
            key.push('/');
            key.push_str(tail);
            key.into_boxed_str()
        };
        let schema_key = key("schema");
        let head_key = key("head");
        Ok(Self {
            name,
            schema,
            declaration,
            schema_key,
            head_key,
        })
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    /// Validate and measure the exact encoded record without allocating it.
    pub fn encoded_len(&self, record: &Record) -> Result<usize> {
        if record.ordinal == 0
            || record.ordinal == u64::MAX
            || record.fields.len() != self.schema.fields.len()
        {
            return Err(Error::Invalid("row ordinal/declared fields"));
        }
        let mut len = 16usize;
        for values in &record.fields {
            if values.len() > 1024 {
                return Err(Error::Invalid("field value count"));
            }
            if len > MAX_VALUE - 2 {
                return Err(Error::Invalid("encoded row bound"));
            }
            len += 2;
            for value in values {
                len = len
                    .checked_add(4)
                    .and_then(|n| n.checked_add(value.len()))
                    .filter(|&n| value.len() <= MAX_VALUE && n <= MAX_VALUE)
                    .ok_or(Error::Invalid("field bytes"))?;
            }
        }
        len.checked_add(4)
            .and_then(|n| n.checked_add(record.payload.len()))
            .filter(|&n| record.payload.len() <= MAX_VALUE && n <= MAX_VALUE)
            .ok_or(Error::Invalid("payload bytes"))
    }
    fn key(&self, tail: &str) -> String {
        format!("x/{}/{tail}", self.name)
    }
    fn row_key_matches(&self, key: &str, ordinal: u64) -> bool {
        key.strip_prefix("x/")
            .and_then(|tail| tail.strip_prefix(self.name.as_str()))
            .and_then(|tail| tail.strip_prefix("/r/"))
            .is_some_and(|suffix| ordinal_suffix_matches(suffix, ordinal))
    }
    fn row_key(&self, n: u64) -> String {
        // x/ + the validated collection name + /r/ + twenty decimal digits.
        let mut key = String::with_capacity(self.name.len() + 25);
        write!(&mut key, "x/{}/r/{n:020}", self.name)
            .expect("writing strings and integers into a String is infallible");
        key
    }
    fn encode(&self, row: &Record) -> Result<Vec<u8>> {
        let mut b = Vec::with_capacity(self.encoded_len(row)?);
        b.extend(b"MBIX1R");
        b.extend(row.ordinal.to_le_bytes());
        b.extend((row.fields.len() as u16).to_le_bytes());
        for values in &row.fields {
            b.extend((values.len() as u16).to_le_bytes());
            for value in values {
                lp(&mut b, value);
            }
        }
        lp(&mut b, &row.payload);
        Ok(b)
    }
    fn decode(&self, mut b: Vec<u8>) -> Result<Record> {
        if b.len() < 16 || &b[..6] != b"MBIX1R" {
            return Err(Error::Corrupt("row header"));
        }
        let mut c = Cursor::new(&b[6..]);
        let ordinal = malformed(c.u64())?;
        if ordinal == 0 || malformed(c.u16())? as usize != self.schema.fields.len() {
            return Err(Error::Corrupt("row schema/ordinal"));
        }
        let mut fields = Vec::with_capacity(self.schema.fields.len());
        for _ in &self.schema.fields {
            let n = malformed(c.u16())? as usize;
            if n > 1024 {
                return Err(Error::Corrupt("field count"));
            }
            let mut values = Vec::with_capacity(n);
            for _ in 0..n {
                values.push(malformed(c.bytes(MAX_VALUE))?.to_vec());
            }
            fields.push(values);
        }
        let payload = malformed(c.bytes(MAX_VALUE))?;
        malformed(c.end())?;
        let payload = if b.capacity() <= payload.len().saturating_mul(2) {
            // Keep the received allocation when payload bytes dominate. Do not
            // retain a large field encoding behind an empty or small payload.
            let len = payload.len();
            let start = b.len() - len;
            b.copy_within(start.., 0);
            b.truncate(len);
            b
        } else {
            payload.to_vec()
        };
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
            &self.schema_key,
            Cow::Borrowed(&self.declaration),
            &format!("{op}:schema"),
            ack,
            d,
        )?;
        let key = self.head_key.as_ref();
        match c.get(ns, key, &format!("{op}:head-read"), d)? {
            Some(v) => {
                count(&v.bytes)?;
            }
            None => {
                let mut a = c.prepare(MutationRequest {
                    operation_id: format!("{op}:head-init"),
                    namespace: ns.into(),
                    key: key.into(),
                    value: head(0),
                    ack,
                    expected: Some(0),
                })?;
                match c.mutate(&mut a, d) {
                    Ok(_) => (),
                    Err(client::Error::Refused(r)) if r.code == 12 => {
                        count(
                            &c.get(ns, key, &format!("{op}:head-reconcile"), d)?
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
        let key = self.head_key.as_ref();
        let h = c
            .get(ns, key, &format!("{op}:boundary"), d)?
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
            Cow::Owned(body),
            &format!("{op}:row"),
            ack,
            d,
        )?;
        for (i, values) in row.fields.iter().enumerate() {
            let mut publish = |bucket| {
                // Validated schemas have at most 64 fields: two hex digits.
                let mut key = String::with_capacity(self.name.len() + 37);
                write!(
                    &mut key,
                    "x/{}/i/{i:02x}/{bucket:08x}/{:020}",
                    self.name, row.ordinal
                )
                .expect("writing strings and integers into a String is infallible");
                immutable(
                    c,
                    ns,
                    &key,
                    Cow::Owned(row.ordinal.to_le_bytes().to_vec()),
                    &format!("{op}:i{i}-{bucket:08x}"),
                    ack,
                    d,
                )
            };
            match values.as_slice() {
                [] => (),
                [value] => publish(crc32(value))?,
                _ => {
                    let buckets: BTreeSet<u32> = values.iter().map(|v| crc32(v)).collect();
                    for bucket in buckets {
                        publish(bucket)?;
                    }
                }
            }
        }
        let mut a = c.prepare(MutationRequest {
            operation_id: format!("{op}:commit"),
            namespace: ns.into(),
            key: key.into(),
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
                    .get(ns, key, &format!("{op}:commit-reconcile"), d)?
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
        if limits.max_rows == 0 {
            return Err(Error::Invalid("query limits"));
        }
        let declaration = exact(
            c,
            ns,
            snapshot,
            &self.schema_key,
            &format!("{op}:schema"),
            d,
        )?;
        if declaration.as_slice() != self.declaration.as_ref() {
            return Err(Error::Conflict("schema differs"));
        }
        let through = count(&exact(
            c,
            ns,
            snapshot,
            &self.head_key,
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
                    let max_frame = c.negotiated().max_frame;
                    let page_limit = (last - first + 1).min(c.negotiated().max_batch as u64) as u16;
                    // Header, ordinal, field count, empty field lists and the
                    // payload length are required even when every value is empty.
                    let minimum_record = 20 + 2 * self.schema.fields.len();
                    let mut request = None;
                    loop {
                        if expected <= last
                            && last - expected + 1
                                > (limits.max_bytes.saturating_sub(bytes) / minimum_record) as u64
                        {
                            return Err(Error::Limit);
                        }
                        let budget = range_page_budget(
                            max_frame,
                            limits.max_bytes.saturating_sub(bytes),
                            page_limit,
                        );
                        // Reuse these immutable identifiers across pages, but
                        // create them only after the byte-budget preflight.
                        let (lower, operation) = request
                            .get_or_insert_with(|| (self.row_key(first), format!("{op}:rows")));
                        let p = match c.scan_range(
                            ns,
                            lower,
                            &upper,
                            snapshot,
                            &cursor,
                            page_limit,
                            budget,
                            operation,
                            d,
                        ) {
                            Ok(page) => page,
                            Err(client::Error::Refused(r)) if r.code == 4 && budget < max_frame => {
                                return Err(Error::Limit);
                            }
                            Err(e) => return Err(e.into()),
                        };
                        let received = p.items.len();
                        for (key, value) in p.items {
                            bytes = bytes.checked_add(value.bytes.len()).ok_or(Error::Limit)?;
                            if bytes > limits.max_bytes {
                                return Err(Error::Limit);
                            }
                            let row = self.decode(value.bytes)?;
                            if row.ordinal != expected || !self.row_key_matches(&key, expected) {
                                return Err(Error::Corrupt("ordered row gap/identity"));
                            }
                            if records.is_empty() {
                                // Reserve only a received page after its first
                                // row passes decoding and identity checks. The
                                // negotiated page count bounds these slots;
                                // the committed boundary never sizes them.
                                records.reserve(received);
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
                // Validated field index and CRC32 widths add 17 fixed bytes.
                let mut prefix = String::with_capacity(self.name.len() + 17);
                write!(
                    &mut prefix,
                    "x/{}/i/{index:02x}/{:08x}/",
                    self.name,
                    crc32(value)
                )
                .expect("writing strings and integers into a String is infallible");
                let posting_operation = format!("{op}:postings");
                let mut value_operation = None;
                let mut cursor = Vec::new();
                let mut examined = 0usize;
                let mut previous = 0;
                loop {
                    // One extra posting witnesses overflow without prefetching
                    // an entire negotiated page past the query-work allowance.
                    let page_limit = limits
                        .max_rows
                        .saturating_sub(examined)
                        .saturating_add(1)
                        .min(c.negotiated().max_batch) as u16;
                    let page = c.scan_prefix(
                        ns,
                        &prefix,
                        snapshot,
                        &cursor,
                        page_limit,
                        c.negotiated().max_frame,
                        &posting_operation,
                        d,
                    )?;
                    for (key, v) in page.items {
                        if v.bytes.len() != 8 {
                            return Err(Error::Corrupt("posting"));
                        }
                        let n = u64::from_le_bytes(v.bytes.try_into().unwrap());
                        if n == 0
                            || n <= previous
                            || !key
                                .strip_prefix(prefix.as_str())
                                .is_some_and(|suffix| ordinal_suffix_matches(suffix, n))
                        {
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
                        let b = exact_bounded(
                            c,
                            ns,
                            snapshot,
                            &self.row_key(n),
                            limits.max_bytes.saturating_sub(bytes),
                            value_operation.get_or_insert_with(|| format!("{op}:value")),
                            d,
                        )?;
                        bytes = bytes.checked_add(b.len()).ok_or(Error::Limit)?;
                        if bytes > limits.max_bytes {
                            return Err(Error::Limit);
                        }
                        let row = self.decode(b)?;
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
    bytes: Cow<'_, [u8]>,
    op: &str,
    ack: Ack,
    d: Deadline,
) -> Result<()> {
    if let Some(v) = c.get(ns, key, &format!("{op}:read"), d)? {
        return if v.bytes == bytes.as_ref() && v.ack == ack {
            Ok(())
        } else {
            Err(Error::Conflict("immutable bytes/ack differ"))
        };
    }
    let mut a = c.prepare(MutationRequest {
        operation_id: op.into(),
        namespace: ns.into(),
        key: key.into(),
        // A prepared attempt owns its bytes only after an absent read.
        value: bytes.into_owned(),
        ack,
        expected: Some(0),
    })?;
    match c.mutate(&mut a, d) {
        Ok(_) => Ok(()),
        Err(client::Error::Refused(r)) if r.code == 12 => {
            let v = c
                .get(ns, key, &format!("{op}:reconcile"), d)?
                .ok_or(Error::Corrupt("immutable conflict absent"))?;
            if v.bytes == a.request().value && v.ack == ack {
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
    exact_bounded(c, ns, snapshot, key, usize::MAX, op, d)
}
fn exact_bounded<T: Transport>(
    c: &mut Client<T>,
    ns: &str,
    snapshot: &[u8; 16],
    key: &str,
    remaining: usize,
    op: &str,
    d: Deadline,
) -> Result<Vec<u8>> {
    let max_frame = c.negotiated().max_frame;
    let budget = range_page_budget(max_frame, remaining, 1);
    let p = match c.scan_prefix(ns, key, snapshot, &[], 1, budget, op, d) {
        Ok(page) => page,
        Err(client::Error::Refused(r)) if r.code == 4 && budget < max_frame => {
            return Err(Error::Limit);
        }
        Err(e) => return Err(e.into()),
    };
    if !p.done || p.items.len() != 1 || p.items[0].0 != key {
        return Err(Error::Corrupt("required snapshot key absent/ambiguous"));
    }
    Ok(p.items.into_iter().next().unwrap().1.bytes)
}
