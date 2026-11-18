//! Bounded committed views and change hints using the existing KV1 operations.
use super::*;
// Nonempty key, presence, version, ack and an empty length-prefixed value.
const MIN_SCAN_ITEM_BYTES: usize = 4 + 1 + 1 + 8 + 1 + 4;

#[derive(Clone, Debug)]
pub struct ScanPage {
    pub snapshot: [u8; 16],
    pub cursor: Vec<u8>,
    pub done: bool,
    pub items: Vec<(String, CommittedValue)>,
}
#[derive(Clone, Debug)]
pub struct Subscription {
    pub token: [u8; 16],
    pub cursor: [u8; 16],
}
#[derive(Clone, Debug)]
pub struct SubscriptionStatus {
    pub active: bool,
    pub status: u8,
    pub queued: u32,
    pub charged_bytes: u64,
    pub written: u64,
    pub failed: u64,
    pub dropped: u64,
    pub cursor: [u8; 16],
}
/// A notification is a hint, not receiver receipt or useful-work completion.
/// KV included bytes belong to this publication; a later GET reads current state
/// and may name another revision. Timer wakes instead carry MBT1 envelopes: the
/// outer version is configuration generation and no public KV value is created
/// at the signal key. Reconcile those envelopes with timer status.
#[derive(Clone, Debug)]
pub struct Change {
    pub subscription: [u8; 16],
    pub cursor: [u8; 16],
    pub namespace: String,
    pub key: String,
    pub version: u64,
    pub ack: Ack,
    pub tombstone: bool,
    pub value: Option<Vec<u8>>,
}
fn token(c: &mut Cursor<'_>) -> Result<[u8; 16]> {
    parse(c.bytes(16))?
        .try_into()
        .map_err(|_| Error::Protocol("token length"))
}
impl Change {
    /// Decode the body following the transport's bounded u32 length.
    pub fn decode(body: &[u8]) -> Result<Self> {
        if body.len() < 38 || &body[..4] != b"MBC1" || body[4..6] != [1, 0] {
            return Err(Error::Protocol("change header"));
        }
        let subscription = body[6..22].try_into().unwrap();
        let cursor = body[22..38].try_into().unwrap();
        let mut c = Cursor::new(&body[38..]);
        let namespace = parse(c.text())?;
        let key = parse(c.text())?;
        let version = parse(c.u64())?;
        let ack = match parse(c.u8())? {
            1 => Ack::Os,
            2 => Ack::Flush,
            _ => return Err(Error::Protocol("change ack")),
        };
        let tombstone = parse(c.u8())?;
        let included = parse(c.u8())?;
        if tombstone > 1 || included > 1 || version == 0 {
            return Err(Error::Protocol("change flags"));
        }
        let value = if included == 1 {
            Some(parse(c.bytes(MAX_VALUE))?.to_vec())
        } else {
            None
        };
        parse(c.end())?;
        Ok(Self {
            subscription,
            cursor,
            namespace,
            key,
            version,
            ack,
            tombstone: tombstone == 1,
            value,
        })
    }
}
// Only a completely decoded page may transfer its final value's receive storage.
// Earlier values and cursors own independent bytes. Tiny tails shed page metadata.
fn retain_scan_tail(
    body: ReplyPayload,
    items: &mut [(String, CommittedValue)],
    tail: Option<usize>,
) {
    if let Some(len) = tail {
        items.last_mut().expect("decoded final scan item").1.bytes = body.into_tail(len);
    }
}
impl<T: Transport> Client<T> {
    fn m3_response<R>(
        &mut self,
        op: u8,
        p: &[u8],
        until: Instant,
        decode: impl FnOnce(&Self, &[u8]) -> Result<R>,
    ) -> Result<R> {
        let b = self.exchange(op, p, until)?;
        self.m3_decode_response(b, |this, b| decode(this, &b))
    }
    fn m3_decode_response<R>(
        &mut self,
        b: ReplyPayload,
        decode: impl FnOnce(&Self, ReplyPayload) -> Result<R>,
    ) -> Result<R> {
        let result = decode(self, b);
        if result.is_err() {
            self.usable = false;
        }
        result
    }
    fn snapshot_frame(
        &self,
        opcode: u8,
        ns: &str,
        snapshot: Option<&[u8; 16]>,
        op: &str,
        d: Deadline,
    ) -> Result<Vec<u8>> {
        // Retain prefix's validation order before reserving the final frame.
        identifier(op)?;
        identifier(ns)?;
        let payload_len = 8 + 2 * 4 + op.len() + ns.len()
            + if snapshot.is_some() { 4 + 16 } else { 0 };
        let mut bytes = crate::codec::frame_header(opcode, 0, payload_len);
        bytes.extend(d.unix_ms.to_le_bytes());
        lp(&mut bytes, op.as_bytes());
        lp(&mut bytes, ns.as_bytes());
        if let Some(snapshot) = snapshot {
            lp(&mut bytes, snapshot);
        }
        Ok(bytes)
    }
    // Callers validate scan bounds first; identifiers retain prefix's order.
    #[allow(clippy::too_many_arguments)]
    fn scan_frame(
        &self,
        opcode: u8,
        ns: &str,
        snapshot: &[u8; 16],
        cursor: &[u8],
        limit: u16,
        max_bytes: usize,
        bounds: &[&str],
        op: &str,
        d: Deadline,
    ) -> Result<Vec<u8>> {
        identifier(op)?;
        identifier(ns)?;
        // Every variable input is bounded before this exact reservation.
        let payload_len = 8 + 4 + op.len() + 4 + ns.len() + 20 + 4 + cursor.len() + 2 + 4
            + bounds.iter().map(|bound| 4 + bound.len()).sum::<usize>();
        let mut bytes = crate::codec::frame_header(opcode, 0, payload_len);
        bytes.extend(d.unix_ms.to_le_bytes());
        lp(&mut bytes, op.as_bytes());
        lp(&mut bytes, ns.as_bytes());
        lp(&mut bytes, snapshot);
        lp(&mut bytes, cursor);
        bytes.extend(limit.to_le_bytes());
        bytes.extend((max_bytes as u32).to_le_bytes());
        for bound in bounds {
            lp(&mut bytes, bound.as_bytes());
        }
        Ok(bytes)
    }
    fn m3_frame_response<R>(
        &mut self,
        op: u8,
        bytes: Vec<u8>,
        until: Instant,
        decode: impl FnOnce(&Self, ReplyPayload) -> Result<R>,
    ) -> Result<R> {
        let b = self.m3_frame_exchange(op, bytes, until)?;
        self.m3_decode_response(b, decode)
    }
    fn m3_frame_exchange(&mut self, op: u8, mut bytes: Vec<u8>, until: Instant) -> Result<ReplyPayload> {
        // Keep readiness/deadline/frame admission after complete encoding.
        // Refused sends consume no request ID and perform no transport I/O.
        let id = self.begin_exchange(bytes.len() - 17, until)?;
        bytes[9..17].copy_from_slice(&id.to_le_bytes());
        self.exchange_bytes(op, id, &bytes, until)
    }
    pub fn snapshot_open(&mut self, ns: &str, op: &str, d: Deadline) -> Result<[u8; 16]> {
        if !self.negotiated.scans {
            return Err(Error::Invalid("scans unavailable"));
        }
        let p = self.snapshot_frame(crate::SNAPSHOT_OPEN, ns, None, op, d)?;
        self.m3_frame_response(crate::SNAPSHOT_OPEN, p, d.local, |_, b| {
            let mut c = Cursor::new(&b);
            let t = token(&mut c)?;
            parse(c.end())?;
            Ok(t)
        })
    }
    pub fn snapshot_release(
        &mut self,
        ns: &str,
        snapshot: &[u8; 16],
        op: &str,
        d: Deadline,
    ) -> Result<()> {
        if !self.negotiated.scans {
            return Err(Error::Invalid("scans unavailable"));
        }
        let p = self.snapshot_frame(crate::SNAPSHOT_RELEASE, ns, Some(snapshot), op, d)?;
        self.m3_frame_response(crate::SNAPSHOT_RELEASE, p, d.local, |_, b| {
            if !b.is_empty() {
                return Err(Error::Protocol("release payload"));
            }
            Ok(())
        })
    }
    /// Require complete-query eligibility for the supplied immutable capture.
    pub fn snapshot_require_complete(&mut self, ns: &str, snapshot: &[u8; 16], op: &str, d: Deadline) -> Result<()> {
        self.require_atomic_wire()?;
        if !self.negotiated.scans {
            return Err(Error::Invalid("scans unavailable"));
        }
        let p = self.snapshot_frame(crate::SNAPSHOT_REQUIRE_COMPLETE, ns, Some(snapshot), op, d)?;
        self.m3_frame_response(crate::SNAPSHOT_REQUIRE_COMPLETE, p, d.local, |_, b| {
            if !b.is_empty() {
                return Err(Error::Protocol("snapshot completeness payload"));
            }
            Ok(())
        })
    }
    /// One page of one immutable view. The caller owns paging and release.
    pub fn scan_prefix(
        &mut self,
        ns: &str,
        prefix: &str,
        snapshot: &[u8; 16],
        cursor: &[u8],
        limit: u16,
        max_bytes: usize,
        op: &str,
        d: Deadline,
    ) -> Result<ScanPage> {
        self.scan_prefix_inner(ns, prefix, snapshot, cursor, limit, max_bytes, op, d, false)
    }
    pub(crate) fn scan_population_page(
        &mut self, ns: &str, snapshot: &[u8; 16], cursor: &[u8], op: &str, d: Deadline,
    ) -> Result<ScanPage> {
        // The bounded owner can release its already authenticated token on an
        // associated payload error. Framing/transport failures still poison immediately.
        self.scan_prefix_inner(ns, "", snapshot, cursor, self.negotiated.max_batch as u16,
            self.negotiated.max_frame, op, d, true)
    }
    #[allow(clippy::too_many_arguments)]
    fn scan_prefix_inner(
        &mut self, ns: &str, prefix: &str, snapshot: &[u8; 16], cursor: &[u8], limit: u16,
        max_bytes: usize, op: &str, d: Deadline, release_on_payload_error: bool,
    ) -> Result<ScanPage> {
        if !self.negotiated.scans {
            return Err(Error::Invalid("scans unavailable"));
        }
        if prefix.len() > MAX_ID
            || limit == 0
            || limit as usize > self.negotiated.max_batch
            || !(42..=self.negotiated.max_frame).contains(&max_bytes)
            || cursor.len() > 2 * MAX_ID + 48
        {
            return Err(Error::Invalid("scan bounds"));
        }
        let p = self.scan_frame(crate::SCAN_PREFIX, ns, snapshot, cursor, limit, max_bytes, &[prefix], op, d)?;
        let b = self.m3_frame_exchange(crate::SCAN_PREFIX, p, d.local)?;
        let result = (|| {
            if b.len() > max_bytes {
                return Err(Error::Protocol("scan page bytes"));
            }
            let mut c = Cursor::new(&b);
            let returned = token(&mut c)?;
            let next = parse(c.bytes(2 * MAX_ID + 48))?.to_vec();
            let done = parse(c.u8())?;
            let count = parse(c.u16())?;
            if &returned != snapshot
                || done > 1
                || (done == 1) != next.is_empty()
                || count > limit
                || (done == 0 && (count == 0 || next == cursor))
            {
                return Err(Error::Protocol("scan progress"));
            }
            // Received bytes bound reservations even when the advertised count lies.
            // Keep decoding so malformed-item diagnostics retain their precedence.
            let capacity = usize::from(count).min(c.remainder().len() / MIN_SCAN_ITEM_BYTES);
            let mut items = Vec::with_capacity(capacity);
            let mut tail = None;
            for index in 0..count {
                let key = parse(c.text())?;
                let (version, ack, bytes) = self
                    .borrowed_value(&mut c)?
                    .ok_or(Error::Protocol("scan tombstone"))?;
                let bytes = if index + 1 == count {
                    tail = Some(bytes.len());
                    Vec::new()
                } else {
                    bytes.to_vec()
                };
                let value = CommittedValue {
                    version,
                    ack,
                    bytes,
                };
                if !key.starts_with(prefix)
                    || items
                        .last()
                        .is_some_and(|(last, _): &(String, CommittedValue)| last >= &key)
                {
                    return Err(Error::Protocol("scan ordering"));
                }
                items.push((key, value));
            }
            parse(c.end())?;
            retain_scan_tail(b, &mut items, tail);
            Ok(ScanPage {
                snapshot: returned,
                cursor: next,
                done: done == 1,
                items,
            })
        })();
        if result.is_err() && !release_on_payload_error {
            self.usable = false;
        }
        result
    }
    /// No retry after a lost reply: the remote subscription may already exist.
    pub fn subscribe(
        &mut self,
        ns: &str,
        prefix: &str,
        endpoint: &str,
        op: &str,
        d: Deadline,
    ) -> Result<Subscription> {
        self.subscribe_with(
            ns,
            prefix,
            endpoint,
            SubscribeOptions {
                include_values: false,
                resume: None,
                max_frame: self.negotiated.max_frame,
            },
            op,
            d,
        )
    }
    /// Registration has no operation-ID deduplication. An unknown outcome must
    /// retain its cleanup obligation; do not automatically create another one.
    pub fn subscribe_with(
        &mut self,
        ns: &str,
        prefix: &str,
        endpoint: &str,
        options: SubscribeOptions,
        op: &str,
        d: Deadline,
    ) -> Result<Subscription> {
        if !self.negotiated.subscriptions {
            return Err(Error::Invalid("subscriptions unavailable"));
        }
        if prefix.len() > MAX_ID
            || endpoint.len() > MAX_ID
            || !(1024..=self.feature_limits.signals.as_ref().unwrap().max_frame)
                .contains(&options.max_frame)
        {
            return Err(Error::Invalid("subscription bounds"));
        }
        let mut p = self.prefix(op, ns, d)?;
        lp(&mut p, prefix.as_bytes());
        lp(&mut p, endpoint.as_bytes());
        p.push(options.include_values as u8);
        lp(&mut p, options.resume.as_ref().map_or(&[][..], |c| &c[..]));
        lp(&mut p, b"membus.change/1");
        p.extend((options.max_frame as u32).to_le_bytes());
        self.m3_response(crate::SUBSCRIBE, &p, d.local, |_, b| {
            let mut c = Cursor::new(b);
            let s = Subscription {
                token: token(&mut c)?,
                cursor: token(&mut c)?,
            };
            parse(c.end())?;
            Ok(s)
        })
    }
    /// Half-open immutable range page using the resident's existing opcode12.
    #[allow(clippy::too_many_arguments)] // Explicit snapshot, query and operation operands mirror scan_prefix.
    pub fn scan_range(
        &mut self,
        ns: &str,
        lower: &str,
        upper: &str,
        snapshot: &[u8; 16],
        cursor: &[u8],
        limit: u16,
        max_bytes: usize,
        op: &str,
        d: Deadline,
    ) -> Result<ScanPage> {
        if !self.negotiated.scans {
            return Err(Error::Invalid("scans unavailable"));
        }
        if lower.len() > MAX_ID
            || upper.len() > MAX_ID
            || lower >= upper
            || limit == 0
            || limit as usize > self.negotiated.max_batch
            || !(42..=self.negotiated.max_frame).contains(&max_bytes)
            || cursor.len() > 2 * MAX_ID + 48
        {
            return Err(Error::Invalid("scan bounds"));
        }
        let p = self.scan_frame(crate::SCAN_RANGE, ns, snapshot, cursor, limit, max_bytes, &[lower, upper], op, d)?;
        self.m3_frame_response(crate::SCAN_RANGE, p, d.local, |this, b| {
            if b.len() > max_bytes {
                return Err(Error::Protocol("scan page bytes"));
            }
            let mut c = Cursor::new(&b);
            let returned = token(&mut c)?;
            let next = parse(c.bytes(2 * MAX_ID + 48))?.to_vec();
            let done = parse(c.u8())?;
            let count = parse(c.u16())?;
            if &returned != snapshot
                || done > 1
                || (done == 1) != next.is_empty()
                || count > limit
                || (done == 0 && (count == 0 || next == cursor))
            {
                return Err(Error::Protocol("scan progress"));
            }
            // Received bytes bound reservations even when the advertised count lies.
            // Keep decoding so malformed-item diagnostics retain their precedence.
            let capacity = usize::from(count).min(c.remainder().len() / MIN_SCAN_ITEM_BYTES);
            let mut items: Vec<(String, CommittedValue)> = Vec::with_capacity(capacity);
            let mut tail = None;
            for index in 0..count {
                let key = parse(c.text())?;
                let (version, ack, bytes) = this
                    .borrowed_value(&mut c)?
                    .ok_or(Error::Protocol("scan tombstone"))?;
                let bytes = if index + 1 == count {
                    tail = Some(bytes.len());
                    Vec::new()
                } else {
                    bytes.to_vec()
                };
                let value = CommittedValue {
                    version,
                    ack,
                    bytes,
                };
                if key.as_str() < lower
                    || key.as_str() >= upper
                    || items.last().is_some_and(|(last, _)| last >= &key)
                {
                    return Err(Error::Protocol("scan ordering"));
                }
                items.push((key, value));
            }
            parse(c.end())?;
            retain_scan_tail(b, &mut items, tail);
            Ok(ScanPage {
                snapshot: returned,
                cursor: next,
                done: done == 1,
                items,
            })
        })
    }
    pub fn unsubscribe(
        &mut self,
        ns: &str,
        subscription: &[u8; 16],
        op: &str,
        d: Deadline,
    ) -> Result<()> {
        if !self.negotiated.subscriptions {
            return Err(Error::Invalid("subscriptions unavailable"));
        }
        let mut p = self.prefix(op, ns, d)?;
        lp(&mut p, subscription);
        self.m3_response(crate::UNSUBSCRIBE, &p, d.local, |_, b| {
            if !b.is_empty() {
                return Err(Error::Protocol("unsubscribe payload"));
            }
            Ok(())
        })
    }
    pub fn subscription_status(
        &mut self,
        ns: &str,
        subscription: &[u8; 16],
        op: &str,
        d: Deadline,
    ) -> Result<SubscriptionStatus> {
        if !self.negotiated.subscriptions {
            return Err(Error::Invalid("subscriptions unavailable"));
        }
        let mut p = self.prefix(op, ns, d)?;
        lp(&mut p, subscription);
        self.m3_response(crate::SUBSCRIPTION_STATUS, &p, d.local, |_, b| {
            let mut c = Cursor::new(b);
            let active = parse(c.u8())?;
            let status = parse(c.u8())?;
            let s = SubscriptionStatus {
                active: active == 1,
                status,
                queued: parse(c.u32())?,
                charged_bytes: parse(c.u64())?,
                written: parse(c.u64())?,
                failed: parse(c.u64())?,
                dropped: parse(c.u64())?,
                cursor: token(&mut c)?,
            };
            parse(c.end())?;
            if active > 1 || status > 4 {
                return Err(Error::Protocol("subscription status"));
            }
            Ok(s)
        })
    }
}
