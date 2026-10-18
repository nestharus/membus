//! Bounded committed views and change hints using the existing KV1 operations.
use super::*;

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
/// A notification is a hint; re-read the committed key before using its state.
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
impl<T: Transport> Client<T> {
    fn m3_response<R>(
        &mut self,
        op: u8,
        p: &[u8],
        until: Instant,
        decode: impl FnOnce(&Self, &[u8]) -> Result<R>,
    ) -> Result<R> {
        let b = self.exchange(op, p, until)?;
        let result = decode(self, &b);
        if result.is_err() {
            self.usable = false;
        }
        result
    }
    pub fn snapshot_open(&mut self, ns: &str, op: &str, d: Deadline) -> Result<[u8; 16]> {
        if !self.negotiated.scans {
            return Err(Error::Invalid("scans unavailable"));
        }
        let p = self.prefix(op, ns, d)?;
        self.m3_response(crate::SNAPSHOT_OPEN, &p, d.local, |_, b| {
            let mut c = Cursor::new(b);
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
        let mut p = self.prefix(op, ns, d)?;
        lp(&mut p, snapshot);
        self.m3_response(crate::SNAPSHOT_RELEASE, &p, d.local, |_, b| {
            if !b.is_empty() {
                return Err(Error::Protocol("release payload"));
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
        let mut p = self.prefix(op, ns, d)?;
        lp(&mut p, snapshot);
        lp(&mut p, cursor);
        p.extend(limit.to_le_bytes());
        p.extend((max_bytes as u32).to_le_bytes());
        lp(&mut p, prefix.as_bytes());
        self.m3_response(crate::SCAN_PREFIX, &p, d.local, |this, b| {
            if b.len() > max_bytes {
                return Err(Error::Protocol("scan page bytes"));
            }
            let mut c = Cursor::new(b);
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
            let mut items = Vec::new();
            for _ in 0..count {
                let key = parse(c.text())?;
                let value = this
                    .value(&mut c)?
                    .ok_or(Error::Protocol("scan tombstone"))?;
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
            Ok(ScanPage {
                snapshot: returned,
                cursor: next,
                done: done == 1,
                items,
            })
        })
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
        let mut p = self.prefix(op, ns, d)?;
        lp(&mut p, snapshot);
        lp(&mut p, cursor);
        p.extend(limit.to_le_bytes());
        p.extend((max_bytes as u32).to_le_bytes());
        lp(&mut p, lower.as_bytes());
        lp(&mut p, upper.as_bytes());
        self.m3_response(crate::SCAN_RANGE, &p, d.local, |this, b| {
            if b.len() > max_bytes {
                return Err(Error::Protocol("scan page bytes"));
            }
            let mut c = Cursor::new(b);
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
            let mut items: Vec<(String, CommittedValue)> = Vec::new();
            for _ in 0..count {
                let key = parse(c.text())?;
                let value = this
                    .value(&mut c)?
                    .ok_or(Error::Protocol("scan tombstone"))?;
                if key.as_str() < lower
                    || key.as_str() >= upper
                    || items.last().is_some_and(|(last, _)| last >= &key)
                {
                    return Err(Error::Protocol("scan ordering"));
                }
                items.push((key, value));
            }
            parse(c.end())?;
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
