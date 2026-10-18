//! Client of the resident timer service. No scheduler, store or implicit retry.
use super::*;

#[derive(Clone, Debug)]
pub struct TimerLimits {
    pub identities: u32,
    pub charged_bytes: u64,
    pub payload_bytes: usize,
    pub minimum_period_ms: u64,
    pub maximum_jitter_ms: u64,
}
impl TimerLimits {
    pub(super) fn parse(c: &mut Cursor<'_>) -> Result<Self> {
        if parse(c.u16())? != 1 {
            return Err(Error::Protocol("timer version"));
        }
        let out = Self {
            identities: parse(c.u32())?,
            charged_bytes: parse(c.u64())?,
            payload_bytes: parse(c.u32())? as usize,
            minimum_period_ms: parse(c.u64())?,
            maximum_jitter_ms: parse(c.u64())?,
        };
        if out.identities == 0
            || out.charged_bytes == 0
            || out.payload_bytes == 0
            || out.payload_bytes > 65536
            || out.minimum_period_ms == 0
            || out.maximum_jitter_ms > i64::MAX as u64
        {
            return Err(Error::Protocol("timer limits"));
        }
        Ok(out)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerExpectation {
    Unconditional,
    Absent,
    Generation(u64),
}
impl TimerExpectation {
    fn wire(self) -> Result<u64> {
        match self {
            Self::Unconditional => Ok(u64::MAX),
            Self::Absent => Ok(0),
            Self::Generation(v) if v != 0 && v != u64::MAX => Ok(v),
            _ => Err(Error::Invalid("timer expected generation")),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerSetRequest {
    pub operation_id: String,
    pub namespace: String,
    pub timer_id: String,
    pub expected: TimerExpectation,
    pub key: String,
    pub anchor_ms: u64,
    pub period_ms: u64,
    pub jitter_low_ms: i64,
    pub jitter_high_ms: i64,
    pub payload: Vec<u8>,
    pub ack: Ack,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerCancelRequest {
    pub operation_id: String,
    pub namespace: String,
    pub timer_id: String,
    pub expected_generation: u64,
}
#[derive(Debug)]
struct Horizon {
    epoch: u64,
    first_send: Option<Instant>,
    retry_window_ms: u64,
}
impl Horizon {
    fn check(&self, epoch: u64) -> Result<()> {
        if self.epoch != epoch {
            return Err(Error::EpochChanged {
                expected: self.epoch,
                actual: epoch,
            });
        }
        if self
            .first_send
            .is_some_and(|t| t.elapsed().as_millis() >= self.retry_window_ms as u128)
        {
            return Err(Error::RetryWindowExpired);
        }
        Ok(())
    }
}
#[derive(Debug)]
pub struct TimerSetAttempt {
    request: TimerSetRequest,
    horizon: Horizon,
}
#[derive(Debug)]
pub struct TimerCancelAttempt {
    request: TimerCancelRequest,
    horizon: Horizon,
}
impl TimerSetAttempt {
    pub fn request(&self) -> &TimerSetRequest {
        &self.request
    }
    pub fn epoch(&self) -> u64 {
        self.horizon.epoch
    }
}
impl TimerCancelAttempt {
    pub fn request(&self) -> &TimerCancelRequest {
        &self.request
    }
    pub fn epoch(&self) -> u64 {
        self.horizon.epoch
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerEnvelope {
    pub timer_id: String,
    pub generation: u64,
    pub index: u64,
    pub nominal_ms: u64,
    pub due_ms: u64,
    pub draw_ms: i64,
    pub missed_count: u64,
    pub payload: Vec<u8>,
}
impl TimerEnvelope {
    pub fn decode(b: &[u8]) -> Result<Self> {
        if b.get(..6) != Some(&b"MBT1\x01\x00"[..]) {
            return Err(Error::Protocol("timer envelope version"));
        }
        let mut c = Cursor::new(&b[6..]);
        let out = Self {
            timer_id: parse(c.text())?,
            generation: parse(c.u64())?,
            index: parse(c.u64())?,
            nominal_ms: parse(c.u64())?,
            due_ms: parse(c.u64())?,
            draw_ms: parse(c.u64())? as i64,
            missed_count: parse(c.u64())?,
            payload: parse(c.bytes(65536))?.to_vec(),
        };
        parse(c.end())?;
        if out.timer_id.is_empty()
            || out.generation == 0
            || out.missed_count == 0
            || out.nominal_ms.checked_add_signed(out.draw_ms) != Some(out.due_ms)
        {
            return Err(Error::Protocol("timer envelope fields"));
        }
        Ok(out)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerSchedule {
    pub namespace: String,
    pub timer_id: String,
    pub key: String,
    pub anchor_ms: u64,
    pub period_ms: u64,
    pub jitter_low_ms: i64,
    pub jitter_high_ms: i64,
    pub seed: u64,
    pub payload: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct TimerStatus {
    pub generation: u64,
    pub active: bool,
    pub error: u16,
    pub next_index: u64,
    pub next_due_ms: u64,
    pub schedule: TimerSchedule,
    pub last: Option<TimerEnvelope>,
}
fn identity(ns: &str, id: &str) -> Result<()> {
    identifier(ns)?;
    identifier(id)?;
    if ns == "__membus_timers_v1"
        || ns.contains('\0')
        || id.contains('\0')
        || ns.len() + id.len() + 3 > MAX_ID
    {
        return Err(Error::Invalid("timer identity"));
    }
    Ok(())
}
fn schedule_valid(s: &TimerSchedule, l: &TimerLimits) -> Result<()> {
    identity(&s.namespace, &s.timer_id)?;
    identifier(&s.key)?;
    if s.payload.len() > l.payload_bytes
        || s.jitter_low_ms > s.jitter_high_ms
        || s.jitter_low_ms < -(l.maximum_jitter_ms as i64)
        || s.jitter_high_ms > l.maximum_jitter_ms as i64
        || (s.period_ms != 0
            && (s.period_ms < l.minimum_period_ms
                || s.jitter_high_ms.abs_diff(s.jitter_low_ms) >= s.period_ms))
        || s.anchor_ms.checked_add_signed(s.jitter_low_ms).is_none()
        || s.anchor_ms.checked_add_signed(s.jitter_high_ms).is_none()
    {
        return Err(Error::Invalid("timer schedule bounds"));
    }
    Ok(())
}
impl<T: Transport> Client<T> {
    fn timer_limits(&self) -> Result<&TimerLimits> {
        self.feature_limits
            .timers
            .as_ref()
            .ok_or(Error::Invalid("timers unavailable"))
    }
    fn timer_set_payload(&self, r: &TimerSetRequest, d: Deadline) -> Result<Vec<u8>> {
        let limits = self.timer_limits()?;
        let s = TimerSchedule {
            namespace: r.namespace.clone(),
            timer_id: r.timer_id.clone(),
            key: r.key.clone(),
            anchor_ms: r.anchor_ms,
            period_ms: r.period_ms,
            jitter_low_ms: r.jitter_low_ms,
            jitter_high_ms: r.jitter_high_ms,
            seed: 0,
            payload: r.payload.clone(),
        };
        schedule_valid(&s, limits)?;
        let mut p = self.prefix(&r.operation_id, &r.namespace, d)?;
        lp(&mut p, r.timer_id.as_bytes());
        p.extend(r.expected.wire()?.to_le_bytes());
        lp(&mut p, r.key.as_bytes());
        for v in [
            r.anchor_ms,
            r.period_ms,
            r.jitter_low_ms as u64,
            r.jitter_high_ms as u64,
        ] {
            p.extend(v.to_le_bytes());
        }
        lp(&mut p, &r.payload);
        p.push(r.ack as u8);
        Ok(p)
    }
    fn timer_cancel_payload(&self, r: &TimerCancelRequest, d: Deadline) -> Result<Vec<u8>> {
        self.timer_limits()?;
        identity(&r.namespace, &r.timer_id)?;
        if r.expected_generation == 0 {
            return Err(Error::Invalid("timer generation"));
        }
        let mut p = self.prefix(&r.operation_id, &r.namespace, d)?;
        lp(&mut p, r.timer_id.as_bytes());
        p.extend(r.expected_generation.to_le_bytes());
        Ok(p)
    }
    pub fn prepare_timer_set(&self, request: TimerSetRequest) -> Result<TimerSetAttempt> {
        let p = self.timer_set_payload(
            &request,
            Deadline {
                unix_ms: 0,
                local: Instant::now(),
            },
        )?;
        if p.len() + 13 > self.negotiated.max_frame {
            return Err(Error::Invalid("frame limit"));
        }
        Ok(TimerSetAttempt {
            request,
            horizon: Horizon {
                epoch: self.negotiated.epoch,
                first_send: None,
                retry_window_ms: self.negotiated.retry_window_ms,
            },
        })
    }
    pub fn prepare_timer_cancel(&self, request: TimerCancelRequest) -> Result<TimerCancelAttempt> {
        let p = self.timer_cancel_payload(
            &request,
            Deadline {
                unix_ms: 0,
                local: Instant::now(),
            },
        )?;
        if p.len() + 13 > self.negotiated.max_frame {
            return Err(Error::Invalid("frame limit"));
        }
        Ok(TimerCancelAttempt {
            request,
            horizon: Horizon {
                epoch: self.negotiated.epoch,
                first_send: None,
                retry_window_ms: self.negotiated.retry_window_ms,
            },
        })
    }
    fn timer_exchange<R>(
        &mut self,
        opcode: u8,
        p: &[u8],
        id: &str,
        h: &mut Horizon,
        d: Deadline,
        decode: impl FnOnce(&[u8]) -> Result<R>,
    ) -> Result<R> {
        h.check(self.negotiated.epoch)?;
        if !self.usable {
            return Err(Error::Invalid("connection requires reconnect"));
        }
        if Instant::now() >= d.local {
            return Err(Error::Invalid("local deadline elapsed before send"));
        }
        if p.len() + 13 > self.negotiated.max_frame {
            return Err(Error::Invalid("frame limit"));
        }
        h.first_send.get_or_insert_with(Instant::now);
        match self.exchange(opcode, p, d.local).and_then(|b| decode(&b)) {
            Ok(v) => Ok(v),
            Err(e @ Error::Invalid(_)) => Err(e),
            Err(Error::Refused(r)) if !matches!(r.code, 16 | 17) => Err(Error::Refused(r)),
            Err(e) => {
                self.usable = false;
                Err(Error::Uncertain {
                    epoch: h.epoch,
                    operation_id: id.to_owned(),
                    cause: Box::new(e),
                })
            }
        }
    }
    /// A successful reply contains generation only, never an achieved-ACK field.
    pub fn timer_set(&mut self, a: &mut TimerSetAttempt, d: Deadline) -> Result<u64> {
        let p = self.timer_set_payload(&a.request, d)?;
        self.timer_exchange(
            crate::TIMER_SET,
            &p,
            &a.request.operation_id,
            &mut a.horizon,
            d,
            |b| {
                let mut c = Cursor::new(b);
                let g = parse(c.u64())?;
                parse(c.end())?;
                if g == 0 {
                    return Err(Error::Protocol("timer generation"));
                }
                Ok(g)
            },
        )
    }
    pub fn timer_cancel(&mut self, a: &mut TimerCancelAttempt, d: Deadline) -> Result<()> {
        let p = self.timer_cancel_payload(&a.request, d)?;
        self.timer_exchange(
            crate::TIMER_CANCEL,
            &p,
            &a.request.operation_id,
            &mut a.horizon,
            d,
            |b| {
                if !b.is_empty() {
                    return Err(Error::Protocol("timer cancel payload"));
                }
                Ok(())
            },
        )
    }
    pub fn timer_status(
        &mut self,
        ns: &str,
        id: &str,
        op: &str,
        d: Deadline,
    ) -> Result<Option<TimerStatus>> {
        let limits = self.timer_limits()?.clone();
        identity(ns, id)?;
        let mut p = self.prefix(op, ns, d)?;
        lp(&mut p, id.as_bytes());
        let b = self.exchange(crate::TIMER_STATUS, &p, d.local)?;
        let result = (|| {
            let mut c = Cursor::new(&b);
            let present = parse(c.u8())?;
            if present == 0 {
                parse(c.end())?;
                return Ok(None);
            }
            if present != 1 {
                return Err(Error::Protocol("timer presence"));
            }
            let generation = parse(c.u64())?;
            let active = parse(c.u8())?;
            let error = parse(c.u16())?;
            let next_index = parse(c.u64())?;
            let next_due_ms = parse(c.u64())?;
            let encoded = parse(c.bytes(3 * MAX_ID + limits.payload_bytes + 64))?;
            if encoded.get(..4) != Some(b"MTS1") {
                return Err(Error::Protocol("timer schedule version"));
            }
            let mut s = Cursor::new(&encoded[4..]);
            let schedule = TimerSchedule {
                namespace: parse(s.text())?,
                timer_id: parse(s.text())?,
                key: parse(s.text())?,
                anchor_ms: parse(s.u64())?,
                period_ms: parse(s.u64())?,
                jitter_low_ms: parse(s.u64())? as i64,
                jitter_high_ms: parse(s.u64())? as i64,
                seed: parse(s.u64())?,
                payload: parse(s.bytes(limits.payload_bytes))?.to_vec(),
            };
            parse(s.end())?;
            schedule_valid(&schedule, &limits)
                .map_err(|_| Error::Protocol("timer schedule fields"))?;
            let last = parse(c.bytes(MAX_ID + limits.payload_bytes + 64))?;
            let last = if last.is_empty() {
                None
            } else {
                Some(TimerEnvelope::decode(last)?)
            };
            parse(c.end())?;
            if generation == 0
                || active > 1
                || schedule.namespace != ns
                || schedule.timer_id != id
                || (error != 0 && !self.negotiated.refusals.contains_key(&error))
                || last
                    .as_ref()
                    .is_some_and(|w| w.timer_id != id || w.generation != generation)
            {
                return Err(Error::Protocol("timer status association"));
            }
            Ok(Some(TimerStatus {
                generation,
                active: active == 1,
                error,
                next_index,
                next_due_ms,
                schedule,
                last,
            }))
        })();
        if result.is_err() {
            self.usable = false;
        }
        result
    }
}
