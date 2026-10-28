//! Continuing delivery and expiry participant, owned and joined by Hub.
use super::*;

pub(super) struct DeliveryWorker {
    epoch: u64,
    owner: Arc<(Mutex<State>, Condvar)>,
    sink: Sink,
    clock: crate::chunks::Clock,
}
impl DeliveryWorker {
    pub(super) fn start(
        epoch: u64,
        owner: Arc<(Mutex<State>, Condvar)>,
        sink: Sink,
        clock: crate::chunks::Clock,
    ) -> thread::JoinHandle<()> {
        let worker = Self {
            epoch,
            owner,
            sink,
            clock,
        };
        thread::spawn(move || worker.run())
    }
    fn run(self) {
        let Self {
            epoch,
            owner,
            sink,
            clock,
        } = self;
        loop {
            let job = {
                let (lock, wake) = &*owner;
                let mut s = lock.lock().unwrap();
                loop {
                    let retired = detach_expired(&mut s, clock.mono_ms());
                    if !retired.is_empty() {
                        drop(s);
                        settle_rows(retired);
                        s = lock.lock().unwrap();
                        continue;
                    }

                    if s.stop {
                        let retired = std::mem::take(&mut s.finite);
                        drop(s);
                        settle_rows(retired);
                        return;
                    }
                    let mut job = None;
                    let count = s.subs.len();
                    for _ in 0..count {
                        let mut sub = s.subs.pop_front().unwrap();
                        if let Some(change) = sub.queue.pop_front() {
                            job = Some(Job::Legacy(
                                sub.id,
                                sub.slot.as_ref().unwrap().clone(),
                                sub.values,
                                change,
                            ));
                        }
                        s.subs.push_back(sub);
                        if job.is_some() {
                            break;
                        }
                    }
                    if let Some(job) = job {
                        break job;
                    }
                    let mut hint = None;
                    for _ in 0..s.finite.len() {
                        #[cfg(test)]
                        {
                            s.worker_visits.1 += 1;
                        }
                        let mut row = s.finite.pop_front().unwrap();
                        if hint.is_none() {
                            if let Some(seq) = row.pending.take() {
                                hint = Some(Job::Hint(
                                    row.binding,
                                    row.slot.clone(),
                                    seq,
                                    row.write_ms,
                                    row.until,
                                    row.generation,
                                ));
                            }
                        }
                        s.finite.push_back(row);
                        if hint.is_some() {
                            break;
                        }
                    }
                    if let Some(job) = hint {
                        break job;
                    }
                    if let Some(until) = s.finite_expiry {
                        let wait = Duration::from_millis(until.saturating_sub(clock.mono_ms()));
                        s = wake.wait_timeout(s, wait).unwrap().0;
                    } else {
                        s = wake.wait(s).unwrap();
                    }
                }
            };
            let (id, slot, values, change) = match job {
                Job::Legacy(id, slot, values, change) => (id, slot, values, change),
                Job::Hint(binding, slot, seq, write_ms, until, generation) => {
                    // Local completion confers no recipient-accepted evidence. No
                    // persistent connection survives the actual in-flight owner.
                    let mut frame = [0u8; 64];
                    frame[..4].copy_from_slice(&60u32.to_le_bytes());
                    frame[4..8].copy_from_slice(b"MBH2");
                    frame[8..56].copy_from_slice(&binding.bytes());
                    frame[56..].copy_from_slice(&seq.to_le_bytes());
                    let now = clock.mono_ms();
                    if now < until {
                        let mut connection = None;
                        let timeout = (until - now).min(write_ms as u64) as u32;
                        let result = sink(&slot.endpoint, &frame, timeout, &mut connection, &clock);
                        drop(connection);
                        if result.is_err() {
                            // Exact old identity only. Detach while locked; return
                            // allocation ownership after releasing the Hub mutex.
                            let mut s = owner.0.lock().unwrap();
                            let mut retired = std::collections::LinkedList::new();
                            for _ in 0..s.finite.len() {
                                let row = s.finite.pop_front().unwrap();
                                if row.binding == binding && row.generation == generation {
                                    retired.push_back(row);
                                } else {
                                    s.finite.push_back(row);
                                }
                            }
                            drop(s);
                            settle_rows(retired);
                        }
                    }
                    settle_slot(slot);
                    continue;
                }
            };
            // Encoding and all endpoint I/O are outside the publication/hub lock.
            let result = slot.send(sink, &encode(epoch, id, &change, values), &clock);
            let mut s = owner.0.lock().unwrap();
            s.queue_bytes -= change.charge;
            if let Some(sub) = s.subs.iter_mut().find(|sub| sub.id == id) {
                sub.bytes -= change.charge;
                if result.is_ok() {
                    sub.written += 1;
                    sub.last = change.seq;
                    if sub.active {
                        sub.status = if sub.queue.is_empty() {
                            WRITTEN
                        } else {
                            QUEUED
                        };
                    }
                } else {
                    sub.failed += 1;
                    if sub.active {
                        sub.status = FAILED;
                    }
                    sub.active = false;
                    drop(sub.slot.take());
                    let freed = clear(sub);
                    s.queue_bytes -= freed;
                }
            }
        }
    }
}
