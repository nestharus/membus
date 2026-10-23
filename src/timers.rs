//! Persisted schedules, compact progress and one deadline owner. No payload execution.
use crate::{lp, Cursor, Mutation, Store, Value, MAX_ID};
#[cfg(test)]
use crate::now_ms;
use std::{collections::{BTreeSet, HashMap, HashSet}, io, sync::{Arc, Condvar, Mutex, Weak}, thread, time::Duration};

pub(crate) const NS: &str = "__membus_timers_v1";
pub(crate) fn is_progress(ns:&str,key:&str)->bool {ns==NS && key.starts_with("p/")}
const MAX_TIMERS: usize = 4096;
const BUDGET: usize = 8 * 1024 * 1024;
const PAYLOAD_MAX: usize = 65536;
const MIN_PERIOD: u64 = 100;
const JITTER_MAX: i64 = 86_400_000;

#[derive(Clone)]
struct Schedule {
    ns: String, id: String, key: String, anchor: u64, period: u64,
    low: i64, high: i64, seed: u64, payload: Vec<u8>,
}
#[derive(Clone, Copy, Debug)]
struct Progress { generation: u64, index: u64, nominal: u64, due: u64, draw: i64, missed: u64 }
struct Timer { schedule: Schedule, generation: u64, next: u64, last: Option<Progress>, active: bool, error: u16 }
pub(crate) struct State {
    identities: HashSet<String>, timers: HashMap<String, Timer>,
    deadlines: BTreeSet<(u64, String)>, bytes: usize, stop: bool, running: bool,
}
pub(crate) type Shared = Arc<(Mutex<State>, Condvar)>;

fn identity(ns: &str, id: &str) -> Result<String, u16> {
    if ns == NS || ns.contains('\0') || id.contains('\0') || ns.is_empty() || id.is_empty() { return Err(3); }
    let key = format!("{ns}\0{id}");
    if key.len() + 2 > MAX_ID { return Err(4); }
    Ok(key)
}
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
impl Schedule {
    fn validate(&self) -> Result<(), u16> {
        identity(&self.ns, &self.id)?;
        if self.key.is_empty() || self.key.len() > MAX_ID || self.payload.len() > PAYLOAD_MAX { return Err(4); }
        if self.low > self.high || self.low < -JITTER_MAX || self.high > JITTER_MAX ||
            (self.period != 0 && (self.period < MIN_PERIOD || (self.high - self.low) as u64 >= self.period)) { return Err(3); }
        // Every legal offset must fit the first due time. Future overflow completes the timer.
        self.anchor.checked_add_signed(self.low).ok_or(3u16)?;
        self.anchor.checked_add_signed(self.high).ok_or(3u16)?;
        Ok(())
    }
    fn charge(&self) -> usize { 768 + 3 * (self.ns.len() + self.id.len()) + self.key.len() + self.payload.capacity() }
    fn draw(&self, index: u64) -> i64 {
        let width = (self.high - self.low + 1) as u64;
        // Rejection sampling avoids modulo bias; deterministic seed/index survives replay.
        let bound = u64::MAX - u64::MAX % width;
        let mut x = mix(self.seed ^ mix(index));
        while x >= bound { x = mix(x); }
        self.low + (x % width) as i64
    }
    fn time(&self, index: u64) -> Option<(u64,u64,i64)> {
        if self.period == 0 && index != 0 { return None; }
        let nominal = self.anchor.checked_add(self.period.checked_mul(index)?)?;
        let draw = self.draw(index);
        Some((nominal, nominal.checked_add_signed(draw)?, draw))
    }
    fn latest(&self, next: u64, now: u64) -> Option<u64> {
        if self.time(next)?.1 > now { return None; }
        if self.period == 0 { return Some(0); }
        // Width < period means times strictly increase; logarithmic search, no missed-interval walk.
        let upper = now.saturating_add(JITTER_MAX as u64).saturating_sub(self.anchor) / self.period;
        let mut lo = next;
        let mut hi = upper.max(next);
        while lo < hi {
            let mid = lo + (hi - lo) / 2 + 1;
            if self.time(mid).is_some_and(|(_,due,_)| due <= now) { lo = mid; } else { hi = mid - 1; }
        }
        Some(lo)
    }
    fn encode(&self) -> Vec<u8> {
        let mut b = b"MTS1".to_vec();
        for text in [&self.ns, &self.id, &self.key] { lp(&mut b,text.as_bytes()); }
        for n in [self.anchor,self.period,self.low as u64,self.high as u64,self.seed] { b.extend(n.to_le_bytes()); }
        lp(&mut b,&self.payload); b
    }
    fn decode(b: &[u8]) -> Result<Self,u16> {
        if b.get(..4) != Some(b"MTS1") { return Err(3); }
        let mut c=Cursor::new(&b[4..]);
        let s=Self { ns:c.text()?,id:c.text()?,key:c.text()?,anchor:c.u64()?,period:c.u64()?,
            low:c.u64()? as i64,high:c.u64()? as i64,seed:c.u64()?,payload:c.bytes(PAYLOAD_MAX)?.to_vec() };
        c.end()?;s.validate()?;Ok(s)
    }
}
impl Progress {
    fn encode(self) -> Vec<u8> {
        [self.generation,self.index,self.nominal,self.due,self.draw as u64,self.missed].into_iter().flat_map(u64::to_le_bytes).collect()
    }
    fn decode(b: &[u8]) -> Result<Self,u16> {
        let mut c=Cursor::new(b);
        let p=Self { generation:c.u64()?,index:c.u64()?,nominal:c.u64()?,due:c.u64()?,draw:c.u64()? as i64,missed:c.u64()? };
        c.end()?;Ok(p)
    }
    fn signal(self, s: &Schedule) -> Vec<u8> {
        let mut b=b"MBT1".to_vec();b.extend(1u16.to_le_bytes());lp(&mut b,s.id.as_bytes());
        b.extend(self.encode());lp(&mut b,&s.payload);b
    }
}
impl State {
    pub(crate) fn restore(values: &HashMap<(String,String),Arc<Value>>) -> io::Result<Shared> {
        let mut s=Self {identities:HashSet::new(),timers:HashMap::new(),deadlines:BTreeSet::new(),bytes:0,stop:false,running:false};
        for ((ns,key),v) in values {
            if ns != NS || !key.starts_with("s/") { continue; }
            let id=key[2..].to_owned();s.bytes+=256+id.len();s.identities.insert(id.clone());
            if v.tombstone { continue; }
            let schedule=Schedule::decode(&v.bytes).map_err(|_|io::Error::other("invalid persisted timer schedule"))?;
            if identity(&schedule.ns,&schedule.id).ok().as_ref() != Some(&id) { return Err(io::Error::other("timer identity mismatch")); }
            let progress=values.get(&(NS.to_owned(),format!("p/{id}"))).filter(|p| !p.tombstone)
                .map(|p| Progress::decode(&p.bytes)).transpose().map_err(|_|io::Error::other("invalid timer progress"))?
                .filter(|p|p.generation==v.version);
            if let Some(p)=progress {
                if schedule.time(p.index) != Some((p.nominal,p.due,p.draw)) || p.missed==0 { return Err(io::Error::other("inconsistent timer progress")); }
            }
            let next=progress.map_or(0,|p|p.index.saturating_add(1));
            let active=schedule.time(next).is_some() && progress.is_none_or(|p|p.index != u64::MAX);
            s.bytes+=schedule.charge();
            if active { s.deadlines.insert((schedule.time(next).unwrap().1,id.clone())); }
            s.timers.insert(id,Timer {schedule,generation:v.version,next,last:progress,active,error:0});
        }
        if s.identities.len()>MAX_TIMERS || s.bytes>BUDGET { return Err(io::Error::other("persisted timer limits exceeded")); }
        Ok(Arc::new((Mutex::new(s),Condvar::new())))
    }
}
pub(crate) fn hello_limits() -> Vec<u8> {
    let mut p=1u16.to_le_bytes().to_vec();
    p.extend((MAX_TIMERS as u32).to_le_bytes());p.extend((BUDGET as u64).to_le_bytes());
    p.extend((PAYLOAD_MAX as u32).to_le_bytes());p.extend(MIN_PERIOD.to_le_bytes());p.extend((JITTER_MAX as u64).to_le_bytes());p
}
impl Store {
    pub(crate) fn timer_call(&self,op:u8,ns:String,op_id:String,deadline:u64,c:&mut Cursor<'_>) -> Result<Vec<u8>,u16> {
        let name=c.text()?;let id=identity(&ns,&name)?;
        let mut state=self.timers.0.lock().unwrap();
        // Admission rechecks deadline after serial timer lock wait.
        if self.clock.utc_ms()>=deadline {return Err(5);}
        if op==crate::TIMER_STATUS {
            c.end()?;let Some(t)=state.timers.get(&id) else {return Ok(vec![0]);};
            let mut p=vec![1];p.extend(t.generation.to_le_bytes());p.push(t.active as u8);p.extend(t.error.to_le_bytes());
            p.extend(t.next.to_le_bytes());p.extend(t.schedule.time(t.next).map_or(0,|x|x.1).to_le_bytes());
            lp(&mut p,&t.schedule.encode());lp(&mut p,&t.last.map_or_else(Vec::new,|last|last.signal(&t.schedule)));return Ok(p);
        }
        let expected=c.u64()?;
        if op==crate::TIMER_CANCEL {
            c.end()?;
            let t=state.timers.get(&id).ok_or(25u16)?;
            if t.generation!=expected {return Err(12);}
            let outcome=self.mutate(Mutation { ns:NS.into(),key:format!("s/{id}"),op_id,deadline,ack:1,
                expected:Some(expected),bytes:Vec::new(),remove:true });
            if outcome.code!=0 {return Err(outcome.code);}
            let old=state.timers.remove(&id).unwrap();state.bytes-=old.schedule.charge();
            if let Some((_,due,_))=old.schedule.time(old.next) {state.deadlines.remove(&(due,id));}
            self.timers.1.notify_one();return Ok(Vec::new());
        }
        let key=c.text()?;let anchor=c.u64()?;let period=c.u64()?;let low=c.u64()? as i64;let high=c.u64()? as i64;
        let payload=c.bytes(PAYLOAD_MAX)?.to_vec();let ack=c.u8()?;c.end()?;
        if !matches!(ack,1|2) {return Err(3);}
        // Stable per-operation seed prevents retry fingerprints changing after an uncertain set.
        let seed=((crate::crc32(op_id.as_bytes()) as u64)<<32)|crate::crc32(id.as_bytes()) as u64;
        let schedule=Schedule {ns,id:name,key,anchor,period,low,high,seed,payload};schedule.validate()?;
        let old_charge=state.timers.get(&id).map_or(0,|t|t.schedule.charge());
        let identity_charge=if state.identities.contains(&id) {0}else{256+id.len()};
        if (!state.identities.contains(&id) && state.identities.len()==MAX_TIMERS) || state.bytes-old_charge+schedule.charge()+identity_charge>BUDGET {return Err(7);}
        let outcome=self.mutate(Mutation {ns:NS.into(),key:format!("s/{id}"),op_id,deadline,ack,
            expected:if expected==u64::MAX {None}else{Some(expected)},bytes:schedule.encode(),remove:false});
        if outcome.code!=0 {return Err(outcome.code);}
        // A retry of an older op cannot replace newer runtime state or rewind completed progress.
        let value=self.get_handle(NS,&format!("s/{id}")).ok_or(12u16)?;
        if value.version!=outcome.version {return Err(12);}
        if state.timers.get(&id).is_some_and(|t|t.generation==outcome.version) {return Ok(outcome.version.to_le_bytes().to_vec());}
        if let Some(t)=state.timers.get(&id) {
            if let Some((_,due,_))=t.schedule.time(t.next) {state.deadlines.remove(&(due,id.clone()));}
        }
        state.bytes=state.bytes-old_charge+schedule.charge()+identity_charge;state.identities.insert(id.clone());
        state.deadlines.insert((schedule.time(0).unwrap().1,id.clone()));
        state.timers.insert(id,Timer {schedule,generation:outcome.version,next:0,last:None,active:true,error:0});
        self.timers.1.notify_one();Ok(outcome.version.to_le_bytes().to_vec())
    }
    pub fn start_timers(self:&Arc<Self>) -> io::Result<Owner> {
        {
            let mut state=self.timers.0.lock().unwrap();
            if state.running {return Err(io::Error::other("timer owner already running"));}
            state.running=true;state.stop=false;
        }
        let weak=Arc::downgrade(self);let shared=self.timers.clone();let owner_shared=shared.clone();
        let thread=thread::spawn(move||run(weak,shared));Ok(Owner {shared:owner_shared,thread:Some(thread)})
    }
}
/// Caller owns this guard and must drop it before dropping its Store. It never owns a Store Arc.
pub struct Owner {shared:Shared,thread:Option<thread::JoinHandle<()>>}
impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.0.lock().unwrap().stop=true;self.shared.1.notify_all();
        if let Some(thread)=self.thread.take() {let _=thread.join();}
        self.shared.0.lock().unwrap().running=false;
    }
}
fn run(weak:Weak<Store>,shared:Shared) {
    let mut s=shared.0.lock().unwrap();
    loop {
        if s.stop {return;}
        let Some((due,id))=s.deadlines.first().cloned() else {s=shared.1.wait(s).unwrap();continue;};
        let Some(store)=weak.upgrade() else {return;};
        let now=store.clock.utc_ms();
        if due>now {
            drop(store);
            // One service owner rechecks wall-clock corrections at most once a second.
            s=shared.1.wait_timeout(s,Duration::from_millis((due-now).min(1000))).unwrap().0;continue;
        }
        s.deadlines.remove(&(due,id.clone()));
        let t=s.timers.get_mut(&id).unwrap();
        let index=t.schedule.latest(t.next,now).unwrap();
        let (nominal,due,draw)=t.schedule.time(index).unwrap();
        let progress=Progress {generation:t.generation,index,nominal,due,draw,missed:index-t.next+1};
        let outcome=publish_and_checkpoint(&store,t,progress,|m|store.mutate_progress(m));
        t.last=Some(progress);
        if outcome.code!=0 {t.error=outcome.code;t.active=false;continue;}
        t.next=index.saturating_add(1);
        t.active=index!=u64::MAX && t.schedule.time(t.next).is_some();
        if t.active {let due=t.schedule.time(t.next).unwrap().1;s.deadlines.insert((due,id));}
    }
}

fn publish_and_checkpoint(store:&Store,t:&Timer,progress:Progress,persist:impl FnOnce(Mutation)->crate::Outcome)->crate::Outcome {
    // Generic ephemeral wake. The injected refusal unit seam uses this same production ordering.
    store.signals.publish(&t.schedule.ns,&t.schedule.key,Arc::new(Value {version:t.generation,ack:1,
        bytes:progress.signal(&t.schedule),tombstone:false}));
    let id=identity(&t.schedule.ns,&t.schedule.id).unwrap();
    let op_id=format!("tf-{}-{}-{}",store.epoch,t.generation,progress.index);
    persist(Mutation {ns:NS.into(),key:format!("p/{id}"),op_id,deadline:store.clock.utc_ms().saturating_add(10000),
        ack:1,expected:None,bytes:progress.encode(),remove:false})
}

#[cfg(test)]
mod tests {
    use super::*;
    fn schedule()->Schedule {Schedule {ns:"quota".into(),id:"reset".into(),key:"timer/reset".into(),anchor:10000,
        period:60000,low:-9000,high:9000,seed:42,payload:b"tick".to_vec()}}
    fn path(label:&str)->std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".test-output").join(format!("timer-unit-{label}-{}-{}",std::process::id(),now_ms()))
    }
    fn schedule_mutation(s:&Schedule,id:&str)->Mutation {
        Mutation {ns:NS.into(),key:format!("s/{}",identity(&s.ns,&s.id).unwrap()),op_id:id.into(),deadline:now_ms()+10000,
            ack:1,expected:None,bytes:s.encode(),remove:false}
    }
    #[test] fn progress_never_retains_client_dedup_slots_on_write_or_replay() {
        let path=path("dedup");let config=crate::Config {dedup_cap:2,..Default::default()};
        {
            let store=Store::open(&path,config.clone()).unwrap();let s=schedule();let generation=store.mutate(schedule_mutation(&s,"set")).version;
            for index in 0..20 {
                let (nominal,due,draw)=s.time(index).unwrap();let p=Progress {generation,index,nominal,due,draw,missed:1};
                assert_eq!(store.mutate_progress(Mutation {ns:NS.into(),key:format!("p/{}",identity(&s.ns,&s.id).unwrap()),
                    op_id:format!("progress-{index}"),deadline:now_ms()+10000,ack:1,expected:None,bytes:p.encode(),remove:false}).code,0);
            }
            assert_eq!(store.state.lock().unwrap().dedup.len(),1);
            let mut m=schedule_mutation(&s,"client");m.ns="ordinary".into();m.key="x".into();m.bytes=b"x".to_vec();
            assert_eq!(store.mutate(m.clone()).code,0);m.op_id="third".into();assert_eq!(store.mutate(m).code,18);
        }
        let store=Store::open(&path,config).unwrap();assert_eq!(store.state.lock().unwrap().dedup.len(),2);
        let state=store.timers.0.lock().unwrap();assert_eq!(state.timers.values().next().unwrap().next,20);
    }
    #[test] fn published_wake_without_checkpoint_repeats_stable_due_id_after_replay() {
        let path=path("refusal");let mut s=schedule();s.period=0;s.low=0;s.high=0;
        let generation;
        {
            let store=Store::open(&path,Default::default()).unwrap();generation=store.mutate(schedule_mutation(&s,"set")).version;
            let p=Progress {generation,index:0,nominal:s.anchor,due:s.anchor,draw:0,missed:1};
            let t=Timer {schedule:s.clone(),generation,next:0,last:None,active:true,error:0};
            let out=publish_and_checkpoint(&store,&t,p,|_|crate::Outcome::error(6));assert_eq!(out.code,6);
            let usage=store.signals.usage();assert_eq!(u64::from_le_bytes(usage[..8].try_into().unwrap()),1);
            assert!(store.get_handle(NS,&format!("p/{}",identity(&s.ns,&s.id).unwrap())).is_none());
        }
        let store=Arc::new(Store::open(&path,Default::default()).unwrap());
        {
            let state=store.timers.0.lock().unwrap();let timer=state.timers.values().next().unwrap();
            assert_eq!((timer.generation,timer.next),(generation,0));assert_eq!(timer.schedule.latest(timer.next,s.anchor),Some(0));
        }
        let owner=store.start_timers().unwrap();
        let until=std::time::Instant::now()+Duration::from_secs(2);
        loop {
            let completed=!store.timers.0.lock().unwrap().timers.values().next().unwrap().active;
            if completed {break;} assert!(std::time::Instant::now()<until);thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(u64::from_le_bytes(store.signals.usage()[..8].try_into().unwrap()),1);
        let value=store.get_handle(NS,&format!("p/{}",identity(&s.ns,&s.id).unwrap())).unwrap();
        let p=Progress::decode(&value.bytes).unwrap();assert_eq!((p.generation,p.index,p.missed),(generation,0,1));
        drop(owner);
    }
    #[test] fn real_checkpoint_capacity_refusal_stops_owner_then_repeats_after_reopen() {
        let path=path("capacity-refusal");let mut s=schedule();s.period=0;s.low=0;s.high=0;
        let m=schedule_mutation(&s,"set");let limit=(16+crate::mutation_bytes(&m,1,now_ms()).len()) as u64;
        let generation;
        {
            let store=Store::open(&path,crate::Config {state_limit_bytes:limit,..Default::default()}).unwrap();
            let out=store.mutate(m);assert_eq!(out.code,0);generation=out.version;
        }
        {
            // Direct internal mutation writes the schedule; reopening restores its timer map.
            let store=Arc::new(Store::open(&path,crate::Config {state_limit_bytes:limit,..Default::default()}).unwrap());
            let owner=store.start_timers().unwrap();let until=std::time::Instant::now()+Duration::from_secs(2);
            loop {
                let state=store.timers.0.lock().unwrap();let t=state.timers.values().next().unwrap();
                if !t.active {assert_eq!(t.error,7);assert_eq!((t.last.unwrap().generation,t.last.unwrap().index),(generation,0));break;}
                drop(state);assert!(std::time::Instant::now()<until);thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(u64::from_le_bytes(store.signals.usage()[..8].try_into().unwrap()),1);
            assert!(store.get_handle(NS,&format!("p/{}",identity(&s.ns,&s.id).unwrap())).is_none());drop(owner);
        }
        // A larger configured budget is the deliberate recovery action; no hidden retry.
        let store=Arc::new(Store::open(&path,Default::default()).unwrap());let owner=store.start_timers().unwrap();
        let until=std::time::Instant::now()+Duration::from_secs(2);
        loop {
            if !store.timers.0.lock().unwrap().timers.values().next().unwrap().active {break;}
            assert!(std::time::Instant::now()<until);thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(u64::from_le_bytes(store.signals.usage()[..8].try_into().unwrap()),1);
        let value=store.get_handle(NS,&format!("p/{}",identity(&s.ns,&s.id).unwrap())).unwrap();
        let p=Progress::decode(&value.bytes).unwrap();assert_eq!((p.generation,p.index),(generation,0));drop(owner);
    }
    #[test] fn restore_filters_old_generation_progress_completed_oneoff_and_cancel_charge() {
        let mut s=schedule();s.period=0;s.low=0;s.high=0;let id=identity(&s.ns,&s.id).unwrap();
        let p=Progress {generation:1,index:0,nominal:s.anchor,due:s.anchor,draw:0,missed:1};
        let mut values=HashMap::from([
            ((NS.into(),format!("s/{id}")),Arc::new(Value {version:2,ack:1,bytes:s.encode(),tombstone:false})),
            ((NS.into(),format!("p/{id}")),Arc::new(Value {version:3,ack:1,bytes:p.encode(),tombstone:false}))]);
        let state=State::restore(&values).unwrap();assert_eq!(state.0.lock().unwrap().timers[&id].next,0);
        let matched=Progress {generation:2,..p};Arc::make_mut(values.get_mut(&(NS.into(),format!("p/{id}"))).unwrap()).bytes=matched.encode();
        let state=State::restore(&values).unwrap();assert!(!state.0.lock().unwrap().timers[&id].active);
        let wrong=Progress {draw:1,..matched};Arc::make_mut(values.get_mut(&(NS.into(),format!("p/{id}"))).unwrap()).bytes=wrong.encode();
        assert!(State::restore(&values).is_err());
        Arc::make_mut(values.get_mut(&(NS.into(),format!("s/{id}"))).unwrap()).tombstone=true;
        let state=State::restore(&values).unwrap();let state=state.0.lock().unwrap();assert!(state.timers.is_empty());assert_eq!(state.bytes,256+id.len());
    }
    #[test] fn only_one_service_timer_owner_can_run() {
        let store=Arc::new(Store::open(&path("owner"),Default::default()).unwrap());
        let owner=store.start_timers().unwrap();assert!(store.start_timers().is_err());drop(owner);
        let owner=store.start_timers().unwrap();drop(owner);
    }
    #[test] fn draws_bounded_stable_and_times_increasing() {
        let s=schedule();s.validate().unwrap();let mut previous=0;
        for i in 0..10000 {let (_,due,draw)=s.time(i).unwrap();assert!((-9000..=9000).contains(&draw));assert!(due>previous);previous=due;assert_eq!(s.draw(i),s.draw(i));}
        assert!(s.draw(0)!=s.draw(1));
    }
    #[test] fn catchup_is_one_logarithmic_latest_due() {
        let mut s=schedule();s.low=0;s.high=0;s.anchor=1000;s.period=100;
        assert_eq!(s.latest(2,1550),Some(5));assert_eq!(s.latest(6,1550),None);
        assert_eq!(s.latest(0,1_000_000_000),Some(9_999_990));
        s.period=0;assert_eq!(s.latest(0,2000),Some(0));assert_eq!(s.latest(1,2000),None);
    }
    #[test] fn record_roundtrip_and_compact_progress() {
        let s=schedule();let decoded=Schedule::decode(&s.encode()).unwrap();assert_eq!(decoded.time(7),s.time(7));
        let (nominal,due,draw)=s.time(7).unwrap();let p=Progress {generation:1,index:7,nominal,due,draw,missed:3};
        assert_eq!(p.encode().len(),48);assert_eq!(Progress::decode(&p.encode()).unwrap().draw,draw);
        assert!(Schedule::decode(b"junk").is_err());assert!(Progress::decode(&[0;47]).is_err());
    }
    #[test] fn reject_collisions_and_overflow_and_crossing_jitter() {
        assert!(identity("a\0b","c").is_err());assert!(identity(NS,"c").is_err());
        let mut s=schedule();s.low=-30000;s.high=30000;assert!(s.validate().is_err());
        s.period=99;s.low=0;s.high=0;assert!(s.validate().is_err());
        s.period=100;s.anchor=u64::MAX;s.high=1;assert!(s.validate().is_err());
        s.anchor=1;s.low=-2;s.high=0;assert!(s.validate().is_err());
    }
}
