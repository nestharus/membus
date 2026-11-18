//! Small retained MBK1 fixtures on production read/capture/decoder boundaries.
use crate::*;
use super::*;
use std::{collections::{BTreeMap, HashSet, VecDeque}, io::{Seek, SeekFrom, Write},
    sync::{atomic::{AtomicUsize, Ordering}, Barrier}};

static WORK: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
pub(crate) fn work(kind: usize, n: usize) { WORK[kind].fetch_add(n, Ordering::SeqCst); }
fn reset() { for v in &WORK { v.store(0, Ordering::SeqCst); } }
fn counts() -> [usize; 4] { std::array::from_fn(|i| WORK[i].load(Ordering::SeqCst)) }
fn expected() -> Vec<(&'static str, Option<Vec<u8>>)> {
    vec![("row/a", Some(vec![0, 255, 128, b'\n', b'"', 1])),
        ("id/1", Some(1u64.to_le_bytes().to_vec())),
        ("source/1", Some(0x0102030405060708u64.to_le_bytes().to_vec())),
        ("row/b", Some((0..4097).map(|i| (i * 37 % 256) as u8).collect())),
        ("id/2", Some(2u64.to_le_bytes().to_vec())),
        ("source/2", Some(0xff80706050403020u64.to_le_bytes().to_vec())),
        ("row/c", Some(b"opaque unknown fields: {\"future\":[null,true,17]}".to_vec())),
        ("empty", Some(vec![])), ("deleted", None)]
}
// Encode authored independent original bytes; the primary decoder and writer
// must agree, rather than deriving the expected values from decoded output.
fn original(ack: u8) -> Vec<u8> {
    let mut b = b"MAR1".to_vec(); lp(&mut b, b"n"); lp(&mut b, b"authored");
    b.extend(1u64.to_le_bytes()); b.extend(2_000_000u64.to_le_bytes()); b.push(ack);
    b.extend(0u32.to_le_bytes()); b.extend((expected().len() as u16).to_le_bytes());
    for (key, value) in expected() {
        lp(&mut b, key.as_bytes()); b.push(value.is_none() as u8); lp(&mut b, value.as_deref().unwrap_or_default());
    }
    b
}
fn payload(first: u64, ack: u8) -> Vec<u8> {
    let b = original(ack);
    let request = atomic::AtomicRequest::from_bytes(&b).unwrap();
    assert_eq!(request.to_bytes(), b);
    atomic::envelope_bytes(&request, first, 1_000_000)
}
fn entries(file: Arc<Mutex<File>>, offset: u64, len: usize, first: u64, ack: u8) -> Vec<((String,String),Arc<value_disk::Entry>)> {
    expected().into_iter().enumerate().map(|(i,(key,value))| {
        let m = Mutation { ns: "n".into(), key: key.into(), op_id: "authored".into(), deadline: 0,
            ack, expected: None, remove: value.is_none(), bytes: value.unwrap_or_default() };
        let l = value_disk::Location {class: Class::FOREVER, file: file.clone(), offset, record_len: len as u64, subrecord: Some(i as u16)};
        (("n".into(), key.into()), Arc::new(value_disk::Entry::new(&m,first+i as u64,l)))
    }).collect()
}
fn fixture(label: &str) -> (PathBuf, Arc<Store>, usize) {
    let root = PathBuf::from(std::env::var_os("MEMBUS_COHORT_FIXTURE_ROOT").expect("explicit retained root"));
    assert!(root.is_absolute() && root.is_dir());
    let path = root.join(format!("{label}-{}-{}",std::process::id(),now_ms())); fs::create_dir(&path).unwrap();
    let encoded = chunks::frame(b"MBK1", &payload(10,2));
    let mut file = OpenOptions::new().read(true).write(true).create_new(true).open(path.join("authored.frame")).unwrap();
    file.write_all(&encoded).unwrap(); file.flush().unwrap();
    let committed = Arc::new(RwLock::new(entries(Arc::new(Mutex::new(file)),0,encoded.len(),10,2).into_iter().collect::<BTreeMap<_,_>>()));
    let config = Config { capacity: 2*1024*1024, cache_bytes: 0, signal_bytes: 0, snapshot_bytes: 1024*1024,
        snapshot_ttl_ms: 1000, clock: Some(chunks::ManualClock::new(1_000_000)), cache_rules: vec![], ..Config::default() };
    let clock = chunks::Clock::new(config.clock.clone());
    let state = Arc::new(Mutex::new(store::State {pending: HashMap::new(), metadata: HashMap::new(),
        namespace_counts: HashMap::from([("n".into(),8)]),keys:9,committed_bytes:9*store::index_charge(&("n".into(),"source/2".into())),
        dedup_bytes:0,dedup:HashMap::new(),policy_publishing:HashSet::new(),retired:VecDeque::new(),retired_set:HashSet::new(),
        expiry:VecDeque::new(),next:19,reserved_bytes:0,memory_reserved:0,stripe_reserved:[0;4],stripe_items:[0;4],
        persisted_bytes:encoded.len() as u64,pending_disk:0,read_only:false,warming:false,tomb_candidates:HashSet::new()}));
    let cache = Arc::new(cache::Cache::new(&config,clock.clone()));
    let chunks = Arc::new(Mutex::new(chunks::ChunkStore::new(path.clone())));
    let policies = Arc::new(RwLock::new(chunks::Policies::default()));
    let class_reservations = Arc::new(Mutex::new(HashMap::new())); let next_issued = Arc::new(AtomicU64::new(19));
    let flags = Arc::new(store::Flags::default());
    let router = Arc::new(routing::ChunkRouter { locations:Mutex::new(HashMap::new()),selection_hook:Mutex::new(None),
        sinks:(0..4).map(|_|Mutex::new(HashMap::new())).collect(),table:RwLock::new(HashMap::new()),
        fenced:std::array::from_fn(|_|AtomicBool::new(false)),closed_io:Mutex::new([0;3]),policies:policies.clone(),
        class_reservations:class_reservations.clone(),chunks:chunks.clone(),clock:clock.clone(),next:next_issued.clone(),
        flags:flags.clone(),chunk_bytes:config.chunk_bytes,rotation_ms:config.rotation_ms,fail_sync:false });
    let admission_gate=Arc::new(RwLock::new(()));
    let maint=Arc::new(maintenance::Maint {admission_gate:admission_gate.clone(),cache:cache.clone(),state:state.clone(),
        committed:committed.clone(),chunks:chunks.clone(),router:router.clone(),flags:flags.clone(),clock:clock.clone(),
        next_issued:next_issued.clone(),config:config.clone(),path:path.clone()});
    let owner=OpenOptions::new().write(true).create_new(true).open(path.join("fixture.owner")).unwrap();
    let s=Arc::new(Store {clock:clock.clone(),timers:timers::State::restore(&HashMap::new()).unwrap(),
        signals:signals::Hub::new(1,&config,clock),capture_metrics:std::array::from_fn(|_|AtomicU64::new(0)),
        capture_visits:AtomicUsize::new(0),capture_waiters:AtomicUsize::new(0),committed,cache,
        cold_readers:(Mutex::new(0),Condvar::new()),
        read_usage:Arc::new(read_cohort::ReadUsage::default()),
        scans:Arc::new(Mutex::new(ScanState {next:1,snapshots:HashMap::new()})),scan_usage:Arc::new(ScanUsage::default()),
        scan_reaper:None,ticker:None,state,done:Condvar::new(),maint,router,chunks,policies,next_issued,flags,path:path.clone(),
        report:Mutex::new(chunks::ReplayReport::default()),appenders:vec![],flush_owner:None,batch_gate:Mutex::new(()),
        admission_gate,policy_updates:Mutex::new(()),class_reservations,policy_after_commit:Mutex::new(None),
        policy_join_observed:Mutex::new(None),config,epoch:1,_owner:owner});
    println!("retained_fixture={} frame_bytes={} members=9 live=8",path.display(),encoded.len());
    (path,s,encoded.len())
}
fn exact(key: &str, v: &Value, first: u64, ack: u8) {
    let (i,(_,bytes))=expected().into_iter().enumerate().find(|(_,r)|r.0==key).unwrap();
    assert_eq!(v.version,first+i as u64); assert_eq!(v.ack,ack);
    assert_eq!(v.tombstone,bytes.is_none()); assert_eq!(v.bytes,bytes.unwrap_or_default(),"{key}");
}
fn live() -> impl Iterator<Item=&'static str> { expected().into_iter().filter(|(_,v)|v.is_some()).map(|(k,_)|k) }
fn entry(s: &Store,key: &str) -> Arc<value_disk::Entry> { s.committed.read().unwrap().get(&("n".into(),key.into())).unwrap().clone() }
fn rewrite(e: &value_disk::Entry, bytes: &[u8]) {
    let mut f=e.location.file.lock().unwrap(); f.seek(SeekFrom::Start(e.location.offset)).unwrap();
    f.write_all(bytes).unwrap(); f.flush().unwrap();
}
fn scan_usage(s: &Store) -> (usize,usize) { (s.scan_usage.count.load(Ordering::Acquire),s.scan_usage.bytes.load(Ordering::Acquire)) }

#[test]
fn default_eight_reads_keep_exact_bypass_and_old_work_witness() {
    let (_,s,f)=fixture("default"); reset();
    for key in live() { exact(key,&s.try_get("n",key).unwrap().unwrap(),10,2); }
    assert!(s.try_get("n","deleted").unwrap().is_none());
    assert!(s.try_get("n","absent").unwrap().is_none());
    let stats=s.cache_stats();assert_eq!((stats.cold_reads,stats.entries,stats.coalesced_reads),(8,0,0));
    assert_eq!(counts(),[8*f,8*(f-16),8,72]);println!("default_work={:?} cold=8 entries=0 coalesced=0",counts());
}
#[test]
fn ordinary_snapshot_shares_complete_frame_work_and_exact_metadata() {
    let (_,s,f)=fixture("snapshot-work");let (token,view)=s.snapshot_open("n").unwrap();reset();
    for (key,row) in &view.rows { exact(key,&row.load(&s,"n",key).unwrap(),10,2); }
    println!("snapshot_work={:?} expected={:?}",counts(),[f,f-16,1,9]);
    s.snapshot_release("n",&token).unwrap();drop(view);
    assert_eq!(scan_usage(&s),(0,0));
    assert_eq!(counts(),[f,f-16,1,9]);
}

// ADDITIVE_CASES: parent runtime witness above remains separate from API absence.
#[test]
fn explicit_cohort_shares_exact_values_without_changing_default_reads() {
    let (_,s,f)=fixture("cohort-work");reset();let before=s.health2().memory_bytes;
    let cohort=s.read_cohort(128*1024).unwrap();assert_eq!(s.read_cohort_bytes(),128*1024);
    assert_eq!(s.health2().memory_bytes,before+128*1024);
    for key in live() { exact(key,&cohort.try_get("n",key).unwrap().unwrap(),10,2); }
    assert!(cohort.try_get("n","deleted").unwrap().is_none());
    assert_eq!(counts(),[f,f-16,1,9]);assert_eq!(s.cache_stats().entries,0);
    println!("explicit_work={:?} reserved={}",counts(),s.read_cohort_bytes());
    drop(cohort);assert_eq!(s.read_cohort_bytes(),0);
    for key in live() { exact(key,&s.try_get("n",key).unwrap().unwrap(),10,2); }
    assert_eq!(counts(),[9*f,9*(f-16),9,81]);assert_eq!(s.cache_stats().cold_reads,9);
}
#[test]
fn bypass_policies_stay_independent_with_explicit_cohort_alive() {
    for mode in 0..3 {
        let (_,mut s,f)=fixture(&format!("policy-{mode}"));let s=Arc::get_mut(&mut s).unwrap();
        s.config.cache_bytes=if mode==0 {0}else{1024*1024};
        s.config.cache_policy=CachePolicy {initial_ttl:0,active_ttl:if mode==2 {0}else{60000},never_cache:mode==1,refresh_on_read:false};
        s.cache=Arc::new(cache::Cache::new(&s.config,s.clock.clone()));s.trim_cache(&s.state.lock().unwrap());
        let c=s.read_cohort(128*1024).unwrap();c.try_get("n","id/1").unwrap();reset();let before=s.cache_stats();
        for _ in 0..8 { exact("id/1",&s.try_get("n","id/1").unwrap().unwrap(),10,2); }
        assert_eq!(s.cache_stats().cold_reads-before.cold_reads,8);assert_eq!(s.cache_stats().entries,0);
        assert_eq!(s.cache_stats().coalesced_reads,0);assert_eq!(counts(),[8*f,8*(f-16),8,72]);
    }
}
#[test]
fn last_cohort_value_owns_budget_after_clear_drop_and_store_drop() {
    let (_,s,f)=fixture("last-owner");reset();let budget=128*1024;
    let cohort=s.read_cohort(budget).unwrap();let value=cohort.try_get("n","source/1").unwrap().unwrap();
    let clone=value.clone();cohort.clear();drop(cohort);assert_eq!(s.read_cohort_bytes(),budget);
    drop(value);assert_eq!(s.read_cohort_bytes(),budget);exact("source/1",&clone,10,2);
    drop(clone);assert_eq!(s.read_cohort_bytes(),0);
    let c=s.read_cohort(budget).unwrap();let v=c.try_get("n","source/1").unwrap().unwrap();
    assert_eq!(counts(),[2*f,2*(f-16),2,18]);drop(c);let usage=s.read_usage.clone();drop(s);
    assert_eq!(usage.bytes.load(Ordering::Acquire),budget);exact("source/1",&v,10,2);drop(v);
    assert_eq!(usage.bytes.load(Ordering::Acquire),0);
}
#[test]
fn snapshot_last_payload_retains_workspace_and_base_charge_after_expiry() {
    let (_,s,_)=fixture("snapshot-owner");let (token,view)=s.snapshot_open("n").unwrap();let base=scan_usage(&s);
    let row=&view.rows[0];let value=row.1.load(&s,"n",&row.0).unwrap();assert!(scan_usage(&s).1>base.1);
    let charge=scan_usage(&s);s.config.clock.as_ref().unwrap().advance(1000);
    assert!(s.snapshot_get("n",&token).is_err());drop(view);assert_eq!(scan_usage(&s),charge);
    exact(&live().find(|k|*k=="empty").unwrap(),&value,10,2);drop(value);assert_eq!(scan_usage(&s),(0,0));
}
#[test]
fn finite_budget_refuses_before_capture_and_reacquires_after_evicted_lease() {
    let (path,s,f)=fixture("budget");let e=entry(&s,"id/1");let charge=read_cohort::frame_charge(&e.location).unwrap();
    let too_small=s.read_cohort(charge+511).unwrap();reset();assert!(too_small.try_get("n","id/1").is_err());
    assert_eq!(counts(),[0;4]);assert!(!s.storage_error());drop(too_small);
    let c=s.read_cohort(charge+512).unwrap();let value=c.try_get("n","id/1").unwrap().unwrap();
    let frame=chunks::frame(b"MBK1",&payload(100,1));let mut file=OpenOptions::new().read(true).write(true).create_new(true).open(path.join("second.frame")).unwrap();
    file.write_all(&frame).unwrap();let replacement=entries(Arc::new(Mutex::new(file)),0,frame.len(),100,1);
    s.committed.write().unwrap().extend(replacement);c.clear();let before=counts();
    assert!(c.try_get("n","source/1").is_err());assert_eq!(counts(),before);drop(value);
    exact("source/1",&c.try_get("n","source/1").unwrap().unwrap(),100,1);
    assert_eq!(counts(),[2*f,2*(f-16),2,18]);drop(c);assert_eq!(s.read_cohort_bytes(),0);
    assert!(s.read_cohort(s.config.capacity).is_err());assert_eq!(s.read_cohort_bytes(),0);
}
#[test]
fn concurrent_cohort_leases_share_one_capture_and_admissions_do_not_overbook() {
    let (_,s,f)=fixture("concurrent");let c=s.read_cohort(128*1024).unwrap();reset();
    let gate=Barrier::new(9);
    std::thread::scope(|scope| {
        let mut jobs=vec![];
        for key in live() { let (c,gate)=(&c,&gate);jobs.push(scope.spawn(move || {gate.wait();c.try_get("n",key).unwrap().unwrap()})); }
        gate.wait();for (key,job) in live().zip(jobs) { exact(key,&job.join().unwrap(),10,2); }
    });
    assert_eq!(counts(),[f,f-16,1,9]);drop(c);
    let available=s.cache_limit(&s.state.lock().unwrap());let each=available/2+1;
    let gate=Barrier::new(3);
    std::thread::scope(|scope| {let mut jobs=vec![];
        for _ in 0..2 {let (s,gate)=(&s,&gate);jobs.push(scope.spawn(move||{gate.wait();s.read_cohort(each)}));}
        gate.wait();let values:Vec<_>=jobs.into_iter().map(|j|j.join().unwrap()).collect();
        assert_eq!(values.iter().filter(|r|r.is_ok()).count(),1);assert_eq!(s.read_cohort_bytes(),each);
    });assert_eq!(s.read_cohort_bytes(),0);
}
#[test]
fn concurrent_snapshot_workspaces_refuse_before_capture_and_release_on_last_value() {
    let (_,mut s,f)=fixture("snapshot-budget");let (token,view)=s.snapshot_open("n").unwrap();
    let base=scan_usage(&s).1;s.snapshot_release("n",&token).unwrap();drop(view);
    let charge=read_cohort::frame_charge(&entry(&s,"id/1").location).unwrap();
    Arc::get_mut(&mut s).unwrap().config.snapshot_bytes=2*base+charge;
    let (t1,v1)=s.snapshot_open("n").unwrap();let (t2,v2)=s.snapshot_open("n").unwrap();reset();
    let gate=Barrier::new(3);
    let results=std::thread::scope(|scope| {
        let jobs:Vec<_>=[&v1,&v2].into_iter().map(|v| {let (s,gate)=(&s,&gate);
            scope.spawn(move||{gate.wait();let (key,row)=&v.rows[0];row.load(s,"n",key)})}).collect();
        gate.wait();jobs.into_iter().map(|j|j.join().unwrap()).collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r|r.is_ok()).count(),1);
    assert_eq!(results.iter().filter(|r|matches!(r,Err(22))).count(),1);
    assert_eq!(counts(),[f,f-16,1,9]);assert!(!s.storage_error());assert_eq!(scan_usage(&s),(2,2*base+charge));
    s.snapshot_release("n",&t1).unwrap();s.snapshot_release("n",&t2).unwrap();drop(v1);drop(v2);
    assert_eq!(scan_usage(&s),(1,base+charge));drop(results);assert_eq!(scan_usage(&s),(0,0));
}
#[test]
fn revisions_offsets_and_file_handles_never_alias_snapshot_old_revision() {
    let (path,s,f)=fixture("identities");let (token,view)=s.snapshot_open("n").unwrap();
    let c=s.read_cohort(256*1024).unwrap();exact("id/1",&c.try_get("n","id/1").unwrap().unwrap(),10,2);
    let old=entry(&s,"id/1");let frame=chunks::frame(b"MBK1",&payload(100,1));assert_eq!(frame.len(),f);
    {let mut file=old.location.file.lock().unwrap();file.seek(SeekFrom::Start(f as u64)).unwrap();file.write_all(&frame).unwrap();}
    s.committed.write().unwrap().extend(entries(old.location.file.clone(),f as u64,f,100,1));
    exact("id/1",&c.try_get("n","id/1").unwrap().unwrap(),100,1);
    let mut file=OpenOptions::new().read(true).write(true).create_new(true).open(path.join("different.frame")).unwrap();
    let third=chunks::frame(b"MBK1",&payload(200,2));file.write_all(&third).unwrap();
    s.committed.write().unwrap().extend(entries(Arc::new(Mutex::new(file)),0,f,200,2));
    exact("id/1",&c.try_get("n","id/1").unwrap().unwrap(),200,2);
    for (key,row) in &view.rows {exact(key,&row.load(&s,"n",key).unwrap(),10,2);}
    let tomb=entry(&s,"deleted");assert!(c.try_get("n","deleted").unwrap().is_none());
    let retained=read_cohort::SnapshotWorkspace::new(view._charge.clone());exact("deleted",&retained.read(&s,"n","deleted",&tomb).unwrap(),200,2);
    s.snapshot_release("n",&token).unwrap();drop(retained);drop(view);assert_eq!(scan_usage(&s),(0,0));
}
#[test]
fn corruption_after_eviction_revalidates_and_errors_stay_sticky() {
    let (_,s,f)=fixture("revalidate");let e=entry(&s,"id/1");let original=chunks::frame(b"MBK1",&payload(10,2));
    let c=s.read_cohort(128*1024).unwrap();c.try_get("n","id/1").unwrap();c.clear();reset();
    let mut corrupt=original.clone();let at=corrupt.windows(6).position(|b|b==[0,255,128,b'\n',b'"',1]).unwrap();
    corrupt[at]^=1;rewrite(&e,&corrupt);
    for _ in 0..2 {assert!(c.try_get("n","id/1").is_err());}
    assert_eq!(counts(),[2*f,2*(f-16),0,0]);assert!(s.storage_error());
    rewrite(&e,&original);exact("id/1",&c.try_get("n","id/1").unwrap().unwrap(),10,2);
    assert!(s.storage_error());assert_eq!(counts(),[3*f,3*(f-16),1,9]);drop(c);assert_eq!(s.read_cohort_bytes(),0);
}
#[test]
fn equal_offset_extent_and_revision_on_different_files_keep_exact_bytes() {
    let (path,s,f)=fixture("equal-identities");let c=s.read_cohort(128*1024).unwrap();
    exact("source/1",&c.try_get("n","source/1").unwrap().unwrap(),10,2);
    let mut body=payload(10,2);let old=0x0102030405060708u64.to_le_bytes();let new=0x8877665544332211u64.to_le_bytes();
    let at=body.windows(8).position(|b|b==old).unwrap();body[at..at+8].copy_from_slice(&new);
    let frame=chunks::frame(b"MBK1",&body);assert_eq!(frame.len(),f);
    let mut file=OpenOptions::new().read(true).write(true).create_new(true).open(path.join("equal.frame")).unwrap();file.write_all(&frame).unwrap();
    s.committed.write().unwrap().extend(entries(Arc::new(Mutex::new(file)),0,f,10,2));
    assert_eq!(c.try_get("n","source/1").unwrap().unwrap().bytes,new);
}
#[test]
fn interleaved_snapshot_frames_each_decode_once_under_their_scan_charge() {
    let (path,s,f)=fixture("interleaved");let frame=chunks::frame(b"MBK1",&payload(100,1));
    let mut file=OpenOptions::new().read(true).write(true).create_new(true).open(path.join("interleaved.frame")).unwrap();file.write_all(&frame).unwrap();
    let replacement=entries(Arc::new(Mutex::new(file)),0,f,100,1);
    s.committed.write().unwrap().extend(replacement.into_iter().filter(|(k,_)|k.1.starts_with("source/")||k.1=="row/b"));
    let (token,view)=s.snapshot_open("n").unwrap();reset();
    for (key,row) in &view.rows {exact(key,&row.load(&s,"n",key).unwrap(),
        if key.starts_with("source/")||key=="row/b" {100}else{10},if key.starts_with("source/")||key=="row/b" {1}else{2});}
    assert_eq!(counts(),[2*f,2*(f-16),2,18]);s.snapshot_release("n",&token).unwrap();drop(view);
    assert_eq!(scan_usage(&s),(0,0));
}
#[test]
fn malformed_unselected_peer_and_tail_refuse_without_partial_installation() {
    for tail in [false,true] {
        let (_,s,_)=fixture(if tail {"bad-tail"}else{"bad-peer"});let e=entry(&s,"id/1");let mut body=payload(10,2);
        if tail {body.push(99);} else {let key=b"deleted";let at=body.windows(key.len()).rposition(|p|p==key).unwrap();body[at+key.len()]=3;}
        let frame=chunks::frame(b"MBK1",&body);rewrite(&e,&frame);
        if tail { let replacement=entries(e.location.file.clone(),0,frame.len(),10,2);s.committed.write().unwrap().extend(replacement); }
        let c=s.read_cohort(128*1024).unwrap();assert!(c.try_get("n","id/1").is_err());assert!(s.storage_error());
        assert!(c.frame.lock().unwrap().is_none());assert_eq!(c.budget.used.load(Ordering::Acquire),512);
    }
}
#[test]
fn crc_valid_identity_length_ack_and_tombstone_mismatches_refuse() {
    for mode in 0..7 {
        let (_,s,_)=fixture(&format!("wrong-identity-{mode}"));let old=entry(&s,"id/1");
        let mut e=value_disk::Entry {cacheable:AtomicBool::new(true),class:old.class,version:old.version,ack:old.ack,
            tombstone:old.tombstone,len:old.len,location:old.location.clone()};
        match mode {0=>e.version+=1,1=>e.len+=1,2=>e.ack=1,3=>e.location.record_len+=1,
            4=>e.location.subrecord=Some(0),5=>e.location.subrecord=Some(65535),_=>e.tombstone=true}
        let c=s.read_cohort(128*1024).unwrap();assert!(c.read("n","id/1",&e).is_err());
        assert!(c.read("other","id/1",&old).is_err());assert!(s.storage_error());assert!(c.frame.lock().unwrap().is_none());
        assert_eq!(c.budget.used.load(Ordering::Acquire),512);
    }
}
#[test]
fn singleton_codec_and_snapshot_cache_share_payload_with_retained_charge() {
    let (path,mut s,_)=fixture("singleton");
    let m=Mutation {ns:"single".into(),key:"key".into(),op_id:"one".into(),deadline:0,
        ack:1,expected:None,bytes:vec![0,128,255,11,0,77],remove:false};
    let frame=chunks::frame(b"MBK1",&codec::mutation_bytes(&m,500,1_000_000));
    let mut file=OpenOptions::new().read(true).write(true).create_new(true).open(path.join("single.frame")).unwrap();file.write_all(&frame).unwrap();
    let e=Arc::new(value_disk::Entry::new(&m,500,value_disk::Location {class:Class::FOREVER,
        file:Arc::new(Mutex::new(file)),offset:0,record_len:frame.len() as u64,subrecord:None}));
    s.committed.write().unwrap().clear();s.committed.write().unwrap().insert((m.ns.clone(),m.key.clone()),e);
    let s=Arc::get_mut(&mut s).unwrap();s.config.cache_bytes=8192;
    s.cache=Arc::new(cache::Cache::new(&s.config,s.clock.clone()));s.trim_cache(&s.state.lock().unwrap());
    let value=s.get_handle_result("single","key").unwrap().unwrap();assert_eq!(value.bytes,m.bytes);
    let (token,view)=s.snapshot_open("single").unwrap();let reserved=scan_usage(s);
    let loaded=view.rows[0].1.load(s,"single","key").unwrap();assert!(Arc::ptr_eq(&value,&loaded.value));
    s.snapshot_release("single",&token).unwrap();drop(view);assert_eq!(scan_usage(s),reserved);
    drop(loaded);assert_eq!(scan_usage(s),(0,0));
    let cohort=s.read_cohort(64*1024).unwrap();let read=cohort.try_get("single","key").unwrap().unwrap();
    assert_eq!((read.version,read.ack,read.tombstone),(500,1,false));assert_eq!(read.bytes,m.bytes);
    drop(cohort);assert_eq!(s.read_cohort_bytes(),64*1024);drop(read);assert_eq!(s.read_cohort_bytes(),0);
}

#[test]
fn ordinary_mutation_respects_a_retained_read_cohort() {
    let root = PathBuf::from(std::env::var_os("MEMBUS_COHORT_FIXTURE_ROOT").unwrap());
    assert!(root.is_absolute() && root.is_dir());
    let path = root.join(format!("write-admission-{}-{}", std::process::id(), now_ms()));
    let store = Store::open(
        &path,
        Config {
            capacity: 2 * 1024 * 1024,
            cache_bytes: 0,
            signal_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap();
    let mutation = |key: &str| Mutation {
        ns: "ledger".into(),
        key: key.into(),
        op_id: key.into(),
        deadline: now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: vec![0x71; 4096],
        remove: false,
    };
    assert_eq!(store.mutate(mutation("seed")).code, 0);
    let budget = store.cache_limit(&store.state.lock().unwrap());
    let cohort = store.read_cohort(budget).unwrap();
    let retained = cohort.try_get("ledger", "seed").unwrap().unwrap();
    drop(cohort);
    assert_eq!(store.read_cohort_bytes(), budget);

    assert_eq!(store.mutate(mutation("new")).code, 7);
    assert!(store.get_handle_result("ledger", "new").unwrap().is_none());
    drop(retained);

    assert_eq!(store.read_cohort_bytes(), 0);
    assert_eq!(store.mutate(mutation("new")).code, 0);
    assert!(store.get_handle_result("ledger", "new").unwrap().is_some());
}
