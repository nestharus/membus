use std::time::Instant;
use super::*;
use crate::{chunks, CachePolicy, Mutation, now_ms};
use std::{fs::OpenOptions, io::{Seek, SeekFrom, Write}, path::PathBuf};

struct Fixture(PathBuf);
impl Fixture {
    fn new(label: &str) -> Self {
        let base=PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_|".test-output".into()));
        std::fs::create_dir_all(&base).unwrap();
        let path=std::fs::canonicalize(base).unwrap().join(format!("view-load-{label}-{}-{}",std::process::id(),now_ms()));
        std::fs::create_dir_all(&path).unwrap();Self(path)
    }
}
impl Drop for Fixture {fn drop(&mut self){std::fs::remove_dir_all(&self.0).unwrap();}}
fn config(never:bool)->Config {
    Config {capacity:64*1024*1024,cache_bytes:8*1024*1024,signal_bytes:0,
        tick_ms:3_600_000,retire_ms:3_600_000,snapshot_ttl_ms:30_000,cache_rules:vec![],
        cache_policy:CachePolicy {initial_ttl:0,active_ttl:60_000,never_cache:never,..CachePolicy::default()},
        ..Config::default()}
}
fn put(s:&Store,key:&str,byte:u8,id:&str)->u64 {
    let result=s.mutate(Mutation {ns:"ledger".into(),key:key.into(),op_id:id.into(),
        bytes:vec![byte;8192],deadline:now_ms()+30_000,ack:1,expected:None,remove:false});
    assert_eq!(result.code,0);result.version
}
fn wait_for(row:&SnapshotValue,count:usize)->bool {
    let until=Instant::now()+Duration::from_secs(5);
    while row.load_attempts.load(Ordering::SeqCst)<count && Instant::now()<until {std::thread::yield_now();}
    row.load_attempts.load(Ordering::SeqCst)>=count
}
fn concurrent(never:bool) {
    let f=Fixture::new(if never {"never"} else {"replaced"});
    let s=Arc::new(Store::open(&f.0,config(never)).unwrap());
    let version=put(&s,"head",7,"seed");let (token,view)=s.snapshot_open("ledger").unwrap();
    if !never {put(&s,"head",9,"replacement");}
    let entry=view.rows[0].1.entry.clone();let blocked=entry.location.file.lock().unwrap();
    let before=s.cache_stats().cold_reads;
    let workers:Vec<_>=(0..2).map(|_| {let (s,view)=(s.clone(),view.clone());
        std::thread::spawn(move||view.rows[0].1.load(&s,"ledger","head").unwrap())}).collect();
    // Both calls have passed the empty-slot fast path before releasing I/O.
    // On the old code, neither can later avoid its independently chosen load.
    let both_started=wait_for(&view.rows[0].1,2);
    drop(blocked);let values:Vec<_>=workers.into_iter().map(|w|w.join().unwrap()).collect();
    let cold=s.cache_stats().cold_reads-before;
    assert_eq!(values[0].version,version);assert_eq!(values[0].bytes,vec![7;8192]);
    assert!(Arc::ptr_eq(&values[0],&values[1]));assert_eq!(s.cache_stats().entries,0);
    s.snapshot_release("ledger",&token).unwrap();drop(view);drop(entry);drop(s);drop(f);
    assert!(both_started,"both readers must encounter the empty snapshot slot");
    assert_eq!(cold,1,"one snapshot revision must need only one successful cold load");
}
#[test]
fn snapshot_load_replaced_revision_is_loaded_once(){concurrent(false);}
#[test]
fn snapshot_load_never_cache_value_is_loaded_once(){concurrent(true);}

fn rewrite(f:&Fixture,entry:&Entry,bytes:&[u8]) {
    let path=std::fs::read_dir(f.0.join("chunks/forever")).unwrap().map(|e|e.unwrap().path())
        .find(|p|p.extension().is_some_and(|x|x=="log")).unwrap();
    let _guard=entry.location.file.lock().unwrap();
    let mut file=OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(entry.location.offset)).unwrap();file.write_all(bytes).unwrap();file.flush().unwrap();
}
#[test]
fn snapshot_load_errors_are_not_pinned_and_valid_retry_is_shared() {
    let f=Fixture::new("errors");let s=Store::open(&f.0,config(true)).unwrap();put(&s,"head",7,"seed");
    let (token,view)=s.snapshot_open("ledger").unwrap();let row=&view.rows[0].1;
    let original=chunks::frame(b"MBK1",&row.entry.location.payload().unwrap());
    let mut corrupt=original.clone();corrupt[16]^=1;rewrite(&f,&row.entry,&corrupt);
    let first=row.load(&s,"ledger","head");let second=row.load(&s,"ledger","head");
    let errors=(first.err(),second.err());let pinned=row.loaded.get().is_some();
    rewrite(&f,&row.entry,&original);
    let valid=row.load(&s,"ledger","head").unwrap();let again=row.load(&s,"ledger","head").unwrap();
    let cold=s.cache_stats().cold_reads;let entries=s.cache_stats().entries;let failed=s.storage_error();
    s.snapshot_release("ledger",&token).unwrap();drop(view);drop(s);drop(f);
    assert_eq!(errors,(Some(16),Some(16)));assert!(!pinned);assert!(failed);
    assert_eq!(valid.bytes,vec![7;8192]);assert!(Arc::ptr_eq(&valid,&again));assert_eq!((cold,entries),(3,0));
}
#[test]
fn snapshot_load_pinned_row_bypasses_another_rows_blocked_load() {
    let f=Fixture::new("hot");let s=Arc::new(Store::open(&f.0,config(true)).unwrap());
    put(&s,"a",7,"seed-a");put(&s,"b",8,"seed-b");
    let (token,view)=s.snapshot_open("ledger").unwrap();let warm=view.rows[0].1.load(&s,"ledger","a").unwrap();
    let entry=view.rows[1].1.entry.clone();let blocked=entry.location.file.lock().unwrap();
    let worker={let (s,view)=(s.clone(),view.clone());
        std::thread::spawn(move||view.rows[1].1.load(&s,"ledger","b").unwrap())};
    let started=wait_for(&view.rows[1].1,1);let (sent,received)=mpsc::channel();
    let reader={let (s,view)=(s.clone(),view.clone());std::thread::spawn(move|| {
        sent.send(view.rows[0].1.load(&s,"ledger","a").unwrap()).unwrap();})};
    let result=received.recv_timeout(Duration::from_secs(1));drop(blocked);
    let other=worker.join().unwrap();reader.join().unwrap();
    s.snapshot_release("ledger",&token).unwrap();drop(view);drop(entry);drop(s);drop(f);
    assert!(started);assert!(Arc::ptr_eq(&warm,&result.expect("pinned row waited for another row's I/O")));
    assert_eq!(other.bytes,vec![8;8192]);assert!(std::mem::size_of::<(String,SnapshotValue)>()<=128);
}
