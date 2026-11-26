use crate::*;
use crate::value_disk::Entry;
use std::{fs::OpenOptions, io::{Seek, SeekFrom, Write}, path::PathBuf, sync::Arc};

struct Fixture(PathBuf);
impl Fixture {
    fn new(label: &str) -> Self {
        let base=PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_|".test-output".into()));
        std::fs::create_dir_all(&base).unwrap();
        Self(std::fs::canonicalize(base).unwrap().join(format!("cold-identity-{label}-{}-{}",std::process::id(),now_ms())))
    }
}
impl Drop for Fixture {fn drop(&mut self) {std::fs::remove_dir_all(&self.0).unwrap();}}
fn config(initial_ttl:u64)->Config {
    Config {capacity:64*1024*1024,cache_bytes:8*1024*1024,signal_bytes:0,
        tick_ms:3_600_000,retire_ms:3_600_000,cache_rules:vec![],
        cache_policy:CachePolicy {initial_ttl,active_ttl:60_000,..CachePolicy::default()},..Config::default()}
}
fn put(s:&Store, ns:&str, key:&str, value:u8, id:&str)->u64 {
    let outcome=s.mutate(Mutation {ns:ns.into(),key:key.into(),op_id:id.into(),
        deadline:now_ms()+30_000,ack:1,expected:None,bytes:vec![value;8192],remove:false});
    assert_eq!(outcome.code,0);outcome.version
}
fn entry(s:&Store)->Arc<Entry> {
    s.committed.read().unwrap().get(&("ledger".into(),"head".into())).unwrap().clone()
}
fn rewrite(f:&Fixture, entry:&Entry, payload:&[u8]) {
    let bytes=crate::chunks::frame(b"MBK1",payload);
    assert_eq!(bytes.len() as u64,entry.location.record_len);
    write_frame(f,entry,&bytes);
}
fn write_frame(f:&Fixture, entry:&Entry, bytes:&[u8]) {
    let path=std::fs::read_dir(f.0.join("chunks/forever")).unwrap().map(|e|e.unwrap().path())
        .find(|p|p.extension().is_some_and(|x|x=="log")).unwrap();
    let _read_handle=entry.location.file.lock().unwrap();
    let mut file=OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(entry.location.offset)).unwrap();
    file.write_all(bytes).unwrap();file.flush().unwrap();
}
fn mismatched(original:&[u8], namespace:bool)->Vec<u8> {
    let (mut mutation,version,at)=crate::codec::parse_record(original).unwrap();
    if namespace {mutation.ns="ledges".into();} else {mutation.key="tail".into();}
    mutation.bytes.fill(9);
    // Keep frame length, version, acknowledgment and payload length unchanged,
    // with a valid CRC: only identity validation can reject this record.
    crate::codec::mutation_bytes(&mutation,version,at)
}
fn get_code(s:&Store)->u16 {
    let mut body=(now_ms()+30_000).to_le_bytes().to_vec();
    lp(&mut body,b"identity-read");lp(&mut body,b"ledger");lp(&mut body,b"head");
    let response=s.dispatch(&frame(GET,1,&body)[4..]);
    u16::from_le_bytes(response[17..19].try_into().unwrap())
}
fn refuses_mismatch(namespace:bool) {
    let f=Fixture::new(if namespace {"namespace"} else {"key"});
    let s=Store::open(&f.0,config(0)).unwrap();
    let version=put(&s,"ledger","head",7,"seed");
    let entry=entry(&s);let original=entry.location.payload().unwrap();
    rewrite(&f,&entry,&mismatched(&original,namespace));
    let before=s.cache_stats();
    let first=s.try_get("ledger","head");
    let second=s.try_get("ledger","head");
    let code=get_code(&s);let failed=s.storage_error();let bad=s.cache_stats();
    let first_error=first.is_err();let second_error=second.is_err();
    let observed=first.as_ref().ok().and_then(|v|v.as_ref()).map(|v|v.bytes[0]);
    // Repair and settle the fixture before asserting, including on old code
    // that accepted and cached the mismatched payload.
    rewrite(&f,&entry,&original);s.cache.remove(version,false);
    let repaired=s.try_get("ledger","head").unwrap().unwrap();
    let after_repair=s.cache_stats();
    let repeated=s.try_get("ledger","head").unwrap().unwrap();let after=s.cache_stats();
    assert_eq!(repaired.bytes,vec![7;8192]);assert_eq!(repeated.bytes,repaired.bytes);
    assert_eq!(after.cold_reads,after_repair.cold_reads);
    assert_eq!(after.hits,after_repair.hits+1);
    drop(entry);drop(s);drop(f);
    assert!(first_error && second_error && code==16 && failed && bad.entries==0
        && bad.cold_reads-before.cold_reads==3 && bad.coalesced_reads==before.coalesced_reads,
        "mismatched record was served or cached: first_error={first_error} second_error={second_error} code={code} storage_error={failed} entries={} cold={} observed={observed:?}",bad.entries,bad.cold_reads-before.cold_reads);
}
#[test]
fn cold_identity_namespace_mismatch_refuses_before_cache_admission() {refuses_mismatch(true);}
#[test]
fn cold_identity_key_mismatch_refuses_before_cache_admission() {refuses_mismatch(false);}

#[test]
fn cold_identity_cached_value_stays_immutable_until_eviction() {
    let f=Fixture::new("warm");let s=Store::open(&f.0,config(60_000)).unwrap();
    let version=put(&s,"ledger","head",7,"seed");let entry=entry(&s);
    let original=entry.location.payload().unwrap();let before=s.cache_stats();
    rewrite(&f,&entry,&mismatched(&original,false));
    let warm=s.try_get("ledger","head").unwrap().unwrap();let warm_stats=s.cache_stats();
    s.cache.remove(version,false);let code=get_code(&s);
    rewrite(&f,&entry,&original);s.cache.remove(version,false);
    assert_eq!(s.try_get("ledger","head").unwrap().unwrap().bytes,vec![7;8192]);
    drop(entry);drop(s);drop(f);
    assert_eq!(warm.bytes,vec![7;8192]);assert_eq!(warm_stats.cold_reads,before.cold_reads);
    assert_eq!(code,16,"cold validation must resume after the cached revision is evicted");
}

#[test]
fn cold_identity_exact_unicode_names_survive_replay_and_warm_reads() {
    let f=Fixture::new("unicode");
    let names=[("é","head"),("e\u{301}","head"),("ledger","é"),("ledger","e\u{301}")];
    {let s=Store::open(&f.0,config(0)).unwrap();
        for (i,(ns,key)) in names.iter().enumerate() {put(&s,ns,key,i as u8,&format!("seed{i}"));}}
    let s=Store::open(&f.0,config(0)).unwrap();
    for _ in 0..2 {for (i,(ns,key)) in names.iter().enumerate() {
        assert_eq!(s.try_get(ns,key).unwrap().unwrap().bytes,vec![i as u8;8192]);}}
    let stats=s.cache_stats();assert_eq!(stats.cold_reads,4);assert_eq!(stats.hits,4);
    assert!(!s.storage_error());drop(s);drop(f);
}

#[test]
fn cold_identity_implicit_value_scan_refuses_and_releases_reservation() {
    let f=Fixture::new("scan");let s=Store::open(&f.0,config(0)).unwrap();
    let version=put(&s,"ledger","head",7,"seed");let entry=entry(&s);
    let original=entry.location.payload().unwrap();rewrite(&f,&entry,&mismatched(&original,true));
    let mut suffix=Vec::new();lp(&mut suffix,&[]);lp(&mut suffix,&[]);
    suffix.extend(1u16.to_le_bytes());suffix.extend((MAX_FRAME as u32).to_le_bytes());lp(&mut suffix,b"head");
    let page=s.scan_page("ledger",&mut Cursor::new(&suffix),SCAN_PREFIX,MAX_FRAME);
    let refused=matches!(&page,Err(16));
    if let Ok(bytes)=page {let token=Cursor::new(&bytes).bytes(16).unwrap().to_vec();
        s.snapshot_release("ledger",&token).unwrap();}
    let count=s.scan_usage.count.load(std::sync::atomic::Ordering::Acquire);
    let charged=s.scan_usage.bytes.load(std::sync::atomic::Ordering::Acquire);
    let cached=s.cache_stats().entries;
    rewrite(&f,&entry,&original);s.cache.remove(version,false);
    drop(entry);drop(s);drop(f);
    assert!(refused,"an implicit value scan must refuse a mismatched stored identity");
    assert_eq!((count,charged,cached),(0,0,0));
}

fn shorten_frame(f:&Fixture, entry:&Entry, original:&[u8]) {
    let (mut mutation,version,at)=crate::codec::parse_record(original).unwrap();
    // Preserve every field checked by Entry::read, including payload length.
    // A shorter operation ID leaves a whole, CRC-valid frame followed by stale
    // bytes inside the committed index's original record boundary.
    assert_eq!(mutation.op_id,"seed");mutation.op_id="s".into();
    mutation.bytes.fill(9);
    let payload=crate::codec::mutation_bytes(&mutation,version,at);
    let bytes=crate::chunks::frame(b"MBK1",&payload);
    assert!((bytes.len() as u64)<entry.location.record_len);
    write_frame(f,entry,&bytes);
}

#[test]
fn cold_boundary_shorter_valid_frame_refuses_before_cache_admission() {
    let f=Fixture::new("short-frame");let s=Store::open(&f.0,config(0)).unwrap();
    let version=put(&s,"ledger","head",7,"seed");let entry=entry(&s);
    let original=entry.location.payload().unwrap();shorten_frame(&f,&entry,&original);
    let before=s.cache_stats();
    let first=s.try_get("ledger","head");let second=s.try_get("ledger","head");
    let code=get_code(&s);let failed=s.storage_error();let bad=s.cache_stats();
    let first_error=first.is_err();let second_error=second.is_err();
    let observed=first.as_ref().ok().and_then(|v|v.as_ref()).map(|v|v.bytes[0]);
    rewrite(&f,&entry,&original);s.cache.remove(version,false);
    let repaired=s.try_get("ledger","head").unwrap().unwrap();let repair_stats=s.cache_stats();
    let repeated=s.try_get("ledger","head").unwrap().unwrap();let after=s.cache_stats();
    assert_eq!(repaired.bytes,vec![7;8192]);assert_eq!(repeated.bytes,repaired.bytes);
    assert_eq!(after.cold_reads,repair_stats.cold_reads);assert_eq!(after.hits,repair_stats.hits+1);
    drop(entry);drop(s);drop(f);
    assert!(first_error && second_error && code==16 && failed && bad.entries==0
        && bad.cold_reads-before.cold_reads==3 && bad.coalesced_reads==before.coalesced_reads,
        "shorter frame was served or cached: first_error={first_error} second_error={second_error} code={code} storage_error={failed} entries={} cold={} observed={observed:?}",bad.entries,bad.cold_reads-before.cold_reads);
}

#[test]
fn cold_boundary_shorter_frame_refuses_implicit_value_scan_and_releases_charge() {
    let f=Fixture::new("short-frame-scan");let s=Store::open(&f.0,config(0)).unwrap();
    let version=put(&s,"ledger","head",7,"seed");let entry=entry(&s);
    let original=entry.location.payload().unwrap();shorten_frame(&f,&entry,&original);
    let mut suffix=Vec::new();lp(&mut suffix,&[]);lp(&mut suffix,&[]);
    suffix.extend(1u16.to_le_bytes());suffix.extend((MAX_FRAME as u32).to_le_bytes());lp(&mut suffix,b"head");
    let page=s.scan_page("ledger",&mut Cursor::new(&suffix),SCAN_PREFIX,MAX_FRAME);
    let refused=matches!(&page,Err(16));
    if let Ok(bytes)=page {let token=Cursor::new(&bytes).bytes(16).unwrap().to_vec();
        s.snapshot_release("ledger",&token).unwrap();}
    let count=s.scan_usage.count.load(std::sync::atomic::Ordering::Acquire);
    let charged=s.scan_usage.bytes.load(std::sync::atomic::Ordering::Acquire);
    let cached=s.cache_stats().entries;
    rewrite(&f,&entry,&original);s.cache.remove(version,false);drop(entry);drop(s);drop(f);
    assert!(refused,"an implicit value scan must refuse a frame shorter than its indexed boundary");
    assert_eq!((count,charged,cached),(0,0,0));
}

#[test]
fn cold_boundary_exact_frame_succeeds_with_an_adjacent_record() {
    let f=Fixture::new("adjacent");let s=Store::open(&f.0,config(0)).unwrap();
    put(&s,"ledger","head",7,"seed");let head=entry(&s);
    let original=head.location.payload().unwrap();
    let (mut mutation,version,at)=crate::codec::parse_record(&original).unwrap();
    mutation.key="tail".into();mutation.op_id="next".into();mutation.bytes.fill(8);
    let next=crate::chunks::frame(b"MBK1",&crate::codec::mutation_bytes(&mutation,version+1,at));
    let path=std::fs::read_dir(f.0.join("chunks/forever")).unwrap().map(|e|e.unwrap().path())
        .find(|p|p.extension().is_some_and(|x|x=="log")).unwrap();
    {let _read_handle=head.location.file.lock().unwrap();
        let mut file=OpenOptions::new().append(true).open(path).unwrap();
        assert_eq!(file.metadata().unwrap().len(),head.location.offset+head.location.record_len);
        file.write_all(&next).unwrap();file.flush().unwrap();}
    for _ in 0..2 {
        assert_eq!(s.try_get("ledger","head").unwrap().unwrap().bytes,vec![7;8192]);
    }
    let stats=s.cache_stats();assert_eq!((stats.cold_reads,stats.hits,stats.entries),(1,1,1));
    assert!(!s.storage_error());drop(head);drop(s);drop(f);
}
