//! Authored inert wire/Client controls. No Store, native endpoint or backing state.
use membus::{client::*, Class};
use std::{collections::VecDeque, io::{self, Read, Write}, sync::{Arc, Mutex},
          time::{Duration, Instant}};
fn lp(b: &mut Vec<u8>, v: &[u8]) { b.extend((v.len() as u32).to_le_bytes()); b.extend(v); }
fn wire(op: u8, id: u64, status: Option<u16>, p: &[u8]) -> Vec<u8> {
    let mut b = vec![1,0,0,0,op]; b.extend(id.to_le_bytes());
    if let Some(code) = status { b.extend(code.to_le_bytes()); } b.extend(p);
    let mut frame = (b.len() as u32).to_le_bytes().to_vec(); frame.extend(b); frame
}
fn hello(epoch: u64, retry: u64, max: u32) -> Vec<u8> {
    let mut b = epoch.to_le_bytes().to_vec(); b.extend(max.to_le_bytes());
    b.extend(4194304u32.to_le_bytes()); b.extend(128u16.to_le_bytes()); b.push(0);
    b.extend(retry.to_le_bytes()); b.extend(19u16.to_le_bytes());
    for &(code,name,transient) in &membus::CODES[..19] {
        b.extend(code.to_le_bytes()); b.push(transient as u8); lp(&mut b,name.as_bytes());
    }
    wire(129,1,Some(0),&b)
}
fn rows(entries: &[(&[u8],u8,u64)]) -> Vec<u8> {
    let mut b = (entries.len() as u16).to_le_bytes().to_vec();
    for &(prefix,kind,retention) in entries { lp(&mut b,prefix); b.push(kind); b.extend(retention.to_le_bytes()); } b
}
fn committed(version: u64, ack: u8) -> Vec<u8> {
    let mut b=version.to_le_bytes().to_vec(); b.push(ack); b
}
struct Inert { input: VecDeque<u8>, writes: Arc<Mutex<Vec<u8>>> }
impl Read for Inert {
    fn read(&mut self,b: &mut [u8])->io::Result<usize> {
        let n=b.len().min(self.input.len()).min(7);
        for v in &mut b[..n] { *v=self.input.pop_front().unwrap(); } Ok(n)
    }
}
impl Write for Inert {
    fn write(&mut self,b: &[u8])->io::Result<usize> { self.writes.lock().unwrap().extend(b); Ok(b.len()) }
    fn flush(&mut self)->io::Result<()> { Ok(()) }
}
impl Transport for Inert {
    fn deadline(&mut self,until: Instant)->io::Result<()> {
        if until <= Instant::now() { Err(io::ErrorKind::TimedOut.into()) } else { Ok(()) }
    }
}
fn d()->Deadline { Deadline { unix_ms:123456789,local:Instant::now()+Duration::from_secs(5) } }
fn connect(epoch:u64,retry:u64,max:u32,replies:Vec<Vec<u8>>)->(Client<Inert>,Arc<Mutex<Vec<u8>>>) {
    let writes=Arc::new(Mutex::new(Vec::new()));
    let input=std::iter::once(hello(epoch,retry,max)).chain(replies).flatten().collect();
    let c=Client::handshake(Inert {input,writes:writes.clone()},Options::new("authored-policy",max as usize),d().local).unwrap();
    writes.lock().unwrap().clear(); (c,writes)
}
fn request(class:Class)->PolicySetRequest {
    PolicySetRequest {operation_id:"original-policy".into(),namespace:"authored".into(),prefix:"pending/".into(),class}
}
fn prefix(id:&str,ns:&str,deadline:Deadline)->Vec<u8> {
    let mut b=deadline.unix_ms.to_le_bytes().to_vec();lp(&mut b,id.as_bytes());lp(&mut b,ns.as_bytes());b
}
#[test]
fn policy_get_exact_frame_namespace_epoch_and_no_implicit_class() {
    let reply=wire(153,2,Some(0),&rows(&[(b"",0,0),(b"completed/",2,u64::MAX),(b"pending/",1,0)]));
    let (mut c,w)=connect(41,86400000,65536,vec![reply]); let deadline=d();
    let table=c.policy_get("authored","read-original",deadline).unwrap();
    assert_eq!(table.namespace(),"authored"); assert_eq!(table.epoch(),41);
    assert_eq!(table.rows().len(),3); assert_eq!(table.exact("pending/"),Some(Class::LATEST));
    assert_eq!(table.exact("completed/"),Some(Class::expire(u64::MAX)));
    assert_eq!(table.exact("missing/"),None);
    assert_eq!(*w.lock().unwrap(),wire(25,2,None,&prefix("read-original","authored",deadline)));
}
#[test]
fn policy_get_rejects_incomplete_duplicate_invalid_and_extra_rows() {
    let mut extra=rows(&[]);extra.push(0);
    let mut overprefix=rows(&[]);overprefix[..2].copy_from_slice(&1u16.to_le_bytes());
    lp(&mut overprefix,&vec![b'x';1025]);overprefix.push(1);overprefix.extend(0u64.to_le_bytes());
    let mut bodies=vec![vec![],vec![1,0],extra,overprefix,
        rows(&[(b"p",1,0),(b"p",2,86400000)]),rows(&[(b"p",3,0)]),
        rows(&[(b"p",2,0)]),rows(&[(b"p",0,7)]),rows(&[(b"p",1,7)]),
        rows(&[(&[255],1,0)]),rows(&[(&vec![b'x';1016],1,0)])];
    let mut cut=rows(&[(b"p",2,u64::MAX)]);cut.pop();bodies.push(cut);
    for body in bodies {
        let (mut c,_)=connect(41,86400000,65536,vec![wire(153,2,Some(0),&body)]);
        assert!(matches!(c.policy_get("authored","read",d()),Err(Error::Protocol(_))));
        assert!(!c.is_usable());
    }
}
#[test]
fn policy_get_checks_wire_association_and_negotiated_reply_bound() {
    let oversized=(65537u32).to_le_bytes().to_vec();
    for frame in [wire(152,2,Some(0),&rows(&[])),wire(153,3,Some(0),&rows(&[])),oversized] {
        let (mut c,_)=connect(41,86400000,65536,vec![frame]);
        assert!(matches!(c.policy_get("authored","read",d()),Err(Error::Protocol(_))));
        assert!(!c.is_usable());
    }
}
#[test]
fn invalid_namespace_and_prefix_refuse_before_transport() {
    let (mut c,w)=connect(41,86400000,65536,vec![]);
    for ns in ["", "bad\0namespace", membus::POLICY_NS, "__membus_timers_v1"] {
        assert!(matches!(c.policy_get(ns,"read",d()),Err(Error::Invalid(_))));
    }
    let mut r=request(Class::LATEST);r.prefix="x".repeat(1016);
    assert!(matches!(c.prepare_policy_set(r),Err(Error::Invalid(_))));
    assert!(w.lock().unwrap().is_empty());
}
#[test]
fn policy_set_exact_original_class_frame_and_full_scalar_version() {
    for class in [Class::FOREVER,Class::LATEST,Class::expire(86400000),Class::expire(u64::MAX)] {
        let (mut c,w)=connect(41,86400000,65536,vec![wire(152,2,Some(0),&committed(u64::MAX,2))]);
        let r=request(class);let mut a=c.prepare_policy_set(r.clone()).unwrap();let deadline=d();
        assert_eq!(a.request(),&r);assert_eq!(a.epoch(),41);
        let result=c.policy_set(&mut a,deadline).unwrap();
        assert_eq!((result.epoch,result.version,result.achieved),(41,u64::MAX,Ack::Flush));
        let mut p=prefix(&r.operation_id,&r.namespace,deadline);lp(&mut p,r.prefix.as_bytes());p.push(class.kind);p.extend(class.retention_ms.to_le_bytes());
        assert_eq!(*w.lock().unwrap(),wire(24,2,None,&p));
    }
}
#[test]
fn policy_set_empty_prefix_and_canonical_class_admission() {
    let (mut c,w)=connect(41,10,65536,vec![wire(152,2,Some(0),&committed(1,2))]);
    for class in [Class{kind:0,retention_ms:1},Class{kind:1,retention_ms:1},Class{kind:3,retention_ms:10},Class::expire(0),Class::expire(9)] {
        assert!(matches!(c.prepare_policy_set(request(class)),Err(Error::Invalid(_))));
    }
    assert!(w.lock().unwrap().is_empty());
    let mut r=request(Class::LATEST);r.prefix.clear();let mut a=c.prepare_policy_set(r).unwrap();
    c.policy_set(&mut a,d()).unwrap();
}
#[test]
fn policy_frame_and_local_deadline_refuse_without_starting_attempt() {
    let (mut c,w)=connect(41,0,1024,vec![wire(152,2,Some(0),&committed(1,2))]);
    let mut too_big=request(Class::LATEST);too_big.operation_id="o".repeat(1024);
    assert!(matches!(c.prepare_policy_set(too_big),Err(Error::Invalid(_))));
    let mut a=c.prepare_policy_set(request(Class::LATEST)).unwrap();
    let expired=Deadline{unix_ms:1,local:Instant::now()};
    assert!(matches!(c.policy_set(&mut a,expired),Err(Error::Invalid(_))));
    assert!(w.lock().unwrap().is_empty());
    // Zero retry horizon still allows the initial send after a local preflight refusal.
    c.policy_set(&mut a,d()).unwrap();
    assert!(matches!(c.policy_set(&mut a,d()),Err(Error::RetryWindowExpired)));
}
#[test]
fn policy_set_unknown_truncated_wrong_ack_and_zero_version_are_uncertain() {
    let mut tail=committed(4,2);tail.push(0);
    for body in [vec![],vec![0],committed(0,2),committed(4,0),committed(4,1),committed(4,3),tail] {
        let (mut c,_)=connect(41,86400000,65536,vec![wire(152,2,Some(0),&body)]);
        let original=request(Class::LATEST);let mut a=c.prepare_policy_set(original.clone()).unwrap();
        assert!(matches!(c.policy_set(&mut a,d()),Err(Error::Uncertain{epoch:41,operation_id,..}) if operation_id=="original-policy"));
        assert_eq!(a.request(),&original);assert!(!c.is_usable());
    }
}
#[test]
fn policy_set_refusal_and_uncertainty_preserve_original_without_retry() {
    for code in [2,3,4,5,6,7,8,10,12,13,14,16,17,18,19,65535] {
        let (mut c,w)=connect(41,86400000,65536,vec![wire(152,2,Some(code),&[])]);
        let r=request(Class::LATEST);let mut a=c.prepare_policy_set(r.clone()).unwrap();
        let result=c.policy_set(&mut a,d());
        if matches!(code,16|17|65535) {assert!(matches!(result,Err(Error::Uncertain{..})));assert!(!c.is_usable());}
        else {assert!(matches!(result,Err(Error::Refused(v)) if v.code==code));assert!(c.is_usable());}
        assert_eq!(a.request(),&r);assert!(!w.lock().unwrap().is_empty());
    }
}
#[test]
fn policy_lost_reply_reconnect_uses_same_original_and_fixed_deadline() {
    let (mut first,w1)=connect(41,86400000,65536,vec![]);let deadline=d();
    let mut changed=request(Class::LATEST);let mut original=first.prepare_policy_set(changed.clone()).unwrap();
    changed.namespace="other".into();changed.prefix="other/".into();changed.class=Class::expire(86400000);
    assert!(matches!(first.policy_set(&mut original,deadline),Err(Error::Uncertain{..})));
    let (mut second,w2)=connect(41,86400000,65536,vec![wire(152,2,Some(0),&committed(88,2))]);
    assert_eq!(second.policy_set(&mut original,deadline).unwrap().version,88);
    assert_eq!(*w1.lock().unwrap(),*w2.lock().unwrap());
    assert_eq!(original.request(),&request(Class::LATEST));
}
#[test]
fn policy_epoch_change_refuses_old_original_before_send() {
    let (first,_)=connect(41,86400000,65536,vec![]);let mut a=first.prepare_policy_set(request(Class::LATEST)).unwrap();
    let (mut next,w)=connect(42,86400000,65536,vec![]);
    assert!(matches!(next.policy_set(&mut a,d()),Err(Error::EpochChanged{expected:41,actual:42})));
    assert_eq!(a.epoch(),41);assert!(w.lock().unwrap().is_empty());
}
#[test]
fn policy_set_confirmed_flush_then_readback_requires_exact_prefix_class() {
    let table=rows(&[(b"pending/",1,0),(b"completed/",2,86400000)]);
    let (mut c,_)=connect(41,86400000,65536,vec![wire(152,2,Some(0),&committed(6,2)),wire(153,3,Some(0),&table)]);
    let mut a=c.prepare_policy_set(request(Class::LATEST)).unwrap();
    let receipt=c.policy_set(&mut a,d()).unwrap();let readback=c.policy_get("authored","verify",d()).unwrap();
    assert_eq!(receipt.achieved,Ack::Flush);assert_eq!(receipt.epoch,readback.epoch());
    assert_eq!(readback.namespace(),a.request().namespace);
    assert_eq!(readback.exact(&a.request().prefix),Some(a.request().class));
    assert_ne!(readback.exact("wrong/"),Some(Class::LATEST));
    assert_ne!(readback.exact("completed/"),Some(Class::LATEST));
}
#[test]
fn policy_set_wrong_association_is_not_a_commit_receipt() {
    for reply in [wire(153,2,Some(0),&committed(1,2)),wire(152,3,Some(0),&committed(1,2))] {
        let (mut c,_)=connect(41,86400000,65536,vec![reply]);let mut a=c.prepare_policy_set(request(Class::LATEST)).unwrap();
        assert!(matches!(c.policy_set(&mut a,d()),Err(Error::Uncertain{..})));
    }
}
#[test]
fn ordinary_attempt_default_and_policy_attempt_share_typed_context() {
    let (c,_)=connect(41,86400000,65536,vec![]);
    let old:Attempt=c.prepare(MutationRequest{operation_id:"ordinary".into(),namespace:"authored".into(),key:"data".into(),value:vec![],ack:Ack::Flush,expected:Some(0)}).unwrap();
    let policy:PolicySetAttempt=c.prepare_policy_set(request(Class::LATEST)).unwrap();
    assert_eq!(old.request().operation_id,"ordinary");assert_eq!(old.epoch(),policy.epoch());
}
