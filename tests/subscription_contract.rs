//! Finite authored transport controls; no resident or native endpoint.
use membus::{client::{AdditionalFeatures, Client, Error, Options, Transport}, lp};
use std::{io::{self, Read, Write}, sync::{Arc, Mutex}, time::{Duration, Instant}};
use membus::subscription_contract::Unavailable;

struct Peer { input: io::Cursor<Vec<u8>>, sent: Arc<Mutex<Vec<u8>>> }
impl Read for Peer { fn read(&mut self, b: &mut [u8]) -> io::Result<usize> { self.input.read(b) } }
impl Write for Peer {
    fn write(&mut self,b:&[u8])->io::Result<usize> { self.sent.lock().unwrap().extend_from_slice(b); Ok(b.len()) }
    fn flush(&mut self)->io::Result<()> { Ok(()) }
}
impl Transport for Peer { fn deadline(&mut self,_:Instant)->io::Result<()> { Ok(()) } }
fn until()->Instant { Instant::now()+Duration::from_secs(20) }
// Authored literal wire refusal vocabulary, independent of the production CODES.
fn hello(suffix:&[u8], legacy:bool, timers:bool)->Vec<u8> {
    let names=["incompatible_version","unsupported","malformed","too_large","deadline","busy","capacity","quota","index_cap","read_only","warming","conflict","retry_window_expired","op_id_mismatch","mem_not_enabled","io_failed_uncertain","pending_timeout_uncertain","dedup_full","pending_conflict","cursor_expired","invalid_cursor","snapshot_full","resume_gap","subscription_missing","timer_missing"];
    let count=if timers {25} else if legacy {24} else {19};
    let mut p=44u64.to_le_bytes().to_vec(); p.extend(4096u32.to_le_bytes()); p.extend(100u32.to_le_bytes()); p.extend(3u16.to_le_bytes()); p.push(0); p.extend(500u64.to_le_bytes()); p.extend((count as u16).to_le_bytes());
    for (i,name) in names[..count].iter().enumerate() {
        let code=(i+1) as u16; p.extend(code.to_le_bytes()); p.push([5,6,10,11,16,17,18,19,22].contains(&code) as u8); lp(&mut p,name.as_bytes());
    }
    if legacy {
        p.extend(1u16.to_le_bytes()); p.extend(4096u32.to_le_bytes());
        p.extend(5u64.to_le_bytes());p.extend(1000u64.to_le_bytes());
        p.extend(2u16.to_le_bytes());p.extend(3u16.to_le_bytes());
        p.extend(400u64.to_le_bytes());p.extend(600u64.to_le_bytes());p.extend(10u32.to_le_bytes());
    }
    if timers { // Literal timer fields are supplied in the dedicated ordinary controls elsewhere.
        panic!("timer payload not selected in this finite fixture");
    }
    p.extend(suffix);
    membus::reply(1,1,0,&p)
}
fn peer(bytes:Vec<u8>)->(Peer,Arc<Mutex<Vec<u8>>>) {
    let sent=Arc::new(Mutex::new(Vec::new())); (Peer{input:io::Cursor::new(bytes),sent:sent.clone()},sent)
}
fn required(bytes:Vec<u8>, options:Options)->(Result<Client<Peer>,Error>,Vec<u8>) {
    let (peer,sent)=peer(bytes);
    let result=Client::handshake_requiring_owned_subscriptions(peer,options,AdditionalFeatures::default(),until());
    let bytes=sent.lock().unwrap().clone(); (result,bytes)
}
fn request(identity:&str)->Vec<u8> {
    let mut p=4096u32.to_le_bytes().to_vec();p.push(0);lp(&mut p,identity.as_bytes());membus::frame(1,1,&p)
}
#[test]
fn explicit_unavailable_is_not_legacy_agreement() {
    let (result,sent)=required(hello(b"MSC1\x01\x00\x00\x01\x00",false,false),Options::new("receiver",4096));
    assert!(matches!(result,Err(Error::SubscriptionContractUnavailable(Unavailable::SharedResourceReservationUnavailable))));
    assert_eq!(sent,request("receiver;subscription-contract=1"));
}
#[test]
fn ignored_feature_and_legacy_peer_are_typed_unnegotiated() {
    let (result,sent)=required(hello(&[],false,false),Options::new("receiver",4096));
    assert!(matches!(result,Err(Error::SubscriptionContractUnavailable(Unavailable::PeerDidNotNegotiate))));
    assert_eq!(sent,request("receiver;subscription-contract=1"));
}
#[test]
fn malformed_unknown_positive_and_trailing_never_agree() {
    let good=b"MSC1\x01\x00\x00\x01\x00";
    let mut variants:Vec<Vec<u8>>=(1..good.len()).map(|n|good[..n].to_vec()).collect();
    for (index,value) in [(0,b'X'),(4,2),(6,1),(7,2),(8,1)] { let mut b=good.to_vec();b[index]=value;variants.push(b); }
    let mut tail=good.to_vec();tail.push(0);variants.push(tail);
    for suffix in variants {
        let (result,sent)=required(hello(&suffix,false,false),Options::new("receiver",4096));
        assert!(matches!(result,Err(Error::Protocol(_))),"suffix {suffix:?}");
        assert_eq!(sent,request("receiver;subscription-contract=1"));
    }
}
#[test]
fn ordinary_hello_has_no_new_feature_or_suffix() {
    let (peer,sent)=peer(hello(&[],false,false));
    let client=Client::handshake(peer,Options::new("rÃ©cipiendaire",4096),until()).unwrap();
    assert_eq!(client.negotiated().epoch,44);
    assert!(!client.negotiated().subscriptions);
    assert_eq!(*sent.lock().unwrap(),request("rÃ©cipiendaire"));
}
#[test]
fn unsolicited_suffix_is_protocol_error_for_ordinary_client() {
    let (peer,sent)=peer(hello(b"MSC1\x01\x00\x00\x01\x00",false,false));
    assert!(matches!(Client::handshake(peer,Options::new("receiver",4096),until()),Err(Error::Protocol(_))));
    assert_eq!(*sent.lock().unwrap(),request("receiver"));
}
#[test]
fn required_contract_does_not_accept_legacy_subscription_negotiation() {
    let mut options=Options::new("receiver",4096); options.subscriptions=true;
    let (result,sent)=required(hello(&[],true,false),options);
    assert!(matches!(result,Err(Error::SubscriptionContractUnavailable(Unavailable::PeerDidNotNegotiate))));
    assert_eq!(sent,request("receiver;subscribe=1;subscription-contract=1"));
}
#[test]
fn explicit_unavailable_composes_after_legacy_limits() {
    let mut options=Options::new("receiver",4096); options.subscriptions=true;
    let (result,sent)=required(hello(b"MSC1\x01\x00\x00\x01\x00",true,false),options);
    assert!(matches!(result,Err(Error::SubscriptionContractUnavailable(Unavailable::SharedResourceReservationUnavailable))));
    assert_eq!(sent,request("receiver;subscribe=1;subscription-contract=1"));
}
#[test]
fn required_feature_length_and_delimiter_refuse_before_io() {
    for identity in ["x".repeat(1005),"invalid;subscription-contract=1".into()] {
        let (result,sent)=required(Vec::new(),Options::new(identity,4096));
        assert!(matches!(result,Err(Error::Invalid(_)))); assert!(sent.is_empty());
    }
}
#[test]
fn lost_hello_reply_is_single_send_without_creation_or_fallback() {
    let (result,sent)=required(Vec::new(),Options::new("receiver",4096));
    assert!(matches!(result,Err(Error::Transport(_))));
    assert_eq!(sent,request("receiver;subscription-contract=1"));
}

fn finite_peer(extra:&[u8],suffix:&[u8])->(Peer,Arc<Mutex<Vec<u8>>>) {
    let mut bytes=hello(suffix,true,false);bytes.extend_from_slice(extra);peer(bytes)
}
fn finite_options()->membus::subscription_contract::HintOptions {
    membus::subscription_contract::HintOptions{profile:1,lifetime_ms:40,write_ms:10,incarnation:[7;16],recipient:[9;16]}
}
fn finite_receipt()->membus::subscription_contract::HintReceipt {
    use membus::subscription_contract::*;
    HintReceipt{binding:HintBinding{epoch:44,id:88,incarnation:[7;16],recipient:[9;16]},authority_until_ms:140,generation:1}
}
fn deadline()->membus::client::Deadline {membus::client::Deadline{unix_ms:999999,local:until()}}
#[test]
fn finite_positive_requires_exact_affirmative_complete_profile() {
    for suffix in [b"".as_slice(),b"MSH2\x01\x00\x01",b"MSH2\x02\x00\x01\x00",b"MSH2\x01\x00\x02\x00",b"MSH2\x01\x00\x01\x00\x00",b"MSC1\x01\x00\x00\x01\x00"] {
        let (peer,sent)=finite_peer(&[],suffix);
        assert!(Client::handshake_finite_hints(peer,Options::new("r",4096),AdditionalFeatures::default(),until()).is_err());
        assert_eq!(*sent.lock().unwrap(),request("r;subscribe=1;subscription-contract=2"));
    }
    let (peer,sent)=finite_peer(&[],b"MSH2\x01\x00\x01\x00");
    let c=Client::handshake_finite_hints(peer,Options::new("r",4096),AdditionalFeatures::default(),until()).unwrap();
    assert!(c.prepare_finite_hints("ns","p",r"\\.\pipe\authored",finite_options()).is_ok());
    assert_eq!(*sent.lock().unwrap(),request("r;subscribe=1;subscription-contract=2"));
}
#[test]
fn finite_creation_wire_and_known_renew_cancel_are_exact() {
    use membus::client::HintCreationState;
    let r=finite_receipt();let mut renewed=r;renewed.authority_until_ms=155;renewed.generation=2;
    let mut replies=membus::reply(28,2,0,&r.bytes());replies.extend(membus::reply(29,3,0,&renewed.bytes()));replies.extend(membus::reply(30,4,0,&[]));
    let (peer,sent)=finite_peer(&replies,b"MSH2\x01\x00\x01\x00");let mut c=Client::handshake_finite_hints(peer,Options::new("r",4096),AdditionalFeatures::default(),until()).unwrap();
    let mut attempt=c.prepare_finite_hints("ns","p",r"\\.\pipe\authored",finite_options()).unwrap();
    assert_eq!(attempt.state(),HintCreationState::Unsent);assert_eq!(c.create_finite_hints(&mut attempt,"create",deadline()).unwrap(),r);
    let mut p=999999u64.to_le_bytes().to_vec();lp(&mut p,b"create");lp(&mut p,b"ns");p.extend(1u16.to_le_bytes());p.extend(40u64.to_le_bytes());p.extend(10u32.to_le_bytes());lp(&mut p,&[7;16]);lp(&mut p,&[9;16]);lp(&mut p,b"p");lp(&mut p,br"\\.\pipe\authored");
    let mut expected=request("r;subscribe=1;subscription-contract=2");expected.extend(membus::frame(28,2,&p));assert_eq!(*sent.lock().unwrap(),expected);
    assert_eq!(c.renew_finite_hints(&mut attempt,"renew",deadline()).unwrap(),renewed);
    c.cancel_finite_hints(&mut attempt,"cancel",deadline()).unwrap();assert_eq!(attempt.state(),HintCreationState::Cancelled);
}
#[test]
fn finite_lost_or_malformed_creation_keeps_intent_and_forbids_retry() {
    use membus::client::HintCreationState;
    let r=finite_receipt();let mut wrong=r;wrong.binding.recipient=[8;16];
    for reply in [Vec::new(),membus::reply(28,2,0,b"bad"),membus::reply(28,2,0,&wrong.bytes())] {
        let (peer,sent)=finite_peer(&reply,b"MSH2\x01\x00\x01\x00");let mut c=Client::handshake_finite_hints(peer,Options::new("r",4096),AdditionalFeatures::default(),until()).unwrap();
        let mut attempt=c.prepare_finite_hints("ns","p",r"\\.\pipe\authored",finite_options()).unwrap();
        assert!(c.create_finite_hints(&mut attempt,"create",deadline()).is_err());assert_eq!(attempt.state(),HintCreationState::Unknown{last_receipt:None});
        let before=sent.lock().unwrap().clone();assert!(c.create_finite_hints(&mut attempt,"replacement",deadline()).is_err());assert_eq!(*sent.lock().unwrap(),before);
        assert_eq!(attempt.endpoint(),r"\\.\pipe\authored");assert_eq!(attempt.options(),finite_options());
    }
}
#[test]
fn finite_lost_renew_and_malformed_cancel_retain_original_known_receipt() {
    use membus::client::HintCreationState;
    let r=finite_receipt();
    for cancel in [false,true] {
        let mut replies=membus::reply(28,2,0,&r.bytes());if cancel {replies.extend(membus::reply(30,3,0,b"not-empty"));}
        let (peer,sent)=finite_peer(&replies,b"MSH2\x01\x00\x01\x00");let mut c=Client::handshake_finite_hints(peer,Options::new("r",4096),AdditionalFeatures::default(),until()).unwrap();
        let mut attempt=c.prepare_finite_hints("ns","p",r"\\.\pipe\authored",finite_options()).unwrap();c.create_finite_hints(&mut attempt,"create",deadline()).unwrap();
        let failed=if cancel {c.cancel_finite_hints(&mut attempt,"cancel",deadline()).map(|_|r)}else{c.renew_finite_hints(&mut attempt,"renew",deadline())};
        assert!(failed.is_err());assert_eq!(attempt.state(),HintCreationState::Unknown{last_receipt:Some(r)});
        let before=sent.lock().unwrap().clone();assert!(c.create_finite_hints(&mut attempt,"replacement",deadline()).is_err());assert_eq!(*sent.lock().unwrap(),before);
    }
}
#[test]
fn finite_unsupported_semantics_and_preflight_cannot_create() {
    use membus::client::HintCreationState;
    let (peer,sent)=finite_peer(&[],b"MSH2\x01\x00\x01\x00");let mut c=Client::handshake_finite_hints(peer,Options::new("r",4096),AdditionalFeatures::default(),until()).unwrap();
    let before=sent.lock().unwrap().clone();
    for profile in [0,2,65535] {let mut o=finite_options();o.profile=profile;assert!(c.prepare_finite_hints("ns","p",r"\\.\pipe\authored",o).is_err());}
    let mut attempt=c.prepare_finite_hints("ns","p",r"\\.\pipe\authored",finite_options()).unwrap();
    assert!(c.create_finite_hints(&mut attempt,"create",membus::client::Deadline{unix_ms:0,local:Instant::now()-Duration::from_millis(1)}).is_err());
    assert_eq!(attempt.state(),HintCreationState::Unsent);assert_eq!(*sent.lock().unwrap(),before);
}
#[test]
fn finite_explicit_preadmission_capacity_refusal_retains_unsent_intent() {
    use membus::client::HintCreationState;
    let (peer,_)=finite_peer(&membus::reply(28,2,7,&[]),b"MSH2\x01\x00\x01\x00");let mut c=Client::handshake_finite_hints(peer,Options::new("r",4096),AdditionalFeatures::default(),until()).unwrap();
    let mut attempt=c.prepare_finite_hints("ns","p",r"\\.\pipe\authored",finite_options()).unwrap();
    assert!(matches!(c.create_finite_hints(&mut attempt,"create",deadline()),Err(Error::Refused(r)) if r.code==7));assert_eq!(attempt.state(),HintCreationState::Unsent);
}
