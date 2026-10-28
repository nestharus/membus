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
    let client=Client::handshake(peer,Options::new("récipiendaire",4096),until()).unwrap();
    assert_eq!(client.negotiated().epoch,44);
    assert!(!client.negotiated().subscriptions);
    assert_eq!(*sent.lock().unwrap(),request("récipiendaire"));
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
