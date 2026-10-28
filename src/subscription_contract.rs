//! Discovery of the stronger process-owned subscription contract.
//!
//! This version advertises a missing shared reservation capability. It cannot
//! select lifetime, buffering, latency or receiver-acceptance guarantees.

/// Exact opt-in HELLO identity feature, separate from legacy change/1.
pub const HELLO_FEATURE: &str = ";subscription-contract=1";

/// A required stronger contract cannot be used. No legacy fallback is implied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unavailable {
    /// The peer did not return the opt-in discovery extension.
    PeerDidNotNegotiate,
    /// The producer has no shared subscription resource reservation capability.
    SharedResourceReservationUnavailable,
}

/// An unknown, truncated, trailing or positive discovery must never grant authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidDiscovery;

/// MSC1, discovery version 1, unavailable, missing shared reservation (reason 1).
/// This is a representation, not a capacity value or an available feature.
pub(crate) const DISCOVERY: &[u8; 9] = b"MSC1\x01\x00\x00\x01\x00";

/// Decode the complete opt-in suffix. Empty means an unnegotiated older peer;
/// malformed or unknown bytes are a protocol error, not an unsupported agreement.
pub fn decode_discovery(bytes: &[u8]) -> Result<Unavailable, InvalidDiscovery> {
    if bytes.is_empty() { return Ok(Unavailable::PeerDidNotNegotiate); }
    if bytes == DISCOVERY { return Ok(Unavailable::SharedResourceReservationUnavailable); }
    Err(InvalidDiscovery)
}

// Affirmatively selected, volatile coalesced hints with finite authority.
// Binding is a receiver-checked capability, never proof of process liveness.
pub const FINITE_HELLO_FEATURE: &str = ";subscription-contract=2";
pub const FINITE_DISCOVERY: &[u8; 8] = b"MSH2\x01\x00\x01\x00";
pub const FINITE_CREATE: u8 = 28;
pub const FINITE_RENEW: u8 = 29;
pub const FINITE_CANCEL: u8 = 30;
/// Exact supported semantics: volatile coalesced hints, buffered, no replay,
/// no every-event/receipt/deadline guarantee, per-registration write counts only.
pub const COALESCED_HINTS: u16 = 1;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HintOptions {
    pub profile: u16,
    pub lifetime_ms: u64,
    pub write_ms: u32,
    /// Nonzero caller-owned fresh incarnation; no OS process mapping is inferred.
    pub incarnation: [u8; 16],
    /// Nonzero fresh endpoint generation, checked by the actual receiver.
    pub recipient: [u8; 16],
}
impl HintOptions {
    pub fn validate(&self) -> Result<(), u16> {
        if self.profile != COALESCED_HINTS { return Err(2); }
        if self.lifetime_ms == 0 || self.write_ms == 0 || self.incarnation == [0;16] || self.recipient == [0;16] { return Err(3); }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HintBinding { pub epoch: u64, pub id: u64, pub incarnation: [u8;16], pub recipient: [u8;16] }
impl HintBinding {
    pub fn bytes(self) -> [u8;48] {
        let mut b=[0;48];b[..8].copy_from_slice(&self.epoch.to_le_bytes());b[8..16].copy_from_slice(&self.id.to_le_bytes());
        b[16..32].copy_from_slice(&self.incarnation);b[32..].copy_from_slice(&self.recipient);b
    }
    pub fn decode(b:&[u8])->Result<Self,u16> {
        if b.len()!=48 {return Err(3);}
        let result=Self {epoch:u64::from_le_bytes(b[..8].try_into().unwrap()),id:u64::from_le_bytes(b[8..16].try_into().unwrap()),incarnation:b[16..32].try_into().unwrap(),recipient:b[32..].try_into().unwrap()};
        if result.id==0 || result.incarnation==[0;16] || result.recipient==[0;16] {return Err(3);} Ok(result)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HintReceipt {pub binding:HintBinding,pub authority_until_ms:u64,pub generation:u64}
impl HintReceipt {
    pub fn bytes(self)->[u8;64] {let mut b=[0;64];b[..48].copy_from_slice(&self.binding.bytes());b[48..56].copy_from_slice(&self.authority_until_ms.to_le_bytes());b[56..].copy_from_slice(&self.generation.to_le_bytes());b}
    pub fn decode(b:&[u8])->Result<Self,u16> {
        if b.len()!=64 {return Err(3);}let result=Self{binding:HintBinding::decode(&b[..48])?,authority_until_ms:u64::from_le_bytes(b[48..56].try_into().unwrap()),generation:u64::from_le_bytes(b[56..].try_into().unwrap())};
        if result.generation==0 {return Err(3);}Ok(result)
    }
}
/// Decode a complete length-framed hint and reject every other incarnation.
/// Successful decoding is a local check, not an acknowledgement to the producer.
pub fn decode_hint(frame:&[u8], expected:HintBinding)->Result<u64,u16> {
    if frame.len()!=64 || frame[..4]!=60u32.to_le_bytes() || &frame[4..8]!=b"MBH2" || HintBinding::decode(&frame[8..56])?!=expected {return Err(3);}
    Ok(u64::from_le_bytes(frame[56..].try_into().unwrap()))
}
