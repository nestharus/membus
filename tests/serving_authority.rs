//! Participating atomic publisher boundary tests.
use membus::{
    atomic::{AtomicAck, AtomicRequest, FinalChange},
    client::*,
    serving_authority::*,
    Config, Store,
};
use std::{
    io::{self, Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct TempDir(PathBuf);
impl TempDir {
    fn new(name: &str) -> Self {
        let base = std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let p = base.join(format!(
            "serving-authority-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        if std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").is_none() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

fn config() -> Config {
    Config {
        flush_ms: 0,
        cache_bytes: 0,
        signal_bytes: 0,
        tick_ms: 60000,
        retire_ms: 60000,
        ..Default::default()
    }
}
fn d() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 30000,
        local: Instant::now() + Duration::from_secs(30),
    }
}
struct Peer {
    s: Arc<Store>,
    pending: Vec<u8>,
    reply: Vec<u8>,
    damage: Arc<Mutex<u8>>,
    max: usize,
}
impl Read for Peer {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let n = b.len().min(self.reply.len()).min(37);
        b[..n].copy_from_slice(&self.reply[..n]);
        self.reply.drain(..n);
        Ok(n)
    }
}
impl Write for Peer {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.pending.extend(b);
        if self.pending.len() >= 4 {
            let n = u32::from_le_bytes(self.pending[..4].try_into().unwrap()) as usize;
            if self.pending.len() == n + 4 {
                let op = self.pending[8];
                self.reply = self
                    .s
                    .dispatch_for_limit(&self.pending[4..], false, self.max);
                self.pending.clear();
                if op == membus::ATOMIC_PUBLISH && *self.damage.lock().unwrap() == 1 {
                    self.reply.clear();
                }
            }
        }
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for Peer {
    fn deadline(&mut self, _: Instant) -> io::Result<()> {
        Ok(())
    }
}
fn client(s: &Arc<Store>, damage: Arc<Mutex<u8>>, max: usize) -> Client<Peer> {
    Client::handshake(
        Peer {
            s: s.clone(),
            pending: vec![],
            reply: vec![],
            damage,
            max,
        },
        Options::new("authored-wire", max),
        d().local,
    )
    .unwrap()
}
fn request(c: &mut Client<Peer>, id: &str, changes: Vec<FinalChange>) -> AtomicRequest {
    c.capture_atomic_namespace("authored", "capture", d())
        .unwrap()
        .prepare(id, AtomicAck::Flush, changes)
        .unwrap()
}

#[derive(Clone)]
struct Independent(Arc<Mutex<Witness>>);
struct Witness {
    _dir: TempDir,
    grant: Grant,
    active: Option<EffectContext>,
    receipts: Vec<Receipt>,
    log: std::fs::File,
    lose_settlement: bool,
}
struct Permit {
    witness: Independent,
    context: EffectContext,
    settled: bool,
}
impl Independent {
    fn fresh() -> Self {
        let dir = TempDir::new("external-witness");
        let log = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.0.join("participation"))
            .unwrap();
        Self(Arc::new(Mutex::new(Witness {
            _dir: dir,
            grant: Grant {
                store_identity: "authored-store".into(),
                authority_incarnation: "independent-owner".into(),
                generation: 1,
                history_digest: [1; 32],
                participants: vec!["writer".into(), "sink".into()],
            },
            active: None,
            receipts: vec![],
            log,
            lose_settlement: false,
        })))
    }
    fn grant(&self) -> Grant {
        self.0.lock().unwrap().grant.clone()
    }
    fn replace(&self, history: [u8; 32]) -> Result<Grant, AuthorityError> {
        let mut w = self.0.lock().unwrap();
        if w.active.is_some() {
            return Err(AuthorityError::Unsettled);
        }
        if history != w.grant.history_digest {
            return Err(AuthorityError::Unsettled);
        }
        w.grant.generation += 1;
        w.log.write_all(b"replacement\n").unwrap();
        w.log.sync_all().unwrap();
        Ok(w.grant.clone())
    }
}
impl EffectAuthority for Independent {
    type Permit = Permit;
    fn enter(&mut self, c: &EffectContext) -> Result<Permit, AuthorityError> {
        let mut w = self.0.lock().unwrap();
        if !w.grant.participants.contains(&c.participant) {
            return Err(AuthorityError::Unsupported);
        }
        if c.grant != w.grant {
            return Err(AuthorityError::Stale);
        }
        if w.active.is_some() {
            return Err(AuthorityError::Unsettled);
        }
        w.log.write_all(&c.original).unwrap();
        w.log.sync_all().unwrap();
        w.active = Some(c.clone());
        Ok(Permit {
            witness: self.clone(),
            context: c.clone(),
            settled: false,
        })
    }
}
impl EffectPermit for Permit {
    fn settle(mut self, s: Settlement) -> Result<Receipt, AuthorityError> {
        let mut w = self.witness.0.lock().unwrap();
        if w.lose_settlement {
            return Err(AuthorityError::Unsettled);
        }
        let receipt = Receipt {
            context: self.context.clone(),
            settlement: s,
        };
        w.log
            .write_all(format!("{receipt:?}\n").as_bytes())
            .unwrap();
        w.log.sync_all().unwrap();
        w.receipts.push(receipt.clone());
        w.active = None;
        self.settled = true;
        Ok(receipt)
    }
}
impl Drop for Permit {
    fn drop(&mut self) { /* Active custody remains in independent witness after loss. */
    }
}

#[test]
fn participating_success_retains_original_receipt_and_fences_former_grant() {
    let dir = TempDir::new("participating");
    let s = Arc::new(Store::open(&dir.0, config()).unwrap());
    let mut c = client(&s, Arc::new(Mutex::new(0)), membus::MAX_FRAME);
    let r = request(&mut c, "owned", vec![FinalChange::put("a", vec![1])]);
    let external = Independent::fresh();
    let old = external.grant();
    let mut p = AtomicPublisher::new(external.clone(), old.clone(), "writer".into()).unwrap();
    let provider = p.publish(&mut c, &r, d()).unwrap();
    let receipt = external.0.lock().unwrap().receipts[0].clone();
    assert_ne!(provider.version, 0);
    assert_eq!(
        receipt.settlement,
        Settlement::Committed {
            version: provider.version,
            ack: provider.achieved as u8,
        }
    );
    assert_eq!(receipt.context.original, r.to_bytes());
    assert!(matches!(
        receipt.settlement,
        Settlement::Committed { ack: 2, .. }
    ));
    let next = external.replace(old.history_digest).unwrap();
    assert!(next.generation > old.generation);
    assert!(matches!(
        p.publish(&mut c, &r, d()),
        Err(ServingError::Authority(AuthorityError::Stale))
    ));
    assert_eq!(external.0.lock().unwrap().receipts.len(), 1);
}

#[test]
fn effect_permit_excludes_replacement_and_unknown_sinks() {
    let mut external = Independent::fresh();
    let grant = external.grant();
    let context = EffectContext {
        grant: grant.clone(),
        participant: "sink".into(),
        original: b"authored sink original".to_vec(),
    };
    let permit = external.enter(&context).unwrap();
    assert_eq!(
        external.replace(grant.history_digest),
        Err(AuthorityError::Unsettled)
    );
    drop(permit);
    assert_eq!(
        external.replace(grant.history_digest),
        Err(AuthorityError::Unsettled)
    );
    let mut unknown = context.clone();
    unknown.participant = "legacy-external".into();
    assert!(matches!(
        external.enter(&unknown),
        Err(AuthorityError::Unsupported)
    ));
    assert_eq!(external.0.lock().unwrap().active.as_ref(), Some(&context));
}

#[test]
fn provider_reply_loss_and_external_receipt_loss_remain_unsettled() {
    for provider_loss in [false, true] {
        let dir = TempDir::new("unsettled");
        let s = Arc::new(Store::open(&dir.0, config()).unwrap());
        let mut c = client(
            &s,
            Arc::new(Mutex::new(if provider_loss { 1 } else { 0 })),
            membus::MAX_FRAME,
        );
        let r = request(&mut c, "owned", vec![FinalChange::put("a", vec![1])]);
        let external = Independent::fresh();
        external.0.lock().unwrap().lose_settlement = !provider_loss;
        let grant = external.grant();
        let mut p = AtomicPublisher::new(external.clone(), grant.clone(), "writer".into()).unwrap();
        match p.publish(&mut c, &r, d()) {
            Err(ServingError::Unsettled {
                context, provider, ..
            }) => {
                assert_eq!(context.original, r.to_bytes());
                assert_eq!(provider.is_some(), !provider_loss);
            }
            v => panic!("unexpected {v:?}"),
        };
        assert_eq!(
            external.replace(grant.history_digest),
            Err(AuthorityError::Unsettled)
        );
        assert!(s.get("authored", "a").is_some());
        assert!(external.0.lock().unwrap().receipts.is_empty());
    }
}

#[test]
fn missing_history_and_missing_participant_never_make_a_grant() {
    let e = Independent::fresh();
    assert_eq!(e.replace([9; 32]), Err(AuthorityError::Unsettled));
    let mut g = e.grant();
    g.participants.clear();
    assert!(matches!(
        AtomicPublisher::new(e, g, "writer".into()),
        Err(AuthorityError::Invalid)
    ));
}
