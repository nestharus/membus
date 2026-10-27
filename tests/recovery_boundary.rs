//! Counterexample: an authored stopped copy can reuse an epoch without authority continuity.
//! This is not a public capture adapter. All images are retained unchanged.
use membus::{chunks::sha256_file, client::*, Config, Store};
use std::{
    collections::VecDeque,
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Wire {
    store: Arc<Store>,
    input: VecDeque<u8>,
    lose_remove_reply: bool,
}
impl Read for Wire {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = bytes.len().min(self.input.len()).min(13);
        for b in &mut bytes[..n] {
            *b = self.input.pop_front().unwrap();
        }
        Ok(n)
    }
}
impl Write for Wire {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert_eq!(
            bytes.len(),
            4 + u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize
        );
        let reply = self.store.dispatch(&bytes[4..]);
        if !(self.lose_remove_reply && bytes[8] == membus::REMOVE) {
            self.input.extend(reply);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for Wire {
    fn deadline(&mut self, until: Instant) -> io::Result<()> {
        if Instant::now() >= until {
            Err(io::ErrorKind::TimedOut.into())
        } else {
            Ok(())
        }
    }
}
fn d() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 30000,
        local: Instant::now() + Duration::from_secs(30),
    }
}
fn client(store: &Arc<Store>, lose_remove_reply: bool) -> Client<Wire> {
    Client::handshake(
        Wire {
            store: store.clone(),
            input: VecDeque::new(),
            lose_remove_reply,
        },
        Options::new("authored-lineage-control", 65536),
        d().local,
    )
    .unwrap()
}
fn config() -> Config {
    Config {
        cache_bytes: 65536,
        signal_bytes: 0,
        flush_ms: 0,
        tick_ms: 60000,
        retire_ms: 60000,
        ..Config::default()
    }
}
fn fixture() -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_REMOVE_TEST_ROOT")
            .expect("explicit private authored root required"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = base.join(format!("lineage-{}-{stamp}", std::process::id()));
    fs::create_dir(&root).unwrap();
    println!(
        "authored retained original/image/working roots {}",
        root.display()
    );
    root
}
fn copy_authored_stopped_tree(source: &Path, destination: &Path) {
    fs::create_dir(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        assert!(!kind.is_symlink());
        if kind.is_dir() {
            copy_authored_stopped_tree(&entry.path(), &destination.join(entry.file_name()));
        } else {
            assert!(kind.is_file());
            fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
        }
    }
}
fn inventory(root: &Path) -> Vec<(PathBuf, u64, String)> {
    fn walk(root: &Path, path: &Path, out: &mut Vec<(PathBuf, u64, String)>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            assert!(!kind.is_symlink());
            if kind.is_dir() {
                walk(root, &entry.path(), out);
            } else {
                assert!(kind.is_file());
                out.push((
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    entry.metadata().unwrap().len(),
                    sha256_file(&entry.path()).unwrap(),
                ));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}
#[test]
fn equal_epoch_clone_can_reexecute_an_original_uncertain_removal() {
    let root = fixture();
    let original = root.join("original");
    let image = root.join("retained-image");
    let working = root.join("isolated-working");
    let original_revision = {
        let store = Arc::new(Store::open(&original, config()).unwrap());
        let mut c = client(&store, false);
        let mut a = c
            .prepare(MutationRequest {
                operation_id: "before-capture".into(),
                namespace: "authored".into(),
                key: "item".into(),
                value: b"original".to_vec(),
                ack: Ack::Flush,
                expected: Some(0),
            })
            .unwrap();
        c.mutate(&mut a, d()).unwrap().version
    }; // All owners have dropped before copying this authored source.
    copy_authored_stopped_tree(&original, &image);
    let retained = inventory(&image);
    assert!(!retained.is_empty());
    let original_store = Arc::new(Store::open(&original, config()).unwrap());
    let mut lost = client(&original_store, true);
    let mut attempt = lost
        .prepare_remove(RemoveRequest {
            operation_id: "original-uncertain-remove".into(),
            namespace: "authored".into(),
            key: "item".into(),
            expected: original_revision,
            ack: Ack::Flush,
        })
        .unwrap();
    assert!(matches!(
        lost.remove(&mut attempt, d()),
        Err(Error::Uncertain { .. })
    ));
    assert!(original_store
        .try_get("authored", "item")
        .unwrap()
        .is_none());
    copy_authored_stopped_tree(&image, &working);
    let restored_store = Arc::new(Store::open(&working, config()).unwrap());
    let mut restored = client(&restored_store, false);
    assert_eq!(restored.negotiated().epoch, attempt.epoch());
    assert_eq!(
        restored_store
            .try_get("authored", "item")
            .unwrap()
            .unwrap()
            .bytes,
        b"original"
    );
    // Existing SDK checks admit this resend even though this history predates
    // the original completed effect. It is an unsafe-continuity counterexample.
    restored.remove(&mut attempt, d()).unwrap();
    assert!(restored_store
        .try_get("authored", "item")
        .unwrap()
        .is_none());
    assert_eq!(
        inventory(&image),
        retained,
        "recovery must not mutate the retained original image"
    );
}
