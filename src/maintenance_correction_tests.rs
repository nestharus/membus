use super::*;
use crate::{Mutation, Store};
use std::cell::Cell;
thread_local! {static VISITS:Cell<usize>=const {Cell::new(0)};}
pub(super) fn visit() {
    VISITS.with(|n| n.set(n.get() + 1));
}
fn fixture(label: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit retained fixture root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let p = base.join(format!(
        "retention-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&p).unwrap();
    println!("retained retry fixture {}", p.display());
    p
}
#[test]
fn ordinary_forever_chunks_do_not_multiply_retry_work() {
    let p = fixture("ordinary");
    let store = Store::open(
        &p,
        Config {
            chunk_bytes: 1,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            signal_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap();
    for n in 0..12 {
        assert_eq!(
            store
                .mutate(Mutation {
                    ns: "authored".into(),
                    key: "same".into(),
                    op_id: format!("ordinary-{n}"),
                    deadline: u64::MAX,
                    ack: 2,
                    expected: None,
                    bytes: vec![n],
                    remove: false
                })
                .code,
            0
        );
    }
    let chunks = store.chunks.lock().unwrap().table.len();
    let entries = store.state.lock().unwrap().dedup.len();
    assert!(chunks >= 12 && entries >= 12);
    VISITS.with(|n| n.set(0));
    assert!(store.maint.retire(None).unwrap().is_empty());
    let visits = VISITS.with(Cell::get);
    println!("ordinary chunks={chunks} dedup={entries} retry_visits={visits}");
    assert!(visits<=entries,"ordinary maintenance made {visits} retry visits for {chunks} chunks and {entries} identities");
    assert_eq!(
        store.try_get("authored", "same").unwrap().unwrap().bytes,
        vec![11]
    );
}
#[test]
fn forced_retry_guard_keeps_whole_atomic_chunk_without_retirement_effects() {
    use crate::atomic::{AtomicAck, FinalChange};
    let p = fixture("forced-guard");
    let store = Store::open(
        &p,
        Config {
            chunk_bytes: 1,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            signal_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap();
    let original = store
        .capture_namespace("authored")
        .unwrap()
        .prepare(
            "atomic-original",
            AtomicAck::Flush,
            vec![
                FinalChange::put("a", b"a".to_vec()),
                FinalChange::put("b", b"b".to_vec()),
            ],
        )
        .unwrap();
    let outcome = store.publish_atomic(&original, u64::MAX).unwrap();
    let next = store
        .capture_namespace("authored")
        .unwrap()
        .prepare(
            "rotate",
            AtomicAck::Flush,
            vec![
                FinalChange::put("a", vec![3]),
                FinalChange::put("b", vec![4]),
            ],
        )
        .unwrap();
    store.publish_atomic(&next, u64::MAX).unwrap();
    let id = store
        .state
        .lock()
        .unwrap()
        .dedup
        .values()
        .find(|d| d.outcome.as_ref().is_some_and(|o| o.version == outcome.version))
        .unwrap()
        .replayed_chunk;
    let chunk = store.chunks.lock().unwrap().table[&id].clone();
    assert_eq!(chunk.state, chunks::ChunkState::Closed);
    let before = chunks::sha256_file(&chunk.path).unwrap();
    let catalogue = chunks::sha256_file(&p.join("chunks/catalogue.log")).unwrap();
    assert!(store.maint.retire(Some(id)).unwrap().is_empty());
    assert!(store.maint.retire(None).unwrap().is_empty());
    assert_eq!(
        store.chunks.lock().unwrap().table[&id].state,
        chunks::ChunkState::Closed
    );
    assert_eq!(chunks::sha256_file(&chunk.path).unwrap(), before);
    assert_eq!(
        chunks::sha256_file(&p.join("chunks/catalogue.log")).unwrap(),
        catalogue
    );
    // An admitted pending outcome keeps custody even past its local horizon.
    // No eligible destructive retirement or forced-delete neighbor is executed.
    let saved = {
        let mut state = store.state.lock().unwrap();
        let d = state
            .dedup
            .values_mut()
            .find(|d| d.replayed_chunk == id)
            .unwrap();
        let saved = d.clone();
        d.until_mono = 0;
        d.outcome = None;
        saved
    };
    assert!(store.maint.retire(Some(id)).unwrap().is_empty());
    assert_eq!(chunks::sha256_file(&chunk.path).unwrap(), before);
    assert_eq!(
        chunks::sha256_file(&p.join("chunks/catalogue.log")).unwrap(),
        catalogue
    );
    {
        let mut state = store.state.lock().unwrap();
        let d = state
            .dedup
            .values_mut()
            .find(|d| d.replayed_chunk == id)
            .unwrap();
        *d = saved;
    }
    assert_eq!(store.publish_atomic(&original, u64::MAX).unwrap(), outcome);
}
