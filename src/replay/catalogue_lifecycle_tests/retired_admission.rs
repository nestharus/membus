//! Retired completion requires a committed transition, except for empty Opens.
use super::*;

fn closed(seq: u64, bytes: u64) -> chunks::Rec {
    chunks::Rec::Close {
        seq,
        bytes,
        next: 2,
    }
}

fn open(seq: u64) -> chunks::Rec {
    chunks::Rec::Open {
        seq,
        stripe: 0,
        class: Class::FOREVER,
        policy: "forever".into(),
        next: 2,
    }
}

#[test]
fn retired_cannot_skip_intent_for_a_populated_closed_chunk() {
    let (f, _, path, _, _) = seeded();
    append(
        &f,
        vec![
            closed(1, fs::metadata(&path).unwrap().len()),
            chunks::Rec::Retired { seq: 1 },
        ],
    );
    refused(
        &f,
        &path,
        "catalogue Retired without committed Retiring for closed chunk 1",
    );
}

#[test]
fn retired_cannot_skip_intent_for_a_missing_closed_chunk() {
    let (f, _, path, _, _) = seeded();
    append(
        &f,
        vec![open(2), closed(2, 100), chunks::Rec::Retired { seq: 2 }],
    );
    refused(
        &f,
        &path,
        "catalogue Retired without committed Retiring for closed chunk 2",
    );
}

#[test]
fn an_empty_close_still_requires_committed_retirement_intent() {
    let (f, _, path, _, _) = seeded();
    append(
        &f,
        vec![open(2), closed(2, 0), chunks::Rec::Retired { seq: 2 }],
    );
    refused(
        &f,
        &path,
        "catalogue Retired without committed Retiring for closed chunk 2",
    );
}

#[test]
fn incomplete_retirement_parts_do_not_authorize_completion() {
    let (f, _, path, m, outcome) = seeded();
    append(
        &f,
        vec![
            closed(1, fs::metadata(&path).unwrap().len()),
            chunks::Rec::RetiringPart {
                seq: 1,
                next: 2,
                index: 0,
                count: 2,
                horizons: vec![(m.ns, m.key, outcome.version)],
                expired: Vec::new(),
            },
            chunks::Rec::Retired { seq: 1 },
        ],
    );
    refused(
        &f,
        &path,
        "catalogue Retired without committed Retiring for closed chunk 1",
    );
}

#[test]
fn a_committed_retirement_can_complete_and_repeat() {
    let (f, manual, path, m, _) = seeded();
    append(
        &f,
        vec![
            closed(1, fs::metadata(&path).unwrap().len()),
            chunks::Rec::Retiring {
                seq: 1,
                next: 2,
                horizons: Vec::new(),
                expired: Vec::new(),
            },
            chunks::Rec::Retired { seq: 1 },
            chunks::Rec::Retired { seq: 1 },
        ],
    );
    fs::remove_file(path).unwrap();
    let store = f.open(&manual);
    assert_eq!(store.epoch, 2);
    assert!(store.get(&m.ns, &m.key).is_none());
    assert_eq!(
        store.chunks.lock().unwrap().table[&1].state,
        chunks::ChunkState::Retired
    );
    assert!(!store.health2().storage_error);
}

#[test]
fn absent_open_completion_and_repetition_survive_live_list_rewrites() {
    let (f, manual, _, m, outcome) = seeded();
    append(
        &f,
        vec![
            open(2),
            chunks::Rec::Retired { seq: 2 },
            chunks::Rec::Retired { seq: 2 },
        ],
    );
    for epoch in [2, 3] {
        let store = f.open(&manual);
        assert_eq!(store.epoch, epoch);
        assert_eq!(store.get(&m.ns, &m.key).unwrap().bytes, m.bytes);
        assert_eq!(store.mutate(m.clone()), outcome);
        store.chunks.lock().unwrap().rewrite_live(UTC, 2).unwrap();
        drop(store);
        f.unlock();
    }
}

mod populated_open {
    use super::*;

    const REASON: &str = "catalogue Retired for nonempty or non-file Open chunk";

    #[test]
    fn retired_cannot_hide_a_populated_open_file() {
        let (f, _, path, _, _) = seeded();
        append(&f, vec![chunks::Rec::Retired { seq: 1 }]);
        refused(&f, &path, &format!("{REASON} 1"));
    }

    #[test]
    fn retired_cannot_bypass_inspection_of_an_open_torn_tail() {
        let (f, _, path, _, _) = seeded();
        fs::write(&path, b"MBK").unwrap();
        append(&f, vec![chunks::Rec::Retired { seq: 1 }]);
        refused(&f, &path, &format!("{REASON} 1"));
    }

    #[test]
    fn a_directory_at_the_open_path_is_not_proof_of_an_empty_file() {
        let (f, _, path, _, _) = seeded();
        let invalid = chunks::chunk_path(&f.0.join("chunks"), Class::FOREVER, 0, 2);
        fs::create_dir_all(&invalid).unwrap();
        append(&f, vec![open(2), chunks::Rec::Retired { seq: 2 }]);
        refused(&f, &path, &format!("{REASON} 2"));
        assert!(invalid.is_dir());
    }

    #[test]
    fn a_zero_length_retired_open_keeps_the_existing_storage_error_observation() {
        let (f, manual, _, m, outcome) = seeded();
        let empty = chunks::chunk_path(&f.0.join("chunks"), Class::FOREVER, 0, 2);
        fs::create_dir_all(empty.parent().unwrap()).unwrap();
        fs::write(&empty, b"").unwrap();
        append(&f, vec![open(2), chunks::Rec::Retired { seq: 2 }]);
        let store = f.open(&manual);
        assert!(store.health2().storage_error);
        assert_eq!(fs::read(empty).unwrap(), b"");
        assert_eq!(store.get(&m.ns, &m.key).unwrap().version, outcome.version);
    }

}
