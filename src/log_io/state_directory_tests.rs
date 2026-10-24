//! State directory names must be covered before ownership or recovery begins.
use super::*;
use crate::Store;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".test-output")
            .join(format!(
                "state-dirs-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn unlock(path: &Path) {
        #[cfg(not(windows))]
        std::fs::remove_file(path.join("owner.lock")).unwrap();
        let _ = path;
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn nested_state_directory_names_precede_children_ownership_and_replay() {
    let fixture = Fixture::new();
    let parent = fixture.0.join("parent");
    let state = parent.join("state");
    let expected = vec![
        fixture.0.parent().unwrap().to_owned(),
        fixture.0.clone(),
        parent.clone(),
    ];
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = events.clone();
    let base = fixture.0.clone();
    let root = state.clone();
    let config = Config {
        state_dir_sync: Some(Arc::new(move |dir| {
            let mut events = seen.lock().unwrap();
            match events.len() {
                0 => assert!(!base.join("parent").exists()),
                1 => {
                    assert!(base.join("parent").is_dir());
                    assert!(
                        !root.exists(),
                        "child creation depends on its parent's barrier"
                    );
                }
                2 => assert!(root.is_dir()),
                n => panic!("unexpected barrier {n}"),
            }
            assert!(!root.join("owner.lock").exists());
            assert!(!root.join("chunks").exists());
            assert!(!root.join("epoch").exists());
            events.push(dir.to_owned());
            Ok(())
        })),
        ..Config::default()
    };
    let store = Store::open(&state, config).unwrap();
    assert_eq!(*events.lock().unwrap(), expected);
    assert_eq!(
        std::fs::read(state.join("epoch")).unwrap(),
        1u64.to_le_bytes()
    );
    drop(store);
    Fixture::unlock(&state);
}

#[test]
fn each_state_directory_failure_preserves_error_and_retries_the_uncovered_name() {
    for failure in 0..3 {
        let fixture = Fixture::new();
        let parent = fixture.0.join("parent");
        let state = parent.join("state");
        let expected = vec![
            fixture.0.parent().unwrap().to_owned(),
            fixture.0.clone(),
            parent.clone(),
        ];
        let events = Arc::new(Mutex::new(Vec::new()));
        let seen = events.clone();
        let config = Config {
            state_dir_sync: Some(Arc::new(move |dir| {
                let mut events = seen.lock().unwrap();
                events.push(dir.to_owned());
                if events.len() == failure + 1 {
                    Err(io::Error::from_raw_os_error(5))
                } else {
                    Ok(())
                }
            })),
            ..Config::default()
        };
        let error = Store::open(&state, config)
            .err()
            .expect("name sync must refuse startup");
        assert_eq!(error.raw_os_error(), Some(5));
        assert_eq!(*events.lock().unwrap(), expected[..=failure]);
        assert!(!state.join("owner.lock").exists());
        for name in ["chunks", "epoch", "epoch.tmp", "replay.json", "replay.log"] {
            assert!(
                !state.join(name).exists(),
                "no recovery after failed name sync: {name}"
            );
        }
        assert_eq!(parent.exists(), failure >= 1);
        assert_eq!(state.exists(), failure == 2);

        let retried = Arc::new(Mutex::new(Vec::new()));
        let seen = retried.clone();
        let store = Store::open(
            &state,
            Config {
                state_dir_sync: Some(Arc::new(move |dir| {
                    seen.lock().unwrap().push(dir.to_owned());
                    Ok(())
                })),
                ..Config::default()
            },
        )
        .unwrap();
        assert_eq!(
            *retried.lock().unwrap(),
            expected[failure..],
            "retry must repeat the failed barrier even though mkdir already succeeded"
        );
        assert_eq!(
            std::fs::read(state.join("epoch")).unwrap(),
            1u64.to_le_bytes()
        );
        drop(store);
        Fixture::unlock(&state);
    }
}

#[test]
fn existing_state_parent_failure_preserves_epoch_catalogue_and_ready_report() {
    let fixture = Fixture::new();
    let state = fixture.0.join("state");
    drop(Store::open(&state, Config::default()).unwrap());
    Fixture::unlock(&state);
    let names = ["epoch", "chunks/catalogue.log", "replay.json", "replay.log"];
    let before: Vec<_> = names
        .iter()
        .map(|name| std::fs::read(state.join(name)).unwrap())
        .collect();
    let parent = fixture.0.clone();
    let calls = Arc::new(AtomicU64::new(0));
    let seen = calls.clone();
    let error = Store::open(
        &state,
        Config {
            state_dir_sync: Some(Arc::new(move |dir| {
                assert_eq!(dir, parent);
                seen.fetch_add(1, Ordering::Relaxed);
                Err(io::Error::from_raw_os_error(5))
            })),
            ..Config::default()
        },
    )
    .err()
    .expect("existing names still need their parent barrier");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(error.raw_os_error(), Some(5));
    for (name, bytes) in names.iter().zip(before) {
        assert_eq!(
            std::fs::read(state.join(name)).unwrap(),
            bytes,
            "changed {name}"
        );
    }
    drop(Store::open(&state, Config::default()).unwrap());
    Fixture::unlock(&state);
    assert_eq!(
        std::fs::read(state.join("epoch")).unwrap(),
        2u64.to_le_bytes()
    );
}

#[test]
fn relative_existing_state_path_uses_its_real_parent() {
    let fixture = Fixture::new();
    let state = fixture.0.join("state");
    std::fs::create_dir(&state).unwrap();
    let relative = state
        .strip_prefix(std::env::current_dir().unwrap())
        .unwrap()
        .to_owned();
    let expected = fixture.0.clone();
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = events.clone();
    let store = Store::open(
        &relative,
        Config {
            state_dir_sync: Some(Arc::new(move |dir| {
                seen.lock().unwrap().push(dir.to_owned());
                Ok(())
            })),
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(*events.lock().unwrap(), vec![expected]);
    drop(store);
    Fixture::unlock(&state);
}

#[test]
fn state_file_obstruction_refuses_without_startup_artifacts() {
    let fixture = Fixture::new();
    let state = fixture.0.join("state");
    std::fs::write(&state, b"authored obstruction").unwrap();
    assert!(Store::open(&state, Config::default()).is_err());
    assert_eq!(std::fs::read(&state).unwrap(), b"authored obstruction");
}
