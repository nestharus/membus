//! Recovery rename barriers and the import's caller-scoped synthetic identity.
use super::*;
use crate::{now_ms, Store};
use std::sync::{atomic::AtomicU64, Mutex};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".test-output")
            .join(format!(
                "recovery-dirs-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::SeqCst)
            ));
        fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn legacy(&self, collisions: bool) -> Vec<String> {
        fs::write(self.0.join("epoch"), 7u64.to_le_bytes()).unwrap();
        let mut bytes = Vec::new();
        let ids = if collisions {
            vec!["import-policy/8/0", "import-policy/8/0/1"]
        } else {
            vec!["original"]
        };
        let si = stripe("caller", "key");
        for (i, id) in ids.iter().enumerate() {
            let m = Mutation {
                ns: "caller".into(),
                key: "key".into(),
                op_id: (*id).into(),
                deadline: u64::MAX,
                ack: 1,
                expected: None,
                bytes: id.as_bytes().to_vec(),
                remove: false,
            };
            let b = mutation_bytes(&m, 7 + i as u64, now_ms());
            bytes.extend(b"MBK1");
            bytes.extend((b.len() as u32).to_le_bytes());
            bytes.extend((!(b.len() as u32)).to_le_bytes());
            bytes.extend(crc32(&b).to_le_bytes());
            bytes.extend(b);
        }
        let name = format!("stripe-{si}.log");
        fs::write(self.0.join(&name), bytes).unwrap();
        fs::write(self.0.join("import-policies.txt"), b"caller\t\tlatest\n").unwrap();
        fs::create_dir_all(self.0.join("chunks")).unwrap();
        vec![name, "import-policies.txt".into()]
    }
    fn clock() -> chunks::Clock {
        chunks::Clock::new(None)
    }
    fn unlock(&self) {
        #[cfg(not(windows))]
        fs::remove_file(self.0.join("owner.lock")).unwrap();
    }
}
fn writable(p: &Path) {
    let mut perms = fs::metadata(p).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    perms.set_readonly(false);
    fs::set_permissions(p, perms).unwrap();
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fn clear(dir: &Path) {
            for e in fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    clear(&p);
                } else {
                    writable(&p);
                }
            }
        }
        clear(&self.0);
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn import_observer(
    f: &Fixture,
    files: Vec<String>,
    fail: usize,
) -> (Config, Arc<Mutex<Vec<PathBuf>>>) {
    let root = f.0.clone();
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = events.clone();
    let config = Config {
        recovery_dir_sync: Some(Arc::new(move |dir| {
            // These witnesses count rename barriers; construction barriers have
            // separate controls below and must complete before the first rename.
            if dir.starts_with(root.join("chunks.import")) {
                return Ok(());
            }
            let mut seen = seen.lock().unwrap();
            seen.push(dir.to_owned());
            let n = seen.len();
            assert_eq!(fs::read(root.join("epoch")).unwrap(), 7u64.to_le_bytes());
            if n == 1 {
                assert_eq!(dir, root);
                assert!(!root.join("chunks").exists());
                assert!(root.join("chunks.pre-import-8").exists());
                assert!(root.join("chunks.import").exists());
                assert!(files.iter().all(|name| root.join(name).exists()));
            } else if n == 2 {
                assert_eq!(dir, root);
                assert!(root.join("chunks").exists());
                assert!(!root.join("chunks.import").exists());
                assert!(files.iter().all(|name| root.join(name).exists()));
            } else {
                let index = (n - 3) / 2;
                let dest = root.join("legacy-8").join(&files[index]);
                assert_eq!(
                    dir,
                    if n % 2 == 1 {
                        root.join("legacy-8")
                    } else {
                        root.clone()
                    }
                );
                assert!(!root.join(&files[index]).exists());
                assert!(dest.exists());
                assert!(
                    !fs::metadata(dest).unwrap().permissions().readonly(),
                    "readonly depends on both directory barriers"
                );
                if let Some(next) = files.get(index + 1) {
                    assert!(
                        root.join(next).exists(),
                        "next rename must follow these barriers"
                    );
                }
            }
            if n == fail {
                Err(io::Error::other("injected recovery directory sync failure"))
            } else {
                Ok(())
            }
        })),
        ..Config::default()
    };
    (config, events)
}

#[test]
fn import_rename_barriers_precede_dependent_steps() {
    let f = Fixture::new();
    let files = f.legacy(false);
    let (config, events) = import_observer(&f, files, 0);
    import_legacy(&f.0, &config, &Fixture::clock(), 8)
        .unwrap()
        .unwrap();
    assert_eq!(
        events.lock().unwrap().len(),
        6,
        "two swap barriers plus both parents of each input move"
    );
}

#[test]
fn each_import_directory_sync_failure_refuses_and_retries() {
    for stop in 1..=6 {
        let f = Fixture::new();
        let files = f.legacy(false);
        let (config, events) = import_observer(&f, files, stop);
        let result = import_legacy(&f.0, &config, &Fixture::clock(), 8);
        assert!(
            result.is_err(),
            "directory barrier {stop} must refuse the import"
        );
        assert_eq!(
            events.lock().unwrap().len(),
            stop,
            "no later barrier may run after failure"
        );
        let s = Store::open(&f.0, Config::default()).unwrap();
        assert_eq!(s.get("caller", "key").unwrap().bytes, b"original");
        assert!(!s.storage_error());
        assert!(fs::metadata(f.0.join("legacy-8/import-policies.txt"))
            .unwrap()
            .permissions()
            .readonly());
    }
}

#[test]
fn moved_backup_retries_both_directory_barriers_before_readonly() {
    let f = Fixture::new();
    let files = f.legacy(false);
    let (import, _) = import_legacy(&f.0, &Config::default(), &Fixture::clock(), 8)
        .unwrap()
        .unwrap();
    let dest = f.0.join("legacy-8").join(&files[0]);
    writable(&dest);
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = events.clone();
    let expected = dest.clone();
    let config = Config {
        recovery_dir_sync: Some(Arc::new(move |dir| {
            assert!(!fs::metadata(&expected).unwrap().permissions().readonly());
            let mut seen = seen.lock().unwrap();
            seen.push(dir.to_owned());
            if seen.len() == 2 {
                Err(io::Error::other("source directory sync failed"))
            } else {
                Ok(())
            }
        })),
        ..Config::default()
    };
    assert!(move_legacy(&f.0, &import, &config).is_err());
    assert_eq!(
        *events.lock().unwrap(),
        vec![f.0.join("legacy-8"), f.0.clone()]
    );
    assert!(!fs::metadata(&dest).unwrap().permissions().readonly());
    move_legacy(&f.0, &import, &Config::default()).unwrap();
    assert!(fs::metadata(dest).unwrap().permissions().readonly());
}

#[test]
fn startup_retries_backup_barriers_with_no_source_names_remaining() {
    let f = Fixture::new();
    let files = f.legacy(false);
    import_legacy(&f.0, &Config::default(), &Fixture::clock(), 8).unwrap();
    let backup = f.0.join("legacy-8");
    writable(&backup.join(&files[0]));
    let config = Config {
        recovery_dir_sync: Some(Arc::new(move |dir| {
            if dir == backup {
                Err(io::Error::other("backup directory sync failed"))
            } else {
                Ok(())
            }
        })),
        ..Config::default()
    };
    assert!(
        Store::open(&f.0, config).is_err(),
        "no surviving source name does not prove the backup barrier completed"
    );
    f.unlock();
    let s = Store::open(&f.0, Config::default()).unwrap();
    assert_eq!(s.get("caller", "key").unwrap().bytes, b"original");
    assert!(fs::metadata(f.0.join("legacy-8").join(&files[0]))
        .unwrap()
        .permissions()
        .readonly());
}

#[test]
fn epoch_directory_sync_follows_rename_before_ready() {
    let f = Fixture::new();
    let expected = f.0.clone();
    let events = Arc::new(Mutex::new(0));
    let seen = events.clone();
    let config = Config {
        recovery_dir_sync: Some(Arc::new(move |dir| {
            if dir == expected.join("chunks") {
                return Ok(()); // The catalogue namespace has its own pre-epoch controls.
            }
            assert_eq!(dir, expected);
            assert_eq!(
                fs::read(expected.join("epoch")).unwrap(),
                1u64.to_le_bytes()
            );
            assert!(!expected.join("epoch.tmp").exists());
            assert!(!expected.join("replay.json").exists());
            *seen.lock().unwrap() += 1;
            Ok(())
        })),
        ..Config::default()
    };
    let s = Store::open(&f.0, config).unwrap();
    assert_eq!(*events.lock().unwrap(), 1);
    assert_eq!(s.epoch, 1);
    assert_eq!(s.replay_report().stage, "ready");
}

#[test]
fn epoch_directory_sync_failure_refuses_publication() {
    let f = Fixture::new();
    let root = f.0.clone();
    let config = Config {
        recovery_dir_sync: Some(Arc::new(move |dir| {
            if dir == root {
                Err(io::Error::other("epoch directory sync failed"))
            } else {
                Ok(())
            }
        })),
        ..Config::default()
    };
    assert!(Store::open(&f.0, config).is_err());
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""));
    assert!(report.contains("\"ready_ms\":0"));
    f.unlock();
    let s = Store::open(&f.0, Config::default()).unwrap();
    assert_eq!(s.epoch, 2);
}

#[test]
fn directory_sync_uses_a_real_handle_and_propagates_open_errors() {
    let f = Fixture::new();
    crate::log_io::sync_dir(&f.0).unwrap();
    assert!(crate::log_io::sync_dir(&f.0.join("missing")).is_err());
}

#[test]
fn synthetic_import_policy_id_salts_the_caller_namespace_collision() {
    let f = Fixture::new();
    f.legacy(true);
    import_legacy(&f.0, &Config::default(), &Fixture::clock(), 8).unwrap();
    let rp =
        replay(&f.0, &Config::default(), &Fixture::clock()).unwrap_or_else(|(e, _)| panic!("{e}"));
    let policy = rp
        .committed
        .get(&(POLICY_NS.into(), "caller\0".into()))
        .unwrap();
    let (m, version, _) = parse_record(&policy.location.payload().unwrap()).unwrap();
    assert_eq!(m.op_id, "import-policy/8/0/2");
    assert_eq!(dedup_identity(&m), ("caller".into(), m.op_id.clone()));
    assert_eq!(
        rp.dedup[&("caller".into(), "import-policy/8/0".into())]
            .outcome
            .as_ref()
            .unwrap()
            .version,
        7
    );
    assert_eq!(
        rp.dedup[&("caller".into(), "import-policy/8/0/1".into())]
            .outcome
            .as_ref()
            .unwrap()
            .version,
        8
    );
    assert!(version > 8);
}

#[test]
fn policy_key_refuses_both_control_namespaces_as_callers() {
    for caller in [POLICY_NS, timers::NS] {
        for prefix in ["", "prefix"] {
            assert_eq!(chunks::policy_key(caller, prefix), Err(3));
        }
        assert!(import_policies(
            format!("{caller}\t\tlatest\n").as_bytes(),
            &Config::default()
        )
        .is_err());
    }
    assert_eq!(
        chunks::policy_key("caller", "prefix"),
        Ok("caller\0prefix".into())
    );
}

#[test]
fn policy_parser_preserves_order_and_rejects_unadmitted_input() {
    let config = Config {
        retry_ms: 100,
        ..Config::default()
    };
    assert_eq!(
        import_policies(
            b"# authored fixture\r\ncaller\t\tlatest\r\ncaller\tp/\texpire:100\n",
            &config
        )
        .unwrap(),
        vec![
            ("caller".into(), "".into(), Class::LATEST),
            ("caller".into(), "p/".into(), Class::expire(100))
        ]
    );
    for bad in [
        b"\xff".as_slice(),
        b"caller\tlatest\n",
        b"caller\t\texpire:99\n",
        b"caller\t\texpire:0\n",
        b"caller\t\tunknown\n",
    ] {
        assert!(import_policies(bad, &config).is_err());
    }
    let many: String = (0..63)
        .map(|n| format!("caller\tp{n}/\texpire:{}\n", 100 + n))
        .collect();
    assert!(import_policies(many.as_bytes(), &config).is_err());
}

fn import_input_bytes(f: &Fixture, files: &[String]) -> Vec<(String, Vec<u8>)> {
    files
        .iter()
        .map(|name| (name.clone(), fs::read(f.0.join(name)).unwrap()))
        .collect()
}
fn assert_import_inputs_unchanged(root: &Path, inputs: &[(String, Vec<u8>)]) {
    for (name, bytes) in inputs {
        assert_eq!(fs::read(root.join(name)).unwrap(), *bytes);
        assert!(!fs::metadata(root.join(name))
            .unwrap()
            .permissions()
            .readonly());
    }
    assert_eq!(fs::read(root.join("epoch")).unwrap(), 7u64.to_le_bytes());
    assert!(root.join("chunks").is_dir());
    assert_eq!(fs::read_dir(root.join("chunks")).unwrap().count(), 0);
    assert!(!root.join("chunks.pre-import-8").exists());
    assert!(!root.join("legacy-8").exists());
}
fn assert_complete_tree_barriers(root: &Path, seen: &[PathBuf]) {
    let tree = root.join("chunks.import");
    assert_eq!(
        seen.len(),
        3,
        "both class directories and the catalogue directory must sync"
    );
    assert_eq!(
        seen.last(),
        Some(&tree),
        "children must sync before their parent"
    );
    let classes: std::collections::BTreeSet<_> = seen[..2].iter().cloned().collect();
    assert_eq!(classes, [tree.join("forever"), tree.join("latest")].into());
}

#[test]
fn import_tree_barriers_follow_complete_files_and_precede_the_first_rename() {
    let f = Fixture::new();
    let files = f.legacy(false);
    let inputs = import_input_bytes(&f, &files);
    let root = f.0.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = seen.clone();
    let config = Config {
        recovery_dir_sync: Some(Arc::new(move |dir| {
            let tree = root.join("chunks.import");
            let mut seen = observed.lock().unwrap();
            if dir.starts_with(&tree) {
                assert_import_inputs_unchanged(&root, &inputs);
                let records = chunks::read_catalogue(&tree.join("catalogue.log"))?.0;
                assert!(records
                    .iter()
                    .any(|r| matches!(r.rec, chunks::Rec::Import(_))));
                let opens = records
                    .iter()
                    .filter(|r| matches!(r.rec, chunks::Rec::Open { .. }))
                    .count();
                let closes = records
                    .iter()
                    .filter(|r| matches!(r.rec, chunks::Rec::Close { .. }))
                    .count();
                assert_eq!(opens, 2);
                assert_eq!(
                    closes, opens,
                    "complete data-file closes precede directory publication"
                );
                assert_eq!(chunks::list_chunk_files(&tree)?.len(), opens);
                seen.push(dir.to_owned());
                if dir == tree {
                    assert_complete_tree_barriers(&root, &seen);
                }
            } else {
                // Every rename barrier, including the first one, depends on the tree.
                assert_complete_tree_barriers(&root, &seen);
            }
            Ok(())
        })),
        ..Config::default()
    };
    import_legacy(&f.0, &config, &Fixture::clock(), 8).unwrap();
    assert_complete_tree_barriers(&f.0, &seen.lock().unwrap());
    assert!(!f.0.join("chunks.import").exists());
    let store = Store::open(&f.0, Config::default()).unwrap();
    assert_eq!(store.get("caller", "key").unwrap().bytes, b"original");
    assert!(!store.storage_error());
}

#[test]
fn each_import_tree_sync_failure_refuses_before_switch_and_can_retry() {
    for stop in 1..=3 {
        let f = Fixture::new();
        let files = f.legacy(false);
        let inputs = import_input_bytes(&f, &files);
        let tree = f.0.join("chunks.import");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = seen.clone();
        let config = Config {
            recovery_dir_sync: Some(Arc::new(move |dir| {
                if dir.starts_with(&tree) {
                    let mut seen = observed.lock().unwrap();
                    seen.push(dir.to_owned());
                    if seen.len() == stop {
                        return Err(io::Error::other(
                            "injected import tree directory sync failure",
                        ));
                    }
                }
                Ok(())
            })),
            ..Config::default()
        };
        let error = Store::open(&f.0, config)
            .err()
            .expect("a failed tree barrier must refuse startup");
        assert!(error
            .to_string()
            .contains("import tree directory sync failure"));
        assert_eq!(
            seen.lock().unwrap().len(),
            stop,
            "stop at the first failed barrier"
        );
        assert_import_inputs_unchanged(&f.0, &inputs);
        assert!(
            !f.0.join("chunks.import").exists(),
            "discard the unpublished replacement"
        );
        let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
        assert!(report.contains("\"stage\":\"refused\""));
        assert!(report.contains("\"ready_ms\":0"));
        f.unlock();
        let store = Store::open(&f.0, Config::default()).unwrap();
        assert_eq!(store.epoch, 8);
        assert_eq!(store.get("caller", "key").unwrap().bytes, b"original");
        assert!(!store.storage_error());
        for (name, bytes) in &inputs {
            let backup = f.0.join("legacy-8").join(name);
            assert_eq!(fs::read(&backup).unwrap(), *bytes);
            assert!(fs::metadata(backup).unwrap().permissions().readonly());
        }
    }
}

#[test]
fn empty_import_syncs_the_catalogue_directory_before_switching() {
    let f = Fixture::new();
    fs::write(f.0.join("epoch"), 7u64.to_le_bytes()).unwrap();
    fs::write(f.0.join("stripe-0.log"), b"").unwrap();
    fs::create_dir(f.0.join("chunks")).unwrap();
    let inputs = vec![("stripe-0.log".into(), Vec::new())];
    let tree = f.0.join("chunks.import");
    let config = Config {
        recovery_dir_sync: Some(Arc::new(move |dir| {
            if dir == tree {
                assert!(chunks::list_chunk_files(&tree)?.is_empty());
                let records = chunks::read_catalogue(&tree.join("catalogue.log"))?.0;
                assert!(records
                    .iter()
                    .any(|r| matches!(&r.rec, chunks::Rec::Import(i) if i.keys == 0)));
                return Err(io::Error::other("empty import directory sync failure"));
            }
            Ok(())
        })),
        ..Config::default()
    };
    let error = Store::open(&f.0, config)
        .err()
        .expect("even a catalogue-only import needs its directory barrier");
    assert!(error
        .to_string()
        .contains("empty import directory sync failure"));
    assert_import_inputs_unchanged(&f.0, &inputs);
    assert!(!f.0.join("chunks.import").exists());
    f.unlock();
    let store = Store::open(&f.0, Config::default()).unwrap();
    assert_eq!(store.epoch, 8);
    assert!(store.chunk_rows().is_empty());
    assert!(!store.storage_error());
    assert!(f.0.join("legacy-8/stripe-0.log").exists());
}

#[test]
fn equivalence_refusal_never_reaches_import_tree_publication() {
    let f = Fixture::new();
    let files = f.legacy(false);
    let inputs = import_input_bytes(&f, &files);
    let config = Config {
        import_inject_difference: true,
        recovery_dir_sync: Some(Arc::new(|_| {
            panic!("refused content must not reach a directory barrier")
        })),
        ..Config::default()
    };
    let error = Store::open(&f.0, config)
        .err()
        .expect("the injected equivalence difference must refuse");
    assert!(error.to_string().contains("import refused"));
    assert_import_inputs_unchanged(&f.0, &inputs);
    assert!(!f.0.join("chunks.import").exists());
}

#[test]
fn fresh_catalogue_directory_sync_failure_refuses_before_epoch_and_can_retry() {
    let f = Fixture::new();
    let root = f.0.clone();
    let config = Config {
        recovery_dir_sync: Some(Arc::new(move |dir| {
            if dir == root.join("chunks") {
                assert_eq!(fs::read(root.join("chunks/catalogue.log"))?, b"");
                assert!(!root.join("epoch").exists());
                return Err(io::Error::other("fresh catalogue directory sync failure"));
            }
            Ok(())
        })),
        ..Config::default()
    };
    let error = Store::open(&f.0, config)
        .err()
        .expect("a fresh catalogue name needs a directory barrier");
    assert!(error
        .to_string()
        .contains("fresh catalogue directory sync failure"));
    assert!(!f.0.join("epoch").exists());
    assert!(!f.0.join("epoch.tmp").exists());
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""));
    assert!(report.contains("\"ready_ms\":0"));
    f.unlock();
    let store = Store::open(&f.0, Config::default()).unwrap();
    assert_eq!(store.epoch, 1);
    assert_eq!(store.replay_report().stage, "ready");
}

#[test]
fn catalogue_directory_sync_failure_retains_observations_before_close_and_epoch() {
    let f = Fixture::new();
    let store = Store::open(&f.0, Config::default()).unwrap();
    let mutation = Mutation {
        ns: "caller".into(),
        key: "key".into(),
        op_id: "original".into(),
        deadline: u64::MAX,
        ack: 2,
        expected: None,
        bytes: b"original".to_vec(),
        remove: false,
    };
    assert_eq!(store.mutate(mutation).code, 0);
    drop(store);
    f.unlock();
    let files = chunks::list_chunk_files(&f.0.join("chunks")).unwrap();
    assert_eq!(files.len(), 1);
    let (_, si, id, original) = &files[0];
    let unregistered = chunks::chunk_path(&f.0.join("chunks"), Class::FOREVER, *si, id + 1000);
    fs::copy(original, &unregistered).unwrap();
    let data = fs::read(original).unwrap();
    let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
    let epoch = fs::read(f.0.join("epoch")).unwrap();
    let root = f.0.clone();
    let config = Config {
        recovery_dir_sync: Some(Arc::new(move |dir| {
            if dir == root.join("chunks") {
                return Err(io::Error::other("catalogue directory sync failure"));
            }
            Ok(())
        })),
        ..Config::default()
    };
    let error = Store::open(&f.0, config)
        .err()
        .expect("catalogue directory sync failure must refuse startup");
    assert!(error
        .to_string()
        .contains("catalogue directory sync failure"));
    assert_eq!(fs::read(original).unwrap(), data);
    assert_eq!(
        fs::read(f.0.join("chunks/catalogue.log")).unwrap(),
        catalogue,
        "the directory barrier must precede a recovered Close"
    );
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), epoch);
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""));
    assert!(report.contains("\"ready_ms\":0"));
    let observation = format!("chunk file with no open record: {}", unregistered.display());
    assert!(report.contains(&format!("{observation:?}")), "{report}");
    f.unlock();
    let store = Store::open(&f.0, Config::default()).unwrap();
    assert_eq!(store.epoch, 2);
    assert_eq!(store.get("caller", "key").unwrap().bytes, b"original");
    assert!(
        store.storage_error(),
        "the observed unregistered copy remains reported"
    );
}

#[test]
fn pending_report_directory_failure_preserves_both_torn_inputs_and_epoch() {
    for stop in 1..=3 {
        let f = Fixture::new();
        let store = Store::open(&f.0, Config::default()).unwrap();
        let mutation = Mutation {
            ns: "caller".into(), key: "key".into(), op_id: "original".into(),
            deadline: u64::MAX, ack: 2, expected: None,
            bytes: b"original".to_vec(), remove: false,
        };
        assert_eq!(store.mutate(mutation).code, 0);
        drop(store);
        f.unlock();
        let data = chunks::list_chunk_files(&f.0.join("chunks")).unwrap()[0].3.clone();
        let catalogue = f.0.join("chunks/catalogue.log");
        OpenOptions::new().append(true).open(&data).unwrap().write_all(b"MBK").unwrap();
        OpenOptions::new().append(true).open(&catalogue).unwrap().write_all(b"MBC").unwrap();
        let old_data = fs::read(&data).unwrap();
        let old_catalogue = fs::read(&catalogue).unwrap();
        let old_epoch = fs::read(f.0.join("epoch")).unwrap();
        fs::write(f.0.join("replay.log"), "old history line\n".repeat(70_000)).unwrap();
        let root = f.0.clone();
        let seen = Arc::new(Mutex::new(0));
        let observed = seen.clone();
        let config = Config {
            replay_report_dir_sync: Some(Arc::new(move |dir| {
                assert_eq!(dir, root);
                let json = fs::read_to_string(root.join("replay.json"))?;
                assert!(json.contains("\"stage\":\"truncation_pending\""));
                assert!(json.contains("\"ready_ms\":0"));
                let mut calls = observed.lock().unwrap();
                *calls += 1;
                if *calls == stop { Err(io::Error::from_raw_os_error(5)) } else { Ok(()) }
            })),
            ..Config::default()
        };
        let error = recover(&f.0, &config).err().expect("report directory failure must precede truncation");
        assert_eq!(error.raw_os_error(), Some(5));
        assert_eq!(*seen.lock().unwrap(), stop);
        assert_eq!(fs::read(&data).unwrap(), old_data);
        assert_eq!(fs::read(&catalogue).unwrap(), old_catalogue);
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), old_epoch);
        let store = Store::open(&f.0, Config::default()).unwrap();
        assert_eq!(store.epoch, 2);
        assert_eq!(store.get("caller", "key").unwrap().bytes, b"original");
        assert_eq!(fs::metadata(&data).unwrap().len(), old_data.len() as u64 - 3);
        assert!(store.replay_report().catalogue_truncation.is_some());
    }
}
