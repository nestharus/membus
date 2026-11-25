//! Native receiving/staging helper exercised only on explicitly authored private fixture data.
use std::{
    fs,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
#[path = "../tools/catalogue_receiver.rs"]
mod receiver;
fn field<'a>(json: &'a str, name: &str) -> &'a str {
    let rest = json.split_once(&format!("\"{name}\":")).unwrap().1;
    rest.split([',', '}'])
        .next()
        .unwrap()
        .trim()
        .trim_matches('"')
}
fn assert_snapshot(receipt: &str, cat: &std::path::Path) {
    let n: usize = field(receipt, "decoded_records").parse().unwrap();
    let bytes = fs::read(cat).unwrap();
    let all = receiver::decode(&bytes).unwrap();
    let mut end = 0;
    for _ in 0..n {
        let len = u32::from_le_bytes(bytes[end + 4..end + 8].try_into().unwrap()) as usize;
        end += 16 + len;
    }
    let prefix = &bytes[..end];
    assert_eq!(receiver::decode(prefix).unwrap().len(), n);
    let mut hash = membus::chunks::Sha256::new();
    hash.update(prefix);
    assert_eq!(
        field(receipt, "catalogue_sha256"),
        hash.hex(),
        "catalogue receipt hash is not the bytes its count decoded"
    );
    assert_eq!(field(receipt, "catalogue_phase"), "before_orderly_reopen");
    println!("catalogue correspondence: decoded before reopen={n}, observed after reopen={}, prefix_bytes={end}, hash={}",
        all.len(), field(receipt, "catalogue_sha256"));
}
fn invoke(args: &[&std::ffi::OsStr], output: &std::path::Path) -> bool {
    let stdout = fs::File::create(output).unwrap();
    let stderr = fs::File::create(output.with_extension("stderr")).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_storage-receive"));
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = command
        .args(args)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            fs::write(
                output.with_extension("exit"),
                format!("{}\n", status.code().unwrap_or(-1)),
            )
            .unwrap();
            return status.success();
        }
        if Instant::now() >= until {
            let _ = child.kill();
            let _ = child.wait();
            panic!("private receiving child timed out");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
#[test]
fn native_stage_receive_exact_value_version_reopen_and_refusal() {
    use std::ffi::OsStr;
    let p = PathBuf::from(
        std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
    )
    .join(format!(
        "native-receiving-{}-{}",
        std::process::id(),
        membus::now_ms()
    ));
    fs::create_dir_all(&p).unwrap();
    let state = p.join("state");
    let input = p.join("input.txt");
    let bytes = b"Authored qualification result: durable bytes, selected policy, orderly reopen.\n";
    fs::write(&input, bytes).unwrap();
    let head = option_env!("MEMBUS_BUILD_HEAD").unwrap_or("unrecorded");
    let stage = p.join("stage.json");
    assert!(invoke(
        &[
            OsStr::new("stage"),
            state.as_os_str(),
            OsStr::new("qualification"),
            OsStr::new("result"),
            input.as_os_str(),
            OsStr::new(head)
        ],
        &stage
    ));
    let receipt = fs::read_to_string(&stage).unwrap();
    println!("native staging received: {receipt}");
    let version = field(&receipt, "version").to_owned();
    assert_eq!(field(&receipt, "ack"), "2");
    assert_eq!(
        field(&receipt, "policy_scope"),
        "singleton_namespace_latest_prefix"
    );
    assert_eq!(field(&receipt, "policy_prefix"), "result");
    assert_snapshot(&receipt, &state.join("chunks/catalogue.log"));
    assert_eq!(field(&receipt, "decoded_records"), "2"); // Two Opens; reopen legitimately adds Close.
    assert_eq!(field(&receipt, "placement_class"), "latest");
    assert_eq!(field(&receipt, "placement_state"), "Open");
    let read = p.join("receive.json");
    assert!(invoke(
        &[
            OsStr::new("receive"),
            state.as_os_str(),
            OsStr::new("qualification"),
            OsStr::new("result"),
            input.as_os_str(),
            OsStr::new(head),
            OsStr::new(&version)
        ],
        &read
    ));
    let received = fs::read_to_string(&read).unwrap();
    assert_snapshot(&received, &state.join("chunks/catalogue.log"));
    assert_eq!(field(&received, "decoded_records"), "4");
    assert_eq!(field(&received, "placement_state"), "Closed");
    for name in [
        "placement_sequence",
        "placement_stripe",
        "placement_offset",
        "placement_frame_bytes",
    ] {
        assert_eq!(field(&received, name), field(&receipt, name));
    }
    assert_eq!(field(&received, "value_hex"), field(&receipt, "value_hex"));
    assert_eq!(
        field(&received, "input_sha256"),
        membus::chunks::sha256_file(&input).unwrap()
    );
    assert_eq!(field(&received, "ack_reply_hex"), "");
    println!(
        "native independent process receive: {}",
        fs::read_to_string(read).unwrap()
    );
    assert!(!invoke(
        &[
            OsStr::new("stage"),
            state.as_os_str(),
            OsStr::new("qualification"),
            OsStr::new("result"),
            input.as_os_str(),
            OsStr::new(head)
        ],
        &p.join("refuse-reuse.json")
    ));
    assert!(!invoke(
        &[
            OsStr::new("receive"),
            state.as_os_str(),
            OsStr::new("qualification"),
            OsStr::new("result"),
            input.as_os_str(),
            OsStr::new("wrong-source"),
            OsStr::new(&version)
        ],
        &p.join("refuse-source.json")
    ));
    let mut corrupted = bytes.to_vec();
    corrupted[0] ^= 1;
    let wrong_input = p.join("wrong-input.txt");
    fs::write(&wrong_input, corrupted).unwrap();
    assert!(!invoke(
        &[
            OsStr::new("receive"),
            state.as_os_str(),
            OsStr::new("qualification"),
            OsStr::new("result"),
            wrong_input.as_os_str(),
            OsStr::new(head),
            OsStr::new(&version)
        ],
        &p.join("refuse-value.json")
    ));
    // KEY really supplies a prefix: a neighboring key selects the same policy. A separate
    // unrelated row is legal Store state but outside this helper's singleton namespace shape.
    let s = membus::Store::open(
        &state,
        membus::Config {
            flush_ms: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let previous_seq = receiver::read(&state.join("chunks/catalogue.log"))
        .unwrap()
        .iter()
        .filter_map(|r| match r.rec {
            membus::chunks::Rec::Open { seq, .. } => Some(seq),
            _ => None,
        })
        .max()
        .unwrap();
    let neighbor = s.mutate(membus::Mutation {
        ns: "qualification".into(),
        key: "result-neighbor".into(),
        op_id: "prefix-neighbor".into(),
        deadline: membus::now_ms() + 30_000,
        ack: 2,
        expected: None,
        bytes: b"neighbor follows prefix".to_vec(),
        remove: false,
    });
    assert_eq!((neighbor.code, neighbor.ack), (0, 2));
    assert!(receiver::read(&state.join("chunks/catalogue.log")).unwrap().iter().any(|r|
        matches!(&r.rec, membus::chunks::Rec::Open { seq, class, policy, .. }
            if *seq > previous_seq && *class == membus::Class::LATEST && policy == "qualification/result")));
    assert_eq!(
        s.get("qualification", "result-neighbor").unwrap().bytes,
        b"neighbor follows prefix"
    );
    assert_eq!(
        s.set_policy(
            "qualification",
            "unrelated/",
            Some(membus::Class::FOREVER),
            membus::now_ms() + 30_000
        )
        .code,
        0
    );
    drop(s);
    assert!(!invoke(
        &[
            OsStr::new("receive"),
            state.as_os_str(),
            OsStr::new("qualification"),
            OsStr::new("result"),
            input.as_os_str(),
            OsStr::new(head),
            OsStr::new(&version)
        ],
        &p.join("refuse-unrelated-policy.json")
    ));
    let refusal = fs::read_to_string(p.join("refuse-unrelated-policy.stderr")).unwrap();
    assert!(refusal.contains("only the selected Latest prefix row"));
    println!("prefix neighbor ack={neighbor:?}; unrelated-row refusal={refusal}");
    assert_eq!(fs::read(input).unwrap(), bytes);
    fs::remove_dir_all(&p).unwrap();
}

#[test]
fn native_existing_state_checks_the_received_frame_even_with_other_latest_frames() {
    use membus::{
        atomic::{AtomicAck, FinalChange},
        chunks::{self, Rec},
        Class, Config, Mutation, Store,
    };
    use std::ffi::OsStr;

    let root = PathBuf::from(
        std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
    )
    .join(format!(
        "receiving-placement-{}-{}",
        std::process::id(),
        membus::now_ms()
    ));
    fs::create_dir_all(&root).unwrap();
    let input = root.join("input");
    let expected = b"selected";
    fs::write(&input, expected).unwrap();
    let config = Config {
        flush_ms: 0,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        ..Config::default()
    };
    let ns = "authored";
    let key = "builtin/value";
    let stripe = |namespace: &str, key: &str| {
        membus::crc32(&[namespace.as_bytes(), &[0], key.as_bytes()].concat()) as usize % 4
    };
    let selected_stripe = stripe(ns, key);
    let other = (0..100)
        .map(|n| format!("other/{n}"))
        .find(|key| stripe("other", key) == selected_stripe)
        .unwrap();
    let put = |store: &Store, ns: &str, key: &str, id: &str, bytes: &[u8]| {
        let result = store.mutate(Mutation {
            ns: ns.into(),
            key: key.into(),
            op_id: id.into(),
            deadline: u64::MAX,
            ack: 2,
            expected: None,
            bytes: bytes.to_vec(),
            remove: false,
        });
        assert_eq!((result.code, result.ack), (0, 2));
        result.version
    };
    let latest = |store: &Store, ns: &str, prefix: &str| {
        let result = store.set_policy(ns, prefix, Some(Class::LATEST), u64::MAX);
        assert_eq!((result.code, result.ack), (0, 2));
    };
    let head = option_env!("MEMBUS_BUILD_HEAD").unwrap_or("unrecorded");
    for case in [
        "shared",
        "before-policy",
        "duplicate-frame",
        "duplicate-chunk",
        "atomic",
    ] {
        let state = root.join(case);
        let store = Store::open(&state, config.clone()).unwrap();
        let revision = match case {
            "shared" | "duplicate-frame" | "duplicate-chunk" => {
                latest(&store, "other", "other/");
                put(&store, "other", &other, "neighbor", b"other");
                latest(&store, ns, key);
                put(&store, ns, key, "target", expected)
            }
            "atomic" => {
                let request = store
                    .capture_namespace(ns)
                    .unwrap()
                    .prepare(
                        "atomic-target",
                        AtomicAck::Flush,
                        vec![
                            FinalChange::put("earlier", b"other".to_vec()),
                            FinalChange::put(key, expected.to_vec()),
                        ],
                    )
                    .unwrap();
                let outcome = store.publish_atomic(&request, u64::MAX).unwrap();
                assert_eq!((outcome.code, outcome.ack), (0, 2));
                latest(&store, ns, key);
                put(&store, ns, &format!("{key}/later"), "later", b"later");
                outcome.version + 1
            }
            _ => {
                let revision = put(&store, ns, key, "target", expected);
                latest(&store, ns, key);
                put(&store, ns, &format!("{key}/later"), "later", b"later");
                revision
            }
        };
        let value = store.get(ns, key).unwrap();
        assert_eq!(
            (value.version, value.ack, value.bytes.as_slice()),
            (revision, 2, expected.as_slice())
        );
        drop(store);

        let dir = state.join("chunks");
        let catalogue = dir.join("catalogue.log");
        if case.starts_with("duplicate") {
            let mut files = chunks::list_chunk_files(&dir)
                .unwrap()
                .into_iter()
                .filter(|(class, _, _, _)| class == "latest");
            let (_, stripe, _, path) = files.next().unwrap();
            assert!(files.next().is_none());
            let bytes = fs::read(&path).unwrap();
            let first_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize + 16;
            let target_frame = &bytes[first_len..];
            if case == "duplicate-frame" {
                fs::OpenOptions::new()
                    .append(true)
                    .open(path)
                    .unwrap()
                    .write_all(target_frame)
                    .unwrap();
            } else {
                // A valid competing copy in Forever must count before any Latest filter.
                let seq = 100;
                let mut writer = chunks::Catalogue::open(&catalogue).unwrap();
                writer
                    .append(&chunks::CatRecord {
                        at_ms: membus::now_ms(),
                        rec: Rec::Open {
                            seq,
                            stripe: stripe as u8,
                            class: Class::FOREVER,
                            policy: "(default)".into(),
                            next: revision + 1,
                        },
                    })
                    .unwrap();
                fs::write(
                    chunks::chunk_path(&dir, Class::FOREVER, stripe, seq),
                    target_frame,
                )
                .unwrap();
            }
        }

        let original_frames: Vec<_> = chunks::list_chunk_files(&dir).unwrap().into_iter()
            .map(|(_, _, _, path)| {
                let bytes = fs::read(&path).unwrap();
                (path, bytes)
            }).collect();
        let output = root.join(format!("{case}.json"));
        let snapshot = root.join(format!("{case}.cat"));
        let ok = invoke(
            &[
                OsStr::new("receive"),
                state.as_os_str(),
                OsStr::new(ns),
                OsStr::new(key),
                input.as_os_str(),
                OsStr::new(head),
                OsStr::new(&revision.to_string()),
                OsStr::new("--catalogue-snapshot"),
                snapshot.as_os_str(),
            ],
            &output,
        );
        let receipt = fs::read_to_string(&output).unwrap();
        if case == "shared" {
            assert!(
                ok,
                "{}",
                fs::read_to_string(output.with_extension("stderr")).unwrap()
            );
            assert_eq!(field(&receipt, "version"), revision.to_string());
            assert_eq!(field(&receipt, "placement_class"), "latest");
            assert_eq!(field(&receipt, "placement_state"), "Closed");
            assert_eq!(
                field(&receipt, "received_reply_hex"),
                field(&receipt, "reopened_reply_hex")
            );
            assert_eq!(field(&receipt, "ack_reply_hex"), "");
            assert_snapshot(&receipt, &snapshot);
            let seq = field(&receipt, "placement_sequence").parse().unwrap();
            let offset = field(&receipt, "placement_offset").parse().unwrap();
            assert!(offset > 0);
            assert_eq!(
                field(&receipt, "placement_stripe"),
                selected_stripe.to_string()
            );
            assert!(receiver::read(&snapshot)
                .unwrap()
                .iter()
                .any(|r| matches!(&r.rec,
                Rec::Open { seq: id, stripe, class, policy, .. }
                    if *id == seq && usize::from(*stripe) == selected_stripe &&
                        *class == Class::LATEST && policy == "other/other/")));
            let path = chunks::chunk_path(&dir, Class::LATEST, selected_stripe, seq);
            let mut file = fs::File::open(path).unwrap();
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(offset)).unwrap();
            let len = file.metadata().unwrap().len();
            let chunks::Frame::Whole(payload, bytes) =
                chunks::read_frame_at(&mut file, b"MBK1", offset, len, "shared data").unwrap()
            else {
                panic!("expected the selected frame");
            };
            assert_eq!(field(&receipt, "placement_frame_bytes"), bytes.to_string());
            assert_eq!(
                chunks::record_key(&payload),
                Some((ns.into(), key.into(), revision))
            );
            println!("{case}: {receipt}");
        } else {
            assert!(!ok, "{case}: {receipt}");
            assert!(receipt.is_empty());
            assert!(!snapshot.exists());
            let refusal = fs::read_to_string(output.with_extension("stderr")).unwrap();
            let reason = if case.starts_with("duplicate") {
                "multiple durable records".to_owned()
            } else {
                format!("received revision {revision} is physically forever")
            };
            assert!(refusal.contains(&reason), "{case}: {refusal}");
            println!("{case}: {refusal}");
        }
        for (path, bytes) in original_frames {
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn catalogue_receiver_rejects_frame_over_16_mib() {
    let len = (16 * 1024 * 1024 + 1) as u32;
    let mut frame = Vec::with_capacity(16 + len as usize);
    frame.extend_from_slice(b"MBC1");
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&(!len).to_le_bytes());
    frame.extend_from_slice(&0u32.to_le_bytes());
    frame.resize(16 + len as usize, 0);
    let err = receiver::decode(&frame).unwrap_err();
    assert!(
        err.to_string().contains("receiving frame limit"),
        "unexpected error: {err}"
    );
}
