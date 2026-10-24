//! Native receiving/staging helper exercised only on explicitly authored private fixture data.
use std::{
    fs,
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
    let mut child = command.args(args)
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
    let p = PathBuf::from(std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()))
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
