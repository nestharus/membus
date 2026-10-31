//! Owned Windows binary checks complement the private resource-factory oracle.
//! Only the package's newly built binary and fresh fixture paths are selected.
//! MEMBUS_CLI_TEST_ROOT selects retained stdout/stderr; no resident is reused.
#![cfg(windows)]
#[path = "support/child.rs"]
mod child;
use std::{
    fs,
    path::PathBuf,
    process::{Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[test]
fn statically_invalid_commands_leave_stdout_and_owned_state_untouched() {
    let base = std::env::var_os("MEMBUS_CLI_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/cli-fixtures"));
    let root = base.join(format!(
        "admission-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    // State is substituted only in server operands. Logs remain beside each
    // fresh state path, so failure diagnostics survive every assertion.
    let cases: &[(&[&str], &str)] = &[
        (
            &["{state}", "bogus", "unused-endpoint", "--heartbeat-ms", "0"],
            "unsupported transport",
        ),
        (
            &["{state}", "tcp", "127.0.0.1:0", "--snapshot-ttl-ms", "0"],
            "snapshot lifetime must be positive",
        ),
        (
            &[
                "{state}",
                "tcp",
                "127.0.0.1:0",
                "--cache-policy",
                "logs,,*,,1,2,true,false",
            ],
            "invalid cache rules",
        ),
        (
            &["{state}", "tcp", "127.0.0.1:0", "--unknown"],
            "missing value",
        ),
        (
            &["{state}", "tcp", "127.0.0.1:0", "--idle-ms", "0"],
            "idle-ms must be",
        ),
        (
            &["{state}", "tcp", "localhost:not-a-port"],
            "invalid port value",
        ),
        (&["{state}", "tcp", "0.0.0.0:0"], "TCP must bind loopback"),
        (&["--help"], "usage: membus STATE_DIR"),
        (&["health"], "usage: membus health PIPE --state-dir PATH"),
        (&["policy"], "usage: membus policy PIPE"),
        (
            &["expiry", "resume"],
            "usage: membus expiry resume STATE_DIR",
        ),
        (
            &["retirement-status"],
            "usage: membus retirement-status PIPE",
        ),
        (&["catalogue-state"], "usage: membus catalogue-state"),
    ];
    for (i, (words, expected)) in cases.iter().enumerate() {
        let case = root.join(i.to_string());
        fs::create_dir(&case).unwrap();
        let state = case.join("never-opened-state");
        let words: Vec<_> = words
            .iter()
            .map(|word| {
                if *word == "{state}" {
                    state.as_os_str().to_owned()
                } else {
                    (*word).into()
                }
            })
            .collect();
        fs::write(case.join("argv.txt"), format!("{words:?}\n")).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_membus"));
        cmd.args(&words)
            .stdin(Stdio::null())
            .stdout(fs::File::create(case.join("stdout")).unwrap())
            .stderr(fs::File::create(case.join("stderr")).unwrap())
            .env_remove("MEMBUS_TEST_IMPORT_DIFFERENCE")
            .env_remove("MEMBUS_TEST_FAIL_AFTER_APPEND")
            .env_remove("MEMBUS_TEST_FAIL_RECOVERY_SYNC");
        let mut owner = child::Owned::spawn(&mut cmd).unwrap();
        let (status, forced) = owner.wait(Duration::from_secs(5)).unwrap();
        fs::write(
            case.join("exit.txt"),
            format!("{status}; forced={forced}\n"),
        )
        .unwrap();
        let stdout = fs::read(case.join("stdout")).unwrap();
        let stderr = fs::read_to_string(case.join("stderr")).unwrap();
        assert!(
            !forced,
            "bounded fixture required termination: {case:?}: {stderr}"
        );
        assert_eq!(status.code(), Some(1), "{case:?}: {stderr}");
        assert!(stdout.is_empty(), "{case:?}: unexpected stdout {stdout:?}");
        assert!(stderr.contains(expected), "{case:?}: {stderr}");
        assert!(!stderr.contains("ready epoch="), "{case:?}: {stderr}");
        assert!(
            !state.try_exists().unwrap(),
            "{case:?}: invalid command opened state"
        );
    }
}
