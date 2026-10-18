use membus_offline_f48::{regenerate, sha256};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
};
const INPUTS: [&str; 7] = [
    "resident-start.json",
    "resident-end.json",
    "WINDOW-COMPLETE.json",
    "disk-counters.jsonl",
    "private-small.json",
    "private-big.json",
    "ledger-rates.json",
];
const PRODUCTS: [&str; 7] = [
    "projection.json",
    "review-supplement.json",
    "metric-retention-scenario.json",
    "host-metrics-supplement.json",
    "PROJECTION.md",
    "REVIEW-SUPPLEMENT.md",
    "METRIC-RETENTION.md",
];
fn run(root: &Path, out: &Path, extra: &[&str]) -> ExitStatus {
    let mut c = Command::new(env!("CARGO_BIN_EXE_membus-offline-f48"));
    c.args([
        "--inputs",
        root.to_str().unwrap(),
        "--change-at",
        "2025-01-01T01:00:00Z",
        "--output",
        out.to_str().unwrap(),
    ])
    .args(extra);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        c.creation_flags(0x08000000);
    }
    c.status().unwrap()
}
fn read(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}
#[test]
fn fresh_cli_products_meaning_preservation_and_negative_controls() {
    let root = PathBuf::from(
        std::env::var_os("MEMBUS_OFFLINE_FIXTURE_ROOT")
            .expect("set MEMBUS_OFFLINE_FIXTURE_ROOT to a fresh directory"),
    );
    fs::create_dir(&root).unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    for name in INPUTS {
        fs::copy(source.join(name), root.join(name)).unwrap();
    }
    let before = INPUTS.map(|name| sha256(&fs::read(root.join(name)).unwrap()));
    let out = root.join("reports");
    assert!(run(&root, &out, &[]).success());
    let expected: BTreeSet<_> = PRODUCTS
        .into_iter()
        .chain(["manifest.json"])
        .map(String::from)
        .collect();
    let actual: BTreeSet<_> = fs::read_dir(&out)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(actual, expected);
    let manifest = read(&out.join("manifest.json"));
    let listed: BTreeSet<_> = manifest["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["role"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(listed, PRODUCTS.map(String::from).into_iter().collect());
    for row in manifest["outputs"].as_array().unwrap() {
        let bytes = fs::read(out.join(row["role"].as_str().unwrap())).unwrap();
        assert_eq!(row["sha256"], sha256(&bytes));
        assert_eq!(row["bytes"], bytes.len());
    }
    for (n, name) in INPUTS.iter().enumerate() {
        assert_eq!(before[n], sha256(&fs::read(root.join(name)).unwrap()));
        assert_eq!(manifest["inputs"][name]["sha256"], before[n]);
    }
    let projection = fs::read_to_string(out.join("PROJECTION.md")).unwrap();
    assert!(projection.contains("growth 1000 bytes, 500.0 bytes/hour, 5 complete records"));
    assert!(projection.contains("| full-window-mixture | 6442450944 | 6442449844 | 500.0 |"));
    assert!(projection.contains("Supplied saved-artifact analysis"));
    let supplement = fs::read_to_string(out.join("REVIEW-SUPPLEMENT.md")).unwrap();
    assert!(supplement.contains("Supplied rate does not exceed the historical comparison value"));
    assert!(supplement.contains("Historical reference only"));
    assert!(!supplement.contains("The discrepancy remains unexplained counterevidence."));
    assert!(!run(&root, &out, &[]).success());
    let host = root.join("host.jsonl");
    let bytes = b"{\"t\":1735689610,\"v\":{\"disk.D.queue\":4,\"disk.D.busy_pct\":900}}\n";
    fs::write(&host, bytes).unwrap();
    let rejected = root.join("unverified-host");
    assert!(!run(&root, &rejected, &["--host", host.to_str().unwrap()]).success());
    assert!(!rejected.exists());
    assert!(regenerate(
        &root,
        "2025-01-01T01:00:00Z",
        &root.join("wrong-host"),
        Some((&host, &"0".repeat(64)))
    )
    .is_err());
    let with_host = root.join("verified-host");
    let hash = sha256(bytes);
    assert!(run(
        &root,
        &with_host,
        &["--host", host.to_str().unwrap(), "--host-sha256", &hash]
    )
    .success());
    let h = read(&with_host.join("host-metrics-supplement.json"));
    assert_eq!(h["rows"][0]["busy_pct"], 900);
    assert_eq!(
        h["status"],
        "supplied-hash-verified-historical-version-unqualified"
    );
    assert_eq!(
        read(&out.join("host-metrics-supplement.json"))["status"],
        "not-supplied"
    );
    let malformed = root.join("malformed");
    fs::create_dir(&malformed).unwrap();
    for name in INPUTS {
        fs::copy(root.join(name), malformed.join(name)).unwrap();
    }
    fs::write(malformed.join("resident-start.json"), b"{malformed").unwrap();
    let bad_output = root.join("bad-output");
    assert!(!run(&malformed, &bad_output, &[]).success());
    assert!(!bad_output.exists());
    assert!(regenerate(
        &root.join("missing"),
        "2025-01-01T01:00:00Z",
        &root.join("missing-output"),
        None
    )
    .is_err());
    // An excess relative to the comparison value is a computed relation, not
    // evidence that the historical owner's policy governs this fixture.
    let excess = root.join("excess");
    fs::create_dir(&excess).unwrap();
    for name in INPUTS {
        fs::copy(root.join(name), excess.join(name)).unwrap();
    }
    let mut end = read(&excess.join("resident-end.json"));
    let mut records = vec![];
    for n in 0..50 {
        records.push(json!({"namespace":"quota","key":"tokens/test","family":"tokens/","offset":n*100,"end":(n+1)*100,"bytes":100,"value_bytes":50,"at_ms":1735693600000u64,"removed":false,"version":50-n}));
    }
    end["logs"]["b"]["records"] = json!(records);
    end["logs"]["b"]["complete_bytes"] = json!(5000);
    end["health"]["quota"]["bytes_written"] = json!(5700);
    fs::write(
        excess.join("resident-end.json"),
        serde_json::to_vec_pretty(&end).unwrap(),
    )
    .unwrap();
    let excess_out = root.join("excess-reports");
    assert!(run(&excess, &excess_out, &[]).success());
    let text = fs::read_to_string(excess_out.join("REVIEW-SUPPLEMENT.md")).unwrap();
    assert!(text.contains("Supplied rate exceeds the historical comparison value"));
    assert!(text.contains("50.0 records/hour"));
    // Reproduce the admitted-scalar overflow witness through the real CLI.
    for row in end["logs"]["b"]["records"].as_array_mut().unwrap() {
        row["value_bytes"] = json!(1u64 << 53);
    }
    fs::write(
        excess.join("resident-end.json"),
        serde_json::to_vec(&end).unwrap(),
    )
    .unwrap();
    let impossible = root.join("impossible-output");
    let status = run(&excess, &impossible, &[]);
    assert_eq!(status.code(), Some(1));
    assert!(!impossible.exists());
}
