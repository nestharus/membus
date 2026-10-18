use membus_offline_f48::{calculate, sha256, Inputs};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::PathBuf, process::Command};
const START: f64 = 1735689600.0;
fn row(
    ns: &str,
    key: &str,
    family: &str,
    offset: u64,
    bytes: u64,
    seconds: u64,
    removed: bool,
) -> Value {
    json!({"namespace":ns,"key":key,"family":family,"offset":offset,"end":offset+bytes,"bytes":bytes,"value_bytes":bytes-50,"at_ms":(START as u64+seconds)*1000,"removed":removed,"version":999999-seconds})
}
fn fixture() -> Inputs {
    let old = row("slots", "all", "all", 0, 100, 0, false);
    let start = json!({"unix_time":START,"hello_epoch":1,"binary_sha256":"fixture-image-identity","logs":{"a":{"complete_bytes":100,"records":[old.clone()]},"b":{"complete_bytes":0,"records":[]}},"health":{"quota":{"bytes_written":100,"flush_calls":1,"keys":1,"index_cap":1000,"namespace_keys":0,"namespace_quota":1000},"slots":{"namespace_keys":1,"namespace_quota":100}},"scans":{"slots":{"live_keys":1,"family_counts":{"all":1,"resource":0}},"quota":{"live_keys":0,"family_counts":{"tokens/":0}}},"heartbeat":{"pid":1,"epoch":1,"unix_ms":START*1000.0,"heartbeat_bytes":100}});
    let end = json!({"unix_time":START+7200.0,"hello_epoch":1,"binary_sha256":"fixture-image-identity","logs":{"a":{"complete_bytes":700,"records":[old,row("slots","cpu","resource",100,200,1800,false),row("slots","cpu","resource",300,300,5400,false),row("slots","cpu","resource",600,100,6000,true)]},"b":{"complete_bytes":400,"records":[row("quota","tokens/test","tokens/",0,150,4000,false),row("quota","tokens/test","tokens/",150,250,5000,false)]}},"health":{"quota":{"bytes_written":1100,"flush_calls":4,"keys":3,"index_cap":1000,"namespace_keys":1,"namespace_quota":1000},"slots":{"namespace_keys":2,"namespace_quota":100}},"scans":{"slots":{"live_keys":1,"family_counts":{"all":1,"resource":0}},"quota":{"live_keys":1,"family_counts":{"tokens/":1}}},"heartbeat":{"pid":1,"epoch":1,"unix_ms":(START+7200.0)*1000.0,"heartbeat_bytes":200}});
    let disk = |at: &str, pid: u64, t: u64, bytes: u64, writes: u64, q: u64| json!({"at":at,"collector_pid":pid,"physical_device":"fixture-device","raw":{"Timestamp_PerfTime":t,"Frequency_PerfTime":10,"DiskWriteBytesPersec":bytes,"DiskWritesPersec":writes,"CurrentDiskQueueLength":q}});
    Inputs {
        start,
        end,
        marker: json!({"started_at":"2025-01-01T00:00:00+00:00","completed_at":"2025-01-01T02:00:00+00:00","duration_seconds":7200,"resident_pid":1,"resident_epoch":1,"same_identity":true}),
        disk: vec![
            disk("2025-01-01T00:00:20Z", 1, 200, 3000, 30, 2),
            disk("2025-01-01T00:00:05Z", 2, 50, 9000000, 9000, 99),
            disk("2025-01-01T00:00:00Z", 1, 0, 0, 0, 0),
            disk("2025-01-01T00:00:10Z", 1, 100, 1000, 10, 1),
        ],
        small: json!({"restart":{"total_log_bytes":100,"startup_to_ready_seconds":1}}),
        big: json!({"synthetic":{"bytes":1000},"runs":[{"label":"first","startup_to_ready_seconds":10},{"label":"restart","startup_to_ready_seconds":19}]}),
        rates: json!({"measured_at":"2025-01-01T02:00:00Z","total_rows_last_24h":24,"total_rows_per_hour":1,"publisher_upper_bytes_per_hour":400,"loops":[{"instance":"corrections/fixture","most_recent_at":"2025-01-01T01:59:00Z","rows_last_24h":24,"rows_per_hour":1,"publisher_key_utf8_bytes":98,"assumption_op_id_utf8_bytes":36,"assumption_value_upper_bytes":199,"mbk1_bytes_per_append_upper":400,"publisher_upper_bytes_per_hour":400}]}),
        identities: BTreeMap::new(),
        host: None,
    }
}
fn close(v: &Value, expected: f64) {
    let actual = v.as_f64().unwrap();
    assert!(
        (actual - expected).abs() <= 1e-9 * expected.abs().max(1.0),
        "{actual} != {expected}"
    );
}
#[test]
fn independently_calculated_populations_and_thresholds() {
    let r = calculate(&fixture(), "2025-01-01T01:00:00Z").unwrap();
    let p = &r.projection;
    let s = &r.supplement;
    let m = &r.metric;
    // Five new records: 200 before the split, 300+100+150+250 after.
    assert_eq!(p["records"], 5);
    assert_eq!(p["log_growth_bytes"], 1000);
    close(&p["bytes_per_hour"], 500.0);
    close(&p["publisher_change_eras"][1]["bytes_per_hour"], 800.0);
    assert_eq!(p["namespaces"]["slots"]["new_keys"], 1);
    assert_eq!(p["namespaces"]["slots"]["overwrites"], 1);
    assert_eq!(p["namespaces"]["slots"]["removes"], 1);
    // Reversed opaque versions never reorder stripe records or duplicate-key attribution.
    assert_eq!(s["post_new_keys"], json!({"quota":1}));
    close(&s["token_post_records_per_hour"], 2.0);
    assert_eq!(p["physical_disk"]["primary_collector_pid"], 1);
    assert_eq!(
        p["physical_disk"]["collector_sample_counts"],
        json!({"1":3,"2":1})
    );
    close(&p["physical_disk"]["mean_write_bytes_per_second"], 150.0);
    close(&p["physical_disk"]["sampled_queue_mean"], 1.0);
    close(
        &p["replay_fits"][0]["projected_6GiB_seconds"],
        6.0 * 1073741824.0 / 100.0,
    );
    close(
        &p["replay_fits"][1]["projected_8GiB_seconds"],
        8.0 * 1073741824.0 / 50.0 - 1.0,
    );
    close(&s["key_capacity"][0]["warning_days"], 68.0 / 0.5 / 24.0);
    assert!(s["key_capacity"][2]["cap_days"].is_null());
    close(&m["base_bytes_per_hour"], 1200.0);
    close(
        &m["rows"][0]["headroom_after_base_30days_bytes"],
        6.0 * 1073741824.0 - 1100.0 - 1200.0 * 720.0,
    );
    close(
        &m["key_thresholds"][2]["remaining_keys_after_base_30days"],
        917000.0 * 0.7 - 357.0 - 720.0,
    );
    close(
        &m["rows"][1]["ideal_72h_metric_threshold_bytes_per_hour"],
        (8.0 * 1073741824.0 - 1100.0 - 864000.0) / 72.0,
    );
    assert_eq!(r.host["status"], "not-supplied");
    assert!(m["metric_key_shape"].is_null());
    assert!(m["actual_metric_membus_record_bytes_per_hour"].is_null());
}
#[test]
fn rejects_inconsistent_inputs_and_separates_gaps() {
    let mut i = fixture();
    i.end["logs"]["a"]["records"][1]["offset"] = json!(99);
    assert!(calculate(&i, "2025-01-01T01:00:00Z")
        .unwrap_err_string()
        .contains("offset"));
    let mut i = fixture();
    i.end["hello_epoch"] = json!(2);
    assert!(calculate(&i, "2025-01-01T01:00:00Z").is_err());
    let mut i = fixture();
    i.rates["publisher_upper_bytes_per_hour"] = json!(401);
    assert!(calculate(&i, "2025-01-01T01:00:00Z").is_err());
    let mut i = fixture();
    i.disk[0]["raw"]["DiskWriteBytesPersec"] = json!(1);
    assert!(calculate(&i, "2025-01-01T01:00:00Z").is_err());
    let mut i = fixture();
    i.big["synthetic"]["bytes"] = json!(100);
    assert!(calculate(&i, "2025-01-01T01:00:00Z").is_err());
    let mut i = fixture();
    i.disk[0]["at"] = json!("2025-01-01T00:01:00Z");
    let r = calculate(&i, "2025-01-01T01:00:00Z").unwrap();
    assert_eq!(
        r.projection["physical_disk"]["sampling_gaps"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(calculate(&fixture(), "2025-01-01T00:00:00Z").is_err());
}
trait ErrorString {
    fn unwrap_err_string(self) -> String;
}
impl<T> ErrorString for Result<T, String> {
    fn unwrap_err_string(self) -> String {
        match self {
            Err(e) => e,
            Ok(_) => panic!("expected rejection"),
        }
    }
}
fn root() -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_OFFLINE_FIXTURE_ROOT")
            .expect("set MEMBUS_OFFLINE_FIXTURE_ROOT to a fresh bounded evidence location"),
    );
    fs::create_dir(&base).unwrap();
    base
}
fn write_inputs(root: &std::path::Path, i: &Inputs) {
    for (n, v) in [
        ("resident-start.json", &i.start),
        ("resident-end.json", &i.end),
        ("WINDOW-COMPLETE.json", &i.marker),
        ("private-small.json", &i.small),
        ("private-big.json", &i.big),
        ("ledger-rates.json", &i.rates),
    ] {
        fs::write(root.join(n), serde_json::to_vec_pretty(v).unwrap()).unwrap();
    }
    fs::write(
        root.join("disk-counters.jsonl"),
        i.disk
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .unwrap();
}
#[test]
fn fresh_cli_files_hashes_host_identity_and_negative_controls() {
    let root = root();
    write_inputs(&root, &fixture());
    let exe = env!("CARGO_BIN_EXE_membus-offline-f48");
    let args = [
        "--inputs",
        root.to_str().unwrap(),
        "--change-at",
        "2025-01-01T01:00:00Z",
        "--output",
    ];
    let output = root.join("reports");
    let mut command = Command::new(exe);
    command.args(args).arg(&output);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    assert!(command.status().unwrap().success());
    let manifest: Value =
        serde_json::from_slice(&fs::read(output.join("manifest.json")).unwrap()).unwrap();
    for row in manifest["outputs"].as_array().unwrap() {
        let bytes = fs::read(output.join(row["role"].as_str().unwrap())).unwrap();
        assert_eq!(row["sha256"], sha256(&bytes));
    }
    assert_eq!(
        manifest["inputs"]["resident-start.json"]["sha256"],
        sha256(&fs::read(root.join("resident-start.json")).unwrap())
    );
    let mut command = Command::new(exe);
    command.args(args).arg(&output);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    assert!(!command.status().unwrap().success()); // Existing output is never overwritten.
    let host = root.join("host.jsonl");
    let bytes = b"{\"t\":1735689610,\"v\":{\"disk.D.queue\":4,\"disk.D.busy_pct\":900}}\n";
    fs::write(&host, bytes).unwrap();
    assert!(Inputs::load(&root, Some((&host, &"0".repeat(64)))).is_err());
    let supplied = Inputs::load(&root, Some((&host, &sha256(bytes)))).unwrap();
    let r = calculate(&supplied, "2025-01-01T01:00:00Z").unwrap();
    assert_eq!(r.host["rows"][0]["busy_pct"], 900);
    assert_eq!(
        r.host["status"],
        "supplied-hash-verified-historical-version-unqualified"
    );
    let malformed = root.join("malformed");
    fs::create_dir(&malformed).unwrap();
    write_inputs(&malformed, &fixture());
    fs::write(malformed.join("resident-start.json"), b"{malformed").unwrap();
    assert!(Inputs::load(&malformed, None).is_err());
    assert!(Inputs::load(&root.join("missing"), None).is_err());
}
