use membus_offline_f48::{calculate, Inputs, Result};
use std::path::Path;
fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut options = std::collections::BTreeMap::new();
    while let Some(key) = args.next() {
        if key == "--help" {
            println!("membus-offline-f48 --inputs DIRECTORY --change-at RFC3339 --output NEW_DIRECTORY [--host JSONL --host-sha256 HEX]\nReads saved files only. See README.md for input schema and historical limits.");
            return Ok(());
        }
        if ![
            "--inputs",
            "--change-at",
            "--output",
            "--host",
            "--host-sha256",
        ]
        .contains(&key.as_str())
        {
            return Err(format!("unknown option {key}"));
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {key}"))?;
        if options.insert(key, value).is_some() {
            return Err("duplicate option".into());
        }
    }
    let required = |key: &str| {
        options
            .get(key)
            .ok_or_else(|| format!("required {key}; use --help"))
    };
    let directory = required("--inputs")?;
    let change = required("--change-at")?;
    let output = required("--output")?;
    let host = match (options.get("--host"), options.get("--host-sha256")) {
        (Some(p), Some(h)) => Some((Path::new(p), h.as_str())),
        (None, None) => None,
        _ => return Err("--host and --host-sha256 must be supplied together".into()),
    };
    let inputs = Inputs::load(Path::new(directory), host)?;
    let reports = calculate(&inputs, change)?;
    reports.write_fresh(Path::new(output))?;
    println!("Offline numerical reports and input/output hashes written; host historical read-version remains unqualified.");
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("offline-f48: {e}");
        std::process::exit(1)
    }
}
