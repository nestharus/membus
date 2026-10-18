use super::*;
use std::{fs::File, io::Read};
const MAX_BYTES: u64 = 32 * 1024 * 1024;
fn read(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path).map_err(|e| {
        format!(
            "input {}: {e}",
            path.file_name().unwrap_or_default().to_string_lossy()
        )
    })?;
    if !file.metadata().map_err(|e| e.to_string())?.is_file() {
        return Err("input must be a regular file".into());
    }
    let mut bytes = vec![];
    file.take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err("input exceeds 32 MiB bound".into());
    }
    Ok(bytes)
}
fn utf8(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes.strip_prefix(&[239, 187, 191]).unwrap_or(bytes))
        .map_err(|e| e.to_string())
}
fn parse(bytes: &[u8]) -> Result<Value> {
    serde_json::from_str(utf8(bytes)?).map_err(|e| format!("JSON: {e}"))
}
fn lines(bytes: &[u8]) -> Result<Vec<Value>> {
    let mut out = vec![];
    for line in utf8(bytes)?.lines().filter(|s| !s.trim().is_empty()) {
        if out.len() >= 100_000 {
            return Err("JSONL exceeds 100000 rows".into());
        }
        out.push(
            serde_json::from_str(line).map_err(|e| format!("JSONL row {}: {e}", out.len() + 1))?,
        );
    }
    Ok(out)
}
pub(super) fn load(directory: &Path, host: Option<(&Path, &str)>) -> Result<Inputs> {
    let mut identities = BTreeMap::new();
    let mut values = BTreeMap::new();
    for name in [
        "resident-start.json",
        "resident-end.json",
        "WINDOW-COMPLETE.json",
        "private-small.json",
        "private-big.json",
        "ledger-rates.json",
    ] {
        let bytes = read(&directory.join(name))?;
        identities.insert(
            name.into(),
            json!({"bytes":bytes.len(),"sha256":sha256(&bytes)}),
        );
        values.insert(name, parse(&bytes)?);
    }
    let bytes = read(&directory.join("disk-counters.jsonl"))?;
    identities.insert(
        "disk-counters.jsonl".into(),
        json!({"bytes":bytes.len(),"sha256":sha256(&bytes)}),
    );
    let disk = lines(&bytes)?;
    let host = if let Some((path, expected)) = host {
        if expected.len() != 64 || !expected.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("host SHA256 must have 64 hexadecimal digits".into());
        }
        let bytes = read(path)?;
        let hash = sha256(&bytes);
        if !hash.eq_ignore_ascii_case(expected) {
            return Err("host SHA256 mismatch".into());
        }
        identities.insert(
            "host-supplement".into(),
            json!({"bytes":bytes.len(),"sha256":hash,"historical_read_version":"unestablished"}),
        );
        Some(lines(&bytes)?)
    } else {
        None
    };
    Ok(Inputs {
        start: values.remove("resident-start.json").unwrap(),
        end: values.remove("resident-end.json").unwrap(),
        marker: values.remove("WINDOW-COMPLETE.json").unwrap(),
        small: values.remove("private-small.json").unwrap(),
        big: values.remove("private-big.json").unwrap(),
        rates: values.remove("ledger-rates.json").unwrap(),
        disk,
        identities,
        host,
    })
}
