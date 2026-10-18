//! Offline arithmetic over saved measurement artifacts. No capture or resident API.
mod input;
mod metric;
mod project;
mod render;
mod supplement;
#[cfg(test)]
mod tests;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, io::Write, path::Path};

pub type Result<T> = std::result::Result<T, String>;
pub(crate) fn finite(value: f64, context: &str) -> Result<f64> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(format!("nonfinite calculation: {context}"))
    }
}
pub(crate) fn add(total: &mut u64, value: u64, context: &str) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| format!("integer overflow: {context}"))?;
    Ok(())
}
pub(crate) fn checked_sum(values: impl IntoIterator<Item = u64>, context: &str) -> Result<u64> {
    let mut total = 0;
    for value in values {
        add(&mut total, value, context)?;
    }
    Ok(total)
}
pub(crate) fn object(v: &Value) -> Result<&serde_json::Map<String, Value>> {
    v.as_object().ok_or_else(|| "expected object".into())
}
pub(crate) fn array(v: &Value) -> Result<&Vec<Value>> {
    v.as_array().ok_or_else(|| "expected array".into())
}
pub(crate) fn field<'a>(v: &'a Value, key: &str) -> Result<&'a Value> {
    v.get(key).ok_or_else(|| format!("missing field {key}"))
}
pub(crate) fn number(v: &Value) -> Result<f64> {
    v.as_f64()
        .filter(|n| n.is_finite())
        .ok_or_else(|| "expected finite number".into())
}
pub(crate) fn num(v: &Value, key: &str) -> Result<f64> {
    number(field(v, key)?)
}
pub(crate) fn uint(v: &Value, key: &str) -> Result<u64> {
    field(v, key)?
        .as_u64()
        .filter(|n| *n <= (1u64 << 53))
        .ok_or_else(|| format!("{key}: expected exact nonnegative integer <= 2^53"))
}
pub(crate) fn string(v: &Value) -> Result<&str> {
    v.as_str().ok_or_else(|| "expected string".into())
}
pub(crate) fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    string(field(v, key)?)
}
pub(crate) fn stamp(s: &str) -> Result<f64> {
    let t = DateTime::parse_from_rfc3339(s).map_err(|e| format!("timestamp: {e}"))?;
    // The original Python datetime parser retained microseconds (truncating finer
    // JSONL fractions). Preserve that arithmetic boundary for correspondence.
    Ok(t.timestamp() as f64 + t.timestamp_subsec_micros() as f64 / 1e6)
}
pub(crate) fn utc(t: f64) -> Result<String> {
    if !t.is_finite() || t.abs() > 1e11 {
        return Err("timestamp out of range".into());
    }
    let micros = (t * 1e6).round() as i64;
    DateTime::<Utc>::from_timestamp_micros(micros)
        .map(|d| d.to_rfc3339_opts(SecondsFormat::Micros, false))
        .ok_or_else(|| "timestamp out of range".into())
}
pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// All required files are selected by the supplied directory; host is opt-in.
struct Inputs {
    start: Value,
    end: Value,
    marker: Value,
    disk: Vec<Value>,
    small: Value,
    big: Value,
    rates: Value,
    identities: BTreeMap<String, Value>,
    host: Option<Vec<Value>>,
}
struct Reports {
    projection: Value,
    supplement: Value,
    metric: Value,
    host: Value,
    manifest: Value,
}

/// Regenerate from files through the verified loader. In-memory input/report
/// construction is internal; callers cannot replace verified data or identities.
pub fn regenerate(
    directory: &Path,
    change_at: &str,
    output: &Path,
    host: Option<(&Path, &str)>,
) -> Result<()> {
    calculate(&Inputs::load(directory, host)?, change_at)?.write_fresh(output)
}

impl Inputs {
    fn load(directory: &Path, host: Option<(&Path, &str)>) -> Result<Self> {
        input::load(directory, host)
    }
}
fn calculate(inputs: &Inputs, change_at: &str) -> Result<Reports> {
    let change = stamp(change_at)?;
    let p = project::calculate(inputs, change)?;
    let s = supplement::calculate(inputs, &p, change)?;
    let m = metric::calculate(&p, &s)?;
    let first = stamp(text(field(&p, "physical_disk")?, "first")?)?;
    let last = stamp(text(field(&p, "physical_disk")?, "last")?)?;
    let mut rows = vec![];
    if let Some(host) = &inputs.host {
        for row in host {
            let t = num(row, "t")?;
            if first <= t && t <= last {
                let v = field(row, "v")?;
                object(v)?;
                rows.push(json!({"at":utc(t)?,"queue":v.get("disk.D.queue"),"busy_pct":v.get("disk.D.busy_pct")}));
            }
        }
    }
    let host = json!({"status":if inputs.host.is_some(){"supplied-hash-verified-historical-version-unqualified"}else{"not-supplied"},"rows":rows,
        "limits":"Optional host bytes are explicitly identified. Their hash does not establish the version read in the historical experiment. Queue is instantaneous; busy_pct scaling is unverified. Core arithmetic is independent of this supplement."});
    Ok(Reports {
        projection: p,
        supplement: s,
        metric: m,
        host,
        manifest: json!({"format":"offline-f48/v1","inputs":inputs.identities,"change_at":change_at,
        "calculation_order":["project","review_supplement","metric_scenario"],"meaning":"Numerical regeneration from supplied artifacts, not acquisition, capture reconstruction, current qualification or operating adoption."}),
    })
}

impl Reports {
    /// Refuses an existing output location. Inputs are never opened for writing.
    fn write_fresh(&self, output: &Path) -> Result<()> {
        let mut products = vec![];
        for (name, value) in [
            ("projection.json", &self.projection),
            ("review-supplement.json", &self.supplement),
            ("metric-retention-scenario.json", &self.metric),
            ("host-metrics-supplement.json", &self.host),
        ] {
            let mut bytes = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
            bytes.push(b'\n');
            products.push((name, bytes));
        }
        products.extend(
            render::reports(self)?
                .into_iter()
                .map(|(n, s)| (n, s.into_bytes())),
        );
        let mut manifest = self.manifest.clone();
        manifest["outputs"] = json!(products
            .iter()
            .map(|(name, bytes)| json!({"role":name,"bytes":bytes.len(),"sha256":sha256(bytes)}))
            .collect::<Vec<_>>());
        let mut bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        products.push(("manifest.json", bytes));
        fs::create_dir(output).map_err(|e| format!("fresh output directory: {e}"))?;
        for (name, bytes) in products {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(output.join(name))
                .map_err(|e| e.to_string())?;
            f.write_all(&bytes)
                .and_then(|_| f.sync_all())
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
