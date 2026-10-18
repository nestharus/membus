use super::*;
use std::collections::BTreeMap;
pub(super) fn calculate(i: &Inputs, p: &Value, change: f64) -> Result<Value> {
    let mut prior = project::prior(&i.start)?;
    let tail = project::tail(i)?;
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for row in &tail {
        let ns = text(row, "namespace")?;
        let new = prior.insert((ns.into(), text(row, "key")?.into()));
        if new && uint(row, "at_ms")? as f64 / 1000.0 >= change {
            *counts.entry(ns.into()).or_default() += 1;
        }
    }
    let eras = array(field(p, "publisher_change_eras")?)?;
    let post_hours = num(&eras[1], "hours")?;
    let a = field(&i.start, "health")?;
    let b = field(&i.end, "health")?;
    let a_quota = field(a, "quota")?;
    let b_quota = field(b, "quota")?;
    let a_slots = field(a, "slots")?;
    let b_slots = field(b, "slots")?;
    let mut warnings = vec![];
    for (basis, hours, global, slots) in [
        (
            "full-window",
            num(p, "hours")?,
            num(b_quota, "keys")? - num(a_quota, "keys")?,
            num(b_slots, "namespace_keys")? - num(a_slots, "namespace_keys")?,
        ),
        (
            "post-change",
            post_hours,
            counts.values().sum::<u64>() as f64,
            *counts.get("slots").unwrap_or(&0) as f64,
        ),
    ] {
        for (scope, current, n, cap) in [
            (
                "slots-namespace",
                num(b_slots, "namespace_keys")?,
                slots,
                num(b_slots, "namespace_quota")?,
            ),
            (
                "global-index",
                num(b_quota, "keys")?,
                global,
                num(b_quota, "index_cap")?,
            ),
        ] {
            if n < 0.0 || cap <= 0.0 || current < 0.0 {
                return Err("invalid key capacity population".into());
            }
            let rate = n / hours;
            warnings.push(json!({"basis":basis,"scope":scope,"current_keys":current,"new_keys_per_hour":rate,"warning_keys":0.7*cap,"warning_days":if rate>0.0{Some((0.7*cap-current)/rate/24.0)}else{None},"cap_keys":cap,"cap_days":if rate>0.0{Some((cap-current)/rate/24.0)}else{None}}));
        }
    }
    let tokens = tail
        .iter()
        .filter(|r| {
            r["namespace"] == "quota"
                && r["family"] == "tokens/"
                && r["at_ms"].as_f64().is_some_and(|n| n / 1000.0 >= change)
        })
        .collect::<Vec<_>>();
    let anchor = stamp(text(&i.rates, "measured_at")?)?;
    let loops = array(field(&i.rates, "loops")?)?;
    let mut selected = vec![];
    let mut corrections = vec![];
    let mut all_bytes = 0.0;
    let mut all_rows = 0.0;
    for row in loops {
        let rows = uint(row, "rows_last_24h")?;
        let rate = num(row, "rows_per_hour")?;
        let bytes = num(row, "publisher_upper_bytes_per_hour")?;
        let record = 58
            + 9
            + uint(row, "publisher_key_utf8_bytes")?
            + uint(row, "assumption_op_id_utf8_bytes")?
            + uint(row, "assumption_value_upper_bytes")?;
        if uint(row, "assumption_op_id_utf8_bytes")? != 36
            || uint(row, "assumption_value_upper_bytes")? != 199
            || uint(row, "mbk1_bytes_per_append_upper")? != record
            || (rate - rows as f64 / 24.0).abs() > 1e-8
            || (bytes - rate * record as f64).abs() > 1e-6
        {
            return Err("publisher shape/rate summary mismatch".into());
        }
        all_bytes += bytes;
        all_rows += rows as f64;
        if stamp(text(row, "most_recent_at")?)? >= anchor - 3600.0 {
            selected.push(row)
        }
        if text(row, "instance")?.contains("corrections") {
            corrections.push(row)
        }
    }
    if (num(&i.rates, "total_rows_last_24h")? - all_rows).abs() > 0.01
        || (num(&i.rates, "total_rows_per_hour")? - all_rows / 24.0).abs() > 1e-8
        || (num(&i.rates, "publisher_upper_bytes_per_hour")? - all_bytes).abs() > 1e-6
    {
        return Err("publisher aggregate does not reconcile".into());
    }
    let subset = |rows: &Vec<&Value>| -> Result<Value> {
        let mut bytes = 0.0;
        let mut count = 0;
        for row in rows {
            bytes += num(row, "publisher_upper_bytes_per_hour")?;
            count += uint(row, "rows_last_24h")?;
        }
        Ok(json!({"ledgers":rows.len(),"rows_last_24h":count,"bytes_per_hour":bytes}))
    };
    let mut active = subset(&selected)?;
    let bytes = num(&active, "bytes_per_hour")?;
    let base = num(&eras[1], "bytes_per_hour")?;
    active["combined_share"] = if bytes + base > 0.0 {
        json!(bytes / (bytes + base))
    } else {
        Value::Null
    };
    let targets = [6u64 * 1024u64.pow(3), 8u64 * 1024u64.pow(3)];
    let current = num(p, "complete_log_bytes_end")?;
    let thresholds = targets
        .into_iter()
        .map(|t| (t.to_string(), json!((t as f64 - current) / 720.0)))
        .collect::<serde_json::Map<_, _>>();
    let disk = field(p, "physical_disk")?;
    let window = field(p, "window")?;
    Ok(
        json!({"token_post_records":tokens.len(),"token_post_records_per_hour":tokens.len() as f64/post_hours,"token_post_bytes":tokens.iter().map(|r|uint(r,"bytes")).collect::<Result<Vec<_>>>()?.iter().sum::<u64>(),"owner_stated_token_max_per_hour":42,
        "needed_total_bytes_per_hour_for_30days":thresholds,"key_capacity":warnings,"post_new_keys":counts,"publisher_active_last_hour_subset":active,"publisher_corrections_subset":subset(&corrections)?,
        "disk_start_offset_seconds":stamp(text(disk,"first")?)?-stamp(text(window,"started_at")?)?,"disk_end_offset_seconds":stamp(text(disk,"last")?)?-stamp(text(window,"completed_at")?)?,"ledger_read_anchor":text(&i.rates,"measured_at")?,
        "limits":"Key dates assume constant new-key rates and no reclamation/cap changes. Zero growth produces null dates; negative dates mean already beyond a threshold. Publisher last-hour subset is an activity proxy at the saved ledger-read anchor, not adoption. Historical observed token rate versus the stated 42/hour bound remains counterevidence; 10% movements are a possible explanation, cause unconfirmed. Missing file-write/sync-attempt counters remain missing. Synthetic at_ms=1 excludes retry fingerprints; warm readiness and different retained-key shapes do not establish 6/8 GiB bootability. Historical latest-version/no-time-expiry policy and whole-chunk expiry were distinct; this arithmetic establishes no current implementation or operating policy."}),
    )
}
