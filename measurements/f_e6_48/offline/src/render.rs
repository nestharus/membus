use super::*;
fn cell(v: &Value) -> String {
    match v {
        Value::String(s) => s.replace('|', "\\|").replace(['\r', '\n'], " "),
        Value::Null => "unknown / not reached".into(),
        _ => v.to_string(),
    }
}
fn table(v: &Value, cols: &[&str]) -> Result<String> {
    let mut s = format!(
        "| {} |\n| {} |\n",
        cols.join(" | "),
        vec!["---"; cols.len()].join(" | ")
    );
    for row in array(v)? {
        s.push_str(&format!(
            "| {} |\n",
            cols.iter()
                .map(|c| cell(&row[*c]))
                .collect::<Vec<_>>()
                .join(" | ")
        ));
    }
    Ok(s)
}
pub(super) fn reports(r: &Reports) -> Result<Vec<(&'static str, String)>> {
    let p = &r.projection;
    let s = &r.supplement;
    let m = &r.metric;
    let pre="Supplied saved-artifact analysis using the F48 historical calculation model. Historical reference only: the original experiment was conducted on 2026-10-10 at baseline 11c312c. That context does not identify another supplied fixture or establish its capture gap, policy or operating behavior. Original reference producers/results remain retained. This report does not reconstruct capture, qualify a current resident or select operating policy. See manifest.json for exact supplied input/output hashes.\n\n";
    let mut projection=format!("# Offline growth and projection\n\n{pre}Window: {} to {}. Endpoint hours: {}. Complete log bytes: {} → {}; growth {} bytes, {} bytes/hour, {} complete records. Health byte difference: {} bytes. Health flush delta: {}.\n\n",cell(&p["window"]["started_at"]),cell(&p["window"]["completed_at"]),cell(&p["hours"]),cell(&p["complete_log_bytes_start"]),cell(&p["complete_log_bytes_end"]),cell(&p["log_growth_bytes"]),cell(&p["bytes_per_hour"]),cell(&p["records"]),cell(&p["health_vs_log_difference_bytes"]),cell(&p["health_flush_delta"]));
    let ns = object(&p["namespaces"])?
        .iter()
        .map(|(ns, row)| {
            let mut row = row.clone();
            row["namespace"] = json!(ns);
            row
        })
        .collect::<Vec<_>>();
    projection.push_str(&table(
        &json!(ns),
        &[
            "namespace",
            "bytes",
            "bytes_per_hour",
            "records",
            "new_keys",
            "overwrites",
            "removes",
            "quota_slots_start",
            "quota_slots_end",
            "live_keys_start",
            "live_keys_end",
        ],
    )?);
    projection.push('\n');
    projection.push_str(&table(
        &p["families"],
        &[
            "namespace",
            "family",
            "bytes",
            "bytes_per_hour",
            "bytes_per_record",
            "records_per_hour",
            "live_keys_start",
            "live_keys_end",
        ],
    )?);
    projection.push_str(
        "\nPopulation split (record timestamps; same-key ordering uses stripe offsets):\n\n",
    );
    projection.push_str(&table(
        &p["publisher_change_eras"],
        &[
            "label",
            "start_at",
            "end_at",
            "hours",
            "bytes",
            "records",
            "bytes_per_hour",
        ],
    )?);
    projection.push_str("\nPublisher assumption: 58 fixed + 9 namespace + resolved key + 36 UUID + 199 value bytes per record, saved trailing-24-hour row rates. All located loops may include stopped ledgers. No acknowledgement policy or flush-pressure prediction follows.\n\n");
    projection.push_str(&table(
        &p["capacity_projections"],
        &[
            "scenario",
            "target_bytes",
            "remaining_bytes",
            "rate_bytes_per_hour",
            "hours",
            "days",
            "projected_at",
        ],
    )?);
    projection.push_str("\nReadiness slope=(big_seconds-small_seconds)/(big_bytes-small_bytes); intercept=small_seconds-slope*small_bytes. Both observations retained:\n\n");
    projection.push_str(&table(
        &p["replay_fits"],
        &[
            "big_reading",
            "small_bytes",
            "small_seconds",
            "big_bytes",
            "big_seconds",
            "seconds_per_byte",
            "intercept_seconds",
            "projected_6GiB_seconds",
            "projected_8GiB_seconds",
        ],
    )?);
    projection.push_str(&format!("\nPhysical disk (shared, no attribution):\n\n```json\n{}\n```\n\n{}\n\nHost supplement: {}. {}\n",serde_json::to_string_pretty(&p["physical_disk"]).map_err(|e|e.to_string())?,cell(&p["limits"]),cell(&r.host["status"]),cell(&r.host["limits"])));
    let comparison = if s["token_exceeds_historical_comparison_value"] == true {
        "Supplied rate exceeds the historical comparison value"
    } else {
        "Supplied rate does not exceed the historical comparison value"
    };
    let mut supplement=format!("# Offline review supplement\n\n{pre}Post-change token records: {}; {} records/hour, {} bytes. {comparison} of 42/hour; this does not establish that the historical owner's bound governs these inputs. Historical reference only: the original approximately 136.54/hour versus 42/hour excess remains unexplained counterevidence.\n\n",cell(&s["token_post_records"]),cell(&s["token_post_records_per_hour"]),cell(&s["token_post_bytes"]));
    supplement.push_str(&table(
        &s["key_capacity"],
        &[
            "basis",
            "scope",
            "current_keys",
            "new_keys_per_hour",
            "warning_keys",
            "warning_days",
            "cap_keys",
            "cap_days",
        ],
    )?);
    supplement.push_str(&format!("\n30-day total-byte-rate thresholds (B/hour): {}.\n\nSaved ledger anchor: {}. Last-hour subset: {}. Corrections subset: {}. Device interval offsets: {} seconds at start, {} seconds at end.\n\n{}\n",cell(&s["needed_total_bytes_per_hour_for_30days"]),cell(&s["ledger_read_anchor"]),cell(&s["publisher_active_last_hour_subset"]),cell(&s["publisher_corrections_subset"]),cell(&s["disk_start_offset_seconds"]),cell(&s["disk_end_offset_seconds"]),cell(&s["limits"])));
    let mut metric=format!("# Offline metric retention scenario\n\n{pre}{}\n\nSerialized rate r is unknown. Forever: S0+b*t+r*t. Ideal: S0+b*t+r*min(t,72). At 720 hours H=target-S0-b*720; thresholds are max(0,H/720) and max(0,H/72).\n\n",cell(&m["historical_basis"]));
    metric.push_str(&table(
        &m["rows"],
        &[
            "target_bytes",
            "headroom_after_base_30days_bytes",
            "forever_metric_threshold_bytes_per_hour",
            "ideal_72h_metric_threshold_bytes_per_hour",
            "ideal_72h_metric_threshold_bytes_per_second",
        ],
    )?);
    metric.push_str("\nSeparate historical key assumptions: new namespace cap 500000, global cap 917000, existing 331 + 26 fixed publisher keys. New metric records are assumed to be distinct keys. Fixed-key rewriting differs.\n\n");
    metric.push_str(&table(
        &m["key_thresholds"],
        &[
            "scope",
            "fraction",
            "remaining_keys_after_base_30days",
            "forever_new_metric_keys_per_hour",
            "ideal_72h_reclaimed_new_metric_keys_per_hour",
        ],
    )?);
    metric.push_str(&format!("\nChunk bound: {}.\n\nHistorical proposed granularity: {}.\n\nIllustrative rate 1000000 serialized B/hour, not observed: {}.\n\n{}\n",cell(&m["equations"]["whole_chunk_bound"]),cell(&m["historical_proposed_granularity"]),cell(&m["example_one_million_bytes_per_hour"]),cell(&m["limits"])));
    Ok(vec![
        ("PROJECTION.md", projection),
        ("REVIEW-SUPPLEMENT.md", supplement),
        ("METRIC-RETENTION.md", metric),
    ])
}
