use super::*;
pub(super) fn calculate(p: &Value, s: &Value) -> Result<Value> {
    let current = num(p, "complete_log_bytes_end")?;
    let base = array(field(p, "capacity_projections")?)?
        .iter()
        .find(|r| r["scenario"] == "current-post-change-plus-proposed-publisher")
        .ok_or("missing combined scenario")?;
    let base = num(base, "rate_bytes_per_hour")?;
    let horizon = 720.0;
    let retention = 72.0;
    let mut rows = vec![];
    for target in [6u64 * 1024u64.pow(3), 8u64 * 1024u64.pow(3)] {
        let headroom = target as f64 - current - base * horizon;
        rows.push(json!({"target_bytes":target,"headroom_after_base_30days_bytes":headroom,"forever_metric_threshold_bytes_per_hour":(headroom/horizon).max(0.0),"ideal_72h_metric_threshold_bytes_per_hour":(headroom/retention).max(0.0),"ideal_72h_metric_threshold_bytes_per_second":(headroom/retention/3600.0).max(0.0)}));
    }
    let global = array(field(s, "key_capacity")?)?
        .iter()
        .find(|r| r["basis"] == "post-change" && r["scope"] == "global-index")
        .ok_or("missing global key population")?;
    let global_rate = num(global, "new_keys_per_hour")?;
    // These are explicitly retained historical scenario assumptions, not current configuration.
    let mut keys = vec![];
    for (scope, cap, existing, rate) in [
        ("new-metric-namespace", 500000.0, 0.0, 0.0),
        ("global-index", 917000.0, 331.0 + 26.0, global_rate),
    ] {
        for fraction in [0.7, 1.0] {
            let h = cap * fraction - existing - rate * horizon;
            keys.push(json!({"scope":scope,"fraction":fraction,"remaining_keys_after_base_30days":h,"forever_new_metric_keys_per_hour":(h/horizon).max(0.0),"ideal_72h_reclaimed_new_metric_keys_per_hour":(h/retention).max(0.0)}));
        }
    }
    Ok(
        json!({"historical_basis":"F48 experiment, 2026-10-10, baseline 11c312c; original projection -> review supplement -> metric scenario",
        "current_bytes":current,"base_bytes_per_hour":base,"horizon_hours":horizon,"retention_hours":retention,"actual_metric_membus_record_bytes_per_hour":null,"rows":rows,"metric_key_shape":null,"new_publisher_keys_assumed":26,"historical_global_existing_keys_assumed":331,"historical_namespace_cap_assumed":500000,"historical_global_cap_assumed":917000,"base_new_global_keys_per_hour":global_rate,"key_thresholds":keys,
        "equations":{"forever":"S(t)=S0+b*t+r*t","ideal_retired":"S(t)=S0+b*t+r*min(t,72)","whole_chunk_bound":"metric_bytes <= r*(72+delta_hours)+metadata, assuming constant rate, bounded chunk age span, prompt deletion, no pins/backlog, and no preexisting metric backlog"},
        "example_one_million_bytes_per_hour":{"forever_total_bytes_at_30days":current+base*horizon+1e6*horizon,"ideal_retired_total_bytes_at_30days":current+base*horizon+1e6*retention},
        "historical_proposed_granularity":{"rotation_bytes":8*1024*1024,"rotation_seconds":60,"expiry_summary_retention_days":30,"four_stripe_72h_minute_files":17280,"four_stripe_30day_minute_summaries":172800,"one_minute_over_72h_fraction":1.0/60.0/72.0},
        "limits":"r is serialized MBK1 bytes/hour including overhead, not raw JSON or physical bytes. Actual future metric rate/key shape are unknown. In the 2026-10-10 experiment, whole-chunk deletion and retained-index/tombstone reclamation were unimplemented; this dated statement is not about current source. The historical plan proposed 8 MiB/60-second rotation and 30-day expiry summaries. Pins, lag, metadata, file counts and age span exceed the ideal 72-hour bound. Finite retention bounds metric storage, not ingress writes/flushes or forever base. Existing quota fit/token writes are in the base; derived rates need a separate budget. Key assumptions are fixed historical scenario values, not inferred current caps. Ideal key reclamation is hypothetical; expiry summaries may retain slots through the horizon. Byte thresholds do not establish earliest failure or adoption."}),
    )
}
