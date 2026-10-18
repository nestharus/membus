use super::*;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Default)]
struct Bucket {
    bytes: u64,
    records: u64,
    value_bytes: u64,
    new_keys: u64,
    overwrites: u64,
    removes: u64,
}
impl Bucket {
    fn value(&self) -> Value {
        json!({"bytes":self.bytes,"records":self.records,"value_bytes":self.value_bytes,"new_keys":self.new_keys,"overwrites":self.overwrites,"removes":self.removes})
    }
    fn add(&mut self, r: &Value, new: bool) -> Result<()> {
        let removed = field(r, "removed")?
            .as_bool()
            .ok_or("removed must be boolean")?;
        add(&mut self.bytes, uint(r, "bytes")?, "bucket bytes")?;
        add(&mut self.records, 1, "bucket records")?;
        add(
            &mut self.value_bytes,
            uint(r, "value_bytes")?,
            "bucket value bytes",
        )?;
        add(&mut self.new_keys, u64::from(new), "bucket new keys")?;
        add(
            &mut self.overwrites,
            u64::from(!new && !removed),
            "bucket overwrites",
        )?;
        add(&mut self.removes, u64::from(removed), "bucket removes")?;
        Ok(())
    }
}
pub(super) fn logs(endpoint: &Value) -> Result<&serde_json::Map<String, Value>> {
    object(field(endpoint, "logs")?)
}
fn key(row: &Value) -> Result<(String, String)> {
    Ok((text(row, "namespace")?.into(), text(row, "key")?.into()))
}
pub(super) fn prior(endpoint: &Value) -> Result<BTreeSet<(String, String)>> {
    let mut keys = BTreeSet::new();
    for log in logs(endpoint)?.values() {
        for row in array(field(log, "records")?)? {
            keys.insert(key(row)?);
        }
    }
    Ok(keys)
}
pub(super) fn tail(i: &Inputs) -> Result<Vec<&Value>> {
    let a = logs(&i.start)?;
    let b = logs(&i.end)?;
    if a.keys().collect::<Vec<_>>() != b.keys().collect::<Vec<_>>() {
        return Err("endpoint stripe names differ".into());
    }
    let mut stripes = BTreeMap::new();
    let mut out = vec![];
    for (name, end) in b {
        let start = &a[name];
        let boundary = uint(start, "complete_bytes")?;
        let end_bytes = uint(end, "complete_bytes")?;
        if end_bytes < boundary {
            return Err("stripe shrank".into());
        }
        let before = array(field(start, "records")?)?;
        let after = array(field(end, "records")?)?;
        if after.len() < before.len() || after[..before.len()] != before[..] {
            return Err("saved stripe prefix changed".into());
        }
        let mut offset = 0;
        for row in after {
            if uint(row, "offset")? != offset {
                return Err("stripe record offsets are not contiguous".into());
            }
            let bytes = uint(row, "bytes")?;
            if bytes == 0 {
                return Err("zero record length".into());
            }
            offset = offset.checked_add(bytes).ok_or("stripe byte overflow")?;
            if uint(row, "end")? != offset {
                return Err("record end disagrees with byte length".into());
            }
            let k = key(row)?;
            if let Some(old) = stripes.insert(k, name) {
                if old != name {
                    return Err("key belongs to multiple stripes".into());
                }
            }
            text(row, "family")?;
            uint(row, "at_ms")?;
            if uint(row, "value_bytes")? > bytes {
                return Err(format!(
                    "stripe {name}: value bytes exceed encoded record bytes"
                ));
            }
            if uint(row, "offset")? >= boundary {
                out.push(row)
            }
        }
        if offset != end_bytes
            || before
                .last()
                .map(|r| uint(r, "end"))
                .transpose()?
                .unwrap_or(0)
                != boundary
        {
            return Err("complete stripe byte boundary mismatch".into());
        }
    }
    Ok(out)
}
fn at(v: &Value, keys: &[&str]) -> Result<f64> {
    let mut p = v;
    for k in keys {
        p = field(p, k)?;
    }
    number(p)
}
fn optional_at(v: &Value, keys: &[&str], default: Value) -> Value {
    let mut p = v;
    for k in keys {
        match p.get(*k) {
            Some(q) => p = q,
            None => return default,
        }
    }
    p.clone()
}
fn scan<'a>(endpoint: &'a Value, ns: &str) -> Result<&'a Value> {
    let scan = field(field(endpoint, "scans")?, ns)
        .map_err(|_| format!("missing required scan for namespace {ns}"))?;
    uint(scan, "live_keys")?;
    for value in object(field(scan, "family_counts")?)?.values() {
        value
            .as_u64()
            .filter(|n| *n <= 1u64 << 53)
            .ok_or("malformed scan family count")?;
    }
    Ok(scan)
}
pub(super) fn calculate(i: &Inputs, change: f64) -> Result<Value> {
    let start = num(&i.start, "unix_time")?;
    let end = num(&i.end, "unix_time")?;
    if !(start < change && change < end) {
        return Err("change must be strictly inside endpoint window".into());
    }
    if field(&i.marker, "same_identity")?.as_bool() != Some(true) {
        return Err("window does not establish same identity".into());
    }
    if field(&i.start, "hello_epoch")? != field(&i.end, "hello_epoch")?
        || field(&i.start, "binary_sha256")? != field(&i.end, "binary_sha256")?
    {
        return Err("endpoint identity changed".into());
    }
    if field(&i.marker, "resident_epoch")? != field(&i.start, "hello_epoch")? {
        return Err("marker epoch differs".into());
    }
    for endpoint in [&i.start, &i.end] {
        let heartbeat = field(endpoint, "heartbeat")?;
        if field(heartbeat, "pid")? != field(&i.marker, "resident_pid")?
            || field(heartbeat, "epoch")? != field(&i.marker, "resident_epoch")?
        {
            return Err("heartbeat identity differs from marker".into());
        }
    }
    for (key, t) in [("started_at", start), ("completed_at", end)] {
        if (stamp(text(&i.marker, key)?)? - t).abs() > 5.0 {
            return Err("marker/endpoint time difference exceeds 5 seconds".into());
        }
    }
    if num(&i.marker, "duration_seconds")? <= 0.0 {
        return Err("window duration must be positive".into());
    }
    let hours = finite((end - start) / 3600.0, "endpoint hours")?;
    if hours <= 0.0 {
        return Err("endpoint hours underflowed".into());
    }
    let tail = tail(i)?;
    let mut previous = prior(&i.start)?;
    let mut groups: BTreeMap<(String, String), Bucket> = BTreeMap::new();
    let mut namespaces: BTreeMap<String, Bucket> = BTreeMap::new();
    for r in &tail {
        let k = key(r)?;
        let new = previous.insert(k.clone());
        groups
            .entry((k.0.clone(), text(r, "family")?.into()))
            .or_default()
            .add(r, new)?;
        namespaces.entry(k.0).or_default().add(r, new)?;
    }
    let families = groups
        .iter()
        .map(|((ns, fam), b)| -> Result<Value> {
            scan(&i.start, ns)?;
            scan(&i.end, ns)?;
            let mut v = b.value();
            v["namespace"] = json!(ns);
            v["family"] = json!(fam);
            v["bytes_per_hour"] = json!(finite(b.bytes as f64 / hours, "family byte rate")?);
            v["bytes_per_record"] = json!(b.bytes as f64 / b.records as f64);
            v["records_per_hour"] = json!(finite(b.records as f64 / hours, "family record rate")?);
            v["live_keys_start"] =
                optional_at(&i.start, &["scans", ns, "family_counts", fam], json!(0));
            v["live_keys_end"] =
                optional_at(&i.end, &["scans", ns, "family_counts", fam], json!(0));
            Ok(v)
        })
        .collect::<Result<Vec<_>>>()?;
    let health_a = field(&i.start, "health")?;
    let health_b = field(&i.end, "health")?;
    for ns in object(health_a)?.keys().chain(object(health_b)?.keys()) {
        namespaces.entry(ns.clone()).or_default();
    }
    let namespaces = namespaces
        .iter()
        .map(|(ns, b)| -> Result<(String, Value)> {
            let mut v = b.value();
            v.as_object_mut().unwrap().remove("value_bytes");
            v["bytes_per_hour"] = json!(finite(b.bytes as f64 / hours, "namespace byte rate")?);
            v["bytes_per_record"] = if b.records > 0 {
                json!(b.bytes as f64 / b.records as f64)
            } else {
                Value::Null
            };
            for (label, e) in [("start", &i.start), ("end", &i.end)] {
                let observed = scan(e, ns)?;
                v[format!("quota_slots_{label}")] =
                    optional_at(e, &["health", ns, "namespace_keys"], Value::Null);
                v[format!("live_keys_{label}")] = field(observed, "live_keys")?.clone();
            }
            Ok((ns.clone(), v))
        })
        .collect::<Result<serde_json::Map<_, _>>>()?;
    let sum = |e: &Value| -> Result<u64> {
        logs(e)?.values().try_fold(0u64, |n, l| {
            n.checked_add(uint(l, "complete_bytes")?)
                .ok_or_else(|| "log size overflow".into())
        })
    };
    let sizes_start = sum(&i.start)?;
    let sizes_end = sum(&i.end)?;
    let growth = sizes_end - sizes_start;
    if growth != checked_sum(groups.values().map(|b| b.bytes), "grouped tail bytes")? {
        return Err("growth does not reconcile with tail".into());
    }
    let own_rate = finite(growth as f64 / hours, "full-window byte rate")?;
    let added = num(&i.rates, "publisher_upper_bytes_per_hour")?;
    if added < 0.0 || num(&i.rates, "total_rows_per_hour")? < 0.0 {
        return Err("negative publisher rate".into());
    }
    let mut eras = vec![];
    for (label, lo, hi, before) in [
        ("before-slots-publisher-change", start, change, true),
        ("after-slots-publisher-change", change, end, false),
    ] {
        let duration = finite((hi - lo) / 3600.0, "era hours")?;
        if duration <= 0.0 {
            return Err("era hours underflowed".into());
        }
        let mut by: BTreeMap<(String, String), (u64, u64)> = BTreeMap::new();
        for row in &tail {
            let t = uint(row, "at_ms")? as f64 / 1000.0;
            if (before && t < hi) || (!before && t >= lo) {
                let b = by
                    .entry((text(row, "namespace")?.into(), text(row, "family")?.into()))
                    .or_default();
                add(&mut b.0, uint(row, "bytes")?, "era bytes")?;
                add(&mut b.1, 1, "era records")?;
            }
        }
        let bytes = checked_sum(by.values().map(|b| b.0), "era total bytes")?;
        let records = checked_sum(by.values().map(|b| b.1), "era total records")?;
        let era_families=by.iter().map(|((ns,f),(b,n))| -> Result<Value>{Ok(json!({"namespace":ns,"family":f,"bytes":b,"records":n,"bytes_per_hour":finite(*b as f64/duration,"era family byte rate")?}))}).collect::<Result<Vec<_>>>()?;
        eras.push(json!({"label":label,"start_at":utc(lo)?,"end_at":utc(hi)?,"hours":duration,"bytes":bytes,"records":records,"bytes_per_hour":finite(bytes as f64/duration,"era byte rate")?,
            "families":era_families,
            "basis":"Supplied split timestamp; offset-selected tail records are partitioned by record timestamp without an extra endpoint-time filter. The reference experiment used the reported effective publisher-change minute."}));
    }
    let current_rate = num(&eras[1], "bytes_per_hour")?;
    let mut capacity = vec![];
    for (scenario, rate) in [
        ("full-window-mixture", own_rate),
        (
            "full-window-mixture-plus-proposed-publisher",
            finite(own_rate + added, "full-window combined rate")?,
        ),
        ("current-post-change", current_rate),
        (
            "current-post-change-plus-proposed-publisher",
            finite(current_rate + added, "post-change combined rate")?,
        ),
    ] {
        for target in [6u64 * 1024u64.pow(3), 8u64 * 1024u64.pow(3)] {
            let remain = target.saturating_sub(sizes_end);
            let h = if rate > 0.0 {
                Some(finite(remain as f64 / rate, "capacity hours")?)
            } else {
                None
            };
            capacity.push(json!({"scenario":scenario,"target_bytes":target,"current_bytes":sizes_end,"rate_bytes_per_hour":rate,"remaining_bytes":remain,"hours":h,"days":h.map(|h|h/24.0),"projected_at":h.map(|h|utc(end+h*3600.0)).transpose()?}));
        }
    }
    let small = field(&i.small, "restart")?;
    let x0 = num(small, "total_log_bytes")?;
    let t0 = num(small, "startup_to_ready_seconds")?;
    let x1 = at(&i.big, &["synthetic", "bytes"])?;
    if x1 <= x0 || x0 < 0.0 || t0 < 0.0 {
        return Err("invalid readiness fit anchors".into());
    }
    let runs = array(field(&i.big, "runs")?)?;
    if runs.len() != 2 {
        return Err("both readiness observations required".into());
    }
    let mut fits = vec![];
    for run in runs {
        let t1 = num(run, "startup_to_ready_seconds")?;
        if t1 < 0.0 {
            return Err("negative readiness duration".into());
        }
        let slope = finite((t1 - t0) / (x1 - x0), "readiness slope")?;
        let intercept = finite(t0 - slope * x0, "readiness intercept")?;
        fits.push(json!({"big_reading":text(run,"label")?,"small_bytes":x0,"small_seconds":t0,"big_bytes":x1,"big_seconds":t1,"seconds_per_byte":slope,"intercept_seconds":intercept,"projected_6GiB_seconds":finite(intercept+slope*(6u64*1024u64.pow(3)) as f64,"6 GiB readiness")?,"projected_8GiB_seconds":finite(intercept+slope*(8u64*1024u64.pow(3)) as f64,"8 GiB readiness")?}));
    }
    let physical = disk(&i.disk)?;
    let bytes_delta = finite(
        at(health_b, &["quota", "bytes_written"])? - at(health_a, &["quota", "bytes_written"])?,
        "health byte delta",
    )?;
    let flush_delta = finite(
        at(health_b, &["quota", "flush_calls"])? - at(health_a, &["quota", "flush_calls"])?,
        "health flush delta",
    )?;
    if bytes_delta < 0.0 || flush_delta < 0.0 {
        return Err("health cumulative counters reset".into());
    }
    let mut all = vec![];
    for log in logs(&i.end)?.values() {
        all.extend(array(field(log, "records")?)?)
    }
    let latest = all
        .iter()
        .filter(|r| r["namespace"] == "slots" && r["key"] == "all")
        .map(|r| uint(r, "at_ms"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max();
    let first_resource = all
        .iter()
        .filter(|r| {
            r["namespace"] == "slots"
                && r["key"] != "all"
                && r["key"].as_str().is_some_and(|s| !s.starts_with("event/"))
        })
        .map(|r| uint(r, "at_ms"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|t| *t as f64 / 1000.0 >= change)
        .min();
    Ok(
        json!({"window":{"started_at":text(&i.marker,"started_at")?,"completed_at":text(&i.marker,"completed_at")?,"duration_seconds":num(&i.marker,"duration_seconds")?,"resident_pid":field(&i.marker,"resident_pid")?,"resident_epoch":field(&i.marker,"resident_epoch")?,"same_identity":true},
        "endpoint_identity":{"binary_sha256":field(&i.start,"binary_sha256")?,"hello_epoch":field(&i.start,"hello_epoch")?,"meaning":"Supplied installed-file hash and observed epoch; not independent loaded-image proof."},"hours":hours,"complete_log_bytes_start":sizes_start,"complete_log_bytes_end":sizes_end,"log_growth_bytes":growth,"bytes_per_hour":own_rate,"records":tail.len(),"records_per_hour":finite(tail.len() as f64/hours,"total record rate")?,"mean_bytes_per_record":if tail.is_empty(){None}else{Some(growth as f64/tail.len() as f64)},"families":families,"namespaces":namespaces,
        "health_bytes_delta":bytes_delta,"health_bytes_per_hour":finite(bytes_delta/hours,"health byte rate")?,"health_flush_delta":flush_delta,"health_flushes_per_hour":finite(flush_delta/hours,"health flush rate")?,"health_vs_log_difference_bytes":finite(bytes_delta-growth as f64,"health/log difference")?,
        "heartbeat_bytes_delta":finite(at(&i.end,&["heartbeat","heartbeat_bytes"])?-at(&i.start,&["heartbeat","heartbeat_bytes"])?,"heartbeat byte delta")?,"heartbeat_at_start":field(field(&i.start,"heartbeat")?,"unix_ms")?,"heartbeat_at_end":field(field(&i.end,"heartbeat")?,"unix_ms")?,
        "physical_disk":physical,"publisher":{"rows_per_hour":num(&i.rates,"total_rows_per_hour")?,"upper_bytes_per_hour":added,"share_of_combined_rate":if current_rate+added>0.0{Some(added/(current_rate+added))}else{None},"assumptions":"Saved trailing-24-hour ledger summary; 58 fixed record bytes + 9 namespace bytes + resolved key bytes + 36 assumed UUID bytes + 199 value bytes. Not a census of active loops or acknowledgement-policy selection."},
        "publisher_change_eras":eras,"last_slots_all_record_at":latest.map(|t|utc(t as f64/1000.0)).transpose()?,"first_post_change_slots_resource_record_at":first_resource.map(|t|utc(t as f64/1000.0)).transpose()?,"capacity_projections":capacity,"replay_fits":fits,
        "limits":"Historical reference only: the F48 experiment at baseline 11c312c on 2026-10-10 had a 135.324-second heartbeat sampling gap and continuous primary disk sampling. Those are not assertions about another supplied fixture. Endpoint continuity does not fill missing heartbeat samples; supplied disk gaps and offsets are computed separately. Shared device traffic is unattributed. The reference capture lacked file-write/sync-attempt counters; this command cannot manufacture them. Logical persisted records and health flush calls differ. Forecasts assume constant populations and publisher shape; RAM/index/namespace constraints may fail earlier. Warm readiness fits remain conditional different-shape extrapolations, not measured 6/8 GiB bootability. Historical removal retained log bytes/slots; dated design statements do not describe current source. Future metric rate/key shape and the reference token-excess cause remain unknown. Saved ledger summaries reproduce downstream arithmetic, not acquisition. Endpoint elapsed time is the rate denominator; positive marker duration has a separate unreconciled anchor. Stripe offsets select the tail; timestamps split it without another endpoint-time filter."}),
    )
}

fn disk(series: &[Value]) -> Result<Value> {
    let mut collectors: BTreeMap<u64, Vec<(f64, &Value)>> = BTreeMap::new();
    let mut device = None;
    for row in series {
        let t = stamp(text(row, "at")?)?;
        let raw = field(row, "raw")?;
        if raw.is_null() {
            continue;
        }
        let d = text(row, "physical_device")?;
        if device.is_some_and(|old| old != d) {
            return Err("disk series mixes physical devices".into());
        }
        device = Some(d);
        collectors
            .entry(uint(row, "collector_pid")?)
            .or_default()
            .push((t, row));
    }
    for rows in collectors.values_mut() {
        rows.sort_by(|a, b| a.0.total_cmp(&b.0));
    }
    let (&primary, valid) = collectors
        .iter()
        .min_by(|(pa, a), (pb, b)| a[0].0.total_cmp(&b[0].0).then(pa.cmp(pb)))
        .ok_or("no valid disk samples")?;
    if valid.len() < 2 {
        return Err("primary disk collector needs two samples".into());
    }
    let first = valid[0].1;
    let last = valid.last().unwrap().1;
    let f = field(first, "raw")?;
    let l = field(last, "raw")?;
    let frequency = num(f, "Frequency_PerfTime")?;
    if frequency <= 0.0 {
        return Err("invalid performance frequency".into());
    }
    let mut gaps = vec![];
    let mut queues = vec![];
    for (_, row) in valid {
        let r = field(row, "raw")?;
        if num(r, "Frequency_PerfTime")? != frequency {
            return Err("performance frequency changed".into());
        }
        let q = num(r, "CurrentDiskQueueLength")?;
        if q < 0.0 {
            return Err("negative queue".into());
        }
        queues.push(q);
        for key in [
            "Timestamp_PerfTime",
            "DiskWriteBytesPersec",
            "DiskWritesPersec",
        ] {
            if num(r, key)? < 0.0 {
                return Err(format!("negative cumulative disk counter: {key}"));
            }
        }
    }
    for pair in valid.windows(2) {
        let a = field(pair[0].1, "raw")?;
        let b = field(pair[1].1, "raw")?;
        if pair[1].0 <= pair[0].0 || num(b, "Timestamp_PerfTime")? <= num(a, "Timestamp_PerfTime")?
        {
            return Err("disk sample times do not increase".into());
        }
        for k in ["DiskWriteBytesPersec", "DiskWritesPersec"] {
            if num(b, k)? < num(a, k)? {
                return Err("device cumulative counters reset".into());
            }
        }
        if pair[1].0 - pair[0].0 > 25.0 {
            gaps.push(json!({"before":text(pair[0].1,"at")?,"after":text(pair[1].1,"at")?,"seconds":pair[1].0-pair[0].0}));
        }
    }
    let seconds = finite(
        (num(l, "Timestamp_PerfTime")? - num(f, "Timestamp_PerfTime")?) / frequency,
        "disk interval seconds",
    )?;
    if seconds <= 0.0 {
        return Err("disk interval underflowed".into());
    }
    let bytes = finite(
        num(l, "DiskWriteBytesPersec")? - num(f, "DiskWriteBytesPersec")?,
        "disk byte delta",
    )?;
    let writes = finite(
        num(l, "DiskWritesPersec")? - num(f, "DiskWritesPersec")?,
        "disk write delta",
    )?;
    Ok(
        json!({"device":device,"primary_collector_pid":primary,"collector_sample_counts":collectors.iter().map(|(p,r)|(p.to_string(),json!(r.len()))).collect::<serde_json::Map<_,_>>(),"first":text(first,"at")?,"last":text(last,"at")?,"seconds":seconds,"cumulative_write_bytes_delta":bytes,"cumulative_writes_delta":writes,"mean_write_bytes_per_second":finite(bytes/seconds,"disk byte rate")?,"mean_writes_per_second":finite(writes/seconds,"disk write rate")?,"samples":valid.len(),"sampled_queue_mean":finite(queues.iter().sum::<f64>()/queues.len() as f64,"sampled queue mean")?,"sampled_queue_max":queues.iter().copied().fold(0.0,f64::max),"sampled_queue_nonzero":queues.iter().filter(|q|**q>0.0).count(),"sampling_gaps":gaps,"limits":"Earliest valid collector is primary; duplicate collectors are counted separately, never summed. Gaps >25 seconds are explicit. Queue statistics are sampled instants only; shared physical traffic is not attributable to Membus."}),
    )
}
