"""Conditional three-day chunk-retirement arithmetic; no writer-rate claim."""
import json
from window import OUT, save

def main():
    p=json.loads((OUT/'projection.json').read_text())
    current=p['complete_log_bytes_end']
    base=next(row['rate_bytes_per_hour'] for row in p['capacity_projections']
              if row['scenario']=='current-post-change-plus-proposed-publisher')
    horizon=30*24
    retention=3*24
    rows=[]
    for target in (6*1024**3,8*1024**3):
        headroom=target-current-base*horizon
        rows.append(dict(target_bytes=target,headroom_after_base_30days_bytes=headroom,
            forever_metric_threshold_bytes_per_hour=max(0,headroom/horizon),
            ideal_72h_metric_threshold_bytes_per_hour=max(0,headroom/retention),
            ideal_72h_metric_threshold_bytes_per_second=max(0,headroom/retention/3600)))
    result=dict(source_rows=['E-7f6026f0590f4cf980b2d691a6008a22','E-690c2c616ec74708a6fc3e758f9d507a'],
        current_bytes=current,base_bytes_per_hour=base,horizon_hours=horizon,retention_hours=retention,
        actual_metric_membus_record_bytes_per_hour=None,rows=rows,
        equations=dict(forever='S(t)=S0+b*t+r*t',ideal_retired='S(t)=S0+b*t+r*min(t,72)',
            whole_chunk_bound='metric_bytes <= r*(72+delta_hours)+metadata for constant rate, maximum chunk age-span delta, prompt deletion, no pins/backlog, and no preexisting metric backlog'),
        limits='r is serialized MBK1 bytes/hour including record overhead, not raw JSON or physical disk bytes. No measured proposed metric writer rate or codec exists in this frontier. Raw metrics currently remain day-rotated files. Whole-chunk deletion and retained-index/tombstone reclamation are unimplemented; namespace time expiry or remove alone does not bound current logs/slots. Finite retention bounds metric storage, not ingress writes/flushes or the forever base. Derived rates/predictions are longer-lived and require a separate unmeasured budget. Actual chunk granularity, pinned readers, retirement lag and metadata increase storage above the ideal72h model.')
    save('metric-retention-scenario.json',result)
    text=['# F-E6-48 three-day metric scenario','',
        'Scoped late input: lane87 seq1104 / E-7f6026f0590f4cf980b2d691a6008a22, reiterated by parent seq1221. CAPACITY.md:155-168 separates three-day raw metrics from longer-lived derived rates/predictions. membus docs/PLAN.md:78-97 describes expiring chunks as design, not the current resident.',
        '',f'No actual future metric membus writer rate is measured. Let r be full serialized MBK1 bytes/hour, b={base:.6f} bytes/hour (measured post-change rate plus proposed ledger publisher), S0={current:,} bytes, t hours. Forever append: S(t)=S0+b*t+r*t. Ideal prompt whole-chunk retirement with zero granularity/metadata: S(t)=S0+b*t+r*min(t,72).',
        '', 'At30days (t=720), remaining metric headroom H=target-S0-b*720. Rate needed to reach the target within30days: forever r>=H/720; ideal three-day r>=H/72. This is threshold arithmetic, not an adopted forecast.',
        '', '| Target | H after base30days (bytes) | Forever threshold B/hour | Ideal72h threshold B/hour | Ideal72h B/s |',
        '|---|---:|---:|---:|---:|']
    for row in rows:
        text.append(f'| {row["target_bytes"]/1024**3:g}GiB | {row["headroom_after_base_30days_bytes"]:,.3f} | {row["forever_metric_threshold_bytes_per_hour"]:,.3f} | {row["ideal_72h_metric_threshold_bytes_per_hour"]:,.3f} | {row["ideal_72h_metric_threshold_bytes_per_second"]:,.3f} |')
    text.extend(['', 'For whole chunks spanning at most delta hours, a conservative constant-rate metric bound is r*(72+delta)+metadata, assuming prompt deletion, no pins/backlog and no preexisting metric backlog. The30day threshold becomes H/(72+delta) after deducting metadata. Because granularity is unselected, the table is an ideal baseline; it does not claim an enforceable72-hour byte ceiling.', '',
        'Example only: at r=1,000,000 serialized bytes/hour, metric additions at30days are720,000,000 bytes forever versus72,000,000 ideal-retired bytes; combined totals are1,575,932,120 and927,932,120 bytes. The example rate is not an observed writer rate.', '',result['limits'], '',
        'Lane87’s8GiB-within30days trigger is not reached by the measured base/publisher scenario. Unknown metric adoption could change that conclusion: forever requires about10.74MB/hour extra serialized writes; ideal72h retirement requires about107.42MB/hour. Priority between retirement and leases remains with Orient/Decide.'])
    (OUT/'METRIC-RETENTION.md').write_text('\n'.join(text)+'\n')
    print(json.dumps(result))

if __name__=='__main__':main()
