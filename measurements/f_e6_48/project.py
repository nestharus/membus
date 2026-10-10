"""Compute located growth/rate arithmetic from the completed measurement."""
import collections, datetime, json, math, pathlib, statistics
from window import OUT, save, utc

def read(name):return json.loads((OUT/name).read_text(encoding='utf-8-sig'))
def stamp(value):return datetime.datetime.fromisoformat(value).timestamp()
def flat(reading):return [row for log in reading['logs'].values() for row in log['records']]

def main():
    start,end=read('resident-start.json'),read('resident-end.json')
    marker=read('WINDOW-COMPLETE.json')
    hours=(end['unix_time']-start['unix_time'])/3600
    previous={(row['namespace'],row['key']) for row in flat(start)}
    grouped=collections.defaultdict(lambda:dict(bytes=0,records=0,value_bytes=0,new_keys=0,overwrites=0,removes=0))
    tail=[]
    for name,log in end['logs'].items():
        boundary=start['logs'][name]['complete_bytes']
        for row in log['records']:
            if row['offset']>=boundary:tail.append(row)
    # One key always maps to one stripe; its file offsets order writes.
    # Revision tokens are opaque and are not a cross-stripe ordering contract.
    for row in tail:
        key=(row['namespace'],row['key'])
        bucket=grouped[(row['namespace'],row['family'])]
        bucket['bytes']+=row['bytes'];bucket['records']+=1;bucket['value_bytes']+=row['value_bytes']
        bucket['new_keys']+=int(key not in previous)
        bucket['overwrites']+=int(key in previous and not row['removed'])
        bucket['removes']+=int(row['removed'])
        previous.add(key)
    families=[]
    namespaces=collections.defaultdict(lambda:dict(bytes=0,records=0,new_keys=0,overwrites=0,removes=0))
    for (ns,family),bucket in sorted(grouped.items()):
        families.append(dict(namespace=ns,family=family,**bucket,bytes_per_hour=bucket['bytes']/hours,
            bytes_per_record=bucket['bytes']/bucket['records'],records_per_hour=bucket['records']/hours,
            live_keys_start=start['scans'].get(ns,{}).get('family_counts',{}).get(family,0),
            live_keys_end=end['scans'].get(ns,{}).get('family_counts',{}).get(family,0)))
        for field in namespaces[ns]:namespaces[ns][field]+=bucket[field]
    for ns in sorted(set(start['health'])|set(end['health'])):
        bucket=namespaces[ns]
        bucket['bytes_per_hour']=bucket['bytes']/hours
        bucket['bytes_per_record']=bucket['bytes']/bucket['records'] if bucket['records'] else None
        bucket['quota_slots_start']=start['health'].get(ns,{}).get('namespace_keys')
        bucket['quota_slots_end']=end['health'].get(ns,{}).get('namespace_keys')
        bucket['live_keys_start']=start['scans'].get(ns,{}).get('live_keys',0)
        bucket['live_keys_end']=end['scans'].get(ns,{}).get('live_keys',0)
    sizes_start=sum(row['complete_bytes'] for row in start['logs'].values())
    sizes_end=sum(row['complete_bytes'] for row in end['logs'].values())
    growth=sizes_end-sizes_start
    assert growth==sum(row['bytes'] for row in tail)
    global_start=start['health']['quota'];global_end=end['health']['quota']
    health_bytes_delta=global_end['bytes_written']-global_start['bytes_written']
    health_flush_delta=global_end['flush_calls']-global_start['flush_calls']
    series=[json.loads(line) for line in (OUT/'disk-counters.jsonl').read_text(encoding='utf-8-sig').splitlines() if line.strip()]
    series.sort(key=lambda row:stamp(row['at']))
    by_collector=collections.defaultdict(list)
    for row in series:
        if row['raw'] is not None:by_collector[row['collector_pid']].append(row)
    primary=min(by_collector,key=lambda pid:stamp(by_collector[pid][0]['at']))
    valid=by_collector[primary]
    first,last=valid[0],valid[-1]
    seconds=(last['raw']['Timestamp_PerfTime']-first['raw']['Timestamp_PerfTime'])/first['raw']['Frequency_PerfTime']
    write_bytes=last['raw']['DiskWriteBytesPersec']-first['raw']['DiskWriteBytesPersec']
    writes=last['raw']['DiskWritesPersec']-first['raw']['DiskWritesPersec']
    if write_bytes<0 or writes<0:raise RuntimeError('device cumulative counters reset')
    gaps=[dict(before=a['at'],after=b['at'],seconds=stamp(b['at'])-stamp(a['at'])) for a,b in zip(valid,valid[1:]) if stamp(b['at'])-stamp(a['at'])>25]
    queues=[row['raw']['CurrentDiskQueueLength'] for row in valid]
    hostpath=pathlib.Path('D:/projects/metrics/data/raw/host/2026-10-10.jsonl')
    supplemental=[]
    for line in hostpath.read_text(encoding='utf-8-sig').splitlines():
        row=json.loads(line)
        if first and stamp(first['at'])<=row['t']<=stamp(last['at']):
            supplemental.append(dict(at=utc(row['t']),queue=row['v'].get('disk.D.queue'),busy_pct=row['v'].get('disk.D.busy_pct')))
    save('host-metrics-supplement.json',dict(source=str(hostpath),rows=supplemental,method='read-only host collector queue/busy samples over window; no write counters in this source; queue values remain instantaneous'))
    rates=read('ledger-rates.json')
    own_rate=growth/hours;added=rates['publisher_upper_bytes_per_hour']
    change_at=stamp('2026-10-10T01:35:00+00:00')
    eras=[]
    for label,lower,upper in [('before-slots-publisher-change',start['unix_time'],change_at),('after-slots-publisher-change',change_at,end['unix_time'])]:
        era_rows=[row for row in tail if (row['at_ms']/1000<upper if label.startswith('before') else row['at_ms']/1000>=lower)]
        duration_hours=(upper-lower)/3600
        era_groups=collections.defaultdict(lambda:dict(bytes=0,records=0))
        for row in era_rows:
            group=era_groups[(row['namespace'],row['family'])];group['bytes']+=row['bytes'];group['records']+=1
        total=sum(row['bytes'] for row in era_rows)
        eras.append(dict(label=label,start_at=utc(lower),end_at=utc(upper),hours=duration_hours,bytes=total,
            records=len(era_rows),bytes_per_hour=total/duration_hours,
            families=[dict(namespace=ns,family=family,**group,bytes_per_hour=group['bytes']/duration_hours) for (ns,family),group in sorted(era_groups.items())],
            basis='Lane88 seq1164 reports slots/all stopped at01:35Z pass after975af1d landed01:34:27Z; split at owner-reported effective minute. Record timestamps retain observed last all and first resource writes.'))
    current_rate=eras[1]['bytes_per_hour']
    projections=[]
    for label,rate in [('full-window-mixture',own_rate),('full-window-mixture-plus-proposed-publisher',own_rate+added),
                       ('current-post-change',current_rate),('current-post-change-plus-proposed-publisher',current_rate+added)]:
        for target in (6*1024**3,8*1024**3):
            remain=max(0,target-sizes_end);time_hours=remain/rate if rate else None
            future=datetime.datetime.fromtimestamp(end['unix_time'],datetime.timezone.utc)+datetime.timedelta(hours=time_hours) if time_hours is not None else None
            projections.append(dict(scenario=label,target_bytes=target,current_bytes=sizes_end,rate_bytes_per_hour=rate,
                remaining_bytes=remain,hours=time_hours,days=time_hours/24 if time_hours is not None else None,
                projected_at=future.isoformat() if future else None))
    small,big=read('private-small.json'),read('private-big.json')
    x0,t0=small['restart']['total_log_bytes'],small['restart']['startup_to_ready_seconds']
    x1=big['synthetic']['bytes'];fits=[]
    for reading in big['runs']:
        slope=(reading['startup_to_ready_seconds']-t0)/(x1-x0)
        intercept=t0-slope*x0
        fits.append(dict(big_reading=reading['label'],small_bytes=x0,small_seconds=t0,big_bytes=x1,
            big_seconds=reading['startup_to_ready_seconds'],seconds_per_byte=slope,intercept_seconds=intercept,
            projected_6GiB_seconds=intercept+slope*6*1024**3,
            projected_8GiB_seconds=intercept+slope*8*1024**3))
    result=dict(window=marker,hours=hours,complete_log_bytes_start=sizes_start,complete_log_bytes_end=sizes_end,
        log_growth_bytes=growth,bytes_per_hour=own_rate,records=len(tail),records_per_hour=len(tail)/hours,
        mean_bytes_per_record=growth/len(tail) if tail else None,namespaces=dict(namespaces),families=families,
        health_bytes_delta=health_bytes_delta,health_bytes_per_hour=health_bytes_delta/hours,
        health_flush_delta=health_flush_delta,health_flushes_per_hour=health_flush_delta/hours,
        health_vs_log_difference_bytes=health_bytes_delta-growth,
        heartbeat_bytes_delta=end['heartbeat']['heartbeat_bytes']-start['heartbeat']['heartbeat_bytes'],
        heartbeat_at_start=start['heartbeat']['unix_ms'],heartbeat_at_end=end['heartbeat']['unix_ms'],
        physical_disk=dict(device='PHYSICALDRIVE2 CT4000T700SSD3 (D:)',primary_collector_pid=primary,
            collector_sample_counts={str(pid):len(rows) for pid,rows in by_collector.items()},
            first=first['at'],last=last['at'],seconds=seconds,
            cumulative_write_bytes_delta=write_bytes,cumulative_writes_delta=writes,mean_write_bytes_per_second=write_bytes/seconds,
            mean_writes_per_second=writes/seconds,samples=len(valid),sampled_queue_mean=statistics.mean(queues),
            sampled_queue_max=max(queues),sampled_queue_nonzero=sum(queue>0 for queue in queues),sampling_gaps=gaps,
            limits='Primary collector remained continuous with no gaps over25s. Queue means/maxima apply only to sampled instants. Disk is shared, including our private build and synthetic writes; no causal attribution of physical bytes to membus. Host collector supplies supplementary1-minute queue/busy samples.'),
        publisher=dict(rows_per_hour=rates['total_rows_per_hour'],upper_bytes_per_hour=added,share_of_combined_rate=added/(current_rate+added)),
        publisher_change_eras=eras,last_slots_all_record_at=utc(max(row['at_ms'] for row in flat(end) if row['namespace']=='slots' and row['key']=='all')/1000),
        first_post_change_slots_resource_record_at=utc(min(row['at_ms'] for row in flat(end) if row['namespace']=='slots' and row['key']!='all' and not row['key'].startswith('event/') and row['at_ms']/1000>=change_at)/1000),
        capacity_projections=projections,replay_fits=fits,
        limits='One resident hour is not a forecast of future load. Capacity dates model the stripe-byte limit only; RAM/index pressure or namespace quotas may fail earlier. File-stat/health/heartbeat capture intervals are not atomic; exact MBK1 complete-record accounting retained. Flush counter reports fsync calls, not logical puts; record writes come from logs, file write-call9 is a different metric. Publisher scenario assumes36-byte UUID op_ids,199-byte values and all located operational ledgers; stopped ledgers with recent rows are included, not a census of currently active loops. It adds2304.416667 logical puts/hour; acknowledgement policy is unselected, so no flush-pressure prediction follows. Linear replay is a two-point extrapolation of startup-to-ready under warm uncontrolled cache, different live-key shapes and shared load; neither6GiB nor8GiB replay was measured.')
    save('projection.json',result)
    text=['# F-E6-48 measured growth and projection','',f'Window: {marker["started_at"]} to {marker["completed_at"]}; duration {marker["duration_seconds"]:.3f}s; resident PID15524 epoch5 retained. Logical logs {sizes_start:,} -> {sizes_end:,} bytes; {growth:,} bytes/{hours:.6f} hours = **{own_rate:,.2f} bytes/hour**.',
        f'{len(tail):,} complete MBK1 records ({len(tail)/hours:.2f}/hour), {result["mean_bytes_per_record"]:.2f} bytes/record. Health bytes delta {health_bytes_delta:,}; difference from complete logs {health_bytes_delta-growth:+,} bytes despite separately captured boundaries. Flush calls delta {health_flush_delta}, {health_flush_delta/hours:.2f}/hour. Actual File::write calls and sync attempts (opcode9) were not captured; the reported write rate counts persisted logical records.', '',
        '| Namespace | Added bytes | Bytes/hour | Records | New slots | Overwrites | Removes | Slots start/end | Live start/end |',
        '|---|---:|---:|---:|---:|---:|---:|---|---|']
    for ns,row in sorted(namespaces.items()):
        text.append(f'| {ns} | {row["bytes"]:,} | {row["bytes_per_hour"]:,.2f} | {row["records"]} | {row["new_keys"]} | {row["overwrites"]} | {row["removes"]} | {row["quota_slots_start"]}/{row["quota_slots_end"]} | {row["live_keys_start"]}/{row["live_keys_end"]} |')
    text.extend(['','| Namespace/family | Added bytes | Bytes/hour | Bytes/record | Records/hour | Live keys start/end |','|---|---:|---:|---:|---:|---|'])
    for row in families:
        text.append(f'| {row["namespace"]}/{row["family"]} | {row["bytes"]:,} | {row["bytes_per_hour"]:,.2f} | {row["bytes_per_record"]:.2f} | {row["records_per_hour"]:.2f} | {row["live_keys_start"]}/{row["live_keys_end"]} |')
    text.extend(['',f'Physical D: {write_bytes:,} cumulative write bytes/{seconds:.3f}s = {write_bytes/seconds:,.2f} bytes/s; {writes:,} writes/{seconds:.3f}s = {writes/seconds:.2f} writes/s. {len(valid)} queue samples, mean {statistics.mean(queues):.3f}, max {max(queues)}; {len(gaps)} gaps longer than25s. Primary sampling remained continuous; host-metrics-supplement.json supplies additional1-minute queue/busy observations. Shared device counters include our private build and synthetic writes and cannot be attributed to membus.',
        '', 'Publisher changed inside the window: lane88 seq1164 reports975af1d live at01:35Z, stopping key all in namespace slots and writing changed resource keys (cpu, mem, gpu, disk:c, disk:d, inference, inference-codex). Full-window mixture is retained as history, and the post-change period is the measured current-load basis. Observed token writes exceed the owner’s stated42/hour bound; see REVIEW-SUPPLEMENT.md.',
        f'Observed last namespace slots key all record {result["last_slots_all_record_at"]}; first post-change bare resource-key write {result["first_post_change_slots_resource_record_at"]}. Resource keys already existed at the start.'])
    for era in eras:
        text.append(f'- {era["label"]}: {era["bytes"]:,} bytes/{era["hours"]:.6f} hours = **{era["bytes_per_hour"]:,.2f} bytes/hour** ({era["records"]} records). Per-family era arithmetic is in projection.json.')
    text.extend(['',f'Proposed publisher: {rates["total_rows_last_24h"]:,} rows/24h = {rates["total_rows_per_hour"]:.6f} rows/h. MBK1 record =58fixed bytes +9namespace bytes + resolved key bytes +36assumed UUID bytes +199value bytes. Sum each loop rate times its record size = **{added:,.2f} bytes/hour**, {100*added/(current_rate+added):.2f}% of post-change combined rate. See ledger-rates.json for every located ledger, its read hash and row counts. Under200value bytes is not an upper bound on full record size.',
        '', '| Scenario | Target | Remaining bytes / bytes per hour | Days from endpoint | Projected UTC |','|---|---|---|---:|---|'])
    for row in projections:
        text.append(f'| {row["scenario"]} | {row["target_bytes"]/1024**3:g}GiB | {row["remaining_bytes"]:,} / {row["rate_bytes_per_hour"]:,.2f} | {row["days"]:.2f} | {row["projected_at"]} |')
    text.extend(['','Replay arithmetic: slope=(big_seconds-small_seconds)/(268400000-680000); intercept=small_seconds-slope*680000; estimate(target)=intercept+slope*target. Measured small startup-to-ready65.6382ms; synthetic first1.6078677s/restart1.8239713s. These include process start, service initialization, pipe hello and health, with uncontrolled warm cache. Different retained-key shapes prevent a causal bytes-only model.'])
    for row in fits:text.append(f'- {row["big_reading"]}: slope {row["seconds_per_byte"]:.12g}s/byte, intercept {row["intercept_seconds"]:.6f}s;6GiB {row["projected_6GiB_seconds"]:.3f}s,8GiB {row["projected_8GiB_seconds"]:.3f}s.')
    text.extend(['', 'Pruning evidence: private1000-key test put289000bytes/1000slots; overwrite584000bytes/1000slots; remove680000bytes/1000slots; restart680000bytes/1000slots, all live gets absent. Thus pruning deletes live event payloads and appends96bytes per tested tombstone; it reclaims neither log bytes nor quota slots. Whole-chunk retirement remains designed, not implemented here.', '',result['limits'], '',
        'Three-day metrics scenario (lane87 seq1104, parent seq1221): see METRIC-RETENTION.md and metric-retention-scenario.json for derived extra-rate thresholds under forever and ideal72h retirement. Actual metric writer rate is unknown; retirement and proposed60s/8MiB chunk granularity are unimplemented. Existing quota fit/token writes are already included in the measured base; additional derived rates/predictions need a separate budget.', '',
        'Review counterevidence: REVIEW-SUPPLEMENT.md/review-supplement.json provide30day total-rate thresholds, earlier constant-rate namespace/index limits, last-hour ledger deployment sensitivity, exact device interval offsets, synthetic retry-fingerprint exclusion and the unresolved latest-version/whole-chunk retention distinction. These limits are part of this projection. New slots and overwrite columns describe already-consumed slot identity; resurrection would count as an overwrite, and a first-ever remove could also consume a new slot. No such cases occurred in this window.'])
    (OUT/'PROJECTION.md').write_text('\n'.join(text)+'\n',encoding='utf-8')
    print(json.dumps(dict(bytes_per_hour=own_rate,publisher_upper_bytes_per_hour=added,capacity_projections=projections)))

if __name__=='__main__':main()
