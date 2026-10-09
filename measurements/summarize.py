"""Render recorded runs into compact Markdown tables; does not launch probes."""
import argparse, collections, json, pathlib, statistics, xml.etree.ElementTree as ET

def rows(path):
    return [json.loads(line) for line in path.read_text(encoding='utf-8-sig').splitlines() if line]

def span(values, digits=1):
    low,high=min(values),max(values)
    return f'{low:.{digits}f}' if low==high else f'{low:.{digits}f}–{high:.{digits}f}'

def table(lines,headers,values):
    lines.extend(['','| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |'])
    lines.extend('| '+' | '.join(str(v) for v in row)+' |' for row in values)

def summarize(raw,repeat,output):
    lines=['# F-E6-2 measurements, 2026-10-09','',
      'Raw evidence: '+str(raw).replace('\\','/')+'. Each case has before/during/after `.context.jsonl` with lineup status, process count and PhysicalDisk samples. Commands and reserved slots are in `commands.jsonl`. The definitive final-build IPC passes are e1 `ipc-final.jsonl` and e2 `ipc-repeat.jsonl`; `ipc-1/2.jsonl` are preliminary single-interpreter thread results and are excluded here.',
      '', 'Ranges below are the minimum and maximum of two observed runs, not confidence intervals or promises about future runs. This shared host is noisy; a p99 based on 64 or 200 disk samples is an empirical order statistic with low tail confidence.',
      '', '## Append, write-through and flush', '',
      'Each write extends a newly truncated file, including NTFS allocation, size updates and metadata journal costs. Latencies are write-plus-optional-FlushFileBuffers, not isolated device flush time. Sizes are KiB. `write_through` issues FILE_FLAG_WRITE_THROUGH writes with zero explicit flush API calls; `write_through_flush` adds an explicit flush. The unflushed controls are drained once each outside the timer, counted separately. Write-through-only cases have no metadata drain; later cases may inherit dirty NTFS metadata, affecting their first samples and tails. Application bytes exclude filesystem metadata and NAND amplification.']
    flush_groups=collections.defaultdict(list)
    totals={}
    for device in ('C','D'):
        for run in (1,2):
            path=(repeat/'flush-D-repeat.jsonl') if device=='D' and run==2 else raw/f'flush-{device}-{2 if device=="D" else run}.jsonl'
            data=rows(path)
            for n,r in enumerate(data,1):r['source']=str(path).replace(chr(92),'/')+':'+str(n)
            totals[str(path).replace(chr(92),'/')]={'application_bytes':sum(r['bytes_written'] for r in data),'explicit_flush_calls':sum(r['flushes'] for r in data)}
            for row in data:
                if 'samples' in row:flush_groups[(device,row['kind'],row['payload'])].append(row)
    historical=rows(raw/'flush-D-1.jsonl')
    totals[str(raw/'flush-D-1.jsonl').replace(chr(92),'/')]={'application_bytes':sum(r['bytes_written'] for r in historical),'explicit_flush_calls':sum(r['flushes'] for r in historical)}
    values=[]
    for (device,kind,size),data in sorted(flush_groups.items()):
        if kind=='group_commit':continue
        values.append((device,kind,size//1024,span([r['p50_us'] for r in data]),span([r['p95_us'] for r in data]),span([r['p99_us'] for r in data]),span([r['flushes']/r['seconds'] for r in data],0),span([r['operations_per_sec'] for r in data],0),'; '.join(r['source'] for r in data)))
    table(lines,['Drive','Mode','KiB','p50 µs','p95 µs','p99 µs','Explicit flushes/s','Writes/s','Raw file:line'],values)
    lines.extend(['','## Group commit','', '2253 bytes per record (2.2 KiB rounded). Every batch has one explicit flush; each single-record and eight-record case has 200 samples, each 64/512-record case has 64 samples.'])
    values=[]
    for (device,kind,batch),data in sorted(flush_groups.items()):
        if kind!='group_commit':continue
        values.append((device,batch,span([r['p50_us'] for r in data]),span([r['p95_us'] for r in data]),span([r['p99_us'] for r in data]),span([r['operations_per_sec'] for r in data],0),span([r['flushes']/r['seconds'] for r in data],0),'; '.join(r['source'] for r in data)))
    table(lines,['Drive','Records/flush','p50 µs','p95 µs','p99 µs','Records/s','Flushes/s','Raw file:line'],values)
    lines.extend(['','## IPC','', 'Both languages use the same dependency-free Rust echo server executable, SHA256 recorded in every definitive row. No membus storage, serialization or subscription work occurs. Each client warms up for 16 echoes, then records 500 echoes with a unique client/request header. Python uses N spawned interpreter processes, Rust N client threads. Blocking TCP and TCP_NODELAY on both ends; byte-mode local named pipes with 64 KiB buffers. Connections and warmup are untimed. Rates are aggregate completed round trips/s for N closed-loop clients and include coordination/validation overhead; they are not per-client rates or server maximum capacity. Python start uses an Event and shared QPC end timestamps; Rust uses two barriers and final joins.'])
    ipc_groups=collections.defaultdict(list)
    definitive=[]
    for run,path in enumerate((raw/'ipc-final.jsonl',repeat/'ipc-repeat.jsonl'),1):
        data=rows(path);assert len(data)==36
        for n,r in enumerate(data,1):r['source']=str(path).replace(chr(92),'/')+':'+str(n)
        for row in data:
            assert row['samples']==row['clients']*500
            definitive.append(dict(run=run,**row));ipc_groups[(row['kind'],row['payload'],row['clients'])].append(row)
    assert len({r['exe_sha256'] for r in definitive})==1
    assert len({r['client_sha256'] for r in definitive})==1
    values=[]
    for (kind,size,clients),data in sorted(ipc_groups.items()):
        values.append((kind,size,clients,span([r['p50_us'] for r in data]),span([r['p99_us'] for r in data]),span([r['operations_per_sec'] for r in data],0),'; '.join(r['source'] for r in data)))
    table(lines,['Client/transport','Bytes','Clients','p50 µs','p99 µs','Aggregate round trips/s','Raw file:line'],values)
    lines.extend(['','## DiskSpd','', 'Microsoft v2.3 official ZIP SHA256 `18609981e8b04b4f645f1fd182ad0541615fe71722de36a9a933c54f747218a3`, matched the release asset digest. `trustcheck check microsoft/diskspd` returned `below thresholds`, Scorecard 3.3 < 5.0; the historical e1 runs preceded the required scan/review. e2 directory and ZIP scans both report ClamAV clean (exit 0); no GuardDog result is reported for this non-Python package. No DiskSpd ran in e2. Admission review and lane87 question receipt are located in the repeat folder. Each run is 15 s, one thread, queue depth one, 32 MiB target, no warmup/cooldown, unbuffered write-through (`-Sh`), 100% writes. Small-write runs use random 4 KiB; sequential runs use 64 KiB. These are observed QD1 write-through capacities under co-tenant load, not maximum device capacities. DiskSpd does not expose actual NVMe flush counts and issues no FlushFileBuffers loop. Its overwrite case avoids the std probe’s append allocation cost.'])
    values=[];disk_totals=collections.defaultdict(int)
    for device in ('C','D'):
        for kind in ('small','sequential'):
            for run in (1,2):
                root=ET.parse(raw/f'diskspd-{kind}-{device}-{run}.xml').getroot()
                seconds=float(root.findtext('.//TestTimeSeconds'))
                targets=root.findall('.//Thread/Target')
                count=sum(int(t.findtext('WriteCount','0')) for t in targets)
                amount=sum(int(t.findtext('WriteBytes','0')) for t in targets)
                if not amount:amount=sum(int(t.findtext('BytesCount','0')) for t in targets)
                disk_totals[device]+=amount
                latency=root.find('.//TimeSpan/Latency')
                if latency is None:latency=root.find('.//Latency')
                quant={float(b.findtext('Percentile')):float(b.findtext('WriteMilliseconds'))*1000 for b in latency.findall('Bucket')}
                values.append((device,kind,run,amount,count,round(amount/seconds/1048576,1),round(count/seconds,1),quant.get(50),quant.get(99)))
    table(lines,['Drive','Workload','Run','App write bytes','Writes','MiB/s','Writes/s','p50 µs','p99 µs'],values)
    lines.extend(['','## Device baseline','', 'PhysicalDisk cooked values are 5 s averages, excluding `_total`. No suite disk probes run inside either baseline window. Other agents’ work and the IPC experiment still appear in these shared-machine counters. E: is baseline-only: the first-log plan weighs C: and D:.'])
    values=[]
    for run in (1,2):
        data=rows(raw/f'baseline-{run}.jsonl')
        start=data[0]['utc'];end=data[-1]['utc']
        lines.extend(['',f'Baseline {run}: {start} through {end}, {len(data)} samples, processes {min(r["processes"] for r in data)}–{max(r["processes"] for r in data)}. Lineup snapshots: `baseline-{run}.jsonl.tenants.jsonl`.'])
        groups=collections.defaultdict(list)
        for row in data:
            for sample in row['samples']:
                assert sample['status']==0
                groups[(sample['instance'],sample['path'].rsplit('\\',1)[1])].append(sample['value'])
        for (instance,counter),nums in sorted(groups.items()):
            values.append((run,instance,counter,round(statistics.mean(nums),1),round(min(nums),1),round(max(nums),1)))
    table(lines,['Run','Device','Counter','Mean','Min','Max'],values)
    lines.extend(['','## Application load','', 'IPC wire bytes below include untimed warmup. Python pipe timing includes a bytearray slice copy on each write; TCP has no matching copy. Language order is reversed at eight clients, and remains Rust-first at one and 32 clients. These are measurement limitations, and the recorded host noise prevents treating a small difference as a settled transport advantage.', '',json.dumps({'std_flush_suite':totals,'diskspd_total_write_bytes_by_device':dict(disk_totals),'definitive_ipc_wire_bytes':sum(r['wire_bytes'] for r in definitive),'ipc_disk_payload_writes':0},indent=2).join(['```json\n','\n```'])])
    lines.extend(['','## Limits and source checks','',
      'Write-cache policy is decoded in `policy.py` / `write-cache-policy.jsonl`: both T700 disks report write-back enabled, write-through supported, FlushCacheSupported=true, UserDefinedPowerProtection=false and NVCacheEnabled=false. Microsoft’s [API documentation](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddstor/ns-ntddstor-_storage_write_cache_property) defines the fields. Its [classpnp source](https://github.com/microsoft/Windows-driver-samples/blob/main/storage/class/classpnp/src/utils.c#L1260) maps UserDefinedPowerProtection to DEV_POWER_PROTECTED. Registry device keys exist but Device Parameters/Disk and the CacheIsPowerProtected/UserWriteCacheSetting overrides were absent. The policy conclusion that Windows does not suppress flushes is an inference from the API and driver source, not a hardware command trace.',
      '', 'The manufacturer [T700 specification flyer](https://assets.micron.com/adobe/assets/urn%3Aaaid%3Aaem%3A3fd11ecf-6ec7-4271-90fb-78cbdcb55b5d/renditions/original/as/crucial-t700-b2c-product-flyer-en.pdf) was not successfully opened: both primary PDF addresses and the product page returned Request Rejected. No in-flight capacitor-backed power-loss guarantee was located; see e2 CRUCIAL-SOURCE.md. PLP is therefore unlocated from this primary source; absence of a published promise is not proof of absence. No power-loss experiment or actual NVMe command trace was run. Larger latency for FlushFileBuffers than for unflushed writes and the Windows policy support host-issued flushing; they do not prove the device honours flushes across a power cut.',
      '', 'Survey read: D:/dev/handoff/fast-local-store-survey.md SHA256 b128d2588930d54819fceae4047299e4591535763f7048d89c4cf31a9fcfd10f. Design record read SHA256 074d22267522e57ad4ed32e1d95aef64833358a41fc9908e1bf958d99d2ffcb6; this is the current e2 read; e1 review hashes remain in review-return.json. The survey’s 20–60 µs Python p50 and 100–200 µs p99 are workload-dependent estimates: compare them to the 1-client rows and separately to 8/32-process rows, not to one universal latency. Its 0.3–3 ms NVMe durable-write estimate describes some D: medians, but the loaded C: repeat has medians and tails far above it. Neither that estimate nor the two-run empirical range is a latency guarantee; this study measures API completion, not power-loss durability.',
      '', 'Observe reviews and each response are located in the raw folder’s review-return.json, review-followup-return.json and review-responses.md. No code dependencies were fetched. Build/test output stays in ignored target directories. Temporary C:/Users/xteam/AppData/Local/Temp/membus-F-E6-2-probe.bin and the raw-folder probe-D.bin are removed through cognition.remove; cleanup receipts are kept in the e2 repeat folder.'])
    lines.extend(['','## Build identity, repeat contexts and survey coverage','',
      'D: tables pair e1 flush-D-2 with e2 flush-D-repeat, both clean 599f33d at different UTC times. e1 flush-D-1 used an older binary and is excluded from this range; historical numbers remain in its raw file and earlier observations. C: retains e1 run1/run2, which are mixed builds and therefore not two runs of one build; every C: number is marked mixed-build evidence. C: was not remeasured. IPC tables pair e1 ipc-final with e2 ipc-repeat (500 timed echoes/client), both 599f33d, matching executable and client hashes in the rows. ipc-reviewed-1 (2000/client) is an older build and is excluded; ipc-reviewed-2 never ran. Two-run min/max is not a confidence interval. The repeated Get-FileHash failure is in flush-repeat-exit.txt; unchanged fallback records clean commit, and Python still records its executable/client hashes. No measurement code changed.',
      '', 'Survey 4.4: server memory get/put <1-2 us and >1M ops/s were not measured (echo server has no store). Rust named-pipe 5-20 us compares to single-client rows, varying by payload; 8/32-client tails cannot confirm a single-client estimate. Python 20-60 us p50 / 100-200 us p99 is workload dependent: many small named-pipe one-client rows are faster than those estimates; 50 KiB and multi-process rows add different costs. Per-client 15-40K ops/s is compatible with many one-client rows, not a universal bound. Consumer flush 0.3-3 ms fits D: medians but D: sparse tails reach about 13-25 ms and loaded C: medians of 13-25 ms exceed it. Enterprise PLP 50-150 us was not measured. Group commit raises records/s as shown; QD1 probes do not establish GB/s WAL capacity or a server maximum.',
      '', 'Each DiskSpd table row maps by drive/workload/run to raw diskspd-<workload>-<drive>-<run>.xml, fields TimeSpan/Thread/Target WriteBytes/BytesCount, WriteCount, TimeSpan/TestTimeSeconds and TimeSpan/Latency/Bucket. Baseline rows map to baseline-<run>.jsonl, matching instance and counter in each samples element. Baseline ranges are over all cooked samples, not paired latency runs. Load JSON totals sum every bytes_written/flushes in the corresponding JSONL file, including untimed control drains. Historical e1 std writes are on both devices; repeat writes are D: only.',
      '', 'Admission and cleanup: e2 DISKSPD-ADMISSION.md, CRUCIAL-SOURCE.md and REPORT.md. Raw data remains outside git.'])
    for name,slot in [('flush-D-repeat.jsonl','cpu=2,mem=1,disk:d'),('ipc-repeat.jsonl','cpu=4,mem=1')]:
        contexts=rows(repeat/(name+'.context.jsonl'))
        provenance=json.loads((repeat/(name+'.provenance.json')).read_text(encoding='utf-8-sig'))
        assert contexts and all(s['status']==0 for c in contexts for s in c['physical'])
        lines.extend(['',f"Repeat {name}: {contexts[0]['utc']} through {contexts[-1]['utc']}; reserved {slot}; processes {min(c['processes'] for c in contexts)}-{max(c['processes'] for c in contexts)}; {len(contexts)} before/during/after PhysicalDisk samples; full co-tenants in {repeat}/{name}.context.jsonl. Git {provenance['git_commit']}; dirty {provenance['git_dirty']}."])
    output.write_text('\n'.join(lines)+'\n',encoding='utf-8',newline='\n')
    (repeat/'summary.json').write_text(json.dumps({'flush_load':totals,'diskspd_bytes':dict(disk_totals),'ipc_2253':[r for r in definitive if r['payload']==2253]},indent=2)+'\n',encoding='utf-8')

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--raw',type=pathlib.Path,required=True);parser.add_argument('--output',type=pathlib.Path,required=True);parser.add_argument('--repeat',type=pathlib.Path,required=True)
    args=parser.parse_args();summarize(args.raw,args.repeat,args.output)
