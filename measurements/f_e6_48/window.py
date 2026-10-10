"""F-E6-48: read-only resident window; metadata only, no stored values."""
import collections, datetime, hashlib, json, pathlib, struct, subprocess, sys, time, uuid, zlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'python'))
from membus import Client
OUT = pathlib.Path('D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e9/act-work/F-E6-48')
STATE = pathlib.Path('D:/projects/membus/state')
PIPE = r'\\.\pipe\LOCAL\membus-kv1'

def utc(t=None):
    return datetime.datetime.fromtimestamp(time.time() if t is None else t, datetime.timezone.utc).isoformat()

def save(name, data):
    path = OUT / name
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(data, indent=2), encoding='utf-8')
    tmp.replace(path)

def family(key):
    return key.split('/')[0] + '/' if '/' in key else key

def records(path, limit):
    rows = []
    offset = 0
    with path.open('rb') as stream:
        while offset + 16 <= limit:
            header = stream.read(16)
            magic, n, inverse, crc = struct.unpack('<4sIII', header)
            if magic != b'MBK1' or inverse != (n ^ 0xffffffff) or n > 16 * 1024 * 1024:
                raise ValueError(f'bad header {path}:{offset}')
            if offset + 16 + n > limit:
                break
            data = stream.read(n)
            if len(data) != n or zlib.crc32(data) != crc:
                raise ValueError(f'bad crc/length {path}:{offset}')
            at, version, ack, removed = struct.unpack_from('<QQBB', data)
            pos = 18
            fields = []
            for _ in range(3):
                size, = struct.unpack_from('<I', data, pos)
                pos += 4
                fields.append(data[pos:pos+size].decode('utf-8'))
                pos += size
            expected, size = struct.unpack_from('<QI', data, pos)
            pos += 12
            if pos + size != len(data):
                raise ValueError('record trailing bytes')
            ns, key, op_id = fields
            rows.append(dict(offset=offset, end=offset+16+n, at_ms=at, version=version,
                             ack=ack, removed=bool(removed), namespace=ns, key=key,
                             family=family(key), bytes=16+n, value_bytes=size,
                             op_id_bytes=len(op_id.encode('utf-8'))))
            offset += 16+n
    return rows, offset

class ReadOnlyClient(Client):
    def call(self, opcode, ns, suffix=b'', op_id=None, deadline=None):
        if opcode not in (2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 19, 20):
            raise RuntimeError('resident write prohibited by measurement tool')
        return super().call(opcode, ns, suffix, op_id or 'F-E6-48-read-' + str(uuid.uuid4()), deadline)

def capture(label):
    began = time.time()
    heartbeat = json.loads((STATE / 'heartbeat.json').read_text())
    logs = {}
    allrows = []
    for i in range(4):
        path = STATE / f'stripe-{i}.log'
        size = path.stat().st_size
        rows, parsed = records(path, size)
        logs[path.name] = dict(size=size, complete_bytes=parsed, partial_tail=size-parsed,
                               records=rows)
        allrows.extend(rows)
    namespaces = sorted(set(r['namespace'] for r in allrows) | {'quota'})
    client = ReadOnlyClient(PIPE, transport='pipe', timeout=10)
    health, scans = {}, {}
    try:
        epoch = client.epoch
        for ns in namespaces:
            health[ns] = client.health(ns)
            counts = collections.Counter()
            sizes = collections.Counter()
            versions = []
            with client.snapshot(ns) as token:
                cursor = None
                while True:
                    page = client.scan_prefix(ns, snapshot=token, cursor=cursor)
                    for item in page['items']:
                        group = family(item['key'])
                        counts[group] += 1
                        sizes[group] += len(item['value'])
                        versions.append(dict(key=item['key'], version=item['version'], value_bytes=len(item['value'])))
                    if page['done']:
                        break
                    cursor = page['cursor']
            scans[ns] = dict(live_keys=sum(counts.values()), family_counts=dict(counts),
                             family_value_bytes=dict(sizes), versions=versions)
    finally:
        client.close()
    lineup = subprocess.run(['lineup.cmd', 'status'], capture_output=True, text=True)
    context = subprocess.run(['powershell.exe','-NoProfile','-Command',
        "[pscustomobject]@{count=(Get-Process).Count;resident=(Get-Process -Id 15524 | Select-Object Id,StartTime,CPU,WorkingSet64)} | ConvertTo-Json -Depth 4"],
        capture_output=True, text=True)
    result = dict(label=label, began_at=utc(began), captured_at=utc(), unix_time=time.time(),
                  heartbeat=heartbeat, hello_epoch=epoch, logs=logs, health=health, scans=scans,
                  client_identity='F-E6-48-read-<uuid> operation IDs',
                  binary_sha256=hashlib.sha256(pathlib.Path('D:/projects/membus/bin/membus.exe').read_bytes()).hexdigest(),
                  lineup=dict(code=lineup.returncode, stdout=lineup.stdout, stderr=lineup.stderr),
                  processes=dict(code=context.returncode, stdout=context.stdout, stderr=context.stderr))
    save(f'resident-{label}.json', result)
    return result

def main():
    reused_collector = None
    if '--resume' in sys.argv:
        start = json.loads((OUT/'resident-start.json').read_text())
        marker = json.loads((OUT/'WINDOW-START.json').read_text())
        anchor, expected = marker['unix_time'], marker['expected_endpoint_unix']
        # Reuse only the original collector with the expected script and start time.
        first_row = json.loads((OUT/'disk-counters.jsonl').open(encoding='utf-8-sig').readline())
        original_pid = int(first_row['collector_pid'])
        query = f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={original_pid}'; if($p){{[pscustomobject]@{{pid=$p.ProcessId;started=$p.CreationDate.ToUniversalTime().ToString('o');command=$p.CommandLine}} | ConvertTo-Json -Compress}}"
        facts = subprocess.run(['powershell.exe','-NoProfile','-Command',query],capture_output=True,text=True)
        if facts.returncode == 0 and facts.stdout.strip():
            process = json.loads(facts.stdout)
            if (str(ROOT/'measurements/f_e6_48/disk.ps1').lower() in process['command'].lower()
                    and abs(datetime.datetime.fromisoformat(process['started']).timestamp()-anchor)<30):
                reused_collector = original_pid
        save('WINDOW-MONITOR-RESUME.json', dict(at=utc(), original_monitor_pid=marker['monitor_pid'],
             monitor_pid=__import__('os').getpid(), reason='background process disappeared without stderr or failure marker; cause not located',
             preserves_start=True, expected_endpoint=utc(expected), reused_disk_collector=reused_collector,
             sampling_gap='heartbeat missing sample interval retained; disk gaps are assessed per collector from raw timestamps'))
    else:
        start = capture('start')
        anchor = time.time()
        expected = anchor + 3600
        save('WINDOW-START.json', dict(started_at=utc(anchor), unix_time=anchor,
             expected_endpoint=utc(expected), expected_endpoint_unix=expected,
             resident_pid=start['heartbeat']['pid'], resident_epoch=start['hello_epoch'],
             heartbeat_epoch=start['heartbeat']['epoch'], duration_seconds=3600,
             monitor_pid=__import__('os').getpid(), start_reading=str(OUT / 'resident-start.json'),
             disk_counter_output=str(OUT / 'disk-counters.jsonl')))
    disklog = (OUT / 'disk-collector-log.txt').open('a')
    collector = None if reused_collector else subprocess.Popen(['powershell.exe','-NoProfile','-WindowStyle','Hidden','-File',
                                 str(ROOT / 'measurements/f_e6_48/disk.ps1')], stdout=disklog, stderr=subprocess.STDOUT,
                                 creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        while time.time() < expected:
            heartbeat = json.loads((STATE / 'heartbeat.json').read_text())
            with (OUT / 'heartbeat-series.jsonl').open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(dict(observed_at=utc(), heartbeat=heartbeat))+'\n')
            if heartbeat['pid'] != start['heartbeat']['pid'] or heartbeat['epoch'] != start['hello_epoch']:
                raise RuntimeError('resident identity changed inside window')
            time.sleep(min(30, max(0, expected-time.time())))
        end = capture('end')
        same = (start['heartbeat']['pid'], start['hello_epoch'], start['binary_sha256']) == (end['heartbeat']['pid'], end['hello_epoch'], end['binary_sha256'])
        elapsed = end['unix_time'] - anchor
        if not same or elapsed < 3600 or end['heartbeat']['epoch'] != start['hello_epoch']:
            raise RuntimeError('resident endpoint identity/duration invalid')
        save('WINDOW-COMPLETE.json', dict(completed_at=utc(), duration_seconds=elapsed,
             started_at=utc(anchor), expected_endpoint=utc(expected), resident_pid=end['heartbeat']['pid'],
             resident_epoch=end['hello_epoch'], same_identity=same,
             readings=[str(OUT/'resident-start.json'), str(OUT/'resident-end.json')],
             disk_counter_output=str(OUT/'disk-counters.jsonl'), timer_frontier_may_install=True))
    except BaseException as error:
        save('WINDOW-FAILED.json', dict(at=utc(), error=repr(error)))
        raise
    finally:
        if collector is not None:
            try:
                collector.wait(timeout=25)
            except subprocess.TimeoutExpired:
                collector.terminate()
                collector.wait(timeout=10)
        disklog.close()

if __name__ == '__main__':
    main()
