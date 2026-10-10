"""Small F-E6-48 account operations, preserving concurrent late-use entries."""
import datetime, json, msvcrt, os, pathlib, re, subprocess, sys, time

OUT = pathlib.Path('D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e9/act-work/F-E6-48')
INSTANCE = OUT.parents[3] / 'instance.json'
ENV = dict(os.environ, PYTHONPATH='D:/dev/runtime-pins/430c52c/cognition-workflows', PYTHONUTF8='1')

def append(name, row):
    row.setdefault('frontier_id', 'F-E6-48')
    row.setdefault('consumer', '*')
    row.setdefault('source', f'e9:act:F-E6-48:{name}:v1')
    path = OUT / f'{name}-entry.json'
    path.write_text(json.dumps(row, indent=2), encoding='utf-8')
    response = subprocess.run([sys.executable, '-m', 'cognition.ledger', 'append', '--instance', str(INSTANCE), '--entry', str(path)], env=ENV, capture_output=True, text=True)
    (OUT / f'{name}-append-receipt.txt').write_text(response.stdout + response.stderr, encoding='utf-8')
    if response.returncode:
        raise RuntimeError(response.stdout+response.stderr)
    print(name, response.stdout.strip())
    return response.stdout

def late_use(name, since=920):
    view = OUT / f'{name}-masked.md'
    response = subprocess.run([sys.executable, '-m', 'cognition.ledger', 'view', '--instance', str(INSTANCE), '--consumer', 'act', '--since-seq', str(since), '--output', str(view)], env=ENV, capture_output=True, text=True)
    if response.returncode:
        raise RuntimeError(response.stderr)
    content = view.read_text(encoding='utf-8')
    blocks = re.split(r'(?=^- E-[^\s]+ \(position \d+,)', content, flags=re.M)
    selected = []
    ids = []
    for block in blocks:
        match = re.match(r'- (E-[^\s]+) \(position (\d+),', block)
        if match and int(match[2]) > since and re.search(r'F-E6-48|F-E6-41|F-E6-47|prun|state_bytes|quota|membus|publisher|pause|stop', block, flags=re.I):
            selected.append(block.split('\n## ')[0])
            ids.append(dict(id=match[1], seq=int(match[2])))
    summary = OUT / f'{name}-relevant.md'
    summary.write_text('\n'.join(selected), encoding='utf-8')
    path = OUT.parents[1] / 'late-input-use.json'
    lock = path.with_suffix('.F-E6-48.lock')
    with lock.open('a+b') as stream:
        if stream.tell() == 0:
            stream.write(b'0'); stream.flush()
        for attempt in range(120):
            try:
                stream.seek(0); msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1); break
            except OSError:
                time.sleep(.05)
        else:
            raise RuntimeError('late-use lock timed out')
        try:
            # Atomic compare-and-retry protects writers not using this lock too.
            for attempt in range(100):
                before = path.read_bytes() if path.exists() else b'{}'
                data = json.loads(before.decode('utf-8-sig'))
                reads = data.setdefault('act', {}).setdefault('reads', [])
                reads.append(dict(frontier_id='F-E6-48', by='F-E6-48 engineer', at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    since_seq=since, view=str(view), relevant=str(summary), rows=ids,
                    use='Read masked Act view; only scoped measurement inputs affect F-E6-48; installation waits for completed stable-identity window; other frontiers stay with parent Act.'))
                tmp = path.with_suffix(f'.F-E6-48-{os.getpid()}.tmp')
                tmp.write_text(json.dumps(data, indent=2), encoding='utf-8')
                if (path.read_bytes() if path.exists() else b'{}') != before:
                    removed=subprocess.run([sys.executable,'-m','cognition.remove',str(tmp)],env=ENV,capture_output=True,text=True)
                    if removed.returncode:raise RuntimeError('temporary late-use cleanup failed: '+removed.stderr)
                    continue
                tmp.replace(path); break
            else:
                raise RuntimeError('concurrent late-use writes did not settle')
        finally:
            stream.seek(0); msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
    print(json.dumps(dict(relevant=str(summary), rows=ids)))

if __name__ == '__main__':
    late_use('late-start-current')
    for lane, session in [('lane87','f9ae75ca-c238-40b9-9042-54b212a9531d'), ('lane88','5aac0486-84ce-4e09-9d7c-4ba42dcea761')]:
        receipt = OUT / f'plan-{lane}-receipt.txt'
        append(f'plan-{lane}', dict(kind='observation', signal='lane-message', location=str(OUT/'PLAN.md'),
            text=f'Measurement plan sent before code to {lane} {session}. '+receipt.read_text(encoding='utf-8-sig').strip(), receipts=[str(receipt)]))
    append('window-start', dict(kind='observation', signal='measurement', location=str(OUT/'WINDOW-START.json'),
        text='Read-only resident window started 2026-10-10T01:20:40.912153Z; PID15524 epoch5; expected endpoint 02:20:40.912153Z. Timer frontier may install only after WINDOW-COMPLETE.json. Physical PHYSICALDRIVE2 D: sampled every10s; heartbeat identity every30s. Private tests do not touch resident.',
        locations=[str(OUT/'resident-start.json'),str(OUT/'disk-counters.jsonl')]))
