"""Read-only replay of membus's stripe records under candidate chunk-retirement forms (F-E6-52 plan, rival forms).
Classes: E3 (event/ keys in quota, slots, capacity; kept 3 days from chunk close), E1 (lease-claims; 24 h, leases plan
section 3), S (lane 88's rewritten state keys, lease-control, timers: latest version only), F (everything else: kept).
For each rotation age R, chunks are (stripe, class, R-bucket) cut also at 8 MiB. At the read time it reports the bytes
each form keeps: W (wait: an S chunk retires only when it holds no key's latest version; also with the 24 h retry
window), C (carry: an S chunk older
than the carry age has its latest versions re-appended to the open chunk, then retires), K (checkpoint: one snapshot
of every live latest version; every S chunk before it retires), plus the live set and the carry bytes an hour.
Record format as stripe_split.py. Writes nothing to membus.
Usage: python -I retire_sim.py STATE_DIR OUT.json"""
import collections
import datetime
import json
import pathlib
import struct
import sys
import time
import zlib

state, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
read_at = int(time.time() * 1000)
CHUNK_MAX = 8 * 1024 * 1024
HOUR = 3600 * 1000


def klass(ns, key):
    if ns in ('quota', 'slots', 'capacity') and key.startswith('event/'):
        return 'E3'
    if ns == 'lease-claims':
        return 'E1'
    if ns in ('quota', 'slots', 'lease-control', '__membus_timers_v1'):
        return 'S'
    return 'F'


RETAIN = {'E3': 3 * 24 * HOUR, 'E1': 24 * HOUR}
rows = []  # (stripe, order, at, ns, key, size, removed)
for i in range(4):
    path = state / f'stripe-{i}.log'
    limit = path.stat().st_size
    offset, order = 0, 0
    with path.open('rb') as stream:
        while offset + 16 <= limit:
            magic, n, inverse, crc = struct.unpack('<4sIII', stream.read(16))
            if magic != b'MBK1' or inverse != (n ^ 0xffffffff):
                raise ValueError(f'bad header {path}:{offset}')
            if offset + 16 + n > limit:
                break
            data = stream.read(n)
            if len(data) != n or zlib.crc32(data) != crc:
                break
            at, version, ack, removed = struct.unpack_from('<QQBB', data)
            pos, fields = 18, []
            for _ in range(3):
                size, = struct.unpack_from('<I', data, pos)
                pos += 4
                fields.append(data[pos:pos + size].decode('utf-8', 'replace'))
                pos += size
            rows.append((i, order, at, fields[0], fields[1], 16 + n, bool(removed)))
            order += 1
            offset += 16 + n

latest = {}
for r in rows:  # file order within a stripe; a key lives in one stripe
    latest[(r[3], r[4])] = r
live = collections.Counter()
live_keys = collections.Counter()
removed_latest = collections.Counter()
for (ns, key), r in latest.items():
    live[klass(ns, key)] += r[5]
    live_keys[klass(ns, key)] += 1
    removed_latest[klass(ns, key)] += r[6]
total = collections.Counter()
for r in rows:
    total[klass(r[3], r[4])] += r[5]
first = min(r[2] for r in rows)

report = dict(read_at=datetime.datetime.fromtimestamp(read_at / 1000, datetime.timezone.utc).isoformat(),
              records=len(rows), first_record=datetime.datetime.fromtimestamp(first / 1000, datetime.timezone.utc).isoformat(),
              stored_bytes_by_class=dict(total), live_latest_bytes_by_class=dict(live), live_keys_by_class=dict(live_keys),
              latest_is_tombstone_by_class=dict(removed_latest), forms={})
for R in (60 * 1000, HOUR):
    chunks = {}  # (stripe, class, bucket, part) -> [bytes, close_ms, latest_count]
    cur = {}
    for r in rows:
        c = klass(r[3], r[4])
        bucket = (r[2] - first) // R
        k = (r[0], c, bucket)
        part, size = cur.get(k, (0, 0))
        if size + r[5] > CHUNK_MAX:
            part, size = part + 1, 0
        cur[k] = (part, size + r[5])
        ck = k + (part,)
        e = chunks.setdefault(ck, [0, first + (bucket + 1) * R, 0])
        e[0] += r[5]
        if latest[(r[3], r[4])] is r:
            e[2] += 1
    form = dict(chunks=len(chunks), chunks_by_class=dict(collections.Counter(k[1] for k in chunks)))
    kept = collections.Counter()
    for (s, c, b, p), (size, close, nlatest) in chunks.items():
        if c in RETAIN:
            if close > read_at or close + RETAIN[c] > read_at:
                kept['E'] += size
        elif c == 'S':
            if close > read_at or nlatest:
                kept['S_wait'] += size
        else:
            kept['F'] += size
    form['E_kept_bytes'] = kept['E']
    form['F_kept_bytes'] = kept['F']
    form['S_wait_kept_bytes'] = kept['S_wait']
    form['S_wait_pinned_chunks'] = sum(1 for (s, c, b, p), v in chunks.items() if c == 'S' and v[1] <= read_at and v[2])
    form['S_closed_chunks'] = sum(1 for (s, c, b, p), v in chunks.items() if c == 'S' and v[1] <= read_at)
    # W with the 24 h retry window: a superseded S chunk also waits until close + 24 h, so a retried op_id still meets
    # its dedup entry after a restart rebuilds the table from the retained records.
    form['S_wait_retry24h_kept_bytes'] = sum(v[0] for (s, c, b, p), v in chunks.items()
                                             if c == 'S' and (v[1] + 24 * HOUR > read_at or v[2]))
    form['S_largest_chunk_bytes'] = max(v[0] for (s, c, b, p), v in chunks.items() if c == 'S')
    form['S_wait_pinned_largest_chunk_bytes'] = max([v[0] for (s, c, b, p), v in chunks.items()
                                                     if c == 'S' and v[1] <= read_at and v[2]] or [0])
    for carry_h in (1, 24):
        C = carry_h * HOUR
        young = sum(v[0] for (s, c, b, p), v in chunks.items() if c == 'S' and v[1] + C > read_at)
        stale = [r for (ns, key), r in latest.items() if klass(ns, key) == 'S' and r[2] < read_at - C]
        form[f'S_carry_{carry_h}h_kept_bytes'] = young + sum(r[5] for r in stale)
        form[f'S_carry_{carry_h}h_carry_bytes_per_hour'] = round(sum(r[5] for r in stale) / carry_h, 1)
        form[f'S_carry_{carry_h}h_keys_carried_per_period'] = len(stale)
    report['forms'][f'rotate_{R // 1000}s'] = form
report['S_checkpoint_snapshot_bytes'] = live['S']
out.write_text(json.dumps(report, indent=1), encoding='utf-8')
print(json.dumps(report, indent=1))
