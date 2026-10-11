"""Read-only split of membus's stripe logs by writer (namespace and key family) over time windows, with the
24 h distinct op_id count (the dedup table's occupancy proxy). Record format as membus measurements/f_e6_48/window.py
(MBK1 header, crc32 body). Reads each stripe file up to its size at open; writes nothing to membus.
Usage: python -I stripe_split.py STATE_DIR OUT.json"""
import collections
import datetime
import json
import pathlib
import struct
import sys
import time
import zlib

state, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
read_at = time.time()


def family(key):
    return key.split('/')[0] + '/' if '/' in key else key


rows = []
files = {}
for i in range(4):
    path = state / f'stripe-{i}.log'
    limit = path.stat().st_size
    files[path.name] = limit
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
                break  # a record still being written at the read's end
            at, version, ack, removed = struct.unpack_from('<QQBB', data)
            pos = 18
            fields = []
            for _ in range(3):
                size, = struct.unpack_from('<I', data, pos)
                pos += 4
                fields.append(data[pos:pos + size].decode('utf-8', 'replace'))
                pos += size
            ns, key, op_id = fields
            rows.append((at, ns, family(key), key, op_id, 16 + n, bool(removed)))
            offset += 16 + n

now_ms = int(read_at * 1000)
iso = lambda ms: datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).isoformat()
windows = {
    'since_2026-10-11T00:44Z': int(datetime.datetime(2026, 10, 11, 0, 44, tzinfo=datetime.timezone.utc).timestamp() * 1000),
    'since_2026-10-11T07:21Z': int(datetime.datetime(2026, 10, 11, 7, 21, tzinfo=datetime.timezone.utc).timestamp() * 1000),
    'last_24h': now_ms - 24 * 3600 * 1000,
    'last_6h': now_ms - 6 * 3600 * 1000,
}
report = dict(read_at=iso(now_ms), state=str(state), stripe_bytes=files, total_stripe_bytes=sum(files.values()),
              records=len(rows), first_record=iso(min(r[0] for r in rows)), last_record=iso(max(r[0] for r in rows)),
              windows={})
for name, since in windows.items():
    sel = [r for r in rows if r[0] >= since]
    hours = (now_ms - since) / 3600000
    by = collections.defaultdict(lambda: [0, 0])
    for r in sel:
        by[r[1] + ' ' + r[2]][0] += 1
        by[r[1] + ' ' + r[2]][1] += r[5]
    ops = {r[4] for r in sel if r[4]}
    report['windows'][name] = dict(
        since=iso(since), hours=round(hours, 3), records=len(sel), bytes=sum(r[5] for r in sel),
        bytes_per_hour=round(sum(r[5] for r in sel) / hours, 1), records_per_hour=round(len(sel) / hours, 1),
        distinct_op_ids=len(ops), removes=sum(1 for r in sel if r[6]),
        by_writer={k: dict(records=v[0], bytes=v[1], bytes_per_hour=round(v[1] / hours, 1))
                   for k, v in sorted(by.items(), key=lambda kv: -kv[1][1])})
day = report['windows']['last_24h']
report['dedup'] = dict(table_entries=262144, distinct_op_ids_last_24h=day['distinct_op_ids'],
                       occupancy=round(day['distinct_op_ids'] / 262144, 4),
                       note='Distinct op_ids among records appended in the last 24 h; the table also holds op_ids of puts whose records it deduplicated and of refused puts, which leave no record, so this is a lower bound of its occupancy.')
keys = collections.Counter((r[1], r[3]) for r in rows)
report['key_slots'] = dict(distinct_keys_all_time=len(keys),
                           per_namespace=dict(collections.Counter(ns for ns, _ in keys)))
out.write_text(json.dumps(report, indent=1), encoding='utf-8')
print(json.dumps({k: report[k] for k in ('read_at', 'total_stripe_bytes', 'records', 'first_record', 'last_record', 'dedup', 'key_slots')}, indent=1))
for name, w in report['windows'].items():
    print(name, w['hours'], 'h', w['bytes'], 'B', w['bytes_per_hour'], 'B/h', w['records_per_hour'], 'rec/h', 'ops', w['distinct_op_ids'])
    for k, v in list(w['by_writer'].items())[:14]:
        print('   ', k, v)
