"""Read each operational loop ledger once, projecting only row identity/time."""
import datetime, hashlib, json, os, pathlib, time
from window import OUT, save, utc
from run_recorded import context

def timestamp(value):
    if isinstance(value,(int,float)):return float(value)
    if isinstance(value,str):
        try:return datetime.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
        except ValueError:return None
    return None

def load_once(path):
    started=utc()
    before=path.stat()
    data=path.read_bytes()
    after=path.stat()
    return data,dict(path=str(path),bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),
        read_started=started,read_finished=utc(),mtime_before=before.st_mtime,mtime_after=after.st_mtime,
        changed_during_read=before.st_mtime_ns!=after.st_mtime_ns or before.st_size!=after.st_size)

def main():
    now=time.time();floor=now-86400
    result=dict(measured_at=utc(now),horizon_start=utc(floor),horizon_hours=24,
        method='One disk:d pass of operational research instances excluding runs/acts/tests, discarded folders, evidence snapshots and calibration fixtures. Deduplicate resolved ledger paths; read snapshot and active journal once each. Extract row id,seq,at only; no goal content. All kinds count as a publisher append. No retry/re-read under concurrent folds; file changes and journal coverage are reported.',
        context_before=context(),loops=[],excluded=[],errors=[])
    seen=set()
    retry_path=OUT/'ledger-rates.json'
    retry=__import__('sys').argv[1:]==['--retry-errors']
    previous=json.loads(retry_path.read_text()) if retry else None
    if previous:
        result=previous
        pending={row['instance'] for row in previous['errors']}
        result['prior_path_errors']=previous['errors']
        result['errors']=[]
        result['retry_at']=utc()
        now=datetime.datetime.fromisoformat(result['measured_at']).timestamp();floor=now-86400
        seen={os.path.normcase(row['ledger']) for row in previous['loops']}
    for raw in (OUT/'instances.txt').read_text(encoding='utf-8-sig').splitlines():
        instance=pathlib.Path(raw)
        if previous and str(instance) not in pending:continue
        if any(part in ('_discarded','evidence','calibration') for part in instance.parts):
            result['excluded'].append(dict(instance=str(instance),reason='historical/fixture'));continue
        try:
            info=json.loads(instance.read_text(encoding='utf-8-sig'))
            if not info.get('ledger'):
                result['excluded'].append(dict(instance=str(instance),reason='no ledger'));continue
            path=pathlib.Path(info['ledger'])
            if not path.is_absolute():path=pathlib.Path(info.get('root',instance.parent))/path
            path=path.resolve()
            identity=os.path.normcase(str(path))
            if identity in seen:
                result['excluded'].append(dict(instance=str(instance),reason='same resolved ledger'));continue
            seen.add(identity)
            data,read=load_once(path)
            snapshot=json.loads(data.decode('utf-8-sig'))
            records=snapshot['entries']
            reads=[read]
            journal_name=snapshot.get('journal',{}).get('file','ledger.journal.jsonl')
            journal=path.parent/journal_name
            journal_included=False
            if info.get('storage')=='journal' or snapshot.get('journal'):
                if journal.exists():
                    data,read=load_once(journal);reads.append(read)
                    for line in data.decode('utf-8-sig').splitlines():
                        if line.strip():records.append(json.loads(line))
                    journal_included=True
            rows={}
            for i,row in enumerate(records):
                identity=row.get('id',f'seq:{row.get("seq",i)}')
                rows[identity]=dict(seq=row.get('seq'),at=timestamp(row.get('at')))
            dated=[row for row in rows.values() if row['at'] is not None]
            trailing=[row for row in dated if floor<=row['at']<=now]
            key='ledger/'+str(instance.resolve())
            key_bytes=len(key.encode('utf-8'))
            # MBK1:16 header+18 fixed+4*4 length prefixes+8 expected + ns/key/op/value.
            record_overhead=16+18+16+8+len(b'cognition')+key_bytes+36
            result['loops'].append(dict(instance=str(instance.resolve()),ledger=str(path),storage=info.get('storage','single-file'),
                rows_total=len(rows),dated_rows=len(dated),undated_rows=len(rows)-len(dated),rows_last_24h=len(trailing),
                rows_per_hour=len(trailing)/24,oldest_row_at=utc(min(r['at'] for r in dated)) if dated else None,
                most_recent_at=utc(max(r['at'] for r in dated)) if dated else None,
                journal_included=journal_included,reads=reads,publisher_key=key,publisher_key_utf8_bytes=key_bytes,
                assumption_op_id_utf8_bytes=36,assumption_value_upper_bytes=199,
                mbk1_bytes_per_append_upper=record_overhead+199,
                publisher_upper_bytes_per_hour=(record_overhead+199)*len(trailing)/24))
        except Exception as error:
            result['errors'].append(dict(instance=str(instance),error=repr(error)))
    result['total_rows_last_24h']=sum(row['rows_last_24h'] for row in result['loops'])
    result['total_rows_per_hour']=sum(row['rows_per_hour'] for row in result['loops'])
    result['publisher_upper_bytes_per_hour']=sum(row['publisher_upper_bytes_per_hour'] for row in result['loops'])
    result['context_after']=context()
    result['limitations']='Operational locations found under this research root, not a machine-wide loop census; stopped historical ledgers can retain last24h rows, so all-found rate is an upper deployment scenario. Counts divide by24 even for younger ledgers. No fold race observed only means sampled mtime/size unchanged, not a transactional snapshot. Publisher not adopted; op_id36bytes and value199bytes are explicit assumptions, no upper bound on a future chosen op_id.'
    save('ledger-rates.json',result)
    print(json.dumps({k:result[k] for k in ['total_rows_last_24h','total_rows_per_hour','publisher_upper_bytes_per_hour','errors']}))

if __name__=='__main__':main()
