"""Lane83 seq402: fully flush-acked recorded-shape burst, kill and actual replay list."""
import argparse
import json
from pathlib import Path
import time
from integration import Server
from membus import deadline_ms

def run(args):
    output=Path(args.output);output.mkdir(parents=True,exist_ok=True)
    server=None
    acknowledged=[];values={};latencies=[]
    try:
        server=Server(args.binary,output/'state',output/'server.txt',flags=('--flush-ms',str(args.flush_ms)))
        started=time.monotonic();client=server.client
        for offset in range(0,3070,128):
            items=[]
            for index in range(offset,min(offset+128,3070)):
                seq=index+1;size=59499 if seq==1533 else (1039 if seq%2 else 1311)
                if seq>3065: size=16
                key=f'row/{seq:016d}';value=(f'op-{seq}:'.encode()+b'x'*size)[:size]
                items.append({'key':key,'value':value,'op_id':f'flush-op-{seq}','ack':2,'expected':0})
            tick=time.monotonic()
            results=client.batch('ledger',items,deadline=deadline_ms(30))
            latencies.append(time.monotonic()-tick)
            assert all(result['code']==0 and result['ack']==2 for result in results)
            for item in items: acknowledged.append(item['key']);values[item['key']]=item['value']
            print(f'flush-acked {len(acknowledged)}/3070 elapsed={time.monotonic()-started:.3f}s',flush=True)
        elapsed=time.monotonic()-started;metrics=client.health();epoch=client.epoch
        server.stop();server=Server(args.binary,output/'state',output/'restarted-server.txt',flags=('--flush-ms',str(args.flush_ms)))
        replayed=[]
        for offset in range(0,len(acknowledged),128):
            keys=acknowledged[offset:offset+128];read=server.client.get_many('ledger',keys)
            for key,value in zip(keys,read):
                assert value and value['ack']==2 and value['value']==values[key]
                replayed.append(key)
        missing=sorted(set(acknowledged)-set(replayed));assert not missing
        assert len(values['row/0000000000001533'])==59499
        result={'method':'single Python client, conditional row seq keys, serial non-atomic batches <=128; every write flush-acked; subprocess.kill then actual committed get_many replay',
                'configured_flush_ms':args.flush_ms,'acknowledged_flush_ids':len(acknowledged),'replayed_flush_ids':len(replayed),'missing_ids':missing,
                'largest_row_bytes':59499,'dense_seq':'1..3070','seconds':elapsed,'batch_seconds':latencies,
                'metrics':metrics,'epoch_before':epoch,'epoch_after':server.client.epoch,
                'startup_epoch_bytes':16,'startup_epoch_sync_calls':2,'physical_device_bytes':'unmeasured',
                'grouped_flush':False,'all_tests_passed':True}
        (output/'acknowledged-flush-ids.json').write_text(json.dumps(acknowledged),encoding='utf8')
        (output/'replayed-flush-ids.json').write_text(json.dumps(replayed),encoding='utf8')
        (output/'integration-flush.json').write_text(json.dumps(result,indent=2),encoding='utf8')
        print(json.dumps(result,indent=2),flush=True)
    finally:
        if server: server.stop()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,required=True);parser.add_argument('--output',required=True);parser.add_argument('--flush-ms',type=int,default=50)
    run(parser.parse_args())
