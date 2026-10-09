"""Small Windows resident witnesses; every owned process stopped in finally."""
import argparse, json, os, pathlib, struct, subprocess, sys, time
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'python')); sys.path.insert(0,str(ROOT/'tests'))
from integration import Server
from membus import Client, Pipe, Refusal, deadline_ms

def main():
    p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True);p.add_argument('--state-cap',action='store_true');args=p.parse_args()
    out=pathlib.Path(args.output);out.mkdir(parents=True,exist_ok=True)
    result={'pass_conditions':['pipe partial header and body close within idle bound plus1s tolerance','os/flush acknowledged keys equal replay after kill and torn tail; new epoch','heartbeat bytes/counts match observed updates; only bounded files','state cap refuses before acceptance and old reads survive'], 'method':'std-only server and Python stdlib client; isolated D: state; kill via subprocess.kill', 'physical_device_bytes':'unmeasured','process_count':len(subprocess.check_output(['tasklist','/fo','csv','/nh']).splitlines())}
    server=None
    try:
        flags=['--idle-ms','200','--heartbeat-ms','250','--flush-ms','0']
        server=Server(args.binary,out/'state',out/'server.txt','pipe',flags)
        for data in (b'\x0d',struct.pack('<I',13)+b'\x01\x00'):
            raw=Pipe(server.address,deadline_ms()); raw.io(data,len(data),deadline_ms(),True)
            start=time.monotonic();time.sleep(.35)
            try: raw.io(None,1,deadline_ms(1),False)
            except OSError: pass
            else: raise AssertionError('idle partial frame stayed live')
            finally:raw.close()
            result.setdefault('idle_close_seconds',[]).append(time.monotonic()-start)
            assert result['idle_close_seconds'][-1]<1.2
        c=server.client; expected={}
        for ack in (1,2):
            key='ack/'+str(ack); expected[key]=('value/'+str(ack)).encode(); assert c.put('resident',key,expected[key],ack=ack)['ack']==ack
        epoch=c.epoch
        samples=[];start=time.monotonic()
        while time.monotonic()-start<1.1:
            try: item=json.loads((out/'state'/'heartbeat.json').read_text()); length=(out/'state'/'heartbeat.json').stat().st_size
            except (FileNotFoundError,json.JSONDecodeError):time.sleep(.01);continue
            assert length<256
            if not samples or samples[-1]['heartbeat_writes']!=item['heartbeat_writes']:
                item['object_bytes']=length;samples.append(item)
            time.sleep(.015)
        assert len(samples)>=3
        for before,after in zip(samples,samples[1:]):
            assert after['heartbeat_writes']==before['heartbeat_writes']+1
            assert after['heartbeat_bytes']-before['heartbeat_bytes']==after['object_bytes']
        result['heartbeat']=samples
        check=[str(args.binary),'health',server.address,'--state-dir',str(out/'state')]
        healthy=subprocess.run(check,capture_output=True,text=True)
        assert healthy.returncode==0,(healthy.stdout,healthy.stderr)
        heartbeat_path=out/'state'/'heartbeat.json'
        # Keep a mismatch stable by using the other fixture directory, without
        # racing the real server's bounded heartbeat writer.
        wrong=out/'wrong-state';wrong.mkdir(exist_ok=True)
        bad=dict(samples[-1],pid=0);(wrong/'heartbeat.json').write_text(json.dumps(bad,separators=(',',':')))
        unhealthy=subprocess.run(check[:-1]+[str(wrong)],capture_output=True,text=True)
        assert unhealthy.returncode!=0
        result['native_health']={'matching_pid_epoch':healthy.stdout.strip(),'wrong_pid_exit':unhealthy.returncode}
        server.stop();server=None
        stripe=next((out/'state').glob('stripe-*.log'))
        with stripe.open('ab') as f:f.write(b'MBK')
        server=Server(args.binary,out/'state',out/'server.txt','pipe',flags)
        assert server.client.epoch!=epoch
        observed={key:server.client.get('resident',key)['value'] for key in expected}
        assert observed==expected
        result['replay']={'acknowledged_keys':sorted(expected),'replayed_keys':sorted(observed),'new_epoch':server.client.epoch,'torn_tail':'3-byte final header removed'}
        if args.state_cap:
            server.stop();server=None
            server=Server(args.binary,out/'cap-state',out/'cap-server.txt','pipe',flags+['--state-limit-bytes','250'])
            server.client.put('resident','first',b'first',ack=1)
            try:server.client.put('resident','too-big',b'x'*300,ack=1)
            except Refusal as e:assert e.code==7
            else:raise AssertionError('cap accepted')
            assert server.client.get('resident','too-big') is None
            assert server.client.get('resident','first')['value']==b'first'
            result['state_cap']='capacity7 before acceptance; old read succeeds'
        result['passed']=True
    finally:
        if server:server.stop()
        (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))
if __name__=='__main__':main()
