"""Small Windows resident witnesses; every owned process stopped in finally."""
import argparse, json, os, pathlib, struct, subprocess, sys, time, uuid
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
        address=r'\\.\pipe\LOCAL\membus-resident-test-'+str(uuid.uuid4())
        server=Server(args.binary,out/'state',out/'server.txt','pipe',flags,address=address)
        for data in (b'\x0d',struct.pack('<I',13)+b'\x01\x00'):
            raw=Pipe(server.address,deadline_ms()); raw.io(data,len(data),deadline_ms(),True)
            start=time.monotonic()
            try: raw.io(None,1,deadline_ms(1),False)
            except TimeoutError: raise AssertionError('client timed out instead of server closing')
            except OSError as error:
                result.setdefault('idle_errors',[]).append({'type':type(error).__name__,'winerror':getattr(error,'winerror',None)})
            else: raise AssertionError('idle partial frame stayed live')
            finally:raw.close()
            result.setdefault('idle_close_seconds',[]).append(time.monotonic()-start)
            assert .1<=result['idle_close_seconds'][-1]<1.2
        c=server.client; expected={}
        for ack in (1,2):
            key='ack/'+str(ack); expected[key]=('value/'+str(ack)).encode(); assert c.put('resident',key,expected[key],ack=ack)['ack']==ack
        epoch=c.epoch
        samples=[];start=time.monotonic()
        while time.monotonic()-start<1.1:
            try: item=json.loads((out/'state'/'heartbeat.json').read_text()); length=(out/'state'/'heartbeat.json').stat().st_size
            except (FileNotFoundError,json.JSONDecodeError):time.sleep(.01);continue
            assert length<512
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
        # Force failed replacement with a Win32 handle denying delete sharing.
        import ctypes
        from ctypes import wintypes as w
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,w.LPVOID,w.DWORD,w.DWORD,w.HANDLE];kernel.CreateFileW.restype=w.HANDLE
        kernel.CloseHandle.argtypes=[w.HANDLE]
        locked=kernel.CreateFileW(str(heartbeat_path),0x80000000,3,None,3,0,None)
        assert locked!=ctypes.c_void_p(-1).value
        try: time.sleep(.6)
        finally: kernel.CloseHandle(locked)
        time.sleep(.4)
        recovered=json.loads(heartbeat_path.read_text())
        assert recovered['heartbeat_failures']>=1
        assert recovered['heartbeat_attempts']>recovered['heartbeat_writes']
        assert recovered['heartbeat_bytes_attempted']>recovered['heartbeat_bytes']
        result['heartbeat_failure_recovery']=recovered
        peers=[]
        try:
            for _ in range(64):peers.append(Pipe(server.address,deadline_ms()))
            busy_started=time.monotonic()
            health_busy=subprocess.run(check,capture_output=True,text=True)
            busy_seconds=time.monotonic()-busy_started
            assert health_busy.returncode==0,(health_busy.stdout,health_busy.stderr)
            assert busy_seconds<2.5
            result['native_health_busy']={'seconds':busy_seconds,'exit':health_busy.returncode,'slots_held':len(peers)}
        finally:
            for peer in peers:peer.close()
        stale=Pipe(server.address,deadline_ms())
        stale.io(b'\x0d',1,deadline_ms(),True)
        kill_started=time.monotonic()
        server.client.close()
        server.stop();server=None
        kill_seconds=time.monotonic()-kill_started
        assert kill_seconds<.2,'stale server IO could have idled out before kill'
        # The newest chunk was open at the kill, so its torn tail is truncated at replay (CHUNK-RETIREMENT.md 5.5).
        stripe=max((p for p in (out/'state').glob('chunks/*/*.log') if p.stat().st_size),key=lambda p:int(p.stem.split('-')[1]))
        before_tail=stripe.stat().st_size
        with stripe.open('ab') as f:f.write(b'MBK')
        server=Server(args.binary,out/'state',out/'server.txt','pipe',flags,address=address)
        try:stale.io(None,1,deadline_ms(1),False)
        except TimeoutError:raise AssertionError('stale client timed out instead of observing kill')
        except OSError as error:
            result['stale_client']={'winerror':getattr(error,'winerror',None),'kill_seconds':kill_seconds}
        else:raise AssertionError('old pipe delivered data after kill')
        stale.close()
        assert stripe.stat().st_size==before_tail
        assert server.client.epoch!=epoch
        observed={key:server.client.get('resident',key)['value'] for key in expected}
        assert observed==expected
        result['replay']={'acknowledged_keys':sorted(expected),'replayed_keys':sorted(observed),'new_epoch':server.client.epoch,'pipe_reused_with_stale_client':address,'torn_tail':'3-byte final header removed after acknowledged record','torn_stripe':stripe.name}
        if args.state_cap:
            server.stop();server=None
            server=Server(args.binary,out/'cap-state',out/'cap-server.txt','pipe',flags+['--state-limit-bytes','250'])
            server.client.put('resident','first',b'x'*100,ack=1)
            try:server.client.put('resident','too-big',b'x'*300,ack=1)
            except Refusal as e:assert e.code==7
            else:raise AssertionError('cap accepted')
            assert server.client.get('resident','too-big') is None
            assert server.client.get('resident','first')['value']==b'x'*100
            time.sleep(.3)
            pressure=json.loads((out/'cap-state'/'heartbeat.json').read_text())
            assert pressure['state_high_water'] is True
            assert 187<=pressure['state_bytes']<=250
            assert pressure['state_limit_bytes']==250
            result['state_pressure']=pressure
            result['state_cap']='capacity7 before acceptance; old read succeeds'
        result['passed']=True
    finally:
        if server:server.stop()
        (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))
if __name__=='__main__':main()
