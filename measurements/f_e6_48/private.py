"""Mutation and synthetic replay measurements: private state and pipe only."""
import hashlib, json, os, pathlib, struct, subprocess, sys, time, uuid, zlib
from window import OUT, ROOT, utc, save
from membus import Client
from run_recorded import usage, context

BINARY=ROOT/'target/release/membus.exe'
SMALL=OUT/'private-state'
BIG=ROOT/'.bench/F-E6-48-synthetic'
NS='measure'

def lengths(state):
    return {p.name:p.stat().st_size for p in sorted(state.glob('stripe-*.log'))}

class Server:
    def __init__(self,state,label):
        self.log=(OUT/f'private-{label}-server.txt').open('ab',buffering=0)
        self.address=r'\\.\pipe\LOCAL\membus-F-E6-48-'+str(uuid.uuid4())
        assert 'membus-kv1' not in self.address and state != pathlib.Path('D:/projects/membus/state')
        self.began=time.perf_counter()
        self.process=subprocess.Popen([str(BINARY),str(state),'pipe',self.address,'--heartbeat-ms','0'],
            stdout=self.log,stderr=self.log,creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            while time.perf_counter()-self.began < 120:
                if self.process.poll() is not None:
                    raise RuntimeError(f'private exited {self.process.returncode}')
                try:
                    self.client=Client(self.address,'pipe',timeout=.05)
                    self.client.timeout=30
                    self.client.health(NS)
                    self.ready_seconds=time.perf_counter()-self.began
                    break
                except (OSError,ConnectionError):
                    time.sleep(.005)
            else:
                raise TimeoutError('private readiness')
        except BaseException:
            self.stop();raise

    def snapshot(self,label,state):
        return dict(label=label,at=utc(),health=self.client.health(NS),stripe_bytes=lengths(state),
                    total_log_bytes=sum(lengths(state).values()),epoch=self.client.epoch,
                    pid=self.process.pid,process_usage=usage(self.process._handle))

    def stop(self):
        if hasattr(self,'client'):self.client.close()
        if self.process.poll() is None:self.process.kill()
        self.process.wait(timeout=15)
        self.log.close()

def small():
    if SMALL.exists():raise RuntimeError('refuse reuse private state')
    server=Server(SMALL,'small')
    stages=[]
    try:
        stages.append(server.snapshot('empty',SMALL))
        versions={}
        for label,value in [('put',b'p'*200),('overwrite',b'o'*200)]:
            began=time.perf_counter()
            for i in range(1000):
                key=f'key/{i:04d}'
                versions[key]=server.client.put(NS,key,value,ack=2,op_id=f'F-E6-48-{label}-{i:04d}')['version']
            reading=server.snapshot(label,SMALL)
            reading['elapsed_seconds']=time.perf_counter()-began
            stages.append(reading)
            assert reading['health']['namespace_keys']==1000
        began=time.perf_counter()
        for key,version in versions.items():
            server.client.remove(NS,key,version,ack=2,op_id='F-E6-48-remove-'+key)
        reading=server.snapshot('remove',SMALL)
        reading['elapsed_seconds']=time.perf_counter()-began
        stages.append(reading)
        assert reading['health']['namespace_keys']==1000
        assert all(server.client.get(NS,key) is None for key in versions)
        result=dict(method='1000 serial flush-acked puts, overwrites, removes; remove expected latest version; process kill then new private pipe; startup-to-hello+health readiness',
            stages=stages,binary=str(BINARY),binary_sha256=hashlib.sha256(BINARY.read_bytes()).hexdigest(),base='11c312c',context_before=context())
    finally:server.stop()
    restarted=Server(SMALL,'small-restart')
    try:
        result['restart']=restarted.snapshot('after-kill-restart',SMALL)
        result['restart']['startup_to_ready_seconds']=restarted.ready_seconds
        assert result['restart']['health']['namespace_keys']==1000
        assert result['restart']['total_log_bytes']==stages[-1]['total_log_bytes']
        assert result['restart']['epoch']==stages[-1]['epoch']+1
        assert all(restarted.client.get(NS,key) is None for key in versions)
    finally:restarted.stop()
    result['context_after']=context()
    save('private-small.json',result)
    return result

def generate():
    if BIG.exists():raise RuntimeError('refuse reuse synthetic state')
    BIG.mkdir(parents=True)
    files=[(BIG/f'stripe-{i}.log').open('wb') for i in range(4)]
    value=b'x'*1243
    ns=NS.encode()
    lp=lambda b:struct.pack('<I',len(b))+b
    began=time.perf_counter()
    try:
        for i in range(200000):
            key=f'key/{i:08d}'.encode()
            op=f'F-E6-48-synth-{i:08d}'.encode()
            body=struct.pack('<QQBB',1,i+1,1,0)+lp(ns)+lp(key)+lp(op)+struct.pack('<Q',0xffffffffffffffff)+lp(value)
            n=len(body)
            encoded=struct.pack('<4sIII',b'MBK1',n,n^0xffffffff,zlib.crc32(body))+body
            files[zlib.crc32(ns+b'\0'+key)%4].write(encoded)
    finally:
        for stream in files:
            stream.flush();os.fsync(stream.fileno());stream.close()
    return dict(state=str(BIG),records=200000,value_bytes_per_record=len(value),at_ms=1,
        reason='old timestamps exclude expired retry fingerprints; ordinary value capacity remains default256MiB',
        bytes=sum(lengths(BIG).values()),stripe_bytes=lengths(BIG),generate_seconds=time.perf_counter()-began)

def big():
    result=dict(context_before=context(),synthetic=generate(),runs=[],method='warm uncontrolled Windows cache; own encoded MBK1 records,200000 distinct keys,~256MiB; two private kill/restart reads; not a cold-device replay measurement')
    for label in ['big-first','big-restart']:
        server=Server(BIG,label)
        try:
            reading=server.snapshot(label,BIG)
            reading['startup_to_ready_seconds']=server.ready_seconds
            assert reading['health']['namespace_keys']==200000
            assert reading['health']['bytes_written']==0
            assert server.client.get(NS,'key/00000000')['value']==b'x'*1243
            assert server.client.get(NS,'key/00199999')['value']==b'x'*1243
            reading['endpoints_verified']=True
            result['runs'].append(reading)
        finally:server.stop()
    result['context_after']=context()
    save('private-big.json',result)
    return result

if __name__=='__main__':
    result=dict(small=small(),big=big())
    save('private-tests.json',result)
    print(json.dumps(dict(small_bytes=result['small']['restart']['total_log_bytes'],
        small_replay_seconds=result['small']['restart']['startup_to_ready_seconds'],
        synthetic_bytes=result['big']['synthetic']['bytes'],
        big_replays_seconds=[r['startup_to_ready_seconds'] for r in result['big']['runs']])))
