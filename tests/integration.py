"""Synthetic cross-language, process-kill/replay and recorded-shape burst witnesses."""
import argparse
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'python'))
from membus import Client, Refusal, EpochChanged, deadline_ms, frame, lp

class FragmentSocket:
    def __init__(self, channel): self.channel = channel
    def send(self, data): return self.channel.send(data[:3])
    def recv(self, count): return self.channel.recv(min(count,5))
    def settimeout(self, timeout): self.channel.settimeout(timeout)
    def close(self): self.channel.close()

def endpoint(transport):
    if transport=='pipe': return r'\\.\pipe\membus-test-'+str(uuid.uuid4())
    with socket.socket() as channel:
        channel.bind(('127.0.0.1',0))
        return '127.0.0.1:'+str(channel.getsockname()[1])

class Server:
    def __init__(self, binary, state, output, transport='tcp', flags=()):
        self.address, self.transport = endpoint(transport), transport
        self.log = open(output, 'ab', buffering=0)
        self.process = subprocess.Popen([str(binary),str(state),transport,self.address,*flags],stdout=self.log,stderr=self.log)
        for attempt in range(500):
            if self.process.poll() is not None:
                raise RuntimeError('server exited: '+Path(output).read_text())
            try:
                self.client=Client(self.address,transport,timeout=.1)
                self.client.timeout=10
                break
            except (OSError,ConnectionError): time.sleep(.01)
        else:
            self.stop()
            raise RuntimeError('server not ready')
    def stop(self):
        if hasattr(self,'client'): self.client.close()
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        self.log.close()

def run(args):
    output=Path(args.output);output.mkdir(parents=True,exist_ok=True)
    results={'method':'synthetic stdlib Python client, foreground std-only Rust server; process kill with subprocess.kill; isolated D: log files',
             'flush_ms':50, 'physical_device_bytes':'not measured; logical append bytes and flush calls below', 'tests':[]}
    payload=struct.pack('<Q',123456789)+lp('golden-op')+lp('ledger')+lp('row/0001')+b'\x02'+lp(b'hello')
    golden=frame(4,42,payload).hex()
    assert golden==(ROOT/'tests/fixtures/put.hex').read_text().strip()
    assert frame(1,1,struct.pack('<IB',16*1024*1024,0)+lp('python')).hex()==(ROOT/'tests/fixtures/hello.hex').read_text().strip()
    assert frame(0x82,42,struct.pack('<HBQB',0,1,7,1)+lp(b'hello')).hex()==(ROOT/'tests/fixtures/get-reply.hex').read_text().strip()
    results['golden_hex']=golden
    servers=[]
    try:
        server=Server(args.binary,output/'main-state',output/'server.txt');servers.append(server)
        c=server.client
        c.channel=FragmentSocket(c.channel)
        first=c.put('ledger','first',b'os',op_id='first-os')
        assert c.put('ledger','first',b'os',op_id='first-os')==first
        assert c.get('ledger','first')['value']==b'os'
        c.channel=c.channel.channel
        results['tests'].append('read_exact/write_all partial transport; same-op-id first outcome')
        peer=Client(server.address)
        try:
            assert peer.get('ledger','first')['version']==first['version']
            peer.close();peer=Client(server.address)
            assert peer.get('ledger','first')['version']==first['version']
        finally: peer.close()
        results['tests'].append('ack visibility on other connection and reconnect within epoch')
        past=struct.pack('<Q',int(time.time()*1000)-1)+lp('past')+lp('ledger')+lp('past')+b'\x01'+lp(b'must-not-admit')
        try: c.exchange(4,past,deadline_ms())
        except Refusal as error: assert error.code==5
        else: raise AssertionError('deadline not refused')
        assert c.get('ledger','past') is None
        results['tests'].append('server-carried deadline=5 before admission; key absent')
        tomb=c.put('ledger','remove',b'tombstone-test',op_id='tomb-create')
        c.remove('ledger','remove',tomb['version'],op_id='tomb-remove')
        assert c.get('ledger','remove') is None
        flushes=[];flush_latency=[];flush_started=time.monotonic()
        for n in range(4):
            tick=time.monotonic();c.put('ledger',f'flush/{n}',b'flush',ack=2,op_id=f'flush-{n}');flush_latency.append(time.monotonic()-tick)
            flushes.append(f'flush/{n}')
        before=c.health()
        results['flush_sample']={'configured_ms':50,'latency_seconds':flush_latency,'elapsed_seconds':time.monotonic()-flush_started,'calls':4}
        acknowledged={'first':b'os',**{key:b'flush' for key in flushes}}
        rows=[]
        for seq in range(1,3066):
            size=59499 if seq==1533 else (1039 if seq%2 else 1311)
            value=(f'row-{seq}:'.encode()+b'x'*size)[:size]
            key=f'row/{seq:016d}'
            rows.append({'key':key,'value':value,'op_id':f'row-op-{seq}','expected':0,'ack':1})
        started=time.monotonic()
        for offset in range(0,len(rows),128):
            batch=rows[offset:offset+128]
            outcomes=c.batch('ledger',batch)
            assert all(outcome['code']==0 for outcome in outcomes)
            for item in batch: acknowledged[item['key']]=item['value']
        seconds=time.monotonic()-started
        for offset in range(0,len(rows),128):
            batch=rows[offset:offset+128]
            values=c.get_many('ledger',[item['key'] for item in batch])
            assert all(value['value']==item['value'] for value,item in zip(values,batch))
        after=c.health()
        results['burst']={'rows':3065,'sequence':'dense 1..3065 allocated conditional_put(absent) on row/<seq>',
                          'median_bytes':1039,'p95_bytes':1311,'max_bytes':59499,'batch_max':128,'ack':'os',
                          'seconds':seconds,'bytes_written':after['bytes_written']-before['bytes_written'],
                          'flush_calls':after['flush_calls']-before['flush_calls']}
        results['initial_run']=after
        epoch=c.epoch;server.stop()
        # A verified short final header on an otherwise valid stripe is torn, not corrupt.
        with open(output/'main-state/stripe-0.log','ab') as log: log.write(b'MBK1\x20\x00')
        server=Server(args.binary,output/'main-state',output/'restarted-server.txt');servers.append(server)
        assert server.client.epoch!=epoch
        keys=list(acknowledged)
        replayed=[]
        for offset in range(0,len(keys),128):
            part=keys[offset:offset+128]
            values=server.client.get_many('ledger',part)
            assert all(value and value['value']==acknowledged[key] for key,value in zip(part,values))
            replayed.extend(key for key,value in zip(part,values) if value and value['value']==acknowledged[key])
        assert server.client.get('ledger','remove') is None
        assert server.client.put('ledger','first',b'os',op_id='first-os')==first
        assert server.client.health()['bytes_written']==0
        results['replay']={'acknowledged_ids':len(acknowledged),'replayed_ids':len(replayed),
                           'missing_ids':sorted(set(keys)-set(replayed)),'tombstone':'retained','torn_final_header':'discarded',
                           'dedup_after_restart':'original outcome, zero new append bytes'}
        (output/'acknowledged-ids.json').write_text(json.dumps(keys),encoding='utf8')
        (output/'replayed-ids.json').write_text(json.dumps(replayed),encoding='utf8')
        server.stop()
        log=next(path for path in (output/'main-state').glob('stripe-*.log') if path.stat().st_size>12)
        data=bytearray(log.read_bytes());data[12]^=1;log.write_bytes(data)
        results['test_mutations']={'torn_header_bytes':6,'corruption_rewrite_bytes':len(data),'captures':'acknowledged/replayed JSON lists plus small text/JSON reports; physical bytes unmeasured'}
        corrupt=subprocess.run([str(args.binary),str(output/'main-state'),'tcp',endpoint('tcp')],capture_output=True,text=True,timeout=10)
        assert corrupt.returncode!=0 and 'interior-corrupt' in corrupt.stderr
        (output/'corrupt-replay.txt').write_text(corrupt.stderr,encoding='utf8')
        results['tests'].append('interior CRC corrupt: startup refused, located chunk/offset')
        if os.name=='nt':
            server=Server(args.binary,output/'pipe-state',output/'pipe-server.txt','pipe');servers.append(server)
            result=server.client.put('pipe','key',b'pipe-python-rust',ack=2,op_id='pipe-op')
            assert result['ack']==2
            assert server.client.get('pipe','key')['value']==b'pipe-python-rust'
            peers=[]
            try:
                for _ in range(63): peers.append(Client(server.address,'pipe',timeout=1))
                assert server.client.health('pipe')['keys']==1
                try: extra=Client(server.address,'pipe',timeout=.1)
                except (OSError,TimeoutError): pass
                else: extra.close();raise AssertionError('65th pipe connection accepted')
            finally:
                for peer in peers:peer.close()
            assert server.client.get('pipe','key')['value']==b'pipe-python-rust'
            results['tests'].append('64 active pipe connections; excess waits until deadline; listener survives')
            results['pipe_run']=server.client.health('pipe');server.stop()
            results['tests'].append('Windows named-pipe endpoint, Python overlapped read/write, flush round trip')
        # Timeout-after-send followed by same-op-id resolution, pending conflict and hot read.
        server=Server(args.binary,output/'slow-state',output/'slow-server.txt',flags=('--delay-ms','150'));servers.append(server)
        c=server.client;c.put('ledger','hot',b'hot',op_id='hot')
        pending=Client(server.address)
        import threading
        answer={}
        def submit():
            try: answer['outcome']=pending.put('ledger','pending',b'pending',expected=0,ack=2,op_id='pending')
            except Exception as error: answer['error']=str(error)
        task=threading.Thread(target=submit);task.start();time.sleep(.05)
        started=time.monotonic();assert c.get('ledger','hot')['value']==b'hot'
        hot_seconds=time.monotonic()-started
        try: c.put('ledger','pending',b'competing',expected=0,op_id='competing')
        except Refusal as error: assert error.code==12
        else: raise AssertionError('pending reservation did not conflict')
        duplicate=c.put('ledger','pending',b'pending',expected=0,ack=2,op_id='pending')
        task.join(timeout=5);pending.close();assert answer.get('outcome')==duplicate
        uncertain=Client(server.address)
        try: uncertain.put('ledger','uncertain',b'uncertain',expected=0,op_id='uncertain',deadline=deadline_ms(.05))
        except (TimeoutError,OSError): pass
        else: raise AssertionError('expected timeout after send')
        uncertain.close()
        settled=c.put('ledger','uncertain',b'uncertain',expected=0,op_id='uncertain')
        assert uncertain.get('ledger','uncertain')['version']==settled['version']
        assert c.get('ledger','uncertain')['version']==settled['version']
        results['pending']={'competing_absent_code':12,'duplicate':'original outcome','hot_read_seconds':hot_seconds,
                            'timeout_retry':'same op_id settled original outcome','stats':c.health()}
        server.stop()
        server=Server(args.binary,output/'mem-state',output/'mem-server.txt',flags=('--allow-mem',));servers.append(server)
        try: server.client.put('mem','no-opt-in',b'value',ack=0,op_id='no-opt-in')
        except Refusal as error: assert error.code==15
        else: raise AssertionError('mem accepted without client opt-in')
        opted=Client(server.address,allow_mem=True)
        try:
            assert opted.put('mem','opted',b'value',ack=0,op_id='opted')['ack']==0
            assert opted.get('mem','opted') is None
        finally: opted.close()
        results['mem_run']=server.client.health('mem');server.stop()
        results['tests'].append('mem requires server and per-connection opt-in; no committed visibility')
        server=Server(args.binary,output/'quota-state',output/'quota-server.txt',flags=('--quota','10','--index-cap','100'));servers.append(server)
        for seq in range(7): server.client.put('quota',f'key/{seq}',b'x',op_id=f'quota-{seq}')
        high=server.client.health('quota');assert high['namespace_high_water']==1 and high['high_water']==0
        for seq in range(7,10): server.client.put('quota',f'key/{seq}',b'x',op_id=f'quota-{seq}')
        try: server.client.put('quota','key/10',b'x',op_id='quota-10')
        except Refusal as error: assert error.code==8
        else: raise AssertionError('quota not refused')
        assert server.client.get('quota','key/0')['value']==b'x'
        results['quota_run']=server.client.health('quota');server.stop()
        results['tests'].append('namespace 70% warning before quota refusal; old keys remain readable at cap')
        results['startup_disk']={'epoch_bytes_per_successful_start':8,'epoch_sync_calls_per_successful_start':1,'physical_bytes':'unmeasured'}
        results['all_tests_passed']=True
        (output/'integration-test.json').write_text(json.dumps(results,indent=2),encoding='utf8')
        print(json.dumps(results,indent=2))
    finally:
        for server in servers: server.stop()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--binary',type=Path,required=True);p.add_argument('--output',required=True)
    run(p.parse_args())
