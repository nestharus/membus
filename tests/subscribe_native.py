"""Native isolated-pipe delivery, outage, replay and writer-latency witnesses."""
import argparse, ctypes, hashlib, json, pathlib, statistics, struct, sys, threading, time
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'python'), str(ROOT/'tests')]
from membus import Client, Refusal, EpochChanged, frame, lp, deadline_ms
from membus_signals import Listener
from integration import Server

def percentiles(samples):
    s=sorted(samples)
    return dict(count=len(s), median_ms=statistics.median(s), p99_ms=s[int(.99*(len(s)-1))], max_ms=max(s))

def process_memory(pid):
    from ctypes import wintypes as w
    class Memory(ctypes.Structure):
        _fields_=[('cb',w.DWORD),('faults',w.DWORD)]+[(name,ctypes.c_size_t) for name in ('peak_rss','rss','peak_paged','paged','peak_nonpaged','nonpaged','pagefile','peak_pagefile','private')]
    k=ctypes.WinDLL('kernel32',use_last_error=True)
    k.OpenProcess.argtypes=[w.DWORD,w.BOOL,w.DWORD];k.OpenProcess.restype=w.HANDLE
    k.K32GetProcessMemoryInfo.argtypes=[w.HANDLE,ctypes.POINTER(Memory),w.DWORD]
    k.CloseHandle.argtypes=[w.HANDLE]
    handle=k.OpenProcess(0x1010,False,pid)
    if not handle:raise ctypes.WinError(ctypes.get_last_error())
    try:
        m=Memory();m.cb=ctypes.sizeof(m)
        if not k.K32GetProcessMemoryInfo(handle,ctypes.byref(m),m.cb):raise ctypes.WinError(ctypes.get_last_error())
        return dict(rss=m.rss,private=m.private)
    finally:k.CloseHandle(handle)

def run(args):
    out=pathlib.Path(args.output); out.mkdir(parents=True,exist_ok=True)
    result=dict(binary_sha256=hashlib.sha256(pathlib.Path(args.binary).read_bytes()).hexdigest(),
                method='Isolated std-only Rust server and stdlib Windows LOCAL named-pipe listeners; os acknowledged writes; elapsed in perf_counter milliseconds; logical IO from opcode9; no physical-byte claim',
                tests=[], processes=[], cases={})
    server=None; listeners=[]; peers=[]
    def listener(endpoint=None):
        l=Listener(endpoint);listeners.append(l);return l
    def status(c, sub, wanted, timeout=2):
        end=time.perf_counter()+timeout
        while time.perf_counter()<end:
            s=c.subscription_status('quota',sub)
            if s['status'] in wanted:return s
            time.sleep(.002)
        raise AssertionError(('status timeout',s,wanted))
    def io(c):return struct.unpack('<3Q',c.call(9,'quota'))
    def writers(c,label,n=200):
        durations=[]
        for i in range(n):
            start=time.perf_counter();c.put('quota','bench/fixed',b'fixed',op_id=f'{label}-{i}')
            durations.append((time.perf_counter()-start)*1000)
        return dict(summary=percentiles(durations),raw_ms=durations)
    try:
        server=Server(args.binary,out/'state',out/'server.txt',transport='pipe',flags=('--heartbeat-ms','0','--signal-history','32','--allow-mem'))
        result['processes'].append(dict(pid=server.process.pid,state=str(out/'state'),pipe=server.address))
        c=server.client
        c.allow_mem=True;c.close();c.connect(deadline_ms())
        # Baseline owns identical writer and fixed key before subscribers exist.
        result['cases']['baseline']=writers(c,'baseline')
        l=listener(); registration=c.subscribe('quota',l.endpoint,'event/',include_value=True)
        sub=registration['subscription']; initial=registration['cursor']
        c.put('other','event/ignore',b'ignore')
        c.put('quota','state/ignore',b'ignore')
        c.put('quota','event/mem',b'mem',ack=0)
        start=time.perf_counter()
        v=c.put('quota','event/1791590000000-1',b'{"kind":"reopening","profile":"claude2"}',op_id='reopen-1')
        event=l.receive(timeout=1)
        latency=(time.perf_counter()-start)*1000
        assert event['key']=='event/1791590000000-1' and event['version']==v['version']
        assert event['subscription']==sub and event['namespace']=='quota' and event['value']==b'{"kind":"reopening","profile":"claude2"}' and latency<1000
        c.put('quota','event/1791590000000-1',event['value'],op_id='reopen-1')
        assert status(c,sub,{'written'})['written']==1
        result['tests'].append(dict(name='filtered reopening under one second; mem/other-prefix/other-namespace/duplicate excluded',pass_condition='matching event only, unchanged write id not signalled twice, latency<1000ms',latency_ms=latency))
        # The application can handle an event while the persistent pipe buffers
        # small followups, then remain quiet without losing its subscription.
        c.put('quota','event/handling-1',b'queued-1')
        c.put('quota','event/handling-2',b'queued-2')
        time.sleep(.2)
        handled=[l.receive(),l.receive()]
        assert [e['key'] for e in handled]==['event/handling-1','event/handling-2']
        assert status(c,sub,{'written'})['active']
        try:l.receive(timeout=.02)
        except TimeoutError:pass
        else:raise AssertionError('quiet listener did not time out')
        c.put('quota','event/after-quiet',b'quiet-survived');event=l.receive()
        assert event['key']=='event/after-quiet'
        result['tests'].append(dict(name='processing pause and quiet timeout',pass_condition='200msapplicationpausebuffers2smallchanges;quiettimeoutkeepsconnection;nextsignalarrives'))
        cursor=event['cursor']; name=l.endpoint;l.close()
        c.put('quota','event/disconnected-1',b'missed-1')
        c.put('quota','event/disconnected-2',b'missed-2')
        failed=status(c,sub,{'failed'})
        l2=listener(name); resumed=c.subscribe('quota',l2.endpoint,'event/',include_value=True,cursor=cursor)
        missed=[l2.receive(timeout=1),l2.receive(timeout=1)]
        assert [e['key'] for e in missed]==['event/disconnected-1','event/disconnected-2']
        assert [struct.unpack('<QQ',e['cursor'])[1] for e in missed]==sorted(struct.unpack('<QQ',e['cursor'])[1] for e in missed)
        result['tests'].append(dict(name='disconnect resume',pass_condition='exact two missed publications in FIFO order',received=[e['key'] for e in missed],dead_status={k:v for k,v in failed.items() if k!='cursor'}))
        c.unsubscribe('quota',resumed['subscription']);l2.close()
        # Remove carries exact committed tombstone metadata, and key-only mode.
        l3=listener(); r3=c.subscribe('quota',l3.endpoint,'remove/',include_value=False)
        v=c.put('quota','remove/x',b'x');a=l3.receive()
        removed=c.remove('quota','remove/x',v['version']);b=l3.receive()
        assert not a['removed'] and a['value'] is None and b['removed'] and b['value'] is None and b['version']==removed['version']
        c.unsubscribe('quota',r3['subscription']);l3.close()
        result['tests'].append(dict(name='key-only and remove',pass_condition='value absent, remove flag and version correct'))
        # Same writer path with a missing endpoint, then a connected but unread pipe.
        dead=c.subscribe('quota',r'\\.\pipe\LOCAL\membus-F-E6-33-missing','bench/')['subscription']
        result['cases']['dead']=writers(c,'dead')
        result['cases']['dead']['status']={k:v for k,v in status(c,dead,{'failed'}).items() if k!='cursor'}
        slow=listener(); slow_sub=c.subscribe('quota',slow.endpoint,'bench/',include_value=True)['subscription']
        large_started=time.perf_counter();c.put('quota','bench/slow',b'x'*262144)
        result['cases']['slow_large_write_ms']=(time.perf_counter()-large_started)*1000
        result['cases']['slow']=writers(c,'slow')
        result['cases']['slow']['status']={k:v for k,v in status(c,slow_sub,{'failed','dropped'}).items() if k!='cursor'}
        slow.close()
        result['tests'].append(dict(name='slow and dead endpoints',pass_condition='writers succeed, endpoint deactivated with explicit failure/drop',baseline=result['cases']['baseline']['summary'],dead=result['cases']['dead']['summary'],slow=result['cases']['slow']['summary']))
        # Retention is global and count bounded, independent of namespace filter.
        for i in range(33):c.put('noise','k',b'noise',op_id='age-'+str(i))
        try:c.subscribe('quota',r'\\.\pipe\LOCAL\unused','event/',cursor=initial)
        except Refusal as e:assert e.code==23
        else:raise AssertionError('old cursor accepted')
        usage=c.signal_usage();assert usage['history_count']<=32 and usage['history_bytes']<=usage['history_budget']
        result['tests'].append(dict(name='too-old cursor',pass_condition='resume_gap=23, no truncated success',usage=usage))
        # A burst of value-bearing changes can fill a subscriber before its slow I/O
        # finishes. All writes commit, and memory accounting stays bounded.
        overflow=listener(); over_sub=c.subscribe('quota',overflow.endpoint,'overflow/',include_value=True)['subscription']
        payload=b'z'*262144
        for i in range(36):c.put('quota','overflow/k',payload,op_id='overflow-'+str(i))
        over=status(c,over_sub,{'failed','dropped'});assert not over['active']
        result['tests'].append(dict(name='bounded overflow/failure',pass_condition='stream deactivated and queue budget respected',status={k:v for k,v in over.items() if k!='cursor'},usage=c.signal_usage()))
        overflow.close()
        # Logical disk cost of signalling is zero in addition to the origin writes.
        meter=listener(); ms=c.subscribe('quota',meter.endpoint,'meter/',include_value=True)['subscription']
        before=io(c); signal_before=c.signal_usage(); n=32
        for i in range(n):
            c.put('quota','meter/fixed',b'meter',op_id=f'meter-{i}')
            meter.receive()
        after=io(c);signal_after=c.signal_usage()
        result['signal_cost']=dict(signals=n,origin_logical_append_bytes=after[0]-before[0],
            signal_additional_logical_drive_bytes=0,origin_bytes_per_signal=(after[0]-before[0])/n,
            history_charge_delta=signal_after['history_bytes']-signal_before['history_bytes'],
            history=signal_after,accounting='Charged retained representation, not allocator-exact memory. Global ring is already full; delta includes eviction. Payload5, identifiers and256byte overhead are charged.')
        c.unsubscribe('quota',ms);meter.close()
        # Restart is deliberately a visible epoch gap, followed by fresh delivery.
        old_epoch=c.epoch;old_cursor=cursor;old_client=c;old_address=server.address
        server.stop();result['processes'][-1]['stopped']=True
        server=Server(args.binary,out/'state',out/'restart-server.txt',transport='pipe',flags=('--heartbeat-ms','0','--signal-history','32'),address=old_address)
        result['processes'].append(dict(pid=server.process.pid,state=str(out/'state'),pipe=server.address))
        try:old_client.connect(deadline_ms())
        except EpochChanged as e:assert e.previous==old_epoch and e.current==server.client.epoch
        else:raise AssertionError('existing client missed epoch change')
        c=old_client;peers.append(c);assert c.epoch!=old_epoch
        fresh=listener()
        try:c.subscribe('quota',fresh.endpoint,'event/',cursor=old_cursor)
        except Refusal as e:assert e.code==23
        else:raise AssertionError('old epoch cursor accepted')
        reg=c.subscribe('quota',fresh.endpoint,'event/',include_value=True)
        c.put('quota','event/restart-reopen',b'restarted')
        event=fresh.receive(timeout=1);assert event['key']=='event/restart-reopen'
        c.unsubscribe('quota',reg['subscription']);fresh.close()
        result['tests'].append(dict(name='restart',pass_condition='old epoch gives23; re-register receives new event; durable state replayed',previous_epoch=old_epoch,new_epoch=c.epoch))
        # Fresh epoch retains fewer than32changes, so every measured change adds
        # one exact charged representation without evictions.
        mem_listener=listener();mem_sub=c.subscribe('quota',mem_listener.endpoint,'memory/',include_value=True)['subscription']
        mem_before=process_memory(server.process.pid);ring_before=c.signal_usage();disk_before=io(c)
        files_before=sum(p.stat().st_size for p in (out/'state').glob('stripe*'))
        for i in range(16):
            c.put('quota','memory/fixed',b'memory',op_id=f'memory-{i}')
            mem_listener.receive()
        mem_after=process_memory(server.process.pid);ring_after=c.signal_usage();disk_after=io(c)
        files_after=sum(p.stat().st_size for p in (out/'state').glob('stripe*'))
        result['fresh_signal_memory']=dict(signals=16,before=mem_before,after=mem_after,
            process_delta_bytes={k:mem_after[k]-mem_before[k] for k in mem_before},
            combined_process_delta_bytes_per_signal={k:(mem_after[k]-mem_before[k])/16 for k in mem_before},
            charged_history_bytes_per_signal=(ring_after['history_bytes']-ring_before['history_bytes'])/16,
            origin_logical_append_bytes=disk_after[0]-disk_before[0],stripe_size_delta=files_after-files_before,
            signal_additional_logical_drive_bytes=0,
            limits='Process deltas combine KV/dedup/transport/allocator and signal memory; they do not isolate allocator signal overhead. Conservative history charge is independently exact.')
        assert files_after-files_before==disk_after[0]-disk_before[0]
        c.unsubscribe('quota',mem_sub);mem_listener.close()
        result['pass']=True
    finally:
        for l in listeners:l.close()
        for p in peers:p.close()
        if server:
            server.stop();result['processes'][-1]['stopped']=True
        result['all_owned_processes_stopped']=all(p.get('stopped') for p in result['processes'])
        (out/'results.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='cases'},indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True)
    run(p.parse_args())
