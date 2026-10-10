"""Private Windows-pipe timer/replay/bytes/current-client witnesses; never kills resident."""
import argparse, hashlib, importlib.util, json, os, pathlib, struct, sys, time, zlib
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'python'),str(ROOT/'tests')]
from membus import Client, Refusal, Pipe, Decoder, lp, deadline_ms
from membus_timers import decode_timer
from membus_signals import Listener
from integration import Server

def serial(value):
    if isinstance(value,bytes):return value.hex()
    raise TypeError(type(value))
def wait_for(fn, timeout=3):
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        value=fn()
        if value:return value
        time.sleep(.005)
    raise AssertionError('condition timeout')
def receive(listener):
    event=listener.receive(timeout=3)
    return event,decode_timer(event['value'])
def records(state):
    rows=[]
    for path in sorted(state.glob('stripe-*.log')):
        data=path.read_bytes();at=0
        while at<len(data):
            assert data[at:at+4]==b'MBK1'
            size,complement,crc=struct.unpack_from('<III',data,at+4);body=data[at+16:at+16+size]
            assert complement==size^0xffffffff and zlib.crc32(body)==crc
            d=Decoder(body)
            stamp,version,ack,removed=d.unpack('<QQBB');ns,key,op_id=(d.blob().decode() for _ in range(3))
            expected,=d.unpack('<Q');value=d.blob()
            assert d.pos==len(d.data)
            rows.append(dict(path=str(path),offset=at,total=16+size,namespace=ns,key=key,op_id=op_id,value_len=len(value),version=version,removed=removed))
            at+=16+size
    return rows

def run(args):
    out=pathlib.Path(args.output);out.mkdir(parents=True,exist_ok=True)
    result=dict(binary_sha256=hashlib.sha256(pathlib.Path(args.binary).read_bytes()).hexdigest(),
        method='stdlib clients/listeners; private pipes/state only; UTC due in ms, perf_counter for waits; logical stripe lengths/MBK1 records, no physical-device claim',tests=[],processes=[],cases={})
    servers=[];listeners=[]
    def start(name,flags=(),**kw):
        server=Server(args.binary,out/(name+'-state'),out/(name+'-server.txt'),transport='pipe',flags=('--heartbeat-ms','0')+tuple(flags),**kw)
        servers.append(server);result['processes'].append(dict(pid=server.process.pid,pipe=server.address,state=str(out/(name+'-state'))));return server
    def listener():
        l=Listener();listeners.append(l);return l
    def check(name,condition,**evidence):
        assert condition,(name,evidence);result['tests'].append(dict(name=name,passed=True,**evidence))
    try:
        server=start('main');c=server.client
        check('timer limits behind hello',c.timer_limits['identities_max']==4096 and c.timer_limits['min_period_ms']==100,limits=c.timer_limits)
        # Strict old hello variants keep their original refusal counts and extension sizes.
        for identity,count,extension in [('python',19,0),('python;scans=1',22,0),('python;subscribe=1;scans=1',24,46),('python;timers=1',23,34),('python;timers=1;subscribe=1;scans=1',25,80)]:
            legacy=Client.__new__(Client);legacy.transport='pipe';legacy.channel=Pipe(server.address,deadline_ms());legacy.request_id=0;legacy.timeout=10
            try:
                d=Decoder(legacy.exchange(1,struct.pack('<IB',16*1024*1024,0)+lp(identity),deadline_ms()));d.read(27);n,=d.unpack('<H')
                codes=[]
                for _ in range(n):
                    code,transient=d.unpack('<HB');d.blob();codes.append(code)
                expected=list(range(1,count+1)) if identity!='python;timers=1' else list(range(1,23))+[25]
                check('legacy exact hello '+identity,n==count and codes==expected and len(d.data)-d.pos==extension,count=n,codes=codes,extension_bytes=len(d.data)-d.pos)
                if 'timers=1' not in identity:
                    try:legacy.call(23,'quota',lp('absent'))
                    except Refusal as error:assert error.code==2
                    else:raise AssertionError('unnegotiated timer accepted')
                else:
                    assert legacy.call(23,'quota',lp('absent'))==b'\x00'
                    if 'subscribe=1' not in identity:
                        try:legacy.call(19,'quota')
                        except Refusal as error:assert error.code==2
                        else:raise AssertionError('timers-only subscription access')
            finally:legacy.close()
        l=listener();sub=c.subscribe('quota',l.endpoint,'timer/',include_value=True)['subscription']
        before=sum(p.stat().st_size for p in (out/'main-state').glob('stripe-*.log'))
        due=int(time.time()*1000)+500
        gen=c.timer_set('quota','once','timer/once',due,payload=b'tick',expected=0)
        after_set=sum(p.stat().st_size for p in (out/'main-state').glob('stripe-*.log'))
        assert int(time.time()*1000)<due
        event,wake=receive(l);received=int(time.time()*1000)
        check('one-off at due, not early, under 1000ms late',wake['due_ms']==due and 0<=received-due<1000 and wake['missed_count']==1 and wake['due_id']==(gen,0),lateness_ms=received-due,received_ms=received,wake=wake)
        wait_for(lambda:not c.timer_status('quota','once')['active'])
        after_fire=sum(p.stat().st_size for p in (out/'main-state').glob('stripe-*.log'))
        fire_rows=[r for r in records(out/'main-state') if r['key'].startswith('p/')]
        check('one isolated fire appends exactly one compact progress record',len(fire_rows)==1 and after_fire-after_set==fire_rows[0]['total'],
              stripe_bytes_before_set=before,stripe_bytes_after_set=after_set,stripe_bytes_after_fire=after_fire,
              setup_bytes=after_set-before,fire_bytes=after_fire-after_set,record=fire_rows[0])
        try:l.receive(timeout=.15)
        except TimeoutError:pass
        else:raise AssertionError('one-off repeated')
        check('one-off stops and status records draw',c.timer_status('quota','once')['last']==wake,status=c.timer_status('quota','once'))
        # Deterministic per-fire draws, two watchers, one shared identity.
        l2=listener();sub2=c.subscribe('quota',l2.endpoint,'timer/',include_value=True)['subscription']
        anchor=int(time.time()*1000)+350
        gen=c.timer_set('quota','jitter','timer/jitter',anchor,period_ms=200,jitter_ms=(-30,30),payload=b'periodic',expected=0)
        jitter=[]
        for index in range(12):
            _,a=receive(l);_,b=receive(l2)
            check('jitter pair '+str(index),a==b and a['due_id']==(gen,index) and -30<=a['draw_ms']<=30 and a['due_ms']==anchor+index*200+a['draw_ms'] and a['missed_count']==1,wake=a)
            jitter.append(a)
        c.timer_cancel('quota','jitter',gen)
        check('cancel removes timer status',c.timer_status('quota','jitter') is None)
        for listener_ in (l,l2):
            try:listener_.receive(timeout=.3)
            except TimeoutError:pass
            else:raise AssertionError('cancelled timer fired in quiet interval')
        # Replace generation fencing; the old future one-off must never fire.
        old=c.timer_set('quota','replace','timer/old',int(time.time()*1000)+300,payload=b'old')
        new=c.timer_set('quota','replace','timer/new',int(time.time()*1000)+600,payload=b'new',expected=old)
        try:c.timer_cancel('quota','replace',old)
        except Refusal as e:assert e.code==12
        else:raise AssertionError('old-generation cancel accepted')
        _,a=receive(l);_,b=receive(l2)
        check('replace and stale cancel',a==b and a['generation']==new and a['payload']==b'new',wake=a)
        # Repeat op_id does not rewind one-off or replace a newer generation.
        anchor=int(time.time()*1000)+200
        one=c.timer_set('quota','retry','timer/retry',anchor,op_id='retry-one')
        _,a=receive(l);_,b=receive(l2)
        c.timer_set('quota','retry','timer/retry',anchor,op_id='retry-one')
        check('same set op_id does not rewind completed timer',not c.timer_status('quota','retry')['active'])
        newer=c.timer_set('quota','retry','timer/retry',int(time.time()*1000)+10000,expected=one)
        try:c.timer_set('quota','retry','timer/retry',anchor,op_id='retry-one')
        except Refusal as e:assert e.code==12
        else:raise AssertionError('old retry replaced newer timer')
        c.timer_cancel('quota','retry',newer)
        # Internal reserved namespace is denied to reads/mutations/subscriptions.
        for opcode,suffix in [(2,lp('x')),(4,lp('x')+b'\x01'+lp(b'x')),(16,lp('')+lp(l.endpoint)+b'\x01'+lp(b'')+lp('membus.change/1')+struct.pack('<I',1024))]:
            try:c.call(opcode,'__membus_timers_v1',suffix)
            except Refusal as e:assert e.code==3
            else:raise AssertionError('internal namespace admitted')
        check('reserved timer namespace refused',True)
        for options in [dict(period_ms=99),dict(period_ms=100,jitter_ms=(-50,50)),dict(jitter_ms=(5,-5)),dict(payload=b'x'*65537)]:
            try:c.timer_set('quota','bad','timer/bad',int(time.time()*1000)+1000,**options)
            except (Refusal,ValueError):pass
            else:raise AssertionError('invalid schedule accepted')
        check('invalid timer bounds rejected',True)
        # Private kill/restart with two watchers. Replay retained startup catch-up in the new epoch.
        anchor=int(time.time()*1000)+350
        gen=c.timer_set('quota','restart','timer/restart',anchor,period_ms=1000,payload=b'restart')
        _,a=receive(l);_,b=receive(l2);assert a==b and a['index']==0
        wait_for(lambda:c.timer_status('quota','restart')['next_index']==1)
        old_epoch=c.epoch;server.stop();time.sleep(3.3)
        l.close();l2.close()
        restarted=Server(args.binary,out/'main-state',out/'restart-server.txt',transport='pipe',address=server.address,flags=('--heartbeat-ms','0'))
        servers.append(restarted);c=restarted.client;result['processes'].append(dict(pid=restarted.process.pid,pipe=restarted.address,state=str(out/'main-state')))
        l=listener();l2=listener();cursor=struct.pack('<QQ',c.epoch,0)
        for listener_ in (l,l2):c.subscribe('quota',listener_.endpoint,'timer/restart',include_value=True,cursor=cursor)
        _,a=receive(l);_,b=receive(l2)
        check('one catch-up shared by two watchers with exact missed count',a==b and c.epoch!=old_epoch and a['generation']==gen and a['missed_count']==a['index'] and a['index']>=3,wake=a,old_epoch=old_epoch,new_epoch=c.epoch)
        catchup=a
        for _ in range(2):
            _,a=receive(l);_,b=receive(l2)
            check('after restart one normal due per watcher',a==b and a['index']==catchup['index']+1 and a['missed_count']==1,wake=a)
            catchup=a
        c.timer_cancel('quota','restart',gen)
        # Catch-up was queued once; no per-watcher progress record and no KV event keys.
        rows=records(out/'main-state');progress=[r for r in rows if r['key'].startswith('p/')]
        for r in progress:
            formula=58+len(r['namespace'].encode())+len(r['key'].encode())+len(r['op_id'].encode())+48
            assert r['value_len']==48 and r['total']==formula
        check('compact progress stripe bytes match exact formula',bool(progress),records=progress,
            per_fire_bytes=sorted(set(r['total'] for r in progress)),
            timer_day_60s_bytes_range=[min(r['total'] for r in progress)*1440,max(r['total'] for r in progress)*1440],
            note='1440-fire extrapolation; decimal epoch/generation/index digits affect op_id lengths; setup/cancel extra; logical append not physical I/O')
        check('cancel persists across restart',c.timer_status('quota','jitter') is None)
        check('fire creates no quota KV event key',c.get('quota','timer/restart') is None and c.get('quota','timer/once') is None)
        # Current lane88 source uses unchanged put/get/subscribe/scan/status calls on new build.
        os.environ.update(PILLARS_HOME=str(out/'pillars'),QUOTA_BUS_PIPE=restarted.address,QUOTA_BUS_TASK='',MEMBUS_PY=str(ROOT/'python'))
        sys.path.insert(0,'D:/projects/machine-tools/trunk')
        import quotabus,quotafwd
        lr=listener();c.subscribe('quota',lr.endpoint,'event/',include_value=True)
        key=f'event/{int(time.time()*1000):016d}-private-1'
        sent=quotabus.membus_put([(key,dict(kind='opened'))]);event=lr.receive()
        got=[];forwarder=quotafwd.Forwarder(pipe=restarted.address,forward=True,notify=lambda kinds:got.append(kinds),state_path=out/'forwarder.json',log=lambda *a:None)
        forwarder.deliver([forwarder.take(event['key'],event['value'])],'pushed');forwarder.save()
        forwarder.scan_since(c)
        check('lane88 actual publisher and forwarder calls',sent=='ok 1' and got==[['opened']],publisher=sent,notifications=got)
        result['cases']['source_hashes']={str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in [ROOT/'python/membus.py',ROOT/'src/timers.rs',pathlib.Path(quotabus.__file__),pathlib.Path(quotafwd.__file__)]}
        small=start('dedup',flags=('--dedup-cap','3'))
        sc=small.client;sl=listener();sc.subscribe('test',sl.endpoint,'timer/',include_value=True)
        sg=sc.timer_set('test','fixed','timer/test',int(time.time()*1000)+150,period_ms=100)
        for _ in range(8):receive(sl)
        sc.timer_cancel('test','fixed',sg)
        ordinary=sc.put('test','ordinary',b'works')
        check('eight fire checkpoints preserve client dedup capacity',ordinary['ack']==1)
        small.stop();sl.close()
        reopened=Server(args.binary,out/'dedup-state',out/'dedup-restart.txt',transport='pipe',flags=('--heartbeat-ms','0','--dedup-cap','3'))
        servers.append(reopened)
        check('replay excludes progress dedup entries',reopened.client.get('test','ordinary')['value']==b'works')
        if args.old_binary:
            old_server=Server(args.old_binary,out/'old-server-state',out/'old-server.txt',transport='pipe',flags=('--heartbeat-ms','0'))
            servers.append(old_server);oc=old_server.client;ol=listener()
            registration=oc.subscribe('compat',ol.endpoint,'old/',include_value=True)
            oc.put('compat','old/newclient',b'works');ev=ol.receive()
            check('new Python client on old server in merge/swap interval',oc.timer_limits is None and ev['value']==b'works' and oc.get('compat','old/newclient')['value']==b'works',old_binary_sha256=hashlib.sha256(pathlib.Path(args.old_binary).read_bytes()).hexdigest())
            try:oc.timer_set('compat','timer','old/timer',int(time.time()*1000)+1000)
            except Refusal as error:assert error.code==2
            else:raise AssertionError('timer accepted against old server')
        if args.old_client:
            spec=importlib.util.spec_from_file_location('old_membus',args.old_client);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
            old=mod.Client(restarted.address,transport='pipe')
            try:
                old.put('compat','old',b'works');check('unchanged old Python client on new server',old.get('compat','old')['value']==b'works')
            finally:old.close()
        result['passed']=True
    finally:
        for listener_ in listeners:listener_.close()
        for server in servers:server.stop()
        result['all_private_processes_stopped']=all(s.process.poll() is not None for s in servers)
        (out/'timers-native.json').write_text(json.dumps(result,indent=2,default=serial)+'\n',encoding='utf8')
    print(json.dumps(dict(passed=True,tests=len(result['tests']),output=str(out/'timers-native.json'))))
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',required=True);parser.add_argument('--output',required=True);parser.add_argument('--old-client');parser.add_argument('--old-binary');run(parser.parse_args())
