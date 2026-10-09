"""Pure standard-library ping-pong client and suite orchestrator; no disk payloads."""
import argparse, hashlib, json, math, multiprocessing, pathlib, socket, subprocess, sys, time

def exact_read(stream, count, tcp):
    result = bytearray()
    while len(result) < count:
        block = stream.recv(count-len(result)) if tcp else stream.read(count-len(result))
        if not block:
            raise EOFError('server closed before echo completed')
        result.extend(block)
    return result

def worker(transport,address,size,count,barrier,release,queue,client_id):
    try:
        payload = bytearray(b'Z'*size)
        if transport == 'tcp':
            host, port = address.rsplit(':',1)
            stream = socket.create_connection((host,int(port)),timeout=30)
            stream.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
            stream.settimeout(None)
        else:
            stream = open(address,'r+b',buffering=0)
        with stream:
            samples=[]
            def ping(sequence,timed):
                payload[:8]=((client_id << 32)|sequence).to_bytes(8,'little')
                start=time.perf_counter_ns()
                if transport=='tcp':
                    stream.sendall(payload)
                else:
                    offset=0
                    while offset < size:
                        written=stream.write(payload[offset:])
                        if not written:
                            raise IOError('short pipe write')
                        offset+=written
                echoed=exact_read(stream,size,transport=='tcp')
                elapsed=time.perf_counter_ns()-start
                if echoed!=payload:
                    raise IOError('echo differs')
                if timed:samples.append(elapsed)
            for sequence in range(16):ping(sequence,False)
            barrier.wait(timeout=30)
            if not release.wait(timeout=30):raise TimeoutError('start release missing')
            for sequence in range(16,count+16):ping(sequence,True)
            queue.put((samples,time.perf_counter_ns(),None))
    except BaseException as error:
        barrier.abort()
        queue.put(([],0,repr(error)))

def client(transport, address, size, clients, count):
    context=multiprocessing.get_context('spawn')
    barrier=context.Barrier(clients+1)
    release=context.Event()
    queue=context.Queue()
    workers=[context.Process(target=worker,args=(transport,address,size,count,barrier,release,queue,i)) for i in range(clients)]
    for process in workers:process.start()
    try:
        barrier.wait(timeout=30)
        # Startup/connection latency ends at the barrier. Worker QPC clocks share
        # Windows' clock domain; collection and child teardown are untimed.
        start=time.perf_counter_ns()
        release.set()
        results=[queue.get(timeout=60) for _ in workers]
        errors=[r[2] for r in results if r[2]]
        if errors:raise RuntimeError(errors)
        seconds=(max(r[1] for r in results)-start)/1e9
        samples=[v for r in results for v in r[0]]
        samples.sort()
        for process in workers:
            process.join(timeout=5)
            if process.exitcode!=0:raise RuntimeError('client process failed')
    finally:
        for process in workers:
            if process.is_alive():process.terminate()
            process.join()
        queue.close()
    percentile=lambda q:samples[math.ceil((len(samples)-1)*q)]/1000
    return dict(kind='python_'+transport,payload=size,clients=clients,samples=len(samples),p50_us=percentile(.5),p95_us=percentile(.95),p99_us=percentile(.99),seconds=seconds,operations_per_sec=clients*count/seconds,bytes_written=0,flushes=0,records=clients*count,records_per_flush=0,client_processes=clients)

def suite(executable, output, count):
    with open(output,'w',encoding='utf-8') as out:
        for transport in ('tcp','pipe'):
            for size in (64,2253,51200):
                for clients in (1,8,32):
                    for language in (('rust','python') if clients!=8 else ('python','rust')):
                        address='127.0.0.1:0' if transport=='tcp' else r'\\.\pipe\membus-measure-'+str(time.time_ns())
                        server=subprocess.Popen([executable,'server',transport,address,str(size),str(clients),str(count+16)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                        try:
                            ready=server.stdout.readline().strip()
                            if not ready.startswith('READY '):
                                raise RuntimeError('server failed: '+ready+server.stderr.read())
                            address=ready[6:]
                            if language=='python':
                                row=client(transport,address,size,clients,count)
                            else:
                                result=subprocess.run([executable,'client',transport,address,str(size),str(clients),str(count)],capture_output=True,text=True,timeout=90,check=True)
                                row=json.loads(result.stdout)
                            server.wait(timeout=30)
                            if server.returncode:
                                raise RuntimeError(server.stderr.read())
                            row['utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
                            row['wire_bytes']=2*size*clients*(count+16)
                            row['warmup_per_client']=16
                            row['exe_sha256']=hashlib.sha256(pathlib.Path(executable).read_bytes()).hexdigest()
                            row['client_sha256']=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
                            row['python_version']=sys.version
                            out.write(json.dumps(row)+'\n');out.flush()
                            print(language,transport,size,clients,'p50_us',row['p50_us'],flush=True)
                        finally:
                            if server.poll() is None:
                                server.kill();server.wait()

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--exe',required=True);parser.add_argument('--output',required=True);parser.add_argument('--count',type=int,default=2000)
    args=parser.parse_args();suite(args.exe,args.output,args.count)
