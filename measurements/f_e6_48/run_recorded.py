"""Run one native command and retain direct-process consumption and context."""
import ctypes, datetime, json, pathlib, subprocess, sys, time
from ctypes import wintypes as w

def usage(handle):
    class Memory(ctypes.Structure):
        _fields_ = [('cb',w.DWORD),('PageFaultCount',w.DWORD),('PeakWorkingSetSize',ctypes.c_size_t),
          ('WorkingSetSize',ctypes.c_size_t),('QuotaPeakPagedPoolUsage',ctypes.c_size_t),
          ('QuotaPagedPoolUsage',ctypes.c_size_t),('QuotaPeakNonPagedPoolUsage',ctypes.c_size_t),
          ('QuotaNonPagedPoolUsage',ctypes.c_size_t),('PagefileUsage',ctypes.c_size_t),
          ('PeakPagefileUsage',ctypes.c_size_t)]
    memory = Memory(cb=ctypes.sizeof(Memory))
    psapi = ctypes.WinDLL('psapi',use_last_error=True)
    psapi.GetProcessMemoryInfo.argtypes=[w.HANDLE,ctypes.c_void_p,w.DWORD]
    ok = psapi.GetProcessMemoryInfo(handle,ctypes.byref(memory),ctypes.sizeof(memory))
    times = [w.FILETIME() for _ in range(4)]
    kernel = ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.GetProcessTimes.argtypes=[w.HANDLE,*([ctypes.POINTER(w.FILETIME)]*4)]
    cpu_ok = kernel.GetProcessTimes(handle,*(ctypes.byref(t) for t in times))
    value = lambda t: ((t.dwHighDateTime<<32)+t.dwLowDateTime)/1e7
    return dict(cpu_seconds=value(times[2])+value(times[3]) if cpu_ok else None,
        peak_working_set_bytes=memory.PeakWorkingSetSize if ok else None,
        peak_pagefile_bytes=memory.PeakPagefileUsage if ok else None,
        boundary='direct process only; excludes descendants; cargo compiler consumption unmeasured')

def context():
    status=subprocess.run(['lineup.cmd','status'],capture_output=True,text=True)
    count=subprocess.run(['powershell.exe','-NoProfile','-Command','(Get-Process).Count'],capture_output=True,text=True)
    return dict(at=datetime.datetime.now(datetime.timezone.utc).isoformat(),lineup=status.stdout,process_count=count.stdout.strip())

if __name__ == '__main__':
    path=pathlib.Path(sys.argv[1]); command=sys.argv[2:]
    result=dict(command=command,started=context(),reservation='cpu=4,mem=2,disk:d; -j2 when cargo')
    began=time.perf_counter()
    with path.with_suffix('.txt').open('w',encoding='utf-8') as log:
        process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
        while process.poll() is None:
            result['direct_process_usage']=usage(process._handle)
            time.sleep(.02)
        result['cpu_at_exit']=usage(process._handle)
        result['exit_code']=process.wait()
    result['elapsed_seconds']=time.perf_counter()-began
    result['ended']=context()
    path.write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(dict(output=str(path),elapsed_seconds=result['elapsed_seconds'],exit_code=result['exit_code'])))
    sys.exit(result['exit_code'])
