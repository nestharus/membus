"""Read-only Windows device cache query; no changes to drive policy."""
import ctypes, json, struct
from ctypes import wintypes
k=ctypes.WinDLL('kernel32',use_last_error=True)
k.CreateFileW.argtypes=[wintypes.LPCWSTR,wintypes.DWORD,wintypes.DWORD,ctypes.c_void_p,wintypes.DWORD,wintypes.DWORD,wintypes.HANDLE]
k.CreateFileW.restype=wintypes.HANDLE
k.DeviceIoControl.argtypes=[wintypes.HANDLE,wintypes.DWORD,ctypes.c_void_p,wintypes.DWORD,ctypes.c_void_p,wintypes.DWORD,ctypes.POINTER(wintypes.DWORD),ctypes.c_void_p]
k.CloseHandle.argtypes=[wintypes.HANDLE]
for disk in (1,2):
    path=r'\\.\PhysicalDrive'+str(disk)
    h=k.CreateFileW(path,0,3,None,3,0,None)
    row={'disk':disk,'path':path,'method':'IOCTL_STORAGE_QUERY_PROPERTY StorageDeviceWriteCacheProperty=4'}
    if h==wintypes.HANDLE(-1).value:
        row['error']=ctypes.get_last_error()
    else:
        try:
            query=ctypes.create_string_buffer(struct.pack('<III',4,0,0))
            out=ctypes.create_string_buffer(1024);received=wintypes.DWORD()
            if not k.DeviceIoControl(h,0x2D1400,query,len(query),out,len(out),ctypes.byref(received),None):
                row['error']=ctypes.get_last_error()
            else:
                fields=['Version','Size','WriteCacheType','WriteCacheEnabled','WriteCacheChangeable','WriteThroughSupported','FlushCacheSupported','UserDefinedPowerProtection','NVCacheEnabled']
                values=struct.unpack_from('<IIIIII???',out.raw)
                row.update(zip(fields,values));row['received_bytes']=received.value;row['raw_hex']=out.raw[:received.value].hex()
        finally:k.CloseHandle(h)
    print(json.dumps(row))
