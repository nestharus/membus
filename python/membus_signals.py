"""Stdlib Windows listener for membus.change/1. Receipt belongs to its caller."""
import ctypes
from ctypes import wintypes as w
import struct
import time
import uuid
from membus import Pipe, Decoder, MAX_VALUE, MAX_FRAME, deadline_ms

def decode_change(body):
    d = Decoder(body)
    if d.read(4) != b'MBC1' or d.unpack('<H') != (1,):
        raise ValueError('unsupported change protocol')
    subscription, cursor = d.read(16), d.read(16)
    ns, key = d.blob().decode('utf8'), d.blob().decode('utf8')
    version, ack, removed, included = d.unpack('<QBBB')
    if ack not in (1, 2) or removed > 1 or included > 1:
        raise ValueError('invalid change flags')
    value = d.blob() if included else None
    if d.pos != len(body) or (value is not None and len(value) > MAX_VALUE):
        raise ValueError('invalid change length')
    return dict(subscription=subscription, cursor=cursor, namespace=ns, key=key,
                version=version, ack=ack, removed=bool(removed), value=value)

class Listener:
    """One persistent pipe connection per subscription; close in a finally/with.

    Create before subscribe, then receive(timeout). No background thread or poller.
    By default LOCAL belongs to this user's logon session. A unique name and
    FIRST_PIPE_INSTANCE prevent accidental replacement of an existing endpoint.
    """
    def __init__(self, endpoint=None):
        self.endpoint = endpoint or r'\\.\pipe\LOCAL\membus-change-' + str(uuid.uuid4())
        k = ctypes.WinDLL('kernel32', use_last_error=True)
        k.CreateNamedPipeW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, w.DWORD, w.DWORD, w.DWORD, w.DWORD, w.LPVOID]
        k.CreateNamedPipeW.restype = w.HANDLE
        k.ConnectNamedPipe.argtypes = [w.HANDLE, w.LPVOID]
        k.DisconnectNamedPipe.argtypes = [w.HANDLE]
        self.k = k
        handle = k.CreateNamedPipeW(self.endpoint, 0x40000000 | 0x80000 | 1, 8, 1, 65536, 65536, 0, None)
        if handle == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        # Adopt the server handle into the existing cancellation-safe Pipe class.
        pipe = Pipe.__new__(Pipe)
        pipe.k = ctypes.WinDLL('kernel32', use_last_error=True)
        # Initialize FFI signatures without opening another pipe.
        self.pipe = pipe
        try:
            self._configure(pipe, handle)
        except BaseException:
            k.CloseHandle(handle)
            raise
        self.connected = False

    @staticmethod
    def _configure(pipe, handle):
        class Overlapped(ctypes.Structure):
            _fields_ = [('Internal', ctypes.c_size_t), ('InternalHigh', ctypes.c_size_t),
                        ('Offset', w.DWORD), ('OffsetHigh', w.DWORD), ('hEvent', w.HANDLE)]
        pipe.O = Overlapped
        pipe.handle = handle
        pipe.k.CreateEventW.restype = w.HANDLE
        pipe.k.CreateEventW.argtypes = [w.LPVOID, w.BOOL, w.BOOL, w.LPCWSTR]
        pipe.k.CloseHandle.argtypes = [w.HANDLE]
        pipe.k.ReadFile.argtypes = [w.HANDLE,w.LPVOID,w.DWORD,ctypes.POINTER(w.DWORD),ctypes.POINTER(Overlapped)]
        pipe.k.WriteFile.argtypes = pipe.k.ReadFile.argtypes
        pipe.k.GetOverlappedResult.argtypes = [w.HANDLE,ctypes.POINTER(Overlapped),ctypes.POINTER(w.DWORD),w.BOOL]
        pipe.k.CancelIoEx.argtypes = [w.HANDLE,ctypes.POINTER(Overlapped)]
        pipe.k.WaitForSingleObject.argtypes = [w.HANDLE,w.DWORD]

    def accept(self, deadline):
        if self.connected:
            return
        event = self.pipe.k.CreateEventW(None, True, False, None)
        if not event:
            raise ctypes.WinError(ctypes.get_last_error())
        operation = self.pipe.O(hEvent=event)
        done = w.DWORD()
        try:
            ok = self.k.ConnectNamedPipe(self.pipe.handle, ctypes.byref(operation))
            error = ctypes.get_last_error()
            if not ok and error in (535, 232):  # connected, possibly already closed after full write
                self.connected = True
                return
            if not ok and error != 997:
                raise ctypes.WinError(error)
            if not ok:
                wait = max(0, deadline - int(time.time()*1000))
                if self.pipe.k.WaitForSingleObject(event, min(wait, 0xfffffffe)) != 0:
                    self.pipe.k.CancelIoEx(self.pipe.handle, ctypes.byref(operation))
                    completed=self.pipe.k.GetOverlappedResult(self.pipe.handle, ctypes.byref(operation), ctypes.byref(done), True)
                    if completed:
                        self.connected=True
                        return
                    raise TimeoutError('signal accept deadline')
                if not self.pipe.k.GetOverlappedResult(self.pipe.handle, ctypes.byref(operation), ctypes.byref(done), False):
                    raise ctypes.WinError(ctypes.get_last_error())
            self.connected = True
        finally:
            self.pipe.k.CloseHandle(event)

    def receive(self, timeout=10):
        deadline = deadline_ms(timeout)
        consumed = 0
        def exact(n):
            nonlocal consumed
            out = bytearray()
            while len(out) < n:
                chunk = self.pipe.io(None, n-len(out), deadline, False)
                if not chunk:
                    raise ConnectionError('signal truncated')
                out.extend(chunk)
                consumed += len(chunk)
            return bytes(out)
        try:
            self.accept(deadline)
            n, = struct.unpack('<I', exact(4))
            if not 57 <= n <= MAX_FRAME:
                raise ValueError('signal frame size')
            return decode_change(exact(n))
        except TimeoutError as error:
            if consumed:
                if self.connected:
                    self.k.DisconnectNamedPipe(self.pipe.handle)
                    self.connected=False
                raise ConnectionError('partial signal timed out; resume from last handled cursor') from error
            raise
        except (OSError, ValueError, ConnectionError):
            if self.connected:
                self.k.DisconnectNamedPipe(self.pipe.handle)
                self.connected = False
            raise

    def close(self):
        if self.connected:
            self.k.DisconnectNamedPipe(self.pipe.handle)
            self.connected = False
        self.pipe.close()

    def __enter__(self): return self
    def __exit__(self, *args): self.close()
