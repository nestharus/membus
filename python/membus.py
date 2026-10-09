"""stdlib membus.kv/1 client. One caller owns a connection; retries retain op_id."""
import ctypes
import os
import socket
import struct
import time
import uuid

MAX_FRAME = 16 * 1024 * 1024
MAX_VALUE = 4 * 1024 * 1024
MAX_BATCH = 128
CODES = {1: ('incompatible_version', False), 2: ('unsupported', False),
         3: ('malformed', False), 4: ('too_large', False), 5: ('deadline', True),
         6: ('busy', True), 7: ('capacity', False), 8: ('quota', False),
         9: ('index_cap', False), 10: ('read_only', True), 11: ('warming', True),
         12: ('conflict', False), 13: ('retry_window_expired', False),
         14: ('op_id_mismatch', False), 15: ('mem_not_enabled', False),
         16: ('io_failed_uncertain',True),17: ('pending_timeout_uncertain',True),
         18: ('dedup_full',True),19: ('pending_conflict',True)}

class EpochChanged(Exception):
    def __init__(self, previous, current):
        self.previous,self.current=previous,current
        super().__init__(f'server epoch changed {previous}->{current}; reconcile deterministic key before uncertain retry')

class Refusal(Exception):
    def __init__(self, code):
        self.code = code
        self.name, self.transient = CODES[code]
        super().__init__(self.name)

def deadline_ms(seconds=10):
    return int(time.time() * 1000 + seconds * 1000)

def lp(value):
    if isinstance(value, str):
        value = value.encode('utf8')
    return struct.pack('<I', len(value)) + value

def frame(opcode, request_id, payload):
    body = struct.pack('<HHBQ', 1, 0, opcode, request_id) + payload
    if len(body) > MAX_FRAME:
        raise Refusal(4)
    return struct.pack('<I', len(body)) + body

class Decoder:
    def __init__(self, data):
        self.data, self.pos = data, 0

    def read(self, count):
        end = self.pos + count
        if end > len(self.data):
            raise ValueError('truncated reply')
        result = self.data[self.pos:end]
        self.pos = end
        return result

    def unpack(self, fmt):
        return struct.unpack(fmt, self.read(struct.calcsize(fmt)))

    def blob(self):
        count, = self.unpack('<I')
        if count > MAX_FRAME:
            raise ValueError('oversize reply field')
        return self.read(count)

    def value(self):
        present, = self.unpack('<B')
        if not present:
            return None
        version, ack = self.unpack('<QB')
        return {'version': version, 'ack': ack, 'value': self.blob()}

class Pipe:
    """Windows overlapped pipe I/O; cancellation drains before buffers are released."""
    def __init__(self, address, deadline):
        from ctypes import wintypes as w
        self.k = ctypes.WinDLL('kernel32', use_last_error=True)
        self.k.CreateFileW.restype = w.HANDLE
        self.k.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, w.LPVOID,
                                      w.DWORD, w.DWORD, w.HANDLE]
        self.k.CreateEventW.restype = w.HANDLE
        self.k.CreateEventW.argtypes = [w.LPVOID, w.BOOL, w.BOOL, w.LPCWSTR]
        self.k.CloseHandle.argtypes = [w.HANDLE]
        class Overlapped(ctypes.Structure):
            _fields_ = [('Internal', ctypes.c_size_t), ('InternalHigh', ctypes.c_size_t),
                        ('Offset', w.DWORD), ('OffsetHigh', w.DWORD), ('hEvent', w.HANDLE)]
        self.O = Overlapped
        self.k.ReadFile.argtypes = [w.HANDLE, w.LPVOID, w.DWORD, ctypes.POINTER(w.DWORD), ctypes.POINTER(Overlapped)]
        self.k.WriteFile.argtypes = self.k.ReadFile.argtypes
        self.k.GetOverlappedResult.argtypes = [w.HANDLE, ctypes.POINTER(Overlapped), ctypes.POINTER(w.DWORD), w.BOOL]
        self.k.CancelIoEx.argtypes = [w.HANDLE, ctypes.POINTER(Overlapped)]
        self.k.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
        while True:
            self.handle = self.k.CreateFileW(address, 0xc0000000, 0, None, 3, 0x40000000, None)
            if self.handle != ctypes.c_void_p(-1).value:
                break
            if int(time.time()*1000) >= deadline:
                raise TimeoutError('pipe connect deadline')
            time.sleep(.005)

    def io(self, data, count, deadline, write):
        from ctypes import wintypes as w
        count=min(count,65536)
        buffer = ctypes.create_string_buffer(data[:count] if write else count)
        event = self.k.CreateEventW(None, True, False, None)
        if not event:
            raise ctypes.WinError(ctypes.get_last_error())
        operation = self.O(hEvent=event)
        done = w.DWORD()
        try:
            function = self.k.WriteFile if write else self.k.ReadFile
            ok = function(self.handle, buffer, count, ctypes.byref(done), ctypes.byref(operation))
            if not ok:
                error = ctypes.get_last_error()
                if error != 997:
                    raise ctypes.WinError(error)
                wait = max(0, deadline-int(time.time()*1000))
                result = self.k.WaitForSingleObject(event, min(wait, 0xfffffffe))
                if result != 0:
                    self.k.CancelIoEx(self.handle, ctypes.byref(operation))
                    self.k.GetOverlappedResult(self.handle, ctypes.byref(operation), ctypes.byref(done), True)
                    raise TimeoutError('pipe I/O deadline; outcome uncertain after send')
                if not self.k.GetOverlappedResult(self.handle, ctypes.byref(operation), ctypes.byref(done), False):
                    raise ctypes.WinError(ctypes.get_last_error())
            return done.value if write else buffer.raw[:done.value]
        finally:
            self.k.CloseHandle(event)

    def close(self):
        if self.handle:
            self.k.CloseHandle(self.handle)
            self.handle = None

class Client:
    def __init__(self, address, transport='tcp', allow_mem=False, timeout=10):
        self.address, self.transport = address, transport
        self.allow_mem, self.timeout, self.request_id = allow_mem, timeout, 0
        self.channel = None
        self.connect(deadline_ms(timeout))

    def connect(self, deadline):
        previous=getattr(self,'epoch',None)
        if self.transport == 'tcp':
            host, port = self.address.rsplit(':', 1)
            self.channel = socket.create_connection((host, int(port)), max(.001, (deadline-int(time.time()*1000))/1000))
            self.channel.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        else:
            self.channel = Pipe(self.address, deadline)
        response = self.exchange(1, struct.pack('<IB', MAX_FRAME, self.allow_mem)+lp('python-stdlib'), deadline)
        decoder = Decoder(response)
        self.epoch, self.max_frame, self.max_value, self.max_batch, self.mem, self.retry_ms = decoder.unpack('<QIIHBQ')
        count, = decoder.unpack('<H')
        codes = {}
        for _ in range(count):
            code, transient = decoder.unpack('<HB')
            codes[code] = (decoder.blob().decode(), bool(transient))
        if codes != CODES:
            self.close()
            raise ValueError('unknown refusal-code list')
        if previous is not None and self.epoch!=previous:
            raise EpochChanged(previous,self.epoch)

    def close(self):
        if self.channel:
            self.channel.close()
            self.channel = None

    def read_exact(self, count, deadline):
        output = bytearray()
        while len(output) < count:
            if int(time.time()*1000) >= deadline:
                raise TimeoutError('read deadline; outcome uncertain after send')
            if self.transport == 'tcp':
                self.channel.settimeout(max(.001,(deadline-int(time.time()*1000))/1000))
                chunk = self.channel.recv(count-len(output))
            else:
                chunk = self.channel.io(None, count-len(output), deadline, False)
            if not chunk:
                raise ConnectionError('partial frame')
            output.extend(chunk)
        return bytes(output)

    def write_all(self, data, deadline):
        position = 0
        while position < len(data):
            if int(time.time()*1000) >= deadline:
                raise TimeoutError('write deadline; partial send uncertain')
            if self.transport == 'tcp':
                self.channel.settimeout(max(.001,(deadline-int(time.time()*1000))/1000))
                count = self.channel.send(data[position:])
            else:
                count = self.channel.io(data[position:], len(data)-position, deadline, True)
            if not count:
                raise ConnectionError('zero write')
            position += count

    def exchange(self, opcode, payload, deadline):
        self.request_id += 1
        encoded=frame(opcode,self.request_id,payload)
        if len(encoded)-4>getattr(self,'max_frame',MAX_FRAME): raise Refusal(4)
        self.write_all(encoded, deadline)
        count, = struct.unpack('<I', self.read_exact(4, deadline))
        if not 15 <= count <= getattr(self,'max_frame',MAX_FRAME):
            self.close()
            raise ValueError('reply frame size')
        body = self.read_exact(count, deadline)
        major, minor, op, request_id, code = struct.unpack('<HHBQH', body[:15])
        if (major, minor, op, request_id) != (1, 0, opcode|128, self.request_id):
            self.close()
            raise ValueError('reply association')
        if code:
            raise Refusal(code)
        return body[15:]

    def call(self, opcode, ns, suffix=b'', op_id=None, deadline=None):
        op_id = op_id or str(uuid.uuid4())
        deadline = deadline or deadline_ms(self.timeout)
        payload = struct.pack('<Q', deadline)+lp(op_id)+lp(ns)+suffix
        for attempt in range(2):
            try:
                if self.channel is None: self.connect(deadline)
                return self.exchange(opcode, payload, deadline)
            except (OSError, ConnectionError):
                self.close()
                if attempt or int(time.time()*1000) >= deadline:
                    raise
                self.connect(deadline)
        raise AssertionError('unreachable')

    def get(self, ns, key, **options):
        return Decoder(self.call(2, ns, lp(key), **options)).value()

    def get_many(self, ns, keys, **options):
        d = Decoder(self.call(3, ns, struct.pack('<H',len(keys))+b''.join(lp(k) for k in keys), **options))
        count, = d.unpack('<H')
        return [d.value() for _ in range(count)]

    def put(self, ns, key, value, ack=1, expected=None, op_id=None, deadline=None):
        opcode = 4 if expected is None else 5
        suffix = lp(key)+struct.pack('<B',ack)
        if expected is not None:
            suffix += struct.pack('<Q',expected)
        suffix += lp(value)
        version, achieved = struct.unpack('<QB', self.call(opcode,ns,suffix,op_id,deadline))
        return {'version':version,'ack':achieved}

    def remove(self, ns, key, expected, ack=1, **options):
        version,achieved=struct.unpack('<QB',self.call(6,ns,lp(key)+struct.pack('<BQ',ack,expected),**options))
        return {'version':version,'ack':achieved}

    def batch(self, ns, items, **options):
        if len(items)>MAX_BATCH:
            raise Refusal(4)
        parts=[struct.pack('<H',len(items))]
        for item in items:
            op=6 if item.get('remove') else (5 if 'expected' in item else 4)
            part=struct.pack('<B',op)+lp(item['op_id'])+lp(item['key'])+struct.pack('<B',item.get('ack',1))
            if op in (5,6):
                part+=struct.pack('<Q',item['expected'])
            parts.append(part if op==6 else part+lp(item['value']))
        d=Decoder(self.call(7,ns,b''.join(parts),**options))
        count,=d.unpack('<H')
        outcomes=[dict(zip(('code','version','ack'),d.unpack('<HQB'))) for _ in range(count)]
        for outcome in outcomes: outcome['name'],outcome['transient']=CODES.get(outcome['code'],('success',False))
        return outcomes

    def health(self, ns='ledger'):
        return dict(zip(('bytes_written','flush_calls','keys','namespace_keys','namespace_quota','index_cap','high_water','namespace_high_water'),
                        struct.unpack('<QQQQQQBB',self.call(8,ns))))
