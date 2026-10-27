"""stdlib membus.kv/1 client. One caller owns a connection; retries retain op_id."""
import ctypes
import contextlib
import os
import socket
import struct
import time
import uuid
from typing import NamedTuple

MAX_FRAME = 16 * 1024 * 1024
MAX_VALUE = 4 * 1024 * 1024
MAX_BATCH = 128
def decode_timer(body):
    # Lazy import keeps existing client import paths intact.
    from membus_timers import decode_timer as decode
    return decode(body)
DEFAULT_SCAN_BYTES = 256 * 1024
CODES = {1: ('incompatible_version', False), 2: ('unsupported', False),
         3: ('malformed', False), 4: ('too_large', False), 5: ('deadline', True),
         6: ('busy', True), 7: ('capacity', False), 8: ('quota', False),
         9: ('index_cap', False), 10: ('read_only', True), 11: ('warming', True),
         12: ('conflict', False), 13: ('retry_window_expired', False),
         14: ('op_id_mismatch', False), 15: ('mem_not_enabled', False),
         16: ('io_failed_uncertain',True),17: ('pending_timeout_uncertain',True),
         18: ('dedup_full',True),19: ('pending_conflict',True),
         20: ('cursor_expired',False),21: ('invalid_cursor',False),
         22: ('snapshot_full',True),23: ('resume_gap',False),
         24: ('subscription_missing',False),25: ('timer_missing',False)}

class EpochChanged(Exception):
    def __init__(self, previous, current):
        self.previous,self.current=previous,current
        super().__init__(f'server epoch changed {previous}->{current}; reconcile deterministic key before uncertain retry')

class NotAvailable(Exception):
    """The server predates the call (it answered `unsupported`)."""
    def __init__(self, call):
        super().__init__(f'{call} is not available on this server')

class Refusal(Exception):
    def __init__(self, code, codes=None):
        self.code = code
        self.name, self.transient = (codes or CODES).get(code, ('unknown_refusal', False))
        super().__init__(self.name)

class CallOriginal(NamedTuple):
    """In-memory evidence of one call; retaining it does not authorize a resend."""
    endpoint: tuple
    epoch: int
    opcode: int
    operation_id: str
    namespace: str
    payload: bytes
    first_send_ns: int
    retry_ms: int

class CallUncertain(ConnectionError):
    """An earlier send may have taken effect, regardless of the final error."""
    def __init__(self, original, cause, last_error):
        self.original, self.cause, self.last_error = original, cause, last_error
        self.epoch, self.operation_id = original.epoch, original.operation_id
        super().__init__(f'uncertain operation {self.operation_id}: {last_error}; reconcile original intent before another call')

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
        try:
            if self.transport == 'tcp':
                host, port = self.address.rsplit(':', 1)
                self.channel = socket.create_connection((host, int(port)), max(.001, (deadline-int(time.time()*1000))/1000))
                self.channel.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            else:
                self.channel = Pipe(self.address, deadline)
            response = self.exchange(1, struct.pack('<IB', MAX_FRAME, self.allow_mem)+lp('python-stdlib;timers=1;subscribe=1;scans=1'), deadline)
            decoder = Decoder(response)
            epoch, max_frame, max_value, max_batch, mem, retry_ms = decoder.unpack('<QIIHBQ')
            count, = decoder.unpack('<H')
            codes = {}
            for _ in range(count):
                code, transient = decoder.unpack('<HB')
                if code in codes or code == 0 or transient not in (0, 1):
                    self.close()
                    raise ValueError('invalid refusal-code list')
                codes[code] = (decoder.blob().decode(), bool(transient))
            self.signal_limits = None
            if 23 in codes:
                names=('protocol_version','max_frame','history_max','history_budget','subscriptions_max',
                       'queued_max','subscriber_bytes','queue_bytes','write_deadline_ms')
                self.signal_limits=dict(zip(names,decoder.unpack('<HIQQHHQQI')))
            self.timer_limits = None
            if 25 in codes:
                names=('protocol_version','identities_max','timer_bytes','payload_max','min_period_ms','jitter_max_ms')
                self.timer_limits=dict(zip(names,decoder.unpack('<HIQIQQ')))
            if (decoder.pos != len(response) or
                    any(codes.get(code) != value for code, value in CODES.items() if code <= 19) or
                    any(code in codes and codes[code] != value for code, value in CODES.items())):
                self.close()
                raise ValueError('incompatible refusal-code list')
            self.epoch, self.max_frame, self.max_value = epoch, max_frame, max_value
            self.max_batch, self.mem, self.retry_ms = max_batch, mem, retry_ms
            self.codes = codes
        except BaseException:
            self.close()
            raise
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
            codes = getattr(self, 'codes', CODES)
            if code not in codes or len(body) != 15:
                self.close()
                raise ValueError('invalid refusal reply')
            raise Refusal(code, codes)
        return body[15:]

    def call(self, opcode, ns, suffix=b'', op_id=None, deadline=None, *, retry_transport=True):
        """At most one resend. See docs/PYTHON_RETRY.md for custody and limits."""
        op_id = op_id or str(uuid.uuid4())
        deadline = deadline or deadline_ms(self.timeout)
        payload = struct.pack('<Q', deadline)+lp(op_id)+lp(ns)+suffix
        creates_snapshot = opcode in (10, 16, 21, 22) or (opcode in (11, 12, 14, 15) and suffix[:4] == b'\x00' * 4)
        # Dispatch contracts, including generic callers, determine retry policy.
        # Known-token scans/release and read/status operations retain their policy.
        read_retry = opcode in (2, 3, 8, 9, 11, 12, 13, 14, 15, 18, 19, 20, 23, 25, 26, 27, 31)
        bounded_retry = opcode in (4, 5, 6, 7, 17, 24)
        original = lost = None
        for attempt in range(2):
            try:
                if self.channel is None: self.connect(deadline)
                if bounded_retry and retry_transport:
                    if original is None:
                        original = CallOriginal((self.transport, self.address), self.epoch,
                                                opcode, op_id, ns, payload,
                                                time.monotonic_ns(), self.retry_ms)
                    else:
                        if (self.transport, self.address) != original.endpoint:
                            raise ValueError('original retry endpoint changed')
                        if self.epoch != original.epoch:
                            raise EpochChanged(original.epoch, self.epoch)
                        if time.monotonic_ns() - original.first_send_ns >= original.retry_ms * 1_000_000:
                            raise TimeoutError('original retry horizon elapsed')
                        if int(time.time()*1000) >= deadline:
                            raise TimeoutError('original request deadline elapsed')
                return self.exchange(opcode, payload, deadline)
            except Exception as error:
                transport_error = isinstance(error, (OSError, ConnectionError))
                if transport_error or lost is not None:
                    self.close()
                if original is not None and transport_error and lost is None:
                    lost = error
                if (not transport_error or not retry_transport or creates_snapshot or attempt
                        or not (read_retry or bounded_retry) or int(time.time()*1000) >= deadline
                        or (original is not None and
                            time.monotonic_ns() - original.first_send_ns >= original.retry_ms * 1_000_000)):
                    if lost is not None:
                        raise CallUncertain(original, lost, error) from error
                    raise
        raise AssertionError('unreachable')

    def get(self, ns, key, **options):
        return Decoder(self.call(2, ns, lp(key), **options)).value()

    def timer_set(self, ns, timer_id, key, at_ms, *, period_ms=0, jitter_ms=(0,0),
                  payload=b'', expected=None, ack=1, **options):
        """Persist/replace a schedule. Lost reply requires status reconciliation before retry."""
        if self.timer_limits is None: raise Refusal(2,self.codes)
        low,high=jitter_ms
        suffix=lp(timer_id)+struct.pack('<Q',2**64-1 if expected is None else expected)+lp(key)
        suffix+=struct.pack('<QQqq',at_ms,period_ms,low,high)+lp(payload)+bytes([ack])
        result=self.call(21,ns,suffix,**options)
        if len(result)!=8:raise ValueError('invalid timer set reply')
        return struct.unpack('<Q',result)[0]

    def timer_cancel(self, ns, timer_id, generation, **options):
        if self.timer_limits is None: raise Refusal(2,self.codes)
        if self.call(22,ns,lp(timer_id)+struct.pack('<Q',generation),**options):
            raise ValueError('invalid timer cancel reply')

    def timer_status(self, ns, timer_id, **options):
        if self.timer_limits is None: raise Refusal(2,self.codes)
        d=Decoder(self.call(23,ns,lp(timer_id),**options));present,=d.unpack('<B')
        if present==0:
            if d.pos!=len(d.data):raise ValueError('invalid absent timer status')
            return None
        if present!=1:raise ValueError('invalid timer status')
        generation,active,error,next_index,next_due=d.unpack('<QBHQQ')
        record=Decoder(d.blob())
        if record.read(4)!=b'MTS1':raise ValueError('invalid schedule record')
        names=('namespace','timer_id','key');schedule=dict(zip(names,(record.blob().decode('utf8') for _ in names)))
        schedule.update(zip(('at_ms','period_ms','jitter_low_ms','jitter_high_ms','seed'),record.unpack('<QQqqQ')))
        schedule['payload']=record.blob();last=d.blob()
        if active>1 or d.pos!=len(d.data) or record.pos!=len(record.data):raise ValueError('invalid timer status length')
        return dict(generation=generation,active=bool(active),error=error,next_index=next_index,
                    next_due_ms=next_due,schedule=schedule,last=decode_timer(last) if last else None)

    def get_many(self, ns, keys, **options):
        d = Decoder(self.call(3, ns, struct.pack('<H',len(keys))+b''.join(lp(k) for k in keys), **options))
        count, = d.unpack('<H')
        return [d.value() for _ in range(count)]

    def put(self, ns, key, value, ack=1, expected=None, op_id=None, deadline=None, *, retry_transport=True):
        opcode = 4 if expected is None else 5
        suffix = lp(key)+struct.pack('<B',ack)
        if expected is not None:
            suffix += struct.pack('<Q',expected)
        suffix += lp(value)
        options = {} if retry_transport else {'retry_transport': False}
        version, achieved = struct.unpack('<QB', self.call(opcode,ns,suffix,op_id,deadline,**options))
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

    def health2(self, ns='ledger'):
        """HEALTH2 (opcode 27): the store's state, limits and chunk retirement as a dict of strings.
        Raises NotAvailable on a server without it."""
        try:
            d = Decoder(self.call(27, ns))
        except Refusal as error:
            if error.code == 2:
                raise NotAvailable('health2') from error
            raise
        count, = d.unpack('<H')
        fields = {}
        for _ in range(count):
            key, sep, value = d.blob().decode('utf8').partition('=')
            if not sep or key in fields:
                raise ValueError('invalid health2 field')
            fields[key] = value
        if d.pos != len(d.data):
            raise ValueError('invalid health2 reply')
        return fields

    def get_status(self, ns, key, **options):
        """GET_STATUS (opcode 26): {'status': 'live'|'expired'|'missing', 'version': n}. 'expired' holds for 30 days
        after an expire class retires the key. Raises NotAvailable on a server without it."""
        try:
            d = Decoder(self.call(26, ns, lp(key), **options))
        except Refusal as error:
            if error.code == 2:
                raise NotAvailable('get_status') from error
            raise
        status, version = d.unpack('<BQ')
        if status > 2 or d.pos != len(d.data):
            raise ValueError('invalid status reply')
        return {'status': ('live', 'expired', 'missing')[status], 'version': version}

    def subscribe(self, ns, endpoint, prefix='', *, include_value=False, cursor=None,
                  protocol='membus.change/1', max_frame=MAX_FRAME, **options):
        """Register live or resume retained changes. A lost reply is not auto-retried."""
        if 23 not in self.codes:
            raise Refusal(2, self.codes)
        d = Decoder(self.call(16, ns, lp(prefix)+lp(endpoint)+bytes([bool(include_value)])+lp(cursor or b'')+lp(protocol)+struct.pack('<I',max_frame), **options))
        sub, start = d.blob(), d.blob()
        if len(sub) != 16 or len(start) != 16 or d.pos != len(d.data):
            raise ValueError('invalid subscription reply')
        return dict(subscription=sub, cursor=start)

    def unsubscribe(self, ns, subscription, **options):
        if self.call(17, ns, lp(subscription), **options):
            raise ValueError('invalid unsubscribe reply')

    def subscription_status(self, ns, subscription, **options):
        d = Decoder(self.call(18, ns, lp(subscription), **options))
        active, status, queued, charged_bytes, written, failed, dropped = d.unpack('<BBIQQQQ')
        cursor = d.blob()
        if active > 1 or status > 4 or len(cursor) != 16 or d.pos != len(d.data):
            raise ValueError('invalid subscription status')
        return dict(active=bool(active), status=('queued','written','failed','dropped','cancelled')[status],
                    queued=queued, charged_bytes=charged_bytes, written=written,
                    failed=failed, dropped=dropped, cursor=cursor)

    def signal_usage(self, ns='quota'):
        names = ('head','floor','history_count','history_bytes','queue_bytes','subscriptions',
                 'history_max','history_budget','queue_budget')
        return dict(zip(names, struct.unpack('<9Q',self.call(19,ns))))

    def capture_metrics(self, ns='ledger'):
        names = ('captures','lock_wait_ns','charge_walk_ns','selection_walk_ns','namespace_copy_ns',
                 'lock_held_ns','sort_and_release_ns')
        return dict(zip(names, struct.unpack('<7Q',self.call(20,ns))))

    def snapshot_open(self, ns, **options):
        """Capture one committed namespace view; release the returned bytes token."""
        d = Decoder(self.call(10, ns, **options))
        token = d.blob()
        if len(token) != 16 or d.pos != len(d.data):
            raise ValueError('invalid snapshot reply')
        return token

    @contextlib.contextmanager
    def snapshot(self, ns, **options):
        """Release a shared snapshot when its with block ends, including errors."""
        token = self.snapshot_open(ns, **options)
        consumer_error = None
        try:
            yield token
        except BaseException as error:
            consumer_error = error
            raise
        finally:
            try:
                # Release is a separate bounded call, with fresh deadline/op_id.
                self.snapshot_release(ns, token)
            except BaseException as error:
                if isinstance(error, Refusal) and error.code == 20:
                    pass
                elif consumer_error is not None:
                    if hasattr(consumer_error, 'add_note'):
                        consumer_error.add_note('snapshot release failed: ' + str(error))
                else:
                    raise

    def snapshot_release(self, ns, snapshot, **options):
        """Release retention. A released/expired token returns cursor_expired."""
        if self.call(13, ns, lp(snapshot), **options):
            raise ValueError('invalid release reply')

    def snapshot_require_complete(self, ns, snapshot, **options):
        """Require captured complete-query eligibility on this exact token.

        Unsupported providers raise NotAvailable; captured uncertainty and token
        refusals retain their original codes. This does not inspect live health.
        """
        if not isinstance(snapshot, bytes) or len(snapshot) != 16:
            raise ValueError('explicit 16-byte snapshot token required')
        try:
            reply = self.call(31, ns, lp(snapshot), **options)
        except Refusal as error:
            if error.code == 2:
                raise NotAvailable('snapshot_require_complete') from error
            raise
        if reply != b'':
            raise ValueError('invalid snapshot completeness reply')

    def _scan(self, opcode, ns, bounds, snapshot, cursor, limit, max_bytes, options):
        if not 1 <= limit <= MAX_BATCH or not 42 <= max_bytes <= MAX_FRAME:
            raise ValueError('scan limit must be 1..128; max_bytes must be 42..MAX_FRAME')
        suffix = lp(snapshot or b'') + lp(cursor or b'') + struct.pack('<HI', limit, max_bytes)
        suffix += b''.join(lp(bound) for bound in bounds)
        d = Decoder(self.call(opcode, ns, suffix, **options))
        token, next_cursor = d.blob(), d.blob()
        done, count = d.unpack('<BH')
        items = []
        for _ in range(count):
            key = d.blob().decode('utf8')
            if opcode in (14, 15):
                items.append(dict(key=key))
            else:
                value = d.value()
                if value is None:
                    raise ValueError('scan returned tombstone')
                items.append(dict(key=key, **value))
        if (len(token) != 16 or done not in (0, 1) or bool(done) == bool(next_cursor)
                or count > limit or d.pos != len(d.data)):
            raise ValueError('invalid scan reply')
        return dict(snapshot=token, cursor=next_cursor, done=bool(done), items=items)

    def scan_prefix(self, ns, prefix='', *, snapshot=None, cursor=None,
                    limit=128, max_bytes=DEFAULT_SCAN_BYTES, keys_only=False, **options):
        """One bounded page. Reuse snapshot/cursor to retry or continue the query."""
        return self._scan(14 if keys_only else 11, ns, [prefix], snapshot, cursor, limit, max_bytes, options)

    def scan_range(self, ns, lower='', upper='', *, snapshot=None, cursor=None,
                   limit=128, max_bytes=DEFAULT_SCAN_BYTES, keys_only=False, **options):
        """One page of [lower, upper) in UTF-8 order; empty upper means no bound."""
        return self._scan(15 if keys_only else 12, ns, [lower, upper], snapshot, cursor, limit, max_bytes, options)
