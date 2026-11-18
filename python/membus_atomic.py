"""Canonical bounded atomic publication. See docs/PYTHON_ATOMIC.md for custody.

This binding uses the existing Client transport once per call. It never reconnects
or retries a mutation. Only a complete validated population can prepare an original.
"""
from dataclasses import dataclass
from enum import IntEnum
import hashlib
import os
from pathlib import Path
import struct
import time
from membus import CODES, MAX_BATCH, MAX_FRAME, MAX_VALUE, Refusal, lp


class Ack(IntEnum):
    OS = 1
    FLUSH = 2


class Invalid(ValueError):
    pass


class Refused(Exception):
    def __init__(self, status, resource=None):
        self.status, self.resource = status, resource
        super().__init__(f'atomic refusal {status}' + (f' resource {resource}' if resource else ''))


class Uncertain(Exception):
    def __init__(self, original, cause):
        self.original, self.cause = original, cause
        super().__init__(f'atomic outcome uncertain: {cause}')


def _identifier(s):
    if not isinstance(s, str) or not s or '\0' in s or len(s.encode('utf8')) > 1024:
        raise Invalid('identifier')
    return s


class _Reader:
    def __init__(self, data):
        self.data, self.pos = data, 0

    def take(self, n):
        if n < 0 or self.pos+n > len(self.data):
            raise Invalid('incomplete encoding')
        out = self.data[self.pos:self.pos+n]
        self.pos += n
        return out

    def unpack(self, fmt):
        return struct.unpack(fmt, self.take(struct.calcsize(fmt)))

    def blob(self, limit):
        n, = self.unpack('<I')
        if n > limit:
            raise Invalid('encoded bound')
        return self.take(n)

    def text(self):
        try:
            return _identifier(self.blob(1024).decode('utf8'))
        except UnicodeDecodeError as error:
            raise Invalid('identifier encoding') from error

    def end(self):
        if self.pos != len(self.data):
            raise Invalid('trailing bytes')


@dataclass(frozen=True)
class Limits:
    max_keys: int
    max_capture_bytes: int
    max_changes: int
    max_original_bytes: int

    def encode(self):
        if (type(self.max_keys) is not int or not 1 <= self.max_keys <= 2**32-1
                or type(self.max_capture_bytes) is not int or not 1 <= self.max_capture_bytes <= 2**63-1
                or type(self.max_changes) is not int or not 1 <= self.max_changes <= 65535
                or type(self.max_original_bytes) is not int or not 1 <= self.max_original_bytes <= MAX_FRAME//3):
            raise Invalid('population limits')
        return struct.pack('<IQHI', self.max_keys, self.max_capture_bytes,
                           self.max_changes, self.max_original_bytes)

    @classmethod
    def decode(cls, r):
        v = cls(*r.unpack('<IQHI'))
        v.encode()
        return v


@dataclass(frozen=True)
class Change:
    key: str
    value: bytes | None


@dataclass(frozen=True)
class Row:
    key: str
    version: int
    ack: Ack
    value: bytes


def _digest(rows):
    h = hashlib.sha256(b'MAP2')
    for row in rows:
        h.update(lp(row.key))
        h.update(struct.pack('<Q', row.version))
    h.update(struct.pack('<Q', len(rows)))
    return h.digest()


@dataclass(frozen=True, init=False)
class View:
    namespace: str
    epoch: int
    expires: int
    limits: Limits
    rows: tuple
    _guard: bytes
    charged_bytes: int

    def prepare(self, operation_id, ack, changes):
        _identifier(operation_id)
        ack = Ack(ack)
        changes = tuple(changes)
        if not 1 <= len(changes) <= self.limits.max_changes:
            raise Invalid('final change count')
        # Measure before joining value buffers into an original.
        size = 85 + len(self.namespace.encode()) + len(operation_id.encode())
        seen = set()
        for c in changes:
            _identifier(c.key)
            if c.key in seen or not (c.value is None or type(c.value) is bytes and len(c.value) <= MAX_VALUE):
                raise Invalid('duplicate target or unsupported value')
            seen.add(c.key)
            size += 9 + len(c.key.encode()) + (len(c.value) if c.value is not None else 0)
        if size > self.limits.max_original_bytes:
            raise Refused('Resource', 4)
        raw = b'MAR2' + lp(self.namespace) + lp(operation_id)
        raw += struct.pack('<QQB', self.epoch, self.expires, ack) + self._guard
        raw += struct.pack('<H', len(changes))
        raw += b''.join(lp(c.key) + bytes([c.value is None]) + lp(c.value or b'') for c in changes)
        return Original.from_bytes(raw)


@dataclass(frozen=True, init=False)
class Original:
    raw: bytes
    namespace: str
    operation_id: str
    epoch: int
    ack: Ack

    @classmethod
    def from_bytes(cls, raw):
        if type(raw) is not bytes or len(raw) > MAX_FRAME//3:
            raise Invalid('original size')
        r = _Reader(raw)
        magic = r.take(4)
        if magic not in (b'MAR1', b'MAR2'):
            raise Invalid('original version')
        ns, op = r.text(), r.text()
        if ns in ('__membus_policy_v1', '__membus_timers_v1'):
            raise Invalid('reserved namespace')
        epoch, expires, ack = r.unpack('<QQB')
        ack = Ack(ack)
        count, = r.unpack('<I')
        if magic == b'MAR1':
            if len(raw)+29 > MAX_FRAME//3:
                raise Invalid('legacy original budget')
            if count > (len(raw) - r.pos) // 13:
                raise Invalid('legacy population count')
            previous = b''
            for _ in range(count):
                key = r.text().encode()
                version, = r.unpack('<Q')
                if key <= previous or not version:
                    raise Invalid('population order/version')
                previous = key
            max_changes = 128
        else:
            r.take(32)
            limits = Limits.decode(r)
            if not epoch or not expires or count > limits.max_keys or len(raw) > limits.max_original_bytes:
                raise Invalid('original capture bounds')
            max_changes = limits.max_changes
        n, = r.unpack('<H')
        if not 1 <= n <= max_changes:
            raise Invalid('final count')
        keys = set()
        for _ in range(n):
            key = r.text()
            remove, = r.unpack('<B')
            value = r.blob(MAX_VALUE)
            if key in keys or remove not in (0, 1) or remove and value:
                raise Invalid('final set')
            keys.add(key)
        r.end()
        obj = object.__new__(cls)
        for k, v in dict(raw=raw, namespace=ns, operation_id=op, epoch=epoch, ack=ack).items():
            object.__setattr__(obj, k, v)
        return obj


def _reply(raw):
    r = _Reader(raw)
    if r.take(4) != b'MAW1':
        raise Invalid('atomic reply version')
    status, = r.unpack('<B')
    if status == 0:
        return raw[r.pos:]
    names = {1:'Conflict', 2:'Expired', 3:'Unsupported', 4:'Resource', 5:'Invalid',
             6:'IdentityMismatch', 7:'Uncertain', 8:'RestartUncertain',
             9:'RetryExpired', 10:'Unavailable'}
    if status not in names:
        raise Invalid('unknown atomic status')
    code = r.unpack('<H')[0] if status == 4 else None
    if status == 4 and code not in (4, 6, 7, 8, 9, 18):
        raise Invalid('atomic resource code')
    r.end()
    raise Refused(names[status], code)


def _request_payload(client, ns, op, suffix, deadline):
    _identifier(ns)
    _identifier(op)
    if type(deadline) is not int or not 0 < deadline < 2**64:
        raise Invalid('explicit Unix millisecond deadline required')
    if client.channel is None:
        raise Invalid('explicit connected client required')
    if int(time.time()*1000) >= deadline:
        raise Refused('Expired')
    size = 13 + 8 + 8 + len(ns.encode()) + len(op.encode()) + len(suffix)
    if size > client.max_frame:
        raise Refused('Resource', 4)
    return struct.pack('<Q', deadline)+lp(op)+lp(ns)+suffix


def _exchange(client, opcode, ns, op, suffix, deadline):
    return client.exchange(opcode, _request_payload(client, ns, op, suffix, deadline), deadline)


def capture(client, namespace, limits, read_id, deadline):
    """Return one complete view or raise. Partial pages never escape as a View."""
    encoded_limits = limits.encode()
    try:
        payload = _request_payload(client, namespace, read_id, encoded_limits, deadline)
    except ValueError:
        client.close()
        raise
    codes = getattr(client, 'codes', {})
    if any(codes.get(code) != CODES[code] for code in (20, 21, 22)):
        raise Invalid('scans unavailable')
    # HELLO's max_batch field also bounds scan pages.
    page_limit = getattr(client, 'max_batch', None)
    if type(page_limit) is not int or not 1 <= page_limit <= MAX_BATCH:
        raise Invalid('invalid scan page limit')
    try:
        raw = _reply(client.exchange(30, payload, deadline))
        r = _Reader(raw)
        if r.take(4) != b'MAS2' or r.text() != namespace:
            raise Invalid('capture identity')
        epoch, expires = r.unpack('<QQ')
        start = r.pos
        count, = r.unpack('<I')
        digest = r.take(32)
        actual_limits = Limits.decode(r)
        guard = raw[start:r.pos]
        charge, = r.unpack('<Q')
        token = r.take(16)
        r.end()
    except (Invalid, ValueError):
        client.close()
        raise
    if (not epoch or not expires or epoch != client.epoch or actual_limits != limits
            or count > limits.max_keys or charge > limits.max_capture_bytes or struct.unpack('<Q', token[:8])[0] != epoch):
        client.close()
        raise Invalid('capture association or bounds')
    rows, cursor, charged = [], b'', 256+len(namespace.encode())+4*(MAX_FRAME//3)+4096
    primary = None
    try:
        while True:
            # Canonical scan operation on the exact retained token. Direct exchange
            # prevents legacy call() reconnect/retry behavior in this binding.
            suffix = lp(token)+lp(cursor)+struct.pack('<HI', page_limit, client.max_frame)+lp(b'')
            page = _Reader(_exchange(client, 11, namespace, read_id, suffix, deadline))
            returned, next_cursor = page.blob(16), page.blob(2096)
            done, n = page.unpack('<BH')
            if (returned != token or done not in (0, 1) or bool(done) == bool(next_cursor)
                    or n > page_limit or not n and not done):
                raise Invalid('incomplete atomic page')
            for _ in range(n):
                key = page.text()
                present, version, ack = page.unpack('<BQB')
                value = page.blob(MAX_VALUE)
                if present != 1 or not version or ack not in (1, 2) or rows and rows[-1].key.encode() >= key.encode():
                    raise Invalid('atomic row/order')
                charged += 2*len(key.encode())+640+2*len(value)
                if len(rows) >= count or charged > limits.max_capture_bytes:
                    raise Invalid('capture budget exceeded')
                rows.append(Row(key, version, Ack(ack), value))
            page.end()
            if done:
                break
            if next_cursor == cursor:
                raise Invalid('nonadvancing atomic cursor')
            cursor = next_cursor
        rows = tuple(rows)
        if len(rows) != count or _digest(rows) != digest or charged != charge:
            raise Invalid('incomplete population')
        obj = object.__new__(View)
        for k, v in dict(namespace=namespace, epoch=epoch, expires=expires, limits=limits, rows=rows, _guard=guard, charged_bytes=charge).items():
            object.__setattr__(obj, k, v)
        return obj
    except BaseException as e:
        primary = e
        raise
    finally:
        try:
            cleanup = int(time.time()*1000)+1000
            if _exchange(client, 13, namespace, read_id, lp(token), cleanup):
                raise Invalid('release tail')
        except BaseException as e:
            if primary is None:
                if isinstance(e, Invalid):
                    client.close()
                raise
            primary.add_note(f'capture release failed: {e}')
        if isinstance(primary, Invalid):
            client.close()


class Publication:
    """Original and endpoint custody. Every uncertainty is retained after refusals/success."""
    def __init__(self, client, original):
        self.original = original
        self.endpoint = (client.transport, client.address)
        self.uncertainties = []

    def persist_new(self, path):
        path = Path(path)
        if not path.is_absolute() or not path.parent.is_dir():
            raise Invalid('explicit existing custody parent required')
        raw = b'MPC2'+lp(self.endpoint[0])+lp(self.endpoint[1])+lp(self.original.raw)
        with path.open('xb') as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        if os.name == 'nt':
            # Same owned directory barrier as the Rust custody writer.
            import ctypes
            from ctypes import wintypes as w
            k = ctypes.WinDLL('kernel32', use_last_error=True)
            k.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, w.LPVOID, w.DWORD, w.DWORD, w.HANDLE]
            k.CreateFileW.restype = w.HANDLE
            k.FlushFileBuffers.argtypes = [w.HANDLE]
            k.FlushFileBuffers.restype = w.BOOL
            k.CloseHandle.argtypes = [w.HANDLE]
            handle = k.CreateFileW(str(path.parent), 0x40000000, 7, None, 3, 0x02000000, None)
            if handle == w.HANDLE(-1).value:
                raise ctypes.WinError(ctypes.get_last_error())
            try:
                if not k.FlushFileBuffers(handle):
                    raise ctypes.WinError(ctypes.get_last_error())
            finally:
                k.CloseHandle(handle)
        else:
            fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    @classmethod
    def restore(cls, client, path):
        path = Path(path)
        if not path.is_absolute():
            raise Invalid('explicit custody path required')
        with path.open('rb') as f:
            raw = f.read(MAX_FRAME//3 + 8193)
        if len(raw) > MAX_FRAME//3+8192:
            raise Invalid('custody size')
        r = _Reader(raw)
        if r.take(4) != b'MPC2':
            raise Invalid('custody version')
        endpoint = (r.blob(1024).decode(), r.blob(4096).decode())
        original = Original.from_bytes(r.blob(MAX_FRAME//3))
        r.end()
        if endpoint != (client.transport, client.address):
            raise Invalid('custody endpoint mismatch')
        obj = cls(client, original)
        obj.uncertainties.append('restored original may have been sent before caller restart')
        return obj

    def publish(self, client, deadline):
        if self.endpoint != (client.transport, client.address):
            raise Invalid('original endpoint mismatch')
        o = self.original
        # Perform all local admission before entering the possibly-sent region.
        suffix = lp(o.raw)
        _identifier(o.namespace)
        _identifier(o.operation_id)
        if type(deadline) is not int or not 0 < deadline < 2**64:
            raise Invalid('explicit Unix millisecond deadline required')
        if client.channel is None or int(time.time()*1000) >= deadline:
            raise Refused('Unavailable' if client.channel is None else 'Expired')
        if 29+len(o.namespace.encode())+len(o.operation_id.encode())+len(suffix) > client.max_frame:
            raise Refused('Resource', 4)
        try:
            raw = _reply(_exchange(client, 29, o.namespace, o.operation_id, suffix, deadline))
            r = _Reader(raw)
            version, ack = r.unpack('<QB')
            r.end()
            if not version or ack != o.ack:
                raise Invalid('achieved acknowledgement')
            return dict(version=version, ack=Ack(ack), epoch=client.epoch)
        except Refused as e:
            if e.status not in ('Uncertain', 'RestartUncertain'):
                raise
            self.uncertainties.append(e)
            client.close()
            raise Uncertain(o, e) from e
        except Refusal as e:
            if e.code not in (16, 17):
                raise
            self.uncertainties.append(e)
            client.close()
            raise Uncertain(o, e) from e
        except BaseException as e:
            self.uncertainties.append(e)
            client.close()
            raise Uncertain(o, e) from e
