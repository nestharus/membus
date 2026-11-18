"""Typed MBIX1 collections over the supported stdlib Client; no backing store.

Publication tokens are caller-owned, in-memory custody, not durable journals.
See docs/PYTHON_INDEXED.md for retained staging and recovery boundaries.
"""
from asyncio import CancelledError as AsyncCancelledError
from concurrent.futures import CancelledError
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import IntEnum
import re
import struct
import time
import zlib

from membus import Client, Decoder, EpochChanged, MAX_FRAME, MAX_VALUE, Refusal, _ExchangeEvidence, lp

MAX_ID = 1024
U64_MAX = 2**64 - 1


class Error(Exception):
    pass


class Invalid(Error):
    pass


class Conflict(Error):
    pass


class Corrupt(Error):
    pass


class Limit(Error):
    pass


class Unsupported(Invalid):
    """The requested representation is outside this bounded capability."""


class RetryWindowExpired(Error):
    pass


class Uncertain(Error):
    def __init__(self, publication, request, cause):
        self.publication, self.request, self.cause = publication, request, cause
        self.epoch = publication.epoch
        self.operation_id = request.operation_id
        self.cleanup_errors = ()
        super().__init__(f'uncertain operation {self.operation_id}: {cause}')


_cancellation_types = ContextVar('indexed_cancellation_types', default=())


@contextmanager
def cancellation_exceptions(*types: type[Exception]):
    """Register exact application cancellation classes for this calling context."""
    for cls in types:
        if (not isinstance(cls, type) or not issubclass(cls, Exception) or
                issubclass(cls, Refusal) or cls in (Exception, OSError, ValueError, Corrupt, struct.error)):
            raise TypeError('dedicated application cancellation exception class required')
    token = _cancellation_types.set(_cancellation_types.get() + types)
    try:
        yield
    finally:
        _cancellation_types.reset(token)


def _cancelled(error):
    return (isinstance(error, (KeyboardInterrupt, SystemExit, GeneratorExit, CancelledError, AsyncCancelledError)) or
            any(type(error) is cls for cls in _cancellation_types.get()))


class Ack(IntEnum):
    OS = 1
    FLUSH = 2


@dataclass(frozen=True)
class Deadline:
    unix_ms: int
    local: float

    @classmethod
    def after(cls, seconds: float):
        if not 0 < seconds < float('inf'):
            raise Invalid('positive finite deadline budget required')
        return cls(int(time.time() * 1000 + seconds * 1000), time.monotonic() + seconds)


@dataclass(frozen=True)
class Schema:
    fields: tuple[str, ...]

    def __post_init__(self):
        if not isinstance(self.fields, (tuple, list)):
            raise Invalid('ordered field list required')
        object.__setattr__(self, 'fields', tuple(self.fields))


@dataclass(frozen=True)
class Record:
    ordinal: int
    fields: tuple[tuple[bytes, ...], ...]
    payload: bytes

    def __post_init__(self):
        if not isinstance(self.fields, (tuple, list)) or any(not isinstance(vs, (tuple, list)) for vs in self.fields):
            raise Invalid('ordered field value lists required')
        if not isinstance(self.payload, bytes) or any(
                not isinstance(v, bytes) for values in self.fields for v in values):
            raise Invalid('record values and payload must be immutable bytes')
        object.__setattr__(self, 'fields', tuple(tuple(v) for v in self.fields))


@dataclass(frozen=True)
class All:
    pass


@dataclass(frozen=True)
class Ordinals:
    first: int
    last: int


@dataclass(frozen=True)
class Equal:
    field: str
    value: bytes


Query = All | Ordinals | Equal


@dataclass(frozen=True)
class Limits:
    max_rows: int
    max_bytes: int


@dataclass(frozen=True)
class AppendReceipt:
    through: int
    already_committed: bool


@dataclass(frozen=True)
class Read:
    through: int
    records: tuple[Record, ...]
    encoded_bytes_read: int


@dataclass(frozen=True)
class MutationRequest:
    operation_id: str
    namespace: str
    key: str
    value: bytes
    ack: Ack
    expected: int


@dataclass
class _Mutation:
    request: MutationRequest
    first_send: float | None = None


@dataclass
class _Custody:
    oldest_first_send: float | None = None
    retry_ms: int | None = None


@dataclass(frozen=True)
class Publication:
    """Retain this original token after refusal or uncertainty; never reset it."""
    collection: str
    schema: Schema
    namespace: str
    operation: str
    ack: Ack
    record: Record | None
    epoch: int
    retry_ms: int
    address: str
    transport: str
    _mutations: dict = field(default_factory=dict, repr=False, compare=False)
    _uncertainties: list = field(default_factory=list, repr=False, compare=False)
    _custody: _Custody = field(default_factory=_Custody, repr=False, compare=False)

    @property
    def uncertainties(self) -> tuple[Uncertain, ...]:
        return tuple(self._uncertainties)


def _integer(value, low, high, name):
    if type(value) is not int or not low <= value <= high:
        raise Invalid(name)


def _identifier(value):
    if not isinstance(value, str) or not 1 <= len(value.encode('utf8')) <= MAX_ID:
        raise Invalid('nonempty bounded UTF-8 identifier required')


def _deadline(d):
    if not isinstance(d, Deadline):
        raise Invalid('explicit Deadline required')
    _integer(d.unix_ms, 1, U64_MAX, 'absolute deadline')
    if type(d.local) not in (float, int) or not time.monotonic() < d.local < float('inf'):
        raise Invalid('local deadline elapsed or invalid')
    # Existing Client takes a Unix deadline for each transfer. Bound it by the
    # remaining monotonic budget too; retain the original server admission bound.
    return min(d.unix_ms, int(time.time() * 1000 + (d.local - time.monotonic()) * 1000))


def _value(v, max_value=MAX_VALUE):
    if v is not None and (type(v.get('version')) is not int or
            not 1 <= v['version'] <= U64_MAX or v.get('ack') not in (1, 2) or
            not isinstance(v.get('value'), bytes) or len(v['value']) > max_value):
        raise Corrupt('committed value metadata')
    return v


def _head(n):
    return b'MBIX1H' + struct.pack('<Q', n)


def _count(b):
    if len(b) != 14 or b[:6] != b'MBIX1H':
        raise Corrupt('boundary')
    n, = struct.unpack('<Q', b[6:])
    if n == U64_MAX:
        raise Corrupt('ordinal bound')
    return n


def _budget(c, remaining, limit):
    return min(c.max_frame, remaining + 42 + 2 * MAX_ID + 48 + limit * (4 + MAX_ID + 14))


class Collection:
    def __init__(self, name: str, schema: Schema):
        if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', name):
            raise Invalid('collection name')
        if not isinstance(schema, Schema) or len(schema.fields) > 64:
            raise Invalid('at most 64 indexed fields')
        for name_part in schema.fields:
            _identifier(name_part)
        if len(set(schema.fields)) != len(schema.fields):
            raise Invalid('distinct field names required')
        self._name, self._schema = name, schema
        self._declaration = b'MBIX1S' + struct.pack('<H', len(schema.fields)) + b''.join(lp(s) for s in schema.fields)
        self._schema_key = self._key('schema')
        self._head_key = self._key('head')

    @property
    def schema(self) -> Schema:
        return self._schema

    def _key(self, tail):
        return f'x/{self._name}/{tail}'

    def _row_key(self, ordinal):
        return self._key(f'r/{ordinal:020d}')

    def encoded_len(self, record: Record) -> int:
        if not isinstance(record, Record):
            raise Invalid('Record required')
        _integer(record.ordinal, 1, U64_MAX - 1, 'row ordinal')
        if len(record.fields) != len(self.schema.fields):
            raise Invalid('declared fields')
        size = 16
        for values in record.fields:
            if len(values) > 1024:
                raise Invalid('field value count')
            size += 2
            for value in values:
                size += 4 + len(value)
                if size > MAX_VALUE:
                    raise Unsupported('encoded row exceeds existing value bound; large-record publication unavailable')
        size += 4 + len(record.payload)
        if size > MAX_VALUE:
            raise Unsupported('encoded row exceeds existing value bound; large-record publication unavailable')
        return size

    def _encode(self, record):
        self.encoded_len(record)
        return (b'MBIX1R' + struct.pack('<QH', record.ordinal, len(record.fields)) +
                b''.join(struct.pack('<H', len(vs)) + b''.join(lp(v) for v in vs) for vs in record.fields) + lp(record.payload))

    def _decode(self, body):
        try:
            d = Decoder(body)
            if d.read(6) != b'MBIX1R':
                raise Corrupt('row header')
            ordinal, count = d.unpack('<QH')
            if not 1 <= ordinal < U64_MAX or count != len(self.schema.fields):
                raise Corrupt('row schema/ordinal')
            fields = []
            for _ in range(count):
                n, = d.unpack('<H')
                if n > 1024:
                    raise Corrupt('field count')
                fields.append(tuple(d.blob() for _ in range(n)))
            row = Record(ordinal, tuple(fields), d.blob())
            if d.pos != len(body) or self.encoded_len(row) != len(body):
                raise Corrupt('row length')
            return row
        except (ValueError, struct.error, Invalid) as e:
            raise Corrupt('indexed encoding') from e

    def _prepare(self, c, namespace, operation, ack, record):
        _identifier(namespace)
        _identifier(operation)
        if not isinstance(ack, Ack):
            raise Invalid('explicit Ack.OS or Ack.FLUSH required')
        _integer(c.retry_ms, 0, U64_MAX, 'negotiated retry horizon')
        self._preflight(c, namespace, operation, record)
        if c.retry_ms == 0:
            raise Unsupported('indexed publication requires a positive retry horizon; no effects prepared')
        return Publication(self._name, self.schema, namespace, operation, ack, record, c.epoch, c.retry_ms,
                           c.address, c.transport)

    def _preflight(self, c, namespace, operation, record):
        if not 1024 <= c.max_frame <= MAX_FRAME or not 1 <= c.max_value <= MAX_VALUE:
            raise Invalid('negotiated frame/value limits')
        head_size = len(_head(0))

        def immutable(key, size, op, label):
            _identifier(op + ':read')
            _identifier(op + ':reconcile')
            self._admit_put(c, namespace, key, size, op, label)
        base = operation + ':declare' if record is not None else operation
        immutable(self._schema_key, len(self._declaration), base + ':schema', 'declaration')
        self._admit_put(c, namespace, self._head_key, head_size, base + ':head-init', 'head')
        _identifier(base + ':head-read')
        _identifier(base + ':head-reconcile')
        if record is not None:
            size = self.encoded_len(record)
            immutable(self._row_key(record.ordinal), size, operation + ':row', 'record')
            self._admit_put(c, namespace, self._head_key, head_size, operation + ':commit', 'head')
            _identifier(operation + ':boundary')
            _identifier(operation + ':existing')
            _identifier(operation + ':commit-reconcile')
            for index, values in enumerate(record.fields):
                if values:
                    # Every CRC bucket has the same eight-digit spelling width.
                    key = self._key(f'i/{index:02x}/00000000/{record.ordinal:020d}')
                    immutable(key, 8, f'{operation}:i{index}-00000000', 'posting')

    @staticmethod
    def _admit_put(c, namespace, key, size, op, label):
        for text in (op, key, namespace):
            _identifier(text)
        if size > c.max_value:
            raise Unsupported(f'{label} exceeds negotiated value bound')
        body = 13 + 8 + sum(4 + len(s.encode('utf8')) for s in (op, key, namespace)) + 1 + 8 + 4 + size
        if body > c.max_frame:
            raise Unsupported(f'{label} exceeds negotiated mutation frame bound')

    def prepare_declaration(self, c: Client, namespace: str, operation: str, ack: Ack) -> Publication:
        return self._prepare(c, namespace, operation, ack, None)

    def prepare_append(self, c: Client, namespace: str, record: Record, operation: str, ack: Ack) -> Publication:
        return self._prepare(c, namespace, operation, ack, record)

    def _check(self, c, p, d):
        if not isinstance(p, Publication) or (p.collection, p.schema) != (self._name, self.schema):
            raise Invalid('publication belongs to a different collection')
        _deadline(d)
        if c.channel is None:
            raise Invalid('explicit connected Client required; reconnect is caller-owned')
        if (c.address, c.transport) != (p.address, p.transport):
            raise Invalid('publication endpoint changed; equal epoch does not authorize another history')
        if c.epoch != p.epoch:
            raise EpochChanged(p.epoch, c.epoch)
        _integer(c.retry_ms, 0, U64_MAX, 'negotiated retry horizon')
        if c.retry_ms == 0:
            raise Unsupported('indexed publication requires a positive executing retry horizon')
        custody = p._custody
        previous = p.retry_ms if custody.retry_ms is None else custody.retry_ms
        custody.retry_ms = min(previous, c.retry_ms)
        first = custody.oldest_first_send
        if first is not None and time.monotonic() >= first + custody.retry_ms / 1000:
            raise RetryWindowExpired('publication retry horizon elapsed; reconcile original custody')

    def _get(self, c, p, key, op, d):
        self._check(c, p, d)
        return _value(c.get(p.namespace, key, op_id=op, deadline=_deadline(d), retry_transport=False), c.max_value)

    def _put(self, c, p, key, body, op, expected, d):
        self._check(c, p, d)
        request = MutationRequest(op, p.namespace, key, body, p.ack, expected)
        m = p._mutations.setdefault(op, _Mutation(request))
        if m.request != request:
            raise Conflict('original mutation identity cannot be rebound')
        self._admit_put(c, p.namespace, key, len(body), op, 'mutation')
        deadline = _deadline(d)
        channel = c.channel
        evidence = _ExchangeEvidence(channel)
        if m.first_send is None:
            m.first_send = time.monotonic()
            first = p._custody.oldest_first_send
            p._custody.oldest_first_send = m.first_send if first is None else min(first, m.first_send)
        try:
            with c._defer_error_close():
                result = c.put(p.namespace, key, body, ack=p.ack, expected=expected,
                               op_id=op, deadline=deadline, retry_transport=False, _evidence=evidence)
            if (not isinstance(result, dict) or type(result.get('version')) is not int or
                    not 1 <= result['version'] <= U64_MAX or result.get('ack') != p.ack):
                raise Corrupt('mutation achieved acknowledgment/revision')
            return result
        except BaseException as cause:
            if cause is evidence.refusal and c.channel is channel and cause.code not in (16, 17):
                raise
            error = Uncertain(p, m.request, cause)
            p._uncertainties.append(error)
            interrupted = (_cancelled(cause) or
                           not isinstance(cause, (Refusal, OSError, ValueError, struct.error, Corrupt)))
            if c.channel is channel:
                c.channel = None
            # A physical close can fail. Give an ordinary I/O failure one
            # further close on the detached channel, without delaying cancellation.
            for _ in range(1 if interrupted else 2):
                try:
                    channel.close()
                except BaseException as cleanup:
                    error.cleanup_errors += (cleanup,)
                    if not interrupted and _cancelled(cleanup):
                        raise
                    if not isinstance(cleanup, OSError):
                        break
                else:
                    break
            if interrupted:
                raise
            raise error from cause

    def _immutable(self, c, p, key, body, op, d):
        v = self._get(c, p, key, op + ':read', d)
        if v is not None:
            if v['value'] != body or v['ack'] != p.ack:
                raise Conflict('immutable bytes/ack differ')
            return
        try:
            self._put(c, p, key, body, op, 0, d)
        except Refusal as e:
            if e.code != 12:
                raise
            v = self._get(c, p, key, op + ':reconcile', d)
            if v is None:
                raise Corrupt('immutable conflict absent')
            if v['value'] != body or v['ack'] != p.ack:
                raise Conflict('immutable collision')

    def _declare(self, c, p, op, d):
        self._immutable(c, p, self._schema_key, self._declaration, op + ':schema', d)
        value = self._get(c, p, self._head_key, op + ':head-read', d)
        if value is not None:
            _count(value['value'])
            return
        try:
            self._put(c, p, self._head_key, _head(0), op + ':head-init', 0, d)
        except Refusal as e:
            if e.code != 12:
                raise
            value = self._get(c, p, self._head_key, op + ':head-reconcile', d)
            if value is None:
                raise Corrupt('lost head')
            _count(value['value'])

    def declare(self, c: Client, publication: Publication, deadline: Deadline) -> None:
        self._check(c, publication, deadline)
        if publication.record is not None:
            raise Invalid('declaration token required')
        self._preflight(c, publication.namespace, publication.operation, None)
        self._declare(c, publication, publication.operation, deadline)

    def append(self, c: Client, publication: Publication, deadline: Deadline) -> AppendReceipt:
        p, d = publication, deadline
        self._check(c, p, d)
        row = p.record
        if row is None:
            raise Invalid('append token required')
        self._preflight(c, p.namespace, p.operation, row)
        body = self._encode(row)
        self._declare(c, p, p.operation + ':declare', d)
        h = self._get(c, p, self._head_key, p.operation + ':boundary', d)
        if h is None:
            raise Corrupt('missing head')
        through = _count(h['value'])
        if row.ordinal <= through:
            v = self._get(c, p, self._row_key(row.ordinal), p.operation + ':existing', d)
            if v is None:
                raise Corrupt('committed row missing')
            if v['value'] != body or v['ack'] != p.ack:
                raise Conflict('committed row differs')
            return AppendReceipt(through, True)
        if through + 1 != row.ordinal:
            raise Conflict('ordinal gap')
        self._immutable(c, p, self._row_key(row.ordinal), body, p.operation + ':row', d)
        for index, values in enumerate(row.fields):
            for bucket in sorted({zlib.crc32(value) for value in values}):
                key = self._key(f'i/{index:02x}/{bucket:08x}/{row.ordinal:020d}')
                self._immutable(c, p, key, struct.pack('<Q', row.ordinal), f'{p.operation}:i{index}-{bucket:08x}', d)
        try:
            self._put(c, p, self._head_key, _head(row.ordinal), p.operation + ':commit', h['version'], d)
            return AppendReceipt(row.ordinal, False)
        except Refusal as e:
            if e.code != 12:
                raise
            current = self._get(c, p, self._head_key, p.operation + ':commit-reconcile', d)
            if current is None:
                raise Corrupt('head missing after conflict')
            n = _count(current['value'])
            if n < row.ordinal:
                raise Conflict('boundary changed')
            return AppendReceipt(n, True)

    def _page(self, c, ns, snapshot, cursor, limit, budget, op, d, *, prefix=None, lower=None, upper=None):
        if c.channel is None:
            raise Invalid('explicit connected Client required')
        options = dict(snapshot=snapshot, cursor=cursor, limit=limit, max_bytes=budget,
                       op_id=op, deadline=_deadline(d), retry_transport=False)
        if prefix is not None:
            page = c.scan_prefix(ns, prefix, **options)
        else:
            page = c.scan_range(ns, lower, upper, **options)
        items = page['items']
        if (page['snapshot'] != snapshot or len(page['cursor']) > 2 * MAX_ID + 48 or
                len(items) > limit or type(page['done']) is not bool or
                page['done'] == bool(page['cursor']) or
                (not page['done'] and (not items or page['cursor'] == cursor))):
            raise Corrupt('scan token/progress')
        previous = None
        size = 42 + len(page['cursor'])
        for item in items:
            _value(item, c.max_value)
            key = item['key']
            _identifier(key)
            if ((previous is not None and key <= previous) or
                    (prefix is not None and not key.startswith(prefix)) or
                    (prefix is None and not lower <= key < upper)):
                raise Corrupt('scan order/bounds')
            previous = key
            size += 18 + len(key.encode('utf8')) + len(item['value'])
        if size > budget:
            raise Corrupt('scan acquisition budget')
        return page

    def _exact(self, c, ns, snapshot, key, remaining, op, d):
        budget = _budget(c, remaining, 1)
        try:
            p = self._page(c, ns, snapshot, b'', 1, budget, op, d, prefix=key)
        except Refusal as e:
            if e.code == 4 and budget < c.max_frame:
                raise Limit('encoded query budget') from e
            raise
        if not p['done'] or len(p['items']) != 1 or p['items'][0]['key'] != key:
            raise Corrupt('required snapshot key absent/ambiguous')
        return p['items'][0]['value']

    def query(self, c: Client, namespace: str, snapshot: bytes, query: Query,
              limits: Limits, operation: str, deadline: Deadline) -> Read:
        _identifier(namespace)
        _identifier(operation + ':integrity')
        if not isinstance(snapshot, bytes) or len(snapshot) != 16:
            raise Invalid('explicit pinned snapshot required')
        if not isinstance(limits, Limits):
            raise Invalid('explicit Limits required')
        _integer(limits.max_rows, 1, U64_MAX, 'positive row budget')
        _integer(limits.max_bytes, 0, U64_MAX, 'nonnegative encoded-byte budget')
        if not 1 <= c.max_batch <= 128 or not 42 <= c.max_frame:
            raise Invalid('negotiated scan limits')
        ns, op, d = namespace, operation, deadline
        if c.channel is None:
            raise Invalid('explicit connected Client required')
        c.snapshot_require_complete(ns, snapshot, op_id=op + ':integrity',
                                    deadline=_deadline(d), retry_transport=False)
        if self._exact(c, ns, snapshot, self._schema_key, U64_MAX, op + ':schema', d) != self._declaration:
            raise Conflict('schema differs')
        through = _count(self._exact(c, ns, snapshot, self._head_key, U64_MAX, op + ':head', d))
        rows, charged = [], 0
        if isinstance(query, (All, Ordinals)):
            first, last = 1, through
            if isinstance(query, Ordinals):
                _integer(query.first, 1, U64_MAX, 'first ordinal')
                _integer(query.last, query.first, U64_MAX, 'last ordinal')
                first, last = query.first, min(query.last, through)
            if first <= last:
                if last - first >= limits.max_rows:
                    raise Limit('row budget')
                cursor, expected = b'', first
                page_limit = min(last - first + 1, c.max_batch)
                previous = None
                while True:
                    if expected <= last and last - expected + 1 > (limits.max_bytes - charged) // (20 + 2 * len(self.schema.fields)):
                        raise Limit('minimum encoded-row budget')
                    budget = _budget(c, limits.max_bytes - charged, page_limit)
                    try:
                        page = self._page(c, ns, snapshot, cursor, page_limit, budget, op + ':rows', d,
                                          lower=self._row_key(first), upper=self._row_key(last + 1))
                    except Refusal as e:
                        if e.code == 4 and budget < c.max_frame:
                            raise Limit('encoded query budget') from e
                        raise
                    for item in page['items']:
                        charged += len(item['value'])
                        if charged > limits.max_bytes:
                            raise Limit('encoded query budget')
                        row = self._decode(item['value'])
                        if row.ordinal != expected or item['key'] != self._row_key(expected) or (previous is not None and item['key'] <= previous):
                            raise Corrupt('ordered row gap/identity')
                        previous = item['key']
                        rows.append(row)
                        expected += 1
                    if page['done']:
                        break
                    cursor = page['cursor']
                if expected - 1 != last:
                    raise Corrupt('incomplete committed range')
        elif isinstance(query, Equal):
            if query.field not in self.schema.fields or not isinstance(query.value, bytes) or len(query.value) > MAX_VALUE:
                raise Invalid('declared field and immutable query bytes required')
            index = self.schema.fields.index(query.field)
            prefix = self._key(f'i/{index:02x}/{zlib.crc32(query.value):08x}/')
            cursor, examined, previous = b'', 0, 0
            while True:
                page = self._page(c, ns, snapshot, cursor, min(limits.max_rows - examined + 1, c.max_batch),
                                  c.max_frame, op + ':postings', d, prefix=prefix)
                for item in page['items']:
                    if len(item['value']) != 8:
                        raise Corrupt('posting')
                    n, = struct.unpack('<Q', item['value'])
                    if n == 0 or n <= previous or item['key'] != prefix + f'{n:020d}':
                        raise Corrupt('posting order/identity')
                    previous = n
                    examined += 1
                    if examined > limits.max_rows:
                        raise Limit('posting candidate budget')
                    if n > through:
                        continue
                    body = self._exact(c, ns, snapshot, self._row_key(n), limits.max_bytes - charged, op + ':value', d)
                    charged += len(body)
                    if charged > limits.max_bytes:
                        raise Limit('encoded query budget')
                    row = self._decode(body)
                    if row.ordinal != n:
                        raise Corrupt('posting row identity')
                    if query.value in row.fields[index]:
                        rows.append(row)
                if page['done']:
                    break
                cursor = page['cursor']
        else:
            raise Invalid('supported Query required')
        return Read(through, tuple(rows), charged)
