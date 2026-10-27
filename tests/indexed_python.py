"""Authored indexed SDK controls. Private IPC endpoint is supplied by Rust harness."""
import concurrent.futures
import os
import pathlib
import struct
import sys
import threading
import unittest
import uuid
from dataclasses import replace
from unittest.mock import patch

sys.path.insert(0, os.environ.get('MEMBUS_PYTHON_SDK_ROOT', str(pathlib.Path(__file__).resolve().parents[1] / 'python')))
from membus import Client, Refusal, EpochChanged, NotAvailable, MAX_FRAME, MAX_VALUE, CODES, lp


def no_retry_control():
    class Channel:
        def close(self):
            pass

    class Lost(Client):
        def __init__(self):
            self.timeout, self.channel, self.calls, self.reconnects = 10, Channel(), 0, 0

        def exchange(self, *args):
            self.calls += 1
            raise ConnectionError('authored lost reply')

        def connect(self, *args):
            self.reconnects += 1
            self.channel = Channel()

    c = Lost()
    try:
        c.put('fixture', 'key', b'value', op_id='original', retry_transport=False)
    except ConnectionError:
        pass
    assert c.calls == 1 and c.reconnects == 0 and c.channel is None
    c = Lost()
    try:
        c.put('fixture', 'key', b'value', op_id='legacy')
    except ConnectionError:
        pass
    assert c.calls == 2 and c.reconnects == 1
    class LegacyOverride(Client):
        def __init__(self):
            pass

        def call(self, opcode, ns, suffix, op_id, deadline):
            return struct.pack('<QB', 7, 1)
    assert LegacyOverride().put('fixture', 'key', b'value') == {'version': 7, 'ack': 1}
    print('PASS: opt-out sends once; legacy default still attempts twice', flush=True)


if '--no-retry-control' in sys.argv:
    no_retry_control()
    sys.exit(0)

from membus_indexed import (Collection, Schema, Record, All, Ordinals, Equal, Limits,
                            Ack, Deadline, Invalid, Unsupported, Conflict, Corrupt,
                            Limit, RetryWindowExpired, Uncertain)

SCHEMA = Schema(('id', 'reference', 'revises'))


class MutationVisits(dict):
    """Observe actual custody traversal, independently of elapsed runtime."""
    visits = 0

    def values(self):
        for value in super().values():
            self.visits += 1
            yield value


class PreparationControls(unittest.TestCase):
    def metadata(self):
        # Preparation consumes negotiated metadata only; any I/O is a test error.
        from types import SimpleNamespace
        return SimpleNamespace(max_value=MAX_VALUE, max_frame=MAX_FRAME, retry_ms=0,
                               epoch=7, address='authored', transport='pipe')

    def test_zero_retry_declaration_refuses_during_pure_preparation(self):
        with self.assertRaises(Unsupported):
            Collection('zero', SCHEMA).prepare_declaration(self.metadata(), 'fixture', 'original', Ack.OS)

    def test_zero_retry_append_refuses_during_pure_preparation(self):
        with self.assertRaises(Unsupported):
            Collection('zero', SCHEMA).prepare_append(self.metadata(), 'fixture', row(1), 'original', Ack.OS)

    def test_documented_helper_closes_owned_handle_on_preparation_refusal(self):
        root = pathlib.Path(os.environ['MEMBUS_PYTHON_SDK_ROOT']).parent if 'MEMBUS_PYTHON_SDK_ROOT' in os.environ else pathlib.Path(__file__).resolve().parents[1]
        source = (root / 'docs/PYTHON_INDEXED.md').read_text()
        example = source.split('```python\n', 1)[1].split('```', 1)[0]
        handles = []
        class Handle:
            def __init__(inner, *args, **kwargs):
                inner.__dict__.update(self.metadata().__dict__)
                inner.retry_ms = 60000
                inner.closed = 0
                handles.append(inner)

            def close(inner):
                inner.closed += 1
        scope = {}
        with patch('membus.Client', Handle):
            exec(compile(example, '<documented-helper>', 'exec'), scope)
        for payload, expected in ((b'x' * MAX_VALUE, Unsupported), (bytearray(b'x'), Invalid)):
            with self.subTest(expected=expected.__name__):
                with self.assertRaises(expected):
                    scope['publish_and_read']('authored', 'fixture', 'original', payload)
                self.assertEqual(handles[-1].closed, 1)
        with patch.object(Collection, 'append', side_effect=Conflict('authored post-preparation refusal')):
            with self.assertRaises(Conflict):
                scope['publish_and_read']('authored', 'fixture', 'original', b'valid')
        self.assertEqual(handles[-1].closed, 1)


def row(n, ident=None, reference=b'repeated', payload=None):
    return Record(n, ((ident or str(n).encode(),), (reference, reference), (b'prior', b'prior')),
                  payload if payload is not None else b'{"ordinal":' + str(n).encode() + b',"n":9007199254740993}')


class SnapshotContractControls(unittest.TestCase):
    def test_snapshot_requirement_wire_bytes_and_strict_empty_reply(self):
        class Reply(Client):
            def __init__(inner):
                inner.calls, inner.reply = [], b''

            def call(inner, *args, **kwargs):
                inner.calls.append((args, kwargs))
                return inner.reply
        c, token = Reply(), bytes(range(16))
        options = dict(op_id='original', deadline=123, retry_transport=False)
        self.assertIsNone(c.snapshot_require_complete('authored', token, **options))
        self.assertEqual(c.calls, [((31, 'authored', lp(token)), options)])
        c.reply = b'unexpected'
        with self.assertRaises(ValueError):
            c.snapshot_require_complete('authored', token, **options)
        for invalid in (b'', bytes(15), bytes(17), bytearray(token), 'invalid'):
            before = len(c.calls)
            with self.assertRaises(ValueError):
                c.snapshot_require_complete('authored', invalid)
            self.assertEqual(len(c.calls), before)

    def test_unsupported_snapshot_contract_is_unavailable_not_eligible(self):
        class UnsupportedServer(Client):
            def __init__(self):
                pass

            def call(self, *args, **kwargs):
                raise Refusal(2)
        with self.assertRaises(NotAvailable):
            UnsupportedServer().snapshot_require_complete('authored', bytes(16))

    def test_known_incomplete_and_token_refusals_keep_exact_codes(self):
        class RefusingServer(Client):
            def __init__(self, code):
                self.code = code

            def call(self, *args, **kwargs):
                raise Refusal(self.code)
        for code in (16, 20, 21):
            with self.subTest(code=code):
                with self.assertRaises(Refusal) as caught:
                    RefusingServer(code).snapshot_require_complete('authored', bytes(16))
                self.assertEqual(caught.exception.code, code)


class LocalControls(unittest.TestCase):
    def test_no_retry_is_explicit_and_legacy_default_preserved(self):
        no_retry_control()

    def test_canonical_independent_schema_record_crc_and_immutable_inputs(self):
        values = [[b'abc', b'abc']]
        r = Record(7, values, b'\x00opaque\xff')
        values[0].append(b'changed')
        c = Collection('facts', Schema(['tag']))
        self.assertEqual(c._declaration, b'MBIX1S\x01\x00\x03\x00\x00\x00tag')
        expected = (b'MBIX1R\x07\x00\x00\x00\x00\x00\x00\x00\x01\x00\x02\x00' +
                    b'\x03\x00\x00\x00abc\x03\x00\x00\x00abc\x08\x00\x00\x00\x00opaque\xff')
        self.assertEqual(c._encode(r), expected)
        self.assertEqual(c.encoded_len(r), len(expected))
        self.assertEqual(c._decode(expected), r)

    def test_invalid_schema_and_ordinal_and_field_bounds(self):
        for name, schema in [('', SCHEMA), ('a/b', SCHEMA), ('x', Schema(('a', 'a'))),
                             ('x', Schema(('a',) * 65)), ('x', Schema(('é' * 513,)))]:
            with self.assertRaises(Invalid):
                Collection(name, schema)
        c = Collection('facts', SCHEMA)
        for r in [row(0), row(2**64 - 1), Record(1, (), b''),
                  Record(1, ((b'x',) * 1025, (), ()), b'')]:
            with self.assertRaises(Invalid):
                c.encoded_len(r)

    def test_large_representation_refuses_without_truncation(self):
        c = Collection('facts', Schema(()))
        r = Record(1, (), b'x' * (MAX_VALUE - 20))
        self.assertEqual(c.encoded_len(r), MAX_VALUE)
        with self.assertRaises(Unsupported):
            c.encoded_len(Record(1, (), r.payload + b'x'))

    def test_corrupt_record_bytes_never_become_empty(self):
        c = Collection('facts', SCHEMA)
        body = c._encode(row(1))
        for bad in (b'', body[:-1], body + b'x', b'WRONG!' + body[6:]):
            with self.assertRaises(Corrupt):
                c._decode(bad)

    def test_invalid_explicit_deadlines(self):
        for seconds in (0, -1, float('inf'), float('nan')):
            with self.assertRaises(Invalid):
                Deadline.after(seconds)


class PrivateIPC(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pipe = os.environ['MEMBUS_PYTHON_INDEXED_PIPE']
        cls.namespace = os.environ['MEMBUS_PYTHON_INDEXED_NAMESPACE']
        if not cls.pipe.startswith('\\\\.\\pipe\\') or not cls.namespace:
            raise RuntimeError('explicit private pipe/namespace required')

    def setUp(self):
        self.c = self.client()
        self.col = Collection('case_' + uuid.uuid4().hex, SCHEMA)
        self.addCleanup(self.c.close)

    def client(self, kind=Client):
        return kind(self.pipe, transport='pipe', timeout=30)

    def prepare(self, r, op=None, ack=Ack.OS, client=None, col=None):
        return (col or self.col).prepare_append(client or self.c, self.namespace, r, op or uuid.uuid4().hex, ack)

    def append(self, r, **kw):
        p = self.prepare(r, **kw)
        return self.col.append(self.c, p, Deadline.after(30))

    def declare(self, ack=Ack.OS):
        p = self.col.prepare_declaration(self.c, self.namespace, uuid.uuid4().hex, ack)
        self.col.declare(self.c, p, Deadline.after(30))

    def query(self, q=All(), limits=Limits(1000, 8 * MAX_VALUE), token=None):
        if token is not None:
            return self.col.query(self.c, self.namespace, token, q, limits, uuid.uuid4().hex, Deadline.after(30))
        with self.c.snapshot(self.namespace) as token:
            return self.query(q, limits, token)

    def test_declare_open_empty_and_schema_conflict(self):
        self.declare()
        self.assertEqual(self.query(limits=Limits(1, 0)).records, ())
        other = Collection(self.col._name, Schema(('different',)))
        p = other.prepare_declaration(self.c, self.namespace, uuid.uuid4().hex, Ack.OS)
        with self.assertRaises(Conflict):
            other.declare(self.c, p, Deadline.after(30))

    def test_missing_declaration_is_corrupt_unavailable(self):
        with self.assertRaises(Corrupt):
            self.query()

    def test_exact_metadata_repeated_references_ids_order_revision_links(self):
        records = (row(1, b'id-a', payload=b'{"time":7,"request_id":"same","unknown":null}'),
                   row(2, b'id-b', payload=b'{"time":7,"request_id":"same","revises":"id-a"}'))
        for r in records:
            self.append(r)
        self.assertEqual(self.query().records, records)
        self.assertEqual(self.query(Equal('reference', b'repeated')).records, records)
        self.assertEqual(self.query(Ordinals(2, 2)).records, records[1:])

    def test_snapshot_pinned_across_more_than_one_page_and_current_get(self):
        records = tuple(row(n) for n in range(1, 131))
        for r in records:
            self.append(r)
        with self.c.snapshot(self.namespace) as token:
            self.append(row(131))
            current_head = self.c.get(self.namespace, self.col._head_key)['value']
            self.assertEqual(struct.unpack('<Q', current_head[6:])[0], 131)
            result = self.query(token=token)
            self.assertEqual((result.through, result.records), (130, records))
            self.assertEqual(self.query(Equal('reference', b'repeated'), token=token).records, records)
        self.assertEqual(self.query().through, 131)

    def test_independent_operation_identity_is_separate_from_repeated_reference(self):
        p = self.prepare(row(1), op='independent_' + uuid.uuid4().hex)
        self.col.append(self.c, p, Deadline.after(30))
        self.assertTrue(self.col.append(self.c, p, Deadline.after(30)).already_committed)
        self.append(row(2), op=uuid.uuid4().hex)
        self.assertEqual(len(self.query(Equal('reference', b'repeated')).records), 2)

    def test_competing_publishers_exactly_one_record_and_explicit_conflict(self):
        self.declare()
        barrier = threading.Barrier(2)
        def publish(ident):
            c = self.client()
            try:
                p = self.prepare(row(1, ident), client=c)
                barrier.wait(timeout=10)
                try:
                    self.col.append(c, p, Deadline.after(30))
                    return ident, 'committed'
                except Conflict:
                    return ident, 'conflict'
                except Refusal as e:
                    return ident, e.code
            finally:
                c.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(publish, (b'left', b'right')))
        committed = [ident for ident, status in outcomes if status == 'committed']
        self.assertEqual(len(committed), 1, outcomes)
        self.assertTrue(all(status in ('committed', 'conflict', 19) for _, status in outcomes), outcomes)
        self.assertEqual(self.query().records, (row(1, committed[0]),))

    def test_flush_is_achieved_and_never_silently_downgraded(self):
        p = self.prepare(row(1), ack=Ack.FLUSH)
        self.col.append(self.c, p, Deadline.after(30))
        value = self.c.get(self.namespace, self.col._row_key(1))
        self.assertEqual(value['ack'], 2)
        with self.assertRaises(Conflict):
            self.append(row(1), ack=Ack.OS)

    def test_large_bound_and_query_limits_preserve_exact_bytes(self):
        r = row(1, payload=b'x' * (MAX_VALUE - 128))
        self.append(r)
        self.assertEqual(self.query(limits=Limits(1, MAX_VALUE)).records, (r,))
        with self.assertRaises(Limit):
            self.query(limits=Limits(1, self.col.encoded_len(r) - 1))
        before = self.c.request_id
        with self.assertRaises(Unsupported):
            self.prepare(row(2, payload=b'x' * MAX_VALUE))
        self.assertEqual(self.c.request_id, before)
        self.assertEqual(self.query().through, 1)

    def test_unknown_fields_ordinal_gap_and_zero_row_budget_refuse(self):
        self.declare()
        with self.assertRaises(Conflict):
            self.append(row(2))
        with self.assertRaises(Invalid):
            self.query(Equal('undeclared', b'x'))
        with self.assertRaises(Invalid):
            self.query(limits=Limits(0, 0))

    def loss_client(self, target, after=True):
        class Loss(Client):
            armed = True
            sends = 0
            def exchange(inner, opcode, payload, deadline):
                if opcode == 5:
                    from membus import Decoder
                    d = Decoder(payload)
                    d.unpack('<Q')
                    op = d.blob().decode()
                    if inner.armed and op.endswith(target):
                        inner.armed = False
                        inner.sends += 1
                        if after:
                            super(Loss, inner).exchange(opcode, payload, deadline)
                        raise ConnectionError('authored response loss at exchange boundary')
                return super(Loss, inner).exchange(opcode, payload, deadline)
        c = self.client(Loss)
        self.addCleanup(c.close)
        return c

    def test_uncertain_original_commit_preserved_on_explicit_reconnect(self):
        c = self.loss_client(':commit')
        p = self.prepare(row(1), client=c)
        with self.assertRaises(Uncertain) as caught:
            self.col.append(c, p, Deadline.after(30))
        original = caught.exception
        self.assertEqual((c.sends, original.request.value), (1, b'MBIX1H\x01\x00\x00\x00\x00\x00\x00\x00'))
        self.assertIs(original.publication, p)
        self.assertEqual(self.query().records, (row(1),))
        c.connect(int(__import__('time').time() * 1000) + 30000)
        result = self.col.append(c, p, Deadline.after(30))
        self.assertTrue(result.already_committed)
        self.assertEqual(p.uncertainties, (original,))
        self.assertEqual(c.sends, 1)

    def test_staged_original_retained_invisible_and_competitor_cannot_skip(self):
        c = self.loss_client(':row')
        p = self.prepare(row(1, b'original'), client=c)
        with self.assertRaises(Uncertain):
            self.col.append(c, p, Deadline.after(30))
        self.assertEqual(self.query().records, ())
        self.assertEqual(self.query(Equal('reference', b'repeated'), Limits(1, 0)).records, ())
        with self.assertRaises(Conflict):
            self.append(row(1, b'competitor'))
        with self.assertRaises(Conflict):
            self.append(row(2))
        c.connect(int(__import__('time').time() * 1000) + 30000)
        self.col.append(c, p, Deadline.after(30))
        self.assertEqual(self.query().records, (row(1, b'original'),))
        self.assertEqual(len(p.uncertainties), 1)

    def test_unknown_unadmitted_original_cannot_be_declared_absent_from_error(self):
        c = self.loss_client(':commit', after=False)
        p = self.prepare(row(1), client=c)
        with self.assertRaises(Uncertain):
            self.col.append(c, p, Deadline.after(30))
        self.assertEqual(self.query().records, ())
        self.assertEqual(len(p.uncertainties), 1)
        self.assertEqual(self.query(Equal('reference', b'repeated'), Limits(1, 0)).records, ())
        # The same original is explicitly resumed, never a replacement identity.
        c.connect(int(__import__('time').time() * 1000) + 30000)
        original = p.uncertainties[0]
        with patch.object(c, 'put', side_effect=Refusal(8)):
            with self.assertRaises(Refusal):
                self.col.append(c, p, Deadline.after(30))
        self.assertEqual(p.uncertainties, (original,))
        self.col.append(c, p, Deadline.after(30))
        self.assertEqual(self.query().records, (row(1),))

    def test_expired_horizon_refuses_before_any_send(self):
        p = self.prepare(row(1))
        self.col.append(self.c, p, Deadline.after(30))
        before = self.c.request_id
        with patch('membus_indexed.time.monotonic', return_value=__import__('time').monotonic() + p.retry_ms / 1000 + 1):
            with self.assertRaises(RetryWindowExpired):
                self.col.append(self.c, p, Deadline.after(30))
        self.assertEqual(self.c.request_id, before)

    def test_equal_epoch_cannot_rebind_endpoint_or_original_request_and_changed_epoch_refuses(self):
        p = self.prepare(row(1))
        before = self.c.request_id
        original = self.c.address
        self.c.address = original + '-other'
        try:
            with self.assertRaises(Invalid):
                self.col.append(self.c, p, Deadline.after(30))
        finally:
            self.c.address = original
        self.assertEqual(self.c.request_id, before)
        from dataclasses import FrozenInstanceError
        with self.assertRaises(FrozenInstanceError):
            p.operation = 'replacement'
        self.c.epoch += 1
        try:
            with self.assertRaises(EpochChanged):
                self.col.append(self.c, p, Deadline.after(30))
        finally:
            self.c.epoch -= 1
        self.assertEqual(self.c.request_id, before)

    def test_malformed_and_wrong_ack_mutation_responses_are_uncertain(self):
        for outcome in ({'version': 0, 'ack': 1}, {'version': 1, 'ack': 2}):
            p = self.prepare(row(1), col=Collection('bad_' + uuid.uuid4().hex, SCHEMA))
            col = Collection(p.collection, p.schema)
            c = self.client()
            self.addCleanup(c.close)
            with patch.object(c, 'put', return_value=outcome):
                with self.assertRaises(Uncertain) as caught:
                    col.append(c, p, Deadline.after(30))
            self.assertIs(caught.exception.publication, p)
            self.assertIsNone(c.channel)

    def test_definitive_conflict_pending_and_uncertain_refusals_stay_distinct(self):
        for code in (12, 19, 16, 17, 14, 13, 8):
            col = Collection('refusal_' + uuid.uuid4().hex, SCHEMA)
            c = self.client()
            self.addCleanup(c.close)
            p = self.prepare(row(1), client=c, col=col)
            with patch.object(c, 'put', side_effect=Refusal(code)):
                expected = Uncertain if code in (16, 17) else (Corrupt if code == 12 else Refusal)
                with self.assertRaises(expected):
                    col.append(c, p, Deadline.after(30))
            self.assertEqual(len(p.uncertainties), int(code in (16, 17)))

    def test_expired_snapshot_and_missing_committed_row_refuse(self):
        self.append(row(1))
        token = self.c.snapshot_open(self.namespace)
        self.c.snapshot_release(self.namespace, token)
        with self.assertRaises(Refusal) as caught:
            self.query(token=token)
        self.assertEqual(caught.exception.code, 20)
        value = self.c.get(self.namespace, self.col._row_key(1))
        self.c.remove(self.namespace, self.col._row_key(1), expected=value['version'])
        with self.assertRaises(Corrupt):
            self.query()

    def test_crc_collision_candidates_and_staged_postings_charge_work(self):
        self.append(row(1, reference=b'plumless'))
        self.append(row(2, reference=b'buckeroo'))
        self.assertEqual(self.query(Equal('reference', b'plumless')).records, (row(1, reference=b'plumless'),))
        with self.assertRaises(Limit):
            self.query(Equal('reference', b'plumless'), Limits(1, MAX_VALUE))
        with self.assertRaises(Limit):
            self.query(Equal('reference', b'plumless'), Limits(2, self.col.encoded_len(row(1, reference=b'plumless'))))

    def test_distinct_posting_custody_visits_grow_at_most_linearly(self):
        for count in (8, 16, 32):
            with self.subTest(postings=count):
                col = Collection('visits_' + uuid.uuid4().hex, Schema(('tag',)))
                record = Record(1, (tuple(str(n).encode() for n in range(count)),), b'exact')
                visits = MutationVisits()
                p = replace(self.prepare(record, col=col), _mutations=visits)
                result = col.append(self.c, p, Deadline.after(30))
                self.assertFalse(result.already_committed)
                self.assertEqual(len(visits), count + 4)
                print('CUSTODY', count, 'mutations', len(visits), 'visits', visits.visits, flush=True)
                self.assertLessEqual(visits.visits, 4 * (count + 4))
                with self.c.snapshot(self.namespace) as token:
                    self.assertEqual(col.query(self.c, self.namespace, token, All(), Limits(1, MAX_VALUE),
                                               uuid.uuid4().hex, Deadline.after(30)).records, (record,))

    def test_exact_oldest_send_expiry_is_not_renewed_by_later_mutations(self):
        col = Collection('oldest_' + uuid.uuid4().hex, SCHEMA)
        p = self.prepare(row(1), col=col)
        # Exercise actual PUTs at independently selected clock instants. No
        # mutation or successful publication reply is substituted.
        with patch('membus_indexed.time.monotonic', return_value=100.0):
            col._put(self.c, p, col._schema_key, col._declaration, p.operation + ':schema', 0,
                     Deadline(2**64 - 1, 100000000000.0))
        with patch('membus_indexed.time.monotonic', return_value=100.001):
            col._put(self.c, p, col._head_key, b'MBIX1H' + bytes(8), p.operation + ':head', 0,
                     Deadline(2**64 - 1, 100000000000.0))
        first = min(m.first_send for m in p._mutations.values())
        self.assertEqual(first, 100.0)
        horizon = first + p.retry_ms / 1000
        with patch('membus_indexed.time.monotonic', return_value=horizon - .0001):
            col._check(self.c, p, Deadline(2**64 - 1, 100000000000.0))
        before = self.c.request_id
        with patch('membus_indexed.time.monotonic', return_value=horizon):
            with self.assertRaises(RetryWindowExpired):
                col._get(self.c, p, col._head_key, 'expired', Deadline(2**64 - 1, 100000000000.0))
        self.assertEqual(self.c.request_id, before)

    def test_short_horizon_staged_uncertainty_survives_reconnect_and_expiry(self):
        c = self.loss_client(':row')
        negotiated = c.retry_ms
        c.retry_ms = 1
        p = self.prepare(row(1), client=c)
        c.retry_ms = negotiated
        d = Deadline(2**64 - 1, 100000000000.0)
        with patch('membus_indexed.time.monotonic', return_value=0.0):
            with self.assertRaises(Uncertain) as caught:
                self.col.append(c, p, d)
        original = caught.exception
        requests = tuple(m.request for m in p._mutations.values())
        self.assertEqual(p.retry_ms, 1)
        self.assertEqual(self.query().records, ())
        c.connect(int(__import__('time').time() * 1000) + 30000)
        before = c.request_id
        with patch('membus_indexed.time.monotonic', return_value=.001):
            with self.assertRaises(RetryWindowExpired):
                self.col.append(c, p, d)
        self.assertEqual(c.request_id, before)
        self.assertEqual(tuple(m.request for m in p._mutations.values()), requests)
        self.assertEqual(p.uncertainties, (original,))
        self.assertIs(original.publication, p)
        self.assertEqual(self.query().records, ())


def zero_retry_control():
    class ZeroRetry(unittest.TestCase):
        def check(self, kind):
            c = Client(os.environ['MEMBUS_PYTHON_INDEXED_PIPE'], transport='pipe', timeout=30)
            self.addCleanup(c.close)
            self.assertEqual(c.retry_ms, 0)
            ns = os.environ['MEMBUS_PYTHON_INDEXED_NAMESPACE']
            col = Collection('zero_' + uuid.uuid4().hex, SCHEMA)
            if kind == 'declared':
                c.put(ns, col._schema_key, col._declaration, expected=0)
                c.put(ns, col._head_key, b'MBIX1H' + bytes(8), expected=0)
            before = c.request_id
            caught = None
            try:
                if kind == 'declaration':
                    p = col.prepare_declaration(c, ns, uuid.uuid4().hex, Ack.OS)
                    col.declare(c, p, Deadline.after(30))
                else:
                    p = col.prepare_append(c, ns, row(1), uuid.uuid4().hex, Ack.OS)
                    col.append(c, p, Deadline.after(30))
            except Exception as error:
                caught = error
            after = c.request_id
            schema = c.get(ns, col._schema_key)
            stored_row = c.get(ns, col._row_key(1))
            print('ZERO', kind, 'publication_requests', after - before,
                  'error', type(caught).__name__, 'schema_present', schema is not None,
                  'row_present', stored_row is not None, flush=True)
            self.assertIsInstance(caught, Unsupported)
            self.assertEqual(after, before)
            self.assertEqual(schema is not None, kind == 'declared')
            self.assertIsNone(stored_row)

        def test_declaration(self):
            self.check('declaration')

        def test_undeclared_append(self):
            self.check('undeclared')

        def test_declared_append(self):
            self.check('declared')
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ZeroRetry))
    raise SystemExit(not result.wasSuccessful())


def integrity_control(posting_count):
    complete = posting_count == 2
    class Integrity(unittest.TestCase):
        def test_captured_population_is_complete_or_refused(self):
            c = Client(os.environ['MEMBUS_PYTHON_INDEXED_PIPE'], transport='pipe', timeout=30)
            self.addCleanup(c.close)
            col = Collection('fixture', Schema(('kind',)))
            ns = 'authored'
            expected = tuple(Record(n, ((b'match',),), bytes([n])) for n in (1, 2))
            with c.snapshot(ns) as token:
                def scan(prefix):
                    page = c.scan_prefix(ns, prefix, snapshot=token, limit=16, max_bytes=MAX_FRAME,
                                         retry_transport=False)
                    self.assertEqual(page['snapshot'], token)
                    self.assertTrue(page['done'])
                    return page['items']
                self.assertEqual(scan('x/fixture/schema')[0]['value'], col._declaration)
                self.assertEqual(scan('x/fixture/head')[0]['value'], b'MBIX1H' + struct.pack('<Q', 2))
                rows = tuple(col._decode(item['value']) for item in scan('x/fixture/r/'))
                self.assertEqual(rows, expected)
                postings = scan('x/fixture/i/')
                self.assertEqual(len(postings), posting_count)
                self.assertEqual([struct.unpack('<Q', item['value'])[0] for item in postings],
                                 [1, 2] if complete else ([2] if posting_count else []))
                for q in (Equal('kind', b'match'), All(), Ordinals(1, 2)):
                    with self.subTest(query=type(q).__name__):
                        try:
                            result = col.query(c, ns, token, q, Limits(8, MAX_VALUE), uuid.uuid4().hex, Deadline.after(30))
                        except Refusal as error:
                            print('INTEGRITY', type(q).__name__, 'refusal', error.code, 'complete', complete, flush=True)
                            self.assertFalse(complete)
                            self.assertEqual(error.code, 16)
                        else:
                            print('INTEGRITY', type(q).__name__, 'ordinals', [r.ordinal for r in result.records],
                                  'through', result.through, 'complete', complete, flush=True)
                            self.assertTrue(complete, 'known missing posting storage must refuse the whole query')
                            self.assertEqual(result.records, expected)
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Integrity))
    raise SystemExit(not result.wasSuccessful())


def old_provider_control():
    c = Client(os.environ['MEMBUS_PYTHON_INDEXED_PIPE'], transport='pipe', timeout=30)
    try:
        ns = os.environ['MEMBUS_PYTHON_INDEXED_NAMESPACE']
        with c.snapshot(ns) as token:
            before = c.request_id
            try:
                Collection('absent', SCHEMA).query(c, ns, token, Equal('id', b'x'), Limits(1, 1024),
                                                 'no-old-provider-fallback', Deadline.after(30))
            except NotAvailable:
                assert c.request_id == before + 1
                print('PASS: actual older provider unavailable before any query selection', flush=True)
            else:
                raise AssertionError('older provider cannot establish snapshot completeness')
    finally:
        c.close()


def interop(phase):
    pipe = os.environ['MEMBUS_PYTHON_INDEXED_PIPE']
    ns = os.environ['MEMBUS_PYTHON_INDEXED_NAMESPACE']
    c = Client(pipe, transport='pipe', timeout=30)
    col = Collection('cross_language', SCHEMA)
    try:
        with c.snapshot(ns) as token:
            result = col.query(c, ns, token, All(), Limits(10, MAX_VALUE), 'python-interop-read-' + phase, Deadline.after(30))
        if phase == 'write':
            assert result.records == (row(1, b'rust', payload=b'\x00rust\xff'),)
            p = col.prepare_append(c, ns, row(2, b'python', payload=b'\x00python\xff'), 'python-original', Ack.FLUSH)
            col.append(c, p, Deadline.after(30))
        else:
            assert result.records == (row(1, b'rust', payload=b'\x00rust\xff'),
                                      row(2, b'python', payload=b'\x00python\xff'),
                                      row(3, b'rust-next', payload=b'\x00last\xff'))
        print('PASS: cross-language phase', phase, flush=True)
    finally:
        c.close()


if __name__ == '__main__':
    if '--integrity-complete' in sys.argv:
        integrity_control(2)
    elif '--integrity-fault' in sys.argv:
        integrity_control(1)
    elif '--integrity-empty' in sys.argv:
        integrity_control(0)
    elif '--old-provider' in sys.argv:
        old_provider_control()
    elif '--zero-retry' in sys.argv:
        zero_retry_control()
    elif '--interop' in sys.argv:
        interop(sys.argv[-1])
    elif '--discover' in sys.argv:
        suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
        print('DISCOVERED', suite.countTestCases())
        for group in suite:
            for test in group:
                print(test.id())
    else:
        unittest.main(verbosity=2)
