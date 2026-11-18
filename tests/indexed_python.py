"""Authored indexed SDK controls. Private IPC endpoint is supplied by Rust harness."""
import concurrent.futures
import os
import pathlib
import signal
import struct
import sys
import threading
import unittest
import uuid
from dataclasses import replace
from unittest.mock import patch

sys.path.insert(0, os.environ.get('MEMBUS_PYTHON_SDK_ROOT', str(pathlib.Path(__file__).resolve().parents[1] / 'python')))
from membus import Client, Decoder, Refusal, EpochChanged, NotAvailable, MAX_FRAME, MAX_VALUE, CODES, deadline_ms, lp


def no_retry_control():
    class Channel:
        def close(self):
            pass

    class Lost(Client):
        def __init__(self):
            self.timeout, self.channel, self.calls, self.reconnects = 10, Channel(), 0, 0
            self.transport, self.address, self.epoch, self.retry_ms = 'authored', 'no-retry-control', 7, 60000

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

    def test_declaration_preflights_head_with_only_negotiated_metadata(self):
        c = self.metadata()
        c.retry_ms = 60000
        col = Collection('small', Schema(()))
        for c.max_value, refusal in ((7, 'declaration'), (8, 'head'), (13, 'head'), (14, None), (15, None)):
            with self.subTest(max_value=c.max_value):
                if refusal:
                    with self.assertRaisesRegex(Unsupported, refusal):
                        col.prepare_declaration(c, 'fixture', 'original', Ack.OS)
                else:
                    p = col.prepare_declaration(c, 'fixture', 'original', Ack.OS)
                    self.assertEqual(p.uncertainties, ())
                    self.assertIsNone(p._custody.oldest_first_send)

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


class PublicationWireControls(unittest.TestCase):
    def setUp(self):
        from batch_refusal_python import BytePeer, response

        self.cap, self.frame, self.retry_ms = 4096, MAX_FRAME, 120000
        self.peers, self.writes, self.values = [], [], {}
        self.receipt, self.target = None, ':commit'

        def answer(packet):
            peer = self.peers[-1]
            peer.pending.clear()
            op, request_id = struct.unpack_from('<BQ', packet, 8)
            if op == 1:
                codes = [(code, entry) for code, entry in CODES.items() if code <= 19]
                hello = struct.pack('<QIIHBQH', 7, self.frame, self.cap, 128, 0, self.retry_ms, len(codes))
                hello += b''.join(struct.pack('<HB', code, transient) + lp(name)
                                  for code, (name, transient) in codes)
                return response(op, request_id, hello)
            request = Decoder(packet[17:])
            deadline, = request.unpack('<Q')
            logical, namespace, key = (request.blob().decode() for _ in range(3))
            current = self.values.get((namespace, key))
            if op == 2:
                value = b'\0' if current is None else b'\1' + struct.pack('<QB', *current[:2]) + lp(current[2])
                return response(op, request_id, value)
            self.assertEqual(op, 5)
            ack, expected = request.unpack('<BQ')
            value = request.blob()
            self.assertLessEqual(len(value), self.cap)
            self.assertLessEqual(len(packet) - 4, self.frame)
            self.writes.append((logical, namespace, key, value, ack, expected, deadline))
            if logical.endswith(self.target):
                if isinstance(self.receipt, int):
                    return response(op, request_id, b'', self.receipt)
                if self.receipt == 'lost-before':
                    return b''
            self.assertEqual(expected, current[0] if current else 0)
            revision = len(self.writes)
            self.values[namespace, key] = (revision, ack, value)
            if logical.endswith(self.target) and self.receipt == 'association':
                request_id += 1
            return response(op, request_id, struct.pack('<QB', revision, ack))

        def connect(*args):
            peer = BytePeer(answer)
            self.peers.append(peer)
            return peer

        self.enterContext(patch('membus.Pipe', side_effect=connect))
        self.client = Client('publication-wire', transport='pipe')
        self.addCleanup(self.client.close)
        self.collection = Collection('small', Schema(()))

    def test_retains_first_failure_before_cleanup_and_detaches_exact_channel(self):
        from concurrent.futures import CancelledError

        cases = [('loss', ('io',)), ('loss', ('io', 'io')), ('loss', ('io', 'cancel')),
                 ('association', ('io',)), ('unassociated', ()),
                 (16, ('interrupt',)), (17, ('cancel',)), ('cancel', ('io',)),
                 (16, ('refusal',)), (8, ()), (13, ()), (14, ()), (19, ()), (None, ())]
        for index, (receipt, cleanup) in enumerate(cases):
            with self.subTest(receipt=receipt, cleanup=cleanup):
                c = self.client
                if index:
                    c.close()
                if c.channel is None:
                    c.connect(deadline_ms())
                channel = c.channel
                self.receipt = receipt
                namespace = f'fixture-{index}'
                p = self.collection.prepare_append(c, namespace, Record(1, (), b''), 'original', Ack.FLUSH)
                deadline = Deadline.after(10)
                loss = ConnectionResetError('receipt body loss')
                cancel = CancelledError('caller cancellation')
                unassociated = Refusal(8)
                secondary = {'io': OSError('physical close'), 'interrupt': KeyboardInterrupt(),
                             'cancel': CancelledError('cleanup cancellation'), 'refusal': Refusal(12)}
                read_exact = c.read_exact

                def read(size, until):
                    if size == 24 and self.writes[-1][0] == 'original:commit':
                        if receipt == 'loss':
                            raise loss
                        if receipt == 'cancel':
                            raise cancel
                        if receipt == 'unassociated':
                            raise unassociated
                    return read_exact(size, until)

                attempts = []

                def close():
                    self.assertIsNone(c.channel)
                    error, = p.uncertainties
                    m = p._mutations['original:commit']
                    self.assertIs(error.request, m.request)
                    self.assertLess(p._custody.oldest_first_send, m.first_send)
                    self.assertEqual((error.request.expected, error.request.ack),
                                     (self.writes[-1][5], Ack.FLUSH))
                    n = len(attempts)
                    attempts.append(error)
                    if n < len(cleanup):
                        raise secondary[cleanup[n]]

                before, peers = len(self.writes), len(self.peers)
                with patch.object(c, 'read_exact', side_effect=read), patch.object(channel, 'close', side_effect=close):
                    if receipt is None:
                        self.assertEqual(self.collection.append(c, p, deadline).through, 1)
                    elif receipt in (8, 13, 14, 19):
                        with self.assertRaises(Refusal) as caught:
                            self.collection.append(c, p, deadline)
                        self.assertEqual(caught.exception.code, receipt)
                    else:
                        promoted = cancel if receipt == 'cancel' else next(
                            (secondary[k] for k in cleanup if k in ('interrupt', 'cancel')), None)
                        with self.assertRaises(type(promoted) if promoted else Uncertain) as caught:
                            self.collection.append(c, p, deadline)
                        error, = p.uncertainties
                        self.assertIs(error.publication, p)
                        self.assertIs(caught.exception, promoted or error)
                        self.assertEqual(error.cleanup_errors, tuple(secondary[k] for k in cleanup))
                        self.assertTrue(all(attempt is error for attempt in attempts))
                        self.assertEqual(len(attempts), 2 if cleanup[:1] == ('io',) and receipt != 'cancel' else 1)
                        if receipt in (16, 17):
                            self.assertEqual(error.cause.code, receipt)
                        elif receipt == 'association':
                            self.assertIsInstance(error.cause, ValueError)
                        elif receipt == 'unassociated':
                            self.assertIs(error.cause, unassociated)
                        else:
                            self.assertIs(error.cause, cancel if receipt == 'cancel' else loss)
                        if promoted is None:
                            self.assertIs(error.__cause__, error.cause)
                        self.assertIsNone(c.channel)
                self.assertEqual(len(self.peers), peers)
                self.assertEqual([w[0] for w in self.writes[before:]],
                                 ['original:declare:schema', 'original:declare:head-init', 'original:row', 'original:commit'])
                self.assertTrue(all(0 < w[-1] <= deadline.unix_ms for w in self.writes[before:]))
                if not p.uncertainties:
                    self.assertEqual(attempts, [])
                    self.assertIs(c.channel, channel)
                    self.assertIsNotNone(c.get(namespace, self.collection._head_key))

    def test_wire_refusal_evidence_stays_with_the_current_request_channel_and_error(self):
        c, col = self.client, self.collection
        self.receipt = 14
        exchange = c.exchange
        for mode in ('exact', 'different-request', 'restored-buffer', 'different-error',
                     'later-exchange', 'different-channel'):
            with self.subTest(mode=mode):
                if c.channel is None:
                    c.connect(deadline_ms())
                channel = c.channel
                p = col.prepare_append(c, mode, Record(1, (), b''), 'original', Ack.OS)
                errors = []
                replacement = object()
                buffer = original = None
                write_all = c.write_all

                def write(data, deadline):
                    write_all(data, deadline)
                    if buffer is not None:
                        buffer[:] = original

                def override(opcode, payload, deadline):
                    nonlocal buffer, original
                    request = Decoder(payload)
                    request.unpack('<Q')
                    if opcode != 5 or request.blob() != b'original:commit':
                        return exchange(opcode, payload, deadline)
                    if mode == 'different-request':
                        # Same opcode, request number and channel, different CAS bytes.
                        payload = payload[:-1] + bytes([payload[-1] ^ 1])
                    if mode == 'restored-buffer':
                        original = payload
                        buffer = bytearray(payload)
                        buffer[-1] ^= 1
                        payload = buffer
                    try:
                        return exchange(opcode, payload, deadline)
                    except Refusal as error:
                        self.assertEqual(error.code, 14)
                        if mode == 'different-error':
                            errors.append(Refusal(error.code))
                            raise errors[-1] from error
                        errors.append(error)
                        if mode == 'later-exchange':
                            get_payload = struct.pack('<Q', deadline) + lp('later') + lp(mode) + lp(col._head_key)
                            exchange(2, get_payload, deadline)
                        if mode == 'different-channel':
                            c.channel = replacement
                        raise

                before, peers = len(self.writes), len(self.peers)
                try:
                    with (patch.object(c, 'exchange', side_effect=override),
                          patch.object(c, 'write_all', side_effect=write)):
                        with self.assertRaises(Refusal if mode == 'exact' else Uncertain) as caught:
                            col.append(c, p, Deadline.after(10))
                    self.assertEqual(len(self.peers), peers)
                    self.assertEqual([w[0] for w in self.writes[before:]].count('original:commit'), 1)
                    error, = errors
                    if mode == 'exact':
                        self.assertIs(caught.exception, error)
                        self.assertEqual(p.uncertainties, ())
                        self.assertIs(c.channel, channel)
                        self.assertEqual(channel.closed, 0)
                    else:
                        uncertain, = p.uncertainties
                        self.assertIs(caught.exception, uncertain)
                        self.assertIs(uncertain.cause, error)
                        self.assertIs(uncertain.request, p._mutations['original:commit'].request)
                        self.assertIs(c.channel, replacement if mode == 'different-channel' else None)
                        self.assertEqual(channel.closed, 1)
                finally:
                    if c.channel is replacement:
                        c.channel = None

    def test_success_requires_the_serialized_request_and_decoded_receipt(self):
        c, col = self.client, self.collection
        exchange = c.exchange
        for mode in ('forwarded-buffer', 'different-request', 'substituted-reply'):
            with self.subTest(mode=mode):
                if c.channel is None:
                    c.connect(deadline_ms())
                channel = c.channel
                p = col.prepare_append(c, mode, Record(1, (), b''), 'original', Ack.OS)

                def override(opcode, payload, deadline):
                    request = Decoder(payload)
                    request.unpack('<Q')
                    if opcode != 5 or request.blob() != b'original:commit':
                        return exchange(opcode, payload, deadline)
                    data = bytearray(payload)
                    if mode == 'different-request':
                        data[-1] ^= 1
                    reply = exchange(opcode, data, deadline)
                    return bytes(bytearray(reply)) if mode == 'substituted-reply' else reply

                before = len(self.writes)
                with patch.object(c, 'exchange', side_effect=override):
                    if mode == 'forwarded-buffer':
                        self.assertEqual(col.append(c, p, Deadline.after(10)).through, 1)
                        self.assertIs(c.channel, channel)
                        self.assertEqual(p.uncertainties, ())
                    else:
                        with self.assertRaises(Uncertain) as caught:
                            col.append(c, p, Deadline.after(10))
                        self.assertEqual(p.uncertainties, (caught.exception,))
                        self.assertIsInstance(caught.exception.cause, ValueError)
                        self.assertIsNone(c.channel)
                self.assertEqual([w[0] for w in self.writes[before:]].count('original:commit'), 1)

    def test_transfer_rechecks_channel_at_each_physical_segment_and_receipt(self):
        from batch_refusal_python import BytePeer

        c, col = self.client, self.collection
        exchange, write_all, put = c.exchange, c.write_all, c.put
        cases = ('before-exchange', 'before-write', 'partial-write', 'after-write', 'partial-prefix',
                 'before-body', 'partial-body', 'after-body', 'after-exchange', 'after-put')
        for mode in cases:
            with self.subTest(mode=mode):
                c.close()
                c.connect(deadline_ms())
                channel = c.channel
                io, read_exact = channel.io, c.read_exact
                replacement = BytePeer(lambda _: b'')
                p = col.prepare_append(c, mode, Record(1, (), b''), 'original', Ack.OS)
                active = False
                segments = []

                def rotate():
                    self.assertIs(c.channel, channel)
                    c.channel = replacement

                def override(opcode, payload, deadline):
                    nonlocal active
                    request = Decoder(payload)
                    request.unpack('<Q')
                    active = opcode == 5 and request.blob() == b'original:commit'
                    if active and mode == 'before-exchange':
                        rotate()
                    result = exchange(opcode, payload, deadline)
                    if active and mode == 'after-exchange':
                        rotate()
                    return result

                def write(data, deadline):
                    if active and mode == 'before-write':
                        rotate()
                    write_all(data, deadline)
                    if active and mode == 'after-write':
                        rotate()

                def read(count, deadline):
                    if active and count == 24 and mode == 'before-body':
                        rotate()
                    result = read_exact(count, deadline)
                    if active and count == 24 and mode == 'after-body':
                        rotate()
                    return result

                def physical(data, count, deadline, write):
                    if active:
                        self.assertIs(c.channel, channel)
                        segments.append((write, count))
                    result = io(data, count, deadline, write)
                    if active and ((write and mode == 'partial-write') or
                                   (not write and (count, mode) in ((4, 'partial-prefix'), (24, 'partial-body')))):
                        rotate()
                    return result

                def mutation(*args, **kwargs):
                    result = put(*args, **kwargs)
                    if active and mode == 'after-put':
                        rotate()
                    return result

                before = len(self.writes)
                with (patch.object(c, 'exchange', side_effect=override),
                      patch.object(c, 'write_all', side_effect=write),
                      patch.object(c, 'read_exact', side_effect=read),
                      patch.object(c, 'put', side_effect=mutation),
                      patch.object(channel, 'io', side_effect=physical),
                      patch.object(replacement, 'io', side_effect=AssertionError('replacement transfer'))):
                    with self.assertRaises(Uncertain) as caught:
                        col.append(c, p, Deadline.after(10))
                self.assertEqual(p.uncertainties, (caught.exception,))
                self.assertIs(caught.exception.request, p._mutations['original:commit'].request)
                self.assertEqual([w[0] for w in self.writes[before:]].count('original:commit'),
                                 int(mode not in cases[:3]))
                reads = [count for write, count in segments if not write]
                expected_reads = {'partial-prefix': [4], 'before-body': [4, 1],
                                  'partial-body': [4, 1, 24]}
                if mode in cases[:4] or mode in expected_reads:
                    self.assertEqual(reads, expected_reads.get(mode, []))
                if mode in cases[:2]:
                    self.assertEqual(segments, [])
                if mode == 'partial-write':
                    self.assertEqual(len(segments), 1)
                self.assertEqual(channel.closed, 1)
                self.assertIs(c.channel, replacement)
                self.assertEqual(replacement.closed, 0)

    def test_reconnect_rechecks_complete_value_and_frame_before_any_request(self):
        c, col = self.client, self.collection
        for index, (append, value, frame, payload, fits) in enumerate((
                (False, 8, MAX_FRAME, b'', False), (False, 13, MAX_FRAME, b'', False),
                (False, 14, MAX_FRAME, b'', True), (True, 13, MAX_FRAME, b'', False),
                (True, 19, MAX_FRAME, b'', False), (True, 20, MAX_FRAME, b'', True),
                (True, 4096, 1024, bytes(907), True), (True, 4096, 1024, bytes(908), False))):
            with self.subTest(append=append, value=value, frame=frame, payload=len(payload)):
                namespace = f'fixture-{index}'
                c.close()
                self.cap, self.frame = 4096, MAX_FRAME
                c.connect(deadline_ms())
                if append:
                    p = col.prepare_append(c, namespace, Record(1, (), payload), 'original', Ack.FLUSH)
                else:
                    p = col.prepare_declaration(c, namespace, 'original', Ack.FLUSH)
                c.close()
                self.cap, self.frame = value, frame
                c.connect(deadline_ms())
                self.assertEqual(c.epoch, p.epoch)
                before = len(self.writes)
                execute = col.append if append else col.declare
                if fits:
                    execute(c, p, Deadline.after(10))
                    self.assertEqual(len(self.writes) - before, 4 if append else 2)
                    before = len(self.writes)
                    execute(c, p, Deadline.after(10))
                    self.assertEqual(len(self.writes), before)
                else:
                    with self.assertRaises(Unsupported):
                        execute(c, p, Deadline.after(10))
                    self.assertEqual(len(c.channel.packets), 1)
                    self.assertEqual(len(self.writes), before)
                    self.assertIsNone(p._custody.oldest_first_send)
                    self.assertFalse(any(ns == namespace for ns, _ in self.values))

    def test_reduced_successor_keeps_uncertain_original_and_first_send(self):
        c, col = self.client, self.collection
        p = col.prepare_declaration(c, 'fixture', 'original', Ack.FLUSH)
        self.target, self.receipt = ':schema', 'lost-before'
        with self.assertRaises(Uncertain) as caught:
            col.declare(c, p, Deadline.after(10))
        request = p._mutations['original:schema']
        sent = request.first_send
        self.assertIs(p.uncertainties[0].request, request.request)
        self.assertEqual(p._custody.oldest_first_send, sent)
        self.cap, self.receipt = 13, None
        c.connect(deadline_ms())
        with self.assertRaises(Unsupported):
            col.declare(c, p, Deadline.after(10))
        self.assertEqual(len(c.channel.packets), 1)
        self.assertEqual(len(self.writes), 1)
        self.assertFalse(self.values)
        self.assertEqual(p.uncertainties, (caught.exception,))
        self.assertEqual(p._custody.oldest_first_send, sent)
        self.assertEqual(request.first_send, sent)

    def test_executing_zero_and_permanent_positive_contraction(self):
        c, col = self.client, self.collection
        for append in (False, True):
            with self.subTest(append=append):
                def connect(retry_ms):
                    c.close()
                    self.retry_ms = retry_ms
                    c.connect(deadline_ms())
                    self.assertEqual(c.retry_ms, retry_ms)

                connect(120000)
                d = Deadline.after(10)
                now = __import__('time').monotonic()
                ns = f'contract-{append}'
                if append:
                    p = col.prepare_append(c, ns, Record(1, (), b''), 'original', Ack.FLUSH)
                    execute = col.append
                    self.target = ':row'
                else:
                    p = col.prepare_declaration(c, ns, 'original', Ack.FLUSH)
                    execute = col.declare
                    self.target = ':schema'

                connect(0)
                with self.assertRaises(Unsupported):
                    execute(c, p, d)
                self.assertEqual(len(c.channel.packets), 1)
                self.assertIsNone(p._custody.oldest_first_send)
                self.assertFalse(p._mutations)

                connect(100)
                self.receipt = 'lost-before'
                with patch('membus_indexed.time.monotonic', return_value=now):
                    with self.assertRaises(Uncertain) as caught:
                        execute(c, p, d)
                original = caught.exception
                m = p._mutations[original.operation_id]
                self.assertIs(m.request, original.request)
                self.assertEqual((m.first_send, p._custody.oldest_first_send), (now, now))

                self.receipt = None
                for retry_ms, elapsed, error in ((0, .001, Unsupported), (10, .010, RetryWindowExpired),
                                                 (120000, .010, RetryWindowExpired)):
                    connect(retry_ms)
                    with patch('membus_indexed.time.monotonic', return_value=now + elapsed):
                        with self.assertRaises(error):
                            execute(c, p, d)
                    self.assertEqual(len(c.channel.packets), 1)
                    self.assertEqual(p.uncertainties, (original,))
                    self.assertIs(m.request, original.request)
                    self.assertEqual((p.retry_ms, m.first_send, p._custody.oldest_first_send),
                                     (120000, now, now))

    def test_prior_commit_loss_survives_associated_quota_refusal_and_success(self):
        c, col = self.client, self.collection
        p = col.prepare_append(c, 'fixture', Record(1, (), b''), 'original', Ack.FLUSH)
        deadline = Deadline.after(10)
        self.receipt = 'lost-before'
        with self.assertRaises(Uncertain) as lost:
            col.append(c, p, deadline)
        self.assertIsNone(c.channel)
        self.assertIs(lost.exception.publication, p)

        c.connect(deadline.unix_ms)
        channel = c.channel
        self.receipt = 8
        with self.assertRaises(Refusal) as refused:
            col.append(c, p, deadline)
        self.assertEqual(refused.exception.code, 8)
        self.assertIs(c.channel, channel)
        self.assertEqual(p.uncertainties, (lost.exception,))

        self.receipt = None
        self.assertEqual(col.append(c, p, deadline).through, 1)
        self.assertEqual(p.uncertainties, (lost.exception,))
        commits = [write for write in self.writes if write[0] == 'original:commit']
        self.assertEqual(len(commits), 3)
        self.assertEqual([write[:6] for write in commits], [commits[0][:6]] * 3)

    def test_exact_current_cas_refusal_reconciles_and_keeps_channel(self):
        c, col = self.client, self.collection
        p = col.prepare_append(c, 'fixture', Record(1, (), b''), 'original', Ack.FLUSH)
        self.receipt = 12
        with self.assertRaisesRegex(Conflict, 'boundary changed'):
            col.append(c, p, Deadline.after(10))
        last = Decoder(c.channel.packets[-1][17:])
        last.unpack('<Q')
        self.assertEqual(c.channel.packets[-1][8], 2)
        self.assertEqual(last.blob(), b'original:commit-reconcile')
        self.assertEqual(p.uncertainties, ())
        self.assertEqual(c.get('fixture', col._head_key)['value'], b'MBIX1H' + bytes(8))


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
    class ExistingExchange(Client):
        def exchange(self, opcode, payload, deadline):
            return super().exchange(opcode, payload, deadline)

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

    def test_declaration_small_provider_values_refuse_before_effects(self):
        head = b'MBIX1H' + bytes(8)
        for limit in (7, 8, 13, 14, 15):
            # Emulate a smaller negotiated cap while using the real private resident for state checks.
            with self.subTest(max_value=limit), patch.object(self.c, 'max_value', limit):
                name = 'small_' + uuid.uuid4().hex
                col = Collection(name, Schema(()))
                before = self.c.request_id
                if limit < len(head):
                    with self.assertRaises(Unsupported):
                        col.prepare_declaration(self.c, self.namespace, name, Ack.OS)
                    self.assertEqual(self.c.request_id, before)
                    self.assertIsNone(self.c.get(self.namespace, col._schema_key))
                    self.assertIsNone(self.c.get(self.namespace, col._head_key))
                else:
                    p = col.prepare_declaration(self.c, self.namespace, name, Ack.OS)
                    self.assertEqual(self.c.request_id, before)
                    col.declare(self.c, p, Deadline.after(30))
                    keys = (col._schema_key, col._head_key)
                    values = tuple(self.c.get(self.namespace, key) for key in keys)
                    self.assertEqual(tuple(v['value'] for v in values), (b'MBIX1S\0\0', head))
                    noop = col.prepare_declaration(self.c, self.namespace, name + ':noop', Ack.OS)
                    with patch.object(self.c, 'put', wraps=self.c.put) as put:
                        col.declare(self.c, noop, Deadline.after(30))
                    put.assert_not_called()
                    self.assertEqual(tuple(self.c.get(self.namespace, key) for key in keys), values)

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

    def test_head_receipt_interruption_preserves_original_and_invalidates_channel(self):
        record = row(1, payload=b'A\0\xff')
        for mode in ('healthy', 'interrupted', 'close-failed'):
            with self.subTest(mode=mode):
                col = Collection('receipt_' + uuid.uuid4().hex, SCHEMA)
                c = self.client()
                self.addCleanup(c.close)
                channel = c.channel
                self.addCleanup(channel.close)
                p = self.prepare(record, client=c, col=col)
                operation = p.operation + ':commit'
                interrupt = KeyboardInterrupt('authored receipt interrupt')
                write_all, read_exact = c.write_all, c.read_exact
                writes, first_sends = [], {}

                def write(data, deadline):
                    opcode = data[8]
                    request = Decoder(data[17:])
                    request.unpack('<Q')
                    op = request.blob().decode()
                    writes.append((opcode, op))
                    if opcode == 5:
                        first_sends.setdefault(op, p._mutations[op].first_send)
                    return write_all(data, deadline)

                def read(count, deadline):
                    # The response length has arrived and the mutation receipt body is unread.
                    if mode != 'healthy' and writes[-1] == (5, operation) and count == 24:
                        raise interrupt
                    return read_exact(count, deadline)

                close_failure = OSError('authored close failure') if mode == 'close-failed' else None
                with (patch.object(c, 'write_all', side_effect=write),
                      patch.object(c, 'read_exact', side_effect=read),
                      patch.object(c, 'connect', wraps=c.connect) as connect,
                      patch.object(channel, 'close', wraps=channel.close, side_effect=close_failure) as close):
                    if mode == 'healthy':
                        receipt = col.append(c, p, Deadline.after(30))
                        self.assertEqual((receipt.through, receipt.already_committed), (1, False))
                        self.assertEqual(p.uncertainties, ())
                        self.assertIs(c.channel, channel)
                        close.assert_not_called()
                        self.assertEqual(c.get(self.namespace, col._head_key)['value'], b'MBIX1H\x01' + bytes(7))
                    else:
                        with self.assertRaises(KeyboardInterrupt) as caught:
                            col.append(c, p, Deadline.after(30))
                        self.assertIs(caught.exception, interrupt)
                        self.assertIsNone(c.channel)
                        close.assert_called_once_with()
                        self.assertEqual(writes[-1], (5, operation))
                        self.assertEqual(len(p.uncertainties), 1)
                        original, = p.uncertainties
                        self.assertIs(original.publication, p)
                        self.assertIs(original.request, p._mutations[operation].request)
                        self.assertEqual((original.operation_id, original.request.namespace, original.request.key,
                                          original.request.value, original.request.ack, original.epoch),
                                         (operation, self.namespace, col._head_key,
                                          b'MBIX1H\x01' + bytes(7), Ack.OS, c.epoch))
                        self.assertIs(original.cause, interrupt)
                    connect.assert_not_called()
                self.assertEqual(writes.count((5, operation)), 1)
                self.assertIsNotNone(first_sends[operation])
                self.assertEqual(p._mutations[operation].first_send, first_sends[operation])
                self.assertEqual(p._custody.oldest_first_send, min(first_sends.values()))
                with self.c.snapshot(self.namespace) as token:
                    for q in (All(), Ordinals(1, 1), Equal('reference', b'repeated')):
                        result = col.query(self.c, self.namespace, token, q, Limits(2, MAX_VALUE),
                                           'receipt-observer', Deadline.after(30))
                        self.assertEqual((result.through, result.records), (1, (record,)))

    def test_registered_receipt_and_close_cancellation_with_builtin_integer_format_limit(self):
        from membus_indexed import cancellation_exceptions

        class ApplicationStop(Exception):
            pass

        self.addCleanup(sys.set_int_max_str_digits, sys.get_int_max_str_digits())
        sys.set_int_max_str_digits(4300)
        huge = 10**4300
        receipt_stop, close_stop = ApplicationStop(huge), ApplicationStop(huge)
        with self.assertRaises(ValueError):
            str(receipt_stop)
        with self.assertRaises(ValueError):
            str(close_stop)
        loss = ConnectionResetError('before final receipt body')
        close_io = OSError('first close failed')
        cases = ((ApplicationStop(123), ()), (receipt_stop, ()), (receipt_stop, (close_io,)),
                 (receipt_stop, (close_stop,)), (loss, (close_stop,)), (loss, (close_io, close_stop)))
        record = Record(1, (), b'\xf7\0')
        for index, (cause, cleanup) in enumerate(cases):
            with self.subTest(case=index), cancellation_exceptions(ApplicationStop):
                col = Collection('format_' + uuid.uuid4().hex, Schema(()))
                namespace = self.namespace + '-' + str(index)
                c = self.client()
                channel, write, read = c.channel, c.write_all, c.read_exact
                physical_close = channel.close
                self.addCleanup(physical_close)
                self.addCleanup(c.close)
                p = col.prepare_append(c, namespace, record, 'original', Ack.FLUSH)
                target = (5, 'original:commit')
                writes, first_sends, closes = [], {}, []
                before_commit, observed = [], []

                def capture(data, deadline):
                    request = Decoder(data[17:])
                    request.unpack('<Q')
                    opcode, operation = data[8], request.blob().decode()
                    writes.append((opcode, operation))
                    if opcode == 5:
                        first_sends[operation] = p._mutations[operation].first_send
                    if (opcode, operation) == target:
                        head = self.c.get(namespace, col._head_key, retry_transport=False)
                        self.assertEqual(head['value'], b'MBIX1H' + bytes(8))
                        before_commit.append(head['version'])
                    return write(data, deadline)

                def after_length(count, deadline):
                    if count == 24 and writes[-1] == target:
                        head = self.c.get(namespace, col._head_key, retry_transport=False)
                        self.assertEqual((head['value'], head['ack']), (b'MBIX1H\x01' + bytes(7), Ack.FLUSH))
                        observed.append(head['version'])
                        if cause is loss:
                            raise loss
                        signal.raise_signal(signal.SIGINT)
                    return read(count, deadline)

                def interrupt(*args):
                    raise cause

                def close():
                    self.assertIsNone(c.channel)
                    error, = p.uncertainties
                    self.assertIs(error.cause, cause)
                    self.assertIs(error.request, p._mutations[target[1]].request)
                    n = len(closes)
                    self.assertEqual(error.cleanup_errors, cleanup[:n])
                    closes.append(error)
                    if n + 1 == max(1, len(cleanup)):
                        physical_close()
                    if n < len(cleanup):
                        raise cleanup[n]

                handler = signal.signal(signal.SIGINT, interrupt)
                try:
                    with (patch.object(c, 'write_all', side_effect=capture),
                          patch.object(c, 'read_exact', side_effect=after_length),
                          patch.object(c, 'connect', side_effect=AssertionError('reconnect')) as reconnect,
                          patch.object(channel, 'close', side_effect=close)):
                        with self.assertRaises(ApplicationStop) as caught:
                            col.append(c, p, Deadline.after(30))
                    reconnect.assert_not_called()
                finally:
                    signal.signal(signal.SIGINT, handler)

                error, = p.uncertainties
                self.assertIs(caught.exception, close_stop if cause is loss else cause)
                self.assertIs(error.publication, p)
                self.assertIs(error.cause, cause)
                self.assertEqual(error.cleanup_errors, cleanup)
                self.assertEqual(closes, [error] * max(1, len(cleanup)))
                self.assertEqual(str(error), 'uncertain operation original:commit')
                self.assertIsNone(c.channel)
                self.assertIsNone(channel.handle)
                self.assertEqual(writes.count(target), 1)
                version, = observed
                request = error.request
                self.assertEqual((request.namespace, request.key, request.value, request.ack, error.epoch),
                                 (namespace, col._head_key, b'MBIX1H\x01' + bytes(7), Ack.FLUSH, c.epoch))
                self.assertEqual(before_commit, [request.expected])
                self.assertLess(request.expected, version)
                self.assertEqual(p._mutations[target[1]].first_send, first_sends[target[1]])
                self.assertEqual(p._custody.oldest_first_send, min(first_sends.values()))
                self.assertEqual(self.c.get(namespace, col._row_key(1))['value'], col._encode(record))

    def test_existing_exchange_override_supports_raw_and_indexed_calls(self):
        c, col = self.client(self.ExistingExchange), self.col
        self.addCleanup(c.close)
        c.put('raw', 'key', b'probe')
        self.assertEqual(c.get('raw', 'key')['value'], b'probe')
        channel = c.channel
        declaration = col.prepare_declaration(c, self.namespace, 'existing', Ack.OS)
        with patch.object(c, 'write_all', wraps=c.write_all) as write:
            col.declare(c, declaration, Deadline.after(30))
        self.assertEqual([call.args[0][8] for call in write.call_args_list], [2, 5, 2, 5])
        self.assertEqual(declaration.uncertainties, ())
        self.assertEqual(self.c.get(self.namespace, col._head_key)['value'], b'MBIX1H' + bytes(8))
        publication = self.prepare(row(1), client=c)
        self.assertEqual(col.append(c, publication, Deadline.after(30)).through, 1)
        self.assertEqual(publication.uncertainties, ())
        self.assertIs(c.channel, channel)
        self.assertEqual(self.query().records, (row(1),))

        namespace = self.namespace + '-mismatch'
        self.c.put(namespace, 'different-key', b'first', expected=0, op_id='definitive:schema')
        refused = col.prepare_declaration(c, namespace, 'definitive', Ack.OS)
        with self.assertRaises(Refusal) as caught:
            col.declare(c, refused, Deadline.after(30))
        self.assertEqual(caught.exception.code, 14)
        self.assertEqual(refused.uncertainties, ())
        self.assertIs(c.channel, channel)
        self.assertIsNone(c.get(namespace, col._schema_key))

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
        self.col.append(c, p, Deadline.after(30))
        self.assertEqual(p.uncertainties, (original,))
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

    def test_current_native_conflict_reconciles(self):
        col, c = self.col, self.client(self.ExistingExchange)
        self.addCleanup(c.close)
        p = self.prepare(row(1), client=c)
        observer = self.client()
        self.addCleanup(observer.close)
        write = c.write_all
        requests = []

        def race(frame, deadline):
            request = Decoder(frame[17:])
            request.unpack('<Q')
            op = request.blob().decode()
            requests.append(op)
            if op == p.operation + ':commit':
                previous = observer.get(self.namespace, col._head_key)
                observer.put(self.namespace, col._head_key, previous['value'],
                             expected=previous['version'], ack=Ack.FLUSH)
            return write(frame, deadline)

        channel = c.channel
        with patch.object(c, 'write_all', side_effect=race):
            with self.assertRaisesRegex(Conflict, 'boundary changed'):
                col.append(c, p, Deadline.after(30))
        self.assertEqual(requests.count(p.operation + ':commit'), 1)
        self.assertEqual(requests[-1], p.operation + ':commit-reconcile')
        self.assertEqual(p.uncertainties, ())
        self.assertIs(c.channel, channel)
        self.assertEqual(c.get(self.namespace, col._head_key)['value'], b'MBIX1H' + bytes(8))

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
