"""Literal typed-reply controls through the full client; finite peers and clock only."""
import ctypes
import os
from pathlib import Path
import socket
import struct
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.environ.get('MEMBUS_PYTHON_SDK_ROOT', str(Path(__file__).resolve().parents[1] / 'python')))
# Install barriers before importing the complete selected module, not a copied loop.
with patch.object(ctypes, 'WinDLL', side_effect=AssertionError('native DLL forbidden'), create=True), \
     patch.object(ctypes, 'CDLL', side_effect=AssertionError('native DLL forbidden')), \
     patch.object(socket, 'create_connection', side_effect=AssertionError('socket forbidden')):
    import membus

DEADLINE = 2_000_000
FIXED = bytes.fromhex('5b0000000000000002')
MIXED = bytes.fromhex('020000005b00000000000000020c00000000000000000000')
SUB = bytes(range(16))


def field(value):
    value = value.encode() if isinstance(value, str) else value
    return len(value).to_bytes(4, 'little') + value


def request(op, rid, payload):
    return (13 + len(payload)).to_bytes(4, 'little') + b'\x01\x00\x00\x00' + bytes([op]) + rid.to_bytes(8, 'little') + payload


def response(op, rid, payload, code=0):
    body = b'\x01\x00\x00\x00' + bytes([op | 128]) + rid.to_bytes(8, 'little') + code.to_bytes(2, 'little') + payload
    return len(body).to_bytes(4, 'little') + body


class BytePeer:
    def __init__(self, action):
        self.action, self.pending, self.output = action, bytearray(), bytearray()
        self.packets, self.closed = [], 0

    def io(self, data, count, deadline, write):
        if write:
            part = data[:min(count, 7)]
            self.pending.extend(part)
            if len(self.pending) >= 4 and len(self.pending) == int.from_bytes(self.pending[:4], 'little') + 4:
                packet = bytes(self.pending)
                self.packets.append(packet)
                self.output.extend(self.action(packet))
            return len(part)
        part = bytes(self.output[:min(count, 3)])
        del self.output[:len(part)]
        return part

    def close(self):
        self.closed += 1


class TypedReplies(unittest.TestCase):
    def setUp(self):
        self.ns, self.wall = 123_000_000_000, 1000.0
        self.enterContext(patch.object(membus.time, 'monotonic_ns', lambda: self.ns))
        self.enterContext(patch.object(membus.time, 'time', lambda: self.wall))
        for name in ('WinDLL', 'CDLL', 'PyDLL', 'OleDLL'):
            self.enterContext(patch.object(ctypes, name, side_effect=AssertionError('native DLL forbidden'), create=True))
        self.enterContext(patch.object(socket, 'socket', side_effect=AssertionError('socket forbidden')))
        self.enterContext(patch.object(socket, 'create_connection', side_effect=AssertionError('socket forbidden')))

    def client(self, op, tail, *, loss=True, code=0, refusal_tail=False, elapsed=1,
               reconnect_elapsed=0, epoch=7, endpoint='finite', new_window=999, second_loss=False):
        c = membus.Client.__new__(membus.Client)
        c.timeout, c.transport, c.address, c.request_id = 10, 'authored', 'finite', 0
        c.epoch, c.retry_ms, c.max_frame, c.codes = 7, 10, membus.MAX_FRAME, dict(membus.CODES)
        self.first_loss = ConnectionError('authored first reply lost')
        self.connects = []

        def first(packet):
            self.ns += elapsed
            if loss:
                raise self.first_loss
            return response(op, 1, tail, code)

        def second(packet):
            if second_loss:
                raise ConnectionError('authored second reply lost')
            return response(op, 2, tail, code)

        self.first, self.second = BytePeer(first), BytePeer(second)
        c.channel = self.first

        def reconnect(deadline):
            self.connects.append(deadline)
            self.ns += reconnect_elapsed
            c.epoch, c.address, c.retry_ms, c.channel = epoch, endpoint, new_window, self.second
        c.connect = reconnect
        return c

    def suffix(self, op):
        if op == 4: return field('key') + b'\x02' + field(b'value')
        if op == 5: return field('key') + b'\x02\x09\x00\x00\x00\x00\x00\x00\x00' + field(b'value')
        if op == 6: return field('key') + b'\x02\x09\x00\x00\x00\x00\x00\x00\x00'
        if op == 17: return field(SUB)
        if op == 7:
            return b'\x02\x00\x04' + field('item-one') + field('key') + b'\x02' + field(b'value') + b'\x06' + field('item-two') + field('other') + b'\x01\x09\x00\x00\x00\x00\x00\x00\x00'
        return b'raw'

    def invoke(self, c, op, *, retry_transport=True):
        options = dict(op_id='original', deadline=DEADLINE)
        if not retry_transport: options['retry_transport'] = False
        if op in (4, 5):
            return c.put('authored', 'key', b'value', ack=2, expected=9 if op == 5 else None, **options)
        if op == 6: return c.remove('authored', 'key', 9, ack=2, **options)
        if op == 17: return c.unsubscribe('authored', SUB, **options)
        if op == 7:
            return c.batch('authored', [dict(op_id='item-one', key='key', value=b'value', ack=2),
                                      dict(op_id='item-two', key='other', remove=True, expected=9, ack=1)], **options)
        return c.call(op, 'authored', b'raw', **options)

    def identity(self, op, sends):
        payload = b'\x80\x84\x1e\x00\x00\x00\x00\x00' + field('original') + field('authored') + self.suffix(op)
        self.assertEqual(self.first.packets, [request(op, 1, payload)])
        self.assertEqual(self.second.packets, [request(op, 2, payload)] if sends == 2 else [])
        return payload

    def uncertain(self, error, op, *, sends=2):
        self.assertEqual(type(error).__name__, 'CallUncertain')
        original = error.original
        self.assertEqual(tuple(original), (('authored', 'finite'), 7, op, 'original', 'authored', self.identity(op, sends), 123_000_000_000, 10))
        self.assertIs(error.cause, self.first_loss)
        self.assertIs(error.__cause__, error.last_error)
        with self.assertRaises(AttributeError): original.retry_ms = 999
        self.assertLessEqual(len(self.connects), 1)

    def malformed(self, op, tails, error_type):
        for tail in tails:
            for loss in (False, True):
                with self.subTest(op=op, tail=tail.hex(), loss=loss):
                    self.ns = 123_000_000_000
                    c = self.client(op, tail, loss=loss)
                    with self.assertRaises(Exception) as caught: self.invoke(c, op)
                    if loss:
                        self.uncertain(caught.exception, op)
                        self.assertIsInstance(caught.exception.last_error, error_type)
                        self.assertIsNone(c.channel)
                    else:
                        self.assertIs(type(caught.exception), error_type)
                        self.identity(op, 1)
                        self.assertEqual(self.connects, [])

    def test_put_short_and_long_keep_first_loss(self):
        self.malformed(4, [b'', FIXED[:-1], FIXED+b'\x00'], struct.error)

    def test_conditional_put_short_and_long_keep_first_loss(self):
        self.malformed(5, [b'', FIXED[:-1], FIXED+b'\x00'], struct.error)

    def test_remove_short_and_long_keep_first_loss(self):
        self.malformed(6, [b'', FIXED[:-1], FIXED+b'\x00'], struct.error)

    def test_invalid_mutation_ack_is_not_settlement(self):
        for op in (4, 5, 6): self.malformed(op, [FIXED[:-1]+b'\x03'], ValueError)

    def test_batch_truncation_keeps_first_loss(self):
        self.malformed(7, [b'', b'\x02', b'\x02\x00', MIXED[:-1]], ValueError)

    def test_batch_count_must_match_original_items(self):
        self.malformed(7, [b'\x00\x00', b'\x01\x00'+MIXED[2:13], b'\x03\x00'+MIXED[2:]], ValueError)

    def test_batch_trailing_bytes_are_not_settlement(self):
        self.malformed(7, [MIXED+b'\x00'], ValueError)

    def test_batch_unknown_code_and_invalid_ack_are_not_settlement(self):
        self.malformed(7, [MIXED[:13]+b'\xff\xff'+MIXED[15:], MIXED[:12]+b'\x03'+MIXED[13:]], ValueError)

    def test_unsubscribe_nonempty_keeps_first_loss(self):
        self.malformed(17, [b'\x00', FIXED], ValueError)

    def test_valid_wrappers_preserve_results_with_and_without_loss(self):
        for op in (4, 5, 6, 7, 17):
            for loss in (False, True):
                with self.subTest(op=op, loss=loss):
                    self.ns = 123_000_000_000
                    tail = MIXED if op == 7 else b'' if op == 17 else FIXED
                    c = self.client(op, tail, loss=loss)
                    actual = self.invoke(c, op)
                    expected = ([dict(code=0,version=91,ack=2,name='success',transient=False),
                                 dict(code=12,version=0,ack=0,name='conflict',transient=False)] if op == 7 else None if op == 17 else dict(version=91,ack=2))
                    self.assertEqual(actual, expected)
                    self.identity(op, 2 if loss else 1)

    def test_memory_ack_is_preserved_without_upgrade(self):
        c = self.client(4, FIXED[:-1]+b'\x00')
        self.assertEqual(self.invoke(c, 4), dict(version=91, ack=0))
        self.identity(4, 2)

    def test_empty_batch_is_valid(self):
        c = self.client(7, b'\x00\x00')
        self.assertEqual(c.batch('authored', [], op_id='original', deadline=DEADLINE), [])
        self.assertEqual(len(self.second.packets), 1)

    def test_known_refusals_keep_distinct_prior_loss_meaning(self):
        for op in (4, 5, 6, 7, 17):
            for code in (12, 16, 17):
                for loss in (False, True):
                    with self.subTest(op=op, code=code, loss=loss):
                        self.ns = 123_000_000_000
                        c = self.client(op, b'', loss=loss, code=code)
                        with self.assertRaises(Exception) as caught: self.invoke(c, op)
                        if loss:
                            self.uncertain(caught.exception, op)
                            self.assertEqual(caught.exception.last_error.code, code)
                        else:
                            self.assertIsInstance(caught.exception, membus.Refusal)
                            self.assertEqual(caught.exception.code, code)
                            self.identity(op, 1)


    def test_opt_out_keeps_first_transport_error_and_one_send(self):
        for op in (4, 5, 6, 7, 17):
            c = self.client(op, b'')
            with self.assertRaises(ConnectionError) as caught: self.invoke(c, op, retry_transport=False)
            self.assertIs(caught.exception, self.first_loss)
            self.identity(op, 1)
            self.assertEqual(self.connects, [])

    def test_direct_call_returns_identical_valid_bytes(self):
        for op, tail in [(4,FIXED),(5,FIXED),(6,FIXED),(7,MIXED),(17,b'')]:
            c = self.client(op, tail)
            self.assertEqual(c.call(op,'authored',self.suffix(op),'original',DEADLINE),tail)
            self.identity(op, 2)

    def test_direct_call_malformed_success_keeps_first_loss(self):
        c = self.client(4, b'')
        with self.assertRaises(Exception) as caught: c.call(4,'authored',self.suffix(4),'original',DEADLINE)
        self.uncertain(caught.exception,4)

    def test_original_horizon_and_new_handshake_window_are_unchanged(self):
        for elapsed, reconnect_elapsed in [(10_000_000,0),(1,9_999_999)]:
            self.ns = 123_000_000_000
            c = self.client(4,FIXED,elapsed=elapsed,reconnect_elapsed=reconnect_elapsed)
            with self.assertRaises(ConnectionError) as caught: self.invoke(c,4)
            self.uncertain(caught.exception,4,sends=1)

    def test_epoch_and_endpoint_changes_prevent_second_send(self):
        for changes in [dict(epoch=8),dict(endpoint='different')]:
            self.ns = 123_000_000_000
            c = self.client(4,FIXED,**changes)
            with self.assertRaises(ConnectionError) as caught: self.invoke(c,4)
            self.uncertain(caught.exception,4,sends=1)

    def test_original_deadline_is_not_replaced(self):
        c = self.client(4,FIXED)
        connect = c.connect
        def late(deadline):
            connect(deadline)
            self.wall = DEADLINE / 1000
        c.connect = late
        with self.assertRaises(ConnectionError) as caught: self.invoke(c,4)
        self.uncertain(caught.exception,4,sends=1)
        self.assertEqual(self.connects,[DEADLINE])

    def test_second_transport_loss_never_sends_a_third_time(self):
        c = self.client(4,FIXED,second_loss=True)
        with self.assertRaises(ConnectionError) as caught: self.invoke(c,4)
        self.uncertain(caught.exception,4)
        self.assertEqual(self.connects,[DEADLINE])

    def test_legacy_call_override_signature_still_works(self):
        class Legacy(membus.Client):
            def __init__(self): pass
            def call(self, opcode, ns, suffix, op_id, deadline): return FIXED
        self.assertEqual(Legacy().put('authored','key',b'value'),dict(version=91,ack=2))

    def test_raw_policy_reply_contract_is_unchanged(self):
        c = self.client(24,b'raw reply')
        self.assertEqual(self.invoke(c,24),b'raw reply')
        self.identity(24,2)

    def test_read_status_wrapper_remains_independent(self):
        c = self.client(26,bytes.fromhex('015b00000000000000'))
        self.assertEqual(c.get_status('authored','key',op_id='original',deadline=DEADLINE),dict(status='expired',version=91))
        self.assertEqual(len(self.second.packets),1)



    def test_unknown_raw_call_keeps_baseline_retry_and_bytes(self):
        c = self.client(99, b'raw reply')
        self.assertEqual(c.call(99, 'authored', b'raw', 'original', DEADLINE), b'raw reply')
        self.identity(99, 2)

    def test_snapshot_and_timer_creation_still_send_once(self):
        for op in (10, 16, 21, 22):
            with self.subTest(op=op):
                c = self.client(op, b'')
                with self.assertRaises(ConnectionError) as caught:
                    c.call(op, 'authored', b'raw', 'original', DEADLINE)
                self.assertIs(caught.exception, self.first_loss)
                self.identity(op, 1)
                self.assertEqual(self.connects, [])

    def test_zero_horizon_retains_original_and_prevents_retry(self):
        c = self.client(4, FIXED)
        c.retry_ms = 0
        with self.assertRaises(ConnectionError) as caught:
            self.invoke(c, 4)
        self.assertEqual(type(caught.exception).__name__, 'CallUncertain')
        self.assertEqual(caught.exception.original.retry_ms, 0)
        self.assertIs(caught.exception.cause, self.first_loss)
        self.identity(4, 1)
        self.assertEqual(self.connects, [])


    def test_unknown_outer_refusal_closes_the_channel(self):
        c = self.client(2, b'', loss=False, code=60_000)
        with self.assertRaisesRegex(ValueError, 'invalid refusal reply'):
            c.exchange(2, b'authored request', DEADLINE)
        self.assertIsNone(c.channel)
        self.assertEqual(self.first.closed, 1)
        self.assertEqual(len(self.first.packets), 1)

    def test_known_outer_refusal_rejects_trailing_bytes(self):
        for code in (2, 12, 16, 17):
            with self.subTest(code=code):
                c = self.client(2, b'\x00', loss=False, code=code)
                with self.assertRaisesRegex(ValueError, 'invalid refusal reply'):
                    c.exchange(2, b'authored request', DEADLINE)
                self.assertIsNone(c.channel)
                self.assertEqual(self.first.closed, 1)

    def test_outer_refusal_uses_the_negotiated_vocabulary(self):
        without_code = {code: row for code, row in membus.CODES.items() if code != 25}
        for negotiated in (without_code, {}):
            with self.subTest(codes=len(negotiated)):
                c = self.client(2, b'', loss=False, code=25)
                c.codes = negotiated
                with self.assertRaisesRegex(ValueError, 'invalid refusal reply'):
                    c.exchange(2, b'authored request', DEADLINE)
                self.assertEqual(self.first.closed, 1)

    def test_valid_outer_refusals_keep_the_channel_and_associated_details(self):
        negotiated = {**membus.CODES, 60_000: ('authored_extension', True)}
        for code, details in negotiated.items():
            with self.subTest(code=code):
                c = self.client(2, b'', loss=False, code=code)
                c.codes = negotiated
                with self.assertRaises(membus.Refusal) as caught:
                    c.exchange(2, b'authored request', DEADLINE)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual((caught.exception.name, caught.exception.transient), details)
                self.assertIs(c.channel, self.first)
                self.assertEqual(self.first.closed, 0)

    def test_bootstrap_outer_refusal_uses_the_default_vocabulary(self):
        c = self.client(1, b'', loss=False, code=1)
        del c.codes
        with self.assertRaises(membus.Refusal) as caught:
            c.exchange(1, b'hello', DEADLINE)
        self.assertEqual(caught.exception.code, 1)
        self.assertEqual(self.first.closed, 0)

        c = self.client(1, b'', loss=False, code=60_000)
        del c.codes
        with self.assertRaisesRegex(ValueError, 'invalid refusal reply'):
            c.exchange(1, b'hello', DEADLINE)
        self.assertEqual(self.first.closed, 1)

    def test_outer_refusal_follows_association_and_leaves_success_bytes_opaque(self):
        c = self.client(2, b'opaque success', loss=False)
        self.assertEqual(c.exchange(2, b'request', DEADLINE), b'opaque success')
        self.assertEqual(self.first.closed, 0)

        c = self.client(2, b'', loss=False, code=12)
        self.first.action = lambda packet: response(2, 2, b'', 12)
        with self.assertRaisesRegex(ValueError, 'association'):
            c.exchange(2, b'request', DEADLINE)
        self.assertEqual(self.first.closed, 1)

if __name__ == '__main__': unittest.main(verbosity=2)
