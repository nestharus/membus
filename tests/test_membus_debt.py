"""Ordinary call retry bounds on a finite clock and in-memory byte peers."""
from pathlib import Path
import struct
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'python'))
import membus

DEADLINE = 2_000_000


def field(value):
    value = value.encode('utf8') if isinstance(value, str) else value
    return struct.pack('<I', len(value)) + value


def packet(opcode, request_id, payload):
    return struct.pack('<IHHBQ', 13 + len(payload), 1, 0, opcode, request_id) + payload


def answer(opcode, request_id, tail=b'', code=0):
    return packet(opcode | 128, request_id, struct.pack('<H', code) + tail)


class Clock:
    def __init__(self):
        self.ns, self.wall = 123_000_000_000, 1000.0

    def monotonic_ns(self):
        return self.ns

    def time(self):
        return self.wall


class BytePeer:
    def __init__(self, reply):
        self.reply, self.request, self.output = reply, bytearray(), bytearray()
        self.accepted, self.closed = [], 0

    def io(self, data, count, deadline, write):
        if write:
            part = data[:min(count, 7)]
            self.request.extend(part)
            if len(self.request) >= 4 and len(self.request) == struct.unpack('<I', self.request[:4])[0] + 4:
                self.accepted.append(bytes(self.request))
                self.output.extend(self.reply(bytes(self.request)))
            return len(part)
        part = bytes(self.output[:min(count, 5)])
        del self.output[:len(part)]
        return part

    def close(self):
        self.closed += 1


class RetryControls(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.enterContext(patch.object(membus.time, 'monotonic_ns', lambda: self.clock.monotonic_ns()))
        self.enterContext(patch.object(membus.time, 'time', lambda: self.clock.time()))

    def setup_client(self, *, elapsed=0, reconnect_elapsed=0, retry_ms=10,
                     new_retry_ms=None, second_code=0, first_code=None,
                     reconnect_error=None):
        c = membus.Client.__new__(membus.Client)
        c.timeout, c.transport, c.address, c.request_id = 10, 'authored', 'finite', 0
        c.epoch, c.retry_ms, c.max_frame, c.codes = 7, retry_ms, membus.MAX_FRAME, dict(membus.CODES)
        self.connects = []
        self.first_error = ConnectionError('authored original lost reply')

        def first(request):
            self.clock.ns += elapsed
            if first_code is not None:
                return answer(request[8], 1, code=first_code)
            raise self.first_error

        def second(request):
            if second_code:
                tail = b''
            elif request[8] in (4, 5, 6):
                tail = struct.pack('<QB', 91, 2)
            else:
                tail = b'accepted'
            return answer(request[8], 2, tail, second_code)

        self.first, self.second = BytePeer(first), BytePeer(second)
        c.channel = self.first

        def reconnect(deadline):
            self.connects.append(deadline)
            self.clock.ns += reconnect_elapsed
            if reconnect_error:
                raise reconnect_error
            c.retry_ms = retry_ms if new_retry_ms is None else new_retry_ms
            c.channel = self.second

        c.connect = reconnect
        return c

    def invoke(self, c, opcode=4, suffix=b'authored suffix', **options):
        return c.call(opcode, 'authored', suffix, 'original', DEADLINE, **options)

    def assert_identity(self, opcode=4, suffix=b'authored suffix', sends=1):
        payload = struct.pack('<Q', DEADLINE) + field('original') + field('authored') + suffix
        self.assertEqual(self.first.accepted, [packet(opcode, 1, payload)])
        self.assertEqual(self.second.accepted, [packet(opcode, 2, payload)] if sends == 2 else [])

    def assert_uncertain(self, error, opcode=4, suffix=b'authored suffix', retry_ms=10):
        self.assertIsInstance(error, membus.CallUncertain)
        o = error.original
        self.assertEqual((o.endpoint, o.epoch, o.opcode, o.operation_id, o.namespace),
                         (('authored', 'finite'), 7, opcode, 'original', 'authored'))
        self.assertEqual(o.payload, struct.pack('<Q', DEADLINE) + field('original') + field('authored') + suffix)
        self.assertEqual((o.first_send_ns, o.retry_ms), (123_000_000_000, retry_ms))
        self.assertIs(error.cause, self.first_error)
        with self.assertRaises(AttributeError):
            o.retry_ms = 999

    def test_elapsed_horizon_refuses_resend_for_every_default_mutation_opcode(self):
        for opcode in (4, 5, 6, 7, 17, 24):
            with self.subTest(opcode=opcode):
                self.clock = Clock()
                c = self.setup_client(elapsed=10_000_000)
                with self.assertRaises(ConnectionError) as caught:
                    self.invoke(c, opcode)
                self.assert_uncertain(caught.exception, opcode)
                self.assert_identity(opcode)
                self.assertEqual(self.connects, [])
                self.assertIsNone(c.channel)

    def test_exact_boundary_and_zero_are_not_retryable(self):
        for window, elapsed in ((0, 0), (1, 1_000_000), (10, 10_000_001)):
            with self.subTest(window=window, elapsed=elapsed):
                self.clock = Clock()
                c = self.setup_client(retry_ms=window, elapsed=elapsed)
                with self.assertRaises(ConnectionError) as caught:
                    self.invoke(c)
                self.assert_uncertain(caught.exception, retry_ms=window)
                self.assert_identity()
                self.assertEqual(self.connects, [])

    def test_one_nanosecond_inside_retries_identical_payload_and_returns_bytes(self):
        for next_offer in (1, 10, 999):
            with self.subTest(next_offer=next_offer):
                self.clock = Clock()
                c = self.setup_client(elapsed=9_999_999, new_retry_ms=next_offer)
                self.assertEqual(self.invoke(c), struct.pack('<QB', 91, 2))
                self.assert_identity(sends=2)
                self.assertEqual(self.connects, [DEADLINE])

    def test_reconnect_consumes_original_budget(self):
        c = self.setup_client(elapsed=1_000_000, reconnect_elapsed=9_000_000)
        with self.assertRaises(ConnectionError) as caught:
            self.invoke(c)
        self.assert_uncertain(caught.exception)
        self.assert_identity()
        self.assertEqual(self.connects, [DEADLINE])
        self.assertIsInstance(caught.exception.last_error, TimeoutError)
        self.assertIsNone(c.channel)

    def test_new_handshake_horizon_does_not_renew_original(self):
        c = self.setup_client(elapsed=1_000_000, reconnect_elapsed=9_000_000, new_retry_ms=999999)
        with self.assertRaises(ConnectionError) as caught:
            self.invoke(c)
        self.assert_uncertain(caught.exception)
        self.assert_identity()

    def test_wall_clock_rollback_does_not_renew_monotonic_horizon(self):
        c = self.setup_client(elapsed=10_000_000)
        first_reply = self.first.reply

        def rollback(request):
            self.clock.wall = 1.0
            return first_reply(request)

        self.first.reply = rollback
        with self.assertRaises(ConnectionError) as caught:
            self.invoke(c)
        self.assert_uncertain(caught.exception)
        self.assert_identity()

    def test_reconnect_failure_keeps_first_loss(self):
        failure = OSError('authored reconnect failure')
        c = self.setup_client(reconnect_error=failure)
        with self.assertRaises(ConnectionError) as caught:
            self.invoke(c)
        self.assert_uncertain(caught.exception)
        self.assertIs(caught.exception.last_error, failure)
        self.assert_identity()

    def test_later_refusal_cannot_erase_earlier_loss(self):
        for code in (2, 5, 12, 13, 14, 16, 17, 18, 19):
            with self.subTest(code=code):
                self.clock = Clock()
                c = self.setup_client(second_code=code)
                with self.assertRaises(ConnectionError) as caught:
                    self.invoke(c)
                self.assert_uncertain(caught.exception)
                self.assertEqual(caught.exception.last_error.code, code)
                self.assert_identity(sends=2)

    def test_read_status_and_known_token_operations_keep_transport_retry(self):
        for opcode in (2, 3, 8, 9, 11, 12, 13, 14, 15, 18, 19, 20, 23, 25, 26, 27, 31):
            with self.subTest(opcode=opcode):
                self.clock = Clock()
                c = self.setup_client(retry_ms=0, elapsed=99_000_000)
                suffix = field(bytes(range(16)))
                self.assertEqual(self.invoke(c, opcode, suffix), b'accepted')
                self.assert_identity(opcode, suffix, sends=2)

    def test_creations_and_unknown_generic_operations_remain_single_send(self):
        for opcode in (10, 16, 21, 22, 11, 12, 14, 15, 28, 29, 30, 99):
            with self.subTest(opcode=opcode):
                self.clock = Clock()
                c = self.setup_client()
                with self.assertRaises(ConnectionError) as caught:
                    self.invoke(c, opcode, b'\x00' * 4)
                self.assertIs(caught.exception, self.first_error)
                self.assertIsNone(c.channel)
                self.assert_identity(opcode, b'\x00' * 4)
                self.assertEqual(self.connects, [])

    def test_first_refusal_is_unchanged_including_uncertain_protocol_codes(self):
        for code in (12, 13, 16, 17):
            with self.subTest(code=code):
                c = self.setup_client(first_code=code)
                with self.assertRaises(membus.Refusal) as caught:
                    self.invoke(c)
                self.assertEqual(caught.exception.code, code)
                self.assert_identity()
                self.assertEqual(self.connects, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
