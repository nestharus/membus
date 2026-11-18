"""Finite HELLO/BATCH byte-peer controls; no resident, socket or native library."""
import ctypes
import socket
import struct
import unittest
from unittest.mock import patch

import os
import sys
from pathlib import Path
sys.path.insert(0, os.environ.get('MEMBUS_PYTHON_SDK_ROOT', str(Path(__file__).resolve().parents[1] / 'python')))
with patch.object(ctypes, 'WinDLL', side_effect=AssertionError('native DLL forbidden'), create=True), patch.object(socket, 'create_connection', side_effect=AssertionError('socket forbidden')):
    import membus

def field(value):
    value = value.encode() if isinstance(value, str) else value
    return len(value).to_bytes(4, 'little') + value


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


DEADLINE = 2_000_000
ITEM = dict(op_id='item', key='key', value=b'value', ack=2)


def outcomes(*rows):
    return struct.pack('<H', len(rows)) + b''.join(struct.pack('<HQB', *r) for r in rows)


class BatchRefusals(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(membus.time, 'time', lambda: 1000.0))
        self.enterContext(patch.object(membus.time, 'monotonic_ns', lambda: 123_000_000_000))
        for name in ('WinDLL', 'CDLL', 'PyDLL', 'OleDLL'):
            self.enterContext(patch.object(ctypes, name, side_effect=AssertionError('native DLL forbidden'), create=True))
        self.enterContext(patch.object(socket, 'socket', side_effect=AssertionError('socket forbidden')))
        self.enterContext(patch.object(socket, 'create_connection', side_effect=AssertionError('socket forbidden')))

    def client(self, tail, extensions=(), *, outer=0, codes=None):
        # Advertise only mandatory legacy codes plus explicitly authored extensions.
        vocabulary = {k: v for k, v in membus.CODES.items() if k <= 19} if codes is None else codes.copy()
        vocabulary.update(extensions)
        hello = struct.pack('<QIIHBQH', 7, membus.MAX_FRAME, membus.MAX_VALUE, 128, 0, 10, len(vocabulary))
        hello += b''.join(struct.pack('<HB', k, v[1]) + field(v[0]) for k, v in vocabulary.items())

        def reply(packet):
            op, rid = packet[8], int.from_bytes(packet[9:17], 'little')
            self.peer.pending.clear()
            return response(op, rid, hello if op == 1 else tail, 0 if op == 1 else outer)

        self.peer = BytePeer(reply)
        self.enterContext(patch.object(membus, 'Pipe', return_value=self.peer))
        c = membus.Client('finite', transport='authored', timeout=10)
        self.assertEqual(c.codes, vocabulary)
        return c

    def batch(self, c, items=None):
        return c.batch('authored', [ITEM.copy()] if items is None else items,
                       op_id='batch', deadline=DEADLINE)

    def test_negotiated_transient_refusal(self):
        c = self.client(outcomes((60000, 0, 0)), [(60000, ('authored_extension', True))])
        self.assertEqual(self.batch(c), [dict(code=60000, version=0, ack=0, name='authored_extension', transient=True)])
        self.assertEqual(len(self.peer.packets), 2)

    def test_negotiated_nontransient_refusal(self):
        c = self.client(outcomes((60001, 17, 1)), [(60001, ('authored_permanent', False))])
        self.assertEqual(self.batch(c), [dict(code=60001, version=17, ack=1, name='authored_permanent', transient=False)])

    def test_all_mandatory_mapped_refusals(self):
        codes = {k: v for k, v in membus.CODES.items() if k <= 19}
        c = self.client(outcomes(*[(k, k + 100, k % 3) for k in codes]))
        result = self.batch(c, [dict(ITEM, op_id=str(k)) for k in codes])
        self.assertEqual(result, [dict(code=k, version=k+100, ack=k%3, name=v[0], transient=v[1]) for k, v in codes.items()])

    def test_real_success_keeps_zero_and_all_acks(self):
        c = self.client(outcomes((0, 91, 0), (0, 92, 1), (0, 93, 2)))
        self.assertEqual(self.batch(c, [dict(ITEM, op_id=str(k)) for k in range(3)]),
                         [dict(code=0, version=91+k, ack=k, name='success', transient=False) for k in range(3)])

    def test_mixed_outcomes_keep_position_and_numeric_fields(self):
        c = self.client(outcomes((60000, 7, 0), (0, 91, 2), (12, 9, 1), (60001, 11, 0)),
                        [(60000, ('authored_extension', True)), (60001, ('authored_permanent', False))])
        items = [dict(ITEM, op_id='first'), dict(ITEM, op_id='second', expected=8),
                 dict(op_id='third', key='other', remove=True, expected=9), dict(ITEM, op_id='fourth')]
        self.assertEqual(self.batch(c, items), [
            dict(code=60000, version=7, ack=0, name='authored_extension', transient=True),
            dict(code=0, version=91, ack=2, name='success', transient=False),
            dict(code=12, version=9, ack=1, name='conflict', transient=False),
            dict(code=60001, version=11, ack=0, name='authored_permanent', transient=False)])
        packet = self.peer.packets[-1]
        expected = struct.pack('<Q', DEADLINE) + field('batch') + field('authored') + b'\x04\x00'
        expected += b'\x04' + field('first') + field('key') + b'\x02' + field(b'value')
        expected += b'\x05' + field('second') + field('key') + b'\x02' + struct.pack('<Q', 8) + field(b'value')
        expected += b'\x06' + field('third') + field('other') + b'\x01' + struct.pack('<Q', 9)
        expected += b'\x04' + field('fourth') + field('key') + b'\x02' + field(b'value')
        self.assertEqual(packet[17:], expected)

    def test_empty_batch(self):
        self.assertEqual(self.batch(self.client(outcomes()), []), [])




    def test_outer_negotiated_refusal_remains_exception(self):
        c = self.client(b'', [(60000, ('authored_extension', True))], outer=60000)
        with self.assertRaises(membus.Refusal) as caught:
            self.batch(c)
        self.assertEqual((caught.exception.code, caught.exception.name, caught.exception.transient),
                         (60000, 'authored_extension', True))
        self.assertIs(c.channel, self.peer)


    def test_invalid_negotiation_is_not_admitted(self):
        codes = {k: v for k, v in membus.CODES.items() if k <= 19}
        codes[12] = ('authored_wrong_conflict', False)
        with self.assertRaisesRegex(ValueError, 'incompatible refusal-code list'):
            self.client(outcomes((12, 0, 0)), codes=codes)


if __name__ == '__main__':
    unittest.main(verbosity=2)
