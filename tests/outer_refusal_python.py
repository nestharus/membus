"""Authored byte-channel controls for the shared SDK and explicit atomic source.

Set MEMBUS_ATOMIC_SOURCE_FILE to the selected canonical membus_atomic.py and,
optionally, MEMBUS_PYTHON_SDK_ROOT to an explicit SDK preimage. No endpoint opens.
"""
import importlib.util
import os
from pathlib import Path
import struct
import sys
import unittest

sys.path.insert(0, os.environ.get('MEMBUS_PYTHON_SDK_ROOT', str(Path(__file__).resolve().parents[1] / 'python')))
from membus import Client, CODES, MAX_FRAME, Refusal, lp

spec = importlib.util.spec_from_file_location('selected_atomic', os.environ['MEMBUS_ATOMIC_SOURCE_FILE'])
atomic = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = atomic
spec.loader.exec_module(atomic)

DEADLINE = 4_102_444_800_000


class BytePeer:
    """A finite authored peer; acceptance models a possible commit, not a server."""
    def __init__(self, reply):
        self.reply = reply
        self.request = bytearray()
        self.output = bytearray()
        self.accepted = []
        self.closed = 0

    def io(self, data, count, deadline, write):
        if write:
            part = data[:min(count, 7)]
            self.request.extend(part)
            if len(self.request) >= 4:
                size, = struct.unpack('<I', self.request[:4])
                if len(self.request) == size + 4:
                    self.accepted.append(bytes(self.request))
                    self.output.extend(self.reply(bytes(self.request)))
            return len(part)
        if not self.output:
            return b''
        part = bytes(self.output[:min(count, 5)])
        del self.output[:len(part)]
        return part

    def close(self):
        self.closed += 1


def answer(code=0, tail=b'', association=True):
    def reply(request):
        major, minor, opcode, request_id = struct.unpack('<HHBQ', request[4:17])
        body = struct.pack('<HHBQH', major, minor, opcode | 128,
                           request_id if association else request_id + 1, code) + tail
        return struct.pack('<I', len(body)) + body
    return reply


def client(reply, codes=None):
    c = Client.__new__(Client)
    c.transport, c.address, c.request_id = 'authored', 'byte-peer', 0
    c.epoch, c.max_frame = 7, MAX_FRAME
    if codes is not None:
        c.codes = codes
    c.channel = BytePeer(reply)
    return c, c.channel


def original():
    raw = b'MAR1' + lp('fixture') + lp('original')
    raw += struct.pack('<QQBIH', 7, DEADLINE, 2, 0, 1)
    raw += lp('key') + b'\x00' + lp(b'authored value')
    return atomic.Original.from_bytes(raw)


def success(version=91, ack=2):
    return b'MAW1\x00' + struct.pack('<QB', version, ack)


class ExchangeRefusals(unittest.TestCase):
    def test_unknown_associated_code_is_protocol_error(self):
        c, peer = client(answer(60000), dict(CODES))
        with self.assertRaises(ValueError):
            c.exchange(2, b'authored request', DEADLINE)
        self.assertIsNone(c.channel)
        self.assertEqual(peer.closed, 1)
        self.assertEqual(len(peer.accepted), 1)

    def test_known_refusal_tail_is_protocol_error(self):
        for code in (2, 12, 16, 17):
            with self.subTest(code=code):
                c, peer = client(answer(code, b'\x00'), dict(CODES))
                with self.assertRaises(ValueError):
                    c.exchange(2, b'authored request', DEADLINE)
                self.assertIsNone(c.channel)
                self.assertEqual(peer.closed, 1)

    def test_negotiated_membership_overrides_bootstrap_vocabulary(self):
        for codes in ({k: v for k, v in CODES.items() if k != 25}, {}):
            with self.subTest(codes=len(codes)):
                c, peer = client(answer(25), codes)
                with self.assertRaises(ValueError):
                    c.exchange(2, b'authored request', DEADLINE)
                self.assertEqual(peer.closed, 1)

    def test_negotiated_extension_and_known_refusals_remain_definitive(self):
        codes = dict(CODES, **{})
        codes[60000] = ('authored_extension', True)
        for code in codes:
            with self.subTest(code=code):
                c, peer = client(answer(code), codes)
                with self.assertRaises(Refusal) as caught:
                    c.exchange(2, b'authored request', DEADLINE)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual((caught.exception.name, caught.exception.transient), codes[code])
                self.assertIs(c.channel, peer)
                self.assertEqual(peer.closed, 0)

    def test_bootstrap_known_refusal_and_unknown_reply(self):
        c, peer = client(answer(1))
        with self.assertRaises(Refusal) as caught:
            c.exchange(1, b'hello', DEADLINE)
        self.assertEqual(caught.exception.code, 1)
        self.assertEqual(peer.closed, 0)
        c, peer = client(answer(60000))
        with self.assertRaises(ValueError):
            c.exchange(1, b'hello', DEADLINE)
        self.assertEqual(peer.closed, 1)

    def test_success_bytes_and_association_keep_separate_contracts(self):
        c, peer = client(answer(0, b'opaque success'), dict(CODES))
        self.assertEqual(c.exchange(2, b'request', DEADLINE), b'opaque success')
        self.assertEqual(peer.closed, 0)
        c, peer = client(answer(12, association=False), dict(CODES))
        with self.assertRaisesRegex(ValueError, 'association'):
            c.exchange(2, b'request', DEADLINE)
        self.assertEqual(peer.closed, 1)


class AtomicBoundary(unittest.TestCase):
    def publication(self, reply):
        c, peer = client(reply, dict(CODES))
        o = original()
        return c, peer, o, atomic.Publication(c, o)

    def assert_request(self, peer, o):
        self.assertEqual(len(peer.accepted), 1)
        request = peer.accepted[0]
        self.assertEqual(struct.unpack('<HHBQ', request[4:17]), (1, 0, 29, 1))
        self.assertEqual(request[17:], struct.pack('<Q', DEADLINE) + lp('original') + lp('fixture') + lp(o.raw))

    def uncertain(self, reply):
        c, peer, o, publication = self.publication(reply)
        with self.assertRaises(atomic.Uncertain) as caught:
            publication.publish(c, DEADLINE)
        self.assertIs(caught.exception.original, o)
        self.assertIs(publication.original, o)
        self.assertEqual(publication.uncertainties, [caught.exception.cause])
        self.assertIsNone(c.channel)
        self.assertEqual(peer.closed, 1)
        self.assert_request(peer, o)
        return caught.exception.cause

    def test_unknown_outer_refusal_after_possible_commit_preserves_original(self):
        self.assertIsInstance(self.uncertain(answer(60000)), ValueError)

    def test_known_outer_refusal_with_tail_after_possible_commit_is_uncertain(self):
        self.assertIsInstance(self.uncertain(answer(12, b'invalid tail')), ValueError)

    def test_outer_uncertain_codes_keep_exact_cause(self):
        for code in (16, 17):
            with self.subTest(code=code):
                cause = self.uncertain(answer(code))
                self.assertIsInstance(cause, Refusal)
                self.assertEqual(cause.code, code)

    def test_valid_outer_definitive_refusal_keeps_channel_and_no_new_uncertainty(self):
        for code in (2, 4, 12, 13, 14):
            with self.subTest(code=code):
                c, peer, o, publication = self.publication(answer(code))
                with self.assertRaises(Refusal) as caught:
                    publication.publish(c, DEADLINE)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(publication.uncertainties, [])
                self.assertIs(c.channel, peer)
                self.assert_request(peer, o)

    def test_exact_success_version_and_ack(self):
        c, peer, o, publication = self.publication(answer(0, success()))
        self.assertEqual(publication.publish(c, DEADLINE), dict(version=91, ack=atomic.Ack.FLUSH, epoch=7))
        self.assertEqual(publication.uncertainties, [])
        self.assertIs(c.channel, peer)
        self.assert_request(peer, o)

    def test_malformed_success_wrong_ack_and_zero_version_stay_uncertain(self):
        for raw in (b'', success()[:-1], success() + b'tail', success(0), success(91, 1), b'MAW1\xff'):
            with self.subTest(raw=raw):
                self.uncertain(answer(0, raw))

    def test_transport_loss_after_request_stays_uncertain_without_retry(self):
        self.assertIsInstance(self.uncertain(lambda request: b''), ConnectionError)

    def test_local_refusal_precedes_send(self):
        for mode in ('expired', 'disconnected', 'size'):
            with self.subTest(mode=mode):
                c, peer, o, publication = self.publication(answer(0, success()))
                if mode == 'disconnected':
                    c.channel = None
                if mode == 'size':
                    c.max_frame = 16
                with self.assertRaises(atomic.Refused):
                    publication.publish(c, 1 if mode == 'expired' else DEADLINE)
                self.assertEqual(peer.accepted, [])
                self.assertEqual(peer.request, b'')
                self.assertEqual(publication.uncertainties, [])

    def test_prior_uncertainty_survives_later_refusal_and_success(self):
        c, peer, o, publication = self.publication(lambda request: b'')
        with self.assertRaises(atomic.Uncertain):
            publication.publish(c, DEADLINE)
        prior = tuple(publication.uncertainties)
        for reply in (answer(12), answer(0, success())):
            c.channel = BytePeer(reply)
            try:
                publication.publish(c, DEADLINE)
            except Refusal as error:
                self.assertEqual(error.code, 12)
            self.assertEqual(tuple(publication.uncertainties), prior)
            self.assertIs(publication.original, o)


if __name__ == '__main__':
    unittest.main(verbosity=2)
