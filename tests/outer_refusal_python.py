"""Atomic publication over the SDK's framed, in-memory byte peer."""
from pathlib import Path
import struct
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'python'))
import membus_atomic as atomic
from membus import Client, CODES, MAX_FRAME, MAX_VALUE, Refusal, lp
from typed_reply_python import BytePeer, response

DEADLINE = 4_102_444_800_000


def answer(code=0, tail=b''):
    def reply(request):
        opcode, request_id = struct.unpack_from('<BQ', request, 8)
        return response(opcode, request_id, tail, code)
    return reply


def client(reply):
    c = Client.__new__(Client)
    c.transport, c.address, c.request_id = 'authored', 'byte-peer', 0
    c.epoch, c.max_frame, c.max_value = 7, MAX_FRAME, MAX_VALUE
    c.codes = dict(CODES)
    c.channel = BytePeer(reply)
    return c, c.channel


def original():
    raw = b'MAR1' + lp('fixture') + lp('original')
    raw += struct.pack('<QQBIH', 7, DEADLINE, 2, 0, 1)
    raw += lp('key') + b'\x00' + lp(b'authored value')
    return atomic.Original.from_bytes(raw)


def success(version=91, ack=2):
    return b'MAW1\x00' + struct.pack('<QB', version, ack)


class AtomicBoundary(unittest.TestCase):
    def publication(self, reply):
        c, peer = client(reply)
        o = original()
        return c, peer, o, atomic.Publication(c, o)

    def assert_request(self, peer, o, request_id=1):
        self.assertEqual(len(peer.packets), 1)
        request = peer.packets[0]
        self.assertEqual(struct.unpack('<HHBQ', request[4:17]), (1, 0, 29, request_id))
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
        for mode in ('expired', 'disconnected', 'size', 'value'):
            with self.subTest(mode=mode):
                c, peer, o, publication = self.publication(answer(0, success()))
                if mode == 'disconnected':
                    c.channel = None
                if mode == 'size':
                    c.max_frame = 16
                if mode == 'value':
                    c.max_value = len(b'authored value') - 1
                with self.assertRaises(atomic.Refused) as caught:
                    publication.publish(c, 1 if mode == 'expired' else DEADLINE)
                self.assertEqual(caught.exception.status, {
                    'expired': 'Expired', 'disconnected': 'Unavailable',
                    'size': 'Resource', 'value': 'Resource',
                }[mode])
                self.assertEqual(peer.packets, [])
                self.assertEqual(peer.pending, b'')
                self.assertEqual(publication.uncertainties, [])

    def test_prior_uncertainty_survives_later_refusal_and_success(self):
        c, peer, o, publication = self.publication(lambda request: b'')
        with self.assertRaises(atomic.Uncertain):
            publication.publish(c, DEADLINE)
        prior = tuple(publication.uncertainties)
        self.assert_request(peer, o)
        c.channel = refusal_peer = BytePeer(answer(12))
        with self.assertRaises(Refusal) as caught:
            publication.publish(c, DEADLINE)
        self.assertEqual(caught.exception.code, 12)
        self.assert_request(refusal_peer, o, 2)
        self.assertEqual(tuple(publication.uncertainties), prior)
        self.assertIs(c.channel, refusal_peer)

        c.channel = success_peer = BytePeer(answer(0, success()))
        self.assertEqual(publication.publish(c, DEADLINE),
                         dict(version=91, ack=atomic.Ack.FLUSH, epoch=7))
        self.assert_request(success_peer, o, 3)
        self.assertEqual(tuple(publication.uncertainties), prior)
        self.assertIs(publication.original, o)


if __name__ == '__main__':
    unittest.main(verbosity=2)
