"""Run directly on Windows to exercise the SDK against private named pipes."""
from contextlib import closing
from multiprocessing.connection import BUFSIZE, Listener as DuplexListener
import os
from pathlib import Path
import struct
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'python'))
from membus import Pipe, deadline_ms, lp
from membus_signals import Listener


def change(version):
    body = (b'MBC1' + struct.pack('<H', 1) + b's' * 16 + struct.pack('<QQ', 1, version)
            + lp('fixture') + lp('key') + struct.pack('<QBBB', version, 2, 0, 1)
            + lp(b'payload'))
    return lp(body)


def after_wait_expires(pipe, action):
    # Run the peer action between a real WAIT_TIMEOUT and the real cancellation.
    wait = pipe.k.WaitForSingleObject

    def wait_then_act(event, milliseconds):
        result = wait(event, milliseconds)
        if result == 258:
            action()
        return result

    return patch.object(pipe.k, 'WaitForSingleObject', side_effect=wait_then_act)


@unittest.skipUnless(sys.platform == 'win32', 'requires Windows named pipes')
class WindowsPipeCompletionTests(unittest.TestCase):
    def setUp(self):
        self.listener = Listener()
        self.addCleanup(self.listener.close)

    def connect(self):
        fd = os.open(self.listener.endpoint, os.O_WRONLY | os.O_BINARY)
        self.writer = os.fdopen(fd, 'wb', buffering=0)
        self.addCleanup(self.writer.close)
        self.listener.accept(deadline_ms(1))

    def test_quiet_timeouts_keep_next_frame_readable(self):
        with self.assertRaises(TimeoutError):
            self.listener.receive(0)
        self.connect()
        for timeout in (0, .02):
            with self.assertRaises(TimeoutError):
                self.listener.receive(timeout)
            self.assertTrue(self.listener.connected)
        self.writer.write(change(1))
        self.assertEqual(self.listener.receive(0)['version'], 1)

    def check_boundary(self, split):
        self.connect()
        frame = change(1)
        if split:
            self.writer.write(frame[:split])
        remaining = frame[split:] + change(2)
        with after_wait_expires(self.listener.pipe, lambda: self.writer.write(remaining)) as wait:
            received = self.listener.receive(0)
        self.assertEqual(wait.call_count, 1)
        self.assertEqual((received['version'], received['value']), (1, b'payload'))
        self.assertEqual(self.listener.receive(0)['version'], 2)
        with self.assertRaises(TimeoutError):
            self.listener.receive(0)

    def test_complete_header_at_deadline(self):
        self.check_boundary(0)

    def test_remaining_header_at_deadline(self):
        self.check_boundary(2)

    def test_complete_body_at_deadline(self):
        self.check_boundary(4)

    def test_remaining_body_at_deadline(self):
        self.check_boundary(11)

    def test_genuine_partial_frame_timeout_disconnects(self):
        self.connect()
        self.writer.write(change(1)[:2])
        with self.assertRaisesRegex(ConnectionError, 'partial signal timed out'):
            self.listener.receive(0)
        self.assertFalse(self.listener.connected)

    def test_peer_close_during_timeout_is_a_transport_error(self):
        self.connect()
        with after_wait_expires(self.listener.pipe, self.writer.close):
            with self.assertRaises(OSError) as raised:
                self.listener.receive(0)
        self.assertEqual(raised.exception.winerror, 109)  # ERROR_BROKEN_PIPE
        self.assertFalse(self.listener.connected)

    def test_failed_wait_preserves_error_after_draining_read(self):
        self.connect()
        kernel = self.listener.pipe.k
        wait = kernel.WaitForSingleObject
        with patch.object(kernel, 'WaitForSingleObject', side_effect=lambda event, ms: wait(None, ms)), \
                patch.object(kernel, 'GetOverlappedResult', wraps=kernel.GetOverlappedResult) as drain:
            with self.assertRaises(OSError) as raised:
                self.listener.receive(0)
        self.assertEqual(raised.exception.winerror, 6)  # ERROR_INVALID_HANDLE
        self.assertTrue(drain.call_args.args[-1])
        self.assertFalse(self.listener.connected)

    def test_pending_write_reports_completed_count(self):
        with DuplexListener(family='AF_PIPE') as server, closing(Pipe(server.address, deadline_ms(1))) as pipe, \
                server.accept() as peer:
            full = b'x' * BUFSIZE
            self.assertEqual(pipe.io(full, len(full), deadline_ms(1), True), len(full))
            with self.assertRaises(TimeoutError):
                pipe.io(b'cancel', 6, deadline_ms(0), True)
            consumed = []
            with after_wait_expires(pipe, lambda: consumed.append(peer.recv_bytes())):
                count = pipe.io(b'next', 4, deadline_ms(0), True)
            self.assertEqual(consumed, [full])
            self.assertEqual(count, 4)
            self.assertTrue(peer.poll(1))
            self.assertEqual(peer.recv_bytes(), b'next')
            self.assertFalse(peer.poll(0))


if __name__ == '__main__':
    unittest.main()
