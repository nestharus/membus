"""Authored protocol controls and finite private native interoperation."""
from pathlib import Path
import os
import struct
import sys
import time
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'python'))
from membus import CODES, Client, Decoder, MAX_BATCH, MAX_FRAME, MAX_VALUE, Refusal, deadline_ms, frame, lp
from membus_atomic import Ack, Change, Invalid, Limits, Original, Publication, Refused, Uncertain, capture
import membus_atomic as atomic

LIMITS=Limits(60000, 96*1024*1024, 10000, MAX_FRAME//3)


class Fake:
    def __init__(self, replies):
        self.transport, self.address, self.epoch, self.max_frame = 'pipe', 'authored-pipe', 42, MAX_FRAME
        self.codes, self.max_batch = CODES, MAX_BATCH
        self.max_value = MAX_VALUE
        self.channel, self.replies, self.calls = object(), list(replies), []
    def close(self):
        self.channel=None
    def exchange(self, opcode, payload, deadline):
        self.calls.append((opcode,payload))
        value=self.replies.pop(0)
        if isinstance(value,BaseException):
            raise value
        return value


class WirePeer:
    """Exercise Client.connect and exchange against an in-memory pipe."""
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []
        self.response = b''
        self.closed = False

    def connect(self):
        with patch('membus.Pipe', return_value=self):
            return Client('authored-pipe', transport='pipe')

    def close(self):
        self.closed = True

    def io(self, data, count, deadline, write):
        if write:
            size, major, minor, opcode, request_id = struct.unpack_from('<IHHBQ', data)
            assert (size, major, minor) == (count - 4, 1, 0) and not self.response
            self.calls.append((opcode, data[17:]))
            reply = self.replies.pop(0)
            code, payload = (reply.code, b'') if isinstance(reply, Refusal) else (0, reply)
            self.response = frame(opcode | 128, request_id, struct.pack('<H', code) + payload)
            return count
        chunk, self.response = self.response[:count], self.response[count:]
        return chunk


def hello(page_limit, last_code=25, *, max_value=MAX_VALUE):
    payload = struct.pack('<QIIHBQH', 42, MAX_FRAME, max_value, page_limit, 0, 86400000, last_code)
    for code in range(1, last_code + 1):
        name, transient = CODES[code]
        payload += struct.pack('<HB', code, transient) + lp(name)
    if last_code == 25:
        payload += struct.pack('<HIQQHHQQI', 1, MAX_FRAME, 4096, 16777216, 64, 256, 8388608, 16777216, 100)
        payload += struct.pack('<HIQIQQ', 1, 4096, 8388608, 65536, 100, 86400000)
    return payload


def capture_header(rows, token):
    guard = struct.pack('<I', len(rows)) + atomic._digest(rows) + LIMITS.encode()
    charge = 256 + len('authored') + 4 * (MAX_FRAME // 3) + 4096
    charge += sum(640 + 2 * len(row.key.encode()) + 2 * len(row.value) for row in rows)
    return b'MAW1\0MAS2' + lp('authored') + struct.pack('<QQ', 42, 99999) + guard + struct.pack('<Q', charge) + token


def scan_page(rows, token, next_cursor):
    payload = lp(token) + lp(next_cursor) + struct.pack('<BH', not next_cursor, len(rows))
    for row in rows:
        payload += lp(row.key) + struct.pack('<BQB', 1, row.version, row.ack) + lp(row.value)
    return payload


def view():
    token=struct.pack('<QQ',42,1)
    guard=struct.pack('<I',0)+atomic._digest(())+LIMITS.encode()
    header=b'MAW1\0MAS2'+lp('authored')+struct.pack('<QQ',42,99999)+guard+struct.pack('<Q',256+len('authored')+4*(MAX_FRAME//3)+4096)+token
    page=lp(token)+lp(b'')+struct.pack('<BH',1,0)
    c=Fake([header,page,b''])
    return c,capture(c,'authored',LIMITS,'capture',deadline_ms(5))


class Controls(unittest.TestCase):
    def test_negotiated_pages_complete_and_release_same_token(self):
        token = struct.pack('<QQ', 42, 1)
        rows = tuple(atomic.Row(f'row/{n:04}', n + 1, Ack.FLUSH, bytes([n]))
                     for n in range(MAX_BATCH + 1))
        for page_limit in (1, 2, MAX_BATCH):
            with self.subTest(page_limit=page_limit):
                cursors = [b'']
                pages = []
                for start in range(0, len(rows), page_limit):
                    end = start + page_limit
                    cursors.append(struct.pack('<I', end) if end < len(rows) else b'')
                    pages.append(scan_page(rows[start:end], token, cursors[-1]))
                peer = WirePeer([hello(page_limit), capture_header(rows, token), *pages, b''])
                client = peer.connect()
                self.assertEqual(client.max_batch, page_limit)
                deadline = deadline_ms(5)
                view = capture(client, 'authored', LIMITS, 'read', deadline)
                self.assertEqual(view.rows, rows)
                self.assertEqual((view.epoch, view.expires), (42, 99999))
                self.assertEqual([op for op, _ in peer.calls], [1, 30] + [11] * len(pages) + [13])
                for (_, request), cursor in zip(peer.calls[2:-1], cursors[:-1]):
                    reader = Decoder(request)
                    self.assertEqual(reader.unpack('<Q'), (deadline,))
                    self.assertEqual((reader.blob(), reader.blob()), (b'read', b'authored'))
                    self.assertEqual((reader.blob(), reader.blob()), (token, cursor))
                    self.assertEqual(reader.unpack('<HI'), (page_limit, MAX_FRAME))
                    self.assertEqual(reader.blob(), b'')
                    self.assertEqual(reader.pos, len(request))
                release = Decoder(peer.calls[-1][1])
                release.unpack('<Q')
                self.assertEqual((release.blob(), release.blob(), release.blob()), (b'read', b'authored', token))
                self.assertEqual(release.pos, len(release.data))
                self.assertEqual(peer.replies, [])

    def test_bad_or_missing_hello_capability_sends_no_capture(self):
        for response, error in [(hello(0), 'invalid scan page limit'),
                                (hello(MAX_BATCH + 1), 'invalid scan page limit'),
                                (hello(1, 19), 'scans unavailable'),
                                (hello(1, 21), 'scans unavailable')]:
            with self.subTest(error=error, response=response[:27]):
                peer = WirePeer([response, b'\0'])
                client = peer.connect()
                with self.assertRaisesRegex(Refused, 'Expired'):
                    capture(client, 'authored', LIMITS, 'read', 1)
                with self.assertRaisesRegex(Invalid, error):
                    capture(client, 'authored', LIMITS, 'read', deadline_ms(5))
                self.assertEqual([op for op, _ in peer.calls], [1])
                self.assertIsNone(client.get('authored', 'legacy'))
                self.assertEqual([op for op, _ in peer.calls], [1, 2])
        peer = WirePeer([hello(1)[:17]])  # Truncated within max_batch.
        with self.assertRaisesRegex(ValueError, 'truncated reply'):
            peer.connect()
        self.assertEqual([op for op, _ in peer.calls], [1])
        self.assertTrue(peer.closed)

    def test_excessive_continuation_count_refuses_and_releases_original(self):
        token = struct.pack('<QQ', 42, 1)
        rows = tuple(atomic.Row(key, 1, Ack.OS, b'value') for key in ('a', 'b', 'c'))
        peer = WirePeer([hello(1), capture_header(rows, token), scan_page(rows[:1], token, b'next'),
                         scan_page(rows[1:], token, b''), Refusal(20)])
        client = peer.connect()
        with self.assertRaisesRegex(Invalid, 'incomplete atomic page') as error:
            capture(client, 'authored', LIMITS, 'read', deadline_ms(5))
        self.assertEqual([op for op, _ in peer.calls], [1, 30, 11, 11, 13])
        self.assertTrue(peer.calls[-1][1].endswith(lp(token)))
        self.assertTrue(any('capture release failed: cursor_expired' in note
                            for note in error.exception.__notes__))
        self.assertTrue(peer.closed)

    def test_whole_view_original_and_ack(self):
        c,v=view()
        o=v.prepare('original',Ack.FLUSH,[Change('a',b'A'),Change('b',b'B')])
        self.assertEqual(o.raw,Original.from_bytes(o.raw).raw)
        c.replies=[b'MAW1\0'+struct.pack('<QB',7,2)]
        p=Publication(c,o)
        self.assertEqual(p.publish(c,deadline_ms(5))['ack'],Ack.FLUSH)
        self.assertEqual([op for op,_ in c.calls],[30,11,13,29])

    def test_burst_bounds_and_duplicate_targets(self):
        _,v=view()
        changes=[Change(f'row/{n:016}',b'x'*(59499 if n==1533 else 1039 if n%2 else 1311)) for n in range(1,3066)]
        o=v.prepare('burst',Ack.FLUSH,changes)
        self.assertLess(len(o.raw),MAX_FRAME//3)
        with self.assertRaises(Invalid):v.prepare('dup',Ack.FLUSH,[Change('a',b'A'),Change('a',b'B')])
        with self.assertRaises(Refused):v.prepare('over',Ack.FLUSH,[Change('huge',b'x'*(4*1024*1024)),Change('huge2',b'x'*(4*1024*1024))])

    def test_original_rejects_every_truncated_prefix_and_tail(self):
        _,v=view();raw=v.prepare('o',Ack.OS,[Change('a',b'\0opaque\xff')]).raw
        for n in range(len(raw)):
            with self.assertRaises((Invalid,ValueError)):Original.from_bytes(raw[:n])
        with self.assertRaises(Invalid):Original.from_bytes(raw+b'\0')

    def test_original_reserved_namespace_and_bad_header_refuse(self):
        for ns in ('__membus_policy_v1','__membus_timers_v1'):
            raw=b'MAR2'+lp(ns)+lp('op')+struct.pack('<QQBI',42,99999,2,0)+atomic._digest(())+LIMITS.encode()+struct.pack('<H',1)+lp('key')+b'\0'+lp(b'value')
            with self.assertRaises(Invalid):Original.from_bytes(raw)
        c=Fake([b'MAW1\0MAS2'])
        with self.assertRaises(Invalid):capture(c,'authored',LIMITS,'read',deadline_ms(5))
        self.assertIsNone(c.channel)

    def test_uncertainty_survives_later_refusal_and_exact_success(self):
        c,v=view();o=v.prepare('lost',Ack.FLUSH,[Change('a',b'A')]);p=Publication(c,o)
        c.replies=[ConnectionError('reply lost')]
        with self.assertRaises(Uncertain):p.publish(c,deadline_ms(5))
        self.assertEqual(len(p.uncertainties),1);self.assertIsNone(c.channel)
        successor=Fake([b'MAW1\x04'+struct.pack('<H',8)])
        with self.assertRaises(Refused):p.publish(successor,deadline_ms(5))
        self.assertEqual(len(p.uncertainties),1)
        successor.replies=[b'MAW1\0'+struct.pack('<QB',9,2)]
        self.assertEqual(p.publish(successor,deadline_ms(5))['version'],9)
        self.assertEqual(p.original.raw,o.raw);self.assertEqual(len(p.uncertainties),1)

    def test_value_preflight_keeps_exact_uncertain_original_on_successor(self):
        initial, view_ = view()
        original = view_.prepare('value-original', Ack.FLUSH, [
            Change('remove', None), Change('replace', b'1234567'), Change('insert', b'1234')])
        publication = Publication(initial, original)
        initial.replies = [TimeoutError('after send')]
        with self.assertRaises(Uncertain) as error:
            publication.publish(initial, deadline_ms(5))
        self.assertIs(error.exception.original, original)
        with self.assertRaises(AttributeError):
            publication.original = view_.prepare('changed', Ack.OS, [Change('replace', b'x')])
        narrow_peer = WirePeer([hello(1, max_value=3)])
        narrow = narrow_peer.connect()
        with self.assertRaisesRegex(Refused, 'Resource') as error:
            publication.publish(narrow, deadline_ms(5))
        self.assertEqual(error.exception.resource, 4)
        self.assertEqual([op for op, _ in narrow_peer.calls], [1])
        self.assertEqual(len(publication.uncertainties), 1)
        wide = Fake([b'MAW1\0' + struct.pack('<QB', 31, 2)])
        receipt = publication.publish(wide, deadline_ms(5))
        self.assertEqual(receipt, dict(version=31, ack=Ack.FLUSH, epoch=42))
        self.assertTrue(wide.calls[0][1].endswith(lp(original.raw)))
        self.assertEqual(len(publication.uncertainties), 1)

    def test_capture_transport_failure_closes_but_associated_refusal_does_not(self):
        for failure in (TimeoutError('capture timed out'), ConnectionError('partial capture reply')):
            client = Fake([failure])
            with self.assertRaises(type(failure)) as caught:
                capture(client, 'authored', LIMITS, 'read', deadline_ms(5))
            self.assertIs(caught.exception, failure)
            self.assertIsNone(client.channel)
            self.assertEqual([op for op, _ in client.calls], [30])
        peer = WirePeer([hello(1), Refusal(5), b'\0'])
        client = peer.connect()
        with self.assertRaises(Refusal):
            capture(client, 'authored', LIMITS, 'read', deadline_ms(5))
        self.assertIsNone(client.get('authored', 'ordinary'))
        self.assertEqual([op for op, _ in peer.calls], [1, 30, 2])

    def test_wrong_ack_unknown_status_and_malformed_success_are_uncertain(self):
        for reply in (b'MAW1\0'+struct.pack('<QB',1,1),b'MAW1\xff',b'MAW1\0',b'MAW1\0'+struct.pack('<QB',0,2)):
            c,v=view();p=Publication(c,v.prepare('x',Ack.FLUSH,[Change('a',b'A')]))
            c.replies=[reply]
            with self.assertRaises(Uncertain):p.publish(c,deadline_ms(5))
            self.assertEqual(len(c.calls),4);self.assertIsNone(c.channel)

    def test_restart_and_transport_uncertainty_do_not_retry(self):
        for reply in (b'MAW1\7',b'MAW1\10',Refusal(16),Refusal(17),TimeoutError('after send')):
            c,v=view();p=Publication(c,v.prepare('x',Ack.FLUSH,[Change('a',b'A')]))
            c.replies=[reply]
            with self.assertRaises(Uncertain):p.publish(c,deadline_ms(5))
            self.assertEqual(len(c.calls),4)

    def test_local_bounds_refuse_without_send(self):
        c,v=view();p=Publication(c,v.prepare('x',Ack.FLUSH,[Change('a',b'A')]))
        calls=len(c.calls)
        with self.assertRaises(Refused):p.publish(c,1)
        c.address='other'
        with self.assertRaises(Invalid):p.publish(c,deadline_ms(5))
        self.assertEqual(len(c.calls),calls)

    def test_partial_population_and_repeated_cursor_refuse(self):
        token=struct.pack('<QQ',42,1)
        guard=struct.pack('<I',1)+b'x'*32+LIMITS.encode()
        header=b'MAW1\0MAS2'+lp('authored')+struct.pack('<QQ',42,99999)+guard+struct.pack('<Q',256+len('authored')+4*(MAX_FRAME//3)+4096)+token
        page=lp(token)+lp(b'')+struct.pack('<BH',1,0)
        c=Fake([header,page,b''])
        with self.assertRaises(Invalid):capture(c,'authored',LIMITS,'read',deadline_ms(5))
        self.assertIsNone(c.channel)

    def test_durable_original_new_only_and_restart_unknown(self):
        base=Path(os.environ['MEMBUS_PROVIDER_TEST_ROOT'])
        root=base/f'python-custody-{os.getpid()}-{time.time_ns()}';root.mkdir()
        c,v=view();p=Publication(c,v.prepare('x',Ack.FLUSH,[Change('a',b'bytes')]))
        path=root/'original';p.persist_new(path)
        raw=path.read_bytes()
        with self.assertRaises(FileExistsError):p.persist_new(path)
        q=Publication.restore(c,path)
        self.assertEqual(q.original.raw,p.original.raw);self.assertTrue(q.uncertainties)
        self.assertEqual(path.read_bytes(),raw)
        c.address='wrong'
        with self.assertRaises(Invalid):Publication.restore(c,path)


def native(pipe,root):
    root=Path(root);assert root.is_absolute() and root.is_dir()
    def connect(generation):
        until=time.monotonic()+30
        while not (root/f'ready-{generation}').exists():
            assert time.monotonic()<until,'finite native readiness'
            time.sleep(.01)
        return Client(pipe,transport='pipe',timeout=10)
    c=connect(0)
    v=capture(c,'authored',LIMITS,'capture-0',deadline_ms(20))
    assert len(v.rows)==1 and v.rows[0].key=='rust' and v.rows[0].value==b'rust-original'
    changes=[Change(f'row/{n:016}',b'x'*(59499 if n==1533 else 1039 if n%2 else 1311)) for n in range(1,3066)]
    changes += [Change('sequence',struct.pack('<Q',3065)),Change('source',b'exact-original-source')]
    p=Publication(c,v.prepare('lost',Ack.FLUSH,changes))
    unsent=Publication(c,v.prepare('unsent',Ack.OS,[Change('never',b'no')]))
    p.persist_new(root/'original.custody');raw=(root/'original.custody').read_bytes()
    try:p.publish(c,deadline_ms(20))
    except Uncertain:pass
    else:raise AssertionError('lost native reply was not uncertain')
    assert p.uncertainties
    c=connect(1)
    assert c.max_value == 3
    try:p.publish(c,deadline_ms(20))
    except Refused as e:assert (e.status,e.resource)==('Resource',4)
    else:raise AssertionError('narrow successor sent the uncertain original')
    assert len(p.uncertainties)==1 and c.channel is not None
    c.close()
    c=connect(2)
    known=p.publish(c,deadline_ms(20));assert known['ack']==Ack.FLUSH
    complete=capture(c,'authored',LIMITS,'capture-1',deadline_ms(20));assert len(complete.rows)==3068
    assert dict((r.key,r.value) for r in complete.rows)['sequence']==struct.pack('<Q',3065)
    assert dict((r.key,r.value) for r in complete.rows)['row/0000000000001533']==b'x'*59499
    p2=Publication(c,complete.prepare('after',Ack.OS,[Change('rust',b'newer')]))
    p2.publish(c,deadline_ms(20));c.close()
    c=connect(3)
    q=Publication.restore(c,root/'original.custody');replayed=q.publish(c,deadline_ms(20))
    assert replayed['version']==known['version'] and q.uncertainties
    assert c.get('authored','rust',op_id='read-newer',deadline=deadline_ms(10))['value']==b'newer'
    try:unsent.publish(c,deadline_ms(10))
    except Uncertain as e:assert e.cause.status=='RestartUncertain'
    else:raise AssertionError('unsent old original acquired new authority')
    assert (root/'original.custody').read_bytes()==raw
    c.close();print('native Python burst/reply loss/reconnect/restart/exact custody/neighbors: pass',flush=True)


def native_matrix(pipe, root, limit, mode):
    root, limit = Path(root), int(limit)
    client = Client(pipe, transport='pipe', timeout=10)
    try:
        assert client.max_batch == limit
        view = capture(client, 'authored', LIMITS, 'native-matrix-read', deadline_ms(20))
        assert mode in ('complete', 'value-publish')
        assert [(row.key, row.value, row.ack) for row in view.rows] == [
            (f'key-{n:03}', bytes((n, 19, 0)) if mode == 'value-publish'
             else f'{root.name}:{n}'.encode(), Ack.FLUSH)
            for n in range(129)
        ]
        if mode == 'value-publish':
            assert client.max_value == 3
            rejected = Publication(client, view.prepare('value-too-large', Ack.FLUSH, [
                Change('key-000', b'1234567'), Change('key-001', None), Change('new', b'1234')]))
            try:
                rejected.publish(client, deadline_ms(10))
            except Refused as error:
                assert (error.status, error.resource) == ('Resource', 4)
                assert rejected.uncertainties == [] and client.channel is not None
            else:
                raise AssertionError('oversized publication reached the provider')
            changes = [Change('settled', b'A\0B'), Change('key-002', None)]
        else:
            changes = [Change('settled', root.name.encode())]
        original = view.prepare('native-matrix-original', Ack.FLUSH, changes)
        publication = Publication(client, original)
        publication.persist_new(root/'matrix.custody')
        assert publication.publish(client, deadline_ms(10))['ack'] == Ack.FLUSH
    except Invalid as error:
        assert (mode == 'over-count' and str(error) == 'incomplete atomic page'
                or mode == 'value-read' and str(error) == 'encoded bound')
        assert client.channel is None
    except Refusal as error:
        assert mode == 'release-refused' and error.code == 20
    finally:
        client.close()


def native_stalled_header(pipe, root):
    root = Path(root)
    client = Client(pipe, transport='pipe', timeout=10)
    try:
        try:
            capture(client, 'authored', LIMITS, 'stalled-header', deadline_ms(.25))
        except TimeoutError:
            assert client.channel is None
            (root/'client-timed-out').write_bytes(b'channel closed')
        else:
            raise AssertionError('stalled header did not time out')
        until = time.monotonic() + 12
        while not (root/'fresh-listener').is_file():
            assert time.monotonic() < until
            time.sleep(.01)
        result = client.get('authored', 'current', op_id='fresh-read', deadline=deadline_ms(5),
                            retry_transport=False)
        assert result['value'] == root.name.encode() and result['ack'] == Ack.OS
    finally:
        client.close()


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--native':native(*sys.argv[2:])
    elif len(sys.argv)>1 and sys.argv[1]=='--native-matrix':native_matrix(*sys.argv[2:])
    elif len(sys.argv)>1 and sys.argv[1]=='--native-stalled-header':native_stalled_header(*sys.argv[2:])
    else:unittest.main()
