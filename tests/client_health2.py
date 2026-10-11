"""T14 (docs/CHUNK-RETIREMENT.md section 7): the Python client's health2() and get_status() parse, an older server's
`unsupported` maps to NotAvailable, and the existing calls are unchanged. Also reads the server's heartbeat (T9's size
on a real beat) and its replay record. Private TCP servers on isolated state; nothing touches the resident."""
import argparse, json, pathlib, socket, subprocess, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'python'))
from membus import Client, NotAvailable, Refusal


def endpoint():
    with socket.socket() as channel:
        channel.bind(('127.0.0.1', 0))
        return '127.0.0.1:' + str(channel.getsockname()[1])


class Server:
    def __init__(self, binary, state, log, flags=()):
        self.address = endpoint()
        self.log = open(log, 'ab', buffering=0)
        self.process = subprocess.Popen([str(binary), str(state), 'tcp', self.address, *flags], stdout=self.log, stderr=self.log)
        for _ in range(500):
            if self.process.poll() is not None:
                raise RuntimeError('server exited: ' + pathlib.Path(log).read_text())
            try:
                self.client = Client(self.address, 'tcp', timeout=.1)
                self.client.timeout = 10
                return
            except (OSError, ConnectionError):
                time.sleep(.01)
        self.stop()
        raise RuntimeError('server not ready')

    def stop(self):
        if hasattr(self, 'client'):
            self.client.close()
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        self.log.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--binary', required=True)
    p.add_argument('--old-binary', help='a server built before opcodes 26-27, to check NotAvailable')
    p.add_argument('--output', required=True)
    args = p.parse_args()
    out = pathlib.Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    results = {}
    server = Server(args.binary, out / 'state', out / 'server.txt', ['--heartbeat-ms', '100', '--dedup-cap', '10', '--quota', '10'])
    try:
        c = server.client
        # Existing calls, unchanged in shape.
        first = c.put('ledger', 'a', b'one', op_id='a1')
        assert set(first) == {'version', 'ack'} and first['ack'] == 1
        assert c.get('ledger', 'a')['value'] == b'one'
        assert [v and v['value'] for v in c.get_many('ledger', ['a', 'missing'])] == [b'one', None]
        assert c.put('ledger', 'a', b'one', op_id='a1') == first, 'dedup'
        removed = c.remove('ledger', 'a', first['version'])
        assert removed['version'] > first['version']
        outcomes = c.batch('ledger', [{'op_id': f'b{i}', 'key': f'b{i}', 'value': b'x'} for i in range(6)])
        assert [o['code'] for o in outcomes] == [0] * 6
        health = c.health()
        assert set(health) == {'bytes_written', 'flush_calls', 'keys', 'namespace_keys', 'namespace_quota', 'index_cap',
                               'high_water', 'namespace_high_water'}
        assert health['namespace_keys'] == 6 and health['namespace_high_water'] == 0
        # health2 parses; its fields follow the store.
        h = c.health2()
        for key in ('state_bytes', 'state_limit_bytes', 'state_high_water', 'read_only', 'storage_error',
                    'expiry_degraded', 'dedup_entries', 'dedup_cap', 'dedup_high_water', 'keys', 'index_cap',
                    'quota', 'namespace_high_water', 'chunks_open', 'chunks_closed', 'retained_bytes', 'pinned_bytes',
                    'pinned_high_water', 'retired_chunks', 'retired_bytes', 'retire_blocked',
                    'retire_blocked_oldest_ms', 'retire_blocked_high_water', 'namespace.ledger'):
            assert key in h, key
        assert h['namespace.ledger'] == '6' and h['quota'] == '10' and h['dedup_cap'] == '10'
        assert h['dedup_entries'] == '8' and h['dedup_high_water'] == 'true', h
        assert h['read_only'] == 'false' and int(h['state_bytes']) > 0
        assert h['namespace_high_water'] == 'false'
        c.put('ledger', 'b6', b'x', op_id='b6')
        assert c.health2()['namespace_high_water'] == 'true'
        # get_status parses: live, missing (a removed key is missing, not expired).
        assert c.get_status('ledger', 'b0') == {'status': 'live', 'version': outcomes[0]['version']}
        assert c.get_status('ledger', 'a')['status'] == 'missing'
        assert c.get_status('ledger', 'never')['status'] == 'missing'
        # The policy namespace is gated for clients.
        try:
            c.put('__membus_policy_v1', 'ledger\0x', b'latest')
            raise AssertionError('policy namespace accepted a client put')
        except Refusal as error:
            assert error.code == 3
        # A real beat: the flags heartbeat_warn reads, and its size.
        time.sleep(.3)
        beat_text = (out / 'state' / 'heartbeat.json').read_text()
        beat = json.loads(beat_text)
        assert isinstance(beat['state_high_water'], bool)
        for flag in ('read_only', 'storage_error', 'expiry_degraded', 'dedup_high_water',
                     'namespace_high_water', 'pinned_high_water', 'retire_blocked_high_water'):
            assert beat[flag] in (0, 1) and not isinstance(beat[flag], bool), flag
        assert beat['dedup_high_water'] == 1 and beat['namespace_high_water'] == 1 and beat['read_only'] == 0
        assert len(beat_text.encode()) < 512, len(beat_text.encode())
        results['heartbeat_bytes'] = len(beat_text.encode())
        results['health2_fields'] = len(c.health2())
    finally:
        server.stop()
    replay = json.loads((out / 'state' / 'replay.json').read_text())
    assert replay['epoch'] == 1 and replay['storage_errors'] == []
    summary = [line for line in (out / 'server.txt').read_text().splitlines() if line.startswith('replay epoch=')]
    assert summary, 'membus.log replay summary line missing'
    results['replay_summary'] = summary[0]
    if args.old_binary:
        old = Server(args.old_binary, out / 'old-state', out / 'old-server.txt', ['--heartbeat-ms', '0'])
        try:
            for call in (lambda: old.client.health2(), lambda: old.client.get_status('ledger', 'a')):
                try:
                    call()
                    raise AssertionError('older server answered a new opcode')
                except NotAvailable as error:
                    assert 'not available on this server' in str(error)
            old.client.put('ledger', 'k', b'v')
            assert old.client.get('ledger', 'k')['value'] == b'v'
            results['older_server'] = 'health2 and get_status raise NotAvailable; existing calls work'
        finally:
            old.stop()
    (out / 'results.json').write_text(json.dumps(results, indent=1), encoding='utf8')
    print(json.dumps(results))


if __name__ == '__main__':
    main()
