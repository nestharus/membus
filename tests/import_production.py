"""T11 (docs/CHUNK-RETIREMENT.md section 7) on a copy of production state: the import, its check, a changed legacy
file, an injected difference left for the previous binary, and the rollback served by the previous binary. Every
server is private (TCP on 127.0.0.1) on a copy under --output; the source state is only read."""
import argparse, hashlib, json, os, pathlib, shutil, socket, stat, struct, subprocess, sys, time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'python'))
from membus import Client, Refusal

GATED = ('__membus_timers_v1', '__membus_policy_v1')
DAY = 86_400_000


def endpoint():
    with socket.socket() as channel:
        channel.bind(('127.0.0.1', 0))
        return '127.0.0.1:' + str(channel.getsockname()[1])


class Server:
    def __init__(self, binary, state, log, flags=(), env=None):
        self.address = endpoint()
        self.log = open(log, 'ab', buffering=0)
        self.process = subprocess.Popen([str(binary), str(state), 'tcp', self.address, *flags], stdout=self.log,
                                        stderr=self.log, env=env)
        for _ in range(1500):
            if self.process.poll() is not None:
                raise RuntimeError('server exited: ' + pathlib.Path(log).read_text(errors='replace'))
            try:
                self.client = Client(self.address, 'tcp', timeout=.1)
                self.client.timeout = 30
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
        self.process.wait(timeout=30)
        self.log.close()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def legacy_shas(folder):
    return {p.name: sha(p) for p in sorted(folder.glob('stripe-*.log'))}


def read_legacy(folder):
    """Each key's latest (version, remove, value) and every record, from the stripe logs; a torn tail stops a file."""
    latest, records = {}, []
    for path in sorted(folder.glob('stripe-*.log')):
        data, at = path.read_bytes(), 0
        while at + 16 <= len(data):
            assert data[at:at + 4] == b'MBK1', (path, at)
            size, complement, crc = struct.unpack_from('<III', data, at + 4)
            assert complement == size ^ 0xffffffff
            if at + 16 + size > len(data):
                break
            body = data[at + 16:at + 16 + size]
            stamp, version, ack, remove = struct.unpack_from('<QQBB', body, 0)
            pos = 18
            fields = []
            for _ in range(3):
                n, = struct.unpack_from('<I', body, pos)
                fields.append(body[pos + 4:pos + 4 + n].decode('utf8'))
                pos += 4 + n
            expected, = struct.unpack_from('<Q', body, pos)
            n, = struct.unpack_from('<I', body, pos + 8)
            value = body[pos + 12:pos + 12 + n]
            ns, key, op_id = fields
            record = dict(at=stamp, version=version, ack=ack, remove=bool(remove), ns=ns, key=key, op_id=op_id,
                          expected=None if expected == 2 ** 64 - 1 else expected, value=value)
            records.append(record)
            if (ns, key) not in latest or latest[(ns, key)]['version'] < version:
                latest[(ns, key)] = record
            at += 16 + size
    return latest, records


def live_counts(latest):
    counts = {}
    for (ns, _), r in latest.items():
        if not r['remove']:
            counts[ns] = counts.get(ns, 0) + 1
    return counts


def check_values(client, latest):
    checked = 0
    keys = [k for k in latest if k[0] not in GATED]
    for ns in sorted({k[0] for k in keys}):
        names = [k[1] for k in keys if k[0] == ns]
        for at in range(0, len(names), 128):
            part = names[at:at + 128]
            for key, value in zip(part, client.get_many(ns, part)):
                r = latest[(ns, key)]
                if r['remove']:
                    assert value is None, (ns, key)
                else:
                    assert value and value['value'] == r['value'] and value['version'] == r['version'], (ns, key, value, r['version'])
                checked += 1
    return checked


def copy_state(source, to):
    to.mkdir(parents=True)
    for name in ['epoch', *sorted(p.name for p in source.glob('stripe-*.log'))]:
        if (source / name).exists():
            shutil.copyfile(source / name, to / name)


def policies(latest, expiring=()):
    """The first policies: the machine tools' quota, slots and capacity families; the caller-named test namespaces
    expire(1 d); the lease namespaces latest until the combined qualification. Everything else stays forever.
    expiring holds exact namespace names, or prefixes ending in '*'."""
    lines = ['# machine tools: quota, slots and capacity',
             'quota\tevent/\texpire:259200000', 'slots\tevent/\texpire:259200000', 'capacity\tevent/\texpire:259200000',
             'quota-test\t\texpire:259200000',
             'quota\tstate/\tlatest', 'quota\topen\tlatest', 'quota\tresets\tlatest', 'quota\ttokens/\tlatest',
             'quota\tfit/\tlatest', 'slots\t\tlatest',
             '# membus-leases', 'lease-claims\t\tlatest', 'lease-control\t\tlatest',
             '# test namespaces']
    for ns in sorted({k[0] for k in latest}):
        if any(ns.startswith(e[:-1]) if e.endswith('*') else ns == e for e in expiring):
            lines.append(f'{ns}\t\texpire:{DAY}')
    return '\n'.join(lines) + '\n'


def clear_readonly(path):
    os.chmod(path, stat.S_IWRITE | stat.S_IREAD)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--binary', required=True)
    p.add_argument('--old-binary', required=True)
    p.add_argument('--state', required=True, help='the production state folder, read only')
    p.add_argument('--output', required=True)
    p.add_argument('--expire-namespaces', nargs='*', default=[],
                   help="test namespaces in the source state that get expire(1 d): exact names or prefixes ending in '*'")
    args = p.parse_args()
    out = pathlib.Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    base = out / 'base'
    copy_state(pathlib.Path(args.state), base)
    latest, records = read_legacy(base)
    original = legacy_shas(base)
    model_live = live_counts(latest)
    results = dict(source=args.state, legacy_files={n: (base / n).stat().st_size for n in original}, legacy_sha256=original,
                   records=len(records), keys=len(latest), live=sum(model_live.values()), namespaces=model_live)
    policy_text = policies(latest, args.expire_namespaces)
    (out / 'import-policies.txt').write_text(policy_text, encoding='utf8')

    # Before: the previous binary serves the copy as the model reads it.
    shutil.copytree(base, out / 'A')
    old = Server(args.old_binary, out / 'A', out / 'A-old.txt', ['--heartbeat-ms', '0'])
    try:
        results['before_checked_keys'] = check_values(old.client, latest)
    finally:
        old.stop()

    # The import on copy B, then a further restart.
    shutil.copytree(base, out / 'B')
    (out / 'B' / 'import-policies.txt').write_text(policy_text, encoding='utf8')
    now = int(time.time() * 1000)
    retry = next((r for r in sorted(records, key=lambda r: -r['at'])
                  if r['ns'] not in GATED and now - r['at'] < DAY - 3_600_000
                  and latest[(r['ns'], r['key'])]['version'] != r['version']), None)
    for run in ('import', 'restart'):
        server = Server(args.binary, out / 'B', out / f'B-{run}.txt', ['--heartbeat-ms', '1000'])
        try:
            c = server.client
            checked = check_values(c, latest)
            h = c.health2()
            counts = {k[len('namespace.'):]: int(v) for k, v in h.items() if k.startswith('namespace.') and not k.endswith('__membus_policy_v1')}
            assert counts == model_live, (counts, model_live)
            entry = dict(epoch=c.epoch, checked_keys=checked, health2_retained=int(h['retained_bytes']),
                         retained_by_class={k: int(v) for k, v in h.items() if k.startswith('retained_bytes.')},
                         storage_error=h['storage_error'])
            assert h['storage_error'] == 'false' and h['read_only'] == 'false', h
            if retry:
                before = c.health()['bytes_written']
                if retry['remove']:
                    o = c.remove(retry['ns'], retry['key'], retry['expected'], ack=retry['ack'], op_id=retry['op_id'])
                else:
                    o = c.put(retry['ns'], retry['key'], retry['value'], ack=retry['ack'], expected=retry['expected'], op_id=retry['op_id'])
                assert o['version'] == retry['version'], (o, retry['version'])
                assert c.health()['bytes_written'] == before
                entry['retry'] = dict(ns=retry['ns'], key=retry['key'], age_ms=now - retry['at'], version=o['version'], bytes_written=0)
            results[f'B_{run}'] = entry
        finally:
            server.stop()
        replay = json.loads((out / 'B' / 'replay.json').read_text())
        results[f'B_{run}']['replay_import'] = replay['import']
        results[f'B_{run}']['replay_ms'] = replay['duration_ms']
    epoch = results['B_import']['epoch']
    legacy_dir = out / 'B' / f'legacy-{epoch}'
    assert legacy_shas(legacy_dir) == original, 'legacy files changed by the import'
    assert not any((out / 'B').glob('stripe-*.log')) and not (out / 'B' / 'chunks.import').exists()
    assert all(not os.access(p, os.W_OK) for p in legacy_dir.glob('stripe-*.log'))
    results['B_chunk_bytes_by_class'] = {d.name: sum(f.stat().st_size for f in d.glob('*.log')) for d in (out / 'B' / 'chunks').iterdir() if d.is_dir()}
    # A changed legacy file starts read_only with storage_error.
    victim = legacy_dir / 'stripe-0.log'
    clear_readonly(victim)
    with open(victim, 'ab') as f:
        f.write(b'\0')
    server = Server(args.binary, out / 'B', out / 'B-changed.txt', ['--heartbeat-ms', '0'])
    try:
        h = server.client.health2()
        assert h['storage_error'] == 'true' and h['read_only'] == 'true', h
        try:
            server.client.put('ledger', 'after-change', b'x')
            raise AssertionError('a read_only store admitted a put')
        except Refusal as error:
            assert error.code == 10
        results['changed_legacy_file'] = 'storage_error and read_only; put refused 10'
    finally:
        server.stop()

    # An injected difference: refused, the legacy logs untouched, and the previous binary serves them.
    shutil.copytree(base, out / 'C')
    (out / 'C' / 'import-policies.txt').write_text(policy_text, encoding='utf8')
    env = dict(os.environ, MEMBUS_TEST_IMPORT_DIFFERENCE='1')
    refused = subprocess.run([args.binary, str(out / 'C'), 'tcp', endpoint(), '--heartbeat-ms', '0'], capture_output=True,
                             text=True, timeout=120, env=env)
    assert refused.returncode != 0 and 'import refused' in refused.stderr, refused
    assert legacy_shas(out / 'C') == original and not (out / 'C' / 'chunks.import').exists()
    assert 'missing from the import' in (out / 'C' / 'replay.json').read_text()
    old = Server(args.old_binary, out / 'C', out / 'C-old.txt', ['--heartbeat-ms', '0'])
    try:
        results['difference'] = dict(exit=refused.returncode, stderr=refused.stderr.strip()[-300:],
                                     previous_binary_checked_keys=check_values(old.client, latest))
    finally:
        old.stop()

    # The rollback: import, one write after the switch, then the steps of 5.6 (3); the previous binary serves it.
    shutil.copytree(base, out / 'D')
    (out / 'D' / 'import-policies.txt').write_text(policy_text, encoding='utf8')
    server = Server(args.binary, out / 'D', out / 'D-new.txt', ['--heartbeat-ms', '0'])
    try:
        d_epoch = server.client.epoch
        server.client.put('ledger', 'written-after-switch', b'x')
    finally:
        server.stop()
    (out / 'D' / 'chunks').rename(out / 'D' / f'chunks.rolled-back-{d_epoch}')
    for f in sorted((out / 'D' / f'legacy-{d_epoch}').glob('stripe-*.log')):
        target = out / 'D' / f.name
        f.rename(target)
        clear_readonly(target)
    assert legacy_shas(out / 'D') == original
    old = Server(args.old_binary, out / 'D', out / 'D-old.txt', ['--heartbeat-ms', '0'])
    try:
        checked = check_values(old.client, latest)
        assert old.client.get('ledger', 'written-after-switch') is None
        results['rollback'] = dict(previous_binary_checked_keys=checked, write_after_switch='absent, as 5.6 (3) says')
    finally:
        old.stop()
    (out / 'results.json').write_text(json.dumps(results, indent=1, default=str), encoding='utf8')
    print(json.dumps({k: v for k, v in results.items() if k not in ('namespaces', 'legacy_sha256')}, default=str))


if __name__ == '__main__':
    main()
