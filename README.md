# Membus

Membus is a local key/value resident and Rust library. The resident serves opaque
values through `membus.kv/1`, with conditional writes, explicit OS or flush
acknowledgments, bounded retry deduplication, immutable namespace snapshots,
Windows change subscriptions, persisted timer schedules, and whole-chunk
retirement. `membus::client` connects to an existing endpoint;
`membus::indexed` adds declared append-only collections over that client.
The package uses the Rust standard library and has no third-party crate dependencies.

## Build and run

With Rust and Cargo installed, build from the repository root:

```text
cargo build --release --offline --locked
```

Choose a state directory outside the source checkout and an explicit local pipe
name. On Windows, for example (PowerShell):

```powershell
$state = Join-Path $env:LOCALAPPDATA 'membus-state'
.\target\release\membus.exe $state pipe '\\.\pipe\membus-example'
```

The server runs in the foreground. Keep its state directory across restarts.
Another terminal can check transport and heartbeat together:

```powershell
.\target\release\membus.exe health '\\.\pipe\membus-example' --state-dir $state
```

Set `$state` to the same directory in that terminal. Health requires
`--state-dir`, so it always inspects the intended instance. For loopback tests, the server
also accepts `membus STATE_DIR tcp 127.0.0.1:PORT`. Windows named pipes reject
remote clients and grant access to the resident's current user and LocalSystem.
Loopback TCP provides no application authentication. Endpoint selection and
service supervision belong to the deployment; this repository does not register
a service automatically. See [resident operation](docs/RESIDENT.md).

## Native Rust client

Use this package as a path dependency, with the location selected by your build:

```toml
[dependencies]
membus = { path = "../membus" }
```

This Windows example connects to the pipe above, writes an absent key with a
caller-owned operation ID, and reads its committed value:

```rust,no_run
use membus::client::{Ack, Client, Deadline, MutationRequest, Options};
use std::{path::Path, time::{Duration, Instant}};

#[cfg(windows)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Deadline {
        unix_ms: membus::now_ms() + 2_000,
        local: Instant::now() + Duration::from_secs(2),
    };
    let mut client = Client::connect_pipe(
        Path::new(r"\\.\pipe\membus-example"),
        Options::new("example-client", 65_536),
        deadline.local,
    )?;
    let mut attempt = client.prepare(MutationRequest {
        operation_id: "example-create-1".into(),
        namespace: "example".into(),
        key: "item/1".into(),
        value: b"example-create-1: hello".to_vec(),
        ack: Ack::Flush,
        expected: Some(0),
    })?;
    // This one-shot example exits on error and drops its in-memory attempt.
    // An uncertain outcome then requires deterministic-key reconciliation.
    client.mutate(&mut attempt, deadline)?;
    let value = client.get("example", "item/1", "example-read-1", Deadline {
        unix_ms: membus::now_ms() + 2_000,
        local: Instant::now() + Duration::from_secs(2),
    })?;
    println!("{value:?}");
    Ok(())
}
#[cfg(not(windows))]
fn main() {}
```

`Some(0)` means absent, `Some(version)` compares a committed revision, and
`None` selects unconditional PUT. Choose a new operation ID for each intentional
mutation. The example's fixed ID is for its one logical creation. The client
never retries or reconnects automatically. The example exits on a mutation error,
so it does not preserve the Attempt's in-memory retry context. An application
handling recovery must retain the original attempt and uncertain result. If that
context is lost, reconcile the deterministic key and embedded operation identity
before choosing another send; preparing again does not restore the old retry
window. See [Rust client usage and uncertainty](docs/RUST_CLIENT.md).

## Documentation and checks

- [Protocol, acknowledgments, refusals and snapshots](docs/KV1.md)
- [Mutation retry comparators, encoding aliases and resource accounting](docs/RETRY_RETENTION.md)
- [Recovered Flush prerequisites and historical backup limits](docs/RECOVERY_FLUSH.md)
- [Grouped append and flush behavior](docs/GROUPED-FLUSH.md)
- [Change subscriptions and receiver responsibilities](docs/SUBSCRIPTIONS.md)
- [Persisted timer wakes and crash boundaries](docs/TIMERS.md)
- [Retention classes, retirement, replay and legacy import](docs/CHUNK-RETIREMENT.md)
- [Unresolved catalogue append recovery](docs/CATALOGUE-APPEND-RECOVERY.md)
- [Read-only retirement inspection](docs/RETIREMENT-INSPECTION.md)
- [Indexed append-only collections](docs/INDEXED_COLLECTIONS.md)

Rust checks use `cargo test --offline --locked`; Windows listener/forwarder tests
need the explicit fixture inputs described in [RUST_CLIENT.md](docs/RUST_CLIENT.md).
Tests create isolated state and some launch resident processes. Build and test
commands here are usage instructions, not a claim that checks have run on your
machine. `tests/resident_native.rs` supplies native resident process controls;
the former `tests/resident.py` is retired. Historical design and review documents
remain accessible through Git history.

OS acknowledgment covers completed appends and process restart, not power loss.
Flush acknowledgment additionally synchronizes the covering file; actual
persistence still depends on filesystem and hardware behavior. Startup requires
active recovered data-file synchronization before exposing values or retry
outcomes; an on-disk acknowledgment byte alone does not prove achieved Flush.
Already imported read-only legacy backups retain the historical limits described
in [RECOVERY_FLUSH.md](docs/RECOVERY_FLUSH.md). Snapshots and
change history are bounded. Subscriptions and timer wakes grant no lease,
receiver receipt, or authority to execute an application effect. All committed
values remain resident in memory; cache eviction, warm/cold tiers and replay
index checkpoints are not implemented.
