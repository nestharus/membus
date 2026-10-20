# Component ownership

The crate root keeps the public vocabulary and exports, the `Store` facade, and
startup/shutdown assembly. Implementation modules follow owners that already
have distinct state, execution lifetimes or protocols.

| Module | Component | Runtime evidence |
| --- | --- | --- |
| `append_owner` | One append owner per stripe | `Append` inbox, stripe binding, routed record writes, completion sender, thread and join handle. |
| `flush_owner` | Shared flush owner and its producer ingress | `FlushWait` protocol, ordered arrival classification, pending queue, last-sync time, coverage by sink, failure fences by stripe, receiver thread and join handle. |
| `test_seam` | Instrumentation at named owner boundaries | Append and notice delays, post-append failure, arrival-classification callback, enqueue notice, selected-file sync gate/failure, coverage observation and kill checkpoints. Public configuration remains on `Config`. |
| `codec` | Wire and durable payload codec, opcode dispatch | Bounded `Cursor`, frame/reply protocol, MBK1 payload representation and translation from request opcodes to Store operations. |
| `log_io` | File synchronization and complete record append | Retained file handles, write accounting, partial-write fencing and the feature-selected sync witness. |
| `routing` | `ChunkRouter` and class reservations | Open chunks per stripe/class, sink table, rotation, close coverage, fences and reservation lifetime; `Route` carries sink identity and extent between owners. |
| `replay` | `Replayed`, `Legacy`, `ImportWriter` and startup recovery | Reconstructed values/retries, catalogue records, truncation evidence, replacement output files, checked import/switch protocol and recovered-data sync prerequisites. |
| `maintenance` | `Maint` | Shared retirement dependencies, horizons, retained-key accounting, summaries, catalogue maintenance and a periodic thread with explicit stop/join. |
| `snapshots` | Snapshot registry and scans | Epoch-bound tokens, namespace rows, retained-resource charge, query-bound paging cursor and an independent TTL reaper. |
| `heartbeat` | Health field and resident reporting adapter | `Health2` key/value representation and the heartbeat line consumed by fleet reporting. |
| `store` | Admission, publication and the Store operation facade | Pending/committed metadata, namespace/quota accounting, retry outcomes, class/policy ordering and mutation completion. |
| `chunks` | Durable catalogue and chunk bookkeeping | Catalogue framing, class policies, chunk state table, holders, summaries and replay report format. |
| `signals` | Subscription hub | Subscription registry, bounded change history, delivery state and a worker thread. |
| `timers` | Persisted schedules and timer owner | Restored schedule/progress state, control protocol, wake producer and explicit owner shutdown. |
| `client` | Native Rust client | Connection/transport state, negotiation, caller-owned attempts and deadlines. |
| `client::m3` (`client_m3.rs`) | Typed scans and subscriptions | Snapshot/cursor association, scan pages, change frames and subscription tokens/status. |
| `client::timer_client` (`client_timers.rs`) | Typed timer client | Timer attempts, generation/epoch association, schedule/envelope framing and uncertainty. |
| `client::listener` (`client_listener.rs`, Windows) | Change listener | Owned receiving pipe, buffered frame state, deadlines, cancellation and reset/drain lifecycle. |
| `indexed` | Append-only collections over the client | Declared schema, deterministic keys, append receipt and scan protocol. |
| `pipe_io` (Windows) | Resident pipe transport | Owned pipe/event handles, overlapped I/O, cancellation and drain protocol. |

The resident executable retains transport acceptance, command handling and the
heartbeat producer; its `qualification` module retains stopped-state inspection.

The flush algorithm stays in one receive/run procedure: settlement, gathering,
group selection, synchronization, coverage/failure update and completion remain
ordered together. The append receive/write/completion sequence also stays whole.
The owner types hold their receiver and operating state and move into their
threads. `AppendHandle` and `FlushHandle` retain the controls used by Store;
`FlushIngress` owns producer-side sender, outstanding accounting and the
arrival mutex. Shutdown still stops the reaper and maintenance worker, closes all
append inboxes, joins all append owners, then joins the flush owner.

`ChunkRouter`, `ClassReservation`, `Maint`, `Replayed`, `Legacy`, `ImportWriter`,
and the snapshot/charge types retain their original algorithms and state shapes.
Recovery keeps validation, evidence publication, truncation, synchronization and
catalogue repair in their original order. Import construction, replay comparison
and directory switching remain one procedure. Retirement retains intent,
index/horizon changes, deletion and partial-failure accounting together. Snapshot
capture and pagination each retain their complete accounting and error paths.
Mutation admission through publication/completion and policy projection remain
cohesive Store operations. None of these procedures is divided into stage
wrappers for file size.

Existing unit test module names and bodies remain unchanged. Their files live
beside their components and retain the root test namespace so external test
selection and reported test identities stay stable.
