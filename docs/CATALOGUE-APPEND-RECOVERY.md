# Unresolved catalogue appends

Catalogue publication requires a flushed frame and resolution of its append marker. Before writing
the frame, the writer creates and flushes `catalogue.log.append-pending`, containing a versioned
boundary record. A failed append restores and flushes its previous boundary before removing that
marker. If boundary restoration or marker resolution fails, further appends are fenced.

Startup refuses catalogue replay while the marker exists, including when its contents are incomplete.
It preserves the catalogue and chunk files and records the refusal in the replay report. A complete,
valid trailing frame therefore cannot turn an unresolved failed retirement into permission to delete
its chunk on restart.

An existing owner with known in-memory live state can resolve the fence through the ordinary live-list
rewrite. The replacement is flushed, its handle is prepared and its path published before the marker
is removed. Startup has no such live-state witness and does not remove the marker or infer a safe
boundary. Recovery after that refusal needs an independently established retained-state account; it
is not an automatic repair or a reason to delete the marker manually.

The tests represent complete write followed by sync failure and boundary-recovery failure, partial
markers, live-list recovery, retained chunk bytes on restart refusal, and still-readable values after
a later retirement fails. They exercise process restart and injected I/O error branches. They do not
establish filesystem behavior after power loss, a recovery deadline, or compatibility with an older
binary that does not understand the marker. Do not open unresolved state with such an older binary.

Run the Rust test suite with `cargo test --offline --locked`. `MEMBUS_TEST_OUTPUT` can select an isolated
test output folder. The marker adds a separate flush to each catalogue append; no throughput claim is
made without a measurement for the selected storage and workload.
