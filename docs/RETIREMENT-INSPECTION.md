# Read-only retirement inspection

`membus retirement-status PIPE` reads the existing HEALTH2 operation and emits
five named numeric fields: retire_blocked, retired_chunks, read_only,
storage_error and expiry_degraded. It uses the maintained bounded exchange and
negotiated frame limit. Missing, duplicate or malformed selected fields refuse
the command; arbitrary namespace text is never emitted as metric lines.
The last three fields are validated as wire booleans and projected to 0 or 1.
The endpoint must use the explicit local named-pipe prefix, and the opened handle
must have pipe type before any request is sent. Ordinary files and remote paths
are refused without writing HELLO bytes.

`membus catalogue-state STOPPED_STATE_DIR ABSOLUTE_CHUNK_PATH` uses the public
catalogue decoder and chunk-path association to print the latest recorded state
of that exact chunk. Both paths must be absolute. The writer must be stopped
and waited before inspection: this command does not acquire a writer lock, and
a live catalogue read is not a committed snapshot. It opens no Store and does
not recover, truncate, rewrite, delete or set policy. Unknown chunks, absent
catalogues, torn snapshots and unresolved appends refuse. Compaction may remove
discharged records; unknown never means Retired.

A Retiring record establishes initiation, while actual retire_blocked metrics
establish failed deletion within their observed population. Neither proves a
terminal record. Observe file disappearance and an actual Retired publication
separately. These read-only observations grant no lease or receiver authority.
