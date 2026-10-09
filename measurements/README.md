# Membus measurement suite

Purpose: F-E6-2 establishes Windows append/flush and Python/Rust IPC costs before membus chooses its log acknowledgement and client transport. Account: D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/ledger.json. These are probes, not a membus server or client API.

The Rust executable has no crates beyond the standard library and declares Windows kernel32 named-pipe functions directly. Python and PowerShell scripts use only their standard installations. Build and run through lineup; record the slots and co-tenants. Use the repository's ignored `target` directories and an ignored raw-output directory. The output paths must already exist.

```powershell
lineup.cmd run --uses cpu=2,mem=1,disk:d -- cargo build --offline --release --manifest-path D:/projects/membus/trunk/measurements/Cargo.toml
lineup.cmd run --uses cpu=2,mem=1,disk:d -- powershell -NoProfile -File D:/projects/membus/trunk/measurements/run-probe.ps1 -Kind flush -Output D:/projects/membus/trunk/measurements/raw/flush-D.jsonl -ProbePath D:/projects/membus/trunk/measurements/raw/probe-D.bin
lineup.cmd run --uses cpu=4,mem=1 -- powershell -NoProfile -File D:/projects/membus/trunk/measurements/run-probe.ps1 -Kind ipc -Output D:/projects/membus/trunk/measurements/raw/ipc.jsonl -Count 2000
lineup.cmd run --uses cpu=1 -- powershell -NoProfile -File D:/projects/membus/trunk/measurements/baseline.ps1 -Output D:/projects/membus/trunk/measurements/raw/baseline.jsonl -Seconds 600
python D:/projects/membus/trunk/measurements/policy.py
```

Flush mode emits 4/16/64 KiB append writes for unflushed, flush, write-through-only and write-through-plus-flush modes, plus one flush per batch of 1/8/64/512 records at 2253 bytes each. It extends files and includes NTFS allocation and metadata costs. Every unflushed control has one untimed drain recorded as a separate row. `flushes` counts explicit application FlushFileBuffers calls, not the device's internally generated flushes or FUA requests. `bytes_written` counts application data, excluding metadata and NAND amplification. File reuse truncates earlier data; clean up the named temporary file through the cognition runtime's `python -m cognition.remove ABSOLUTE_PATH` after the run.

IPC runs the same Rust server build for both clients. There are 36 cells across pipe/TCP, 64/2253/51200 bytes, 1/8/32 connections, and Rust/Python. Python clients are separate spawned processes; Rust clients are threads. Each connection performs 16 warmup echoes before 2000 timed echoes. Every echo contains a client/request identifier. TCP is blocking with TCP_NODELAY. Report aggregate closed-loop throughput and empirical latency percentiles; there is no storage, framing, protocol negotiation, key lookup or signal delivery cost here. Multi-process startup is outside the timed interval but appears in the recorded co-tenant process counts.

`run-probe.ps1` records executable/source hashes, commit and versions, and before/during/after lineup/process/PhysicalDisk snapshots. Its five-minute deadline stops its launched process tree. IPC servers also close in the Python orchestrator's normal and exceptional paths. The snapshot loop itself adds CPU work and samples approximately every two seconds. Baselines sample 5-second cooked averages for at least ten minutes, with lineup snapshots about once per minute. Two baselines and two probe passes belong at different times; retained outputs state their actual UTC windows.

Optional DiskSpd runs use `-Kind diskspd-small` or `diskspd-sequential`, plus `-DiskSpdExe ABSOLUTE_PATH`, after trustcheck and an exact match to the published official-release SHA256. Their 15-second, QD1, write-through runs use a 32 MiB target but can issue gigabytes of writes; queued disk slots and exact XML byte totals matter. Small writes are random 4 KiB; sequential writes are 64 KiB. This is observed QD1 capacity with the recorded co-tenants. A trusted Microsoft executable does not imply a passing trustcheck threshold; retain that tool's actual label and failing checks.

`summarize.py --raw ABSOLUTE_RAW_DIRECTORY --output ABSOLUTE_RESULT_PATH` renders two completed definitive runs and baselines to a Markdown table. [RESULTS.md](RESULTS.md) links the 2026-10-09 evidence and states the remaining uncertainty about T700 power-loss protection and actual device persistence. No power-cut test was commissioned or performed.
