# Chunk retirement: plan (F-E6-52)

**Status:** plan for lane 87, written by claude6-eco's e17 Act on 2026-10-11. No code yet. The build follows in a
later Act after lane 87's answers (section 12). Account: claude6-eco's ledger
(`D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/ledger.json`), frontier F-E6-52.
Working records are in that loop's `runs/e17/act-work/F-E6-52/`. This version takes the dispositions of the plan's observe-class read
(`runs/e17/act-work/F-E6-52/review/DISPOSITIONS.md`, O1-O21). Measurement scripts and their outputs are in
`measurements/f_e6_52/`.

**Basis.** This plan builds on these sources:
- The design record `D:/dev/handoff/memory-ecosystem.md`, sha256 `074d22267522e57ad4ed32e1d95aef64833358a41fc9908e1bf958d99d2ffcb6`.
- [PLAN.md](PLAN.md) section 5 ("Cache bounds and expiring chunks", with "Tiering alternatives and first slice"), section 4 and section 12 (O3/O4/O7, O8, O9).
- [KV1.md](KV1.md) section 7 and [RESIDENT.md](RESIDENT.md) "State growth and upgrades", all at main 51d6553.

## 1. What it does for its first clients

Today membus's four stripe logs only grow (RESIDENT.md "State growth and upgrades"). A remove appends a tombstone and
keeps the key's slot (KV1.md section 7). F-E6-48's private run showed the cost. 1,000 puts filled 1,000 slots and
289,000 bytes. Overwrites grew the logs to 584,000 bytes, and removes grew them to 680,000. The slots stayed at 1,000
throughout.

This slice does three things:
- **Retires whole chunk files** for namespaces whose owner marks them expiring or latest-version-only.
- **Frees key slots.** A remove frees its slot when it commits, and a retired chunk frees the slots of the keys whose
  latest version it held.
- **Puts state and capacity limits in health and the heartbeat,** where lane 88's watchers can read them. It also
  writes a replay record at every start.

The first clients are lane 88's writers: quota, slots, the token meter, calibration and capmon. They wrote 94.5% of
the last 24 h's bytes (4,445,750 of 4,703,508 B; section 2).

### The founder's words this plan follows

The design record's "Founder, next: rotation and expiring records in membus" (verbatim):

> "The rotations allow us to delete entire files. So when we expire we expire "chunks" and we only retire a chunk if
> its newest record has expired. So even if there are old records, they don't get deleted yet. We only delete the
> entire chunk. So we could say that the chunk close time (when it got rotated out) is what we base expiration time
> off of."

Also from the design record:
- Eviction is cache-only: "for memory we don't flush to the drive when it expires in membus. it is already on the
  drive lol.. it is just whether it exists in the cache or not. then we have separate stuff to clean up data."
- The tier passage: "we don't have to use all of the tiers".

claude2's reading in the design record adds that nothing is rewritten or compacted. PLAN.md section 5 holds the same:
"No rewriting or compaction of retained records". The recommended form (section 4) keeps that rule. It departs from that
rule once, for the one-time legacy import (section 5.6). An on-disk format change "requires an explicit migration/backup
plan" (RESIDENT.md, "State growth and upgrades"), and PLAN.md section 5 applies policy changes to new chunks "unless an
explicit migration is commissioned". That import is question (c) to lane 87. The plan names its other departures where
they arise: the `latest` class's reading of "expired" (5.1), the 1 h rotation age (5.2), the narrower anti-resurrection
record (5.3) and the reserve's use (5.5).

## 2. Measurements the plan rests on

All of these were read from records with no new load on 2026-10-11. The working copies are in claude6-eco's
`runs/e17/act-work/F-E6-52/`.

| Measurement | Value | Source and read time |
|---|---|---|
| membus state bytes, read 1 | 8,249,518 B (pid 17536, epoch 8) | `D:/projects/membus/state/heartbeat.json`, beat at 08:59:04.862Z (`heartbeat-read1.json`) |
| membus state bytes, read 2 | 8,259,892 B (same pid and epoch) | same file, beat at 09:00:34.867Z (`heartbeat-read2.json`) |
| Growth since 00:44Z (5,828,395 B, claude6-eco Observe 1903) | 293,788 B/h | read 2 against it |
| Growth since 07:21Z (7,602,708 B, Observe 1903) | 395,969 B/h | read 2 against it |
| Stripe records, all four logs | 8,255,306 B, 10,886 records, first record 2026-10-09T19:19:32.595Z | `stripe_split.py`, read 08:59:25.765Z (`stripe-split.json`) |
| Appended in the last 24 h | 4,703,508 B, 195,979.5 B/h, 278.6 records/h, 0 removes | same read |
| Appended in the last 6 h / since 07:21Z | 326,167.8 B/h / 397,806.7 B/h | same read |
| Dedup table entries backed by records appended in 24 h | 5,581 of 262,144 (2.1%): 6,687 distinct op_ids less the 1,106 timer progress puts, which skip dedup (src/lib.rs:1064, :1488-1489) | same read; a refusal before admission and a deduplicated retry add no entry (src/lib.rs:1561-1660), so the entries this count cannot see are mem-ack and failed-append outcomes only |
| Distinct keys ever written | 1,567 (quota 711, slots 636, capacity 195, lease-claims 9, timers 4, lease-control 1, test namespaces 11) | same read |
| Quota probes in 24 h | 174 probes, 7 skipped, 732 `probe-held` events | `C:/Users/xteam/.pillars/quota/events.jsonl`, read 08:59:35Z (`probe-count.txt`) |
| quotafwd | writes no membus records: it subscribes to and scans quota `event/`, and its hourly prune is off (`D:/projects/machine-tools/trunk/quotafwd.py`:1-27, :49-51 at fd17a58); its timer watches arm the membus timers counted under `__membus_timers_v1` | read 09:40Z |
| Lease service writes in 24 h | lease-claims 269 records (71,811 B), lease-control 137 records (20,139 B), about 2.3 records and 530 B a probe, counting the 3 `inference/` claim records and every lease-control write | `stripe-split.json` |
| Metric day files (all series, `D:/projects/metrics/data/raw`) | 2,953,667 B on 2026-10-10 (123,069 B/h); 2,064,032 B on 2026-10-11 to 08:59Z (about 229 kB/h) | read 08:59:42Z (`metric-day-files.txt`) |
| F-E6-48's metric-namespace thresholds (8 GiB within 30 days) | 10,741,670 B/h with nothing retired; 107,416,701 B/h under 72 h retirement | `runs/e9/act-work/F-E6-48/METRIC-RETENTION.md` (seq 1228) |

The writers in the last 24 h, by namespace and key family (`stripe-split.json`, `last_24h`). Each family is mapped to its
writer by lane 88's description of its writers' keys (claude6-eco seq 1164). The split is read from membus's own
records, where every record carries its namespace and key; the writers' own logs were read only for the probe count.

| Writer | Records | Bytes | B/h | Class proposed (section 5.1) |
|---|---|---|---|---|
| slots `inference` | 815 | 2,099,034 | 87,459.8 | latest |
| quota `resets` | 476 | 993,804 | 41,408.5 | latest |
| quota `state/` | 495 | 222,037 | 9,251.5 | latest |
| quota `tokens/` (token meter) | 808 | 208,351 | 8,681.3 | latest |
| slots `cpu`, `mem`, `disk:d`, `gpu`, `disk:c`, `inference-codex` | 1,323 | 495,576 | 20,649.0 | latest |
| quota `event/` | 478 | 183,729 | 7,655.4 | expire 3 d |
| membus timers `p/`, `s/` | 1,107 | 165,808 | 6,908.7 | latest (membus's own) |
| quota `fit/` (calibration) | 491 | 141,455 | 5,894.0 | latest |
| lease-claims `quota/`, `inference/` | 269 | 71,811 | 2,992.1 | expire 24 h (leases PLAN.md section 3) |
| capacity `event/` | 118 | 57,865 | 2,411.0 | expire 3 d (lane 88 to confirm) |
| slots `event/` | 168 | 43,270 | 1,802.9 | expire 3 d |
| lease-control `epoch` | 137 | 20,139 | 839.1 | latest |
| quota `open` | 2 | 629 | 26.2 | latest |

The metric writers' rate of about 123 to 229 kB/h is 47 to 87 times below F-E6-48's lower threshold. A 3-day metric
namespace at that rate would hold about 9 to 16.5 MB. Raw metrics stay in lane 88's day files until lane 87 and lane 88
choose otherwise (seq 1235). This plan only notes that such a namespace fits the expire class.

### Replay of today's records under the candidate forms

`retire_sim.py` replays every stripe record (read 09:06:03.640Z, 10,941 records) under each form. It writes
`retire-sim.json`. It uses lane 88's classes as proposed in section 5.1. Of the 8,301,432 bytes stored:

| Class | Stored | Latest version of each live key | Live keys |
|---|---|---|---|
| latest (S) | 7,692,804 B | 30,604 B | 45 (1 tombstone) |
| expire 3 d (E3) | 514,299 B | 514,299 B (every event key is written once) | 1,507 |
| expire 24 h (E1) | 72,345 B | 2,406 B | 9 |
| other, kept (F: test namespaces) | 21,984 B | 11,135 B | 12 (10 tombstones) |

So 92% of the bytes (7,662,200 of 8,301,432) are superseded versions of 45 state keys. The live set is 558,444 B, or 6.7% of what is stored.
The kept bytes under each form are in section 4.

## 3. The problem points this plan settles

1. **Rarely rewritten keys pin their chunk.** Under whole-chunk expiry, a chunk holding the latest version of a key that
   is seldom rewritten stays (F-E6-48). Six live state keys were not rewritten in the last 24 h (`retire-sim.json`,
   `S_carry_24h_keys_carried_per_period`). Section 4 chooses how such a chunk is retired. Section 5.6 deals with the
   worst case: today's 8.26 MB legacy logs.
2. **Key slots are never reclaimed.** Every quota, slots and capacity `event/` key is new, at about 764 a day. Each
   takes a slot in its namespace's 500,000 quota for good.
3. **Health cannot see the limits that refuse puts.** read_only is sticky after any client's failed append
   (src/lib.rs:1731-1732). The 24 h dedup table fills at about 182 continuously renewing names (1,440 op_ids a day each at a 60 s
   renewal). At today's base of 5,581 entries the ceiling is (262,144 - 5,581) / 1,440 = 178 such names, or a sustained
   10,690 new op_ids an hour on top of today's 233. Only state bytes are
   watched, through fleet's heartbeat_warn `state_high_water` (fleet-manager `config/services.toml`:210 at 67ff2b9).
   The decider asked for heartbeat flags for read_only and dedup occupancy (claude6-eco e14 REPORT.md row 11).
4. **Replay leaves no record.** What replay found after the 14:14:52Z bugcheck is recorded nowhere (seq 1606).

## 4. Rival forms, converged pairwise

These are the forms for retiring superseded state (latest-class) records. Expiring classes retire by close time plus
retention in every form. The kept bytes come from `retire-sim.json` at 09:06:03.640Z. "60 s" and "1 h" are the
rotation age, with chunks also cut at 8 MiB.

| Form | How a chunk holding a rarely rewritten key's latest version is retired | Kept bytes, latest class, 60 s / 1 h | Added writes | Fits the founder's words |
|---|---|---|---|---|
| **W, wait** (the commission's whole-chunk expiry; first dismissed for pinning) | When that key is next rewritten or removed. Until then the chunk stays. | 101,348 / 1,823,830 (37 / 20 pinned chunks, the largest 10,425 / 253,645 B) | none | a reading: "expired" read as superseded (5.1) |
| **W24, wait with the retry window** | As W, and a superseded chunk also waits until close + 24 h | 4,390,154 / 5,120,317 | none | a reading: "expired" read as superseded and past close + 24 h (5.1) |
| **C, carry** (not named in the commission; first dismissed as rewriting) | At carry age, membus re-appends the latest versions still in the oldest chunk to the open chunk, then retires the old one | carry 1 h: 401,204 / 689,992, adding 17,059 B/h; carry 24 h: 4,380,960 / 4,553,637, adding 502 B/h | the carried copies | no: it copies retained records |
| **K, checkpoint** (PLAN.md section 12 O4's checkpoint extended to values; not named in the commission; first dismissed as a new format) | A snapshot file of every live latest version (30,604 B now), after which every earlier latest-class chunk retires | snapshot plus open chunks | the whole live set at each checkpoint | no: it rewrites the live set into a new file |
| **O, owner refresh** (not named in the commission) | membus signals `pinned` (section 6 signals) for a key whose latest version holds a chunk past a pin age. The key's owner re-puts it, which moves the copy decision to the writer. | as W, until owners act | owners' re-puts | yes for membus; writers change |
| **N, nothing** (the null form) | Never. The logs grow and the state limit refuses. | all 7,692,804, growing 196 to 398 kB/h | none | n/a |

**Pairwise, first without the retry window** (as `retire_sim.py` computes C and K):
- **W against N.** N reaches 8 GiB in about 903 days at 395,969 B/h. It keeps every event key's slot, and replay time
  grows with the logs. W holds the store near its live set. W wins.
- **W against C.** At current rates W keeps less than carry at 1 h (101 kB against 401 kB with 60 s rotation) and
  copies nothing. C's only gain is a hard bound when pinned chunks are large. The measured largest pinned chunk is
  10 KB at 60 s and 254 KB at 1 h. W wins. C stays the fallback when `pinned_bytes` crosses its bound (section 6).
- **W against K.** K's snapshot (30,604 B) is the smallest form. But it adds a file format, a rewrite and a second
  replay path. PLAN.md section 12 O4's metadata checkpoint exists to bound startup at 8 GiB. At about 6 MB of retained
  state, startup needs no checkpoint. W wins for this slice. K stays PLAN.md's startup measure for when retained
  state is large.
- **W against O.** O keeps membus non-rewriting and lets the owner decide what to copy. It needs each writer to
  subscribe and re-put. The pinned bytes measured under W are small, so O adds work for no measured gain now. W wins.
  O is the second fallback, with no membus rewrite, if `pinned_high_water` fires.
- **C against K.** Both rewrite. C copies only stale keys (6 to 22 a period), while K copies all 45. C is the better of
  the two rewriting forms.
- **W against W24.** W alone retires a superseded chunk as soon as it closes. A restart then rebuilds the dedup table
  from retained records only (src/lib.rs:1064), so a client's retry of a put whose record was retired would apply
  again. An unconditional retry could then overwrite a newer value with an older one. W24 keeps superseded latest-class
  chunks for the 24 h retry window, which matches the dedup table's own horizon (PLAN.md section 4). It costs about
  4.3 MB more. A persisted dedup record would let W retire sooner, as a later slice weighs.

**Pairwise again, with the retry window on every form.** C and K lose dedup across a restart exactly as W does if they
retire a superseded chunk inside 24 h. So each form keeps every latest-class chunk closed in the last 24 h (4,541,584 B
at 1 h rotation: `S_carry_24h_kept_bytes` 4,553,637 less the 12,053 B of stale latest versions it adds), and the forms
differ only in the chunks older than that:
- **W24 against C24** (carry at 24 h): W24 keeps 5,120,317 B. C24 keeps 4,553,637 B and copies 502 B/h. C24 saves
  566,680 B (11%) at today's rates, at the cost of a rewrite path and a departure from "nothing is rewritten". W24
  wins while pinned bytes stay small. C24 is the fallback when `pinned_high_water` fires.
- **W24 against K24** (checkpoint at 24 h): K24 keeps about 4,572,188 B (the young chunks plus a 30,604 B snapshot). It
  saves about 548 kB, but adds a second replay path and a new file format. W24 wins for the same reason.
- **W24 against O24:** O24 keeps what W24 keeps until owners re-put, and it adds subscriptions to every writer. W24
  wins. O24 is the fallback with no membus rewrite.
- **W24 against N:** N keeps all 7,692,804 B, growing, with every event slot held. W24 wins.
- **C24 against K24:** C24 copies 6 keys a day, while K24 copies all 45, so C24 is the better rewriting fallback.

**W24 is chosen.**

**Chosen: W24 with 1 h rotation for finite classes.** File counts follow from `chunks_by_class` over the 37.78 h the
records span:
- **60 s rotation:** W24 keeps 4.39 MB in about 5,360 files: about 2,690 latest-class files (110.6 an hour for 24 h,
  plus 37 pinned), about 2,580 expire 3 d (35.3 an hour for 73 h) and about 89 expire 24 h.
- **1 h rotation:** W24 keeps 5.12 MB in about 360 files: about 94 latest-class (3.07 an hour for 24 h, plus 20 pinned),
  about 232 expire 3 d, about 28 expire 24 h and 7 forever.

This departs from PLAN.md section 4's initial age of 60 s, for the file count. Forever namespaces rotate by size only
(PLAN.md section 12 O3).

## 5. Design

### 5.1 Policies and classes

Each namespace, or key prefix within one, takes one class. Longest prefix wins. A key with no policy is `forever`.

| Class | Retirement | Proposed first policies (lane 88's seq 1164 (2); lane 88 to confirm each family) |
|---|---|---|
| `expire(retention)` | a closed chunk retires once close time + retention has passed | quota, slots and capacity `event/`, 3 days. lease-claims, 24 h (leases PLAN.md section 3, with the condition in 5.4). |
| `latest` | a closed chunk retires once it holds no key's latest version and close + 24 h retry window has passed | quota `state/`, `open`, `resets`, `tokens/`, `fit/`; slots `<resource>`; lease-control; membus timers (membus's own policy, built in) |
| `forever` | never | everything else, ledgers included (PLAN.md section 5: retention=forever for authoritative ledgers and artifacts) |

**The `latest` class is a reading of the founder's words.** The founder bases expiry on close time: "we only retire a
chunk if its newest record has expired ... the chunk close time ... is what we base expiration time off of" (design
record :243). `latest` reads "expired" as "superseded by a committed newer version", with close + 24 h as the time
floor. That is lane 88's class (seq 1164 (2)), and F-E6-48 located it as the open difference from whole-chunk expiry.
It is named to lane 87 as question (d).

**Setting policies.** A policy is a record in a gated namespace `__membus_policy_v1`. Client puts and removes there are
refused, as the timers namespace refuses them (src/lib.rs:1814). A new opcode `POLICY_SET` (24) writes it and
`POLICY_GET` (25) reads it, and `membus policy` exposes both as a CLI. A class is its kind and retention, for example
`expire(3 d)`. Each distinct class has its own folder and its own open chunk per stripe, and at most 64 classes exist
(PLAN.md section 12 O5); `POLICY_SET` refuses a 65th. A policy change moves a prefix's later writes to another class's
open chunk. No chunk's class or close time ever changes (PLAN.md section 5). eco's own test namespaces left
on the production bus (`e6-resident-restart-*`, `e6-install-proof`, `F-E6-33-health`) take `expire(1 d)`, set by eco at
install. `quota-test` is lane 88's.

### 5.2 Write path and rotation

A put resolves its class from the policy. Its stripe hash is unchanged. The record goes to the open chunk for its
stripe and class:
- File: `state/chunks/<class-dir>/<stripe>-<chunk-seq>.log`, with the MBK1 frame unchanged.
- Chunk numbering: `chunk-seq` is monotonic per stripe across classes.

A chunk closes in two cases. It closes at append when its bytes would pass 8 MiB or it is older than its rotation age
(1 h for finite classes, a departure from PLAN.md section 4's 60 s; section 4 gives the file counts). A 1 s maintenance tick also closes idle chunks past their age. A chunk opens only when a
record arrives, so no empty chunks are made (PLAN.md section 12 O3).

The catalogue is `state/chunks/catalogue.log`. It holds append-only records, flushed before they take effect:
- `open(seq, stripe, class, policy, open_ms)`
- `close(seq, close_ms, bytes)`
- `retiring(seq, horizons)`
- `retired(seq)`

The `open` record is flushed before the chunk file is created. So a crash leaves at most a record with no file, which
replay closes as an empty chunk. A chunk file with no `open` record is not produced by any crash. Replay treats it as a
storage error (5.5).

When the catalogue passes 1 MiB, it is rewritten atomically (tmp, flush, rename) to the live list. That rewrite is
metadata only and never copies records.

Open handles are 4 per class with an open chunk. The first policies make 5 classes (`expire(3 d)`, `expire(24 h)`,
`expire(1 d)`, `latest`, `forever`), so 20 handles. The flush budget counts one flush per touched handle (PLAN.md
section 12 O5). The build measures the flushes a gather adds, against today's ones.

### 5.3 Index, key slots and anti-resurrection

`Meta` gains the chunk holding the key's latest version. Each chunk keeps a count of latest versions it holds and the
set of keys it holds.

**Replay order.** Replay resolves each key by its version, which is membus's global `next` sequence, not by file order.
A key moved to another class by a policy change can have records in two open chunks, and versions still order them.

**The version high-water.** Today replay rebuilds `next` as the highest retained version plus one (src/lib.rs:1050),
and nothing else persists it. Once retirement removes the record that held the highest version, a restart would lower
`next` and reuse version numbers:
- an old client's conditional put expecting a retired version could succeed;
- a new version could land at or below a horizon and be dropped by the next replay.

So every `close` and `retiring` catalogue record carries the current `next`. Replay sets `next` to the larger of the
retained versions plus one and the catalogue's highest recorded `next`. A retirement whose `retiring` record cannot be
flushed with it does not proceed.

**Key slots.** Two counts are kept apart. `state.keys` counts index entries, tombstones and expiry summaries included,
against `index_cap`. `namespace_counts` counts live (non-tombstone) keys against the namespace quota. Today admission
counts a key as new only when it has no index entry (src/lib.rs:1571), and commit raises both counts together
(:1641-1644). The build splits that:
- **Put.** A put to a key with no entry is new to both counts. A put to a tombstoned key is new to the namespace count
  only, and it is checked against the quota at admission (:1575).
- **Remove of a live key.** The namespace count falls when the remove commits. The tombstone stays as an index entry,
  charged to `index_cap`.
- **Remove of a missing key.** It persists a tombstone charged to `index_cap` alone. This changes KV1.md section 7's
  "Missing-key remove persists a tombstone and consumes a key slot", and that section is updated with the build.
- A tombstone's index entry is dropped once no retained chunk holds an older record of its key. Its chunk's latest count
  then falls.
- When an expire-class chunk retires, each key whose latest version it held leaves the index, and its slot is freed.
  The chunk's key set is kept as an expiry summary for 30 days (PLAN.md section 5), charged to `index_cap`.

**Reads after expiry.** Existing GET, GET_MANY and scans treat an expired key as absent, so current clients see no new
code. A conditional put expecting version 0 succeeds on it. A new `GET_STATUS` opcode (26) reports `expired`, `missing`
or the live version (PLAN.md section 12 O13).

**Anti-resurrection** (PLAN.md section 12 O8). A chunk may retire while an older retained chunk holds an older version
of one of its keys. That happens only after a policy change. In that case its `retiring` record carries the key's
(namespace, key, version) horizon. Replay ignores versions at or below a horizon. A horizon is dropped when the older
chunks retire. If the horizon cannot be persisted, retirement waits.

O8 asks for every key's latest horizon to be persisted before any chunk retires. This plan persists a horizon only where
an older version stays retained, which is a narrower record. It suffices because:
- a key with no older retained version has nothing to resurrect;
- versions are never reused (the high-water above).

The narrowing is a departure from O8's wording, named here; T4 tests it.

### 5.4 Retirement

A maintenance pass runs every 10 s on the bounded I/O pool. For each closed chunk that is eligible by its class:
1. Flush `retiring(seq, horizons)` to the catalogue.
2. Drop index entries and free slots (5.3).
3. Delete the file.
4. Flush `retired(seq)`.

Values are served from memory (src/lib.rs:1207-1216), and snapshots and scans read memory, so no membus reader holds a
chunk file in this build. Deletion therefore needs no reader count. A warm tier would need one, but warm is not in this
slice.

A process outside membus can still hold a chunk open: a measurement script, an indexer, or a backup. REPORT.md item 14
names the same case for MoveFileExW under a delete-sharing reader. When the delete fails:
- the chunk stays `retiring`, its bytes stay counted, and each later pass retries it;
- HEALTH2 counts it as `retire_blocked` with the oldest such chunk's age;
- `pinned_high_water` does not cover it, so `retire_blocked_high_water` (any chunk blocked over 1 h) is added as a flag.

**Clock** (PLAN.md section 5). Eligibility uses UTC close time cross-checked against monotonic elapsed time within a
run, and never retires earlier than both allow. After a restart, retirement pauses and health reports
`expiry_degraded` in two cases: when UTC is earlier than the catalogue's last record, or when it is more than 24 h past
the heartbeat's last beat. The pause lasts until an operator command (`membus expiry resume`) or one full rotation age
of stable ticks.

**lease-claims condition.** The lease service reads a claim with GET and writes it with a conditional put expecting the
read version or 0 (membus-leases src/lib.rs:459, :491-497 at 86fba3b). With expired-as-absent reads (5.3), a retired
claim reads as vacant and the next acquire writes version 0. The combined qualification (section 9) runs the lease
harness against the new membus before lease-claims takes `expire(24 h)`. Until then it is `latest`.

### 5.5 Failure and restart

- **A crash between `retiring` and `retired`** is finished at start: the delete is idempotent. The replay record lists
  each one.
- **A file missing with no `retiring` record** is a storage error, not an expiry (PLAN.md section 5). So is a chunk file
  with no `open` record (5.2). In both cases membus starts read_only with `storage_error` set, and the replay record
  names the file.
- **Torn tails** are truncated only in chunks that were open at the crash, and only as today: a short final header or
  an incomplete final body (src/lib.rs:971-973, :994-996). A CRC failure in any complete frame, a final one included,
  still refuses start (src/lib.rs:1001-1006; KV1.md section 6: "A checksum-failed complete final frame is not silently
  discarded"). A torn tail in a closed chunk counts as interior corruption.
- **Interior corruption** still refuses start, as today (replay in src/lib.rs:951-1105). The replay record is written before exit
  and names the file and offset.
- **The state limit** (8 GiB) counts retained chunk bytes. Retired bytes leave the count at `retired`.
  RESIDENT.md requires PLAN.md's 1 GiB maintenance reserve and 7 GiB ordinary ceiling before a rotation rollout
  ("State growth and upgrades"). So ordinary admissions refuse with 7 at 7 GiB, and catalogue, retirement and import
  writes may use the last 1 GiB. `state_high_water` stays at 75% of the 8 GiB limit.
- **read_only** stays sticky after a failed append, as today. It becomes visible (section 6), and a restart clears it.
  Repairing it is the later slice's (5.8).

### 5.6 Legacy logs: the one-time import

Today's four `stripe-N.log` files hold 8.26 MB of every class, mixed, in no chunk. Read as a single forever chunk, they
would never retire. Read as a mixed chunk closed at install, any of the six rarely rewritten state keys would pin all
8.26 MB indefinitely.

So the first start of the new build, during the stopped swap, imports the live set:
- It replays the legacy logs as today and appends each key's latest version (558,444 B now), at its original version
  and timestamp, into new chunks by class.
- Retries stay recognised without copying more. For 24 h after the import, each start also replays the legacy logs for
  their dedup entries alone, never for values. After that it stops, and the legacy logs are left only for rollback.
  Copying instead would mean every record of the last 24 h (4,703,508 B), since replay builds the table from every
  record in the window (src/lib.rs:1064).
- It flushes the catalogue, records `import(legacy_sha256 per stripe, keys, bytes)`, and then moves the legacy files to
  `state/legacy-<epoch>/`, unmodified.

The legacy files stay for rollback until lane 87 or lane 88 asks for their removal. Rolling back to the previous binary
means restoring them, so the old binary never sees writes made after the swap. Whether lane 88's writers re-put them
from their files is not located (seq 1932). It is asked in F-E6-74's post to lane 88.

It is the explicit migration RESIDENT.md asks for an on-disk format change and PLAN.md section 5 for a policy applied to
existing records. Its owner-visible cost is about 0.56 MB written once, plus 8.26 MB kept until removal. It is question
(c) to lane 87.

### 5.7 Expected footprint

At the last 24 h's rates with the chosen form, retained state settles near 6.1 MB:
- E3: 11,869.3 B/h over 73 h is about 0.87 MB.
- E1: 2,992.1 B/h over 25 h is about 0.07 MB.
- Latest class: 5.12 MB measured.
- Forever: 0.02 MB.

Event key slots settle near 2,300 (764 a day for about 3 days). Today membus holds 8.26 MB, growing 0.2 to 0.4 MB/h,
and 1,507 event slots held for good. The live values in memory (558,444 B against the 256 MiB `capacity`) stop growing
with event keys.

### 5.8 What this slice leaves out, and where it goes

**Milestone 4's memory bound** (PLAN.md section 9 item 4 and section 5: a 512 MiB process budget, admission before a
calibrated RSS, byte-weighted LRU, explicit cold reload) is not in this slice. Values today live wholly in memory, and
the live set is 0.2% of `capacity`. Retirement removes the one growing part of it, the event keys. Evicting values needs
reads by chunk offset (a warm path), which is a larger change with its own review. It goes to a later slice once the
live set or lane 83's adoption moves it.

**The lease-control write gate and the four membus faults:**
- **In this slice,** as tests (section 7), because this slice rewrites replay and the state count:
  - a put cut mid-write with replay after a failed sync;
  - a cold restart near the state cap.
- **In a later hardening slice:**
  - the lease-control write gate, with a documented repair for the persistent-refusal states (e14 REPORT.md row 10).
    The decider put it "before any fenced adopter". The fenced adopters it names are lane 88's reopen-timer replace and
    the pillar successor write (REPORT.md:122). Quota uses its token only for release (claude6-eco
    `runs/e17/act-work/F-E6-74/reads-helper/return.md` (d)). Whether those two adopters are live is not read here; it is
    asked of lane 88, and the slice moves ahead of any adoption it reports;
  - a 2000 ms reconcile put cut under a flush stall;
  - boundary 14's remaining entries: e14 REPORT.md item 14, "Not tied to a decision" (:185-196), lists five. The first
    two are in this slice's tests, the reconcile is above, and this plan carries the MoveFileExW case into retirement
    (5.4). The SYSTEM or session 0 open of the LOCAL pipe goes to the later slice.

**Tiers** (warm, cold, shadows; PLAN.md section 5) stay later slices, as PLAN.md's "first slice" orders them.

## 6. Health, heartbeat and the replay record

**HEALTH (opcode 8)** keeps its layout (KV1.md section 2). A new `HEALTH2` opcode (27) returns a length-prefixed
key=value list:
- `state_bytes`, `state_limit_bytes`, `state_high_water` (75%);
- `read_only`, `storage_error`, `expiry_degraded`;
- `dedup_entries`, `dedup_cap`, `dedup_high_water` (70%);
- `keys`, `index_cap`; each namespace's live key count against its quota (11 namespaces today); and
  `namespace_high_water`, set when any namespace is at or above 70%;
- `chunks_open`, `chunks_closed`;
- `retained_bytes` per class, `pinned_bytes`, `pinned_high_water` (64 MiB, configurable);
- `retired_chunks`, `retired_bytes` for the server's lifetime; `retire_blocked` with the oldest blocked chunk's age.

**Wire version.** Opcodes 24-27 are additive under kv/1 minor 0, as IO_METRICS (opcode 9) is (KV1.md, "F-E6-20
implemented grouped write policy"). HELLO and HEALTH keep their layouts, and the server still accepts kv/1.0
(src/lib.rs:1762). A client learns of the new opcodes by calling them: an older server returns `unsupported` (2). The
Python client gains `health2()` and `get_status()`, which map `unsupported` to "not available on this server".

**The heartbeat** (src/main.rs:495-541) adds the flags `read_only`, `storage_error`, `expiry_degraded`,
`dedup_high_water`, `namespace_high_water`, `pinned_high_water` and `retire_blocked_high_water`, and the numbers `dedup_entries`, `retained_bytes`
and `retired_bytes`. Fleet flags a heartbeat count above 0 (fleet-manager 67ff2b9, lane 88's answer in claude6-eco row 1924, watch 7). So only the flags are meant for
`heartbeat_warn`, and which ones lane 88 wires is its question. A beat grows from about 257 B (402,249 B over 1,565
beats) to an estimated 480 B (about 22 B a field for ten fields; T9 measures it). That is about 1.4 MB a day of D: writes at the 30 s beat.

**Replay record.** At ready, membus writes `state/replay.json` (tmp, rename) and appends one line to `state/replay.log`,
which is rewritten to its last 256 lines at 1 MiB. Each record holds:
- the epoch, start and ready times, and duration;
- per stripe: chunks and legacy logs replayed, records and bytes replayed, truncations (chunk, offset, bytes, reason),
  retirements finished, catalogued chunks skipped as retired, horizons applied;
- dedup entries rebuilt, keys and namespace counts;
- any refusal (interior corruption, missing file), written before exit.

`membus.log` gets a one-line summary.

## 7. Tests and pass conditions

The build runs the base's suite first (`cargo test --offline --locked` at 51d6553; its count is read at build start).
Every base test passes unchanged except where KV1.md's slot rule changes, and that change is stated in the diff.

| # | Test | Passes when |
|---|---|---|
| T1 | Rotation by size (8 MiB) and age, idle close by the tick | each close has a catalogue `close` within 1 s of its boundary; no empty chunk exists |
| T2 | Expire class with a mocked clock | the file is deleted at the first tick at or after close + retention and never before; records in an open chunk wait; GET returns absent and GET_STATUS `expired`; after the summary's 30 days GET_STATUS returns `missing` |
| T3 | Latest class | a superseded chunk retires only after close + 24 h; a chunk holding one latest version stays until that key is rewritten, then retires on the next tick past its window |
| T4 | Removes, tombstones, versions and anti-resurrection | a remove frees its quota slot at commit; a put to the removed key takes the slot back and is refused at the quota when the namespace is full; a missing-key remove charges `index_cap` alone; the tombstone's index entry drops once no older record is retained; after the highest-versioned record retires, a restart's `next` is above every version ever issued; after a policy change that retires a newer chunk first, a restart never returns the older value |
| T5 | F-E6-48's 1,000-key run repeated (put, overwrite, remove) | slots return to 0 after removes; after retirement the retained bytes fall below the 680,000 B F-E6-48 recorded, to the chunks still within their windows |
| T6 | Kill at each retirement step (before `retiring`, after it before delete, after delete before `retired`) | restart finishes or keeps the file; every latest value and version is unchanged; the replay record lists each completion |
| T7 | Replay record and storage errors | a short header or short body in an open chunk is truncated and listed with chunk, offset, bytes and reason; a CRC failure in a complete final frame refuses start, as today; interior corruption in a closed chunk refuses start with the record written; a file missing with no `retiring` record, or a chunk file with no `open` record, starts read_only with `storage_error`; a delete held by an outside reader leaves the chunk `retiring`, counted and retried, with `retire_blocked` set |
| T8 | Dedup across retirement and restart | a retry of a put within 24 h, after a restart, returns its original outcome and writes nothing |
| T9 | HEALTH2 and the heartbeat | each field equals the store's state; `read_only` turns true after a failed append (test seam `fail_partial_append`); `dedup_high_water` at 70% of a small `--dedup-cap`; `namespace_high_water` at 70% of a small `--quota`; the beat stays under 512 B |
| T10 | Clock jumps | a forward UTC jump of 1 day mid-run retires nothing early; a backward jump across a restart sets `expiry_degraded` and pauses retirement |
| T11 | Legacy import on a copy of production state | every key's latest value, version and namespace count equal before and after; a retry of a put made within 24 h before the import returns its original outcome after the import and after a further restart; past 24 h the legacy logs are no longer replayed; the legacy files' sha256 is unchanged in `legacy-<epoch>/`; the previous binary serves the restored copy |
| T12 | Fault: replay after a failed sync (`fail_sync`) with a put cut mid-write | replay truncates only the open chunk's short tail; the acknowledged os and flush puts survive the process kill |
| T13 | Fault: cold restart near the cap | with retained bytes within one record of the 7 GiB ordinary ceiling (scaled down by flags) and retirement due, start succeeds, the catalogue and retirement writes use the reserve, and new puts are admitted once retirement brings bytes under the ceiling |
| T14 | Python client | `health2()` and `get_status()` parse; existing calls are unchanged |
| T15 | Production-shaped replay (private pipe, a copy of the state, through lineup with `disk:d`) | after import and a simulated 73 h of the measured mixture, retained bytes are within 10% of section 5.7's estimate; flushes per gather are reported beside today's |
| T16 | Lease harness against the new membus (combined qualification) | membus-leases' qualification passes with lease-claims `latest` and again with `expire(24 h)`, including a vacant read after a retired claim |

## 8. Dependencies, build and repository

- **Dependencies:** std only, as today. No crate enters, so depguard has nothing to clear.
- **Build:** on the host in the worktree through lineup, `cpu=5,mem=4,disk:d` with `-j 2`.
- **Branch:** `F-E6-52-chunk-retirement` (or the frontier's id Decide assigns), in
  `D:/projects/membus/worktrees/<branch>` from the main this plan merges into. Its description names claude6-eco's
  ledger and the frontier.
- **Owner:** eco owns the binary. Lane 88 owns the registration (fleet `services.toml`:199-214).

## 9. Review and install

**Review:** one code review runbook run (CODE_REVIEW_RUNBOOK.md, sha256 9be0c0f3), by lane 87's answer to question (b):
- **Separate runs.** Chunk retirement from 51d6553 to the head being merged, and the leases repair's re-gate on its own.
  That is about 180 seats.
- **One run on the combined release** (runbook :104-107). That covers membus 51d6553 to its merged head and
  membus-leases 86fba3b to its merged head, with an area scout per package, at about 90 seats. Its cost is that the
  leases repair's merge then waits for this build.

**Install:** through lane 88's stopped swap, at a time agreed with lane 88:
- `fleet services swap membus NEXT SHA256` (about 65 s on the writers' file fallback, F-E6-28), then
  `fleet services swap membus-leases NEXT SHA256` in the same window.
- A membus swap re-epochs the lease service anyway (`services.toml`:216-223), so both swaps cost one 300.1 s
  quarantine instead of two.

**Readback for each swap:**
- the installed sha256 against the release manifest;
- health of both services;
- `state/replay.json` of the first start, with the import's counts against T11's;
- the heartbeat's new fields;
- lane 88's heartbeat_warn reading.

**Rollback:** swap back to `membus.prev-<sha12>.exe` and restore `legacy-<epoch>/` (5.6).

## 10. Milestones

1. **CR1:** the catalogue, chunks, rotation and replay by version, with the replay record (T1, T6, T7, T12).
2. **CR2:** policies and the gated policy namespace; expire and latest retirement; key slots; anti-resurrection; clock
   (T2-T5, T8, T10, T13).
3. **CR3:** HEALTH2, the heartbeat fields and the Python client (T9, T14). Update RESIDENT.md's line that "No automatic
   capacity-pressure alert consumer is wired yet", since fleet's heartbeat_warn reads `state_high_water`
   (`services.toml`:210).
4. **CR4:** the legacy import and the production-shaped replay (T11, T15).
5. **CR5:** review (section 9), with fixes by its decider's report.
6. **CR6:** the combined qualification (T16), release, the swap window and readback, and the policies set by their
   owners.

## 11. Beyond this loop's capacity

- One runbook run of about 90 seats (combined) or two of about 90 each.
- One swap window with lane 88.
- Lane 88's policy choices.
- Lane 87's answers (section 12).

The build itself is one Act's work under lane 87's one build at a time (seq 1083 (1)).

## 12. Questions

**To lane 87 (exec):**
- (a) May chunk retirement's build start while the leases repair's re-gate waits for room? The repair's code and unit
  tests are done (membus-leases branch F-E6-74-repair, head bf043ea) and its runbook run has not started.
- (b) Should the re-gate and this slice's own runbook run be one run on the combined release (runbook :104-107)? That
  is about 90 seats instead of about 180, with one stopped window for both installs.
- (c) Will you commission the one-time legacy import (5.6) as the explicit migration RESIDENT.md and PLAN.md section 5
  ask for? It writes about 0.56 MB once and keeps the 8.26 MB legacy logs until their removal is asked for.
- (d) The `latest` class reads the founder's "expired" as superseded and past close + 24 h (5.1). Does lane 87 accept
  that reading, or put it to the founder?

**To lane 88 (tooling):**
- Which class each key family takes (section 5.1's table), including capacity `event/` and `quota-test`.
- Whether any reader of `event/` keys needs them past 3 days. quotafwd's prune, when turned on, removes them after 7
  days (`quotafwd.py`:49).
- Whether lane 88's reopen-timer replace and the pillar successor write are live as fenced lease adopters (5.8).
- Which of section 6's heartbeat flags its watchers read, and whether HEALTH2's fields are the ones it needs.
