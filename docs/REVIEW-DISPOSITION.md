# F-E6-1 observe dispositions

First cold observation: e1/act-work/membus-review.json, O1–O23, original design-read SHA256 010b65c4118e32e3b39c198f5c59bbe4afebef617715ee7a1a2a1dc4f1f9e40a. It read the initial plan before seq90 client answer and seq100 tiers were incorporated. Review adds located observations, not an approval. Plan section12 carries authoritative clarifications for older provisional clauses.

| Observation | Fix or answer |
|---|---|
| O1 | Named speculative immediate visibility versus recoverable committed-read delay; OS append has no flush gather wait. |
| O2 | Explicit server-file versus per-writer-file interpretation and departure; unacknowledged queues can be lost, retries named. |
| O3 | Finite leases24h/maxTTL5min; forever data quota; tombstone does not promise physical reclamation. |
| O4 | Metadata index checkpoint plus quota-bound fallback replay; extra snapshot writes stated. |
| O5 | Device-wide flush call budget and measured p99 rule, write-only backpressure. |
| O6 | Correct one flush per touched handle; class/namespace admission bound64. |
| O7 | Age rotation finite-retention only, skip empty chunks; forever size rotation. |
| O8 | Persist anti-resurrection horizon independent of expired/missing display summaries; policy/tombstone test. |
| O9 | Immutable stripe count/hash seed per data directory; changed setting refuses startup. |
| O10 | Explicit fleet authority loss on restart/outage; no priority, router fencing and supervision external. |
| O11 | remaining_ms + client pre-send monotonic safety deadline; validate failure refuses effect, latency unmeasured. |
| O12 | Seq90 client accepts CAS; extra RTT/conflicts/write cost explicit, increment reconsidered on demand. |
| O13 | High-level Python convenience fetch with deadline, low-level status preserves no-drive hot claim. |
| O14 | Outcome/summary/overlay bytes assigned to caps; rough index estimate labelled proposal, measure overhead. |
| O15 | Subscription persistence explicit, first service same logon; LOCAL-pipe service test deferred packaging. |
| O16 | Complete TCP fallback for all mixed cases; explicit flush-budget heuristic, client durability now located. |
| O17 | Byte-sum batch admission, bounded reply pages, per-item partial outcome semantics. |
| O18 | Trusted signed-in user model explicit; self identity not authentication, fencing cooperating processes. |
| O19 | Durable data outside checkout in project state directory; .gitignore now present for generated output. |
| O20 | F1 design-only versus Act F2 suite wording clarified; real measurement appendix before send; both evolving basis hashes retained. |
| O21 | Proposed slice branch purposes named; future frontier IDs remain Decide's, branches created only once commissioned. |
| O22 | Clean original whole-chunk coverage retained; founder tiers added and scoped local cold codec, cloud external facts. |
| O23 | RwLock publication identified and short contention explicitly stated; no disk/write waits while holding it. |

Followup observation of changed section12, landed client facts and tiers: e1/act-work/membus-review-followup.*; its findings are appended here before final delivery.

Second cold observe a4f977e4-e58e-4a34-993b-18df789ff90c completed, findings F1–F19 (e1/act-work/membus-review-followup.json). Plan section13 resolves them as contracts and explicit limits; subsequent consolidation of stale paths/wording is editorial, not another implemented claim.

| Findings | Final fix/answer |
|---|---|
| F1/F12/F13 | Project-state shadow paths, record in README/commit; statuses and all-mode speculative overlay explicit; landed seq90/tier coverage and terminology consolidated. |
| F2/F19 | All retained/cold-key metadata stays bounded in index; cold does not relieve key count; reserve headroom, no absent-on-cold CAS; pressure coupling explicit. |
| F3 | Sparse full snapshots after10×byte threshold and30min, not60s; amplification/readiness tradeoff and all added I/O named. |
| F4/F18 | Every internal flush budgeted, nonzero ticket spacing from repeated p99, priority/deadline loss, os drain and concrete flush batch cap rule. |
| F5/F6 | Total/logical disk quota and maintenance reserve; refusal/fallback is client-owned unresolved protocol question; optional reconciler lease correctly labelled and not adoption prerequisite. |
| F7 | Forever epoch/claim-id control storage, never reused tokens; stronger flush/reclaim versus client os/renew remains explicitly delivered proposal. |
| F8/F9 | Named tier sub-slices/tests and deterministic archive/catalogue/shadow restart actions/status interruption. |
| F10/F14/F17 | Compression-effectiveness rule and literal fallback, no fictional lineup mutex;1h finite/tier rotation, file cap, full load/capacity consequence. |
| F11/F16 | status/catalogue/change queries, policy fields/default modes, canonical transition event schema and catch-up/gap contract. |
| F15 | Warm rereads/OS-cache uncertainty added to drive cost and hot-age selection. |

No third review is asserted; the commission requires cold observation and each finding fixed or answered, which these two returns/dispositions provide. Build correctness and native performance remain future tests.

## e2 independent changed-section observation

Observe5 returned27located observations in e2/act-work/F-E6-1-observations.json; initialclaude2 launch ended with API429 and no observations. Observe5 completed successfully; no verdict requested. Current design SHA074d2226 and survey SHA b128d258 match the cited full hashes.

| Observation | Fix or answer |
|---|---|
| O-01 | Fixed: final IPC repeat raw table and same-build qualification added; PLAN2e updated. |
| O-02 | Fixed: repeated-build qualifier and exact>10%/no>15% thresholds restored in PLAN2e and MD. |
| O-03 | Answered: recorded exe/client hashes match; summarize-only dirty renderer differs from measured implementation; reviewed-1 lacks pinned commit provenance and is labelled accordingly. |
| O-04 | Fixed: earlier concurrency variants are directional only; final/repeat pair supplies rule qualification. |
| O-05 | Fixed: sizes/p95/max and>64row bursts mapped to per-handle splitting/deadlines; no uniform row-size latency claim. |
| O-06 | Fixed: one-thread/handle shape and percentile/sample limits explicit; concurrent service tests remain open. |
| O-07 | Answered: approximately13ms p95 stalls and variable frequency stated; device completion tail retained, no causal flush-only claim. |
| O-08 | Fixed:4KiB stall included in range wording. |
| O-09 | Fixed: original section12O5 now uses final fixed-spacing rule; original appendix calculations marked historical; wall-time inference not a utilization guarantee. |
| O-10 | Fixed: section4 gather window directly updated; OS drain versus flush tickets distinguished. |
| O-11 | Answered:4handle/256handle fan-out yields200ms/12.8s ticket spacing;50ms applies only isolated row, client deadlines apply under load. |
| O-12 | Fixed: claim control high-water then diagnostic grant both budgeted;2tickets possible; only return after both durable, unused ids retained. |
| O-13 | Answered: stronger flush/epoch/fencing remains an explicit proposal versus clientOSrequest, not founder-owned or adopted. |
| O-14 | Answered: exact operations match; transfer unsupported is explained release/reacquire gap, not silent transfer promise. |
| O-15 | Fixed: registry authority_host explicitly matches leaseProcessIdentity, presentation if present else base; supervised base move versus authority-host move distinguished. |
| O-16 | Fixed: overlapping rollout requires same mutex plus lease on new paths; lease-only starts only after legacy hosts verified stopped and deployment revision requires leases. |
| O-17 | Fixed: OS row says rebuildable/log default; ledger flush namespace default explicit. |
| O-18 | Fixed: raw enum-valued cache fields and repeat47–66us included without inferring power safety. |
| O-19 | Answered: device mapping matches devices/context witness; no alteration needed. |
| O-20 | Fixed: prepare slotcpu10,mem4,disk:c specified; separate std targetdisk:d route explained. |
| O-21 | Answered: RLE/literal departure from ZIP retained and optional/cloud scope stated. |
| O-22 | Fixed: disk:d applies to explicit maintenance command; bounded in-server I/O pool remains separate. |
| O-23 | Fixed: versioned admin transition_chunk signature/rules added for owner-triggered tier formB. |
| O-24 | Fixed: rough ordinary key capacity917,504 at224MiB plus reserve. |
| O-25 | Answered: speculative immediate visibility and committed recoverable acceptance remain explicit departure from survey, not hidden compatibility claim. |
| O-26 | Answered: retention values remain identified proposals, founder optional tier relation and claude2 examples preserved. |
| O-27 | Fixed: row/probe sample count200/64 distinguished from wrapperIPCdefault2000; no new executable hash required bycommission. |

## F-E6-11 protocol/replay review dispositions

Observe return: e3/act-work/F-E6-11/observe-findings.md and observe-return.json (session 2d1ab387-e13e-4f36-8699-7059b98c0ae5). This is located observation, not approval. The candidate evolved during the read; final code and KV1.md below supersede the initial staged line numbers. Verification: cargo-acceptance.txt and integration-acceptance/integration-test.json, retained in the commissioned project evidence folder.

| Observation | Act disposition and location |
|---|---|
| O1 post-admission read_only overload | Fixed: lib.rs returns io_failed_uncertain (16) after admitted I/O failure; read_only (10) is before admission. CODES/Python/KV1 §4 agree; injected post-append failure tested through dispatcher. A partial failed append additionally fences its stripe under the append-owner lock before already-admitted followers can append behind a torn tail. Deterministic two-writer partial-failure test replays the earlier acknowledged key and discards only the torn tail. A complete uncertain record may replay, which is why the caller reconciles a deterministic key. |
| O2 pending deadline overload | Fixed: pending_timeout_uncertain (17) for a waiting duplicate; deadline (5) remains pre-admission. Concurrency test produces 17 and then the original outcome. |
| O3 dedup capacity class/rate | Fixed: transient dedup_full (18), default 262,144 slots, --dedup-cap configurable; FIFO lazy expiry and explicit 24 h horizon. KV1 §6 retains a bounded guarantee, not perpetual exactly once. |
| O4 pending conflict persistence | Fixed classification, amended for lane83 eco seq402: pending_conflict (19) is transient against reservations, while conflict (12) is committed expected-state mismatch. KV1 §§3/4 state re-read/reselect and client-owned hole filling if a pending winner fails. A committed wrong expected state still needs client action. |
| O5 CRC dedup identity | Fixed: exact mutation-byte equality replaces CRC identity; CRC is only log corruption detection. Exact retained payloads are charged to capacity. |
| O6 past-window coverage | Fixed: protocol test covers expired known id, retired-ring id and unknown-after-restart re-execution. Ring eviction has the same unknown-id rule; its finite cap remains documented. |
| O7 ingress/connection memory bounds | Fixed: 64 active connections, aggregate 64 MiB frame admission before allocation, released after dispatch; queue admission remains separate. Foreground server pipe slots can remain occupied until disconnect; finite resource bound and client cancellation are explicit. |
| O8 pipe exhaustion/accept errors | Fixed: bounded active slots stop pipe instance creation at 64, with 255 kernel instance ceiling; excess callers reach their deadline without ending the listener. ERROR_NO_DATA retries accept; TCP accept errors continue. Integration opens 64 pipe connections and checks listener survival. Persistent unrecoverable creation errors remain fatal with stderr. |
| O9 negotiated frame / pipe allocation | Fixed: serve enforces negotiated request body/reply cap; Python enforces stored cap. Pipe read/write buffers are at most 64 KiB each, avoiding remainder-sized repeated allocation. Minimum hello offer is 1,024 bytes for the refusal list. |
| O10 admission scan blocks reads | Fixed: separate metadata, scalar key/namespace/value/dedup counters, bounded retired-id hash membership and ordered expiry queue. Admission never holds the committed read lock or scans its key map. Publication remains a short map update; get takes no write/flush/admission mutex. |
| O11 os blocked by flush sleep | Fixed: append mutex released before ticket sleep and sync on a cloned file handle. OS appends take no deliberate ticket wait. Answered performance reach: slice 1 does not group flushes; 3,065 serial flush rows may take ~153 s at 50 ms, disclosed in KV1 §9. Grouping remains the next write-path slice, not an adoption claim. |
| O12 interior length mistaken for tear | Fixed: log header contains length and its bitwise complement. Complement mismatch reports corruption before considering a final incomplete body. Dedicated test flips interior length and proves the file is unmodified. |
| O13 replay modifications/stripe ownership | Fixed: stage torn-tail truncations until all stripes validate and enforce key-to-stripe ownership. Complete corrupt frames remain untouched and block startup. No automatic repair or power-loss availability claim. |
| O14 pipe timeout/first-instance/ACL | Fixed: first-instance guard and creator-user/LocalSystem ACL; remote clients rejected, active/frame bounds enforced. Answered: hard server pipe idle timeout belongs to later overlapped/service transport; client deadline cancellation is implemented and tested. Foreground endpoint is bounded, not service adoption. |
| O15 TCP idle close | Answered in KV1 §6: 60 s per-I/O idle bound; stable-op-id reconnect handles an idle socket close. Absolute server connection lifetime cutoff is not claimed. |
| O16 Python reconnect/epoch/association | Fixed: calls reconnect from None, close on malformed/association failures, and surface EpochChanged before uncertain replay across a restart. Reuse after timeout tested. Batch supports remove and returns code name/class; remove has the same version/ack dict as put; health uses requested namespace. |
| O17 framing diagnostics | Fixed: short body is malformed (3), large is too_large (4); version error closes; ready prints store.epoch. |
| O18 namespace quota / warnings | Fixed: initial 500k namespace keys covers located 270–350k; requested-namespace count and 70% namespace/global high-water flags on wire. Quota/cap remain configurable; value/dedup limits are distinct. |
| O19 raw seq keys incompatible UTF-8 | Fixed wording: KV1 §1 specifies fixed-width UTF-8 decimal/hex encodings; raw be64 is refused by this version. No covert binary-key claim. Client can preserve lexical order without a server stream. |
| O20 copied replay witness | Fixed: replayed list and counts are collected from actual successful get_many values, compared to acknowledged list, and missing ids computed. |
| O21 contention/flush evidence | Fixed measurement: four configured-50ms flush latencies and elapsed time recorded; API and CLI baselines both stated. Answered scope: main burst is single-writer OS-ack framing/allocation; pending conflict and duplicate use separate concurrent-client tests. This does not establish concurrent dense-seq or ledger flush-burst capacity. |
| O22 idle kill / synthetic tear | Answered: commissioned acceptance is every acknowledged OS/flush write after a killed server; test witnesses all 3,070. Torn header and length corruption are synthesized independently. Kill-during-partial-append stress belongs with queue/grouped-write slice; no natural torn-body or power-failure coverage claim. |
| O23 old in-place plan | Fixed: PLAN §1/§3 and §13 item5 replace the outage story with client-owned unsequenced spool; KV1 §9 supersedes fixed flush spacing with configurable initial value and current comparison. Historical preliminary measurements and future slice designs retain their scope. |
| O24 smaller notes | Answered/fixed: tombstones count as keys and peak-memory overwrite reservation can refuse early (KV1 §7); persisted incrementing epoch replaces wall clock; non-Windows lock cleanup is disclosed; put/hello/get reply now have Rust/Python shared golden fixtures; the closed code list is exact for major1/minor0 and changes require version negotiation. |

Late scoped input eco seq402: lane83 found six of eight statements met, asked which conflict is pending/committed, and requested a timed killed-server flush-acked burst. Pending conflict now has code19; tests/flush_burst.py verifies 3,070 fully flush-acked conditional rows at configured50ms, kill/replay and actual id sets, with output in e3/act-work/F-E6-11/flush-burst/integration-flush.json. Item5 remains a stated later scan scope; item7 now names 54–70% namespace occupancy at adoption and unmeasured per-key allocator memory. KV1 §8 records the client's own shadow invariants rather than treating shadow as selected or primary migration. Grouped commit remains later work.

## F-E6-20 observe dispositions

Source: D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e4/act-work/F-E6-20/observe-observations.md, actual observe-class seat on2b75022, with subsequent code/document observations carried by the same seat. Detailed raw measurements and version binding are in that folder's REPORT.md. Review is observation: these dispositions answer or fix each located artifact issue inside the commissioned write-path scope, without selecting primary adoption or a new problem.

| Observation | Disposition |
|---|---|
| O1 selection/cohort | Fixed docs: KV1 inline policy and PLAN4 amendment distinguish64/144192selection from1024admitted-cohort bound. Successful covered offsets now complete late notices without another sync; delayed same-stripe notification regression proves one sync. |
| O2 device worker | Answered: one monotonic device budget deliberately serializes handle syncs; this is explicit in KV1/GROUPED-FLUSH and measured syncs/row. Parallel device sync optimization remains unselected. |
| O3 idle gather | Fixed docs: arrival starts a full gather window. PLAN amendment locates the departure; measured idle/background latencies and0/1/50ms sensitivity are reported without an optimum claim. |
| O4 sync failure | Fixed code: first failed sync permanently fences flush results for that stripe; late queued waiters receive16 without retrying sync. Injected sync failure regression proves both late callers error16, one attempt, new writes10 and no committed publication. |
| O5 clock steps | Fixed code: device spacing uses Instant elapsed time; deadlines retain existing wall-clock contract. |
| O6 state cap | Answered/owner handoff: stripe bytes include overwrites/tombstones and cap can refuse existing-over-limit writes while preserving reads. F-E6-21 owns CLI --state-limit-bytes; bounded heartbeat/epoch sidecars are outside this stripe cap. |
| O7 queue charge/busy | Fixed docs: complete representation replaces prior value-only wording; a permanently too-small queue returns6 on retries until configured differently. Refusal ordering and meanings otherwise retained. |
| O8 mem slots | Answered: explicit mem opt-in briefly charges the same bounded stripe admission budget; docs name it. |
| O9 helper waves | Fixed docs:128distinct keys require up tofour helper waves/stripe rounds, with measured initial0.72–0.85s batches. Final covering-extent fix removes redundant syncs. |
| O10 helpers/poison | Fixed helper count to min(32,distinctkeys); documented restart requirement after poisoned gate. Added simultaneous-batch busy test proving no refused item admission. Other mutex poisoning remains existing panic behavior. |
| O11 cross-key ordering | Answered: client seq keys/reply order stay intact; cross-key versions are opaque and not ordered. Same-key regression proves input order; no ordered stream promise. |
| O12 partial checkpoint | Answered: test-only variable must be unset; fence holds stripe lock, so diagnostics can wait at injected pause. Normal committed gets use another lock. Hard-kill harness code is tests/integration.py Server.stop (subprocess.kill); client reconnect failures are uncertain outcomes, never acknowledgement. |
| O13 sync boundary | Partial verification retained: markers prove pre-sync worker boundary; five unpaused active flush kills preserve every acknowledged id but cannot prove residence inside Windows FlushFileBuffers. REPORT/result explicitly leave precise kernel-in-flight verification open. |
| O14 drop/deadline | Answered: all five workers drain/join. Existing contract permits post-admission completion after deadline; client timeout can be uncertain. No stronger bounded I/O completion promise added. |
| O15 get/diagnostics | Answered and verified: GET uses only committed map, diagnostics can wait on append lock. Added pure-Store Windows logical-read counters concurrent with1/8/32writers; counters exclude startup/replay and transport, not physical-device instrumentation. |
| O16 coverage bounds | Fixed meaningful regressions: exact-fitting state record admission, next-record capacity refusal, sync-failure fence, late covered notification and simultaneous-batch busy.1024item bound is read-code evidence rather than a1024thread stress; byte saturation is exercised. No claim of an over64cohort measurement. |
| O17 evidence binding | Answered honestly: initial binary was an uncommitted precursor and initial SHA was not captured before overwrite; wrong grouped_flush:false inherited metadata is retained as invalid metadata. Final committed-source burst plus SHA and final tests replace current-build claims. Failed harness runs are retained/disclosed, not counted as passes. |
| O18 comparisons | Answered: initial same-window matrix pairs batched membus and serial runtime APIs, explicitly different call shapes and growing baseline. Final burst is a separate versioned follow-up; no false simultaneity or identical latency comparison. Process logical transfers are not physical-drive bytes. |
| O19 memory caps | Fixed docs: combined RSS/dedup/value/metadata measured with explicit2GiB/400k caps; default256MiB/262144 does not demonstrate350k fit. Isolated metadata cost and expiry effect remain unknown. |
| O20 defining policy | Fixed KV1: actual closing rule, bounds, spacing, counter opcode and measured memory are inline; historical slice1 text is explicitly superseded. This table dispositions all observations and advances previous O11 grouping/O22 partial-append evidence. |
| O21 proposed1MiB | Fixed docs: PLAN amendment records deliberate retention of existing4MiB wire compatibility and bounded complete-frame admission; no silent1MiB implementation claim. |
| O22 client conditions | Answered: grouping measured at retained50ms by fewer syncs/row; reduced serial spacing is a different tradeoff. Scans remain later, metadata-only memory unknown, and lane83 judges flushing acceptance. |
| O23 covered offsets | Answered/fixed: successful-only coverage is retained; terminal sync-failure fence now also prevents re-syncing uncertain extents. Current burst/counters are bound to fixed source. |
| O24 timing seam | Answered: delayed-notice regression passed but relies on scheduler timing; its100/20ms seam is a test facility, not a production latency promise. No deterministic OS scheduling or kernel residence claim. |
| O25 optional opcode | Fixed harness compatibility: unsupported2 from an old minor0 server now records unavailable optional counters rather than failing a baseline run. Existing layouts remain unchanged; diagnostics may wait on log locks. |
| O26 later evidence | Answered: current Rust checks/read-counter runs/burst bind the later final source; earlier memory/latency/concurrency/kill/sensitivity/matrix are versioned as prior-source evidence. Empty stdout logs are valid successful silent helpers; JSON plus lineup brackets are the evidence, not an empty log's content. |
| O27 sensitivity | Answered: prior-source0/1/50ms serial/grouped tradeoff supports no optimal-default assertion. Covered-offset/monotonic changes have final-source durability/burst checks rather than silently relabelling earlier timings. |
| O28 terminal fence | Answered: even previously covered late notices after a later sync failure receive uncertain16 conservatively. Regression codes can arise from several fenced paths; code inspection locates the no-repeat-sync path, without overstating timing coverage. |
| O29 Instant | Answered: monotonic device spacing matches contract; no artificial clock-step test needed for unchanged wall-clock deadlines. |
| O30 exact fit/helper | Answered: test records use equal fixed-field/key/id/value lengths; exact fitting record succeeds then next record7. Helpers min32distinctkeys and concurrent-batch refusal test pass. |
| O31 defining text/times | Fixed §7 in place to current queue charge/combined RSS scope; added final15.106s/294sync result beside explicitly initial batch times. |
| O32 read overlap | Fixed measurement generator: gets now continue until every writer's os/flush phases complete, with at least10000reads and observed maxreadtime. Counter scope remains pureStore WindowslogicalIO. Earlier file retained; final file records the stronger overlap. |
| O33 source hashes | Answered: SOURCE-BINDING.json and REPORT.md bind finalffe865f server source/binarySHA/timestamps, separate from earlier source-unbound matrix. Observe did not independently read those out-of-brief records; that limit stays visible. |
| O34 all observations | Fixed: this table now individually dispositions O1–O34. O26 explicitly identifies empty stdout plus actual JSON/lineup evidence, O25 supports older-server unsupported2. |

## F-E6-21 resident review

Actual observe-class return: e4/act-work/F-E6-21/review3-return.json;
located observations: review-observations.md in the same folder, O-R0..O-R15.
First two pool profiles returned API429 and supplied no observations (review-return.json,
review2-return.json; ledger457). Dispositions apply to the resident review-fix
commit and its later state-cap integration with F-E6-20 implementation2b75022.

| Observation | Disposition |
|---|---|
| O-R0 working-tree base | Answered: initial observer read working files at aded603; resident files are now committed. Supplemental observer receives the final committed diff including health and state-cap integration. |
| O-R1 overlap drain | Retained safe cancel+drain ordering. Fixed immediate-completion byte accounting: ReadFile/WriteFile get null done pointers and GetOverlappedResult supplies completion bytes. WAIT_FAILED remains a fatal pipe-creation/connection error; no recovery claim is made for a broken kernel event. |
| O-R2 event per I/O | Answered: one bounded event per active I/O retained; no measured syscall-cost claim. Reusing events is a potential optimization with its own measurement, not a correctness fix. |
| O-R3 ACL guards | Retained current-user/SY DACL, remote rejection, first-instance guard and64-client cap; service must use xteam's InteractiveToken. |
| O-R4 idle witness | Fixed: read starts immediately, rejects client TimeoutError, records server BrokenPipeError109 and checks >=100ms and <1.2s at200ms idle. Drop releases slot and ingress reservation; exact release counters are not added. |
| O-R5 trickle reservation | Answered explicitly in RESIDENT and KV1: trickling retains ingress as well as slot;64MiB aggregate budget stays bounded. Total-frame deadline remains an uncommissioned policy choice. |
| O-R6 failed heartbeat writes | Fixed: attempts, requested bytes and failures count failed temp/replacement paths and appear on recovery. Witness denies delete sharing, observes failed replacements then successful recovery. Persistent failure becomes stale health; retries are interval-bounded. |
| O-R7 rename/health | Fixed misleading premise and removed custom MoveFileExW FFI; std::fs::rename replaces the heartbeat. Native health checks hello+health+actual pipe-server pid against heartbeat pid/epoch/freshness, so heartbeat-only liveness is not service health. |
| O-R8 object size | Fixed current doc bound:<512bytes per object,<1024 requested bytes/minute at30s. Historical91-byte object predates failure/state counters; final raw result records current sizes, physical bytes unmeasured. |
| O-R9 restart | Fixed witness: reuse one LOCAL pipe name with an old client handle retained through kill/restart; append torn header after an acknowledged frame in a populated stripe and check exact size restoration+os/flush readback. Fleet restart of exact resident endpoint is recorded separately if registered. |
| O-R10 state cap seam | Integrated F-E6-20 exact admission/replay counter through Config.state_limit_bytes and positive CLI option. Test capacity7 before acceptance+old reads; heartbeat state_bytes/limit and75% warning (default6GiB of8GiB) expose retained state across replay without drive reads. No release installed before these checks pass. |
| O-R11 account/LOCAL | Answered by lane88 eco seq452: task runs per-user xteam InteractiveToken; RESIDENT states account requirement. Strengthened witness uses LOCAL pipe namespace; exact service endpoint is checked after owner registration. |
| O-R12 install/logs | Answered: stopped next/previous replacement and hash record outside worktrees; lane88 owns1MiB+prior launcher logs and minute restart. Final Act report locates installation and fleet checks or states registration wait. |
| O-R13 docs/usage | Fixed KV1 section6 cutoff statement and CLI usage, including idle, heartbeat, state limit and existing dedup controls. |
| O-R14 validation | Retained positive bounded idle_ms and zero-or>=100 heartbeat interval; added positive state cap validation. |
| O-R15 mirror reconnection | Answered in recorded lane83-shadow.md: idles reconnect and new epoch requires deterministic-key/op_id reconciliation. Shadow remains lane83 choice and file ledger primary. |

### Final supplemental observe return

e4/act-work/F-E6-21/review-supplement-return.json and
review-supplement-observations.md read committed1c52fe8 and the2b75022 seam.
Final dispositions (O-S0..O-S11):

| Observation | Disposition |
|---|---|
| O-S0 reachable commit | Answered: rebasing changed preliminary health commit1e66fbe to0d38394; final installation message names the reachable merged source and binary hash. F-E6-20 merges first; resident rebases on its main before its separate merge. |
| O-S1 line endings | Fixed KV1 back to LF; real final diff adds only the idle statement and state-cap paragraph rather than whole-file CRLF churn. |
| O-S2 native health | Retained checked hello/health/pid/epoch/freshness. RESIDENT now says heartbeat must stay enabled for resident health; zero is a foreground-test option. Other invalid-file/refusal paths are code-read evidence rather than claimed runtime witnesses. |
| O-S3 busy connection window | Fixed bounded retry for ERROR_PIPE_BUSY231 for up to2s,5ms sleeps, then normal2s I/O bounds. Witness holds64 idle pipe clients and health succeeds after slots clear rather than treating the instance-creation gap as permanent failure. |
| O-S4 pressure consumer | Answered transparently: heartbeat exposes pressure but no automatic alert consumer exists yet. Routed the reporting-policy question to lane88 with the final installed-binary evidence; health availability remains independent of capacity so reads stay useful. This is open owner integration, not a wired alert claim. |
| O-S5 conservative cap accounting | Fixed doc: uncertain failed append charges full attempted frame until replay recounts; heartbeat excludes pending reservations; persistent removes also refuse and cannot shrink logs. Mem remains explicit opt-in with its own independent memory cap. |
| O-S6 maintenance reserve | Answered staging: current cap is8GiB unrotated stripe logs; PLAN's1GiB reserve/7GiB ordinary ceiling must be provisioned before a rotation/maintenance rollout. No snapshot/archive/maintenance tier is supplied by this commission. Operator/accounting choice remains open for that later scope. |
| O-S7 heartbeat accounting | Retained successful replacement+attempted-byte/failure counters; raw witness shows attempts=writes+failures and recovery. heartbeat.tmp is never consumed as health; physical/metadata writes stay unmeasured. |
| O-S8 stale client timing | Strengthened witness sends a partial header immediately before kill, records kill duration<200ms idle bound, then receives BrokenPipeError109 from that retained handle after restart. Same populated-stripe torn tail and os/flush readback remain. Exact registered endpoint is the separate fleet witness. |
| O-S9 original dispositions | All original16 observations remain individually accounted above; final report locates tests and registered-service check. No new protocol/state format change. |
| O-S10 obsolete registration paragraph | Fixed removed pre-answer paragraph; seq452's logon+minute policy is the single current shape. |
| O-S11 KV1 capacity routing | Fixed section7 names persistent state byte cap, replay+inflight accounting, capacity7 for tombstones, and RESIDENT remedy/diagnostic warning. |


## e6 F-E6-20 idle-immediate review

Observe return: runs/e6/act-work/F-E6-20/review-return.json;17located observations in observations.md, quota output review-quota.json. Every observation below has a within-commission fix or a scoped answer; beyond-scope adoption/measurement choices remain with their owners.

**O1.** Answered, completion reserved for merge. READY installation plan compares compiled source hashes/main release and remeasures any integrated or different binary. No staged measurement is called a merged one; MERGED-SOURCE-BINDING.json will record exact merged source/binary.

**O2.** Answered. The report records real append_all versus bounded non-atomic batches; the journal wins the bulk transaction. The final lane83 question explicitly includes this result and does not reuse the old serial-journal burst yes as a new adoption claim.

**O3.** Fixed. final-window has501idle rows with fold sample500 and cadence initialized from499actual recorded tail rows with fold sample1. Runtime38c195f uses >500, not >=500. Boundary costs are in raw samples; cadence boundary average is not asserted as steady amortization.

**O4.** Answered in GROUPED-FLUSH/KV1 and report. Concurrent low-rate writers retain gathering; serial workloads are the commissioned first-client shapes. No parallel latency distribution or general single-writer guarantee under another client is claimed.

**O5.** Answered. One sync/row at93/min and brief501-row serial sample plus bytes/co-tenant counts account for measured load. No global20/s ceiling remains. Sustained serial/physical-device traffic is unmeasured, with no inferred optimum or drive-safety promise.

**O6.** Fixed/qualified. policy-forms.json measures source-bound std-only worker prototypes at per-request50/0 and second-arrival closing, beside the chosen policy. New wire-negotiation/interface cost is explicitly excluded and unknown. Forms, timings and reasons are now in both policy docs and PLAN addendum.

**O7.** Answered with explicit limits. Final between-mark kills and paired completed-call durations meet the commission's stated marker criterion. Kernel interruption/termination ordering, dirty-page state and power-loss behavior are not observed. Extent means32MiB complete OS-ack records with no earlier explicit sync, not proof every page remains dirty.

**O8.** Answered. Killed attempts acknowledge OS records; trigger is uncertain and excluded. OS cache replay alone proves no flush durability. Completed witness attempts and every final-window flush-acked workload separately replay ack2 IDs/values. No power-cut or interrupted-kernel proof is inferred.

**O9.** Fixed reporting. sync-kills.json lists one earlier unclassified join-aborted kill, three earlier verified c57b088 kills and final source859423d attempts. All artifacts remain; report separates verified-source denominators and total earlier attempts.

**O10.** Fixed. The old timing test is accurately named delayed_flush_notification_preserves_acknowledgement. New owner-level late_notice_for_already_covered_extent_needs_no_second_sync asserts exactly one sync with a genuinely late same-extent notice.

**O11.** Fixed. Actual enqueue helper is tested with a forced pause between first arrival classification and delivery; a second sender cannot overtake and is classified gathered behind the queued first. During-sync test waits for enqueue notification instead of20ms sleep.17Rust tests pass.

**O12.** Answered. Full raw samples, p95/max/tail samples and co-tenant context are now in report. Serial-client ordering and decrement-before-ack remove intentional gather eligibility, but no server per-sample classification trace or attribution of slow calls is claimed.

**O13.** Fixed. Failed entering/returned marker writes only log test diagnostics and never alter the sync result. Invalid marker parent yields two successful subsequent flush acknowledgements; marker_failure_preserves_sync_result is true. Ordinary release omits the lookup and remains unaffected.

**O14.** Fixed. Both policy docs restored to LF, matching761cacb; substantive diff is separated from prior EOL churn.

**O15.** Fixed. KV1§9 now says historical pre-grouping comparison basis, and current501/93/3065 source-bound evidence/ack boundaries are explicitly located.

**O16.** Fixed. GROUPED-FLUSH calls ffe865f historical e4 and locates its evidence separately from current859423d. Different-build burst effect is not isolated or claimed.

**O17.** Answered as outside this commission. Scans are F-E6-24; capacity/shadow growth concern is retained, with no new capacity measurement or primary-use choice. The lane83 question is the authorized lone-writer question and includes durability/bulk limits.
