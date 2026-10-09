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
