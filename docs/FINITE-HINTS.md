# Finite coalesced subscription hints

The opt-in HELLO identity feature is `;subscription-contract=2`; the complete
affirmative suffix is eight literal bytes `MSH2 01 00 01 00`. Version-one MSC1
discovery remains negative. Empty, unknown, truncated, trailing or negative
discovery cannot select version two. Existing change/1's 46-byte limits and wire
layout remain separate. For refusal vocabulary, finite clients also select the
existing subscribe feature and parse its limits before the finite suffix.

Only profile u16=1 is implemented: volatile buffered coalesced namespace/prefix
dirty hints. Publication stores the latest matching sequence in one scalar.
There are no payloads, replay, every-event count, lossless guarantee, recipient
acceptance, no-buffer mode or end-to-end deadline guarantee. Other profiles refuse
unsupported. A sink write completes locally only. The single existing worker
serializes all recipients; queue age includes preceding work. The chosen active
write budget excludes native cancellation drain and is not a physical return
deadline. Callers needing stronger semantics must decline this profile.

Create opcode 28 uses the ordinary deadline/op-id/namespace prefix followed by
profile:u16, lifetime_ms:u64, write_ms:u32, LP incarnation:16, LP recipient:16,
LP prefix and LP local endpoint. All integers are little-endian. Lifetime and
write budget must be positive and representable in the authority's clock range.
No new time policy cap is imposed. The Store's real common account grants all
managed row/node/string/slot/encoding demand before allocation. It may refuse
capacity; even a quiet small Store can have no allowance due to the inherited
prospective legacy envelope. Count is resource-derived, with no 64-row limit in
this mode. Native kernel/allocator RSS remains unmeasured.

The 64-byte receipt is epoch:u64, id:u64, incarnation:16, recipient:16,
authority_until_ms:u64, generation:u64. The endpoint is immutable for that ID.
Epoch and IDs are authority-scoped, never reused; the two nonzero fresh caller
generations are a receiver-checked capability, not an authenticated OS identity
or evidence of death. Receivers must compare all binding bytes before accepting
a hint. Restart/replacement needs a fresh incarnation and endpoint generation.
No process identifier or endpoint-name equality is treated as proof of liveness.

Renew opcode 29 and cancel opcode 30 use the ordinary request prefix and an LP
64-byte exact receipt. Renewal atomically compares the full binding, namespace,
deadline and generation, then increments generation and chooses now+lifetime.
Stale renewal/cancel refuses missing; equality with expiry is expired. UTC jumps
do not extend authority. Quiet expiry uses the existing worker's timed wait.
In-flight I/O can finish after authority expiry; its child retains its actual
charge through connection release and completion. Late failure can remove only
the exact generation that started it. Rows and their charges settle outside Hub.

A length-framed 64-byte hint is length:u32=60, `MBH2`, binding:48, sequence:u64.
No per-publication allocation is needed in this mode. Receipt/generation custody
must survive lost/malformed replies. An op ID is not deduplication for creation.
Never blindly retry or create a replacement after an uncertain create; endpoint
or operation lookup cannot resolve it. Authority monotonic deadlines are not
portable to a caller clock. Without actual clock/incarnation witnesses, exact
finite-expiry settlement of an unknown creation remains unresolved debt.

This is opt-in source capability. Legacy callers retain quiet-retention exposure;
Python, caller migration, read-cohort composition, operating qualification and
installation are separate work.
