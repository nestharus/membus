# Stopped library images and quarantined recovery

The legacy capture/validation calls below are **unfunded compatibility APIs**.
The selected caller-funded capability is described in [IMAGE-RESOURCES.md](IMAGE-RESOURCES.md):
`Store::image_budget`, `capture_stopped_funded`, `validate_image_funded` and
`CaptureCompletion::persist_new_funded` use actual allocation/resource owners.
Neither the legacy count limits nor a budget integer in a certificate funds them.

`Store::capture_stopped` consumes the actual library Store owner. It captures
the complete recursive population of ordinary file bytes and directories under
that Store root, including unknown regular files, empty directories, catalogue
sidecars, old imports and historical inputs. It does not select namespaces or
replace missing history with an empty population.

The API requires an absolute new destination with an existing parent. It never
overwrites an existing destination, removes an input or cleans up a partial
output. Callers retain successful and refused images and choose their own
explicit output locations. The image contains `capture.started`, `root/`,
`capture.manifest` and a final `capture.complete`. A started image without a
valid completion is incomplete; opening its payload directly is unsupported.

```rust,no_run
use membus::{client::Ack, recovery_image::{CaptureRequest, recover_quarantined_with_completion}, Config, Store};
use std::path::Path;

fn inspect_stopped(source: &Path, image: &Path, private_working: &Path) -> std::io::Result<()> {
    let config = Config::default();
    let store = Store::open(source, config.clone())?;
    let manifest = store.capture_stopped(image, CaptureRequest {
        source_label: "selected-library-store".into(),
        ack: Ack::Flush,
    })?;
    assert!(manifest.complete && manifest.readback);
    let completion = manifest.completion().expect("successful Flush return");
    // Retain this successful return outside the selected image/rollback domain.
    // Persist it to a caller-selected new custody path before handing it off.
    let recovered = recover_quarantined_with_completion(image, private_working, config, completion)?;
    let _value = recovered.try_get("application", "example")?;
    assert!(recovered.health2().read_only);
    Ok(())
}
```

The source owner is consumed, never inferred from an Arc count. The executable's
continuing heartbeat holds an Arc and cannot provide that owned value until its
owner is joined. A timer service instead holds a Weak reference, so capture
separately checks its actual running state and refuses an unjoined timer owner.
Capture joins the snapshot reaper, maintenance ticker, append owners and flush
owner; it observes their join failures. Signal delivery must have no active
subscription or delivery bytes in flight, then its worker is stopped and joined.
This is a library capture operation. It provides no resident shutdown command,
host-wide writer exclusion or live OS lifecycle settlement protocol.

Store and catalogue ownership locks remain held during capture. Their exact
bytes are copied through existing read handles, avoiding release/reacquisition
of a Windows exclusive lock. Inventory is checked before and after copying,
and the destination population and digests must match. Unknown files that
cannot be read cause refusal. Links, reparse points, nonportable paths, special
files and unsupported Windows attributes cause refusal. Windows alternate data
streams on files or directories are detected and refused rather than omitted.
This format preserves ordinary file contents and directory population, not
ACLs, extended attributes, hardlink identity, sparse allocation or OS authority.

The versioned manifest binds the source label and epoch, source UTC and
monotonic observations, completion time, exact effective configuration, original
record bytes, entry sizes and SHA-256 digests, the whole population digest,
completeness, requested versus achieved image Ack, readback and barrier counts.
`Ack::Os` makes no destination persistence claim. `Ack::Flush` requires all
payload files, directories, the manifest and completion marker to pass their
selected file/name barriers and readback. Per-entry original mutation Ack values
remain unchanged in the copied record bytes: flushing an image does not rewrite
an earlier caller's receipt. Barrier counts describe payload file barriers and
directory barriers; the manifest and start/completion files have their own
mandatory barriers in the completion protocol.

`validate_image` checks the entire envelope and payload before mutable recovery.
For `Ack::Os`, it returns the checked manifest. For `Ack::Flush`, readable image
bytes alone leave **original settlement unknown**, so this call refuses. Use
`validate_image_with_completion(image, completion)` and
`recover_quarantined_with_completion(image, working, config, completion)` with
the original successful `CaptureManifest::completion()` return instead.

The capture issues `CaptureCompletion` only after the completion-file sync,
final directory barrier and whole-image readback have succeeded. The witness
binds the exact encoded manifest, including its requested and achieved Ack.
`to_bytes`, `from_bytes` and new-only `persist_new` support explicit caller
custody. Observe the custody writer's result; retain exact original bytes and
their successful-return provenance separately from the image. The format is
not authenticated: constructing a witness from a marker/hash is unsupported,
just as rewriting image checksums cannot establish authentic source history.
No witness is returned after a capture error. Failed outputs, including readable
completion bytes, are retained unchanged. Lost return evidence therefore remains
unsettled; validation does not resync the image, infer success from readability,
erase a failure or backdate a later barrier. This is a post-barrier return
contract, not an independent serving grant or physical power-loss proof.

The in-image manifest describes the selected completion protocol; its declared
Flush achievement is certified only by the matching original successful return.
The version-one byte layout is unchanged. Earlier images with no independently
retained post-barrier return cannot acquire that evidence from their bytes.
The completion hash binds the manifest; every file and directory must appear
exactly, with no unmanifested addition, missing entry or changed byte. These
checksums detect corruption and population differences; they are not signatures
or a guarantee against an adversary who can rewrite the image and its checksums.

`recover_quarantined` requires the bound configuration and a new absolute private
working root. It validates first, writes and synchronizes a permanent quarantine
marker before copying state, copies and reads back the complete population, then
performs startup recovery only on that private copy. Before ordinary startup it
checks every catalogue Import, including superseded records, against the actual
supported legacy layout: `legacy-<epoch>` with flat `stripe-0.log` through
`stripe-3.log` and optional `import-policies.txt` provenance. Replay and the move
boundary enforce the same rule before using embedded paths. Invalid historical
names refuse; they are never sanitized or dropped. This preserves valid import
movement, backup verification, read-only provenance and retry history.

Quarantined images support both Windows and the existing non-Windows Store
ownership domain. Windows keeps share-denied live ownership. On non-Windows,
the wrapper removes only the copied `owner.lock` from the exclusively created,
fenced private working root and syncs that directory before create-new startup.
That root has never hosted a Store owner. The retained image's ownership bytes,
preexisting roots and live source owners are untouched. This source behavior
still requires qualification on each target platform; Windows controls do not
establish Linux execution. It inspects Health2 after
startup: an opened Store with missing Closed chunks, read-only state, storage
error, degraded expiry or failed catalogue maintenance is a refusal. It never
passes the retained original image to Store recovery. The original image is
revalidated across the copy and successful recovery; refusal tests independently
verify its unchanged contents too. All partial working roots remain fenced and
retained.

The returned `QuarantinedRecovery` exposes fallible reads and Health2 only. It
does not expose Store, dispatch, mutation, timers, subscriptions or an unfence
operation. Ordinary `Store::open` refuses a marked root after restart. Runtime
maintenance is not started for quarantined recovery; remaining library workers
are stopped before its inspection handle is returned. Copied timer records are
data and never cause a restored timer service to start.

Validation grants no recovery execution authority. A copied epoch, label,
manifest or operation token cannot distinguish a restored history from its
former writer. Serving restoration still needs an actual noncopied authority
witness that fences the former writer and binds the selected history, original
operation identities, receipts and post-capture effects. This API introduces
neither such a witness nor an authority manager. It does not prove complete
host rollback, live data handover, retention/archive eligibility, caller restart
custody or application-wide state sufficiency.

The authored `recovery_image` integration controls require an existing absolute
`MEMBUS_PROVIDER_TEST_ROOT`. They retain every source, image and working copy;
there is no Drop deletion or implicit fallback location. They include unknown
files and empty directories, corrupt/additional image entries, missing
completion, changed configuration, an actual Weak timer-owner refusal, Windows
alternate-stream refusal, permanent quarantine and an internally checksum-valid
but missing-Closed-chunk image that must fail recovered Health2 inspection.


The refusal oracle records relative path, file/directory type, file length and
digest, including empty directories. An authored neighboring function that adds
an empty directory and then returns refusal is explicitly rejected by that
oracle. Barrier fault controls fail the required completion-file or final
directory barrier after marker bytes are readable; adjacent earlier manifest
barriers remain incomplete. These are selected software fault controls, not
hardware failure or whole-suite qualification.
