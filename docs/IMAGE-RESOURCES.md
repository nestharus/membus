# Caller-funded complete stopped images

The funded image API extends the existing consumed-owner stopped-root capture.
It is available on Windows, where its directory/name workspace is bounded by
the actual Win32 representation. Other platforms return `Unsupported` before
funded work; the existing unfunded image APIs retain their platform contract.
This is explicit capability applicability, not cross-platform qualification.

## Actual accounts and callable operations

`Store::image_budget(metadata_pool_bytes, ImageLimits)` reserves the complete
metadata pool against the same scan count, snapshot bytes and current memory
allowance as funded namespace originals. It uses the existing `ScanCharge` and
`OriginalBudget` owners, under their scan/state coordination, before growing any
image metadata. No shadow Store is opened. An unavailable Store or insufficient
actual allowance returns the existing `PublicationError`.

For standalone validation, `ImageBudget::new(OriginalBudget, ImageLimits)` takes
an existing finite caller-owned allocation account. Its own fixed account object
is reserved before allocation. Both routes produce an actual shared account;
`ImageBudget::clone` shares its counters. No manifest, completion, wire count or
serialized budget integer creates an account or a grant.

`ImageLimits` supplies finite shared allowances for:

- `backing_bytes`: admitted regular-file output bytes, including image envelope
  and payload files and any selected completion-custody output;
- `io_bytes`: admitted read/write extents, including repeated inventory hashing,
  copy, readback and validation;
- `operations`: admitted path/member/filesystem steps and byte-processing work.

These are logical managed backing/work accounts. They do not measure physical
filesystem blocks, allocator bookkeeping, OS-private memory, RSS, free RAM,
CPU instructions, elapsed time or device durability. Directory population is
funded through metadata and work rather than a fictitious payload byte size.

The supported calls are:

```rust,no_run
use membus::{Store, client::Ack, atomic::OriginalBudget,
    recovery_image::{CaptureRequest, ImageLimits, ImageBudget,
                     validate_image_funded}};
use std::{io, path::Path};

fn capture_owned(
    store: Store, image: &Path, independent_custody: &Path,
    pool_bytes: usize, capture_metadata: usize, custody_metadata: usize,
    validation_metadata: usize, limits: ImageLimits,
) -> Result<(), Box<dyn std::error::Error>> {
    // `store` is the actual library owner selected by the data owner.
    // pool_bytes must fund simultaneously retained results and unused headroom.
    let budget = store.image_budget(pool_bytes, limits)
        .map_err(|e| io::Error::other(format!("image admission: {e:?}")))?;
    let captured = store.capture_stopped_funded(image, CaptureRequest {
        source_label: "selected-complete-root".into(), ack: Ack::Flush,
    }, &budget, capture_metadata)?;
    let original = captured.completion().expect("successful Flush return");
    let custody = original.persist_new_funded(
        independent_custody, &budget, custody_metadata,
    )?;
    let checked = validate_image_funded(
        image, Some(custody.completion()), &budget, validation_metadata,
    )?;
    assert_eq!(checked.manifest(), captured.manifest());
    // These results retain their own reservations until their last shared owner
    // drops. Explicit immutable image and independent receipt custody are still
    // the caller's responsibility; no permanent archive manager is introduced.
    Ok(())
}

fn validate_owned_input(
    image: &Path, original: Option<&membus::recovery_image::CaptureCompletion>,
    caller_memory: OriginalBudget, allowance: usize, limits: ImageLimits,
) -> Result<(), Box<dyn std::error::Error>> {
    let budget = ImageBudget::new(caller_memory, limits)?;
    let checked = validate_image_funded(image, original, &budget, allowance)?;
    assert!(checked.manifest().complete);
    Ok(())
}
```

The destination may itself be the delivered immutable image. These operations
do not require a second generic copy, an archive lease, an online snapshot
manager, serving restore authority, or the entire client SDK.

## Admission, returned allocation and expenditure lifetimes

Each operation explicitly requests `metadata_bytes` from its real account before
starting. The entire requested reservation, including unused headroom, stays
live through its returned `FundedImage`, `FundedCompletion` or `ImageFailure`.
Their managed growing vectors, strings, path sets, directory workspaces, owned
handle tables, encoded manifest and decoded validation populations are admitted
before allocation. Temporary backing is released from the operation's internal
working allowance only after its data is no longer used. The account reservation
itself remains conservative until its result/error owner drops.

`FundedImage::clone` and `FundedCompletion::clone` share their returned allocations
and reservation; they do not deep-copy a manifest or witness. `manifest()` and
`completion()` borrow immutable data. An explicit caller-made deep copy through
legacy types or ordinary collection methods is caller-owned allocation, outside
this operation's account. There is no owned-manifest extraction that detaches
provider-returned allocations from their charge. An actual Store-backed pool
continues holding its entire scan reservation until its last budget/result/error
owner disappears, even after the consumed Store has stopped.

Backing, I/O and operation credits are consumed conservatively at admission and
are **not refunded** by an I/O failure or result drop. For example, a whole next
file's extent is admitted before creating/copying it. A subsequent failure may
leave fewer bytes than that admitted extent. Dropping the record releases its
metadata allocation reservation, never deletes an image, and never treats a
retained file as newly available disk space. A caller explicitly supplying a new
account is a new resource decision; these APIs never do so automatically.

Payload and ownership-file bytes stream through fixed buffers and held handles.
No whole-payload RAM reservation is required. Source growth beyond an admitted
file extent refuses without copying the extra bytes. Inventories and configuration
are rechecked; no funding shortage permits filtering, pagination, omission or
incomplete success. The configuration comparison streams its original canonical
segments rather than allocating a duplicate configuration encoding.

Work charging includes member/path processing, hash bytes, encoding/decoding
bytes, copy blocks and selected filesystem/barrier steps. The deterministic
population sorts are admitted by member count over already funded strings.
Work counters are explicit logical units, not universal instruction accounting
or a latency guarantee. I/O extents include the final bounded growth probe.

Legacy entry/manifest cutoffs remain on legacy calls. Funded populations and
manifests have no replacement row-count or manifest-size constant: representable
lengths, literal-body consistency and actual finite account admission must fit.
Windows alternate-stream/layout representation limits remain unsupported-layout
refusals, not a capacity allowance to omit those inputs.

## Exact refusal and completion meaning

`ImageFailure::shortfall()` identifies `Metadata`, `Backing`, `Io` or `Operations`
with requested and available amounts. `progress` records the actual stage,
requested versus achieved Ack, requested versus reserved metadata, peak managed
metadata, admitted backing/I/O/work and confirmed complete copy-block writes.
A failing write may have written an additional prefix; the confirmed counter
does not certify absence of other bytes. Admission failure reports zero reserved
metadata when no reservation was retained.

Output states are `Untouched`, `Incomplete`, `CompletionUnknown` and `Complete`.
Validation is read-only and reports `Untouched` for output, even when it returns
a successfully validated complete image. Failed captures retain their artifacts:
there is no cleanup, overwrite, hidden retry or silent account increase.

Successful Os capture establishes the complete checked byte image without a
destination persistence claim. Successful Flush capture establishes the existing
selected file/name barriers and readback, then issues the original separate
`CaptureCompletion`. After late file/directory barrier failure, requested Ack is
still Flush, achieved whole-image Ack is absent, and output remains
`CompletionUnknown`. Readable marker bytes never reconstruct the missing witness.

`validate_image_funded` separately funds complete envelope/file/directory checks,
manifest loading and decoded allocation before use. It requires the matching
original successful completion for Flush. It does no new image barrier and
never backdates a later persistence event. A completion read/decoded by its
legitimate custodian must retain its original successful-return provenance;
checksums and the receipt encoding are not authentication or serving authority.
`persist_new_funded` funds new-only original-completion custody and observes file
sync, parent sync and readback. Failed/unknown custody writes remain retained.

Original mutation Ack and raw bytes remain unchanged. Capture still consumes
the actual owner, settles all original workers, timer/signal obligations and
health, and holds source/catalogue ownership through the complete stopped cut.
Unknown ordinary files, empty directories and history remain part of the same
whole-root population. These resource changes do not add a restore promotion,
authority adapter, live handover or an application-wide dataset declaration.

## Qualification boundary

The authored public controls use explicit private roots and no cleanup. They
exercise complete raw populations, real Store/shared account admission, clone
and drop lifetimes, insufficient metadata/backing/I/O/work, partial copy,
independent validation, malformed bodies, missing/corrupt members, separate
completion custody and preservation of Os/Flush/stopped-cut controls. The
allocation observer rejects concrete encode-then-refuse and read-then-refuse
neighbors. Large-payload streaming is a finite software control, not an admitted
maximum-load, OS allocator/RSS, outage or power-loss claim. Actual source/test
selection and exclusions belong to the accompanying qualification record.
