//! Public funded image contract on fresh, explicitly retained nonproduction roots.
#![cfg(windows)]
use membus::{
    atomic::OriginalBudget,
    chunks::{sha256_file, Sha256},
    client::Ack,
    recovery_image::*,
    CachePolicy, CacheRule, Config, Mutation, Store,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

struct ObservedAllocator;
thread_local! {static OBSERVE:Cell<bool>=const {Cell::new(false)};static LARGEST:Cell<usize>=const {Cell::new(0)};}
unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let _ = OBSERVE.try_with(|a| {
            if a.get() {
                let _ = LARGEST.try_with(|n| n.set(n.get().max(l.size())));
            }
        });
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let _ = OBSERVE.try_with(|a| {
            if a.get() {
                let _ = LARGEST.try_with(|m| m.set(m.get().max(n)));
            }
        });
        System.realloc(p, l, n)
    }
}
#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;
fn observed<T>(f: impl FnOnce() -> T) -> (T, usize) {
    LARGEST.with(|n| n.set(0));
    OBSERVE.with(|a| a.set(true));
    let result = f();
    OBSERVE.with(|a| a.set(false));
    (result, LARGEST.with(Cell::get))
}
fn root(label: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_IMAGE_TEST_ROOT").expect("explicit retained image fixture root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let p = base.join(format!(
        "funded-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&p).unwrap();
    println!("retained image fixture {}", p.display());
    p
}
fn config() -> Config {
    Config {
        cache_bytes: 0,
        signal_bytes: 0,
        flush_ms: 0,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        snapshot_bytes: 16 * 1024 * 1024,
        capacity: 64 * 1024 * 1024,
        ..Config::default()
    }
}
fn limits() -> ImageLimits {
    ImageLimits {
        backing_bytes: 16 * 1024 * 1024,
        io_bytes: 128 * 1024 * 1024,
        operations: 128 * 1024 * 1024,
    }
}
fn budget() -> ImageBudget {
    ImageBudget::new(OriginalBudget::new(16 * 1024 * 1024), limits()).unwrap()
}
fn request(ack: Ack) -> CaptureRequest {
    CaptureRequest {
        source_label: "authored-complete-image".into(),
        ack,
    }
}
fn capture(p: &Path, ack: Ack) -> FundedImage {
    let store = Store::open(&p.join("source"), config()).unwrap();
    let b = store.image_budget(8 * 1024 * 1024, limits()).unwrap();
    store
        .capture_stopped_funded(&p.join("image"), request(ack), &b, 2 * 1024 * 1024)
        .unwrap()
}
fn population(root: &Path) -> BTreeMap<PathBuf, (bool, u64, String)> {
    fn walk(root: &Path, p: &Path, map: &mut BTreeMap<PathBuf, (bool, u64, String)>) {
        for e in fs::read_dir(p).unwrap() {
            let path = e.unwrap().path();
            let meta = fs::symlink_metadata(&path).unwrap();
            assert!(!meta.file_type().is_symlink());
            map.insert(
                path.strip_prefix(root).unwrap().into(),
                (
                    meta.is_dir(),
                    if meta.is_dir() { 0 } else { meta.len() },
                    if meta.is_dir() {
                        String::new()
                    } else {
                        sha256_file(&path).unwrap()
                    },
                ),
            );
            if meta.is_dir() {
                walk(root, &path, map);
            }
        }
    }
    let mut map = BTreeMap::new();
    walk(root, root, &mut map);
    map
}
fn copy(source: &Path, target: &Path) {
    fs::create_dir(target).unwrap();
    for e in fs::read_dir(source).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            copy(&e.path(), &target.join(e.file_name()));
        } else {
            fs::copy(e.path(), target.join(e.file_name())).unwrap();
        }
    }
}

#[test]
fn resident_workers_settle_before_capture_of_the_original_store() {
    use std::{
        io,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    let p = root("resident-workers");
    let source = p.join("source");
    let store = Arc::new(Store::open(&source, config()).unwrap());
    let epoch = store.epoch;
    let mut workers: membus::ResidentWorkers = store.start_resident_workers(60_000).unwrap();
    assert!(store.start_timers().is_err());
    let until = Instant::now() + Duration::from_secs(10);
    let wall = SystemTime::now() + Duration::from_secs(10);
    let check_refused = || Err(io::Error::other("owner refused before stop"));
    assert_eq!(
        workers
            .poll_stop(&store, until, wall, check_refused)
            .unwrap_err()
            .to_string(),
        "owner refused before stop"
    );
    while !source.join("heartbeat.json").exists() {
        assert!(Instant::now() < until && SystemTime::now() < wall);
        thread::sleep(Duration::from_millis(2));
    }
    let mut receipts = Vec::new();
    for (key, ack) in [("first", Ack::Os), ("second", Ack::Flush)] {
        let receipt = store.mutate(Mutation {
            ns: "resident".into(),
            key: key.into(),
            op_id: format!("original-{key}"),
            deadline: u64::MAX,
            ack: ack as u8,
            expected: None,
            bytes: key.as_bytes().to_vec(),
            remove: false,
        });
        assert_eq!((receipt.code, receipt.ack), (0, ack as u8));
        receipts.push((key, receipt));
    }
    let budget = store.image_budget(8 * 1024 * 1024, limits()).unwrap();
    let remaining_reader = Arc::clone(&store);
    while !workers.poll_stop(&store, until, wall, || Ok(())).unwrap() {
        thread::yield_now();
    }
    workers.join().unwrap();
    let store = Arc::try_unwrap(store)
        .err()
        .expect("reader still holds original Store");
    drop(remaining_reader);
    let store = Arc::try_unwrap(store)
        .ok()
        .expect("original Store after settlement");
    let image = p.join("image");
    let captured = store
        .capture_stopped_funded(&image, request(Ack::Flush), &budget, 2 * 1024 * 1024)
        .unwrap();
    assert_eq!(captured.manifest().source_epoch, epoch);
    assert_eq!(
        fs::read(image.join("root/heartbeat.json")).unwrap(),
        fs::read(source.join("heartbeat.json")).unwrap()
    );
    let completion = captured
        .completion()
        .unwrap()
        .persist_new_funded(&p.join("independent.custody"), &budget, 512 * 1024)
        .unwrap();
    assert!(validate_image_funded(&image, None, &budget, 2 * 1024 * 1024).is_err());
    let checked = validate_image_funded(
        &image,
        Some(completion.completion()),
        &budget,
        2 * 1024 * 1024,
    )
    .unwrap();
    assert_eq!(checked.manifest(), captured.manifest());
    let recovered = recover_quarantined_with_completion(
        &image,
        &p.join("working"),
        config(),
        completion.completion(),
    )
    .unwrap();
    assert!(recovered.health2().read_only);
    for (key, receipt) in receipts {
        let value = recovered.try_get("resident", key).unwrap().unwrap();
        assert_eq!((value.version, value.ack), (receipt.version, receipt.ack));
        assert_eq!(value.bytes, key.as_bytes());
    }
}

#[test]
fn complete_raw_population_and_separate_os_flush_meanings() {
    for ack in [Ack::Os, Ack::Flush] {
        let p = root("population");
        let source = p.join("source");
        let store = Store::open(&source, config()).unwrap();
        let bytes =
            b" {\"n\":18446744073709551615,\"fraction\":1.00e+2,\"escape\":\"\\u0061\"}\n\0\xff";
        assert_eq!(
            store
                .mutate(Mutation {
                    ns: "authored".into(),
                    key: "opaque".into(),
                    op_id: "raw-original".into(),
                    deadline: u64::MAX,
                    ack: 1,
                    expected: None,
                    bytes: bytes.to_vec(),
                    remove: false
                })
                .code,
            0
        );
        fs::create_dir(source.join("unknown-empty")).unwrap();
        fs::create_dir(source.join("opaque-history")).unwrap();
        fs::write(source.join("opaque-history/unknown.bin"), bytes).unwrap();
        let b = store.image_budget(8 * 1024 * 1024, limits()).unwrap();
        let image = store
            .capture_stopped_funded(&p.join("image"), request(ack), &b, 2 * 1024 * 1024)
            .unwrap();
        assert_eq!(image.manifest().requested_ack, ack);
        assert_eq!(image.manifest().achieved_ack, ack);
        assert_eq!(image.progress().output, ImageOutput::Complete);
        assert_eq!(population(&source), population(&p.join("image/root")));
        assert_eq!(
            fs::read(p.join("image/root/opaque-history/unknown.bin")).unwrap(),
            bytes
        );
        assert!(image
            .manifest()
            .entries
            .iter()
            .any(|e| e.path == "unknown-empty" && e.directory));
        let before = population(&p.join("image"));
        let validation = validate_image_funded(
            &p.join("image"),
            image.completion(),
            &budget(),
            2 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(validation.manifest(), image.manifest());
        assert_eq!(population(&p.join("image")), before);
        assert_eq!(image.completion().is_some(), ack == Ack::Flush);
        if ack == Ack::Flush {
            assert!(validate_image(&p.join("image")).is_err());
        }
    }
}
#[test]
fn real_store_account_and_returned_clone_lifetimes() {
    let p = root("lifetime");
    let memory = OriginalBudget::new(6 * 1024 * 1024);
    let b = ImageBudget::new(memory.clone(), limits()).unwrap();
    let baseline = memory.used();
    assert!(baseline > 0);
    let store = Store::open(&p.join("source"), config()).unwrap();
    let image = store
        .capture_stopped_funded(&p.join("image"), request(Ack::Os), &b, 2 * 1024 * 1024)
        .unwrap();
    assert_eq!(memory.used(), baseline + 2 * 1024 * 1024);
    let shared = image.clone();
    drop(image);
    assert_eq!(memory.used(), baseline + 2 * 1024 * 1024);
    let blocked = validate_image_funded(&p.join("image"), None, &b, 5 * 1024 * 1024).unwrap_err();
    assert_eq!(
        blocked.shortfall().unwrap().resource,
        ImageResource::Metadata
    );
    assert_eq!(blocked.progress.output, ImageOutput::Untouched);
    assert_eq!(blocked.progress.metadata_requested, 5 * 1024 * 1024);
    assert_eq!(blocked.progress.metadata_reserved, 0);
    drop(blocked);
    drop(shared);
    assert_eq!(memory.used(), baseline);
    drop(b);
    assert_eq!(memory.used(), 0);
    let store = Store::open(
        &p.join("store-admission"),
        Config {
            snapshot_max: 1,
            ..config()
        },
    )
    .unwrap();
    let held = store.image_budget(4 * 1024 * 1024, limits()).unwrap();
    let clone = held.clone();
    drop(held);
    assert!(store.image_budget(1024 * 1024, limits()).is_err());
    drop(clone);
    assert!(store.image_budget(1024 * 1024, limits()).is_ok());
}
#[test]
fn insufficient_metadata_is_admitted_before_growth_or_output() {
    let p = root("metadata");
    let b = budget();
    let before = b.metadata_used();
    let store = Store::open(&p.join("source"), config()).unwrap();
    let error = store
        .capture_stopped_funded(&p.join("image"), request(Ack::Os), &b, 8192)
        .unwrap_err();
    assert_eq!(error.shortfall().unwrap().resource, ImageResource::Metadata);
    assert_eq!(error.progress.output, ImageOutput::Untouched);
    assert!(!p.join("image").exists());
    assert_eq!(b.metadata_used(), before + 8192);
    drop(error);
    assert_eq!(b.metadata_used(), before);
}
#[test]
fn partial_copy_preserves_artifacts_and_never_refunds_backing() {
    let p = root("partial");
    let source = p.join("source");
    let store = Store::open(&source, config()).unwrap();
    fs::write(source.join("00-small"), b"exact-small-body").unwrap();
    fs::write(source.join("01-large"), vec![0x5a; 32768]).unwrap();
    let small = b"exact-small-body".len() as u64;
    let b = ImageBudget::new(
        OriginalBudget::new(4 * 1024 * 1024),
        ImageLimits {
            backing_bytes: b"MBCAP1 incomplete unless capture.complete validates\n".len() as u64
                + small
                + 1,
            ..limits()
        },
    )
    .unwrap();
    let error = store
        .capture_stopped_funded(&p.join("image"), request(Ack::Os), &b, 2 * 1024 * 1024)
        .unwrap_err();
    assert_eq!(error.shortfall().unwrap().resource, ImageResource::Backing);
    assert_eq!(error.progress.output, ImageOutput::Incomplete);
    assert_eq!(error.progress.copied_bytes_confirmed, small);
    assert_eq!(
        fs::read(p.join("image/root/00-small")).unwrap(),
        b"exact-small-body"
    );
    assert!(!p.join("image/root/01-large").exists());
    assert!(!p.join("image/capture.complete").exists());
    assert!(validate_image(&p.join("image")).is_err());
    let spent = b.backing_used();
    assert!(spent > 0);
    drop(error);
    assert_eq!(b.backing_used(), spent);
    assert_eq!(
        fs::read(source.join("01-large")).unwrap(),
        vec![0x5a; 32768]
    );
}
#[test]
fn finite_io_and_work_are_real_shared_admission() {
    for (label, limits, resource) in [
        (
            "io",
            ImageLimits {
                io_bytes: 0,
                ..limits()
            },
            ImageResource::Io,
        ),
        (
            "operations",
            ImageLimits {
                operations: 0,
                ..limits()
            },
            ImageResource::Operations,
        ),
    ] {
        let p = root(label);
        let store = Store::open(&p.join("source"), config()).unwrap();
        let b = ImageBudget::new(OriginalBudget::new(4 * 1024 * 1024), limits).unwrap();
        let error = store
            .capture_stopped_funded(&p.join("image"), request(Ack::Os), &b, 2 * 1024 * 1024)
            .unwrap_err();
        assert_eq!(error.shortfall().unwrap().resource, resource);
        assert_ne!(error.progress.output, ImageOutput::Complete);
        assert!(!p.join("image/capture.complete").exists());
    }
}
fn large_config() -> Config {
    let mut c = config();
    c.cache_rules = (0..64)
        .map(|i| CacheRule {
            namespace: Some("n".repeat(1000)),
            prefix: format!("{i:04}{}", "p".repeat(1000)),
            class: None,
            label: "l".repeat(1000),
            policy: CachePolicy::default(),
        })
        .collect();
    c
}
#[test]
fn encoding_reservation_precedes_the_actual_encoded_allocation() {
    let p = root("encoding-admission");
    let store = Store::open(&p.join("source"), large_config()).unwrap();
    let b = budget();
    let (result, largest) =
        observed(|| store.capture_stopped_funded(&p.join("image"), request(Ack::Os), &b, 360_000));
    let error = result.unwrap_err();
    let shortfall = error.shortfall().unwrap();
    assert_eq!(error.progress.stage, ImageStage::Encoding, "{error:?}");
    assert_eq!(shortfall.resource, ImageResource::Metadata);
    println!(
        "pre-encoding shortfall={} largest allocation={largest}",
        shortfall.requested
    );
    assert!(
        largest < shortfall.requested as usize,
        "harmful allocate-then-refuse neighbor must be rejected"
    );
    assert!(!p.join("image/capture.manifest").exists());
    assert!(!p.join("image/capture.complete").exists());
}
#[test]
fn standalone_validation_admits_before_reading_the_manifest_body() {
    let p = root("validation-admission");
    let store = Store::open(&p.join("source"), large_config()).unwrap();
    let b = budget();
    let image = store
        .capture_stopped_funded(&p.join("image"), request(Ack::Os), &b, 2 * 1024 * 1024)
        .unwrap();
    let before = population(&p.join("image"));
    let bytes = fs::metadata(p.join("image/capture.manifest"))
        .unwrap()
        .len() as usize;
    let validation = ImageBudget::new(
        OriginalBudget::new(1024 * 1024),
        ImageLimits {
            backing_bytes: 0,
            ..limits()
        },
    )
    .unwrap();
    let (result, largest) =
        observed(|| validate_image_funded(&p.join("image"), None, &validation, 200_000));
    let error = result.unwrap_err();
    assert_eq!(error.progress.stage, ImageStage::Validation);
    assert_eq!(error.shortfall().unwrap().resource, ImageResource::Metadata);
    println!("manifest bytes={bytes} largest refused-validation allocation={largest}");
    assert!(
        largest < bytes,
        "read-then-refuse neighbor must be rejected"
    );
    assert_eq!(population(&p.join("image")), before);
    assert_eq!(validation.backing_used(), 0);
    drop(image);
}
#[test]
fn large_payload_streams_without_payload_sized_memory_reservation() {
    let p = root("streaming");
    let source = p.join("source");
    let store = Store::open(&source, config()).unwrap();
    fs::write(source.join("opaque-large"), vec![0x81; 2 * 1024 * 1024]).unwrap();
    let b = budget();
    let (result, largest) = observed(|| {
        store.capture_stopped_funded(&p.join("image"), request(Ack::Os), &b, 768 * 1024)
    });
    let image = result.unwrap();
    assert!(image.progress().metadata_peak < 768 * 1024);
    assert!(largest < 1024 * 1024);
    assert_eq!(
        sha256_file(&source.join("opaque-large")).unwrap(),
        sha256_file(&p.join("image/root/opaque-large")).unwrap()
    );
}
#[test]
fn corrupt_missing_extra_and_mismatched_completion_refuse_immutably() {
    let p = root("validation-refusals");
    let image = capture(&p, Ack::Flush);
    let pristine = population(&p.join("image"));
    let missing_evidence =
        validate_image_funded(&p.join("image"), None, &budget(), 2 * 1024 * 1024).unwrap_err();
    assert!(missing_evidence
        .error
        .to_string()
        .contains("settlement unknown"));
    let other = root("other-completion");
    let other_image = capture(&other, Ack::Flush);
    assert!(validate_image_funded(
        &p.join("image"),
        other_image.completion(),
        &budget(),
        2 * 1024 * 1024
    )
    .is_err());
    assert!(validate_image_funded(
        &p.join("missing-image"),
        image.completion(),
        &budget(),
        2 * 1024 * 1024
    )
    .is_err());
    assert!(!p.join("missing-image").exists());
    for kind in ["missing", "missing-member", "corrupt", "extra-empty"] {
        let changed = p.join(kind);
        copy(&p.join("image"), &changed);
        if kind == "missing" {
            fs::rename(
                changed.join("capture.manifest"),
                changed.join("retained-original-manifest"),
            )
            .unwrap();
        } else if kind == "missing-member" {
            fs::rename(
                changed.join("root/epoch"),
                p.join("retained-missing-member-epoch"),
            )
            .unwrap();
        } else if kind == "corrupt" {
            fs::write(changed.join("root/epoch"), b"corrupt existing bytes").unwrap();
        } else {
            fs::create_dir(changed.join("root/extra-empty")).unwrap();
        }
        let before = population(&changed);
        assert!(
            validate_image_funded(&changed, image.completion(), &budget(), 2 * 1024 * 1024)
                .is_err()
        );
        assert_eq!(population(&changed), before);
    }
    assert_eq!(population(&p.join("image")), pristine);
}

#[cfg(windows)]
#[test]
fn unsupported_stream_is_retained_instead_of_filtered_into_success() {
    let p = root("unsupported-stream");
    let source = p.join("source");
    let store = Store::open(&source, config()).unwrap();
    fs::write(source.join("unknown"), b"regular bytes").unwrap();
    fs::write(
        source.join("unknown:hidden"),
        b"retained authored alternate stream",
    )
    .unwrap();
    let error = store
        .capture_stopped_funded(
            &p.join("image"),
            request(Ack::Os),
            &budget(),
            2 * 1024 * 1024,
        )
        .unwrap_err();
    assert!(error
        .error
        .to_string()
        .contains("alternate filesystem streams"));
    assert_eq!(error.progress.output, ImageOutput::Incomplete);
    assert!(!p.join("image/capture.complete").exists());
    assert_eq!(
        fs::read(source.join("unknown:hidden")).unwrap(),
        b"retained authored alternate stream"
    );
}
#[test]
fn original_completion_custody_has_its_own_funding_and_lifetime() {
    let p = root("custody");
    let image = capture(&p, Ack::Flush);
    let b = budget();
    let baseline = b.metadata_used();
    let receipt = image
        .completion()
        .unwrap()
        .persist_new_funded(&p.join("original.custody"), &b, 512 * 1024)
        .unwrap();
    assert_eq!(b.backing_used(), 72);
    assert_eq!(b.metadata_used(), baseline + 512 * 1024);
    let retained = receipt.clone();
    drop(receipt);
    assert_eq!(b.metadata_used(), baseline + 512 * 1024);
    let decoded =
        CaptureCompletion::from_bytes(&fs::read(p.join("original.custody")).unwrap()).unwrap();
    assert_eq!(&decoded, retained.completion());
    assert!(
        validate_image_funded(&p.join("image"), Some(&decoded), &budget(), 2 * 1024 * 1024).is_ok()
    );
    assert!(image
        .completion()
        .unwrap()
        .persist_new_funded(&p.join("original.custody"), &b, 512 * 1024)
        .is_err());
    drop(retained);
    assert_eq!(b.metadata_used(), baseline);
}
#[test]
fn malformed_count_never_funds_or_allocates_a_claimed_population() {
    let p = root("literal-count");
    let image = capture(&p, Ack::Os);
    let authored = p.join("authored-malformed");
    copy(&p.join("image"), &authored);
    let mut bytes = fs::read(authored.join("capture.manifest")).unwrap();
    fn blob_end(bytes: &[u8], at: usize) -> usize {
        at + 8 + u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap()) as usize
    }
    let mut at = blob_end(&bytes, 8) + 32 + 4;
    at = blob_end(&bytes, at);
    at = blob_end(&bytes, at) + 16;
    bytes[at..at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    let mut h = Sha256::new();
    h.update(&bytes);
    fs::write(authored.join("capture.manifest"), bytes).unwrap();
    fs::write(authored.join("capture.complete"), h.hex()).unwrap();
    let before = population(&authored);
    let (result, largest) =
        observed(|| validate_image_funded(&authored, None, &budget(), 512 * 1024));
    let error = result.unwrap_err();
    assert!(error.error.to_string().contains("count"));
    assert!(largest < 128 * 1024);
    assert_eq!(population(&authored), before);
    drop(image);
}
