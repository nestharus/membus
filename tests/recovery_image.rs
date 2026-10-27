//! Private retained fixtures. No Drop cleanup, implicit output selector or resident process.
use membus::{
    chunks::{sha256_file, Sha256},
    client::Ack,
    recovery_image::*,
    Config, Mutation, Store,
};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

fn root(label: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT")
            .expect("explicit retained fixture selector required"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let root = base.join(format!(
        "capture-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    println!("retained authored recovery fixture {}", root.display());
    root
}
fn config() -> Config {
    Config {
        cache_bytes: 65536,
        signal_bytes: 0,
        flush_ms: 0,
        tick_ms: 60000,
        retire_ms: 60000,
        chunk_bytes: 1,
        ..Config::default()
    }
}
fn put(store: &Store, id: &str, bytes: &[u8]) {
    let out = store.mutate(Mutation {
        ns: "authored".into(),
        key: "item".into(),
        op_id: id.into(),
        deadline: membus::now_ms() + 30000,
        ack: 2,
        expected: None,
        bytes: bytes.into(),
        remove: false,
    });
    assert_eq!(out.code, 0);
}
fn request(ack: Ack) -> CaptureRequest {
    CaptureRequest {
        source_label: "authored-provider-image".into(),
        ack,
    }
}
fn hashes(root: &Path) -> BTreeMap<PathBuf, (u64, String)> {
    fn walk(root: &Path, p: &Path, out: &mut BTreeMap<PathBuf, (u64, String)>) {
        for e in fs::read_dir(p).unwrap() {
            let e = e.unwrap();
            let path = e.path();
            let meta = e.metadata().unwrap();
            if meta.is_dir() {
                walk(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().into(),
                    (meta.len(), sha256_file(&path).unwrap()),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}
fn copy_tree(source: &Path, target: &Path) {
    fs::create_dir(target).unwrap();
    for e in fs::read_dir(source).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            copy_tree(&e.path(), &target.join(e.file_name()));
        } else {
            fs::copy(e.path(), target.join(e.file_name())).unwrap();
        }
    }
}

#[test]
fn complete_unknown_population_and_private_recovery_keep_original_immutable_and_fenced() {
    let root = root("complete");
    let original = root.join("original");
    let image = root.join("image");
    let working = root.join("working");
    let store = Store::open(&original, config()).unwrap();
    put(&store, "first", b"first");
    put(&store, "second", b"second");
    fs::create_dir(original.join("unknown-empty-directory")).unwrap();
    fs::create_dir(original.join("history-inputs")).unwrap();
    fs::write(
        original.join("history-inputs/catalogue.sidecar"),
        b"authored retained history",
    )
    .unwrap();
    fs::write(
        original.join("unrecognized-config"),
        b"authored unrecognized config bytes",
    )
    .unwrap();
    let manifest = store.capture_stopped(&image, request(Ack::Flush)).unwrap();
    assert!(manifest.complete && manifest.readback);
    assert_eq!(manifest.requested_ack, Ack::Flush);
    assert_eq!(manifest.achieved_ack, Ack::Flush);
    assert!(manifest
        .entries
        .iter()
        .any(|e| e.path == "unknown-empty-directory" && e.directory));
    assert!(manifest
        .entries
        .iter()
        .any(|e| e.path == "history-inputs/catalogue.sidecar"));
    assert!(manifest
        .entries
        .iter()
        .any(|e| e.path == "unrecognized-config"));
    let retained = hashes(&image);
    assert_eq!(validate_image(&image).unwrap(), manifest);
    let canonical_image = image.canonicalize().unwrap();
    assert_eq!(
        validate_image(&canonical_image).unwrap(),
        manifest,
        "canonicalized absolute selectors must retain prefix/root semantics"
    );
    assert!(
        Store::open(&image.join("root"), config()).is_err(),
        "the retained payload must never be opened as an authorized live Store"
    );
    assert_eq!(
        hashes(&image),
        retained,
        "refusing direct image open must preserve the retained image"
    );
    let recovered = recover_quarantined(&canonical_image, &working, config()).unwrap();
    assert_eq!(
        recovered
            .try_get("authored", "item")
            .unwrap()
            .unwrap()
            .bytes,
        b"second"
    );
    let health = recovered.health2();
    assert!(health.read_only && !health.storage_error && !health.expiry_degraded);
    assert_eq!(
        fs::read(working.join("unrecognized-config")).unwrap(),
        b"authored unrecognized config bytes"
    );
    drop(recovered);
    assert!(
        Store::open(&working, config()).is_err(),
        "a copied epoch must never grant restored serving authority"
    );
    assert_eq!(hashes(&image), retained);
}

#[test]
fn whole_image_validation_refuses_corrupt_and_additional_files_before_working_copy() {
    let root = root("invalid");
    let image = root.join("original-image");
    let original = root.join("original");
    let store = Store::open(&original, config()).unwrap();
    put(&store, "first", b"value");
    fs::write(original.join("unknown"), b"before").unwrap();
    store.capture_stopped(&image, request(Ack::Os)).unwrap();
    let retained = hashes(&image);
    let corrupt = root.join("authored-corrupt-image");
    copy_tree(&image, &corrupt);
    fs::write(corrupt.join("root/unknown"), b"after").unwrap();
    let corrupt_before = hashes(&corrupt);
    assert!(recover_quarantined(&corrupt, &root.join("corrupt-working"), config()).is_err());
    assert!(!root.join("corrupt-working").exists());
    assert_eq!(hashes(&corrupt), corrupt_before);
    let extra = root.join("authored-extra-image");
    copy_tree(&image, &extra);
    fs::write(extra.join("root/extra"), b"unmanifested").unwrap();
    let extra_before = hashes(&extra);
    assert!(recover_quarantined(&extra, &root.join("extra-working"), config()).is_err());
    assert!(!root.join("extra-working").exists());
    assert_eq!(hashes(&extra), extra_before);
    assert_eq!(hashes(&image), retained);
}

#[test]
fn changed_configuration_and_incomplete_capture_refuse_without_mutating_original() {
    let root = root("configuration");
    let image = root.join("image");
    let original = root.join("original");
    let store = Store::open(&original, config()).unwrap();
    put(&store, "first", b"value");
    store.capture_stopped(&image, request(Ack::Flush)).unwrap();
    let retained = hashes(&image);
    assert!(recover_quarantined(
        &image,
        &root.join("changed-config"),
        Config {
            quota: 1,
            ..config()
        }
    )
    .is_err());
    assert!(!root.join("changed-config").exists());
    let incomplete = root.join("incomplete");
    fs::create_dir(&incomplete).unwrap();
    fs::write(incomplete.join("capture.started"), b"incomplete").unwrap();
    let incomplete_before = hashes(&incomplete);
    assert!(recover_quarantined(&incomplete, &root.join("incomplete-working"), config()).is_err());
    assert!(!root.join("incomplete-working").exists());
    assert_eq!(hashes(&incomplete), incomplete_before);
    assert_eq!(hashes(&image), retained);
}

#[test]
fn weak_running_timer_owner_is_explicitly_refused_after_last_arc_is_extracted() {
    let root = root("timer");
    let store = Arc::new(Store::open(&root.join("original"), config()).unwrap());
    let timer = store.start_timers().unwrap();
    // Arc::try_unwrap deliberately succeeds with a Weak owner. This is the
    // counterexample to assuming a unique owned Store proves timer settlement.
    let store = match Arc::try_unwrap(store) {
        Ok(store) => store,
        Err(_) => panic!("idle timer unexpectedly owns strong Store reference"),
    };
    let error = store
        .capture_stopped(&root.join("refused-image"), request(Ack::Flush))
        .unwrap_err();
    assert!(error.to_string().contains("running timer owner"));
    drop(timer);
    assert!(!root.join("refused-image").exists());
}

#[test]
fn active_empty_signal_subscription_is_refused_without_endpoint_io() {
    let root = root("signal-owner");
    let store = Store::open(&root.join("original"), config()).unwrap();
    let mut payload = (membus::now_ms() + 30000).to_le_bytes().to_vec();
    membus::lp(&mut payload, b"authored-subscribe");
    membus::lp(&mut payload, b"authored");
    membus::lp(&mut payload, b"");
    membus::lp(&mut payload, br"\\.\pipe\authored-never-connected");
    payload.push(0);
    membus::lp(&mut payload, b"");
    membus::lp(&mut payload, b"membus.change/1");
    payload.extend(1024u32.to_le_bytes());
    let response = store.dispatch(&membus::frame(membus::SUBSCRIBE, 1, &payload)[4..]);
    assert_eq!(&response[17..19], &[0, 0]);
    // No mutation or queued retained change exists, so the delivery owner has
    // no job and never opens this syntactically valid local endpoint.
    let error = store
        .capture_stopped(&root.join("refused-image"), request(Ack::Flush))
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("signal delivery owner/subscription"));
    assert!(!root.join("refused-image").exists());
}

#[cfg(windows)]
#[test]
fn unsupported_alternate_stream_retains_an_incomplete_output_and_never_completes() {
    let root = root("alternate-stream");
    let original = root.join("original");
    let image = root.join("incomplete-image");
    let store = Store::open(&original, config()).unwrap();
    fs::write(original.join("unknown"), b"regular").unwrap();
    fs::write(
        original.join("unknown:hidden"),
        b"authored alternate stream",
    )
    .unwrap();
    let error = store
        .capture_stopped(&image, request(Ack::Flush))
        .unwrap_err();
    assert!(error.to_string().contains("alternate filesystem streams"));
    assert!(image.join("capture.started").is_file());
    assert!(!image.join("capture.complete").exists());
    assert!(validate_image(&image).is_err());
}

// This helper authors a distinct deliberately incomplete history while keeping
// the canonical image intact. Its checksums are internally consistent so the
// restore must inspect recovered Health2, rather than stopping at byte checks.
fn encode_manifest(m: &CaptureManifest) -> Vec<u8> {
    fn text(b: &mut Vec<u8>, s: &[u8]) {
        b.extend((s.len() as u64).to_le_bytes());
        b.extend(s);
    }
    let mut b = b"MBCAP1".to_vec();
    b.extend(m.version.to_le_bytes());
    text(&mut b, m.source_label.as_bytes());
    for n in [
        m.source_epoch,
        m.source_utc_ms,
        m.source_mono_ms,
        m.completed_utc_ms,
    ] {
        b.extend(n.to_le_bytes());
    }
    b.extend([
        m.requested_ack as u8,
        m.achieved_ack as u8,
        m.complete as u8,
        m.readback as u8,
    ]);
    text(&mut b, &m.config);
    text(&mut b, m.root_sha256.as_bytes());
    b.extend(m.file_barriers.to_le_bytes());
    b.extend(m.directory_barriers.to_le_bytes());
    b.extend((m.entries.len() as u64).to_le_bytes());
    for e in &m.entries {
        text(&mut b, e.path.as_bytes());
        b.push(e.directory as u8);
        b.extend(e.bytes.to_le_bytes());
        text(&mut b, e.sha256.as_bytes());
    }
    b
}
fn manifest_population_hash(entries: &[ImageEntry]) -> String {
    let mut b = Vec::new();
    for e in entries {
        b.extend((e.path.len() as u64).to_le_bytes());
        b.extend(e.path.as_bytes());
        b.push(e.directory as u8);
        b.extend(e.bytes.to_le_bytes());
        b.extend((e.sha256.len() as u64).to_le_bytes());
        b.extend(e.sha256.as_bytes());
    }
    let mut hash = Sha256::new();
    hash.update(&b);
    hash.hex()
}
#[test]
fn internally_valid_image_with_missing_closed_chunk_is_refused_by_recovered_health() {
    let root = root("missing-closed");
    let original = root.join("original");
    let image = root.join("canonical-image");
    let store = Store::open(&original, config()).unwrap();
    put(&store, "first", b"first");
    put(&store, "second", b"second");
    let mut manifest = store.capture_stopped(&image, request(Ack::Os)).unwrap();
    let retained = hashes(&image);
    let (catalogue, tail) =
        membus::chunks::read_catalogue(&image.join("root/chunks/catalogue.log")).unwrap();
    assert!(tail.is_none());
    let closed = catalogue
        .iter()
        .find_map(|r| {
            if let membus::chunks::Rec::Close { seq, .. } = r.rec {
                Some(seq)
            } else {
                None
            }
        })
        .expect("rotation must create Closed chunk");
    let open = catalogue
        .iter()
        .find_map(|r| {
            if let membus::chunks::Rec::Open {
                seq, stripe, class, ..
            } = r.rec
            {
                if seq == closed {
                    Some((stripe, class))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unwrap();
    let missing =
        membus::chunks::chunk_path(&image.join("root/chunks"), open.1, open.0 as usize, closed);
    let missing = missing
        .strip_prefix(image.join("root"))
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/");
    let authored = root.join("authored-missing-closed-image");
    fs::create_dir(&authored).unwrap();
    fs::create_dir(authored.join("root")).unwrap();
    for e in &manifest.entries {
        if e.path == missing {
            continue;
        }
        let target = authored.join("root").join(&e.path);
        if e.directory {
            fs::create_dir(target).unwrap();
        } else {
            fs::copy(image.join("root").join(&e.path), target).unwrap();
        }
    }
    manifest.entries.retain(|e| e.path != missing);
    manifest.root_sha256 = manifest_population_hash(&manifest.entries);
    let bytes = encode_manifest(&manifest);
    let mut hash = Sha256::new();
    hash.update(&bytes);
    fs::write(
        authored.join("capture.started"),
        b"MBCAP1 incomplete unless capture.complete validates\n",
    )
    .unwrap();
    fs::write(authored.join("capture.manifest"), bytes).unwrap();
    fs::write(authored.join("capture.complete"), hash.hex()).unwrap();
    assert!(
        validate_image(&authored).is_ok(),
        "fixture must pass complete byte validation before testing recovery health"
    );
    let authored_before = hashes(&authored);
    let error = match recover_quarantined(&authored, &root.join("health-refused-working"), config())
    {
        Ok(_) => panic!("missing Closed chunk must refuse"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("Health2"),
        "expected actual recovered-health refusal, got {error}"
    );
    assert!(root
        .join("health-refused-working/.membus-quarantine")
        .is_file());
    assert_eq!(hashes(&authored), authored_before);
    assert_eq!(hashes(&image), retained);
    assert!(Store::open(&root.join("health-refused-working"), config()).is_err());
}
