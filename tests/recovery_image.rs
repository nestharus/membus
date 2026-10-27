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
fn hashes(root: &Path) -> BTreeMap<PathBuf, (bool, u64, String)> {
    fn walk(root: &Path, p: &Path, out: &mut BTreeMap<PathBuf, (bool, u64, String)>) {
        for e in fs::read_dir(p).unwrap() {
            let e = e.unwrap();
            let path = e.path();
            let meta = fs::symlink_metadata(&path).unwrap();
            assert!(!meta.file_type().is_symlink());
            if meta.is_dir() {
                out.insert(
                    path.strip_prefix(root).unwrap().into(),
                    (true, 0, String::new()),
                );
                walk(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().into(),
                    (false, meta.len(), sha256_file(&path).unwrap()),
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
    assert_eq!(
        validate_image_with_completion(&image, manifest.completion().unwrap()).unwrap(),
        manifest
    );
    let canonical_image = image.canonicalize().unwrap();
    assert_eq!(
        validate_image_with_completion(&canonical_image, manifest.completion().unwrap()).unwrap(),
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
    let recovered = recover_quarantined_with_completion(
        &canonical_image,
        &working,
        config(),
        manifest.completion().unwrap(),
    )
    .unwrap();
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
    let manifest = store.capture_stopped(&image, request(Ack::Flush)).unwrap();
    let retained = hashes(&image);
    assert!(recover_quarantined_with_completion(
        &image,
        &root.join("changed-config"),
        Config {
            quota: 1,
            ..config()
        },
        manifest.completion().unwrap()
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

#[test]
fn refusal_oracle_rejects_empty_directory_then_refusal_neighbor() {
    let p = root("oracle-neighbor");
    fs::write(p.join("file"), b"unchanged").unwrap();
    let before = hashes(&p);
    let harmful_neighbor = || -> std::io::Result<()> {
        fs::create_dir(p.join("added-empty-directory"))?;
        Err(std::io::Error::other("expected refusal"))
    };
    assert!(harmful_neighbor().is_err());
    assert_ne!(
        hashes(&p),
        before,
        "refusal oracle must reject this exact mutation"
    );
}

#[test]
fn embedded_import_escape_is_refused_before_outside_effects() {
    let p = root("embedded-import");
    let image = p.join("baseline-image");
    let store = Store::open(&p.join("source"), config()).unwrap();
    put(&store, "seed", b"protected baseline");
    let baseline = store.capture_stopped(&image, request(Ack::Os)).unwrap();
    let protected = hashes(&image);
    for (label, directory, filename) in [
        ("parent", "../escape-canary/new-empty", None),
        ("backslash", "..\\escape-canary\\new-empty", None),
        ("file-parent", "legacy-7", Some("../escape-canary/sentinel")),
        (
            "file-backslash",
            "legacy-7",
            Some("..\\escape-canary\\sentinel"),
        ),
        ("earlier-import", "../escape-canary/new-empty", None),
        ("absolute-directory", "@absolute", None),
        ("absolute-file", "legacy-7", Some("@absolute")),
    ] {
        let case = p.join(label);
        fs::create_dir(&case).unwrap();
        let canary = case.join("escape-canary");
        fs::create_dir(&canary).unwrap();
        fs::write(canary.join("sentinel"), b"outside remains exact").unwrap();
        let before = hashes(&canary);
        let directory = if directory == "@absolute" {
            canary.join("new-empty").to_string_lossy().into_owned()
        } else {
            directory.to_owned()
        };
        let filename = filename.map(|name| {
            if name == "@absolute" {
                canary.join("sentinel").to_string_lossy().into_owned()
            } else {
                name.to_owned()
            }
        });
        let authored = case.join("authored-image");
        copy_tree(&image, &authored);
        let cat = authored.join("root/chunks/catalogue.log");
        let mut records = membus::chunks::read_catalogue(&cat).unwrap().0;
        records.push(membus::chunks::CatRecord {
            at_ms: membus::now_ms(),
            rec: membus::chunks::Rec::Import(membus::chunks::Import {
                files: filename
                    .into_iter()
                    .map(|name| (name.into(), 21, "invalid".into()))
                    .collect(),
                keys: 0,
                bytes: 0,
                epoch: 7,
                legacy_dir: directory,
            }),
        });
        if label == "earlier-import" {
            records.push(membus::chunks::CatRecord {
                at_ms: membus::now_ms(),
                rec: membus::chunks::Rec::Import(membus::chunks::Import {
                    files: vec![],
                    keys: 0,
                    bytes: 0,
                    epoch: 8,
                    legacy_dir: "legacy-8".into(),
                }),
            });
        }
        fs::write(
            &cat,
            records
                .iter()
                .flat_map(membus::chunks::encode)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut manifest = baseline.clone();
        let entry = manifest
            .entries
            .iter_mut()
            .find(|e| e.path == "chunks/catalogue.log")
            .unwrap();
        entry.bytes = fs::metadata(&cat).unwrap().len();
        entry.sha256 = sha256_file(&cat).unwrap();
        manifest.root_sha256 = manifest_population_hash(&manifest.entries);
        let bytes = encode_manifest(&manifest);
        let mut h = Sha256::new();
        h.update(&bytes);
        fs::write(authored.join("capture.manifest"), bytes).unwrap();
        fs::write(authored.join("capture.complete"), h.hex()).unwrap();
        assert!(
            validate_image(&authored).is_ok(),
            "internally consistent authored input"
        );
        let input_before = hashes(&authored);
        let result = recover_quarantined(&authored, &case.join("working"), config());
        println!(
            "escape case={label} outside_directory={} startup_refused={}",
            canary.join("new-empty").exists(),
            result.is_err()
        );
        assert!(
            !canary.join("new-empty").exists(),
            "outside empty-directory escape: {label}"
        );
        assert!(result.is_err(), "invalid embedded Import must refuse");
        assert_eq!(hashes(&canary), before, "outside canary: {label}");
        assert_eq!(hashes(&authored), input_before);
    }
    assert_eq!(hashes(&image), protected);
}

#[test]
fn flush_completion_custody_binds_original_success_without_later_image_barriers() {
    let p = root("completion-custody");
    let image = p.join("image");
    let store = Store::open(&p.join("source"), config()).unwrap();
    let original_outcome = store.mutate(Mutation {
        ns: "authored".into(),
        key: "item".into(),
        op_id: "original-os-ack".into(),
        deadline: u64::MAX,
        ack: 1,
        expected: None,
        bytes: b"unchanged original Ack".to_vec(),
        remove: false,
    });
    assert_eq!((original_outcome.code, original_outcome.ack), (0, 1));
    let manifest = store.capture_stopped(&image, request(Ack::Flush)).unwrap();
    let retained = hashes(&image);
    assert!(validate_image(&image)
        .unwrap_err()
        .to_string()
        .contains("settlement unknown"));
    assert!(recover_quarantined(&image, &p.join("no-evidence-working"), config()).is_err());
    assert!(!p.join("no-evidence-working").exists());
    let completion = manifest.completion().unwrap();
    completion
        .persist_new(&p.join("original-completion.custody"))
        .unwrap();
    assert!(completion
        .persist_new(&p.join("original-completion.custody"))
        .is_err());
    let decoded =
        CaptureCompletion::from_bytes(&fs::read(p.join("original-completion.custody")).unwrap())
            .unwrap();
    assert_eq!(&decoded, completion);
    assert_eq!(
        validate_image_with_completion(&image, &decoded).unwrap(),
        manifest
    );
    let other = Store::open(&p.join("other-source"), config())
        .unwrap()
        .capture_stopped(
            &p.join("other-image"),
            CaptureRequest {
                source_label: "other-source".into(),
                ack: Ack::Flush,
            },
        )
        .unwrap();
    assert!(validate_image_with_completion(&image, other.completion().unwrap()).is_err());
    let recovered =
        recover_quarantined_with_completion(&image, &p.join("working"), config(), &decoded)
            .unwrap();
    let value = recovered.try_get("authored", "item").unwrap().unwrap();
    assert_eq!(value.ack, 1);
    assert_eq!(value.bytes, b"unchanged original Ack");
    drop(recovered);
    assert_eq!(hashes(&image), retained);
}

#[test]
fn existing_working_root_is_refused_without_touching_its_owner_or_population() {
    let p = root("existing-owner");
    let image = p.join("image");
    Store::open(&p.join("source"), config())
        .unwrap()
        .capture_stopped(&image, request(Ack::Os))
        .unwrap();
    let working = p.join("existing-working");
    fs::create_dir(&working).unwrap();
    fs::write(working.join("owner.lock"), b"preexisting owner evidence").unwrap();
    fs::create_dir(working.join("empty-directory")).unwrap();
    let before = hashes(&working);
    assert!(recover_quarantined(&image, &working, config()).is_err());
    assert_eq!(hashes(&working), before);
}

#[test]
fn supported_import_preserves_flat_legacy_inputs_and_backup_history() {
    let p = root("valid-import");
    let source = p.join("source");
    let store = Store::open(&source, config()).unwrap();
    let files = vec![
        ("stripe-0.log".to_owned(), Vec::new()),
        (
            "import-policies.txt".to_owned(),
            b"# authored policy history\n".to_vec(),
        ),
    ];
    let imported = membus::chunks::Import {
        files: files
            .iter()
            .map(|(name, bytes)| {
                let mut h = Sha256::new();
                h.update(bytes);
                (name.clone(), bytes.len() as u64, h.hex())
            })
            .collect(),
        keys: 0,
        bytes: 0,
        epoch: 7,
        legacy_dir: "legacy-7".into(),
    };
    drop(store);
    // Only this stopped private authored fixture is changed. No retained capture
    // or original source image is opened or rewritten by this control.
    for (name, bytes) in &files {
        fs::write(source.join(name), bytes).unwrap();
    }
    use std::io::Write;
    let mut cat = fs::OpenOptions::new()
        .append(true)
        .open(source.join("chunks/catalogue.log"))
        .unwrap();
    cat.write_all(&membus::chunks::encode(&membus::chunks::CatRecord {
        at_ms: membus::now_ms(),
        rec: membus::chunks::Rec::Import(imported),
    }))
    .unwrap();
    cat.sync_all().unwrap();
    drop(cat);
    // The following recovery is on a copied authored image, not this root.
    // Build the image from a read-only byte account of the stopped fixture.
    let image = p.join("image");
    let template = Store::open(&p.join("template"), config())
        .unwrap()
        .capture_stopped(&p.join("template-image"), request(Ack::Os))
        .unwrap();
    fs::create_dir(&image).unwrap();
    copy_tree(&source, &image.join("root"));
    fn population(base: &Path, path: &Path, out: &mut Vec<ImageEntry>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            let meta = fs::metadata(&path).unwrap();
            out.push(ImageEntry {
                path: path
                    .strip_prefix(base)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/"),
                directory: meta.is_dir(),
                bytes: if meta.is_dir() { 0 } else { meta.len() },
                sha256: if meta.is_dir() {
                    String::new()
                } else {
                    sha256_file(&path).unwrap()
                },
            });
            if meta.is_dir() {
                population(base, &path, out);
            }
        }
    }
    let mut manifest = template;
    manifest.entries.clear();
    population(
        &image.join("root"),
        &image.join("root"),
        &mut manifest.entries,
    );
    manifest.entries.sort_by(|a, b| a.path.cmp(&b.path));
    manifest.root_sha256 = manifest_population_hash(&manifest.entries);
    let bytes = encode_manifest(&manifest);
    let mut h = Sha256::new();
    h.update(&bytes);
    fs::write(
        image.join("capture.started"),
        b"MBCAP1 incomplete unless capture.complete validates\n",
    )
    .unwrap();
    fs::write(image.join("capture.manifest"), bytes).unwrap();
    fs::write(image.join("capture.complete"), h.hex()).unwrap();
    let before = hashes(&image);
    let result = recover_quarantined(&image, &p.join("working"), config()).unwrap();
    for (name, bytes) in files {
        assert_eq!(
            fs::read(p.join("working/legacy-7").join(name)).unwrap(),
            bytes
        );
    }
    assert_eq!(hashes(&image), before);
    // Backup conversion is a replay obligation, separate from byte preservation
    // and from the image format's intentionally excluded ACL preservation.
    for name in ["stripe-0.log", "import-policies.txt"] {
        let source_readonly = fs::metadata(source.join(name))
            .unwrap()
            .permissions()
            .readonly();
        let image_readonly = fs::metadata(image.join("root").join(name))
            .unwrap()
            .permissions()
            .readonly();
        let backup_readonly = fs::metadata(p.join("working/legacy-7").join(name))
            .unwrap()
            .permissions()
            .readonly();
        println!("Import backup {name}: source_readonly={source_readonly} image_readonly={image_readonly} backup_readonly={backup_readonly}");
        assert!(
            !source_readonly && !image_readonly,
            "authored inputs stay writable: {name}"
        );
        assert!(backup_readonly, "replayed backup must be read-only: {name}");
    }
    drop(result);
}

#[cfg(not(windows))]
#[test]
fn copied_nonwindows_owner_lock_is_prepared_only_in_new_private_root() {
    let p = root("nonwindows-owner");
    let image = p.join("image");
    Store::open(&p.join("source"), config())
        .unwrap()
        .capture_stopped(&image, request(Ack::Os))
        .unwrap();
    let before = hashes(&image);
    assert!(image.join("root/owner.lock").is_file());
    let recovered = recover_quarantined(&image, &p.join("working"), config()).unwrap();
    assert_eq!(hashes(&image), before);
    assert!(recovered.health2().read_only);
    drop(recovered);
    assert!(Store::open(&p.join("working"), config()).is_err());
}
