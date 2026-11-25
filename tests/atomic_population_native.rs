//! Finite native private named pipe tests. No listener port or installed resident.
#![cfg(windows)]
use membus::{
    atomic::{AtomicAck, AtomicRequest, FinalChange},
    atomic_population::PopulationLimits,
    client::{Ack, Client, Deadline, Error, Options},
    indexed::{Collection, Limits as IndexLimits, Query, Record, Schema},
    pipe_io::Pipe,
    Config, Cursor, ManualClock, Store,
};
use std::{
    fs,
    io::Write,
    os::windows::{ffi::OsStrExt, io::FromRawHandle},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
#[link(name = "kernel32")]
unsafe extern "system" {
    fn CreateNamedPipeW(
        name: *const u16,
        open: u32,
        mode: u32,
        instances: u32,
        out: u32,
        input: u32,
        timeout: u32,
        security: *const std::ffi::c_void,
    ) -> *mut std::ffi::c_void;
}
fn config() -> Config {
    Config {
        flush_ms: 0,
        cache_bytes: 0,
        signal_bytes: 0,
        tick_ms: 60000,
        retire_ms: 60000,
        ..Default::default()
    }
}

#[test]
fn published_dispatcher_negotiated_pages_and_cleanup() {
    for (limit, mode) in [
        (1u16, "complete"),
        (2, "complete"),
        (128, "complete"),
        (1, "over-count"),
        (2, "release-refused"),
        (2, "value-read"),
        (2, "value-publish"),
    ] {
        run_matrix_case(limit, mode);
    }
}

fn run_matrix_case(limit: u16, mode: &str) {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("retained fixture root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let fixture = format!(
        "matrix-{mode}-{limit}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = base.join(&fixture);
    fs::create_dir(&root).unwrap();
    println!("retained {}", root.display());
    let pipe = format!(r"\\.\pipe\membus-{fixture}");
    let store = Store::open(&root.join("state"), config()).unwrap();
    let limits = PopulationLimits {
        max_keys: 60_000,
        max_capture_bytes: 96 * 1024 * 1024,
        max_changes: 10_000,
        max_original_bytes: (membus::MAX_FRAME / 3) as u32,
    };
    let seed = store
        .capture_namespace_bounded("authored", limits)
        .unwrap()
        .prepare(
            "provider-only-seed",
            AtomicAck::Flush,
            (0..129)
                .map(|n| {
                    FinalChange::put(
                        &format!("key-{n:03}"),
                        if mode.starts_with("value-") {
                            vec![n as u8, 19, 0]
                        } else {
                            format!("{fixture}:{n}").into_bytes()
                        },
                    )
                })
                .collect(),
        )
        .unwrap();
    store
        .publish_atomic(&seed, membus::now_ms() + 10_000)
        .unwrap();

    let mut server = matrix_pipe(&pipe).unwrap();
    let fixture_bytes = fixture.as_bytes().to_vec();
    std::thread::scope(|scope| {
        let client_handle = scope.spawn(|| {
            let deadline = || Deadline {
                unix_ms: membus::now_ms() + 20_000,
                local: Instant::now() + Duration::from_secs(20),
            };
            let mut options = Options::new("native-rust-matrix", membus::MAX_FRAME);
            options.scans = true;
            let mut client =
                Client::connect_pipe(Path::new(&pipe), options, deadline().local).unwrap();
            assert_eq!(client.negotiated().max_batch, usize::from(limit));
            let cap = client.capture_atomic_namespace_bounded(
                "authored",
                "native-matrix-read",
                limits,
                deadline(),
            );
            match mode {
                "complete" => {
                    let view = cap.unwrap();
                    assert_eq!(view.rows().len(), 129);
                    let req = view
                        .prepare(
                            "native-matrix-publish",
                            AtomicAck::Flush,
                            vec![FinalChange::put("settled", fixture_bytes.clone())],
                        )
                        .unwrap();
                    let ack = client.publish_atomic(&req, deadline()).unwrap();
                    assert_eq!(ack.achieved, Ack::Flush);
                }
                "over-count" => {
                    assert!(matches!(cap, Err(Error::Protocol(_))));
                    assert!(!client.is_usable());
                }
                "release-refused" => {
                    assert!(matches!(cap, Err(Error::Refused(r)) if r.code == 20));
                    assert!(client.is_usable());
                }
                "value-read" => {
                    assert!(matches!(cap, Err(Error::Protocol(_))));
                }
                "value-publish" => {
                    let view = cap.unwrap();
                    assert_eq!(view.rows().len(), 129);
                    let oversized = view
                        .prepare(
                            "native-matrix-too-large",
                            AtomicAck::Flush,
                            vec![FinalChange::put("new", b"1234".to_vec())],
                        )
                        .unwrap();
                    assert!(matches!(
                        client.publish_atomic(&oversized, deadline()),
                        Err(Error::Invalid("value limit"))
                    ));
                    assert!(client.is_usable());
                    let req = view
                        .prepare(
                            "native-matrix-publish",
                            AtomicAck::Flush,
                            vec![
                                FinalChange::put("settled", b"A\0B".to_vec()),
                                FinalChange::remove("key-002"),
                            ],
                        )
                        .unwrap();
                    let ack = client.publish_atomic(&req, deadline()).unwrap();
                    assert_eq!(ack.achieved, Ack::Flush);
                }
                _ => unreachable!(),
            }
        });
        let result = run_matrix_frames(&store, &root, &mut server, limit, mode);
        drop(server);
        client_handle.join().unwrap();
        result.unwrap();
    });
    assert_eq!(
        store.get("authored", "settled").map(|value| value.bytes),
        match mode {
            "complete" => Some(fixture.into_bytes()),
            "value-publish" => Some(b"A\0B".to_vec()),
            _ => None,
        }
    );
    if mode == "value-publish" {
        assert_eq!(store.get("authored", "settled").unwrap().ack, 2);
        assert_eq!(store.get("authored", "key-000").unwrap().bytes, [0, 19, 0]);
        assert!(store.get("authored", "key-001").is_some());
        assert!(store.get("authored", "key-002").is_none());
        assert!(store.get("authored", "new").is_none());
    }
}

fn matrix_pipe(pipe: &str) -> std::io::Result<Pipe> {
    let name: Vec<u16> = std::ffi::OsStr::new(pipe)
        .encode_wide()
        .chain(Some(0))
        .collect();
    let handle = unsafe {
        CreateNamedPipeW(
            name.as_ptr(),
            3 | 0x40000000 | 0x00080000,
            8,
            1,
            65536,
            65536,
            0,
            std::ptr::null(),
        )
    };
    if handle as isize == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(Pipe::new(
        unsafe { fs::File::from_raw_handle(handle) },
        10_000,
    ))
}

fn run_matrix_frames(
    store: &Store,
    root: &std::path::Path,
    pipe: &mut Pipe,
    limit: u16,
    mode: &str,
) -> std::io::Result<()> {
    pipe.connect_until(Instant::now() + Duration::from_secs(20))?;
    let mut trace = fs::File::create(root.join("frames.bin"))?;
    let mut token: Option<Vec<u8>> = None;
    let mut capture_deadline = None;
    let mut pages = 0;
    let page_count = match mode {
        "over-count" => 2,
        "value-read" => 1,
        _ => 129usize.div_ceil(usize::from(limit)),
    };
    let mut sequence = vec![membus::HELLO, membus::ATOMIC_POPULATION];
    sequence.extend(std::iter::repeat_n(membus::SCAN_PREFIX, page_count));
    sequence.push(membus::SNAPSHOT_RELEASE);
    if matches!(mode, "complete" | "value-publish") {
        sequence.push(membus::ATOMIC_PUBLISH);
    }
    for (index, expected) in sequence.into_iter().enumerate() {
        pipe.set_deadline(Instant::now() + Duration::from_secs(20))?;
        let mut body = membus::read_frame_limit(pipe, membus::MAX_FRAME)?;
        assert_eq!(body[4], expected);
        trace.write_all(&(body.len() as u32).to_le_bytes())?;
        trace.write_all(&body)?;
        let id = u64::from_le_bytes(body[5..13].try_into().unwrap());
        assert_eq!(id, index as u64 + 1);
        if matches!(expected, membus::SCAN_PREFIX | membus::SNAPSHOT_RELEASE) {
            let mut c = Cursor::new(&body[13..]);
            let request_deadline = c.u64().unwrap();
            assert_eq!(c.text().unwrap(), "native-matrix-read");
            assert_eq!(c.text().unwrap(), "authored");
            assert_eq!(c.bytes(16).unwrap(), token.as_ref().unwrap().as_slice());
            if expected == membus::SCAN_PREFIX {
                assert_eq!(Some(request_deadline), capture_deadline);
                let cursor_bytes = c.bytes(2096).unwrap().len();
                assert_eq!(cursor_bytes == 0, pages == 0);
                let limit_at = 13
                    + 8
                    + 4
                    + "native-matrix-read".len()
                    + 4
                    + "authored".len()
                    + 20
                    + 4
                    + cursor_bytes;
                assert_eq!(
                    u16::from_le_bytes(body[limit_at..limit_at + 2].try_into().unwrap()),
                    limit
                );
                if mode == "over-count" && pages == 1 {
                    body[limit_at..limit_at + 2].copy_from_slice(&(limit + 1).to_le_bytes());
                }
                pages += 1;
            } else {
                assert!(request_deadline < capture_deadline.unwrap());
            }
        }
        let mut response = store.dispatch_for_limit(&body, false, membus::MAX_FRAME);
        assert_eq!(u16::from_le_bytes(response[17..19].try_into().unwrap()), 0);
        match expected {
            membus::HELLO => {
                assert_eq!(response[35..37], (membus::MAX_BATCH as u16).to_le_bytes());
                response[35..37].copy_from_slice(&limit.to_le_bytes());
                if let Some(max) = match mode {
                    "value-read" => Some(1u32),
                    "value-publish" => Some(3),
                    _ => None,
                } {
                    response[31..35].copy_from_slice(&max.to_le_bytes());
                }
            }
            membus::ATOMIC_POPULATION => {
                assert_eq!(&response[19..28], b"MAW1\0MAS2");
                token = Some(response[response.len() - 16..].to_vec());
                capture_deadline = Some(u64::from_le_bytes(body[13..21].try_into().unwrap()));
            }
            membus::SNAPSHOT_RELEASE => {
                let probe = membus::frame(membus::SNAPSHOT_REQUIRE_COMPLETE, 777, &body[13..]);
                let after_release = store.dispatch(&probe[4..]);
                assert_eq!(&after_release[17..19], &20u16.to_le_bytes());
                if mode == "release-refused" {
                    response = membus::reply(expected, id, 20, &[]);
                }
            }
            _ => {}
        }
        trace.write_all(&response)?;
        pipe.write_all(&response)?;
    }
    Ok(())
}

fn private_fixture(label: &str) -> (PathBuf, String) {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("retained fixture root"),
    );
    let name = format!(
        "{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = base.join(&name);
    fs::create_dir(&root).unwrap();
    println!("retained {}", root.display());
    (root, format!(r"\\.\pipe\membus-{name}"))
}

fn pipe_request(pipe: &mut Pipe, op: u8) -> std::io::Result<Vec<u8>> {
    pipe.set_deadline(Instant::now() + Duration::from_secs(10))?;
    let request = membus::read_frame_limit(pipe, membus::MAX_FRAME)?;
    assert_eq!(request[4], op);
    Ok(request)
}

#[test]
fn lost_capture_header_requires_fresh_pipe_at_original_expiry() {
    let (root, address) = private_fixture("rust-stalled-capture");
    let clock = ManualClock::new(membus::now_ms());
    clock.advance(1);
    let ttl = 4000;
    let s = Store::open(
        &root.join("state"),
        Config {
            clock: Some(clock.clone()),
            snapshot_ttl_ms: ttl,
            ..config()
        },
    )
    .unwrap();
    let marker = root.file_name().unwrap().as_encoded_bytes().to_vec();
    let request = s
        .capture_namespace_funded("authored", 8192)
        .unwrap()
        .prepare(
            "seed",
            AtomicAck::Os,
            vec![FinalChange::put("current", marker.clone())],
        )
        .unwrap();
    s.publish_atomic(&request, membus::now_ms() + 10000)
        .unwrap();
    let mut first = matrix_pipe(&address).unwrap();
    let (tx_timed_out, rx_timed_out) = std::sync::mpsc::channel();
    let (tx_fresh_ready, rx_fresh_ready) = std::sync::mpsc::channel();
    let address_for_client = address.clone();
    std::thread::scope(|scope| {
        let client_handle = scope.spawn(move || {
            let mut options = Options::new("stalled-capture", membus::MAX_FRAME);
            options.scans = true;
            let mut c = Client::connect_pipe(
                Path::new(&address_for_client),
                options.clone(),
                Instant::now() + Duration::from_secs(10),
            )
            .unwrap();
            let short_deadline = Deadline {
                unix_ms: membus::now_ms() + 10_000,
                local: Instant::now() + Duration::from_millis(120),
            };
            let result = c.capture_atomic_namespace_bounded(
                "authored",
                "stalled",
                PopulationLimits {
                    max_keys: 16,
                    max_capture_bytes: 48 * 1024 * 1024,
                    max_changes: 16,
                    max_original_bytes: 65536,
                },
                short_deadline,
            );
            assert!(matches!(result, Err(Error::Transport(_))));
            assert!(!c.is_usable());
            drop(c);
            tx_timed_out.send(()).unwrap();
            rx_fresh_ready.recv_timeout(Duration::from_secs(10)).unwrap();

            let mut fresh = Client::connect_pipe(
                Path::new(&address_for_client),
                options,
                Instant::now() + Duration::from_secs(10),
            )
            .unwrap();
            let val = fresh
                .get(
                    "authored",
                    "current",
                    "read-after-stall",
                    Deadline {
                        unix_ms: membus::now_ms() + 10_000,
                        local: Instant::now() + Duration::from_secs(10),
                    },
                )
                .unwrap()
                .unwrap();
            assert_eq!(val.bytes, marker);
        });

        first
            .connect_until(Instant::now() + Duration::from_secs(10))
            .unwrap();
        let hello = pipe_request(&mut first, 1).unwrap();
        first.write_all(&s.dispatch(&hello)).unwrap();
        let capture = pipe_request(&mut first, 30).unwrap();
        let response = s.dispatch(&capture);
        assert_eq!(&response[17..28], b"\0\0MAW1\0MAS2");
        rx_timed_out.recv_timeout(Duration::from_secs(5)).unwrap();
        let _late_write = first.write_all(&response);
        first
            .set_deadline(Instant::now() + Duration::from_secs(1))
            .unwrap();
        let stale_read = membus::read_frame_limit(&mut first, membus::MAX_FRAME);
        assert!(stale_read
            .as_ref()
            .is_err_and(|error| error.kind() != std::io::ErrorKind::TimedOut));
        drop(first);

        let mut fresh = matrix_pipe(&address).unwrap();
        tx_fresh_ready.send(()).unwrap();
        fresh
            .connect_until(Instant::now() + Duration::from_secs(10))
            .unwrap();
        let hello = pipe_request(&mut fresh, 1).unwrap();
        fresh.write_all(&s.dispatch(&hello)).unwrap();
        let read = pipe_request(&mut fresh, 2).unwrap();
        fresh.write_all(&s.dispatch(&read)).unwrap();

        let mut probe = (membus::now_ms() + 10000).to_le_bytes().to_vec();
        membus::lp(&mut probe, b"observe-original");
        membus::lp(&mut probe, b"authored");
        membus::lp(&mut probe, &response[response.len() - 16..]);
        let probe = membus::frame(31, 77, &probe);
        for (step, expected) in [(ttl - 1, 0u16), (1, 20)] {
            clock.advance(step);
            let reply = s.dispatch(&probe[4..]);
            assert_eq!(&reply[17..], &expected.to_le_bytes());
        }
        client_handle.join().unwrap();
    });
}

#[test]
fn rust_pipe_value_limit_refuses_both_original_formats_before_send() {
    let (root, address) = private_fixture("rust-reduced-value");
    let s = Store::open(&root.join("state"), config()).unwrap();
    let mut server = matrix_pipe(&address).unwrap();
    let deadline = || Deadline {
        unix_ms: membus::now_ms() + 10000,
        local: Instant::now() + Duration::from_secs(10),
    };
    std::thread::scope(|scope| {
        let receive = scope.spawn(|| {
            let mut options = Options::new("native-rust-value-limit", 4096);
            options.scans = true;
            let mut c =
                Client::connect_pipe(std::path::Path::new(&address), options, deadline().local)
                    .unwrap();
            assert_eq!((c.negotiated().max_value, c.negotiated().max_batch), (3, 2));
            let legacy = c
                .capture_atomic_namespace("authored", "legacy-read", deadline())
                .unwrap();
            let bounded = c
                .capture_atomic_namespace_bounded(
                    "authored",
                    "bounded-read",
                    PopulationLimits {
                        max_keys: 10,
                        max_capture_bytes: 48 * 1024 * 1024,
                        max_changes: 3,
                        max_original_bytes: 4096,
                    },
                    deadline(),
                )
                .unwrap();
            let changes = vec![
                FinalChange::put("replace", b"1234567".to_vec()),
                FinalChange::remove("other"),
                FinalChange::put("insert", b"1234".to_vec()),
            ];
            let mar1 = legacy
                .prepare("large-legacy", AtomicAck::Flush, changes.clone())
                .unwrap();
            let mar2 = bounded
                .prepare("large-bounded", AtomicAck::Flush, changes)
                .unwrap();
            for original in [&mar1, &mar2] {
                assert!(matches!(
                    c.publish_atomic(original, deadline()),
                    Err(Error::Invalid("value limit"))
                ));
                assert!(c.is_usable());
            }
            let original = bounded
                .prepare(
                    "allowed",
                    AtomicAck::Os,
                    vec![FinalChange::put("accepted", b"A\0B".to_vec())],
                )
                .unwrap();
            let expired = Deadline {
                unix_ms: 1,
                local: Instant::now() - Duration::from_secs(1),
            };
            assert!(matches!(
                c.publish_atomic(&original, expired),
                Err(Error::Invalid(_))
            ));
            let ack = c.publish_atomic(&original, deadline()).unwrap();
            assert_eq!((ack.version, ack.achieved), (1, Ack::Os));
        });
        server
            .connect_until(Instant::now() + Duration::from_secs(10))
            .unwrap();
        let mut log = fs::File::create(root.join("frames.bin")).unwrap();
        for (index, op) in [1, 28, 30, 11, 13, 29].into_iter().enumerate() {
            let request = pipe_request(&mut server, op).unwrap();
            assert_eq!(
                u64::from_le_bytes(request[5..13].try_into().unwrap()),
                index as u64 + 1
            );
            let mut response = s.dispatch_for_limit(&request, false, 4096);
            if op == 1 {
                response[31..35].copy_from_slice(&3u32.to_le_bytes());
                response[35..37].copy_from_slice(&2u16.to_le_bytes());
            }
            log.write_all(&(request.len() as u32).to_le_bytes())
                .unwrap();
            log.write_all(&request).unwrap();
            log.write_all(&response).unwrap();
            server.write_all(&response).unwrap();
        }
        receive.join().unwrap();
    });
    assert_eq!(s.get("authored", "accepted").unwrap().ack, 1);
    assert!(s.get("authored", "insert").is_none());
    assert!(s.get("authored", "replace").is_none());
}

#[test]
fn exact_indexed_consumer_uses_opcode_31() {
    let (root, pipe) = private_fixture("published-indexed");
    let store = Store::open(&root.join("state"), config()).unwrap();
    let mut server = matrix_pipe(&pipe).unwrap();
    let payload = root.file_name().unwrap().as_encoded_bytes().to_vec();
    let mut eligibility_called = false;
    std::thread::scope(|scope| {
        let client_handle = scope.spawn(|| {
            let d = || Deadline {
                unix_ms: membus::now_ms() + 20_000,
                local: Instant::now() + Duration::from_secs(20),
            };
            let mut options = Options::new("native-indexed", membus::MAX_FRAME);
            options.scans = true;
            let mut client = Client::connect_pipe(Path::new(&pipe), options, d().local).unwrap();
            let collection = Collection::new(
                "provider",
                Schema {
                    fields: vec!["field".into()],
                },
            )
            .unwrap();
            let record = Record {
                ordinal: 1,
                fields: vec![vec![b"wire".to_vec()]],
                payload: payload.clone(),
            };
            collection
                .declare(&mut client, "published", "declaration", Ack::Flush, d())
                .unwrap();
            collection
                .append(&mut client, "published", &record, "append", Ack::Flush, d())
                .unwrap();
            let token = client.snapshot_open("published", "capture", d()).unwrap();
            let result = collection
                .query(
                    &mut client,
                    "published",
                    &token,
                    &Query::All,
                    IndexLimits {
                        max_rows: 2,
                        max_bytes: 8192,
                    },
                    "query",
                    d(),
                )
                .unwrap();
            assert_eq!(result.through, 1);
            assert_eq!(result.records, vec![record]);
            client
                .snapshot_release("published", &token, "release", d())
                .unwrap();
        });
        server
            .connect_until(Instant::now() + Duration::from_secs(20))
            .unwrap();
        for _ in 0..64 {
            server
                .set_deadline(Instant::now() + Duration::from_secs(20))
                .unwrap();
            let request = membus::read_frame_limit(&mut server, membus::MAX_FRAME).unwrap();
            let reply = store.dispatch(&request);
            if request[4] == membus::SNAPSHOT_REQUIRE_COMPLETE {
                eligibility_called = reply.len() == 19 && reply[17..19] == [0, 0];
            }
            server.write_all(&reply).unwrap();
            if request[4] == membus::SNAPSHOT_RELEASE {
                break;
            }
        }
        drop(server);
        client_handle.join().unwrap();
    });
    assert!(eligibility_called);
}

#[test]
fn whole_burst_original_custody_and_native_restart() {
    let (root, pipe) = private_fixture("rust-native-restart");
    let mut store = Some(Store::open(&root.join("state"), config()).unwrap());
    let s = store.as_ref().unwrap();
    let r = s
        .capture_namespace("authored")
        .unwrap()
        .prepare(
            "rust-first",
            AtomicAck::Flush,
            vec![FinalChange::put("rust", b"rust-original".to_vec())],
        )
        .unwrap();
    s.publish_atomic(&r, membus::now_ms() + 10000).unwrap();

    let (tx_ready, rx_ready) = std::sync::mpsc::channel::<usize>();
    let custody_path = root.join("lost.mar");
    let pipe_for_client = pipe.clone();
    std::thread::scope(|scope| {
        let client_handle = scope.spawn(move || {
            let d = || Deadline {
                unix_ms: membus::now_ms() + 30_000,
                local: Instant::now() + Duration::from_secs(30),
            };
            let mut options = Options::new("rust-custody", membus::MAX_FRAME);
            options.scans = true;
            let limits = PopulationLimits {
                max_keys: 64,
                max_capture_bytes: 48 * 1024 * 1024,
                max_changes: 16,
                max_original_bytes: 65536,
            };

            // Generation 0: capture, persist original, publish with lost reply
            assert_eq!(rx_ready.recv_timeout(Duration::from_secs(10)).unwrap(), 0);
            let mut c0 = Client::connect_pipe(Path::new(&pipe_for_client), options.clone(), d().local)
                .unwrap();
            let view = c0
                .capture_atomic_namespace_bounded("authored", "cap-0", limits, d())
                .unwrap();
            let original = view
                .prepare(
                    "lost",
                    AtomicAck::Flush,
                    vec![FinalChange::put("rust", b"newer".to_vec())],
                )
                .unwrap();
            let unsent = view
                .prepare(
                    "never-sent",
                    AtomicAck::Flush,
                    vec![FinalChange::put("never", b"unsent".to_vec())],
                )
                .unwrap();
            original.persist_new(&custody_path).unwrap();
            assert!(matches!(
                c0.publish_atomic(&original, d()),
                Err(Error::Uncertain { .. })
            ));
            drop(c0);

            // Generation 1: server advertises max_value=3, client refuses locally before sending
            assert_eq!(rx_ready.recv_timeout(Duration::from_secs(10)).unwrap(), 1);
            let saved = AtomicRequest::from_bytes(&fs::read(&custody_path).unwrap()).unwrap();
            let mut c1 = Client::connect_pipe(Path::new(&pipe_for_client), options.clone(), d().local)
                .unwrap();
            assert!(matches!(
                c1.publish_atomic(&saved, d()),
                Err(Error::Invalid("value limit"))
            ));
            drop(c1);

            // Generation 2: replay saved original on same Store instance -> exact duplicate settlement
            assert_eq!(rx_ready.recv_timeout(Duration::from_secs(10)).unwrap(), 2);
            let mut c2 = Client::connect_pipe(Path::new(&pipe_for_client), options.clone(), d().local)
                .unwrap();
            let ack = c2.publish_atomic(&saved, d()).unwrap();
            assert_eq!(ack.achieved, Ack::Flush);
            drop(c2);

            // Generation 3: after Store restart, committed original replays exact ack while unsent old-epoch returns RestartUncertain
            assert_eq!(rx_ready.recv_timeout(Duration::from_secs(10)).unwrap(), 3);
            let mut c3 = Client::connect_pipe(Path::new(&pipe_for_client), options, d().local)
                .unwrap();
            let replayed = c3.publish_atomic(&saved, d()).unwrap();
            assert_eq!((replayed.version, replayed.achieved), (ack.version, Ack::Flush));
            assert!(matches!(
                c3.publish_atomic(&unsent, d()),
                Err(Error::Uncertain { cause, .. })
                    if matches!(*cause, Error::Atomic(membus::atomic::PublicationError::RestartUncertain))
            ));
            drop(c3);
        });

        let mut settled_version = None;
        let mut lost_original = Vec::new();
        for generation in 0..4 {
            if generation == 3 {
                drop(store.take());
                store = Some(Store::open(&root.join("state"), config()).unwrap());
            }
            let mut io = matrix_pipe(&pipe).unwrap();
            tx_ready.send(generation).unwrap();
            io.connect_until(Instant::now() + Duration::from_secs(30))
                .unwrap();
            let mut max = membus::MAX_FRAME;
            let mut opcodes = Vec::new();
            for _ in 0..512 {
                io.set_deadline(Instant::now() + Duration::from_secs(30))
                    .unwrap();
                let body = match membus::read_frame_limit(&mut io, max) {
                    Ok(b) => b,
                    Err(_) => break,
                };
                let opcode = body[4];
                opcodes.push(opcode);
                let mut reply = store
                    .as_ref()
                    .unwrap()
                    .dispatch_for_limit(&body, false, max);
                if opcode == membus::ATOMIC_PUBLISH {
                    let mut fields = Cursor::new(&body[13..]);
                    fields.u64().unwrap();
                    let operation = fields.text().unwrap();
                    fields.text().unwrap();
                    let original = fields.bytes(membus::MAX_FRAME / 3).unwrap();
                    if operation == "lost" && generation < 3 {
                        assert_eq!(&reply[17..24], b"\0\0MAW1\0");
                        assert_eq!(reply[32], 2);
                        let version = u64::from_le_bytes(reply[24..32].try_into().unwrap());
                        if generation == 0 {
                            settled_version = Some(version);
                            lost_original = original.to_vec();
                            break;
                        }
                        assert_eq!(Some(version), settled_version);
                        assert_eq!(original, lost_original);
                    }
                }
                if generation == 1 && opcode == 1 {
                    reply[31..35].copy_from_slice(&3u32.to_le_bytes());
                }
                io.write_all(&reply).unwrap();
                if opcode == membus::HELLO {
                    max = (u32::from_le_bytes(body[13..17].try_into().unwrap()) as usize)
                        .min(membus::MAX_FRAME);
                }
            }
            if generation == 1 {
                assert_eq!(opcodes, [1]);
            }
        }
        client_handle.join().unwrap();
    });
    let s = store.as_ref().unwrap();
    assert!(s.get("authored", "never").is_none());
    assert_eq!(s.get("authored", "rust").unwrap().bytes, b"newer");
}

const HEALTH_CAPTURE_ACCOUNT: usize = 8192;

struct CaptureHealthFixture {
    store: Store,
    root: PathBuf,
    log: PathBuf,
    original: Vec<u8>,
    damaged: Vec<u8>,
    revisions: [u64; 2],
}

impl CaptureHealthFixture {
    fn new(root: PathBuf, cache_bytes: usize) -> Self {
        let store = Store::open(
            &root.join("state"),
            Config {
                cache_bytes,
                snapshot_max: 1,
                clock: Some(ManualClock::new(membus::now_ms())),
                ..config()
            },
        )
        .unwrap();
        let put = |key: &str, len| {
            let outcome = store.mutate(membus::Mutation {
                ns: "authored".into(),
                key: key.into(),
                op_id: format!("put-{key}-{len}"),
                bytes: vec![7; len],
                deadline: membus::now_ms() + 30_000,
                ack: 1,
                expected: None,
                remove: false,
            });
            assert_eq!(outcome.code, 0);
            assert_ne!(outcome.version, 0);
            outcome.version
        };
        let damaged_revision = put("key", 16);
        let log = fs::read_dir(root.join("state/chunks/forever"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.extension().is_some_and(|ext| ext == "log"))
            .unwrap();
        let original = fs::read(&log).unwrap();
        assert_eq!(&original[..4], b"MBK1");
        let mut damaged = original.clone();
        damaged[16] ^= 1;
        fs::write(root.join("authored-original.frame"), &original).unwrap();
        fs::write(root.join("authored-damaged.frame"), &damaged).unwrap();
        // The healthy row sorts first, so a failed capture has already read it.
        let first_revision = put("first", 4);
        Self {
            store,
            root,
            log,
            original,
            damaged,
            revisions: [first_revision, damaged_revision],
        }
    }

    fn write_frame(&self, bytes: &[u8]) {
        let mut file = fs::OpenOptions::new().write(true).open(&self.log).unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
    }

    fn assert_complete(&self, view: &membus::atomic::NamespaceView) {
        assert_eq!(view.rows().len(), 2);
        for (((key, value), (expected, len)), revision) in view
            .rows()
            .iter()
            .zip([("first", 4), ("key", 16)])
            .zip(self.revisions)
        {
            assert_eq!(key, expected);
            assert_eq!(
                (value.version, value.ack, value.tombstone),
                (revision, 1, false)
            );
            assert_eq!(value.bytes, vec![7; len]);
        }
    }
}

fn observe_native_capture_health(funded: bool) {
    use membus::atomic::PublicationError;
    let label = if funded {
        "funded-health"
    } else {
        "native-health"
    };
    let (root, _) = private_fixture(label);
    let fixture = CaptureHealthFixture::new(root, if funded { 64 * 1024 } else { 0 });
    let store = &fixture.store;
    let capture = || {
        if funded {
            store.capture_namespace_funded("authored", HEALTH_CAPTURE_ACCOUNT)
        } else {
            store.capture_namespace("authored")
        }
    };
    assert!(!store.storage_error());
    if funded {
        // Prove the damaged key has an eligible resident value before the fault.
        assert_eq!(
            store.try_get("authored", "key").unwrap().unwrap().bytes,
            [7; 16]
        );
        assert!(store.cache_stats().entries > 0);
        let hits = store.cache_stats().hits;
        assert!(store.try_get("authored", "key").unwrap().is_some());
        assert_eq!(store.cache_stats().hits, hits + 1);
    }
    let cache_usage = || {
        let c = store.cache_stats();
        (
            c.hits,
            c.misses,
            c.cold_reads,
            c.coalesced_reads,
            c.entries,
            c.bytes,
        )
    };
    let cache = cache_usage();
    let memory = || store.health2().memory_bytes;
    let idle = memory();
    let view = capture().unwrap();
    fixture.assert_complete(&view);
    assert!(!store.storage_error());
    if funded {
        assert_eq!(memory(), idle + HEALTH_CAPTURE_ACCOUNT);
        let original = view
            .prepare(
                "hold-charge",
                AtomicAck::Os,
                vec![FinalChange::remove("key")],
            )
            .unwrap();
        drop(view);
        assert_eq!(memory(), idle + HEALTH_CAPTURE_ACCOUNT);
        assert!(matches!(capture(), Err(PublicationError::Resource(7))));
        assert!(!store.storage_error());
        drop(original);
    } else {
        drop(view);
    }
    assert_eq!(memory(), idle);

    fixture.write_frame(&fixture.damaged);
    let result = capture();
    assert!(matches!(result, Err(PublicationError::Uncertain)));
    assert_eq!(memory(), idle);
    let sticky = store.storage_error();
    assert_eq!(store.health2().storage_error, sticky);
    println!("{label}: Uncertain, no view, idle account restored, storage_error={sticky}");

    fixture.write_frame(&fixture.original);
    fixture.assert_complete(&capture().unwrap());
    assert_eq!(memory(), idle);
    assert_eq!(cache_usage(), cache);
    assert_eq!(store.storage_error(), sticky);
    assert!(
        sticky,
        "failed native backing read must latch storage_error"
    );
}

#[test]
fn capture_storage_health_native_latches_backing_error() {
    observe_native_capture_health(false);
}

#[test]
fn capture_storage_health_funded_bypasses_cache_and_latches_backing_error() {
    observe_native_capture_health(true);
}

#[test]
fn capture_storage_health_wire_keeps_complete_view_and_typed_backing_error() {
    use membus::atomic::PublicationError;
    let (root, address) = private_fixture("wire-health");
    let fixture = CaptureHealthFixture::new(root, 0);
    let store = &fixture.store;
    let idle = store.health2().memory_bytes;
    let mut server = matrix_pipe(&address).unwrap();
    let deadline = || Deadline {
        unix_ms: membus::now_ms() + 10_000,
        local: Instant::now() + Duration::from_secs(10),
    };

    std::thread::scope(|scope| {
        let receive = scope.spawn(|| {
            let mut client = Client::connect_pipe(
                std::path::Path::new(&address),
                Options::new("authored-wire-health", 4096),
                deadline().local,
            )
            .unwrap();
            let mut capture = |id| client.capture_atomic_namespace("authored", id, deadline());
            fixture.assert_complete(&capture("positive").unwrap());
            assert!(matches!(
                capture("damaged"),
                Err(Error::Atomic(PublicationError::Uncertain))
            ));
            assert!(matches!(
                capture("restored"),
                Err(Error::Atomic(PublicationError::Unavailable))
            ));
            assert!(client.is_usable());
        });
        server.connect_until(deadline().local).unwrap();
        for (index, op) in [
            membus::HELLO,
            membus::ATOMIC_CAPTURE,
            membus::ATOMIC_CAPTURE,
            membus::ATOMIC_CAPTURE,
        ]
        .into_iter()
        .enumerate()
        {
            let request = pipe_request(&mut server, op).unwrap();
            if index == 2 {
                fixture.write_frame(&fixture.damaged);
            } else if index == 3 {
                fixture.write_frame(&fixture.original);
            }
            let reply = store.dispatch_for_limit(&request, false, 4096);
            match index {
                1 => assert!(!store.storage_error()),
                2 | 3 => {
                    let status: &[u8] = if index == 2 { b"MAW1\x07" } else { b"MAW1\x0a" };
                    assert_eq!(&reply[19..], status);
                    assert!(store.health2().storage_error);
                    assert_eq!(store.health2().memory_bytes, idle);
                }
                _ => {}
            }
            fs::write(fixture.root.join(format!("wire-reply-{index}.bin")), &reply).unwrap();
            server.write_all(&reply).unwrap();
        }
        receive.join().unwrap();
    });
    // Two cold reads per attempted capture; the sticky refusal reads neither row.
    assert_eq!(store.cache_stats().cold_reads, 4);
    println!("wire: complete positive view, status-only Uncertain and sticky Unavailable");
}
