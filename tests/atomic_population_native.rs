//! Finite Python/native private named pipe. No listener port or installed resident.
#![cfg(windows)]
use membus::{
    atomic::{AtomicAck, FinalChange},
    atomic_population::PopulationLimits,
    client::{Ack, Client, Deadline, Error, Options},
    pipe_io::Pipe,
    Config, Cursor, ManualClock, Store,
};
use std::{
    fs,
    io::Write,
    os::windows::{ffi::OsStrExt, io::FromRawHandle, process::CommandExt},
    path::PathBuf,
    process::{Command, Stdio},
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
fn published_dispatcher_python_negotiated_pages_and_cleanup() {
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
    let mut child = Command::new("python")
        .args(["-B", "tests/atomic_python.py", "--native-matrix", &pipe])
        .arg(&root)
        .arg(limit.to_string())
        .arg(mode)
        .stdin(Stdio::null())
        .stdout(fs::File::create(root.join("python.stdout")).unwrap())
        .stderr(fs::File::create(root.join("python.stderr")).unwrap())
        .creation_flags(0x08000000)
        .spawn()
        .unwrap();
    let result = run_matrix_frames(&store, &root, &mut server, limit, mode);
    drop(server);
    if result.is_err() {
        let _ = child.kill();
    }
    let status = child.wait().unwrap();
    assert!(
        status.success(),
        "status={status}, dispatch={result:?}, child={}",
        fs::read_to_string(root.join("python.stderr")).unwrap()
    );
    result.unwrap();
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
fn python_lost_capture_header_requires_fresh_pipe_at_original_expiry() {
    let (root, address) = private_fixture("python-stalled-capture");
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
            vec![FinalChange::put("current", marker)],
        )
        .unwrap();
    s.publish_atomic(&request, membus::now_ms() + 10000)
        .unwrap();
    let mut first = matrix_pipe(&address).unwrap();
    let mut child = Command::new("python")
        .args([
            "-B",
            "tests/atomic_python.py",
            "--native-stalled-header",
            &address,
        ])
        .arg(&root)
        .stdin(Stdio::null())
        .stdout(fs::File::create(root.join("python.stdout")).unwrap())
        .stderr(fs::File::create(root.join("python.stderr")).unwrap())
        .creation_flags(0x08000000)
        .spawn()
        .unwrap();
    let run = (|| -> std::io::Result<()> {
        first.connect_until(Instant::now() + Duration::from_secs(10))?;
        let hello = pipe_request(&mut first, 1)?;
        fs::write(root.join("old-hello.bin"), &hello)?;
        first.write_all(&s.dispatch(&hello))?;
        let capture = pipe_request(&mut first, 30)?;
        fs::write(root.join("old-capture.bin"), &capture)?;
        let response = s.dispatch(&capture);
        assert_eq!(&response[17..28], b"\0\0MAW1\0MAS2");
        fs::write(root.join("authentic-header.bin"), &response)?;
        let until = Instant::now() + Duration::from_secs(5);
        while !root.join("client-timed-out").is_file() {
            if Instant::now() >= until {
                return Err(std::io::Error::other(
                    "Python did not close its timed-out channel",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let late_write = first.write_all(&response);
        first.set_deadline(Instant::now() + Duration::from_secs(1))?;
        let stale_read = membus::read_frame_limit(&mut first, membus::MAX_FRAME);
        assert!(stale_read
            .as_ref()
            .is_err_and(|error| error.kind() != std::io::ErrorKind::TimedOut));
        fs::write(
            root.join("old-stream-close.txt"),
            format!("{late_write:?}\n{stale_read:?}\n"),
        )?;
        drop(first);

        let mut fresh = matrix_pipe(&address)?;
        fs::write(root.join("fresh-listener"), b"ready")?;
        fresh.connect_until(Instant::now() + Duration::from_secs(10))?;
        let hello = pipe_request(&mut fresh, 1)?;
        fs::write(root.join("fresh-hello.bin"), &hello)?;
        fresh.write_all(&s.dispatch(&hello))?;
        let read = pipe_request(&mut fresh, 2)?;
        fs::write(root.join("fresh-get.bin"), &read)?;
        fresh.write_all(&s.dispatch(&read))?;

        let mut probe = (membus::now_ms() + 10000).to_le_bytes().to_vec();
        membus::lp(&mut probe, b"observe-original");
        membus::lp(&mut probe, b"authored");
        membus::lp(&mut probe, &response[response.len() - 16..]);
        let probe = membus::frame(31, 77, &probe);
        for (step, expected) in [(ttl - 1, 0u16), (1, 20)] {
            clock.advance(step);
            let reply = s.dispatch(&probe[4..]);
            assert_eq!(&reply[17..], &expected.to_le_bytes());
            fs::write(root.join(format!("token-probe-{expected}.bin")), reply)?;
        }
        Ok(())
    })();
    if run.is_err() {
        let _ = child.kill();
    }
    let status = child.wait().unwrap();
    assert!(
        status.success(),
        "{run:?}: {}",
        fs::read_to_string(root.join("python.stderr")).unwrap()
    );
    run.unwrap();
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
    assert_eq!(s.get("authored", "accepted").unwrap().bytes, b"A\0B");
    assert_eq!(s.get("authored", "accepted").unwrap().ack, 1);
    assert!(s.get("authored", "insert").is_none());
    assert!(s.get("authored", "replace").is_none());
}

#[test]
fn exact_published_indexed_python_consumer_uses_opcode_31() {
    let modules = PathBuf::from(
        std::env::var_os("MEMBUS_PUBLISHED_INDEXED_MODULES")
            .expect("directory containing the exact published Python client and indexed binding"),
    );
    assert!(modules.is_absolute() && modules.join("membus_indexed.py").is_file());
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("retained fixture root"),
    );
    let fixture = format!(
        "published-indexed-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = base.join(&fixture);
    fs::create_dir(&root).unwrap();
    let store = Store::open(&root.join("state"), config()).unwrap();
    let pipe = format!(r"\\.\pipe\membus-{fixture}");
    let mut server = matrix_pipe(&pipe).unwrap();
    let code = r#"
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import membus, membus_indexed as idx
assert Path(membus.__file__).parent == Path(sys.argv[1])
assert Path(idx.__file__).parent == Path(sys.argv[1])
client = membus.Client(sys.argv[2], transport='pipe', timeout=10)
collection = idx.Collection('provider', idx.Schema(('field',)))
ns, deadline = 'published', idx.Deadline.after(20)
record = idx.Record(1, ((b'wire',),), sys.argv[3].encode())
try:
    collection.declare(client, collection.prepare_declaration(client, ns, 'declaration', idx.Ack.FLUSH), deadline)
    collection.append(client, collection.prepare_append(client, ns, record, 'append', idx.Ack.FLUSH), deadline)
    token = client.snapshot_open(ns, op_id='capture', deadline=membus.deadline_ms(10))
    result = collection.query(client, ns, token, idx.All(), idx.Limits(2, 8192), 'query', deadline)
    assert result.through == 1 and result.records == (record,)
    client.snapshot_release(ns, token, op_id='release', deadline=membus.deadline_ms(10))
finally:
    client.close()
"#;
    let mut child = Command::new("python")
        .args(["-B", "-c", code])
        .arg(&modules)
        .arg(&pipe)
        .arg(&fixture)
        .stdin(Stdio::null())
        .stderr(fs::File::create(root.join("python.stderr")).unwrap())
        .creation_flags(0x08000000)
        .spawn()
        .unwrap();
    let run = (|| -> std::io::Result<bool> {
        server.connect_until(Instant::now() + Duration::from_secs(20))?;
        let mut trace = fs::File::create(root.join("frames.bin"))?;
        let mut eligibility_called = false;
        for _ in 0..64 {
            server.set_deadline(Instant::now() + Duration::from_secs(20))?;
            let request = membus::read_frame_limit(&mut server, membus::MAX_FRAME)?;
            let reply = store.dispatch(&request);
            trace.write_all(&(request.len() as u32).to_le_bytes())?;
            trace.write_all(&request)?;
            trace.write_all(&reply)?;
            if request[4] == membus::SNAPSHOT_REQUIRE_COMPLETE {
                eligibility_called = reply.len() == 19 && reply[17..19] == [0, 0];
            }
            server.write_all(&reply)?;
            if request[4] == membus::SNAPSHOT_RELEASE {
                return Ok(eligibility_called);
            }
        }
        Ok(false)
    })();
    drop(server);
    if run.is_err() {
        let _ = child.kill();
    }
    let status = child.wait().unwrap();
    assert!(
        status.success(),
        "{run:?}: {}",
        fs::read_to_string(root.join("python.stderr")).unwrap()
    );
    assert!(run.unwrap());
}
#[test]
fn supported_python_whole_burst_original_custody_and_native_restart() {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit retained root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let root = base.join(format!(
        "python-native-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    println!("retained {}", root.display());
    let pipe = format!(
        r"\\.\pipe\membus-population-{}",
        root.file_name().unwrap().to_string_lossy()
    );
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
    let mut child = Command::new("python")
        .args(["-B", "tests/atomic_python.py", "--native", &pipe])
        .arg(&root)
        .stdin(Stdio::null())
        .stdout(fs::File::create(root.join("python.stdout")).unwrap())
        .stderr(fs::File::create(root.join("python.stderr")).unwrap())
        .creation_flags(0x08000000)
        .spawn()
        .unwrap();
    let result = (|| -> Result<(), String> {
        let mut settled_version = None;
        let mut lost_original = Vec::new();
        for generation in 0..4 {
            if generation == 3 {
                drop(store.take());
                store =
                    Some(Store::open(&root.join("state"), config()).map_err(|e| e.to_string())?);
            }
            let name: Vec<u16> = std::ffi::OsStr::new(&pipe)
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
                return Err(std::io::Error::last_os_error().to_string());
            }
            let mut io = Pipe::new(unsafe { fs::File::from_raw_handle(handle) }, 10000);
            fs::write(root.join(format!("ready-{generation}")), b"ready")
                .map_err(|e| e.to_string())?;
            io.connect_until(Instant::now() + Duration::from_secs(30))
                .map_err(|e| e.to_string())?;
            let mut max = membus::MAX_FRAME;
            let mut opcodes = Vec::new();
            for _ in 0..512 {
                io.set_deadline(Instant::now() + Duration::from_secs(30))
                    .map_err(|e| e.to_string())?;
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
                    if operation == "lost" {
                        assert_eq!(&reply[17..24], b"\0\0MAW1\0");
                        assert_eq!(reply[32], 2);
                        let version = u64::from_le_bytes(reply[24..32].try_into().unwrap());
                        if generation == 0 {
                            settled_version = Some(version);
                            lost_original = original.to_vec();
                            fs::write(root.join("first-sent-original.bin"), original).unwrap();
                            fs::write(root.join("first-dispatched-receipt.bin"), &reply).unwrap();
                            break;
                        }
                        assert_eq!(Some(version), settled_version);
                        assert_eq!(original, lost_original);
                        fs::write(
                            root.join(format!("settled-receipt-{generation}.bin")),
                            &reply,
                        )
                        .unwrap();
                    }
                }
                if generation == 1 && opcode == 1 {
                    reply[31..35].copy_from_slice(&3u32.to_le_bytes());
                }
                io.write_all(&reply).map_err(|e| e.to_string())?;
                if opcode == membus::HELLO {
                    max = (u32::from_le_bytes(body[13..17].try_into().unwrap()) as usize)
                        .min(membus::MAX_FRAME);
                }
            }
            if generation == 1 {
                assert_eq!(opcodes, [1]);
            }
            fs::write(root.join(format!("opcodes-{generation}.bin")), opcodes).unwrap();
        }
        Ok(())
    })();
    let status = child.wait().expect("finite Python child must settle");
    println!("private Python child pid={} exit={status}", child.id());
    assert!(
        status.success(),
        "{}",
        fs::read_to_string(root.join("python.stderr")).unwrap()
    );
    result.unwrap();
    let s = store.as_ref().unwrap();
    assert!(s.get("authored", "never").is_none());
    assert_eq!(s.get("authored", "rust").unwrap().bytes, b"newer");
}
