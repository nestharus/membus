//! Test-only witness of the real native accept branch; not an operating endpoint.
use super::*;
use crate::pipe_io::Pipe;
use std::{
    fs,
    os::windows::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
#[path = "child.rs"]
mod child;
static SEQUENCE: AtomicUsize = AtomicUsize::new(1);
fn unique() -> String {
    format!(
        "{}-{}-{}",
        std::process::id(),
        crate::now_ms(),
        SEQUENCE.fetch_add(1, Ordering::SeqCst)
    )
}
fn d() -> Deadline {
    Deadline {
        unix_ms: crate::now_ms() + 5000,
        local: Instant::now() + Duration::from_secs(5),
    }
}
fn with_resident(test: impl FnOnce(&mut Client<Pipe>)) {
    let root = PathBuf::from(
        std::env::var_os("MEMBUS_FORWARDER_TEST_ROOT")
            .expect("explicit private qualification root"),
    )
    .join(unique());
    fs::create_dir_all(&root).unwrap();
    let image = PathBuf::from(
        std::env::var_os("MEMBUS_FORWARDER_TEST_SERVER")
            .expect("explicit pinned original resident"),
    );
    assert!(image.is_absolute());
    let _image = fs::OpenOptions::new()
        .read(true)
        .share_mode(1)
        .open(&image)
        .unwrap();
    let endpoint = format!(r"\\.\pipe\sdk-corrected-resident-{}", unique());
    let mut command = Command::new(image);
    command
        .arg(root.join("state"))
        .args([
            "pipe",
            &endpoint,
            "--heartbeat-ms",
            "0",
            "--flush-ms",
            "0",
            "--idle-ms",
            "10000",
        ])
        .stdin(Stdio::null())
        .stdout(fs::File::create(root.join("stdout")).unwrap())
        .stderr(fs::File::create(root.join("stderr")).unwrap());
    let mut owner = child::Owned::spawn(&mut command).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut c = Client::connect_pipe(
            Path::new(&endpoint),
            Options {
                identity: "original-resident-closed-writer".into(),
                max_frame: MAX_FRAME,
                scans: true,
                subscriptions: true,
            },
            d().local,
        )
        .unwrap();
        test(&mut c);
    }));
    let (exit, forced) = owner.wait(Duration::ZERO).unwrap();
    println!("original resident directwait=true Jobempty=true exit={exit} forced={forced}");
    if let Err(panic) = result {
        std::panic::resume_unwind(panic)
    }
}
fn listener() -> ChangeListener {
    ChangeListener::bind(
        &format!(r"\\.\pipe\sdk-corrected-listener-{}", unique()),
        MAX_FRAME,
    )
    .unwrap()
}
fn subscribe(c: &mut Client<Pipe>, l: &ChangeListener) -> Subscription {
    c.subscribe_with(
        "owned",
        "event/",
        l.endpoint(),
        SubscribeOptions {
            include_values: true,
            resume: None,
            max_frame: MAX_FRAME,
        },
        &unique(),
        d(),
    )
    .unwrap()
}
fn put(c: &mut Client<Pipe>, key: &str, value: &[u8]) -> MutationReply {
    let mut a = c
        .prepare(MutationRequest {
            operation_id: unique(),
            namespace: "owned".into(),
            key: key.into(),
            value: value.to_vec(),
            ack: Ack::Os,
            expected: None,
        })
        .unwrap();
    c.mutate(&mut a, d()).unwrap()
}
fn written(c: &mut Client<Pipe>, s: &Subscription, count: u64) -> [u8; 16] {
    let until = d().local;
    loop {
        let st = c
            .subscription_status("owned", &s.token, &unique(), d())
            .unwrap();
        if st.written == count {
            return st.cursor;
        }
        assert!(
            Instant::now() < until,
            "resident did not finish frame writes"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
fn finish_writer(c: &mut Client<Pipe>, s: &Subscription) {
    c.unsubscribe("owned", &s.token, &unique(), d()).unwrap();
    std::thread::sleep(Duration::from_millis(100));
}
fn exact(
    change: &Change,
    s: &Subscription,
    key: &str,
    value: &[u8],
    reply: &MutationReply,
    expected_cursor: [u8; 16],
) {
    assert_eq!(change.subscription, s.token);
    assert_eq!(change.namespace, "owned");
    assert_eq!(change.key, key);
    assert_eq!(change.cursor, expected_cursor);
    assert_eq!(change.version, reply.version);
    assert_eq!(change.ack as u8, reply.achieved as u8);
    assert!(!change.tombstone);
    assert_eq!(change.value.as_deref(), Some(value));
}
#[test]
fn original_resident_closed_before_first_accept_buffers_all_frames_and_reuses_endpoint() {
    with_resident(|c| {
        let mut l = listener();
        let s = subscribe(c, &l);
        let mut expected = Vec::new();
        for n in 0..3 {
            let key = format!("event/closed/{n}");
            let value = vec![0, n, 255, 42];
            let reply = put(c, &key, &value);
            let cursor = written(c, &s, (n + 1) as u64);
            expected.push((key, value, reply, cursor));
        }
        finish_writer(c, &s);
        let mut cursor = s.cursor;
        for (key, value, reply, expected_cursor) in expected {
            let change = l.receive(&s, "owned", "event/", d().local).unwrap();
            exact(&change, &s, &key, &value, &reply, expected_cursor);
            assert!(change.cursor[8..] != cursor[8..]);
            cursor = change.cursor;
        }
        assert_eq!(
            l.closing_accepts, 1,
            "test must actually enter ERROR_NO_DATA232, not infer it from elapsed time"
        );
        println!(
            "nativeaccept232=1 complete_original_resident_frames=3 exact_values_revisions_ack=true"
        );
        assert!(matches!(
            l.receive(&s, "owned", "event/", d().local),
            Err(ReceiveError::Interrupted(_))
        ));
        assert!(!l.connected);
        assert!(
            l.pipe.is_some(),
            "actual reset must preserve this reusable handle"
        );
        let next = subscribe(c, &l);
        let reply = put(c, "event/reused", b"new known registration");
        let change = l.receive(&next, "owned", "event/", d().local).unwrap();
        // A disconnected reusable instance must accept before its next writer
        // can complete. Observe its public cursor after receiving that frame.
        let expected_cursor = written(c, &next, 1);
        exact(
            &change,
            &next,
            "event/reused",
            b"new known registration",
            &reply,
            expected_cursor,
        );
        assert_ne!(next.token, s.token);
        finish_writer(c, &next);
        let endpoint = l.endpoint().to_owned();
        l.close();
        assert!(matches!(
            l.receive(&next, "owned", "event/", d().local),
            Err(ReceiveError::Closed)
        ));
        assert!(ChangeListener::bind(&endpoint, MAX_FRAME).is_ok());
    });
}
#[test]
fn connected_quiet_timeout_preserves_actual_resident_stream() {
    with_resident(|c| {
        let mut l = listener();
        let s = subscribe(c, &l);
        let first = put(c, "event/before-idle", b"before");
        let expected_cursor = written(c, &s, 1);
        let got = l.receive(&s, "owned", "event/", d().local).unwrap();
        exact(
            &got,
            &s,
            "event/before-idle",
            b"before",
            &first,
            expected_cursor,
        );
        assert!(matches!(
            l.receive(
                &s,
                "owned",
                "event/",
                Instant::now() + Duration::from_millis(25)
            ),
            Err(ReceiveError::QuietTimeout)
        ));
        assert!(l.connected);
        let second = put(c, "event/after-idle", b"after");
        let expected_cursor = written(c, &s, 2);
        let got = l.receive(&s, "owned", "event/", d().local).unwrap();
        exact(
            &got,
            &s,
            "event/after-idle",
            b"after",
            &second,
            expected_cursor,
        );
        finish_writer(c, &s);
        l.close();
    });
}
#[test]
fn empty_and_partial_closed_writers_fail_then_actual_resident_reuses_each_endpoint() {
    with_resident(|c| {
        for bytes in [vec![], vec![100, 0], {
            let mut b = 100u32.to_le_bytes().to_vec();
            b.extend([1, 2, 3, 4]);
            b
        }] {
            let mut l = listener();
            let mut writer = fs::OpenOptions::new()
                .write(true)
                .open(l.endpoint())
                .unwrap();
            writer.write_all(&bytes).unwrap();
            drop(writer);
            let dummy = Subscription {
                token: [0; 16],
                cursor: [0; 16],
            };
            assert!(matches!(
                l.receive(&dummy, "owned", "event/", d().local),
                Err(ReceiveError::Interrupted(_))
            ));
            assert_eq!(l.closing_accepts, 1);
            assert!(!l.connected);
            let s = subscribe(c, &l);
            let reply = put(c, "event/after-truncation", b"complete new frame");
            let got = l.receive(&s, "owned", "event/", d().local).unwrap();
            let expected_cursor = written(c, &s, 1);
            exact(
                &got,
                &s,
                "event/after-truncation",
                b"complete new frame",
                &reply,
                expected_cursor,
            );
            finish_writer(c, &s);
            l.close();
        }
    });
}
#[test]
fn failed_native_reset_closes_owned_handle() {
    let mut l = listener();
    let endpoint = l.endpoint().to_owned();
    // Real OS control on this owned endpoint: retire an already-disconnected pipe.
    l.retire();
    if l.pipe.is_some() {
        l.retire();
    }
    assert!(
        l.pipe.is_none(),
        "failed reset must close, never promise reuse"
    );
    assert!(matches!(
        l.receive(
            &Subscription {
                token: [0; 16],
                cursor: [0; 16]
            },
            "owned",
            "",
            d().local
        ),
        Err(ReceiveError::Closed)
    ));
    assert!(ChangeListener::bind(&endpoint, MAX_FRAME).is_ok());
}
