use super::serve;
use membus::{frame, lp, Config, Store, HELLO, PUT};
use std::{
    io::{self, Cursor, Read, Write},
    path::PathBuf,
    sync::{atomic::AtomicUsize, Arc},
    time::{Duration, Instant, SystemTime},
};

struct Peer<F> {
    input: Cursor<Vec<u8>>,
    on_reply: F,
}

impl<F> Read for Peer<F> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.input.read(bytes)
    }
}

impl<F: FnMut(&[u8]) -> io::Result<usize>> Write for Peer<F> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        (self.on_reply)(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn put(id: u64, key: &[u8]) -> Vec<u8> {
    let mut payload = u64::MAX.to_le_bytes().to_vec();
    lp(&mut payload, key);
    lp(&mut payload, b"resident-test");
    lp(&mut payload, key);
    payload.push(2); // Flush
    lp(&mut payload, b"committed");
    frame(PUT, id, &payload)
}

#[test]
fn stop_during_a_flush_reply_preserves_the_write_and_fences_the_next_frame() {
    for lose_reply in [false, true] {
        let root = PathBuf::from(
            std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
        )
        .join(format!(
            "resident-wire-{lose_reply}-{}-{}",
            std::process::id(),
            membus::now_ms()
        ));
        let root = std::path::absolute(root).unwrap();
        let store = Arc::new(Store::open(&root.join("source"), Config::default()).unwrap());
        let mut owner = store.start_resident_workers(0).unwrap();
        let resident = owner.wire_dispatch(&store).unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        let wall = SystemTime::now() + Duration::from_secs(10);
        let denied = owner.poll_stop(&store, until, wall, || {
            Err(io::Error::other("owner refused"))
        });
        assert_eq!(denied.unwrap_err().to_string(), "owner refused");

        let mut greeting = 4096u32.to_le_bytes().to_vec();
        greeting.push(0);
        lp(&mut greeting, b"resident-test");
        let mut input = frame(HELLO, 1, &greeting);
        input.extend(put(2, b"accepted"));
        input.extend(put(3, b"after-stop"));
        let mut replies = Vec::new();
        let peer = Peer {
            input: Cursor::new(input),
            on_reply: |reply: &[u8]| {
                assert_eq!(&reply[17..19], &[0, 0]);
                let id = u64::from_le_bytes(reply[9..17].try_into().unwrap());
                replies.push(id);
                if id == 2 {
                    assert!(!owner.poll_stop(&store, until, wall, || Ok(())).unwrap());
                    if lose_reply {
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "lost original reply",
                        ));
                    }
                }
                Ok(reply.len())
            },
        };
        let error = serve(peer, resident.clone(), Arc::new(AtomicUsize::new(0))).unwrap_err();
        assert_eq!(
            error.kind(),
            if lose_reply {
                io::ErrorKind::BrokenPipe
            } else {
                io::ErrorKind::ConnectionAborted
            }
        );
        assert_eq!(replies, [1, 2]);
        while !owner.poll_stop(&store, until, wall, || Ok(())).unwrap() {
            std::thread::yield_now();
        }
        owner.join().unwrap();
        // The listener handle still owns the original Store even after local wire drain.
        let store = Arc::try_unwrap(store)
            .err()
            .expect("transport still holds Store");
        drop(resident);
        let store = Arc::try_unwrap(store)
            .ok()
            .expect("original Store released by transport");
        let value = store.try_get("resident-test", "accepted").unwrap().unwrap();
        assert_eq!(
            (value.bytes.as_slice(), value.ack, value.version),
            (b"committed".as_slice(), 2, 1)
        );
        assert!(store
            .try_get("resident-test", "after-stop")
            .unwrap()
            .is_none());
        #[cfg(windows)]
        if lose_reply {
            check_lost_reply_recovery(&root, store);
        }
    }
}

#[cfg(windows)]
fn check_lost_reply_recovery(root: &std::path::Path, store: Store) {
    use membus::{
        client::Ack,
        recovery_image::{
            recover_quarantined_with_completion, validate_image_funded, CaptureRequest, ImageLimits,
        },
    };
    let mib = 1024 * 1024;
    let limits = ImageLimits {
        backing_bytes: 16 * mib,
        io_bytes: 128 * mib,
        operations: 128 * mib,
    };
    let budget = store.image_budget(8 * 1024 * 1024, limits).unwrap();
    let image = root.join("image");
    let captured = store
        .capture_stopped_funded(
            &image,
            CaptureRequest {
                source_label: "resident-wire".into(),
                ack: Ack::Flush,
            },
            &budget,
            2 * 1024 * 1024,
        )
        .unwrap();
    let receipt = captured
        .completion()
        .unwrap()
        .persist_new_funded(&root.join("completion"), &budget, 512 * 1024)
        .unwrap();
    assert!(validate_image_funded(&image, None, &budget, 2 * 1024 * 1024).is_err());
    validate_image_funded(&image, Some(receipt.completion()), &budget, 2 * 1024 * 1024).unwrap();
    let recovered = recover_quarantined_with_completion(
        &image,
        &root.join("working"),
        Config::default(),
        receipt.completion(),
    )
    .unwrap();
    assert!(recovered.health2().read_only);
    let value = recovered
        .try_get("resident-test", "accepted")
        .unwrap()
        .unwrap();
    assert_eq!(
        (value.bytes.as_slice(), value.ack, value.version),
        (b"committed".as_slice(), 2, 1)
    );
    assert!(recovered
        .try_get("resident-test", "after-stop")
        .unwrap()
        .is_none());
}
