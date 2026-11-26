//! Target-neutral host platform boundary isolating OS-specific FFI, file locking,
//! directory barriers, filesystem stream checks, and Windows named-pipe primitives.

#[cfg(not(windows))]
mod other;
#[cfg(windows)]
mod windows;

#[cfg(not(windows))]
pub use other::{
    canonical_path_workspace_max, check_directory_streams, check_regular_file_streams,
    open_catalogue_owner, open_catalogue_writer, open_exclusive_owner, open_read_only_exclusive,
    prepare_quarantined_lock, sync_dir, sync_file, validate_regular_metadata, PositionedReader,
    CHECKS_ALTERNATE_STREAMS,
};
#[cfg(windows)]
pub use windows::{
    canonical_path_workspace_max, check_directory_streams, check_regular_file_streams,
    create_inbound_pipe, disconnect_pipe, file_id, is_pipe_handle, open_catalogue_owner,
    open_catalogue_writer, open_exclusive_owner, open_held_custody_options, open_overlapped_pipe,
    open_read_only_exclusive, open_signal_pipe_writer, pipe_server_pid, prepare_quarantined_lock,
    sync_dir, sync_file, validate_default_stream_leaf, validate_regular_metadata, PositionedReader,
    SecuredPipeListener, CHECKS_ALTERNATE_STREAMS,
};

#[cfg(test)]
mod positioned_reader_tests {
    use super::PositionedReader;
    use std::fs::{self, OpenOptions};
    use std::io::{ErrorKind, Write};
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};

    fn test_path(label: &str) -> PathBuf {
        let base = PathBuf::from(
            std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
        );
        fs::create_dir_all(&base).unwrap();
        fs::canonicalize(base).unwrap().join(format!(
            "pos-reader-{label}-{}-{}.dat",
            std::process::id(),
            crate::now_ms()
        ))
    }

    #[test]
    fn positioned_reader_concurrent_open_closed_eof_and_unlink() {
        let path = test_path("concurrent");
        const BLOCK: usize = 4096;
        const INITIAL_BLOCKS: usize = 32;
        let mut writer = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .unwrap();
        for i in 0..INITIAL_BLOCKS {
            let block = vec![(i as u8).wrapping_mul(17).wrapping_add(3); BLOCK];
            writer.write_all(&block).unwrap();
        }
        writer.flush().unwrap();

        let reader = Arc::new(PositionedReader::open(&path).unwrap());
        let barrier = Arc::new(Barrier::new(9));
        std::thread::scope(|scope| {
            let w_barrier = barrier.clone();
            let appender = scope.spawn(move || {
                w_barrier.wait();
                for i in INITIAL_BLOCKS..(INITIAL_BLOCKS + 16) {
                    let block = vec![(i as u8).wrapping_mul(17).wrapping_add(3); BLOCK];
                    writer.write_all(&block).unwrap();
                }
                writer.flush().unwrap();
            });
            let readers: Vec<_> = (0..8)
                .map(|tid| {
                    let r = reader.clone();
                    let b = barrier.clone();
                    scope.spawn(move || {
                        b.wait();
                        let mut buf = vec![0u8; BLOCK];
                        for iter in 0..64 {
                            let idx = (tid * 7 + iter) % INITIAL_BLOCKS;
                            r.read_exact_at(&mut buf, (idx * BLOCK) as u64).unwrap();
                            let expected = (idx as u8).wrapping_mul(17).wrapping_add(3);
                            assert!(buf.iter().all(|&x| x == expected));
                        }
                    })
                })
                .collect();
            appender.join().unwrap();
            for h in readers {
                h.join().unwrap();
            }
        });

        // Closed file reads cover appended tail too.
        let mut tail = vec![0u8; BLOCK];
        let last_idx = INITIAL_BLOCKS + 15;
        reader
            .read_exact_at(&mut tail, (last_idx * BLOCK) as u64)
            .unwrap();
        let expected_last = (last_idx as u8).wrapping_mul(17).wrapping_add(3);
        assert!(tail.iter().all(|&x| x == expected_last));

        // Out-of-bounds and straddling-EOF reads return UnexpectedEof.
        let total = ((INITIAL_BLOCKS + 16) * BLOCK) as u64;
        let mut short = [0u8; 64];
        assert_eq!(
            reader.read_exact_at(&mut short, total).unwrap_err().kind(),
            ErrorKind::UnexpectedEof
        );
        assert_eq!(
            reader
                .read_exact_at(&mut short, total - 16)
                .unwrap_err()
                .kind(),
            ErrorKind::UnexpectedEof
        );

        // Unlinking the file while PositionedReader is alive must succeed (FILE_SHARE_DELETE on Windows),
        // and the still-open handle must keep serving bytes until dropped.
        fs::remove_file(&path).unwrap();
        let mut still = [0u8; 64];
        reader.read_exact_at(&mut still, 0).unwrap();
        assert!(still.iter().all(|&x| x == 3));
        drop(reader);
    }
}
