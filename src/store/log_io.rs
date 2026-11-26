//! File synchronization, complete record writes and append fencing.
use crate::test_seam::TestSeam;
use crate::{Config, Log};
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

#[cfg(test)]
#[path = "../unit_tests/store/log_io_state_directory_tests.rs"]
mod state_directory_tests;

/// Cover state-directory names before ownership and replay. Repeat the nearest
/// existing name's parent barrier: it may be left by an earlier failed mkdir sync.
pub(crate) fn prepare_state_directory(path: &Path, config: &Config) -> io::Result<()> {
    let absolute = std::path::absolute(path)?;
    let mut missing = Vec::new();
    let mut existing = absolute.as_path();
    loop {
        match std::fs::metadata(existing) {
            Ok(meta) if meta.is_dir() => break,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "state directory is a file",
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                missing.push(existing);
                existing = existing.parent().ok_or(e)?;
            }
            Err(e) => return Err(e),
        }
    }
    let sync_parent = |directory: &Path| -> io::Result<()> {
        if let Some(parent) = directory.parent() {
            #[cfg(test)]
            if let Some(hook) = &config.state_dir_sync {
                hook(parent)?;
            }
            sync_dir(parent)?;
        }
        Ok(())
    };
    let _ = config;
    sync_parent(existing)?;
    for directory in missing.into_iter().rev() {
        std::fs::create_dir_all(directory)?;
        sync_parent(directory)?;
    }
    Ok(())
}

pub(crate) fn sync_chunk_directory(path: &Path, _config: &Config) -> io::Result<()> {
    #[cfg(test)]
    if let Some(hook) = &_config.chunk_dir_sync {
        hook(path)?;
    }
    sync_dir(path)
}

pub(crate) fn sync_file(file: &File) -> io::Result<()> {
    crate::platform::sync_file(file)
}

pub(crate) fn sync_dir(path: &Path) -> io::Result<()> {
    crate::platform::sync_dir(path)
}

pub(crate) fn write_record(log: &mut Log, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        log.write_calls += 1;
        match (&*log.file).write(bytes) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "append")),
            Ok(n) => {
                log.bytes += n as u64;
                bytes = &bytes[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// One stripe log's append with the test seams; a failed append fences the log.
pub(crate) fn append_to(log: &mut Log, record: &[u8], config: &Config) -> io::Result<()> {
    let result = if log.poisoned {
        Err(io::Error::other("stripe fenced after failed append"))
    } else if TestSeam::partial_append_requested(config) {
        let half = record.len() / 2;
        let r = write_record(log, &record[..half]);
        TestSeam::checkpoint("MEMBUS_TEST_PAUSE_PARTIAL");
        log.poisoned = true;
        r.and(Err(io::Error::other(
            "test injected partial append failure",
        )))
    } else {
        write_record(log, record)
    };
    if result.is_err() {
        log.poisoned = true;
    }
    result
}
