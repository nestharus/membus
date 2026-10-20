//! File synchronization, complete record writes and append fencing.
use crate::test_seam::TestSeam;
use crate::Config;
use crate::Log;
#[cfg(all(windows, feature = "sync-witness"))]
use std::fs;
use std::fs::File;
#[cfg(all(windows, feature = "sync-witness"))]
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
#[cfg(all(windows, feature = "sync-witness"))]
use std::sync::Mutex;

// No marker code or environment lookup exists in the normal release build.
#[cfg(all(windows, feature = "sync-witness"))]
pub(super) fn sync_file(file: &File) -> io::Result<()> {
    use std::{ffi::c_void, os::windows::io::AsRawHandle};
    #[link(name = "kernel32")]
    extern "system" {
        fn FlushFileBuffers(handle: *mut c_void) -> i32;
        fn GetFinalPathNameByHandleW(
            handle: *mut c_void,
            path: *mut u16,
            size: u32,
            flags: u32,
        ) -> u32;
    }
    let marks = std::env::var_os("MEMBUS_SYNC_WITNESS").map(std::path::PathBuf::from);
    let witness = marks.as_ref().map(|_| {
        let mut target = vec![0u16; 1024];
        let mut n = unsafe {
            GetFinalPathNameByHandleW(
                file.as_raw_handle(),
                target.as_mut_ptr(),
                target.len() as u32,
                0,
            )
        };
        if n as usize >= target.len() {
            target.resize(n as usize + 1, 0);
            n = unsafe {
                GetFinalPathNameByHandleW(
                    file.as_raw_handle(),
                    target.as_mut_ptr(),
                    target.len() as u32,
                    0,
                )
            };
        }
        target.truncate((n as usize).min(target.len()));
        (target, file.metadata().map(|m| m.len()).ok())
    });
    if let Some(path) = &marks {
        if let Err(error) = fs::write(path.with_extension("entering"), b"entering") {
            eprintln!("sync-witness entering mark failed: {error}");
        }
    }
    let started = std::time::Instant::now();
    let ok = unsafe { FlushFileBuffers(file.as_raw_handle()) };
    let elapsed = started.elapsed().as_nanos();
    let result = if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    };
    if let Some(path) = &marks {
        if let Err(error) = fs::write(
            path.with_extension("returned"),
            format!("{{\"call_ns\":{elapsed},\"ok\":{}}}", ok != 0),
        ) {
            eprintln!("sync-witness returned mark failed: {error}");
        }
        if let Some((target, bytes)) = witness {
            // Append evidence for the actual retained file handle, not merely the last
            // catalogue/epoch sync. Normal releases contain none of this diagnostic.
            static LOG: Mutex<()> = Mutex::new(());
            let _guard = LOG.lock().unwrap();
            let record = format!(
                "{{\"call_ns\":{elapsed},\"ok\":{},\"target_utf16\":{target:?},\"bytes\":{}}}\n",
                ok != 0,
                bytes.map_or_else(|| "null".into(), |n| n.to_string())
            );
            if let Err(error) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path.with_extension("calls"))
                .and_then(|mut f| f.write_all(record.as_bytes()))
            {
                eprintln!("sync-witness calls mark failed: {error}");
            }
        }
    }
    result
}
#[cfg(not(all(windows, feature = "sync-witness")))]
pub(super) fn sync_file(file: &File) -> io::Result<()> {
    file.sync_all()
}

pub(super) fn write_record(log: &mut Log, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        log.write_calls += 1;
        match log.file.write(bytes) {
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
pub(super) fn append_to(log: &mut Log, record: &[u8], config: &Config) -> io::Result<()> {
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
