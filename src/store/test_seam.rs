//! Named instrumentation points for owner delays, notifications, gates and failures.
use crate::log_io::sync_file;
use crate::Config;
#[cfg(any(test, feature = "test-seams"))]
use std::fs;
use std::fs::File;
use std::io;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::sync::atomic::Ordering;
#[cfg(test)]
use std::sync::mpsc;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::Condvar;
#[cfg(test)]
use std::sync::Mutex;
use std::time::Duration;

#[cfg(test)]
type SyncGate = Arc<(Mutex<(bool, bool)>, Condvar)>;

/// All instrumentation shared by append ingress and flush settlement has one home.
#[derive(Default)]
pub(crate) struct TestSeam {
    write_delay_ms: u64,
    flush_notice_delay_ms: u64,
    fail_after_append: bool,
    fail_sync: bool,
    #[cfg(test)]
    sync_test_gate: Option<SyncGate>,
    #[cfg(test)]
    sync_test_notice: Option<mpsc::Sender<()>>,
    #[cfg(test)]
    flush_covered_count: Option<Arc<AtomicUsize>>,
}
impl TestSeam {
    pub(crate) fn for_append(config: &Config) -> Self {
        Self {
            write_delay_ms: config.write_delay_ms,
            flush_notice_delay_ms: config.flush_notice_delay_ms,
            fail_after_append: config.fail_after_append,
            #[cfg(test)]
            sync_test_notice: config.sync_test_notice.clone(),
            ..Self::default()
        }
    }
    pub(crate) fn for_flush(config: &Config) -> Self {
        Self {
            fail_sync: config.fail_sync,
            #[cfg(test)]
            sync_test_gate: config.sync_test_gate.clone(),
            #[cfg(test)]
            flush_covered_count: config.flush_covered_count.clone(),
            ..Self::default()
        }
    }
    pub(crate) fn before_append(&self) {
        if self.write_delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.write_delay_ms));
        }
    }
    pub(crate) fn after_append(&self, result: io::Result<()>) -> io::Result<()> {
        if result.is_ok() && self.fail_after_append {
            Err(io::Error::other("test injected post-admission I/O failure"))
        } else {
            result
        }
    }
    pub(crate) fn before_flush_notice(&self) {
        if self.flush_notice_delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.flush_notice_delay_ms));
        }
    }
    pub(crate) fn after_classification(hook: impl FnOnce()) {
        hook();
    }
    pub(crate) fn after_flush_notice(&self) {
        #[cfg(test)]
        if let Some(notice) = &self.sync_test_notice {
            let _ = notice.send(());
        }
    }
    pub(crate) fn coverage_retained(&self, count: usize) {
        #[cfg(test)]
        if let Some(covered) = &self.flush_covered_count {
            covered.store(count, Ordering::SeqCst);
        }
        #[cfg(not(test))]
        let _ = count;
    }
    pub(crate) fn sync_selected_file(&self, file: &File) -> io::Result<()> {
        Self::checkpoint("MEMBUS_TEST_PAUSE_SYNC");
        #[cfg(test)]
        if let Some(gate) = &self.sync_test_gate {
            let mut state = gate.0.lock().unwrap();
            if !state.0 {
                state.0 = true;
                gate.1.notify_all();
                while !state.1 {
                    state = gate.1.wait(state).unwrap();
                }
            }
        }
        if self.fail_sync {
            Err(io::Error::other("test injected sync failure"))
        } else {
            sync_file(file)
        }
    }
    pub(crate) fn partial_append_requested(config: &Config) -> bool {
        #[cfg(any(test, feature = "test-seams"))]
        {
            config.fail_partial_append || std::env::var_os("MEMBUS_TEST_PAUSE_PARTIAL").is_some()
        }
        #[cfg(not(any(test, feature = "test-seams")))]
        {
            config.fail_partial_append
        }
    }
    pub(crate) fn sync_closing_chunk(file: &File, fail_sync: bool) -> io::Result<()> {
        if fail_sync {
            Err(io::Error::other("test injected sync failure"))
        } else {
            sync_file(file)
        }
    }
    // Test-only kill checkpoints, enabled explicitly by a path-valued environment
    // variable when built for tests or with `--features test-seams`.
    pub(crate) fn checkpoint(name: &str) {
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(path) = std::env::var_os(name) {
            let _ = fs::write(path, b"reached");
            std::thread::sleep(Duration::from_secs(40));
        }
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = name;
    }
}
