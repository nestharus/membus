//! Per-stripe append owner: record ingress, routed writes and completion handoff.
use crate::flush_owner::FlushIngress;
use crate::flush_owner::FlushWait;
use crate::routing::Route;
use crate::test_seam::TestSeam;
use crate::Config;
use std::io;
use std::sync::mpsc;
use std::sync::Arc;

pub(super) struct Append {
    pub(super) record: Vec<u8>,
    pub(super) ack: u8,
    /// Ok carries the sink (chunk) the record went to.
    pub(super) done: mpsc::Sender<io::Result<u64>>,
}
pub(super) struct AppendHandle {
    pub(super) sender: Option<mpsc::Sender<Append>>,
    pub(super) handle: Option<std::thread::JoinHandle<()>>,
}

impl AppendOwner {
    pub(super) fn start(
        stripe: usize,
        route: Arc<dyn Route>,
        config: Config,
        flush: FlushIngress,
    ) -> AppendHandle {
        let (sender, receiver) = mpsc::channel();
        let worker = Self {
            receiver,
            stripe,
            route,
            seams: TestSeam::for_append(&config),
            config,
            flush,
        };
        let handle = std::thread::spawn(move || worker.run());
        AppendHandle {
            sender: Some(sender),
            handle: Some(handle),
        }
    }
}
/// Each stripe transfers this context into its own append thread.
pub(super) struct AppendOwner {
    receiver: mpsc::Receiver<Append>,
    stripe: usize,
    route: Arc<dyn Route>,
    config: Config,
    flush: FlushIngress,
    seams: TestSeam,
}
impl AppendOwner {
    fn run(self) {
        let Self {
            receiver,
            stripe,
            route,
            config,
            flush,
            seams,
        } = self;
        while let Ok(job) = receiver.recv() {
            seams.before_append();
            let (result, sink, end) = route.write(stripe, &job.record, &config);
            let result = seams.after_append(result);
            if result.is_ok() && job.ack == 2 {
                seams.before_flush_notice();
                let wait = FlushWait {
                    stripe: sink,
                    end,
                    bytes: job.record.len(),
                    done: job.done,
                    gathered: false,
                };
                flush.enqueue(wait);
                seams.after_flush_notice();
            } else {
                let _ = job.done.send(result.map(|_| sink as u64));
            }
        }
    }
}
