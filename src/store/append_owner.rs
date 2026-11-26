//! Per-stripe append owner: record ingress, routed writes and completion handoff.
use crate::chunks::ChunkIndexReservation;
use crate::flush_owner::FlushIngress;
use crate::flush_owner::FlushWait;
use crate::routing::Route;
use crate::test_seam::TestSeam;
use crate::Config;
use std::io;
use std::sync::mpsc;
use std::sync::Arc;

pub(crate) struct Append {
    pub(crate) record: Vec<u8>,
    pub(crate) ack: u8,
    pub(crate) chunk_index: Option<Arc<ChunkIndexReservation>>,
    /// Ok carries the sink (chunk) the record went to.
    pub(crate) done: mpsc::Sender<io::Result<u64>>,
}
pub(crate) struct AppendHandle {
    pub(crate) sender: Option<mpsc::Sender<Append>>,
    pub(crate) handle: Option<std::thread::JoinHandle<()>>,
}

impl AppendOwner {
    pub(crate) fn start(
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
pub(crate) struct AppendOwner {
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
        while let Ok(Append {
            record,
            ack,
            chunk_index,
            done,
        }) = receiver.recv()
        {
            seams.before_append();
            let (result, sink, end) = route.write(stripe, &record, &config, chunk_index.as_deref());
            // Store still owns the reservation. Drop the append owner's clone
            // before completion or Flush handoff so Store can release it at settlement.
            drop(chunk_index);
            let result = seams.after_append(result);
            if result.is_ok() && ack == 2 {
                seams.before_flush_notice();
                let wait = FlushWait {
                    stripe: sink,
                    end,
                    bytes: record.len(),
                    done,
                    gathered: false,
                };
                flush.enqueue(wait);
                seams.after_flush_notice();
            } else {
                let _ = done.send(result.map(|_| sink as u64));
            }
        }
    }
}
