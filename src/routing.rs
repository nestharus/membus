//! Stripe and class routing, chunk rotation, sink identity and fencing.
use crate::chunks;
use crate::codec::crc32;
use crate::log_io::append_to;
use crate::store::Flags;
use crate::test_seam::TestSeam;
use crate::Class;
use crate::Config;
use crate::Log;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
#[cfg(test)]
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

fn sync_chunk_directory(path: &std::path::Path, _config: &Config) -> io::Result<()> {
    #[cfg(test)]
    if let Some(hook) = &_config.chunk_dir_sync { hook(path)?; }
    crate::log_io::sync_dir(path)
}

/// A selected route or admitted policy owns its class until a chunk or the published policy owns it.
/// Coordination order: reservations -> policies -> chunks. None is held across queued I/O;
/// router stripe/chunk/table locks and maintenance never acquire reservations in reverse.
pub(super) struct ClassReservation {
    pub(super) counts: Arc<Mutex<HashMap<Class, usize>>>,
    pub(super) class: Class,
}
impl ClassReservation {
    pub(super) fn add(
        counts: &Arc<Mutex<HashMap<Class, usize>>>,
        held: &mut HashMap<Class, usize>,
        class: Class,
    ) -> Self {
        *held.entry(class).or_default() += 1;
        Self {
            counts: counts.clone(),
            class,
        }
    }
}
impl Drop for ClassReservation {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().unwrap();
        let n = counts.get_mut(&self.class).unwrap();
        *n -= 1;
        if *n == 0 {
            counts.remove(&self.class);
        }
    }
}
/// Where a stripe's records go. The append owner writes through it; the flush owner syncs what it names.
pub(super) trait Route: Send + Sync + 'static {
    /// Append one record for `stripe`: the result, the sink written (its flush identity) and the sink's end.
    fn write(&self, stripe: usize, record: &[u8], config: &Config) -> (io::Result<()>, usize, u64);
    /// A sink's stripe, its log while open, and the extent its close synced.
    fn sink(&self, id: usize) -> Option<(usize, Option<Arc<Mutex<Log>>>, u64)>;
    fn fence(&self, stripe: usize);
}
/// One fixed log per stripe (the pre-chunk shape, kept for the owner-level tests).
#[cfg(test)]
pub(super) struct FixedRoute(pub(super) Vec<Arc<Mutex<Log>>>);
#[cfg(test)]
impl Route for FixedRoute {
    fn write(&self, stripe: usize, record: &[u8], config: &Config) -> (io::Result<()>, usize, u64) {
        let mut log = self.0[stripe].lock().unwrap();
        let result = append_to(&mut log, record, config);
        (result, stripe, log.bytes)
    }
    fn sink(&self, id: usize) -> Option<(usize, Option<Arc<Mutex<Log>>>, u64)> {
        self.0.get(id).map(|log| (id, Some(log.clone()), 0))
    }
    fn fence(&self, stripe: usize) {
        self.0[stripe].lock().unwrap().poisoned = true;
    }
}
pub(super) struct OpenChunk {
    pub(super) reader: Arc<Mutex<std::fs::File>>,
    pub(super) id: u64,
    pub(super) log: Arc<Mutex<Log>>,
    pub(super) opened_mono: u64,
}
pub(super) struct SinkEntry {
    pub(super) stripe: usize,
    pub(super) log: Option<Arc<Mutex<Log>>>,
    pub(super) synced: u64,
}
/// Chunk files by stripe and class (CHUNK-RETIREMENT.md 5.2). Each stripe's open chunks are held under that
/// stripe's lock while a record is written, so a close never meets a half-written record.
pub(super) struct ChunkRouter {
    pub(super) locations: Mutex<HashMap<u64, crate::value_disk::Location>>,
    #[cfg(test)]
    pub(super) selection_hook: Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>,
    pub(super) sinks: Vec<Mutex<HashMap<Class, OpenChunk>>>,
    pub(super) table: RwLock<HashMap<usize, SinkEntry>>,
    pub(super) fenced: [AtomicBool; 4],
    pub(super) closed_io: Mutex<[u64; 3]>,
    pub(super) policies: Arc<RwLock<chunks::Policies>>,
    pub(super) class_reservations: Arc<Mutex<HashMap<Class, usize>>>,
    pub(super) chunks: Arc<Mutex<chunks::ChunkStore>>,
    pub(super) clock: chunks::Clock,
    pub(super) next: Arc<AtomicU64>,
    pub(super) flags: Arc<Flags>,
    pub(super) chunk_bytes: u64,
    pub(super) rotation_ms: u64,
    pub(super) fail_sync: bool,
}
impl ChunkRouter {
    pub(super) fn open_chunk(
        &self,
        stripe: usize,
        class: Class,
        policy: String,
        config: &Config,
    ) -> io::Result<OpenChunk> {
        let mut cs = self.chunks.lock().unwrap();
        let id = cs.next_id;
        let path = chunks::chunk_path(&cs.dir, class, stripe, id);
        let now = self.clock.utc_ms();
        // The open record is flushed before the file exists, so a crash leaves at most a record with no file.
        cs.log(
            now,
            chunks::Rec::Open {
                seq: id,
                stripe: stripe as u8,
                class,
                policy: policy.clone(),
                next: self.next.load(Ordering::SeqCst),
            },
        )?;
        cs.next_id += 1;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let reader = Arc::new(Mutex::new(std::fs::File::open(&path)?));
        // Persist the file name before its class-directory name, before a route
        // or first frame can yield a successful acknowledgement. On failure,
        // retain the durable Open and empty file for the ordinary replay path.
        sync_chunk_directory(path.parent().unwrap(), config)?;
        sync_chunk_directory(&cs.dir, config)?;
        cs.table.insert(
            id,
            chunks::ChunkInfo {
                id,
                stripe,
                class,
                policy,
                open_ms: now,
                close_ms: None,
                close_mono: None,
                bytes: 0,
                path,
                state: chunks::ChunkState::Open,
                keys: HashMap::new(),
                blocked_since: None,
                horizons: Vec::new(),
                expired: Vec::new(),
                retired_ms: None,
            },
        );
        drop(cs);
        let log = Arc::new(Mutex::new(Log {
            file,
            bytes: 0,
            flushes: 0,
            write_calls: 0,
            poisoned: false,
        }));
        self.table.write().unwrap().insert(
            id as usize,
            SinkEntry {
                stripe,
                log: Some(log.clone()),
                synced: 0,
            },
        );
        Ok(OpenChunk {
            reader,
            id,
            log,
            opened_mono: self.clock.mono_ms(),
        })
    }
    /// Sync the chunk, then record its close. A close that cannot be recorded leaves the chunk open.
    pub(super) fn close_chunk(&self, stripe: usize, c: &OpenChunk) -> io::Result<()> {
        let (file, bytes, writes, flushes) = {
            let l = c.log.lock().unwrap();
            if l.poisoned {
                return Err(io::Error::other("poisoned chunk stays open for replay"));
            }
            (l.file.try_clone()?, l.bytes, l.write_calls, l.flushes)
        };
        let synced = TestSeam::sync_closing_chunk(&file, self.fail_sync);
        if let Err(e) = synced {
            c.log.lock().unwrap().poisoned = true;
            self.fence(stripe);
            self.flags.storage_error.store(true, Ordering::SeqCst);
            return Err(e);
        }
        let now = self.clock.utc_ms();
        {
            let mut cs = self.chunks.lock().unwrap();
            if let Err(e) = cs.log(
                now,
                chunks::Rec::Close {
                    seq: c.id,
                    bytes,
                    next: self.next.load(Ordering::SeqCst),
                },
            ) {
                self.flags.storage_error.store(true, Ordering::SeqCst);
                return Err(e);
            }
            if let Some(info) = cs.table.get_mut(&c.id) {
                info.state = chunks::ChunkState::Closed;
                info.close_ms = Some(now);
                info.close_mono = Some(self.clock.mono_ms());
                info.bytes = bytes;
            }
        }
        if let Some(e) = self.table.write().unwrap().get_mut(&(c.id as usize)) {
            e.log = None;
            e.synced = bytes;
        }
        let mut io = self.closed_io.lock().unwrap();
        io[0] += bytes;
        io[1] += writes;
        io[2] += flushes + 1;
        Ok(())
    }
    /// The idle tick: close finite-class chunks past their rotation age.
    pub(super) fn idle_close(&self) {
        for stripe in 0..self.sinks.len() {
            let mut open = self.sinks[stripe].lock().unwrap();
            let now = self.clock.mono_ms();
            let due: Vec<Class> = open
                .iter()
                .filter(|(class, c)| {
                    class.finite() && now.saturating_sub(c.opened_mono) >= self.rotation_ms
                })
                .map(|(class, _)| *class)
                .collect();
            for class in due {
                if self.close_chunk(stripe, &open[&class]).is_ok() {
                    open.remove(&class);
                }
            }
        }
    }
    pub(super) fn io(&self) -> (u64, u64, u64) {
        let closed = *self.closed_io.lock().unwrap();
        let (mut bytes, mut writes, mut flushes) = (closed[0], closed[1], closed[2]);
        for stripe in &self.sinks {
            for c in stripe.lock().unwrap().values() {
                let l = c.log.lock().unwrap();
                bytes += l.bytes;
                writes += l.write_calls;
                flushes += l.flushes;
            }
        }
        (bytes, writes, flushes)
    }
    pub(super) fn open_chunks(&self) -> usize {
        self.sinks.iter().map(|s| s.lock().unwrap().len()).sum()
    }
}
impl Route for ChunkRouter {
    fn write(&self, stripe: usize, record: &[u8], config: &Config) -> (io::Result<()>, usize, u64) {
        let Some((ns, key, version)) = record.get(16..).and_then(chunks::record_key) else {
            return (Err(io::Error::other("unroutable record")), 0, 0);
        };
        let (class, policy, _selection) = {
            let mut counts = self.class_reservations.lock().unwrap();
            let (class, policy) = self.policies.read().unwrap().resolve(&ns, &key);
            let selected = ClassReservation::add(&self.class_reservations, &mut counts, class);
            (class, policy, selected)
        };
        #[cfg(test)]
        {
            let hook = self.selection_hook.lock().unwrap().take();
            if let Some((reached, resume)) = hook {
                let _ = reached.send(());
                let _ = resume.recv();
            }
        }
        let mut open = self.sinks[stripe].lock().unwrap();
        if self.fenced[stripe].load(Ordering::SeqCst) {
            return (
                Err(io::Error::other("stripe fenced after failed append")),
                0,
                0,
            );
        }
        let now = self.clock.mono_ms();
        let rotate = open.get(&class).is_some_and(|c| {
            let b = c.log.lock().unwrap().bytes;
            (b > 0 && b + record.len() as u64 > self.chunk_bytes)
                || (class.finite() && now.saturating_sub(c.opened_mono) >= self.rotation_ms)
        });
        if rotate {
            if let Err(e) = self.close_chunk(stripe, &open[&class]) {
                self.fence(stripe);
                return (Err(e), 0, 0);
            }
            open.remove(&class);
        }
        if !open.contains_key(&class) {
            match self.open_chunk(stripe, class, policy, config) {
                Ok(c) => {
                    open.insert(class, c);
                }
                Err(e) => {
                    self.fence(stripe);
                    return (Err(e), 0, 0);
                }
            }
        }
        let c = &open[&class];
        let mut log = c.log.lock().unwrap();
        let result = append_to(&mut log, record, config);
        let end = log.bytes;
        drop(log);
        if result.is_err() {
            self.fence(stripe);
        } else {
            self.locations.lock().unwrap().insert(version, crate::value_disk::Location {
                class, file: c.reader.clone(), offset: end - record.len() as u64, record_len: record.len() as u64,
            });
            let mut cs = self.chunks.lock().unwrap();
            cs.register(c.id, &(ns, key), version);
            if let Some(info) = cs.table.get_mut(&c.id) {
                info.bytes = end;
            }
        }
        (result, c.id as usize, end)
    }
    fn sink(&self, id: usize) -> Option<(usize, Option<Arc<Mutex<Log>>>, u64)> {
        self.table
            .read()
            .unwrap()
            .get(&id)
            .map(|e| (e.stripe, e.log.clone(), e.synced))
    }
    fn fence(&self, stripe: usize) {
        self.fenced[stripe].store(true, Ordering::SeqCst);
    }
}

pub(super) fn stripe(ns: &str, key: &str) -> usize {
    let mut b = ns.as_bytes().to_vec();
    b.push(0);
    b.extend(key.as_bytes());
    crc32(&b) as usize % 4
}

#[cfg(test)]
#[path = "routing/directory_tests.rs"]
mod directory_tests;
