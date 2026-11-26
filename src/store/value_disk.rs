//! Immutable index entries retain a shared read handle, never a payload.
use crate::{chunks, codec::{crc32, parse_record_view}, platform::PositionedReader, Mutation, Value};
use std::{
    cell::RefCell,
    fmt,
    io,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
};
#[cfg(test)]
use std::sync::{LockResult, RwLock, RwLockWriteGuard};

const MAX_RETAINED_SCRATCH: usize = 256 * 1024;

thread_local! {
    static READ_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Concurrent positioned reader for an immutable or append-only chunk file.
pub(crate) struct ChunkReader {
    reader: PositionedReader,
    #[cfg(test)]
    test_gate: RwLock<()>,
}
impl fmt::Debug for ChunkReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkReader").finish_non_exhaustive()
    }
}
impl ChunkReader {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            reader: PositionedReader::open(path)?,
            #[cfg(test)]
            test_gate: RwLock::new(()),
        })
    }
    pub(crate) fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        #[cfg(test)]
        let _gate = self.test_gate.read().unwrap();
        self.reader.read_exact_at(buf, offset)
    }
    /// White-box test seam to pause reads on a chunk without blocking concurrent readers otherwise.
    #[cfg(test)]
    pub(crate) fn lock(&self) -> LockResult<RwLockWriteGuard<'_, ()>> {
        self.test_gate.write()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Location {
    pub class: crate::Class,
    pub file: Arc<ChunkReader>,
    pub offset: u64,
    pub record_len: u64,
    pub subrecord: Option<u16>,
}
impl Location {
    fn validate_frame_into(&self, buf: &mut Vec<u8>) -> io::Result<()> {
        let _end = self.offset.checked_add(self.record_len)
            .ok_or_else(|| io::Error::other("indexed extent overflow"))?;
        let record_len = usize::try_from(self.record_len)
            .map_err(|_| io::Error::other("short indexed record"))?;
        if record_len < 16 {
            return Err(io::Error::other("short indexed record"));
        }
        if record_len > crate::MAX_FRAME + 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("interior-corrupt indexed chunk offset {}: invalid chunk length", self.offset),
            ));
        }
        buf.resize(record_len, 0);
        self.file.read_exact_at(buf, self.offset)?;
        #[cfg(test)]
        crate::read_work_tests::work(0, record_len);
        let head: &[u8; 16] = buf[0..16].try_into().unwrap();
        let (n, expected) = chunks::parse_frame_header(head, b"MBK1", self.offset, "indexed chunk")?;
        // The index identifies one exact record, rather than the larger
        // file boundary accepted by the general chunk reader.
        if 16 + n != record_len {
            return Err(io::Error::other("short indexed record"));
        }
        #[cfg(test)]
        crate::read_work_tests::work(1, n);
        #[cfg(test)]
        chunks::before_cold_validation();
        if crc32(&buf[16..]) != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("interior-corrupt indexed chunk offset {}: CRC", self.offset),
            ));
        }
        Ok(())
    }
    fn with_validated_payload<R>(&self, f: impl FnOnce(&[u8]) -> io::Result<R>) -> io::Result<R> {
        READ_SCRATCH.with(|slot| {
            if let Ok(mut guard) = slot.try_borrow_mut() {
                let res = self.validate_frame_into(&mut guard).and_then(|()| f(&guard[16..]));
                if guard.capacity() > MAX_RETAINED_SCRATCH {
                    *guard = Vec::new();
                }
                res
            } else {
                let mut local = Vec::new();
                self.validate_frame_into(&mut local)?;
                f(&local[16..])
            }
        })
    }
    pub(crate) fn whole_payload(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.validate_frame_into(&mut buf)?;
        let n = buf.len() - 16;
        buf.copy_within(16.., 0);
        buf.truncate(n);
        Ok(buf)
    }
    pub fn payload(&self) -> io::Result<Vec<u8>> {
        let bytes = self.whole_payload()?;
        match self.subrecord {
            Some(index) => crate::atomic::subrecord(&bytes, index)
                .map_err(|_| io::Error::other("invalid envelope subrecord")),
            None => Ok(bytes),
        }
    }
    pub(crate) fn same_frame(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.file, &other.file)
            && self.offset == other.offset && self.record_len == other.record_len
            && self.subrecord.is_some() == other.subrecord.is_some()
    }
}
#[derive(Debug)]
pub(crate) struct Entry {
    pub cacheable: AtomicBool,
    pub class: crate::Class,
    pub version: u64,
    pub ack: u8,
    pub tombstone: bool,
    pub len: usize,
    pub location: Location,
}
impl Entry {
    pub(crate) fn check_identity(&self, namespace: &str, key: &str,
        actual: (&str, &str, u64, u8, bool, usize)) -> io::Result<()> {
        if actual != (namespace, key, self.version, self.ack, self.tombstone, self.len) {
            return Err(io::Error::other("indexed record changed"));
        }
        Ok(())
    }
    pub fn new(m: &Mutation, version: u64, location: Location) -> Self {
        Self::from_view(version, m.ack, m.remove, m.bytes.len(), location)
    }
    pub(crate) fn from_view(
        version: u64,
        ack: u8,
        tombstone: bool,
        len: usize,
        location: Location,
    ) -> Self {
        Self {
            cacheable: AtomicBool::new(true),
            class: location.class,
            version,
            ack,
            tombstone,
            len,
            location,
        }
    }
    pub fn read(&self, namespace: &str, key: &str) -> io::Result<Arc<Value>> {
        let decode = |payload: &[u8]| {
            let m = parse_record_view(payload)
                .map_err(|_| io::Error::other("invalid indexed record"))?;
            self.check_identity(
                namespace,
                key,
                (m.ns, m.key, m.version, m.ack, m.remove, m.bytes.len()),
            )?;
            Ok(Arc::new(Value {
                version: m.version,
                ack: self.ack,
                bytes: m.bytes.to_vec(),
                tombstone: self.tombstone,
            }))
        };
        if self.location.subrecord.is_none() {
            self.location.with_validated_payload(decode)
        } else {
            let bytes = self.location.payload()?;
            decode(&bytes)
        }
    }
}
