//! Immutable index entries retain a shared read handle, never a payload.
use crate::{chunks, codec::parse_record, Mutation, Value};
use std::{
    fs::File,
    io::{self, Seek, SeekFrom},
    sync::{atomic::AtomicBool, Arc, Mutex},
};

#[derive(Clone, Debug)]
pub(crate) struct Location {
    pub class: crate::Class,
    pub file: Arc<Mutex<File>>,
    pub offset: u64,
    pub record_len: u64,
}
impl Location {
    pub fn payload(&self) -> io::Result<Vec<u8>> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(self.offset))?;
        match chunks::read_frame_at(
            &mut file,
            b"MBK1",
            self.offset,
            self.offset + self.record_len,
            "indexed chunk",
        )? {
            chunks::Frame::Whole(bytes, _) => Ok(bytes),
            _ => Err(io::Error::other("short indexed record")),
        }
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
    pub fn new(m: &Mutation, version: u64, location: Location) -> Self {
        Self {
            cacheable: AtomicBool::new(true),
            class: location.class,
            version,
            ack: m.ack,
            tombstone: m.remove,
            len: m.bytes.len(),
            location,
        }
    }
    pub fn read(&self) -> io::Result<Arc<Value>> {
        let bytes = self.location.payload()?;
        let (m, version, _) =
            parse_record(&bytes).map_err(|_| io::Error::other("invalid indexed record"))?;
        if version != self.version
            || m.bytes.len() != self.len
            || m.ack != self.ack
            || m.remove != self.tombstone
        {
            return Err(io::Error::other("indexed record changed"));
        }
        Ok(Arc::new(Value {
            version,
            ack: self.ack,
            bytes: m.bytes,
            tombstone: self.tombstone,
        }))
    }
}
