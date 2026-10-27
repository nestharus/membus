//! Immutable index entries retain a shared read handle, never a payload.
use crate::{chunks, codec::parse_record_view, Mutation, Value};
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
    pub subrecord: Option<u16>,
}
impl Location {
    pub fn payload(&self) -> io::Result<Vec<u8>> {
        let captured = {
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::Start(self.offset))?;
            chunks::capture_frame_at(
                &mut file,
                b"MBK1",
                self.offset,
                self.offset + self.record_len,
                "indexed chunk",
            )?
        };
        match captured.validate(self.offset, "indexed chunk")? {
            // The index identifies one exact record, rather than the larger
            // file boundary accepted by the general chunk reader.
            chunks::Frame::Whole(bytes, n) if n == self.record_len => match self.subrecord {
                Some(index)=>crate::atomic::subrecord(&bytes,index).map_err(|_|io::Error::other("invalid envelope subrecord")),
                None=>Ok(bytes),
            },
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
    pub fn read(&self, namespace: &str, key: &str) -> io::Result<Arc<Value>> {
        let bytes = self.location.payload()?;
        let m =
            parse_record_view(&bytes).map_err(|_| io::Error::other("invalid indexed record"))?;
        if m.ns != namespace
            || m.key != key
            || m.version != self.version
            || m.bytes.len() != self.len
            || m.ack != self.ack
            || m.remove != self.tombstone
        {
            return Err(io::Error::other("indexed record changed"));
        }
        Ok(Arc::new(Value {
            version: m.version,
            ack: self.ack,
            bytes: m.bytes.to_vec(),
            tombstone: self.tombstone,
        }))
    }
}
