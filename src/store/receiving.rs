//! Private receiving evidence from the Store's owned catalogue and retained frames.
use crate::{atomic, chunks, codec, store::committed_get, Class, Store};
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

#[derive(Debug)]
pub struct DurablePlacement {
    pub sequence: u64,
    pub stripe: usize,
    pub class: Class,
    pub state: chunks::ChunkState,
    pub offset: u64,
    pub frame_bytes: u64,
}

#[derive(PartialEq, Eq)]
struct FrameLocation {
    sequence: u64,
    offset: u64,
    len: u64,
    member: Option<u16>,
}

fn invalid(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

fn locate(
    path: &Path,
    sequence: u64,
    len: u64,
    identity: (&str, &str, u64),
    expected: &[u8],
    found: &mut Option<FrameLocation>,
) -> io::Result<()> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != len {
        return Err(invalid("receiving chunk byte boundary changed"));
    }
    let name = path.display().to_string();
    let mut offset = 0;
    while offset < len {
        let chunks::Frame::Whole(payload, frame_bytes) =
            chunks::read_frame_at(&mut file, b"MBK1", offset, len, &name)?
        else {
            return Err(invalid("receiving chunk has a torn frame"));
        };
        let mut error = None;
        let mut visit = |member, record: codec::RecordView<'_>| {
            if (record.ns, record.key, record.version) != identity {
                return;
            }
            if found.is_some() {
                error = Some(invalid(
                    "multiple durable records match the received revision",
                ));
            } else if record.remove || record.ack != 2 || record.bytes != expected {
                error = Some(invalid("durable frame differs from the received value"));
            }
            *found = Some(FrameLocation {
                sequence,
                offset,
                len: frame_bytes,
                member,
            });
        };
        if atomic::is_envelope(&payload) {
            let envelope = atomic::decode_envelope(&payload)
                .map_err(|_| invalid("receiving chunk has an invalid atomic envelope"))?;
            envelope.visit(|member, record| visit(Some(member), record));
        } else {
            let record = codec::parse_record_view(&payload)
                .map_err(|_| invalid("receiving chunk has an invalid durable record"))?;
            visit(None, record);
        }
        if let Some(error) = error {
            return Err(error);
        }
        offset += frame_bytes;
    }
    Ok(())
}

impl Store {
    /// Verify the flushed value's unique retained frame and capture the catalogue under
    /// the same ownership. For an exclusively held private receiving root.
    pub fn inspect_durable_placement(
        &self,
        ns: &str,
        key: &str,
        revision: u64,
        expected: &[u8],
    ) -> io::Result<(DurablePlacement, Vec<u8>)> {
        let _publication = self.admission_gate.write().unwrap();
        let identity = (ns, key, revision);
        let key_pair = (ns.to_owned(), key.to_owned());
        let sequence = {
            let state = self.state.lock().unwrap();
            if self.storage_error() || state.read_only {
                return Err(invalid("receiving storage health is incomplete"));
            }
            state
                .metadata
                .get(&key_pair)
                .filter(|meta| meta.version == revision && !meta.tombstone)
                .ok_or_else(|| invalid("received revision has no current durable index entry"))?
                .chunk
        };
        let entry = committed_get(&self.committed.read().unwrap(), ns, key)
            .filter(|entry| entry.version == revision && entry.ack == 2 && !entry.tombstone)
            .cloned()
            .ok_or_else(|| invalid("received revision has no flushed value entry"))?;
        if entry.read(ns, key)?.bytes != expected {
            return Err(invalid("durable frame differs from the received value"));
        }
        let chunks = self.chunks.lock().unwrap();
        let cat = chunks
            .cat
            .as_ref()
            .ok_or_else(|| invalid("receiving catalogue unavailable"))?;
        if cat.recovery_required()? {
            return Err(invalid("receiving catalogue append unresolved"));
        }
        let selected = chunks
            .table
            .get(&sequence)
            .filter(|chunk| {
                chunk.id != chunks::LEGACY_CHUNK
                    && matches!(
                        chunk.state,
                        chunks::ChunkState::Open | chunks::ChunkState::Closed
                    )
            })
            .ok_or_else(|| invalid("received revision has no active catalogue chunk"))?;

        // Holder ranges narrow the work. A range alone proves neither exact membership
        // nor uniqueness. Replayed legacy backups serve retries only, outside this table.
        let mut found = None;
        for id in chunks.holders.map.get(&key_pair).into_iter().flatten() {
            let chunk = &chunks.table[id];
            if !matches!(
                chunk.state,
                chunks::ChunkState::Open | chunks::ChunkState::Closed
            ) || !chunk
                .keys
                .get(&key_pair)
                .is_some_and(|&(low, high)| (low..=high).contains(&revision))
            {
                continue;
            }
            if *id == chunks::LEGACY_CHUNK {
                for (_, path) in crate::replay::legacy_files(&self.path) {
                    let len = path.metadata()?.len();
                    locate(&path, *id, len, identity, expected, &mut found)?;
                }
            } else {
                locate(
                    &chunk.path,
                    *id,
                    chunk.bytes,
                    identity,
                    expected,
                    &mut found,
                )?;
            }
        }
        let location = &entry.location;
        if found
            != Some(FrameLocation {
                sequence: selected.id,
                offset: location.offset,
                len: location.record_len,
                member: location.subrecord,
            })
            || location.class != selected.class
        {
            return Err(invalid(
                "durable frame does not match the indexed catalogue placement",
            ));
        }

        const MAX_CATALOGUE: u64 = 64 * 1024 * 1024;
        if cat.bytes > MAX_CATALOGUE {
            return Err(invalid("receiving catalogue limit"));
        }
        let mut catalogue = Vec::new();
        File::open(cat.path())?
            .take(MAX_CATALOGUE + 1)
            .read_to_end(&mut catalogue)?;
        if catalogue.len() as u64 != cat.bytes {
            return Err(invalid("receiving catalogue byte boundary changed"));
        }
        Ok((
            DurablePlacement {
                sequence: selected.id,
                stripe: selected.stripe,
                class: selected.class,
                state: selected.state,
                offset: location.offset,
                frame_bytes: location.record_len,
            },
            catalogue,
        ))
    }
}
