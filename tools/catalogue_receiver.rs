//! Independent read-only receiving decoder for complete MBC1 publications. It deliberately shares
//! neither the producer decoder, frame scanner, cursor, CRC routine nor live-list implementation.
//! It uses the public record types only as a comparison vocabulary. Input is bounded to 64 MiB.
use membus::chunks::{CatRecord, Class, Import, Rec, ResumeBasis};
use std::{fs, io, path::Path};

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| io::Error::other("length overflow"))?;
        let b = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| io::Error::other("short receiving record"))?;
        self.pos = end;
        Ok(b)
    }
    fn byte(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn word(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn wide(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn text(&mut self) -> io::Result<String> {
        let n = self.word()? as usize;
        String::from_utf8(self.take(n)?.to_vec()).map_err(io::Error::other)
    }
    fn key(&mut self) -> io::Result<(String, String)> {
        Ok((self.text()?, self.text()?))
    }
    fn horizons(&mut self) -> io::Result<Vec<(String, String, u64)>> {
        let n = self.word()?;
        if n as usize > self.bytes.len() / 16 {
            return Err(io::Error::other("horizon count"));
        }
        (0..n)
            .map(|_| Ok((self.text()?, self.text()?, self.wide()?)))
            .collect()
    }
    fn keys(&mut self) -> io::Result<Vec<(String, String)>> {
        let n = self.word()?;
        if n as usize > self.bytes.len() / 8 {
            return Err(io::Error::other("key count"));
        }
        (0..n).map(|_| self.key()).collect()
    }
}
fn checksum(b: &[u8]) -> u32 {
    let mut c = u32::MAX;
    for byte in b {
        c ^= u32::from(*byte);
        for _ in 0..8 {
            c = (c >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(c & 1));
        }
    }
    !c
}
fn record(bytes: &[u8]) -> io::Result<CatRecord> {
    let mut r = Reader { bytes, pos: 0 };
    let kind = r.byte()?;
    let at_ms = r.wide()?;
    let rec = match kind {
        1 => {
            let seq = r.wide()?;
            let stripe = r.byte()?;
            let kind = r.byte()?;
            let retention_ms = r.wide()?;
            if stripe >= 4
                || kind > 2
                || (kind < 2 && retention_ms != 0)
                || (kind == 2 && retention_ms == 0)
            {
                return Err(io::Error::other("receiving open class/stripe"));
            }
            Rec::Open {
                seq,
                stripe,
                class: Class { kind, retention_ms },
                policy: r.text()?,
                next: r.wide()?,
            }
        }
        2 => Rec::Close {
            seq: r.wide()?,
            bytes: r.wide()?,
            next: r.wide()?,
        },
        3 | 10 => {
            let seq = r.wide()?;
            let next = r.wide()?;
            let part = if kind == 10 {
                let index = r.word()?;
                let count = r.word()?;
                if count == 0 || index >= count {
                    return Err(io::Error::other("retirement part index/count"));
                }
                Some((index, count))
            } else {
                None
            };
            let horizons = r.horizons()?;
            let expired = r.keys()?;
            match part {
                Some((index, count)) => Rec::RetiringPart { seq, next, index, count, horizons, expired },
                None => Rec::Retiring { seq, next, horizons, expired },
            }
        }
        4 => Rec::Retired { seq: r.wide()? },
        5 => Rec::Horizons(r.horizons()?),
        6 => Rec::Summary {
            retired_ms: r.wide()?,
            keys: r.keys()?,
        },
        7 => Rec::Next(r.wide()?),
        8 => Rec::Resume,
        9 => {
            let n = r.word()?;
            if n as usize > bytes.len() / 16 {
                return Err(io::Error::other("import count"));
            }
            let files = (0..n)
                .map(|_| Ok((r.text()?, r.wide()?, r.text()?)))
                .collect::<io::Result<_>>()?;
            Rec::Import(Import {
                files,
                keys: r.wide()?,
                bytes: r.wide()?,
                epoch: r.wide()?,
                legacy_dir: r.text()?,
            })
        }
        11 => Rec::ResumeWithBasis(match r.byte()? {
            1 => ResumeBasis::Operator,
            2 => ResumeBasis::StableClock {
                since_utc_ms: r.wide()?,
                since_mono_ms: r.wide()?,
                observed_mono_ms: r.wide()?,
                required_ms: r.wide()?,
                tolerance_ms: r.wide()?,
            },
            _ => return Err(io::Error::other("resume basis")),
        }),
        _ => return Err(io::Error::other("unknown receiving record kind")),
    };
    if r.pos != bytes.len() {
        return Err(io::Error::other("extra receiving payload"));
    }
    Ok(CatRecord { at_ms, rec })
}
pub fn decode(bytes: &[u8]) -> io::Result<Vec<CatRecord>> {
    if bytes.len() > 64 * 1024 * 1024 {
        return Err(io::Error::other("receiving catalogue limit"));
    }
    let mut input = Reader { bytes, pos: 0 };
    let mut out = Vec::new();
    while input.pos < bytes.len() {
        if input.take(4)? != b"MBC1" {
            return Err(io::Error::other("receiving magic"));
        }
        let len = input.word()?;
        if input.word()? != !len {
            return Err(io::Error::other("receiving length complement"));
        }
        if len as usize > 16 * 1024 * 1024 {
            return Err(io::Error::other("receiving frame limit"));
        }
        let crc = input.word()?;
        let payload = input.take(len as usize)?;
        if checksum(payload) != crc {
            return Err(io::Error::other("receiving CRC"));
        }
        out.push(record(payload)?);
    }
    Ok(out)
}
#[allow(dead_code)]
pub fn read(path: &Path) -> io::Result<Vec<CatRecord>> {
    capture(path).map(|(_, records)| records)
}
/// Return the exact bytes the receiving decoder observed with their decoded records.
#[allow(dead_code)]
pub fn capture(path: &Path) -> io::Result<(Vec<u8>, Vec<CatRecord>)> {
    if fs::metadata(path)?.len() > 64 * 1024 * 1024 {
        return Err(io::Error::other("receiving catalogue limit"));
    }
    let bytes = fs::read(path)?;
    let records = decode(&bytes)?;
    Ok((bytes, records))
}
