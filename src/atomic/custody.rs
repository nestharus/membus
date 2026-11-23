use super::AtomicRequest;
use crate::log_io;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
};

const WRITE_BUFFER_BYTES: usize = 8192;

impl AtomicRequest {
    /// New-only caller-owned custody file. Never replaces a possibly sent original.
    pub fn persist_new(&self, path: &Path) -> io::Result<()> {
        self.persist_file(path, OpenOptions::new(), log_io::sync_file)
            .map(drop)
    }

    fn persist_file(
        &self,
        path: &Path,
        mut options: OpenOptions,
        sync_file: impl FnOnce(&File) -> io::Result<()>,
    ) -> io::Result<File> {
        let mut file = options.write(true).create_new(true).open(path)?;
        self.write_original(&mut file)?;
        sync_file(&file)?;
        log_io::sync_dir(
            path.parent()
                .ok_or_else(|| io::Error::other("custody requires parent"))?,
        )?;
        Ok(file)
    }

    // Use the canonical encoder on the already funded original. Small headers
    // share a fixed stack buffer. Large values go directly to the file.
    fn write_original(&self, out: &mut impl Write) -> io::Result<()> {
        let mut buffer = [0; WRITE_BUFFER_BYTES];
        let mut used = 0;
        let mut emit = |part: &[u8]| -> io::Result<()> {
            if part.len() > buffer.len() - used {
                out.write_all(&buffer[..used])?;
                used = 0;
            }
            if part.len() >= buffer.len() {
                return out.write_all(part);
            }
            buffer[used..used + part.len()].copy_from_slice(part);
            used += part.len();
            Ok(())
        };
        let mut result = Ok(());
        self.encoding().write(|part| {
            if result.is_ok() {
                result = emit(part);
            }
        });
        result?;
        out.write_all(&buffer[..used])
    }
}

/// A successful new-only MAR save in the caller's protected directory on Windows.
/// Holds the actual writer open, denying other write and delete opens until drop.
#[cfg(windows)]
#[must_use = "keep the writer held through the owner's identity handoff"]
pub struct HeldOriginalFile {
    _writer: File,
    id: [u8; 24],
    _charge: super::Reservation,
}

#[cfg(windows)]
impl HeldOriginalFile {
    /// Additional bytes required on the original's account before creating the file.
    pub const SAVE_BYTES: usize = WRITE_BUFFER_BYTES + size_of::<Self>();

    /// Windows FILE_ID_INFO from the acknowledged writer, as a little-endian
    /// 64-bit volume serial number followed by the 128-bit file ID.
    pub fn file_id(&self) -> &[u8; 24] {
        &self.id
    }
}

#[cfg(windows)]
impl AtomicRequest {
    /// Persist the default data stream and retain its writer after the file
    /// and parent directory barriers. The owner must keep the directory path
    /// bound to its selected directory through the call. Insufficient headroom
    /// on the original's account returns OutOfMemory before file creation.
    /// On error, retain this original and any partial output.
    pub fn persist_new_held(&self, path: &Path) -> io::Result<HeldOriginalFile> {
        self.persist_new_held_with(path, log_io::sync_file)
    }

    fn persist_new_held_with(
        &self,
        path: &Path,
        sync_file: impl FnOnce(&File) -> io::Result<()>,
    ) -> io::Result<HeldOriginalFile> {
        use std::os::windows::{ffi::OsStrExt, fs::OpenOptionsExt};

        // Named streams of the same file share FILE_ID_INFO.
        if !path
            .file_name()
            .is_some_and(|leaf| !leaf.encode_wide().any(|c| c == b':' as u16))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected a file name without a stream selector",
            ));
        }
        let reserve = |bytes| {
            self.data
                ._charge
                .budget
                .reserve_available(bytes)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))
        };
        let charge = reserve(size_of::<HeldOriginalFile>())?;
        let _scratch = reserve(WRITE_BUFFER_BYTES)?;
        let mut options = OpenOptions::new();
        // GENERIC_WRITE | FILE_READ_ATTRIBUTES, with FILE_SHARE_READ only.
        options.access_mode(0x4000_0080).share_mode(1);
        let writer = self.persist_file(path, options, sync_file)?;
        let id = file_id(&writer)?;
        Ok(HeldOriginalFile {
            _writer: writer,
            id,
            _charge: charge,
        })
    }
}

#[cfg(windows)]
fn file_id(file: &File) -> io::Result<[u8; 24]> {
    use std::{ffi::c_void, os::windows::io::AsRawHandle};
    #[link(name = "kernel32")]
    extern "system" {
        fn GetFileInformationByHandleEx(
            handle: *mut c_void,
            class: u32,
            buffer: *mut c_void,
            size: u32,
        ) -> i32;
    }
    #[repr(C)]
    #[derive(Default)]
    struct FileIdInfo {
        volume: u64,
        identifier: [u8; 16],
    }
    let mut info = FileIdInfo::default();
    // FileIdInfo (18) fills exactly the FILE_ID_INFO layout above.
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            18,
            (&raw mut info).cast(),
            size_of::<FileIdInfo>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let mut id = [0; 24];
    id[..8].copy_from_slice(&info.volume.to_le_bytes());
    id[8..].copy_from_slice(&info.identifier);
    Ok(id)
}

#[cfg(all(test, windows))]
mod tests;
