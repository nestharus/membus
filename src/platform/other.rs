//! Non-Windows host platform implementations.
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::Path,
};

pub const CHECKS_ALTERNATE_STREAMS: bool = false;

pub fn sync_file(file: &File) -> io::Result<()> {
    file.sync_all()
}

pub fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

pub fn open_exclusive_owner(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
}

pub fn open_catalogue_owner(path: &Path) -> io::Result<File> {
    let owner = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|e| io::Error::other(format!("catalogue writer ownership refused: {e}")))?;
    owner
        .try_lock()
        .map_err(|e| io::Error::other(format!("catalogue writer ownership refused: {e}")))?;
    Ok(owner)
}

pub fn open_catalogue_writer(path: &Path, create: bool) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .open(path)?;
    file.try_lock()
        .map_err(|e| io::Error::other(format!("catalogue file ownership refused: {e}")))?;
    Ok(file)
}

pub fn open_read_only_exclusive(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).open(path)
}

/// Lock-free positioned file reader using `FileExt::read_exact_at` (`pread`) on Unix.
#[derive(Debug)]
pub struct PositionedReader {
    #[cfg(unix)]
    file: File,
    #[cfg(not(unix))]
    file: std::sync::Mutex<File>,
}

impl PositionedReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            #[cfg(unix)]
            file,
            #[cfg(not(unix))]
            file: std::sync::Mutex::new(file),
        })
    }

    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(buf, offset)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(buf)
        }
    }
}

/// On non-Windows, remove copied owner.lock inside a fresh fenced recovery working copy.
pub fn prepare_quarantined_lock(
    target: &Path,
    sync: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let lock = target.join("owner.lock");
    match fs::remove_file(&lock) {
        Ok(()) => sync(target),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!("private stopped ownership preparation ({}): {e}", lock.display()),
        )),
    }
}

pub fn validate_regular_metadata(_meta: &fs::Metadata) -> io::Result<()> {
    Ok(())
}

pub fn canonical_path_workspace_max(path: &Path) -> usize {
    path.as_os_str().len() * 4 + 64
}

pub fn check_regular_file_streams(_file: &File) -> io::Result<()> {
    Ok(())
}

pub fn check_directory_streams(_path: &Path) -> io::Result<()> {
    Ok(())
}
