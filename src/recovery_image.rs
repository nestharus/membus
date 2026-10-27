//! Complete stopped-library byte images and permanently quarantined recovery.
//!
//! An image is evidence of a stopped durable root, never a grant to resume an
//! old writer. The retained image is never passed to mutable Store recovery.
use crate::{
    chunks::{sha256_file, Sha256},
    client::Ack,
    Config, Health2, Store, Value,
};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
};

pub(crate) const QUARANTINE: &str = ".membus-quarantine";
const MANIFEST: &str = "capture.manifest";
const COMPLETE: &str = "capture.complete";
const PAYLOAD: &str = "root";
const MAX_MANIFEST: u64 = 64 * 1024 * 1024;
const MAX_ENTRIES: usize = 1_000_000;
#[cfg(test)]
#[path = "recovery_image_correction_tests.rs"]
mod correction_tests;
#[cfg(test)]
fn barrier_test(path: &Path, directory: bool) -> io::Result<()> {
    correction_tests::barrier(path, directory)
}

/// The label identifies the source to its caller without embedding a host path.
#[derive(Clone, Debug)]
pub struct CaptureRequest {
    pub source_label: String,
    pub ack: Ack,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageEntry {
    /// A portable relative path; directory entries bind empty directories too.
    pub path: String,
    pub directory: bool,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureManifest {
    pub version: u16,
    pub source_label: String,
    pub source_epoch: u64,
    pub source_utc_ms: u64,
    pub source_mono_ms: u64,
    pub completed_utc_ms: u64,
    pub requested_ack: Ack,
    pub achieved_ack: Ack,
    /// Exact regular-file contents and directory population, not OS authority.
    pub complete: bool,
    pub config: Vec<u8>,
    pub root_sha256: String,
    pub entries: Vec<ImageEntry>,
    pub file_barriers: u64,
    pub directory_barriers: u64,
    pub readback: bool,
    completion: Option<CaptureCompletion>,
}

/// Successful post-barrier return evidence. Retain outside the image; never
/// derive it from capture.complete. This is caller custody, not authentication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureCompletion {
    manifest_sha256: String,
}
impl CaptureCompletion {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = b"MBCDONE1".to_vec();
        bytes.extend(self.manifest_sha256.as_bytes());
        bytes
    }
    /// Decode previously retained successful return evidence, not image bytes.
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != 72
            || &bytes[..8] != b"MBCDONE1"
            || !bytes[8..]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        {
            return Err(invalid("invalid capture completion custody"));
        }
        Ok(Self {
            manifest_sha256: String::from_utf8(bytes[8..].to_vec()).unwrap(),
        })
    }
    /// New-only custody writer. A caller must observe this result and retain the
    /// original bytes; a partial custody write is not a successful handoff.
    pub fn persist_new(&self, path: &Path) -> io::Result<()> {
        let parent = root_directory(
            path.parent()
                .ok_or_else(|| invalid("completion custody parent required"))?,
        )?;
        if !path.is_absolute() {
            return Err(invalid("absolute completion custody selector required"));
        }
        write_new(path, &self.to_bytes(), Ack::Flush)?;
        sync_output(&parent, true, Ack::Flush)
    }
}
impl CaptureManifest {
    /// Present only on a successful Flush capture or evidence-backed validation.
    pub fn completion(&self) -> Option<&CaptureCompletion> {
        self.completion.as_ref()
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn context(stage: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{stage} {}: {error}", path.display()))
}
fn hash(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    h.hex()
}
fn blob(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend((bytes.len() as u64).to_le_bytes());
    out.extend(bytes);
}
fn text(out: &mut Vec<u8>, value: &str) {
    blob(out, value.as_bytes());
}

/// Canonical configuration binding. It includes test seams rather than silently
/// dropping them; callback seams cannot be faithfully captured and are refused.
fn config_bytes(c: &Config) -> io::Result<Vec<u8>> {
    #[cfg(test)]
    if c.sync_test_gate.is_some()
        || c.sync_test_notice.is_some()
        || c.flush_covered_count.is_some()
        || c.recovery_dir_sync.is_some()
        || c.replay_report_dir_sync.is_some()
        || c.chunk_dir_sync.is_some()
        || c.state_dir_sync.is_some()
        || c.stop_after_replay_truncation
    {
        return Err(invalid(
            "capture configuration contains an unsupported callback seam",
        ));
    }
    let mut out = b"MBCFG1".to_vec();
    for value in [
        c.cache_bytes as u64,
        c.signal_history as u64,
        c.signal_bytes as u64,
        c.snapshot_bytes as u64,
        c.snapshot_max as u64,
        c.snapshot_ttl_ms,
        c.state_limit_bytes,
        c.quota as u64,
        c.index_cap as u64,
        c.capacity as u64,
        c.queue_bytes as u64,
        c.retry_ms,
        c.dedup_cap as u64,
        c.flush_ms,
        c.allow_mem as u64,
        c.write_delay_ms,
        c.flush_notice_delay_ms,
        c.fail_after_append as u64,
        c.fail_partial_append as u64,
        c.fail_sync as u64,
        c.fail_recovery_sync as u64,
        c.chunk_bytes,
        c.rotation_ms,
        c.tick_ms,
        c.retire_ms,
        c.catalogue_rewrite_bytes,
        c.summary_ms,
        c.maintenance_reserve_bytes,
        c.pinned_high_water_bytes,
        c.retire_stop_after as u64,
        c.import_inject_difference as u64,
        c.clock.is_some() as u64,
    ] {
        out.extend(value.to_le_bytes());
    }
    fn policy(out: &mut Vec<u8>, p: crate::CachePolicy) {
        out.extend(p.initial_ttl.to_le_bytes());
        out.extend(p.active_ttl.to_le_bytes());
        out.extend([p.refresh_on_read as u8, p.never_cache as u8]);
    }
    policy(&mut out, c.cache_policy);
    out.extend((c.cache_rules.len() as u64).to_le_bytes());
    for r in &c.cache_rules {
        out.push(r.namespace.is_some() as u8);
        text(&mut out, r.namespace.as_deref().unwrap_or(""));
        text(&mut out, &r.prefix);
        out.push(r.class.is_some() as u8);
        if let Some(class) = r.class {
            out.push(class.kind);
            out.extend(class.retention_ms.to_le_bytes());
        }
        text(&mut out, &r.label);
        policy(&mut out, r.policy);
    }
    if let Some(clock) = &c.clock {
        out.extend(clock.utc_ms().to_le_bytes());
    }
    Ok(out)
}

fn portable(path: &Path) -> io::Result<String> {
    let mut names = Vec::new();
    for component in path.components() {
        let Component::Normal(name) = component else {
            return Err(invalid("non-relative image path"));
        };
        let name = name
            .to_str()
            .ok_or_else(|| invalid("non-UTF8 image path"))?;
        if name.is_empty() || name.contains(['\\', '/', ':', '\0']) || name.ends_with(['.', ' ']) {
            return Err(invalid("nonportable image path"));
        }
        names.push(name);
    }
    if names.is_empty() {
        return Err(invalid("empty image path"));
    }
    Ok(names.join("/"))
}
fn relative(text: &str) -> io::Result<PathBuf> {
    let p = PathBuf::from(text);
    if portable(&p)? != text {
        return Err(invalid("noncanonical image path"));
    }
    Ok(p)
}
fn normal(meta: &fs::Metadata) -> io::Result<()> {
    if meta.file_type().is_symlink() || (!meta.is_file() && !meta.is_dir()) {
        return Err(invalid("image layout contains a link or special file"));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        // Reparse, encrypted, offline and device files cannot be preserved by
        // the regular-byte-image contract. Do not follow them.
        if meta.file_attributes() & (0x400 | 0x4000 | 0x1000 | 0x40) != 0 {
            return Err(invalid(
                "image layout contains unsupported Windows attributes",
            ));
        }
    }
    Ok(())
}
fn root_directory(root: &Path) -> io::Result<PathBuf> {
    if !root.is_absolute() {
        return Err(invalid("absolute image selector required"));
    }
    let mut walked = PathBuf::new();
    for c in root.components() {
        if matches!(c, Component::ParentDir | Component::CurDir) {
            return Err(invalid("canonical image selector required"));
        }
        walked.push(c);
        // A Windows verbatim drive prefix (for example the prefix emitted by
        // canonicalize) is not a filesystem path until RootDir is appended.
        // Querying the bare prefix returns ERROR_INVALID_FUNCTION.
        if matches!(c, Component::Prefix(_)) {
            continue;
        }
        normal(
            &fs::symlink_metadata(&walked)
                .map_err(|e| context("selector ancestor metadata", &walked, e))?,
        )?;
    }
    let meta = fs::symlink_metadata(root).map_err(|e| context("selector metadata", root, e))?;
    if !meta.is_dir() {
        return Err(invalid("image selector is not a directory"));
    }
    root.canonicalize()
        .map_err(|e| context("selector canonicalization", root, e))
}
fn new_directory(path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid("absolute new output selector required"));
    }
    let parent = root_directory(
        path.parent()
            .ok_or_else(|| invalid("output parent required"))?,
    )?;
    let name = path
        .file_name()
        .ok_or_else(|| invalid("output name required"))?;
    portable(Path::new(name))?;
    let path = parent.join(name);
    fs::create_dir(&path).map_err(|e| context("create selected output", &path, e))?;
    Ok(path)
}
fn read_handle(mut file: File) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[cfg(windows)]
fn regular_streams(file: &File) -> io::Result<()> {
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
    // FILE_STREAM_INFO is a linked sequence; refuse oversized inventories
    // rather than omitting alternate streams. Existing locked-owner handles
    // permit inspection without releasing directory ownership.
    let mut bytes = vec![0u8; 65536];
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            7,
            bytes.as_mut_ptr().cast(),
            bytes.len() as u32,
        )
    } == 0
    {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(
            error.kind(),
            format!("GetFileInformationByHandleEx(FileStreamInfo) refused: {error}"),
        ));
    }
    let mut offset = 0usize;
    loop {
        let header = bytes
            .get(offset..offset + 24)
            .ok_or_else(|| invalid("invalid filesystem stream inventory"))?;
        let next = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        if length % 2 != 0 {
            return Err(invalid("invalid filesystem stream name"));
        }
        let name = bytes
            .get(offset + 24..offset + 24 + length)
            .ok_or_else(|| invalid("oversized filesystem stream name"))?;
        let name = String::from_utf16(
            &name
                .chunks_exact(2)
                .map(|b| u16::from_le_bytes([b[0], b[1]]))
                .collect::<Vec<_>>(),
        )
        .map_err(|_| invalid("invalid filesystem stream encoding"))?;
        if name != "::$DATA" {
            return Err(invalid(
                "alternate filesystem streams are unsupported by the regular-byte image",
            ));
        }
        if next == 0 {
            break;
        }
        if next < 24 {
            return Err(invalid("invalid filesystem stream offset"));
        }
        offset = offset
            .checked_add(next)
            .ok_or_else(|| invalid("filesystem stream offset overflow"))?;
    }
    Ok(())
}
#[cfg(not(windows))]
fn regular_streams(_: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn directory_streams(path: &Path) -> io::Result<()> {
    use std::{ffi::c_void, os::windows::ffi::OsStrExt};
    #[repr(C)]
    struct Stream {
        size: i64,
        name: [u16; 296],
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn FindFirstStreamW(
            path: *const u16,
            level: u32,
            data: *mut Stream,
            flags: u32,
        ) -> *mut c_void;
        fn FindClose(handle: *mut c_void) -> i32;
    }
    let name: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut stream = Stream {
        size: 0,
        name: [0; 296],
    };
    let handle = unsafe { FindFirstStreamW(name.as_ptr(), 0, &mut stream, 0) };
    if handle as isize == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(38) {
            return Ok(());
        }
        return Err(context("FindFirstStreamW(directory) refused", path, error));
    }
    unsafe {
        FindClose(handle);
    }
    // Directories have no default unnamed data stream. Any discovered stream
    // is outside the supported byte-root image, and cannot be omitted.
    Err(invalid(
        "alternate directory streams are unsupported by the regular-byte image",
    ))
}
#[cfg(not(windows))]
fn directory_streams(_: &Path) -> io::Result<()> {
    Ok(())
}

struct OwnedFile {
    bytes: Vec<u8>,
    handle: File,
}

/// File contents held by directory ownership are read through their existing
/// handles. The source retains every lock for the entire capture.
fn owned_files(store: &Store) -> io::Result<BTreeMap<String, OwnedFile>> {
    let mut files = BTreeMap::new();
    files.insert(
        "owner.lock".into(),
        OwnedFile {
            bytes: read_handle(store._owner.try_clone()?)
                .map_err(|e| context("read owned lock", Path::new("owner.lock"), e))?,
            handle: store._owner.try_clone()?,
        },
    );
    let chunks = store.chunks.lock().unwrap();
    if let Some(cat) = &chunks.cat {
        let name = chunks
            .dir
            .file_name()
            .ok_or_else(|| invalid("chunk directory name"))?
            .to_str()
            .ok_or_else(|| invalid("chunk directory encoding"))?;
        let handle = cat.capture_owner()?;
        files.insert(
            format!("{name}.catalogue-owner"),
            OwnedFile {
                bytes: read_handle(handle.try_clone()?)
                    .map_err(|e| context("read catalogue ownership", Path::new(name), e))?,
                handle,
            },
        );
    }
    Ok(files)
}
fn inventory(root: &Path, owned: &BTreeMap<String, OwnedFile>) -> io::Result<Vec<ImageEntry>> {
    fn walk(
        root: &Path,
        dir: &Path,
        owned: &BTreeMap<String, OwnedFile>,
        out: &mut Vec<ImageEntry>,
    ) -> io::Result<()> {
        directory_streams(dir).map_err(|e| context("inventory directory streams", dir, e))?;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let meta = fs::symlink_metadata(&path)?;
            normal(&meta)?;
            if out.len() >= MAX_ENTRIES {
                return Err(invalid("capture entry limit exceeded"));
            }
            let name = portable(
                path.strip_prefix(root)
                    .map_err(|_| invalid("image path escaped root"))?,
            )?;
            let digest = if meta.is_dir() {
                String::new()
            } else if let Some(file) = owned.get(&name) {
                if meta.len() != file.bytes.len() as u64 {
                    return Err(invalid("owned lock length changed"));
                }
                regular_streams(&file.handle)
                    .map_err(|e| context("inventory owned-file streams", &path, e))?;
                hash(&file.bytes)
            } else {
                regular_streams(&File::open(&path)?)
                    .map_err(|e| context("inventory regular-file streams", &path, e))?;
                sha256_file(&path)?
            };
            out.push(ImageEntry {
                path: name,
                directory: meta.is_dir(),
                bytes: if meta.is_dir() { 0 } else { meta.len() },
                sha256: digest,
            });
            if meta.is_dir() {
                walk(root, &path, owned, out)?;
            }
        }
        Ok(())
    }
    let mut entries = Vec::new();
    walk(root, root, owned, &mut entries)?;
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    let mut folded = std::collections::BTreeSet::new();
    for e in &entries {
        if !folded.insert(e.path.to_lowercase()) {
            return Err(invalid("image has case-ambiguous paths"));
        }
    }
    Ok(entries)
}
fn entries_bytes(entries: &[ImageEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in entries {
        text(&mut out, &e.path);
        out.push(e.directory as u8);
        out.extend(e.bytes.to_le_bytes());
        text(&mut out, &e.sha256);
    }
    out
}
fn sync_output(path: &Path, directory: bool, requested: Ack) -> io::Result<()> {
    if requested == Ack::Flush {
        #[cfg(test)]
        barrier_test(path, directory)?;
        if directory {
            crate::log_io::sync_dir(path).map_err(|e| context("capture directory barrier", path, e))
        } else {
            OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(|e| context("capture barrier open", path, e))?
                .sync_all()
                .map_err(|e| context("capture file barrier", path, e))
        }
    } else {
        Ok(())
    }
}
fn write_new(path: &Path, bytes: &[u8], ack: Ack) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| context("capture create file", path, e))?;
    file.write_all(bytes)
        .map_err(|e| context("capture write file", path, e))?;
    if ack == Ack::Flush {
        #[cfg(test)]
        barrier_test(path, false)?;
        file.sync_all()
            .map_err(|e| context("capture file barrier", path, e))?;
    }
    Ok(())
}
fn copy_entries(
    source: &Path,
    target: &Path,
    entries: &[ImageEntry],
    owned: &BTreeMap<String, OwnedFile>,
    ack: Ack,
) -> io::Result<(u64, u64)> {
    let mut file_barriers = 0;
    let mut dirs = vec![target.to_owned()];
    for e in entries {
        let relative = relative(&e.path)?;
        let destination = target.join(&relative);
        if e.directory {
            fs::create_dir(&destination)
                .map_err(|e| context("capture copy directory", &destination, e))?;
            dirs.push(destination);
        } else {
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&destination)
                .map_err(|e| context("capture copy create", &destination, e))?;
            if let Some(file) = owned.get(&e.path) {
                output
                    .write_all(&file.bytes)
                    .map_err(|e| context("capture copy owned bytes", &destination, e))?;
            } else {
                let input_path = source.join(relative);
                let mut input = File::open(&input_path)
                    .map_err(|e| context("capture copy input open", &input_path, e))?;
                io::copy(&mut input, &mut output)
                    .map_err(|e| context("capture byte copy", &destination, e))?;
            }
            if ack == Ack::Flush {
                output
                    .sync_all()
                    .map_err(|e| context("capture copied-file barrier", &destination, e))?;
                file_barriers += 1;
            }
        }
    }
    dirs.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
    if ack == Ack::Flush {
        for d in &dirs {
            sync_output(d, true, ack)?;
        }
    }
    Ok((
        file_barriers,
        if ack == Ack::Flush {
            dirs.len() as u64
        } else {
            0
        },
    ))
}

impl CaptureManifest {
    fn encode(&self) -> Vec<u8> {
        let mut out = b"MBCAP1".to_vec();
        out.extend(self.version.to_le_bytes());
        text(&mut out, &self.source_label);
        for n in [
            self.source_epoch,
            self.source_utc_ms,
            self.source_mono_ms,
            self.completed_utc_ms,
        ] {
            out.extend(n.to_le_bytes());
        }
        out.extend([
            self.requested_ack as u8,
            self.achieved_ack as u8,
            self.complete as u8,
            self.readback as u8,
        ]);
        blob(&mut out, &self.config);
        text(&mut out, &self.root_sha256);
        out.extend(self.file_barriers.to_le_bytes());
        out.extend(self.directory_barriers.to_le_bytes());
        out.extend((self.entries.len() as u64).to_le_bytes());
        out.extend(entries_bytes(&self.entries));
        out
    }
    fn decode(bytes: &[u8]) -> io::Result<Self> {
        struct Reader<'a> {
            bytes: &'a [u8],
            pos: usize,
        }
        impl<'a> Reader<'a> {
            fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
                let end = self
                    .pos
                    .checked_add(n)
                    .ok_or_else(|| invalid("manifest offset"))?;
                let b = self
                    .bytes
                    .get(self.pos..end)
                    .ok_or_else(|| invalid("short manifest"))?;
                self.pos = end;
                Ok(b)
            }
            fn n(&mut self) -> io::Result<u64> {
                Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
            }
            fn blob(&mut self) -> io::Result<Vec<u8>> {
                let n = usize::try_from(self.n()?).map_err(|_| invalid("manifest size"))?;
                Ok(self.take(n)?.to_vec())
            }
            fn text(&mut self) -> io::Result<String> {
                String::from_utf8(self.blob()?).map_err(|_| invalid("manifest UTF8"))
            }
            fn flag(&mut self) -> io::Result<bool> {
                match self.take(1)?[0] {
                    0 => Ok(false),
                    1 => Ok(true),
                    _ => Err(invalid("manifest flag")),
                }
            }
            fn ack(&mut self) -> io::Result<Ack> {
                match self.take(1)?[0] {
                    1 => Ok(Ack::Os),
                    2 => Ok(Ack::Flush),
                    _ => Err(invalid("manifest Ack")),
                }
            }
        }
        let mut r = Reader { bytes, pos: 0 };
        if r.take(6)? != b"MBCAP1" {
            return Err(invalid("manifest magic"));
        }
        let version = u16::from_le_bytes(r.take(2)?.try_into().unwrap());
        if version != 1 {
            return Err(invalid("unsupported capture version"));
        }
        let source_label = r.text()?;
        let source_epoch = r.n()?;
        let source_utc_ms = r.n()?;
        let source_mono_ms = r.n()?;
        let completed_utc_ms = r.n()?;
        let requested_ack = r.ack()?;
        let achieved_ack = r.ack()?;
        let complete = r.flag()?;
        let readback = r.flag()?;
        let config = r.blob()?;
        let root_sha256 = r.text()?;
        let file_barriers = r.n()?;
        let directory_barriers = r.n()?;
        let count = usize::try_from(r.n()?).map_err(|_| invalid("manifest count"))?;
        if count > MAX_ENTRIES {
            return Err(invalid("manifest entry count limit"));
        }
        let mut entries = Vec::new();
        for _ in 0..count {
            let path = r.text()?;
            relative(&path)?;
            entries.push(ImageEntry {
                path,
                directory: r.flag()?,
                bytes: r.n()?,
                sha256: r.text()?,
            });
        }
        if r.pos != bytes.len() {
            return Err(invalid("manifest trailing bytes"));
        }
        Ok(Self {
            version,
            source_label,
            source_epoch,
            source_utc_ms,
            source_mono_ms,
            completed_utc_ms,
            requested_ack,
            achieved_ack,
            complete,
            config,
            root_sha256,
            entries,
            file_barriers,
            directory_barriers,
            readback,
            completion: None,
        })
    }
}

impl Store {
    /// Consume the actual library owner. A resident heartbeat retains an Arc and
    /// therefore cannot provide this owner until it is joined. Weak timer owners
    /// require the separate explicit running check below.
    pub fn capture_stopped(
        mut self,
        destination: &Path,
        request: CaptureRequest,
    ) -> io::Result<CaptureManifest> {
        if crate::timers::capture_owner_running(&self.timers) {
            return Err(invalid("running timer owner must be joined before capture"));
        }
        if request.source_label.is_empty()
            || request.source_label.len() > 4096
            || request.source_label.contains(['\0', '\n', '\r'])
        {
            return Err(invalid("capture source label required"));
        }
        let config = config_bytes(&self.config)?;
        let source = root_directory(&self.path)?;
        let parent = root_directory(
            destination
                .parent()
                .ok_or_else(|| invalid("image parent required"))?,
        )?;
        if parent.starts_with(&source) {
            return Err(invalid("capture destination is inside source"));
        }
        // Stop every owner and observe every join result. Settlement continues
        // even if one join fails; a panic can never publish a complete image.
        let mut panicked = false;
        if let Some((stop, worker)) = self.scan_reaper.take() {
            let _ = stop.send(());
            panicked |= worker.join().is_err();
        }
        if let Some((stop, worker)) = self.ticker.take() {
            let _ = stop.send(());
            panicked |= worker.join().is_err();
        }
        for owner in &mut self.appenders {
            owner.sender.take();
        }
        for owner in &mut self.appenders {
            if let Some(worker) = owner.handle.take() {
                panicked |= worker.join().is_err();
            }
        }
        if let Some(owner) = self.flush_owner.take() {
            panicked |= owner.join().is_err();
        }
        let signal_result = self.signals.stop_for_capture();
        if panicked {
            return Err(invalid("capture owner failed while settling"));
        }
        signal_result?;
        let health = self.health2();
        if health.storage_error
            || health.read_only
            || health.expiry_degraded
            || health.catalogue_rewrite_error.is_some()
        {
            return Err(invalid("capture source health is incomplete"));
        }
        let owned = owned_files(&self)?;
        let output = new_directory(destination)?;
        // The existence of this file documents incomplete outputs on every
        // later refusal; only capture.complete grants complete-image status.
        write_new(
            &output.join("capture.started"),
            b"MBCAP1 incomplete unless capture.complete validates\n",
            request.ack,
        )?;
        let entries = inventory(&source, &owned)?;
        let source_utc_ms = self.clock.utc_ms();
        let source_mono_ms = self.clock.mono_ms();
        let target = output.join(PAYLOAD);
        fs::create_dir(&target)?;
        let (file_barriers, directory_barriers) =
            copy_entries(&source, &target, &entries, &owned, request.ack)?;
        if inventory(&source, &owned)? != entries
            || inventory(&target, &BTreeMap::new())? != entries
            || config_bytes(&self.config)? != config
        {
            return Err(invalid(
                "capture source/config changed or destination readback differs",
            ));
        }
        let mut manifest = CaptureManifest {
            version: 1,
            source_label: request.source_label,
            source_epoch: self.epoch,
            source_utc_ms,
            source_mono_ms,
            completed_utc_ms: crate::now_ms(),
            requested_ack: request.ack,
            achieved_ack: request.ack,
            complete: true,
            config,
            root_sha256: hash(&entries_bytes(&entries)),
            entries,
            file_barriers,
            directory_barriers: directory_barriers + if request.ack == Ack::Flush { 3 } else { 0 },
            readback: true,
            completion: None,
        };
        let encoded = manifest.encode();
        if encoded.len() as u64 > MAX_MANIFEST {
            return Err(invalid("capture manifest limit"));
        }
        write_new(&output.join(MANIFEST), &encoded, request.ack)?;
        sync_output(&output, true, request.ack)?;
        sync_output(output.parent().unwrap(), true, request.ack)?;
        if fs::read(output.join(MANIFEST))? != encoded {
            return Err(invalid("manifest readback differs"));
        }
        // Completion is last, binds the entire manifest, and is separately
        // covered by the image-directory barrier and readback.
        write_new(
            &output.join(COMPLETE),
            hash(&encoded).as_bytes(),
            request.ack,
        )?;
        sync_output(&output, true, request.ack)?;
        let validated = validate_contents(&output)?;
        if validated != manifest {
            return Err(invalid("completed image readback differs"));
        }
        // This evidence is issued only AFTER successful completion-file and
        // final directory sync, plus complete readback. It is not stored inside
        // the image: no readable in-image marker can prove its own failed sync.
        if request.ack == Ack::Flush {
            manifest.completion = Some(CaptureCompletion {
                manifest_sha256: hash(&encoded),
            });
        }
        Ok(manifest)
    }
}

/// Validate every retained file and directory before any mutable recovery.
pub fn validate_image(image: &Path) -> io::Result<CaptureManifest> {
    validate_selected(image, None)
}
/// Validate a Flush image using original successful post-barrier return evidence.
/// This performs no new image barriers and never backdates later persistence.
pub fn validate_image_with_completion(
    image: &Path,
    completion: &CaptureCompletion,
) -> io::Result<CaptureManifest> {
    validate_selected(image, Some(completion))
}
fn validate_selected(
    image: &Path,
    completion: Option<&CaptureCompletion>,
) -> io::Result<CaptureManifest> {
    let mut manifest = validate_contents(image)?;
    if manifest.requested_ack == Ack::Flush {
        let evidence = completion.ok_or_else(|| invalid("Flush capture settlement unknown: original post-barrier completion evidence required"))?;
        if evidence.manifest_sha256 != hash(&manifest.encode()) {
            return Err(invalid(
                "capture completion evidence does not bind this manifest",
            ));
        }
        manifest.completion = Some(evidence.clone());
    }
    Ok(manifest)
}
fn validate_contents(image: &Path) -> io::Result<CaptureManifest> {
    let image = root_directory(image)?;
    directory_streams(&image)?;
    let names: std::collections::BTreeSet<_> = fs::read_dir(&image)?
        .map(|e| e.map(|x| x.file_name()))
        .collect::<io::Result<_>>()?;
    let expected: std::collections::BTreeSet<_> = ["capture.started", MANIFEST, COMPLETE, PAYLOAD]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
    if names != expected {
        return Err(invalid("image envelope population differs"));
    }
    for name in ["capture.started", MANIFEST, COMPLETE] {
        let path = image.join(name);
        let meta = fs::symlink_metadata(&path)?;
        normal(&meta)?;
        if !meta.is_file() {
            return Err(invalid("image envelope file type"));
        }
        regular_streams(&File::open(path)?)?;
    }
    if fs::read(image.join("capture.started"))?
        != b"MBCAP1 incomplete unless capture.complete validates\n"
    {
        return Err(invalid("capture-start marker differs"));
    }
    if fs::metadata(image.join(MANIFEST))?.len() > MAX_MANIFEST
        || fs::metadata(image.join(COMPLETE))?.len() != 64
    {
        return Err(invalid("manifest/completion size"));
    }
    let encoded = fs::read(image.join(MANIFEST))?;
    if fs::read(image.join(COMPLETE))? != hash(&encoded).as_bytes() {
        return Err(invalid("completion does not bind manifest"));
    }
    let manifest = CaptureManifest::decode(&encoded)?;
    if !manifest.complete || !manifest.readback || manifest.requested_ack != manifest.achieved_ack {
        return Err(invalid("capture incomplete or requested Ack not achieved"));
    }
    let payload = root_directory(&image.join(PAYLOAD))?;
    let actual = inventory(&payload, &BTreeMap::new())?;
    if actual != manifest.entries || hash(&entries_bytes(&actual)) != manifest.root_sha256 {
        return Err(invalid(
            "image contents/population differ from complete manifest",
        ));
    }
    let files = actual.iter().filter(|e| !e.directory).count() as u64;
    let directories = actual.iter().filter(|e| e.directory).count() as u64 + 1;
    if manifest.achieved_ack == Ack::Flush
        && (manifest.file_barriers != files || manifest.directory_barriers != directories + 3)
    {
        return Err(invalid("incomplete capture barrier receipt"));
    }
    if manifest.achieved_ack == Ack::Os
        && (manifest.file_barriers != 0 || manifest.directory_barriers != 0)
    {
        return Err(invalid("invalid OS capture barrier receipt"));
    }
    Ok(manifest)
}

/// An isolated inspection handle. It cannot expose Store, mutation dispatch,
/// timers, subscription delivery or an authority-unfencing method.
pub struct QuarantinedRecovery {
    store: Store,
    pub manifest: CaptureManifest,
    pub working_root: PathBuf,
}
impl QuarantinedRecovery {
    pub fn try_get(&self, namespace: &str, key: &str) -> io::Result<Option<Value>> {
        self.store.try_get(namespace, key)
    }
    pub fn health2(&self) -> Health2 {
        self.store.health2()
    }
}

pub fn recover_quarantined(
    image: &Path,
    working: &Path,
    config: Config,
) -> io::Result<QuarantinedRecovery> {
    recover_selected(image, working, config, None)
}
pub fn recover_quarantined_with_completion(
    image: &Path,
    working: &Path,
    config: Config,
    completion: &CaptureCompletion,
) -> io::Result<QuarantinedRecovery> {
    recover_selected(image, working, config, Some(completion))
}
fn recover_selected(
    image: &Path,
    working: &Path,
    mut config: Config,
    completion: Option<&CaptureCompletion>,
) -> io::Result<QuarantinedRecovery> {
    let manifest = validate_selected(image, completion)?;
    config.signal_bytes = config.signal_bytes.min(config.capacity / 8);
    if config_bytes(&config)? != manifest.config {
        return Err(invalid("recovery configuration differs from capture"));
    }
    let source = root_directory(image)?;
    let parent = root_directory(
        working
            .parent()
            .ok_or_else(|| invalid("working parent required"))?,
    )?;
    if parent.starts_with(&source) {
        return Err(invalid("working root is inside retained image"));
    }
    let target = new_directory(working)?;
    // The fence exists durably before the first copied state file and survives
    // successful recovery, refusal and process restart. No public open bypass.
    write_new(
        &target.join(QUARANTINE),
        b"Membus isolated recovery; no serving or outward-effect authority\n",
        Ack::Flush,
    )?;
    sync_output(&target, true, Ack::Flush)?;
    sync_output(target.parent().unwrap(), true, Ack::Flush)?;
    if manifest.entries.iter().any(|e| e.path == QUARANTINE) {
        return Err(invalid("image already contains a quarantine marker"));
    }
    copy_entries(
        &source.join(PAYLOAD),
        &target,
        &manifest.entries,
        &BTreeMap::new(),
        Ack::Flush,
    )?;
    let mut copied = inventory(&target, &BTreeMap::new())?;
    copied.retain(|entry| entry.path != QUARANTINE);
    if copied != manifest.entries {
        return Err(invalid(
            "private working-copy readback differs before recovery",
        ));
    }
    if validate_selected(&source, completion)? != manifest {
        return Err(invalid("retained image changed during copy"));
    }
    // Validate ALL historical embedded effects before ordinary startup. Inventory
    // already refused links; this fresh private copy has no outward path aliases.
    let catalogue = target.join("chunks/catalogue.log");
    if catalogue.try_exists()? {
        for record in crate::chunks::read_catalogue(&catalogue)?.0 {
            if let crate::chunks::Rec::Import(import) = record.rec {
                crate::replay::validate_import_paths(&import)?;
            }
        }
    }
    #[cfg(not(windows))]
    {
        // new_directory exclusively created this fenced private root; its copied
        // ownership bytes cannot name a live owner. Never apply to the image or
        // an existing/reused root. Keep the original lock bytes in the image.
        let lock = target.join("owner.lock");
        match fs::remove_file(&lock) {
            Ok(()) => sync_output(&target, true, Ack::Flush)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(context("private stopped ownership preparation", &lock, e)),
        }
    }
    let mut store = Store::open_quarantined(&target, config)?;
    let health = store.health2();
    if health.read_only
        || health.storage_error
        || health.expiry_degraded
        || health.catalogue_rewrite_error.is_some()
    {
        return Err(invalid(
            "recovered working copy Health2 refused: incomplete or degraded history",
        ));
    }
    store.stop_workers();
    store.signals.stop_for_capture()?;
    store.set_read_only(true);
    if validate_selected(&source, completion)? != manifest {
        return Err(invalid("retained image changed during recovery"));
    }
    Ok(QuarantinedRecovery {
        store,
        manifest,
        working_root: target,
    })
}
