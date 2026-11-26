//! Complete stopped-library byte images and permanently quarantined recovery.
//!
//! An image is evidence of a stopped durable root, never a grant to resume an
//! old writer. The retained image is never passed to mutable Store recovery.
use crate::{chunks::Sha256, client::Ack, Config, Health2, Store, Value};
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
mod funding;
use funding::Context;
pub use funding::{
    validate_image_funded, FundedCompletion, FundedImage, ImageBudget, ImageFailure, ImageLimits,
    ImageOutput, ImageProgress, ImageResource, ImageShortfall, ImageStage,
};
#[cfg(test)]
#[path = "../../unit_tests/store/recovery_image_correction_tests.rs"]
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
fn blob(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend((bytes.len() as u64).to_le_bytes());
    out.extend(bytes);
}
fn text(out: &mut Vec<u8>, value: &str) {
    blob(out, value.as_bytes());
}

/// Canonical configuration binding. It includes test seams rather than silently
/// dropping them; callback seams cannot be faithfully captured and are refused.
fn config_values(c: &Config) -> impl ExactSizeIterator<Item = u64> {
    [
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
    ]
    .into_iter()
}
fn config_bytes(c: &Config) -> io::Result<Vec<u8>> {
    config_bytes_in(c, &Context::legacy())
}
fn config_bytes_in(c: &Config, ctx: &Context) -> io::Result<Vec<u8>> {
    #[cfg(test)]
    if c.sync_test_gate.is_some()
        || c.sync_test_notice.is_some()
        || c.flush_covered_count.is_some()
        || c.recovery_dir_sync.is_some()
        || c.replay_report_dir_sync.is_some()
        || c.chunk_dir_sync.is_some()
        || c.state_dir_sync.is_some()
        || c.retirement_started.is_some()
        || c.stop_after_replay_truncation
    {
        return Err(invalid(
            "capture configuration contains an unsupported callback seam",
        ));
    }
    let values = config_values(c);
    let mut size = 6usize + values.len() * 8 + 18 + 8 + if c.clock.is_some() { 8 } else { 0 };
    for r in &c.cache_rules {
        size = size
            .checked_add(
                1 + 8
                    + r.namespace.as_ref().map_or(0, String::len)
                    + 8
                    + r.prefix.len()
                    + 1
                    + if r.class.is_some() { 9 } else { 0 }
                    + 8
                    + r.label.len()
                    + 18,
            )
            .ok_or_else(|| invalid("capture configuration size overflow"))?;
    }
    ctx.allocate(size)?;
    ctx.work(size as u64)?;
    let mut out = Vec::with_capacity(size);
    config_parts(c, |part| {
        out.extend_from_slice(part);
        Ok(())
    })?;
    Ok(out)
}

fn config_parts(c: &Config, mut emit: impl FnMut(&[u8]) -> io::Result<()>) -> io::Result<()> {
    fn blob(emit: &mut impl FnMut(&[u8]) -> io::Result<()>, bytes: &[u8]) -> io::Result<()> {
        emit(&(bytes.len() as u64).to_le_bytes())?;
        emit(bytes)
    }
    fn policy(
        emit: &mut impl FnMut(&[u8]) -> io::Result<()>,
        p: crate::CachePolicy,
    ) -> io::Result<()> {
        emit(&p.initial_ttl.to_le_bytes())?;
        emit(&p.active_ttl.to_le_bytes())?;
        emit(&[p.refresh_on_read as u8, p.never_cache as u8])
    }
    emit(b"MBCFG1")?;
    for value in config_values(c) {
        emit(&value.to_le_bytes())?;
    }
    policy(&mut emit, c.cache_policy)?;
    emit(&(c.cache_rules.len() as u64).to_le_bytes())?;
    for r in &c.cache_rules {
        emit(&[r.namespace.is_some() as u8])?;
        blob(&mut emit, r.namespace.as_deref().unwrap_or("").as_bytes())?;
        blob(&mut emit, r.prefix.as_bytes())?;
        emit(&[r.class.is_some() as u8])?;
        if let Some(class) = r.class {
            emit(&[class.kind])?;
            emit(&class.retention_ms.to_le_bytes())?;
        }
        blob(&mut emit, r.label.as_bytes())?;
        policy(&mut emit, r.policy)?;
    }
    if let Some(clock) = &c.clock {
        emit(&clock.utc_ms().to_le_bytes())?;
    }
    Ok(())
}
fn config_matches(c: &Config, bytes: &[u8], ctx: &Context) -> io::Result<bool> {
    let _temporary = ctx.temporary(1024)?;
    ctx.work(bytes.len() as u64)?;
    let mut offset = 0usize;
    let mut same = true;
    config_parts(c, |part| {
        let end = offset
            .checked_add(part.len())
            .ok_or_else(|| invalid("configuration comparison overflow"))?;
        same &= bytes.get(offset..end) == Some(part);
        offset = end;
        Ok(())
    })?;
    Ok(same && offset == bytes.len())
}

fn portable_in(path: &Path, ctx: &Context) -> io::Result<String> {
    let mut bytes = 0usize;
    let mut count = 0usize;
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
        ctx.work(1)?;
        bytes = bytes
            .checked_add(name.len() + usize::from(count != 0))
            .ok_or_else(|| invalid("image path length overflow"))?;
        count += 1;
    }
    if count == 0 {
        return Err(invalid("empty image path"));
    }
    ctx.allocate(bytes)?;
    let mut name = String::with_capacity(bytes);
    for component in path.components() {
        let Component::Normal(part) = component else {
            unreachable!()
        };
        if !name.is_empty() {
            name.push('/');
        }
        name.push_str(part.to_str().unwrap());
    }
    Ok(name)
}
fn relative_in(text: &str, ctx: &Context) -> io::Result<PathBuf> {
    ctx.allocate(
        text.len()
            .checked_mul(2)
            .ok_or_else(|| invalid("image path size overflow"))?
            + 32,
    )?;
    let p = PathBuf::from(text);
    if portable_in(&p, ctx)? != text {
        return Err(invalid("noncanonical image path"));
    }
    Ok(p)
}
fn normal(meta: &fs::Metadata) -> io::Result<()> {
    if meta.file_type().is_symlink() || (!meta.is_file() && !meta.is_dir()) {
        return Err(invalid("image layout contains a link or special file"));
    }
    crate::platform::validate_regular_metadata(meta)?;
    Ok(())
}
fn root_directory(root: &Path) -> io::Result<PathBuf> {
    root_directory_in(root, &Context::legacy())
}
fn root_directory_in(root: &Path, ctx: &Context) -> io::Result<PathBuf> {
    if !root.is_absolute() {
        return Err(invalid("absolute image selector required"));
    }
    ctx.allocate(
        root.as_os_str()
            .len()
            .checked_mul(4)
            .ok_or_else(|| invalid("selector size overflow"))?
            + 64,
    )?;
    let mut walked = PathBuf::with_capacity(root.as_os_str().len() * 2 + 32);
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
        ctx.work(1)?;
        normal(
            &fs::symlink_metadata(&walked)
                .map_err(|e| context("selector ancestor metadata", &walked, e))?,
        )?;
    }
    ctx.work(2)?;
    let meta = fs::symlink_metadata(root).map_err(|e| context("selector metadata", root, e))?;
    if !meta.is_dir() {
        return Err(invalid("image selector is not a directory"));
    }
    let maximum = crate::platform::canonical_path_workspace_max(root);
    ctx.allocate(maximum)?;
    let canonical = root
        .canonicalize()
        .map_err(|e| context("selector canonicalization", root, e))?;
    if canonical.capacity() > maximum {
        return Err(invalid("canonical path exceeds admitted OS representation"));
    }
    ctx.release(maximum - canonical.capacity());
    Ok(canonical)
}
fn new_directory(path: &Path) -> io::Result<PathBuf> {
    new_directory_in(path, &Context::legacy())
}
fn new_directory_in(path: &Path, ctx: &Context) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid("absolute new output selector required"));
    }
    let parent = root_directory_in(
        path.parent()
            .ok_or_else(|| invalid("output parent required"))?,
        ctx,
    )?;
    let name = path
        .file_name()
        .ok_or_else(|| invalid("output name required"))?;
    portable_in(Path::new(name), ctx)?;
    ctx.allocate((parent.as_os_str().len() + name.len() + 2) * 4 + 64)?;
    let path = parent.join(name);
    ctx.work(1)?;
    fs::create_dir(&path).map_err(|e| context("create selected output", &path, e))?;
    ctx.output(ImageOutput::Incomplete);
    Ok(path)
}

fn regular_streams_in(file: &File, ctx: &Context) -> io::Result<()> {
    if !crate::platform::CHECKS_ALTERNATE_STREAMS {
        return Ok(());
    }
    let _workspace = ctx.temporary(65536 + 512)?;
    ctx.work(1)?;
    crate::platform::check_regular_file_streams(file)
}

fn directory_streams_in(path: &Path, ctx: &Context) -> io::Result<()> {
    if !crate::platform::CHECKS_ALTERNATE_STREAMS {
        return Ok(());
    }
    let _workspace = ctx.temporary(path.as_os_str().len() * 4 + 1024)?;
    ctx.work(1)?;
    crate::platform::check_directory_streams(path)
}

fn reserve_vec<T>(values: &mut Vec<T>, ctx: &Context) -> io::Result<()> {
    if values.len() < values.capacity() {
        return Ok(());
    }
    let old = values.capacity();
    let next = old.saturating_mul(2).max(4);
    let bytes = next
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| invalid("image vector size overflow"))?;
    ctx.allocate(bytes)?;
    values
        .try_reserve_exact(next - values.len())
        .map_err(|_| io::Error::other("image backing allocation failed"))?;
    ctx.release(old * std::mem::size_of::<T>());
    Ok(())
}
fn hex_in(h: Sha256, ctx: &Context) -> io::Result<String> {
    ctx.allocate(64)?;
    let _temporary = ctx.temporary(512)?;
    let mut text = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for b in h.finish() {
        text.push(HEX[(b >> 4) as usize] as char);
        text.push(HEX[(b & 15) as usize] as char);
    }
    Ok(text)
}
fn hash_in(bytes: &[u8], ctx: &Context) -> io::Result<String> {
    let _temporary = ctx.temporary(512)?;
    ctx.work(bytes.len() as u64)?;
    let mut h = Sha256::new();
    h.update(bytes);
    hex_in(h, ctx)
}
fn hash_file_in(mut file: File, size: u64, ctx: &Context) -> io::Result<String> {
    let _temporary = ctx.temporary(8192 + 1024)?;
    ctx.io(size
        .checked_add(1)
        .ok_or_else(|| invalid("image input size overflow"))?)?;
    ctx.work(size)?;
    file.seek(SeekFrom::Start(0))?;
    let mut h = Sha256::new();
    let mut buf = [0u8; 8192];
    let mut remaining = size;
    while remaining > 0 {
        let n = remaining.min(buf.len() as u64) as usize;
        file.read_exact(&mut buf[..n])?;
        h.update(&buf[..n]);
        remaining -= n as u64;
    }
    if file.read(&mut buf[..1])? != 0 {
        return Err(invalid("image input grew beyond admitted extent"));
    }
    hex_in(h, ctx)
}
struct OwnedFile {
    handle: File,
}
fn owned_files_in(store: &Store, ctx: &Context) -> io::Result<BTreeMap<String, OwnedFile>> {
    // At most the two existing cooperative ownership handles. Their payloads
    // stream through those handles; no lock-sized read_to_end buffer is made.
    ctx.allocate(1024)?;
    ctx.work(3)?;
    let mut files = BTreeMap::new();
    files.insert(
        "owner.lock".into(),
        OwnedFile {
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
        ctx.allocate(name.len() + 32)?;
        files.insert(
            format!("{name}.catalogue-owner"),
            OwnedFile {
                handle: cat.capture_owner()?,
            },
        );
    }
    Ok(files)
}
fn inventory(root: &Path, owned: &BTreeMap<String, OwnedFile>) -> io::Result<Vec<ImageEntry>> {
    inventory_in(root, owned, &Context::legacy())
}
fn inventory_in(
    root: &Path,
    owned: &BTreeMap<String, OwnedFile>,
    ctx: &Context,
) -> io::Result<Vec<ImageEntry>> {
    let mut entries = Vec::new();
    let mut dirs = Vec::new();
    reserve_vec(&mut dirs, ctx)?;
    ctx.allocate(root.as_os_str().len())?;
    dirs.push(root.to_owned());
    // Iterative traversal: directory depth consumes explicitly admitted backing,
    // not an unbounded recursive call stack.
    while let Some(dir) = dirs.pop() {
        directory_streams_in(&dir, ctx)?;
        let _reader_workspace = ctx.temporary(dir.as_os_str().len() * 4 + 4096)?;
        ctx.work(1)?;
        let mut reader = fs::read_dir(&dir)?;
        loop {
            // Standard directory-entry/name/path extraction uses bounded OS
            // component workspace (Windows WIN32_FIND_DATA has 260 UTF-16 units).
            let _entry = ctx.temporary(dir.as_os_str().len() * 8 + 4 * 260 + 1024)?;
            ctx.work(1)?;
            let Some(entry) = reader.next() else { break };
            let entry = entry?;
            let path = entry.path();
            ctx.work(1)?;
            let meta = fs::symlink_metadata(&path)?;
            normal(&meta)?;
            if !ctx.funded() && entries.len() >= MAX_ENTRIES {
                return Err(invalid("capture entry limit exceeded"));
            }
            let name = portable_in(
                path.strip_prefix(root)
                    .map_err(|_| invalid("image path escaped root"))?,
                ctx,
            )?;
            let digest = if meta.is_dir() {
                String::new()
            } else {
                ctx.work(1)?;
                let file = if let Some(owned) = owned.get(&name) {
                    owned.handle.try_clone()?
                } else {
                    File::open(&path)?
                };
                regular_streams_in(&file, ctx)?;
                hash_file_in(file, meta.len(), ctx)?
            };
            reserve_vec(&mut entries, ctx)?;
            entries.push(ImageEntry {
                path: name,
                directory: meta.is_dir(),
                bytes: if meta.is_dir() { 0 } else { meta.len() },
                sha256: digest,
            });
            if meta.is_dir() {
                reserve_vec(&mut dirs, ctx)?;
                ctx.allocate(path.capacity())?;
                dirs.push(path);
            }
        }
        ctx.release(dir.capacity());
    }
    ctx.release(dirs.capacity() * std::mem::size_of::<PathBuf>());
    // Unstable sort needs no auxiliary population allocation. The case-folded
    // set is a separate, pre-admitted temporary; it never replaces history.
    ctx.work(entries.len() as u64)?;
    entries.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    let folded_bytes = entries.iter().try_fold(0usize, |n, e| {
        let lower = e
            .path
            .chars()
            .flat_map(char::to_lowercase)
            .map(char::len_utf8)
            .sum::<usize>();
        n.checked_add(512 + lower * 2 + e.path.len())
            .ok_or_else(|| invalid("image path-set size overflow"))
    })?;
    let _folded = ctx.temporary(folded_bytes)?;
    let mut folded = std::collections::BTreeSet::new();
    for e in &entries {
        ctx.work(e.path.len() as u64 + 1)?;
        if !folded.insert(e.path.to_lowercase()) {
            return Err(invalid("image has case-ambiguous paths"));
        }
    }
    Ok(entries)
}
fn entries_hash_in(entries: &[ImageEntry], ctx: &Context) -> io::Result<String> {
    let _temporary = ctx.temporary(1024)?;
    let mut h = Sha256::new();
    for e in entries {
        ctx.work((25 + e.path.len() + e.sha256.len()) as u64)?;
        h.update(&(e.path.len() as u64).to_le_bytes());
        h.update(e.path.as_bytes());
        h.update(&[e.directory as u8]);
        h.update(&e.bytes.to_le_bytes());
        h.update(&(e.sha256.len() as u64).to_le_bytes());
        h.update(e.sha256.as_bytes());
    }
    hex_in(h, ctx)
}

fn sync_output(path: &Path, directory: bool, requested: Ack) -> io::Result<()> {
    sync_output_in(path, directory, requested, &Context::legacy())
}
fn sync_output_in(path: &Path, directory: bool, requested: Ack, ctx: &Context) -> io::Result<()> {
    if requested == Ack::Flush {
        let _temporary = ctx.temporary(path.as_os_str().len() * 4 + 1024)?;
        ctx.work(1)?;
        #[cfg(test)]
        barrier_test(path, directory)?;
        if directory {
            crate::log_io::sync_dir(path).map_err(|e| context("capture directory barrier", path, e))
        } else {
            OpenOptions::new()
                .write(true)
                .open(path)
                .and_then(|f| f.sync_all())
                .map_err(|e| context("capture file barrier", path, e))
        }
    } else {
        Ok(())
    }
}
fn read_file_in(path: &Path, ctx: &Context) -> io::Result<Vec<u8>> {
    ctx.work(2)?;
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let n = usize::try_from(size).map_err(|_| invalid("image file cannot be represented"))?;
    ctx.allocate(n)?;
    ctx.io(size
        .checked_add(1)
        .ok_or_else(|| invalid("image input size overflow"))?)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(n)
        .map_err(|_| io::Error::other("image read backing allocation failed"))?;
    bytes.resize(n, 0);
    file.read_exact(&mut bytes)?;
    let mut extra = [0u8; 1];
    if file.read(&mut extra)? != 0 {
        return Err(invalid("image file grew beyond admitted extent"));
    }
    Ok(bytes)
}
fn write_new(path: &Path, bytes: &[u8], ack: Ack) -> io::Result<()> {
    write_new_in(path, bytes, ack, &Context::legacy())
}
fn write_new_in(path: &Path, bytes: &[u8], ack: Ack, ctx: &Context) -> io::Result<()> {
    let _temporary = ctx.temporary(path.as_os_str().len() * 4 + 1024)?;
    ctx.backing(bytes.len() as u64)?;
    ctx.io(bytes.len() as u64)?;
    ctx.work(1)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| context("capture create file", path, e))?;
    if matches!(
        ctx.current_stage(),
        ImageStage::Completion | ImageStage::Custody
    ) {
        ctx.output(ImageOutput::CompletionUnknown);
    }
    ctx.work(1)?;
    file.write_all(bytes)
        .map_err(|e| context("capture write file", path, e))?;
    if ack == Ack::Flush {
        ctx.work(1)?;
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
    copy_entries_in(source, target, entries, owned, ack, &Context::legacy())
}
fn copy_entries_in(
    source: &Path,
    target: &Path,
    entries: &[ImageEntry],
    owned: &BTreeMap<String, OwnedFile>,
    ack: Ack,
    ctx: &Context,
) -> io::Result<(u64, u64)> {
    let _scope = ctx.scope();
    let _buffer = ctx.temporary(8192 + 1024)?;
    let mut file_barriers = 0;
    let mut dirs = Vec::new();
    reserve_vec(&mut dirs, ctx)?;
    ctx.allocate(target.as_os_str().len())?;
    dirs.push(target.to_owned());
    for e in entries {
        ctx.work(1)?;
        let relative = relative_in(&e.path, ctx)?;
        ctx.allocate((target.as_os_str().len() + relative.as_os_str().len() + 2) * 4 + 64)?;
        let destination = target.join(&relative);
        if e.directory {
            ctx.work(1)?;
            fs::create_dir(&destination)
                .map_err(|e| context("capture copy directory", &destination, e))?;
            reserve_vec(&mut dirs, ctx)?;
            dirs.push(destination);
        } else {
            ctx.backing(e.bytes)?;
            ctx.io(e
                .bytes
                .checked_mul(2)
                .and_then(|v| v.checked_add(1))
                .ok_or_else(|| invalid("image copy extent overflow"))?)?;
            ctx.work(2)?;
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&destination)
                .map_err(|e| context("capture copy create", &destination, e))?;
            let mut input = if let Some(file) = owned.get(&e.path) {
                file.handle.try_clone()?
            } else {
                ctx.allocate((source.as_os_str().len() + relative.as_os_str().len() + 2) * 4 + 64)?;
                File::open(source.join(&relative))?
            };
            input.seek(SeekFrom::Start(0))?;
            let mut buf = [0u8; 8192];
            let mut remaining = e.bytes;
            while remaining > 0 {
                ctx.work(1)?;
                let n = remaining.min(buf.len() as u64) as usize;
                input.read_exact(&mut buf[..n])?;
                output.write_all(&buf[..n])?;
                ctx.copied(n as u64);
                remaining -= n as u64;
            }
            if input.read(&mut buf[..1])? != 0 {
                return Err(invalid("copy source grew beyond admitted extent"));
            }
            if ack == Ack::Flush {
                ctx.work(1)?;
                output
                    .sync_all()
                    .map_err(|e| context("capture copied-file barrier", &destination, e))?;
                file_barriers += 1;
            }
        }
    }
    ctx.work(dirs.len() as u64)?;
    dirs.sort_unstable_by_key(|d| std::cmp::Reverse(d.components().count()));
    if ack == Ack::Flush {
        for d in &dirs {
            sync_output_in(d, true, ack, ctx)?;
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
    fn encoded_len(&self) -> io::Result<usize> {
        let mut size = 8usize
            + 8
            + self.source_label.len()
            + 32
            + 4
            + 8
            + self.config.len()
            + 8
            + self.root_sha256.len()
            + 24;
        for e in &self.entries {
            size = size
                .checked_add(25 + e.path.len() + e.sha256.len())
                .ok_or_else(|| invalid("image manifest size overflow"))?;
        }
        Ok(size)
    }
    fn encode_in(&self, ctx: &Context) -> io::Result<Vec<u8>> {
        let size = self.encoded_len()?;
        ctx.allocate(size)?;
        ctx.work(size as u64)?;
        let mut out = Vec::new();
        out.try_reserve_exact(size)
            .map_err(|_| io::Error::other("manifest backing allocation failed"))?;
        out.extend(b"MBCAP1");
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
        for e in &self.entries {
            text(&mut out, &e.path);
            out.push(e.directory as u8);
            out.extend(e.bytes.to_le_bytes());
            text(&mut out, &e.sha256);
        }
        debug_assert_eq!(out.len(), size);
        Ok(out)
    }
    fn decode_in(bytes: &[u8], ctx: &Context) -> io::Result<Self> {
        struct Reader<'a> {
            ctx: &'a Context,
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
                let bytes = self.take(n)?;
                self.ctx.allocate(n)?;
                Ok(bytes.to_vec())
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
        ctx.work(bytes.len() as u64)?;
        let mut r = Reader { bytes, pos: 0, ctx };
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
        if !ctx.funded() && count > MAX_ENTRIES {
            return Err(invalid("manifest entry count limit"));
        }
        // The count must first fit its literal representation, then its real
        // funded vector. Neither a wire count nor a manifest claims resources.
        if count > (bytes.len() - r.pos) / 25 {
            return Err(invalid("manifest count exceeds literal body"));
        }
        ctx.allocate(
            count
                .checked_mul(std::mem::size_of::<ImageEntry>())
                .ok_or_else(|| invalid("manifest entries size overflow"))?,
        )?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| io::Error::other("manifest entry backing allocation failed"))?;
        for _ in 0..count {
            let path = r.text()?;
            {
                let _temporary = ctx.scope();
                relative_in(&path, ctx)?;
            }
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
        self,
        destination: &Path,
        request: CaptureRequest,
    ) -> io::Result<CaptureManifest> {
        self.capture_stopped_in(destination, request, &Context::legacy())
    }
    fn capture_stopped_in(
        mut self,
        destination: &Path,
        request: CaptureRequest,
        ctx: &Context,
    ) -> io::Result<CaptureManifest> {
        crate::timers::require_joined(&self.timers)?;
        if request.source_label.is_empty()
            || request.source_label.len() > 4096
            || request.source_label.contains(['\0', '\n', '\r'])
        {
            return Err(invalid("capture source label required"));
        }
        ctx.allocate(request.source_label.len())?;
        let source_label = request.source_label.as_str().to_owned();
        let config = config_bytes_in(&self.config, ctx)?;
        let source = root_directory_in(&self.path, ctx)?;
        let parent = root_directory_in(
            destination
                .parent()
                .ok_or_else(|| invalid("image parent required"))?,
            ctx,
        )?;
        if parent.starts_with(&source) {
            return Err(invalid("capture destination is inside source"));
        }
        // Stop every owner and observe every join result. Settlement continues
        // even if one join fails; a panic can never publish a complete image.
        ctx.stage(ImageStage::Stopping);
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
        let health_workspace = self
            .chunks
            .lock()
            .unwrap()
            .table
            .len()
            .checked_mul(256)
            .and_then(|n| n.checked_add(131072))
            .ok_or_else(|| invalid("health population size overflow"))?;
        let health_scope = ctx.temporary(health_workspace)?;
        let health = self.health2();
        if health.storage_error
            || health.read_only
            || health.expiry_degraded
            || health.catalogue_rewrite_error.is_some()
        {
            return Err(invalid("capture source health is incomplete"));
        }
        drop(health);
        drop(health_scope);
        ctx.stage(ImageStage::Inventory);
        let owned = owned_files_in(&self, ctx)?;
        let output = new_directory_in(destination, ctx)?;
        // Fixed control/output path joins below are admitted before allocation.
        ctx.allocate((output.as_os_str().len() + 64) * 32)?;
        // The existence of this file documents incomplete outputs on every
        // later refusal; only capture.complete grants complete-image status.
        write_new_in(
            &output.join("capture.started"),
            b"MBCAP1 incomplete unless capture.complete validates\n",
            request.ack,
            ctx,
        )?;
        let entries = inventory_in(&source, &owned, ctx)?;
        let source_utc_ms = self.clock.utc_ms();
        let source_mono_ms = self.clock.mono_ms();
        let target = output.join(PAYLOAD);
        ctx.stage(ImageStage::Copy);
        ctx.work(1)?;
        fs::create_dir(&target)?;
        let (file_barriers, directory_barriers) =
            copy_entries_in(&source, &target, &entries, &owned, request.ack, ctx)?;
        let changed = {
            let _temporary = ctx.scope();
            inventory_in(&source, &owned, ctx)? != entries
        } || {
            let _temporary = ctx.scope();
            inventory_in(&target, &BTreeMap::new(), ctx)? != entries
        } || !config_matches(&self.config, &config, ctx)?;
        if changed {
            return Err(invalid(
                "capture source/config changed or destination readback differs",
            ));
        }
        let mut manifest = CaptureManifest {
            version: 1,
            source_label,
            source_epoch: self.epoch,
            source_utc_ms,
            source_mono_ms,
            completed_utc_ms: self.clock.utc_ms(),
            requested_ack: request.ack,
            achieved_ack: request.ack,
            complete: true,
            config,
            root_sha256: entries_hash_in(&entries, ctx)?,
            entries,
            file_barriers,
            directory_barriers: directory_barriers + if request.ack == Ack::Flush { 3 } else { 0 },
            readback: true,
            completion: None,
        };
        ctx.stage(ImageStage::Encoding);
        let encoded = manifest.encode_in(ctx)?;
        if !ctx.funded() && encoded.len() as u64 > MAX_MANIFEST {
            return Err(invalid("capture manifest limit"));
        }
        write_new_in(&output.join(MANIFEST), &encoded, request.ack, ctx)?;
        sync_output_in(&output, true, request.ack, ctx)?;
        sync_output_in(output.parent().unwrap(), true, request.ack, ctx)?;
        let manifest_differs = {
            let _temporary = ctx.scope();
            read_file_in(&output.join(MANIFEST), ctx)? != encoded
        };
        if manifest_differs {
            return Err(invalid("manifest readback differs"));
        }
        // Completion is last, binds the entire manifest, and is separately
        // covered by the image-directory barrier and readback.
        ctx.stage(ImageStage::Completion);
        let manifest_hash = hash_in(&encoded, ctx)?;
        write_new_in(
            &output.join(COMPLETE),
            manifest_hash.as_bytes(),
            request.ack,
            ctx,
        )?;
        sync_output_in(&output, true, request.ack, ctx)?;
        let differs = {
            let _temporary = ctx.scope();
            validate_contents_in(&output, ctx)? != manifest
        };
        if differs {
            return Err(invalid("completed image readback differs"));
        }
        // This evidence is issued only AFTER successful completion-file and
        // final directory sync, plus complete readback. It is not stored inside
        // the image: no readable in-image marker can prove its own failed sync.
        if request.ack == Ack::Flush {
            manifest.completion = Some(CaptureCompletion {
                manifest_sha256: manifest_hash,
            });
        }
        ctx.output(ImageOutput::Complete);
        ctx.achieved_ack(request.ack);
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
    validate_selected_in(image, completion, &Context::legacy())
}
fn validate_selected_in(
    image: &Path,
    completion: Option<&CaptureCompletion>,
    ctx: &Context,
) -> io::Result<CaptureManifest> {
    let mut manifest = validate_contents_in(image, ctx)?;
    if manifest.requested_ack == Ack::Flush {
        let evidence=completion.ok_or_else(||invalid("Flush capture settlement unknown: original post-barrier completion evidence required"))?;
        let matches = {
            let _temporary = ctx.scope();
            evidence.manifest_sha256 == hash_in(&manifest.encode_in(ctx)?, ctx)?
        };
        if !matches {
            return Err(invalid(
                "capture completion evidence does not bind this manifest",
            ));
        }
        ctx.allocate(64)?;
        manifest.completion = Some(evidence.clone());
    }
    Ok(manifest)
}
fn validate_contents_in(image: &Path, ctx: &Context) -> io::Result<CaptureManifest> {
    let image = root_directory_in(image, ctx)?;
    directory_streams_in(&image, ctx)?;
    ctx.allocate((image.as_os_str().len() + 64) * 24)?;
    // Envelope membership is exactly four names. Refuse unexpected members
    // immediately rather than allocating an arbitrary unmanifested path set.
    let mut found = 0u8;
    let _reader_workspace = ctx.temporary(image.as_os_str().len() * 4 + 4096)?;
    ctx.work(1)?;
    let mut reader = fs::read_dir(&image)?;
    loop {
        let _entry = ctx.temporary(image.as_os_str().len() * 8 + 4 * 260 + 1024)?;
        ctx.work(1)?;
        let Some(entry) = reader.next() else { break };
        let entry = entry?;
        let name = entry.file_name();
        let bit = if name == "capture.started" {
            1
        } else if name == MANIFEST {
            2
        } else if name == COMPLETE {
            4
        } else if name == PAYLOAD {
            8
        } else {
            return Err(invalid("image envelope population differs"));
        };
        if found & bit != 0 {
            return Err(invalid("image envelope population differs"));
        }
        found |= bit;
    }
    if found != 15 {
        return Err(invalid("image envelope population differs"));
    }
    for name in ["capture.started", MANIFEST, COMPLETE] {
        let path = image.join(name);
        ctx.work(2)?;
        let meta = fs::symlink_metadata(&path)?;
        normal(&meta)?;
        if !meta.is_file() {
            return Err(invalid("image envelope file type"));
        }
        regular_streams_in(&File::open(path)?, ctx)?;
    }
    ctx.work(3)?;
    if fs::metadata(image.join("capture.started"))?.len()
        != b"MBCAP1 incomplete unless capture.complete validates\n".len() as u64
        || fs::metadata(image.join(COMPLETE))?.len() != 64
    {
        return Err(invalid("manifest/completion size"));
    }
    {
        let _temporary = ctx.scope();
        if read_file_in(&image.join("capture.started"), ctx)?
            != b"MBCAP1 incomplete unless capture.complete validates\n"
        {
            return Err(invalid("capture-start marker differs"));
        }
    }
    if !ctx.funded() && fs::metadata(image.join(MANIFEST))?.len() > MAX_MANIFEST {
        return Err(invalid("manifest/completion size"));
    }
    let manifest = {
        // The encoded file is temporary; decoded metadata has its own separate
        // charge and lives beyond it. Release only the encoded buffer below.
        let before = ctx.memory_used();
        let encoded = read_file_in(&image.join(MANIFEST), ctx)?;
        let encoded_charge = ctx.memory_used() - before;
        {
            let _temporary = ctx.scope();
            if read_file_in(&image.join(COMPLETE), ctx)? != hash_in(&encoded, ctx)?.as_bytes() {
                return Err(invalid("completion does not bind manifest"));
            }
        }
        let manifest = CaptureManifest::decode_in(&encoded, ctx)?;
        drop(encoded);
        ctx.release(encoded_charge);
        manifest
    };
    if !manifest.complete || !manifest.readback || manifest.requested_ack != manifest.achieved_ack {
        return Err(invalid("capture incomplete or requested Ack not achieved"));
    }
    let payload = root_directory_in(&image.join(PAYLOAD), ctx)?;
    {
        let _temporary = ctx.scope();
        let actual = inventory_in(&payload, &BTreeMap::new(), ctx)?;
        if actual != manifest.entries || entries_hash_in(&actual, ctx)? != manifest.root_sha256 {
            return Err(invalid(
                "image contents/population differ from complete manifest",
            ));
        }
    }
    let files = manifest.entries.iter().filter(|e| !e.directory).count() as u64;
    let directories = manifest.entries.iter().filter(|e| e.directory).count() as u64 + 1;
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
    crate::platform::prepare_quarantined_lock(&target, |dir| sync_output(dir, true, Ack::Flush))?;
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
