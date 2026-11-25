//! Caller-owned image resources and reservation lifetimes. No wire value is a grant.
use super::*;
use crate::atomic::{OriginalBudget, PublicationError, Reservation};
use std::{
    cell::Cell,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

/// Finite logical backing and work allowances shared by every clone.
/// Bytes are regular-file bytes, not physical filesystem block/RSS accounting.
#[derive(Clone, Copy, Debug)]
pub struct ImageLimits {
    pub backing_bytes: u64,
    /// Admitted read/write extents, including repeated inventory/readback.
    pub io_bytes: u64,
    /// Filesystem/entry/path work units; see the image contract.
    pub operations: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageResource {
    Metadata,
    Backing,
    Io,
    Operations,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageShortfall {
    pub resource: ImageResource,
    pub requested: u64,
    pub available: u64,
}
impl fmt::Display for ImageShortfall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "insufficient {:?}: requested {}, available {}",
            self.resource, self.requested, self.available
        )
    }
}
impl std::error::Error for ImageShortfall {}
struct Pool {
    memory: OriginalBudget,
    limits: ImageLimits,
    backing: AtomicU64,
    io: AtomicU64,
    operations: AtomicU64,
    _control: Reservation,
}
/// An actual shared account, not serialized funding. Metadata reservations use
/// the existing OriginalBudget owner. Backing and work are consumptive: failure
/// or dropping a result never refunds expenditure or removes retained artifacts.
#[derive(Clone)]
pub struct ImageBudget(Arc<Pool>);
impl ImageBudget {
    pub fn new(memory: OriginalBudget, limits: ImageLimits) -> Result<Self, ImageShortfall> {
        let bytes = std::mem::size_of::<Pool>() + 64;
        let control = memory
            .reserve_available(bytes)
            .map_err(|available| ImageShortfall {
                resource: ImageResource::Metadata,
                requested: bytes as u64,
                available: available as u64,
            })?;
        Ok(Self(Arc::new(Pool {
            memory,
            limits,
            backing: AtomicU64::new(0),
            io: AtomicU64::new(0),
            operations: AtomicU64::new(0),
            _control: control,
        })))
    }
    pub fn metadata_used(&self) -> usize {
        self.0.memory.used()
    }
    pub fn backing_used(&self) -> u64 {
        self.0.backing.load(Ordering::Acquire)
    }
    pub fn io_used(&self) -> u64 {
        self.0.io.load(Ordering::Acquire)
    }
    pub fn operations_used(&self) -> u64 {
        self.0.operations.load(Ordering::Acquire)
    }
    fn spend(&self, resource: ImageResource, n: u64) -> io::Result<()> {
        let (used, limit) = match resource {
            ImageResource::Backing => (&self.0.backing, self.0.limits.backing_bytes),
            ImageResource::Io => (&self.0.io, self.0.limits.io_bytes),
            ImageResource::Operations => (&self.0.operations, self.0.limits.operations),
            ImageResource::Metadata => unreachable!(),
        };
        let mut old = used.load(Ordering::Acquire);
        loop {
            let next = old.checked_add(n).filter(|v| *v <= limit).ok_or_else(|| {
                io::Error::other(ImageShortfall {
                    resource,
                    requested: n,
                    available: limit.saturating_sub(old),
                })
            })?;
            match used.compare_exchange_weak(old, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Ok(()),
                Err(actual) => old = actual,
            }
        }
    }
}
impl Store {
    /// Reserve image metadata against the same actual Store scan/memory owner
    /// used by namespace originals. Its lifetime can outlive this consumed Store.
    pub fn image_budget(
        &self,
        metadata_bytes: usize,
        limits: ImageLimits,
    ) -> Result<ImageBudget, PublicationError> {
        let mut scans = self.scans.lock().unwrap();
        self.purge_scans(&mut scans);
        let state = self.state.lock().unwrap();
        if state.read_only || state.warming {
            return Err(PublicationError::Unavailable);
        }
        let total = self
            .scan_usage
            .bytes
            .load(Ordering::Acquire)
            .checked_add(metadata_bytes)
            .ok_or(PublicationError::Resource(7))?;
        if self.scan_usage.count.load(Ordering::Acquire) >= self.config.snapshot_max
            || total > self.config.snapshot_bytes
            || self.cache_limit(&state) < metadata_bytes
        {
            return Err(PublicationError::Resource(7));
        }
        self.scan_usage
            .bytes
            .fetch_add(metadata_bytes, Ordering::AcqRel);
        self.scan_usage.count.fetch_add(1, Ordering::AcqRel);
        let memory = OriginalBudget::backed(
            metadata_bytes,
            Some(crate::snapshots::ScanCharge {
                usage: self.scan_usage.clone(),
                bytes: metadata_bytes,
            }),
        );
        self.trim_cache(&state);
        drop(state);
        drop(scans);
        ImageBudget::new(memory, limits).map_err(|_| PublicationError::Resource(7))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageStage {
    Admission,
    Stopping,
    Inventory,
    Copy,
    Encoding,
    Completion,
    Validation,
    Custody,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageOutput {
    Untouched,
    Incomplete,
    CompletionUnknown,
    Complete,
}
#[derive(Clone, Copy, Debug)]
pub struct ImageProgress {
    pub stage: ImageStage,
    pub output: ImageOutput,
    pub requested_ack: Option<Ack>,
    pub achieved_ack: Option<Ack>,
    pub metadata_requested: usize,
    pub metadata_reserved: usize,
    pub metadata_peak: usize,
    pub backing_admitted: u64,
    pub io_admitted: u64,
    pub operations_admitted: u64,
    /// Entire chunks whose write_all returned success. A failing write may
    /// have written an additional prefix; no exact no-effect claim follows.
    pub copied_bytes_confirmed: u64,
}
struct OperationOwner {
    _charge: Reservation,
    _budget: ImageBudget,
}
/// Borrowed manifest inspection, or clone this result to share all returned
/// allocations and their original reservation. There is no uncharged deep clone.
#[derive(Clone)]
pub struct FundedImage(Arc<ImageData>);
impl fmt::Debug for FundedImage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FundedImage")
            .field("progress", &self.0.progress)
            .finish()
    }
}
struct ImageData {
    manifest: CaptureManifest,
    progress: ImageProgress,
    _owner: Arc<OperationOwner>,
}
impl FundedImage {
    pub fn manifest(&self) -> &CaptureManifest {
        &self.0.manifest
    }
    pub fn completion(&self) -> Option<&CaptureCompletion> {
        self.0.manifest.completion()
    }
    pub fn progress(&self) -> ImageProgress {
        self.0.progress
    }
}
pub struct ImageFailure {
    pub error: io::Error,
    pub progress: ImageProgress,
    _owner: Option<Arc<OperationOwner>>,
}
impl ImageFailure {
    pub fn shortfall(&self) -> Option<&ImageShortfall> {
        self.error.get_ref()?.downcast_ref()
    }
}
impl fmt::Debug for ImageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageFailure")
            .field("error", &self.error)
            .field("progress", &self.progress)
            .finish()
    }
}
impl fmt::Display for ImageFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}/{:?}: {}",
            self.progress.stage, self.progress.output, self.error
        )
    }
}
impl std::error::Error for ImageFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

pub(super) struct Context {
    owner: Option<Arc<OperationOwner>>,
    limit: usize,
    used: Cell<usize>,
    peak: Cell<usize>,
    stage: Cell<ImageStage>,
    output: Cell<ImageOutput>,
    requested_ack: Cell<Option<Ack>>,
    achieved_ack: Cell<Option<Ack>>,
    backing: Cell<u64>,
    io: Cell<u64>,
    operations: Cell<u64>,
    copied: Cell<u64>,
}
impl Context {
    pub(super) fn legacy() -> Self {
        Self {
            owner: None,
            limit: 0,
            used: Cell::new(0),
            peak: Cell::new(0),
            stage: Cell::new(ImageStage::Admission),
            output: Cell::new(ImageOutput::Untouched),
            requested_ack: Cell::new(None),
            achieved_ack: Cell::new(None),
            backing: Cell::new(0),
            io: Cell::new(0),
            operations: Cell::new(0),
            copied: Cell::new(0),
        }
    }
    fn admitted(budget: &ImageBudget, bytes: usize) -> Result<Self, ImageFailure> {
        let mut ctx = Self::legacy();
        ctx.limit = bytes;
        if !cfg!(windows) {
            return Err(ctx.failure(io::Error::new(io::ErrorKind::Unsupported,
                "funded image directory-workspace admission is currently supported on Windows; legacy image APIs retain their platform contract")));
        }
        let charge = budget
            .0
            .memory
            .reserve_available(bytes)
            .map_err(|available| {
                ctx.failure(io::Error::other(ImageShortfall {
                    resource: ImageResource::Metadata,
                    requested: bytes as u64,
                    available: available as u64,
                }))
            })?;
        // Operation/result/error control objects and the one-use context are
        // covered before constructing their shared owner or returning data.
        let base = std::mem::size_of::<OperationOwner>()
            + std::mem::size_of::<ImageData>()
            + std::mem::size_of::<ImageFailure>()
            + 512;
        if bytes < base {
            return Err(ctx.failure(io::Error::other(ImageShortfall {
                resource: ImageResource::Metadata,
                requested: base as u64,
                available: bytes as u64,
            })));
        }
        ctx.used.set(base);
        ctx.peak.set(base);
        ctx.owner = Some(Arc::new(OperationOwner {
            _charge: charge,
            _budget: budget.clone(),
        }));
        Ok(ctx)
    }
    pub(super) fn funded(&self) -> bool {
        self.owner.is_some()
    }
    pub(super) fn memory_used(&self) -> usize {
        self.used.get()
    }
    pub(super) fn allocate(&self, n: usize) -> io::Result<()> {
        if !self.funded() {
            return Ok(());
        }
        let next = self
            .used
            .get()
            .checked_add(n)
            .filter(|v| *v <= self.limit)
            .ok_or_else(|| {
                io::Error::other(ImageShortfall {
                    resource: ImageResource::Metadata,
                    requested: n as u64,
                    available: self.limit.saturating_sub(self.used.get()) as u64,
                })
            })?;
        self.used.set(next);
        self.peak.set(self.peak.get().max(next));
        Ok(())
    }
    pub(super) fn release(&self, n: usize) {
        if self.funded() {
            self.used.set(
                self.used
                    .get()
                    .checked_sub(n)
                    .expect("image charge ownership"),
            );
        }
    }
    pub(super) fn temporary(&self, n: usize) -> io::Result<Temporary<'_>> {
        self.allocate(n)?;
        Ok(Temporary {
            ctx: self,
            bytes: n,
        })
    }
    /// Only for a lexical block whose entire owned temporary population drops
    /// before this guard. No returned allocation may escape that block.
    pub(super) fn scope(&self) -> Scope<'_> {
        Scope {
            ctx: self,
            before: self.used.get(),
        }
    }
    fn spend(&self, r: ImageResource, n: u64, local: &Cell<u64>) -> io::Result<()> {
        if let Some(owner) = &self.owner {
            owner._budget.spend(r, n)?;
            local.set(
                local
                    .get()
                    .checked_add(n)
                    .ok_or_else(|| invalid("image work counter overflow"))?,
            );
        }
        Ok(())
    }
    pub(super) fn backing(&self, n: u64) -> io::Result<()> {
        self.spend(ImageResource::Backing, n, &self.backing)
    }
    pub(super) fn io(&self, n: u64) -> io::Result<()> {
        self.spend(ImageResource::Io, n, &self.io)
    }
    pub(super) fn work(&self, n: u64) -> io::Result<()> {
        self.spend(ImageResource::Operations, n, &self.operations)
    }
    pub(super) fn stage(&self, s: ImageStage) {
        self.stage.set(s);
    }
    pub(super) fn current_stage(&self) -> ImageStage {
        self.stage.get()
    }
    pub(super) fn output(&self, s: ImageOutput) {
        self.output.set(s);
    }
    pub(super) fn requested_ack(&self, ack: Ack) {
        self.requested_ack.set(Some(ack));
    }
    pub(super) fn achieved_ack(&self, ack: Ack) {
        self.achieved_ack.set(Some(ack));
    }
    pub(super) fn copied(&self, n: u64) {
        self.copied.set(self.copied.get() + n);
    }
    fn progress(&self) -> ImageProgress {
        ImageProgress {
            stage: self.stage.get(),
            output: self.output.get(),
            requested_ack: self.requested_ack.get(),
            achieved_ack: self.achieved_ack.get(),
            metadata_requested: self.limit,
            metadata_reserved: if self.owner.is_some() { self.limit } else { 0 },
            metadata_peak: self.peak.get(),
            backing_admitted: self.backing.get(),
            io_admitted: self.io.get(),
            operations_admitted: self.operations.get(),
            copied_bytes_confirmed: self.copied.get(),
        }
    }
    fn failure(&self, error: io::Error) -> ImageFailure {
        ImageFailure {
            error,
            progress: self.progress(),
            _owner: self.owner.clone(),
        }
    }
    fn finish(&self, manifest: CaptureManifest) -> FundedImage {
        FundedImage(Arc::new(ImageData {
            manifest,
            progress: self.progress(),
            _owner: self.owner.as_ref().unwrap().clone(),
        }))
    }
}
pub(super) struct Temporary<'a> {
    ctx: &'a Context,
    bytes: usize,
}
impl Drop for Temporary<'_> {
    fn drop(&mut self) {
        self.ctx.release(self.bytes);
    }
}
pub(super) struct Scope<'a> {
    ctx: &'a Context,
    before: usize,
}
impl Drop for Scope<'_> {
    fn drop(&mut self) {
        if self.ctx.funded() {
            self.ctx.used.set(self.before);
        }
    }
}

impl Store {
    /// One consumed-owner, whole-root capture. The actual account is admitted
    /// before metadata growth. All partial outputs and uncertainties survive.
    pub fn capture_stopped_funded(
        self,
        destination: &Path,
        request: CaptureRequest,
        budget: &ImageBudget,
        metadata_bytes: usize,
    ) -> Result<FundedImage, ImageFailure> {
        let ctx = Context::admitted(budget, metadata_bytes).map_err(|mut error| {
            error.progress.requested_ack = Some(request.ack);
            error
        })?;
        ctx.requested_ack(request.ack);
        let result = self.capture_stopped_in(destination, request, &ctx);
        match result {
            Ok(m) => Ok(ctx.finish(m)),
            Err(e) => Err(ctx.failure(e)),
        }
    }
}
/// Standalone validation admits its OWN account and full metadata/read work.
/// Original successful Flush evidence remains independent caller custody.
pub fn validate_image_funded(
    image: &Path,
    completion: Option<&CaptureCompletion>,
    budget: &ImageBudget,
    metadata_bytes: usize,
) -> Result<FundedImage, ImageFailure> {
    let ctx = Context::admitted(budget, metadata_bytes)?;
    ctx.stage(ImageStage::Validation);
    match validate_selected_in(image, completion, &ctx) {
        Ok(m) => {
            ctx.requested_ack(m.requested_ack);
            ctx.achieved_ack(m.achieved_ack);
            Ok(ctx.finish(m))
        }
        Err(e) => Err(ctx.failure(e)),
    }
}

#[derive(Clone)]
pub struct FundedCompletion(Arc<CompletionData>);
struct CompletionData {
    completion: CaptureCompletion,
    progress: ImageProgress,
    _owner: Arc<OperationOwner>,
}
impl FundedCompletion {
    pub fn completion(&self) -> &CaptureCompletion {
        &self.0.completion
    }
    pub fn progress(&self) -> ImageProgress {
        self.0.progress
    }
}
impl CaptureCompletion {
    /// Independently funded NEW custody of this actual original completion.
    /// A failed writer retains its artifact and never reconstructs a witness.
    pub fn persist_new_funded(
        &self,
        path: &Path,
        budget: &ImageBudget,
        metadata_bytes: usize,
    ) -> Result<FundedCompletion, ImageFailure> {
        let ctx = Context::admitted(budget, metadata_bytes)?;
        ctx.stage(ImageStage::Custody);
        ctx.requested_ack(Ack::Flush);
        let result = (|| {
            let parent = root_directory_in(
                path.parent()
                    .ok_or_else(|| invalid("completion custody parent required"))?,
                &ctx,
            )?;
            if !path.is_absolute() {
                return Err(invalid("absolute completion custody selector required"));
            }
            ctx.allocate(72 + 64 + std::mem::size_of::<CompletionData>() + 64)?;
            let bytes = self.to_bytes();
            write_new_in(path, &bytes, Ack::Flush, &ctx)?;
            ctx.output(ImageOutput::CompletionUnknown);
            sync_output_in(&parent, true, Ack::Flush, &ctx)?;
            if read_file_in(path, &ctx)? != bytes {
                return Err(invalid("completion custody readback differs"));
            }
            ctx.output(ImageOutput::Complete);
            ctx.achieved_ack(Ack::Flush);
            Ok(FundedCompletion(Arc::new(CompletionData {
                completion: self.clone(),
                progress: ctx.progress(),
                _owner: ctx.owner.as_ref().unwrap().clone(),
            })))
        })();
        result.map_err(|e| ctx.failure(e))
    }
}
