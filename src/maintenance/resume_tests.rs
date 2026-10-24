//! A clock-pause release is applied only after its catalogue publication succeeds.
use super::*;

fn paused(f: &Fixture) -> (Store, Arc<chunks::ManualClock>, u64, PathBuf) {
    paused_with_threshold(f, Config::default().catalogue_rewrite_bytes)
}

fn paused_with_threshold(f: &Fixture, threshold: u64) -> (Store, Arc<chunks::ManualClock>, u64, PathBuf) {
    let (store, clock, _, _, id, path) = f.seed(UTC, WINDOW, Class::LATEST);
    let mut config = store.config.clone();
    config.catalogue_rewrite_bytes = threshold;
    drop(store);
    f.unlock();
    clock.jump_utc(-1_000);
    let store = Store::open(&f.0, config).unwrap();
    assert!(store.health2().expiry_degraded, "startup must observe UTC behind the retained catalogue");
    clock.jump_utc(2_000);
    assert!(store.maint.retire(None).unwrap().is_empty());
    assert!(path.exists());
    (store, clock, id, path)
}

fn resume_count(f: &Fixture) -> usize {
    chunks::read_catalogue(&f.0.join("chunks/catalogue.log")).unwrap().0.iter()
        .filter(|r| matches!(r.rec, chunks::Rec::Resume | chunks::Rec::ResumeWithBasis(_))).count()
}

#[derive(Clone, Copy)]
enum Failure { Append, Sync, Fenced }

fn explicit_failure(failure: Failure) {
    let f = Fixture::new();
    let (store, clock, id, path) = paused(&f);
    let request = f.0.join("expiry.resume");
    fs::write(&request, b"explicit request").unwrap();
    {
        let mut chunks = store.chunks.lock().unwrap();
        let cat = chunks.cat.as_mut().unwrap();
        match failure {
            Failure::Append => cat.fail_writes = true,
            Failure::Sync => cat.fail_sync_once = true,
            Failure::Fenced => { cat.fail_sync_once = true; cat.fail_recovery_once = true; }
        }
    }
    store.maint.clock_tick();
    assert_eq!(fs::read(&request).unwrap_or_default(), b"explicit request",
        "a failed Resume publication must retain the caller's request");
    assert!(store.health2().expiry_degraded);
    assert!(store.health2().storage_error, "the failed catalogue publication must be observable");
    assert!(store.maint.retire(None).unwrap().is_empty());
    assert!(path.exists(), "a refused resume must leave automatic retirement paused");
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"second");
    if matches!(failure, Failure::Fenced) {
        let mut chunks = store.chunks.lock().unwrap();
        assert!(chunks.log(clock.utc_ms(), chunks::Rec::Resume).is_err());
        chunks.rewrite_live(clock.utc_ms(), store.next_issued.load(Ordering::SeqCst)).unwrap();
    } else {
        assert_eq!(resume_count(&f), 0, "failed or rolled-back publication cannot apply Resume");
        store.chunks.lock().unwrap().cat.as_mut().unwrap().fail_writes = false;
    }
    store.maint.clock_tick();
    assert!(!request.exists(), "a successful publication consumes the retained request");
    assert!(!store.health2().expiry_degraded);
    assert!(store.health2().storage_error, "successful retry keeps the sticky I/O observation");
    assert_eq!(resume_count(&f), 1);
    assert!(store.maint.retire(None).unwrap().is_empty(), "resuming does not expire recovered outcomes");
    clock.advance(WINDOW);
    assert!(store.maint.retire(None).unwrap().is_empty(), "the recovered retry bound stays inclusive");
    assert!(path.exists());
    clock.advance(1);
    assert_eq!(store.maint.retire(None).unwrap(), vec![id]);
    assert!(!path.exists());
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"second");
    store.maint.clock_tick();
    assert_eq!(resume_count(&f), 1, "later ticks do not republish an applied resume");
}

#[test]
fn explicit_resume_request_survives_a_refused_catalogue_append() { explicit_failure(Failure::Append); }

#[test]
fn explicit_resume_request_survives_catalogue_sync_rollback() { explicit_failure(Failure::Sync); }

#[test]
fn explicit_resume_request_survives_a_fenced_catalogue_until_rewrite() { explicit_failure(Failure::Fenced); }

#[test]
fn automatic_resume_publication_failure_is_visible_and_retried() {
    let f = Fixture::new();
    let (store, clock, id, path) = paused(&f);
    store.maint.clock_tick(); // Establish the stable elapsed-time observation.
    clock.advance(store.config.rotation_ms);
    store.chunks.lock().unwrap().cat.as_mut().unwrap().fail_writes = true;
    store.maint.clock_tick();
    assert!(store.health2().storage_error, "automatic Resume publication failures must be visible too");
    assert!(store.health2().expiry_degraded);
    assert_eq!(resume_count(&f), 0);
    assert!(store.maint.retire(None).unwrap().is_empty());
    assert!(path.exists());
    store.chunks.lock().unwrap().cat.as_mut().unwrap().fail_writes = false;
    store.maint.clock_tick();
    assert!(!store.health2().expiry_degraded);
    assert!(store.health2().storage_error);
    assert_eq!(resume_count(&f), 1);
    assert_eq!(store.maint.retire(None).unwrap(), vec![id]);
}

#[test]
fn healthy_clock_consumes_an_explicit_request_without_an_extra_resume_record() {
    let f = Fixture::new();
    let (store, _, _, _, _, _) = f.seed(UTC, WINDOW, Class::LATEST);
    let request = f.0.join("expiry.resume");
    fs::write(&request, b"already healthy").unwrap();
    store.maint.clock_tick();
    assert!(!request.exists());
    assert!(!store.health2().expiry_degraded);
    assert!(!store.health2().storage_error);
    assert_eq!(resume_count(&f), 0);
}

fn repair_with_disabled_threshold(block_replacement: bool) {
    let f = Fixture::new();
    let (store, clock, id, path) = paused_with_threshold(&f, 0);
    let request = f.0.join("expiry.resume");
    let marker = f.0.join("chunks/catalogue.log.append-pending");
    let catalogue = f.0.join("chunks/catalogue.log");
    fs::write(&request, b"retained resume request").unwrap();
    let rewrites = {
        let mut chunks = store.chunks.lock().unwrap();
        let cat = chunks.cat.as_mut().unwrap();
        cat.fail_sync_once = true;
        cat.fail_recovery_once = true;
        cat.rewrites
    };
    store.maint.clock_tick();
    assert!(store.expiry_degraded());
    assert!(store.storage_error());
    assert!(request.exists() && marker.exists());
    assert!(chunks::read_catalogue(&catalogue).is_err());
    let bytes = fs::read(&catalogue).unwrap();
    let pending = fs::read(&marker).unwrap();
    if block_replacement {
        let temporary = catalogue.with_extension("log.tmp");
        fs::create_dir(&temporary).unwrap();
        store.maint.rewrite_catalogue();
        assert!(store.health2().catalogue_rewrite_error.is_some(),
            "a necessary repair must be attempted even with threshold rewrites disabled");
        assert_eq!(fs::read(&catalogue).unwrap(), bytes);
        assert_eq!(fs::read(&marker).unwrap(), pending);
        assert_eq!(fs::read(&request).unwrap(), b"retained resume request");
        assert!(store.expiry_degraded());
        assert!(store.maint.retire(None).unwrap().is_empty());
        assert!(path.exists());
        fs::remove_dir(temporary).unwrap();
    }
    let crate::CatalogueMaintenance::Recovered { evidence } = store.maintain_catalogue().unwrap() else {
        panic!("a fenced catalogue must report recovery");
    };
    assert_eq!(fs::read(evidence.join("catalogue.log")).unwrap(), bytes);
    assert_eq!(fs::read(evidence.join("append-pending")).unwrap(), pending);
    assert_eq!(store.chunks.lock().unwrap().cat.as_ref().unwrap().rewrites, rewrites + 1,
        "the maintenance owner must repair the fence independently of the size threshold");
    assert!(store.health2().catalogue_rewrite_error.is_none());
    assert!(store.expiry_degraded() && request.exists(), "repair alone does not apply Resume");
    assert!(!marker.exists());
    assert_eq!(resume_count(&f), 0, "the failed Resume must be omitted from known-live state");
    store.maint.clock_tick();
    assert!(!store.expiry_degraded() && !request.exists());
    assert!(store.storage_error());
    assert_eq!(resume_count(&f), 1);
    assert!(store.maint.retire(None).unwrap().is_empty(), "repair/resume cannot shorten recovered retry");
    clock.advance(WINDOW);
    assert!(store.maint.retire(None).unwrap().is_empty());
    assert!(path.exists());
    clock.advance(1);
    assert_eq!(store.maint.retire(None).unwrap(), vec![id]);
    assert!(!path.exists());
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"second");
    store.maint.rewrite_catalogue();
    assert_eq!(store.chunks.lock().unwrap().cat.as_ref().unwrap().rewrites, rewrites + 1,
        "healthy growth remains uncompacted with threshold zero");
}

#[test]
fn zero_threshold_repairs_the_fence_before_retrying_a_retained_resume_request() {
    repair_with_disabled_threshold(false);
}

#[test]
fn zero_threshold_repair_failure_keeps_the_fence_and_resume_request_until_retry() {
    repair_with_disabled_threshold(true);
}
