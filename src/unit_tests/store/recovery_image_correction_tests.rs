use super::*;
use std::cell::RefCell;
thread_local! { static FAULT: RefCell<&'static str> = const { RefCell::new("") }; }
pub(super) fn barrier(path: &Path, directory: bool) -> io::Result<()> {
    let fault = FAULT.with(|f| *f.borrow());
    let fail = match fault {
        "completion-file" => !directory && path.file_name().is_some_and(|n| n == COMPLETE),
        "final-directory" => directory && path.join(COMPLETE).is_file(),
        "manifest-file" => !directory && path.file_name().is_some_and(|n| n == MANIFEST),
        "manifest-directory" => {
            directory && path.join(MANIFEST).is_file() && !path.join(COMPLETE).exists()
        }
        _ => false,
    };
    if fail {
        Err(io::Error::other(format!(
            "injected required barrier failure: {fault}"
        )))
    } else {
        Ok(())
    }
}
fn fixture(label: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit retained fixture root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let p = base.join(format!(
        "barrier-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&p).unwrap();
    println!("retained barrier fixture {}", p.display());
    p
}
fn fault_case(fault: &'static str, marker_readable: bool) {
    let p = fixture(fault);
    let known = Store::open(
        &p.join("known-source"),
        Config {
            signal_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap()
    .capture_stopped(
        &p.join("known-image"),
        CaptureRequest {
            source_label: "known-success".into(),
            ack: Ack::Flush,
        },
    )
    .unwrap();
    let evidence = known.completion().unwrap();
    assert_eq!(
        validate_image_with_completion(&p.join("known-image"), evidence).unwrap(),
        known
    );
    let store = Store::open(
        &p.join("source"),
        Config {
            signal_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap();
    FAULT.with(|f| *f.borrow_mut() = fault);
    let result = store.capture_stopped(
        &p.join("image"),
        CaptureRequest {
            source_label: "barrier-fixture".into(),
            ack: Ack::Flush,
        },
    );
    FAULT.with(|f| *f.borrow_mut() = "");
    let error = result.unwrap_err();
    assert!(error.to_string().contains(fault), "{error}");
    assert_eq!(p.join("image/capture.complete").is_file(), marker_readable);
    if marker_readable {
        assert_eq!(
            fs::read(p.join("image/capture.complete")).unwrap().len(),
            64
        );
    }
    assert!(
        validate_image(&p.join("image")).is_err(),
        "readable marker cannot settle failed {fault}"
    );
    assert!(
        validate_image_with_completion(&p.join("image"), evidence).is_err(),
        "unrelated original success cannot settle failed {fault}"
    );
}
#[test]
fn completion_file_failure_remains_unsettled() {
    fault_case("completion-file", true);
}
#[test]
fn final_directory_failure_remains_unsettled() {
    fault_case("final-directory", true);
}
#[test]
fn manifest_file_failure_remains_incomplete() {
    fault_case("manifest-file", false);
}
#[test]
fn manifest_directory_failure_remains_incomplete() {
    fault_case("manifest-directory", false);
}

#[cfg(windows)]
#[test]
fn funded_completion_failures_return_actual_unknown_without_witness() {
    use crate::atomic::OriginalBudget;
    for fault in ["completion-file", "final-directory"] {
        let p = fixture(fault);
        let memory = OriginalBudget::new(4 * 1024 * 1024);
        let budget = ImageBudget::new(
            memory.clone(),
            ImageLimits {
                backing_bytes: 1024 * 1024,
                io_bytes: 16 * 1024 * 1024,
                operations: 16 * 1024 * 1024,
            },
        )
        .unwrap();
        let baseline = memory.used();
        let store = Store::open(
            &p.join("funded-source"),
            Config {
                signal_bytes: 0,
                ..Config::default()
            },
        )
        .unwrap();
        FAULT.with(|f| *f.borrow_mut() = fault);
        let result = store.capture_stopped_funded(
            &p.join("funded-image"),
            CaptureRequest {
                source_label: "funded-failed-completion".into(),
                ack: Ack::Flush,
            },
            &budget,
            2 * 1024 * 1024,
        );
        FAULT.with(|f| *f.borrow_mut() = "");
        let error = result.unwrap_err();
        assert!(error.error.to_string().contains(fault));
        assert_eq!(error.progress.output, ImageOutput::CompletionUnknown);
        assert_eq!(error.progress.requested_ack, Some(Ack::Flush));
        assert_eq!(error.progress.achieved_ack, None);
        assert_eq!(error.progress.stage, ImageStage::Completion);
        assert_eq!(
            fs::read(p.join("funded-image/capture.complete"))
                .unwrap()
                .len(),
            64
        );
        assert!(
            validate_image_funded(&p.join("funded-image"), None, &budget, 1024 * 1024).is_err()
        );
        assert!(validate_image(&p.join("funded-image")).is_err());
        assert_eq!(memory.used(), baseline + 2 * 1024 * 1024);
        drop(error);
        assert_eq!(memory.used(), baseline);
    }
}
