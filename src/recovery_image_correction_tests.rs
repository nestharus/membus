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
