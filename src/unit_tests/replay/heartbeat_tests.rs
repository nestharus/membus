//! Full-range persisted UTC heartbeat comparisons at recovery.
use super::*;
use crate::{chunks::ManualClock, Store};
use std::sync::atomic::AtomicU64;

const DAY: u64 = 86_400_000;
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".test-output")
            .join(format!(
                "heartbeat-range-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn beat(&self, timestamp: u64) {
        fs::write(
            self.0.join("heartbeat.json"),
            format!("{{\"unix_ms\":{timestamp}}}"),
        )
        .unwrap();
    }
    fn config(utc: u64) -> Config {
        Config {
            clock: Some(ManualClock::new(utc)),
            ..Config::default()
        }
    }
    fn recover(&self, utc: u64) -> Recovered {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            recover(&self.0, &Self::config(utc))
        }));
        assert!(
            result.is_ok(),
            "a persisted heartbeat must not panic recovery"
        );
        result.unwrap().unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn future_heartbeat_near_the_integer_limit_does_not_overflow_or_pause_expiry() {
    for beat in [u64::MAX, u64::MAX - DAY + 1, u64::MAX - DAY / 2] {
        let fixture = Fixture::new();
        fixture.beat(beat);
        let recovered = fixture.recover(1_000_000);
        assert!(recovered.report.expiry_degraded.is_none());
        assert!(!recovered.flags.expiry_degraded.load(Ordering::SeqCst));
        assert_eq!(
            fs::read(fixture.0.join("epoch")).unwrap(),
            1u64.to_le_bytes()
        );
    }
}

#[test]
fn maximum_utc_retains_the_strict_day_boundary_without_overflow() {
    for (age, degraded) in [(DAY + 1, true), (DAY, false), (DAY - 1, false), (0, false)] {
        let fixture = Fixture::new();
        fixture.beat(u64::MAX - age);
        let recovered = fixture.recover(u64::MAX);
        assert_eq!(
            recovered.report.expiry_degraded.is_some(),
            degraded,
            "age={age}"
        );
        assert_eq!(
            recovered.flags.expiry_degraded.load(Ordering::SeqCst),
            degraded
        );
    }
}

#[test]
fn ordinary_heartbeat_day_boundary_stays_strict() {
    for (utc, beat, degraded) in [
        (DAY - 1, 0, false),
        (DAY, 0, false),
        (DAY + 1, 0, true),
        (10, 11, false),
    ] {
        let fixture = Fixture::new();
        fixture.beat(beat);
        let recovered = fixture.recover(utc);
        assert_eq!(recovered.report.expiry_degraded.is_some(), degraded);
    }
}

#[test]
fn missing_or_invalid_heartbeat_keeps_the_existing_fallback() {
    for bytes in [None, Some("{}"), Some("{\"unix_ms\":18446744073709551616}")] {
        let fixture = Fixture::new();
        if let Some(bytes) = bytes {
            fs::write(fixture.0.join("heartbeat.json"), bytes).unwrap();
        }
        assert!(fixture.recover(DAY + 1).report.expiry_degraded.is_none());
    }
}

#[test]
fn catalogue_clock_regression_keeps_precedence_over_the_heartbeat() {
    let fixture = Fixture::new();
    fixture.beat(u64::MAX);
    let chunks = fixture.0.join("chunks");
    fs::create_dir(&chunks).unwrap();
    let mut catalogue = chunks::Catalogue::open(&chunks.join("catalogue.log")).unwrap();
    catalogue
        .append(&chunks::CatRecord {
            at_ms: 1_000_001,
            rec: chunks::Rec::Open {
                seq: 1,
                class: Class::FOREVER,
                stripe: 0,
                policy: String::new(),
                next: 2,
            },
        })
        .unwrap();
    drop(catalogue);
    let recovered = fixture.recover(1_000_000);
    let reason = recovered.report.expiry_degraded.as_deref().unwrap();
    assert!(reason.contains("earlier than the catalogue"), "{reason}");
}

#[test]
fn public_store_recovers_a_future_heartbeat_and_publishes_the_normal_ready_report() {
    let fixture = Fixture::new();
    fixture.beat(u64::MAX);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Store::open(&fixture.0, Fixture::config(1_000_000))
    }));
    assert!(
        result.is_ok(),
        "public startup must not panic on the stored timestamp"
    );
    let store = result.unwrap().unwrap();
    assert!(!store.health2().expiry_degraded);
    let report = fs::read_to_string(fixture.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"ready\""), "{report}");
    assert!(report.contains("\"expiry_degraded\":null"), "{report}");
    assert_eq!(store.epoch, 1);
}
