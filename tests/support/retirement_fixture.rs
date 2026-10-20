//! Real MBK1/catalogue inputs for retirement boundary and restart tests.
use super::bus::{
    chunks::{self, CatRecord, Catalogue, Class, Rec},
    *,
};
use std::{fs, path::PathBuf, sync::Arc};

pub struct Fixture {
    pub path: PathBuf,
    pub keys: Vec<String>,
    pub control: String,
    pub clock: Arc<ManualClock>,
    pub config: Config,
}

pub fn path(label: &str) -> PathBuf {
    PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
        .join(format!("{label}-{}-{}", std::process::id(), now_ms()))
}

fn on_stripe_zero(ns: &str, key: &str) -> bool {
    let mut input = ns.as_bytes().to_vec();
    input.push(0);
    input.extend(key.as_bytes());
    crc32(&input).is_multiple_of(4)
}

fn record(at: u64, version: u64, ns: &str, key: &str, id: &str, value: &[u8]) -> Vec<u8> {
    let mut body = at.to_le_bytes().to_vec();
    body.extend(version.to_le_bytes());
    body.extend([1, 0]);
    lp(&mut body, ns.as_bytes());
    lp(&mut body, key.as_bytes());
    lp(&mut body, id.as_bytes());
    body.extend(u64::MAX.to_le_bytes());
    lp(&mut body, value);
    chunks::frame(b"MBK1", &body)
}

impl Fixture {
    pub fn new(label: &str, count: usize) -> Self {
        let f = Self::resume(path(label), count);
        let path = &f.path;
        let keys = &f.keys;
        let control = &f.control;
        let at = f.clock.utc_ms() - 60_000;
        let dir = path.join("chunks");
        fs::create_dir_all(&dir).unwrap();
        let mut records = Vec::new();
        for (seq, class, base, value) in [
            (1, Class::FOREVER, 0, b"old"),
            (2, Class::expire(1000), 10_000, b"new"),
        ] {
            let file = chunks::chunk_path(&dir, class, 0, seq);
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            let mut data = Vec::new();
            for (i, key) in keys.iter().enumerate() {
                data.extend(record(
                    at,
                    base + i as u64 + 1,
                    "aggregate",
                    key,
                    &format!("{seq}-{i}"),
                    value,
                ));
            }
            if seq == 1 {
                data.extend(record(
                    at,
                    19_000,
                    "survivor",
                    control,
                    "control",
                    b"must-survive",
                ));
            }
            assert!(
                data.len() < 16 * 1024 * 1024,
                "the configured chunk ceiling is respected"
            );
            fs::write(&file, &data).unwrap();
            records.push(CatRecord {
                at_ms: at,
                rec: Rec::Open {
                    seq,
                    stripe: 0,
                    class,
                    policy: "historical".into(),
                    next: 20_000,
                },
            });
            records.push(CatRecord {
                at_ms: at,
                rec: Rec::Close {
                    seq,
                    bytes: data.len() as u64,
                    next: 20_000,
                },
            });
        }
        Catalogue::open(&dir.join("catalogue.log"))
            .unwrap()
            .rewrite(&records)
            .unwrap();
        f
    }

    pub fn resume(path: PathBuf, count: usize) -> Self {
        let now = now_ms();
        let mut keys = Vec::new();
        let mut n = 0;
        while keys.len() < count {
            let key = format!("{n:08}{}", "k".repeat(1016));
            n += 1;
            if on_stripe_zero("aggregate", &key) {
                keys.push(key);
            }
        }
        let control = (0..100)
            .map(|n| format!("control{n}"))
            .find(|key| on_stripe_zero("survivor", key))
            .unwrap();
        let clock = ManualClock::new(now);
        let config = Config {
            clock: Some(clock.clone()),
            retry_ms: 0,
            chunk_bytes: 16 * 1024 * 1024,
            quota: 20_000,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            catalogue_rewrite_bytes: 0,
            ..Config::default()
        };
        Self {
            path,
            keys,
            control,
            clock,
            config,
        }
    }

    pub fn open(&self) -> Store {
        Store::open(&self.path, self.config.clone()).unwrap()
    }

    pub fn admit_policy(&self, s: &Store) {
        assert_eq!(
            s.set_policy(
                "aggregate",
                "",
                Some(Class::expire(1000)),
                now_ms() + 30_000
            )
            .code,
            0,
            "the witness configuration must admit its expiry policy"
        );
    }

    pub fn transition(&self, next: u64) -> CatRecord {
        CatRecord {
            at_ms: self.clock.utc_ms(),
            rec: Rec::Retiring {
                seq: 2,
                next,
                horizons: self
                    .keys
                    .iter()
                    .enumerate()
                    .map(|(i, key)| ("aggregate".into(), key.clone(), 10_001 + i as u64))
                    .collect(),
                expired: self
                    .keys
                    .iter()
                    .map(|key| ("aggregate".into(), key.clone()))
                    .collect(),
            },
        }
    }

    pub fn assert_live(&self, s: &Store) {
        for (i, key) in self.keys.iter().enumerate() {
            let value = s
                .get("aggregate", key)
                .expect("partial retirement lost a live value");
            assert_eq!(value.bytes, b"new");
            assert_eq!(value.version, 10_001 + i as u64);
        }
        self.assert_control(s);
    }

    pub fn assert_expired(&self, s: &Store) {
        for key in &self.keys {
            assert!(
                s.get("aggregate", key).is_none(),
                "older forever data resurrected"
            );
            assert_eq!(s.get_status("aggregate", key), (1, 0));
        }
        self.assert_control(s);
    }

    pub fn assert_control(&self, s: &Store) {
        assert_eq!(
            s.get("survivor", &self.control).unwrap().bytes,
            b"must-survive"
        );
    }
}
