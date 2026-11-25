//! Actual resident qualification of the declared indexed collection.
#![cfg(windows)]
use membus::{
    client::*,
    indexed::{Collection, Error as IndexError, Limits, Query, Record, Schema},
};
#[path = "support/child.rs"]
mod child;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn d() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 30_000,
        local: Instant::now() + Duration::from_secs(30),
    }
}
struct Fixture {
    root: PathBuf,
    pipe: String,
    owner: child::Owned,
}
fn fixture_names(stamp: u128) -> (PathBuf, String) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let identity = format!("{stamp}-{}-{}", std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    let root = PathBuf::from(
        std::env::var_os("MEMBUS_INDEXED_TEST_ROOT")
            .unwrap_or_else(|| "target/indexed-fixtures".into()),
    ).join(&identity);
    let pipe = format!(r"\\.\pipe\membus-indexed-{identity}");
    (root, pipe)
}
#[test]
fn equal_timestamp_fixtures_have_distinct_paths_and_endpoints() {
    let first = fixture_names(7);
    let second = fixture_names(7);
    assert_ne!(first.0, second.0, "equal wall timestamps must not share a state directory");
    assert_ne!(first.1, second.1, "equal wall timestamps must not share a pipe endpoint");
}
impl Fixture {
    fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let (root, pipe) = fixture_names(stamp);
        fs::create_dir_all(&root).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_membus"));
        cmd.arg(root.join("state"))
            .args([
                "pipe",
                &pipe,
                "--heartbeat-ms",
                "0",
                "--flush-ms",
                "0",
                "--snapshot-ttl-ms",
                "120000",
            ])
            .stdin(Stdio::null())
            .stdout(fs::File::create(root.join("stdout")).unwrap())
            .stderr(fs::File::create(root.join("stderr")).unwrap());
        let owner = child::Owned::spawn(&mut cmd).unwrap();
        Self { root, pipe, owner }
    }
    fn client(&self, identity: &str) -> Client<membus::pipe_io::Pipe> {
        let mut options = Options::new(identity, membus::MAX_FRAME);
        options.scans = true;
        Client::connect_pipe(Path::new(&self.pipe), options, d().local).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let (status, stopped) = self.owner.wait(Duration::ZERO).unwrap();
        println!(
            "indexed fixture {} child waited={status} controlled_stop={stopped}",
            self.root.display()
        );
    }
}
fn schema() -> Schema {
    Schema {
        fields: vec!["id".into(), "source".into(), "revises".into()],
    }
}
fn row(n: u64, id: &str, source: &str, links: &[&str]) -> Record {
    Record {
        ordinal: n,
        fields: vec![
            vec![id.as_bytes().to_vec()],
            vec![source.as_bytes().to_vec()],
            links.iter().map(|v| v.as_bytes().to_vec()).collect(),
        ],
        payload: format!(
            r#"{{"seq":{n},"id":"{id}","source":"{source}","preserved":9007199254740993}}"#
        )
        .into_bytes(),
    }
}
fn limits() -> Limits {
    Limits {
        max_rows: 100,
        max_bytes: 4 * 1024 * 1024,
    }
}
fn write(c: &mut Client<membus::pipe_io::Pipe>, key: &str, bytes: Vec<u8>, op: &str) {
    let mut a = c
        .prepare(MutationRequest {
            operation_id: op.into(),
            namespace: "fixture".into(),
            key: key.into(),
            value: bytes,
            ack: Ack::Os,
            expected: None,
        })
        .unwrap();
    c.mutate(&mut a, d()).unwrap();
}
#[test]
fn zero_encoded_budget_validates_empty_and_refuses_required_or_corrupt_records() {
    let f = Fixture::new();
    let mut c = f.client("zero-budget");
    let col = Collection::new("empty", schema()).unwrap();
    col.declare(&mut c, "fixture", "empty-schema", Ack::Os, d())
        .unwrap();
    let zero = Limits {
        max_rows: 1,
        max_bytes: 0,
    };
    let token = c.snapshot_open("fixture", "empty-open", d()).unwrap();
    assert_eq!(
        col.query(
            &mut c,
            "fixture",
            &token,
            &Query::All,
            zero,
            "empty-read",
            d()
        )
        .unwrap()
        .through,
        0
    );
    assert!(col
        .query(
            &mut c,
            "fixture",
            &token,
            &Query::All,
            Limits {
                max_rows: 0,
                max_bytes: 0
            },
            "invalid-count",
            d()
        )
        .is_err());
    c.snapshot_release("fixture", &token, "empty-release", d())
        .unwrap();
    col.append(
        &mut c,
        "fixture",
        &row(1, "one", "source", &[]),
        "one",
        Ack::Os,
        d(),
    )
    .unwrap();
    let token = c.snapshot_open("fixture", "one-open", d()).unwrap();
    assert!(matches!(
        col.query(
            &mut c,
            "fixture",
            &token,
            &Query::All,
            zero,
            "one-read",
            d()
        ),
        Err(IndexError::Limit)
    ));
    c.snapshot_release("fixture", &token, "one-release", d())
        .unwrap();
    write(
        &mut c,
        "x/empty/head",
        b"corrupt-boundary".to_vec(),
        "corrupt-empty-head",
    );
    let token = c.snapshot_open("fixture", "bad-open", d()).unwrap();
    assert!(matches!(
        col.query(
            &mut c,
            "fixture",
            &token,
            &Query::All,
            zero,
            "bad-read",
            d()
        ),
        Err(IndexError::Corrupt(_))
    ));
    c.snapshot_release("fixture", &token, "bad-release", d())
        .unwrap();
}
#[test]
fn actual_declared_queries_preserve_identity_history_and_pinned_boundary() {
    let f = Fixture::new();
    let mut c = f.client("indexed-native");
    let col = Collection::new("rows", schema()).unwrap();
    let a = row(1, "old", "same", &[]);
    let b = row(2, "new", "same", &["old", "old"]);
    let z = row(3, "other", "else", &["old"]);
    col.append(&mut c, "fixture", &a, "append1", Ack::Os, d())
        .unwrap();
    let pinned = c.snapshot_open("fixture", "pinned", d()).unwrap();
    col.append(&mut c, "fixture", &b, "append2", Ack::Os, d())
        .unwrap();
    col.append(&mut c, "fixture", &z, "append3", Ack::Os, d())
        .unwrap();
    let old = col
        .query(
            &mut c,
            "fixture",
            &pinned,
            &Query::All,
            limits(),
            "old-read",
            d(),
        )
        .unwrap();
    assert_eq!(old.through, 1);
    assert_eq!(old.records, vec![a.clone()]);
    c.snapshot_release("fixture", &pinned, "release-old", d())
        .unwrap();
    let token = c.snapshot_open("fixture", "current", d()).unwrap();
    let all = col
        .query(&mut c, "fixture", &token, &Query::All, limits(), "all", d())
        .unwrap();
    assert_eq!(all.records, vec![a.clone(), b.clone(), z.clone()]);
    let linked = col
        .query(
            &mut c,
            "fixture",
            &token,
            &Query::Equal {
                field: "revises".into(),
                value: b"old".to_vec(),
            },
            limits(),
            "links",
            d(),
        )
        .unwrap();
    assert_eq!(linked.records, vec![b.clone(), z]);
    assert_eq!(
        col.query(
            &mut c,
            "fixture",
            &token,
            &Query::Ordinals { first: 2, last: 2 },
            limits(),
            "one",
            d()
        )
        .unwrap()
        .records,
        vec![b]
    );
    assert!(matches!(
        col.query(
            &mut c,
            "fixture",
            &token,
            &Query::All,
            Limits {
                max_rows: 2,
                max_bytes: 100000
            },
            "limit",
            d()
        ),
        Err(IndexError::Limit)
    ));
    assert!(matches!(
        col.query(
            &mut c,
            "fixture",
            &token,
            &Query::All,
            Limits {
                max_rows: 100,
                max_bytes: 1
            },
            "bytes",
            d()
        ),
        Err(IndexError::Limit)
    ));
    assert!(matches!(
        col.query(
            &mut c,
            "fixture",
            &token,
            &Query::Equal {
                field: "undeclared".into(),
                value: vec![]
            },
            limits(),
            "unknown",
            d()
        ),
        Err(IndexError::Invalid(_))
    ));
    c.snapshot_release("fixture", &token, "release", d())
        .unwrap();
    let before = c
        .get("fixture", "x/rows/r/00000000000000000001", "before", d())
        .unwrap()
        .unwrap();
    assert!(
        col.append(&mut c, "fixture", &a, "explicit-reconcile", Ack::Os, d())
            .unwrap()
            .already_committed
    );
    assert_eq!(
        c.get("fixture", "x/rows/r/00000000000000000001", "after", d())
            .unwrap()
            .unwrap(),
        before
    );
    assert!(matches!(
        col.append(
            &mut c,
            "fixture",
            &row(1, "different", "same", &[]),
            "collision",
            Ack::Os,
            d()
        ),
        Err(IndexError::Conflict(_))
    ));
    assert!(matches!(
        col.append(
            &mut c,
            "fixture",
            &row(5, "gap", "same", &[]),
            "gap",
            Ack::Os,
            d()
        ),
        Err(IndexError::Conflict(_))
    ));
    assert!(Collection::new(
        "rows",
        Schema {
            fields: vec!["different".into()]
        }
    )
    .unwrap()
    .declare(&mut c, "fixture", "wrong-schema", Ack::Os, d())
    .is_err());
}
#[test]
fn cached_schema_still_checks_each_snapshot_and_namespace() {
    let f = Fixture::new();
    let mut c = f.client("schema-cache");
    let col = Collection::new("rows", schema()).unwrap();
    col.declare(&mut c, "fixture", "declare", Ack::Os, d())
        .unwrap();
    let old = c.snapshot_open("fixture", "old", d()).unwrap();
    col.query(
        &mut c, "fixture", &old, &Query::All, limits(), "prime", d(),
    )
    .unwrap();
    let copy = col.clone();
    assert_eq!(copy.schema(), col.schema());
    assert_eq!(format!("{copy:?}"), format!("{col:?}"));
    drop(col);

    // A raw replacement affects new snapshots, while the pinned view keeps
    // its original declaration. Reusing the collection must check both views.
    write(
        &mut c,
        "x/rows/schema",
        b"different declaration".to_vec(),
        "replace",
    );
    let new = c.snapshot_open("fixture", "new", d()).unwrap();
    let other = Collection::new(
        "rows",
        Schema { fields: vec!["different".into()] },
    )
    .unwrap();
    other.declare(&mut c, "other", "other-declare", Ack::Os, d())
        .unwrap();
    let other_view = c.snapshot_open("other", "other-open", d()).unwrap();

    for (ns, token, valid) in [
        ("fixture", old, true),
        ("fixture", new, false),
        ("other", other_view, false),
    ] {
        let result = copy.query(&mut c, ns, &token, &Query::All, limits(), "read", d());
        if valid {
            assert!(result.unwrap().records.is_empty());
        } else {
            assert!(matches!(
                result,
                Err(IndexError::Conflict("schema differs"))
            ));
        }
        c.snapshot_release(ns, &token, "release", d()).unwrap();
    }
}
#[test]
fn interrupted_publication_never_exposes_staged_row_and_wrong_writer_cannot_post() {
    let f = Fixture::new();
    let mut c = f.client("interrupted");
    let col = Collection::new("rows", schema()).unwrap();
    col.append(
        &mut c,
        "fixture",
        &row(1, "first", "source", &[]),
        "first",
        Ack::Os,
        d(),
    )
    .unwrap();
    let target = row(2, "reserved", "source", &["first"]);
    let index = format!(
        "x/rows/i/00/{:08x}/00000000000000000002",
        membus::crc32(b"reserved")
    );
    // A real definitive write conflict after row reservation stops publication.
    write(
        &mut c,
        &index,
        b"conflicting immutable posting".to_vec(),
        "bad-posting",
    );
    assert!(matches!(
        col.append(&mut c, "fixture", &target, "interrupted", Ack::Os, d()),
        Err(IndexError::Conflict(_))
    ));
    assert!(c
        .get("fixture", "x/rows/r/00000000000000000002", "reserved", d())
        .unwrap()
        .is_some());
    assert!(col
        .append(
            &mut c,
            "fixture",
            &row(2, "loser", "other", &[]),
            "loser",
            Ack::Os,
            d()
        )
        .is_err());
    let loser = format!(
        "x/rows/i/00/{:08x}/00000000000000000002",
        membus::crc32(b"loser")
    );
    assert!(c
        .get("fixture", &loser, "no-loser-posting", d())
        .unwrap()
        .is_none());
    let t = c.snapshot_open("fixture", "interrupted-view", d()).unwrap();
    let view = col
        .query(
            &mut c,
            "fixture",
            &t,
            &Query::All,
            limits(),
            "committed-only",
            d(),
        )
        .unwrap();
    assert_eq!(view.through, 1);
    assert_eq!(view.records.len(), 1);
    c.snapshot_release("fixture", &t, "done", d()).unwrap();
    // Removing a committed row through an unsupported raw writer is corruption,
    // not a successful empty or partial account.
    write(
        &mut c,
        "x/rows/r/00000000000000000001",
        b"broken".to_vec(),
        "corrupt",
    );
    let t = c.snapshot_open("fixture", "corrupt-view", d()).unwrap();
    assert!(matches!(
        col.query(
            &mut c,
            "fixture",
            &t,
            &Query::All,
            limits(),
            "must-refuse",
            d()
        ),
        Err(IndexError::Corrupt(_))
    ));
    c.snapshot_release("fixture", &t, "done-corrupt", d())
        .unwrap();
}
#[test]
fn crc_collision_is_only_a_candidate_bucket_and_exact_values_decide() {
    assert_eq!(membus::crc32(b"plumless"), membus::crc32(b"buckeroo"));
    let f = Fixture::new();
    let mut c = f.client("collision-control");
    let col = Collection::new("rows", schema()).unwrap();
    let a = row(1, "a", "plumless", &[]);
    let b = row(2, "b", "buckeroo", &[]);
    col.append(&mut c, "fixture", &a, "a", Ack::Os, d())
        .unwrap();
    col.append(&mut c, "fixture", &b, "b", Ack::Os, d())
        .unwrap();
    let t = c.snapshot_open("fixture", "capture", d()).unwrap();
    assert_eq!(
        col.query(
            &mut c,
            "fixture",
            &t,
            &Query::Equal {
                field: "source".into(),
                value: b"plumless".to_vec()
            },
            limits(),
            "equal-a",
            d()
        )
        .unwrap()
        .records,
        vec![a]
    );
    assert_eq!(
        col.query(
            &mut c,
            "fixture",
            &t,
            &Query::Equal {
                field: "source".into(),
                value: b"buckeroo".to_vec()
            },
            limits(),
            "equal-b",
            d()
        )
        .unwrap()
        .records,
        vec![b]
    );
    c.snapshot_release("fixture", &t, "release", d()).unwrap();
}

#[test]
fn append_flush_conflicts_when_competing_head_uses_ack_os() {
    let f = Fixture::new();
    let mut c = f.client("indexed-ack-conflict");
    let col = Collection::new("ackcheck", schema()).unwrap();
    let a = row(1, "one", "source", &[]);
    // Establish flushed declaration and row 1, then simulate a competing Ack::Os head commit.
    col.append(&mut c, "fixture", &a, "first-flush", Ack::Flush, d())
        .unwrap();
    let head_bytes = c
        .get("fixture", "x/ackcheck/head", "read-head", d())
        .unwrap()
        .unwrap()
        .bytes;
    write(&mut c, "x/ackcheck/head", head_bytes, "competing-os-head");
    // Caller requesting Ack::Flush for ordinal 1 must not receive an ok flushed receipt.
    let err = col
        .append(&mut c, "fixture", &a, "strict-flush", Ack::Flush, d())
        .unwrap_err();
    assert!(
        matches!(err, IndexError::Conflict("head acknowledgment differs")),
        "expected Conflict(head acknowledgment differs), got {err:?}"
    );
}
