//! Read-only retirement inspection. See docs/RETIREMENT-INSPECTION.md.
#![deny(clippy::all)]
use membus::chunks::{chunk_path, read_catalogue, Rec};
use std::{io, path::Path};

fn catalogue_state(state: &Path, target: &Path) -> io::Result<&'static str> {
    if !state.is_absolute() || !target.is_absolute() {
        return Err(io::Error::other("state and chunk paths must be absolute"));
    }
    let dir = state.join("chunks");
    let path = dir.join("catalogue.log");
    if !path.try_exists()? {
        return Err(io::Error::other("catalogue is missing"));
    }
    let (records, torn) = read_catalogue(&path)?;
    if torn.is_some() {
        return Err(io::Error::other("incomplete catalogue snapshot"));
    }
    let mut selected = None;
    let mut status = "unknown";
    for row in records {
        match row.rec {
            Rec::Open {
                seq, stripe, class, ..
            } if chunk_path(&dir, class, stripe as usize, seq) == target => {
                selected = Some(seq);
                status = "Open";
            }
            Rec::Close { seq, .. } if selected == Some(seq) => status = "Closed",
            Rec::Retiring { seq, .. } if selected == Some(seq) => status = "Retiring",
            Rec::Retired { seq } if selected == Some(seq) => status = "Retired",
            _ => {}
        }
    }
    if selected.is_none() {
        // Compaction can omit discharged chunks. Absence is not Retired proof.
        return Err(io::Error::other(
            "chunk is unknown in this catalogue snapshot",
        ));
    }
    Ok(status)
}
pub fn catalogue_command(args: &[String]) -> io::Result<()> {
    if args.len() != 2 {
        return Err(io::Error::other(
            "usage: membus catalogue-state STOPPED_STATE_DIR ABSOLUTE_CHUNK_PATH",
        ));
    }
    println!(
        "{}",
        catalogue_state(Path::new(&args[0]), Path::new(&args[1]))?
    );
    Ok(())
}

#[cfg(windows)]
fn retirement_fields(body: &[u8]) -> io::Result<Vec<(&'static str, u64)>> {
    let keys = [
        "retire_blocked",
        "retired_chunks",
        "read_only",
        "storage_error",
        "expiry_degraded",
    ];
    let mut values = [None; 5];
    let mut c = membus::Cursor::new(body);
    let invalid = |code| io::Error::other(format!("invalid HEALTH2 payload: {code}"));
    let count = c.u16().map_err(invalid)?;
    for _ in 0..count {
        let bytes = c.bytes(membus::MAX_FRAME).map_err(invalid)?;
        let field = std::str::from_utf8(bytes).map_err(io::Error::other)?;
        if let Some((key, value)) = field.split_once('=') {
            if let Some(i) = keys.iter().position(|k| *k == key) {
                if values[i].is_some() {
                    return Err(io::Error::other("duplicate retirement metric"));
                }
                values[i] = Some(if i < 2 {
                    value.parse::<u64>().map_err(io::Error::other)?
                } else {
                    match value {
                        "true" => 1,
                        "false" => 0,
                        _ => return Err(io::Error::other("invalid boolean retirement metric")),
                    }
                });
            }
        }
    }
    c.end().map_err(invalid)?;
    keys.into_iter()
        .zip(values)
        .map(|(key, value)| {
            value
                .map(|n| (key, n))
                .ok_or_else(|| io::Error::other("missing retirement metric"))
        })
        .collect()
}
#[cfg(windows)]
pub fn retirement_command(args: &[String]) -> io::Result<()> {
    use std::{
        fs,
        os::windows::fs::OpenOptionsExt,
        time::{Duration, Instant},
    };
    if args.len() != 1 {
        return Err(io::Error::other("usage: membus retirement-status PIPE"));
    }
    let until = Instant::now() + Duration::from_secs(2);
    let file = loop {
        match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(0x40000000)
            .open(&args[0])
        {
            Ok(file) => break file,
            Err(e) if e.raw_os_error() == Some(231) && Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(5))
            }
            Err(e) => return Err(e),
        }
    };
    let mut pipe = membus::pipe_io::Pipe::new(file, 2000);
    let mut hello = (membus::MAX_FRAME as u32).to_le_bytes().to_vec();
    hello.push(0);
    membus::lp(&mut hello, b"retirement-inspection");
    let hello = super::health_exchange(&mut pipe, membus::HELLO, 1, &hello)?;
    let limit = u32::from_le_bytes(
        hello
            .get(8..12)
            .ok_or_else(|| io::Error::other("short HELLO"))?
            .try_into()
            .unwrap(),
    ) as usize;
    if !(1024..=membus::MAX_FRAME).contains(&limit) {
        return Err(io::Error::other("invalid negotiated frame bound"));
    }
    let mut payload = (membus::now_ms() + 2000).to_le_bytes().to_vec();
    membus::lp(&mut payload, b"retirement-inspection");
    membus::lp(&mut payload, b"ledger");
    let body = super::health_exchange_limit(&mut pipe, membus::HEALTH2, 2, &payload, limit)?;
    // Emit only known numeric fields; namespace text cannot inject metric lines.
    for (key, value) in retirement_fields(&body)? {
        println!("{key}={value}");
    }
    Ok(())
}
#[cfg(not(windows))]
pub fn retirement_command(_: &[String]) -> io::Result<()> {
    Err(io::Error::other("retirement-status requires Windows pipes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use membus::chunks::{CatRecord, Catalogue, Class};
    fn root() -> std::path::PathBuf {
        let p = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("var")
            .join(format!(
                "catalogue-inspection-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
        std::fs::create_dir_all(p.join("chunks")).unwrap();
        p
    }
    #[test]
    fn stopped_catalogue_transitions_refuse_unknown_and_unresolved() {
        let root = root();
        let path = root.join("chunks/catalogue.log");
        let target = chunk_path(&root.join("chunks"), Class::LATEST, 0, 1);
        assert!(catalogue_state(&root, &target).is_err());
        for (rec, expected) in [
            (
                Rec::Open {
                    seq: 1,
                    stripe: 0,
                    class: Class::LATEST,
                    policy: String::new(),
                    next: 2,
                },
                "Open",
            ),
            (
                Rec::Close {
                    seq: 1,
                    bytes: 10,
                    next: 2,
                },
                "Closed",
            ),
            (
                Rec::Retiring {
                    seq: 1,
                    next: 2,
                    horizons: vec![],
                    expired: vec![],
                },
                "Retiring",
            ),
            (Rec::Retired { seq: 1 }, "Retired"),
        ] {
            let mut cat = Catalogue::open(&path).unwrap();
            cat.append(&CatRecord { at_ms: 1, rec }).unwrap();
            drop(cat);
            assert_eq!(catalogue_state(&root, &target).unwrap(), expected);
        }
        assert!(catalogue_state(&root, &root.join("chunks/missing.log")).is_err());
        std::fs::write(path.with_extension("log.append-pending"), b"unresolved").unwrap();
        assert!(catalogue_state(&root, &target).is_err());
    }
    #[test]
    fn torn_catalogue_never_reports_terminal_snapshot() {
        use std::io::Write;
        let root = root();
        let path = root.join("chunks/catalogue.log");
        let target = chunk_path(&root.join("chunks"), Class::LATEST, 0, 1);
        let mut cat = Catalogue::open(&path).unwrap();
        cat.append(&CatRecord {
            at_ms: 1,
            rec: Rec::Open {
                seq: 1,
                stripe: 0,
                class: Class::LATEST,
                policy: String::new(),
                next: 2,
            },
        })
        .unwrap();
        drop(cat);
        std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(b"MBC")
            .unwrap();
        assert!(catalogue_state(&root, &target).is_err());
    }
    #[cfg(windows)]
    #[test]
    fn metrics_require_named_unique_fields() {
        let h = membus::Health2 {
            retire_blocked: 1,
            retired_chunks: 2,
            ..Default::default()
        };
        let fields = h.fields();
        let mut body = (fields.len() as u16).to_le_bytes().to_vec();
        for (k, v) in fields {
            membus::lp(&mut body, format!("{k}={v}").as_bytes());
        }
        let values = retirement_fields(&body).unwrap();
        assert_eq!(values[0], ("retire_blocked", 1));
        assert_eq!(values[1], ("retired_chunks", 2));
        assert!(retirement_fields(&0u16.to_le_bytes()).is_err());
        for fields in [
            vec!["retire_blocked=1", "retire_blocked=2"],
            vec!["retire_blocked=not-a-number"],
        ] {
            let mut body = (fields.len() as u16).to_le_bytes().to_vec();
            for f in fields {
                membus::lp(&mut body, f.as_bytes());
            }
            assert!(retirement_fields(&body).is_err());
        }
        body.push(0);
        assert!(retirement_fields(&body).is_err());
    }
}
