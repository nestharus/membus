//! Read-only retirement inspection. See docs/RETIREMENT-INSPECTION.md.
#![deny(clippy::all)]
use membus::chunks::{chunk_path, committed_records, read_catalogue, Rec};
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
    for row in committed_records(records).0 {
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
    if args.len() != 1 {
        return Err(io::Error::other("usage: membus retirement-status PIPE"));
    }
    if !args[0].starts_with(r"\\.\pipe\") || args[0].len() == 9 || args[0].contains('\0') {
        return Err(io::Error::other("explicit local named pipe required"));
    }
    let mut pipe = membus::transport::local_pipe::connect_admin_pipe_checked(&args[0], 2000)?;
    let mut hello = (membus::MAX_FRAME as u32).to_le_bytes().to_vec();
    hello.push(0);
    membus::lp(&mut hello, b"retirement-inspection");
    let hello = super::admin::health_exchange(&mut pipe, membus::HELLO, 1, &hello)?;
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
    let body = super::admin::health_exchange_limit(&mut pipe, membus::HEALTH2, 2, &payload, limit)?;
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
    #[test]
    fn stopped_catalogue_requires_all_retiring_parts() {
        let root = root();
        let path = root.join("chunks/catalogue.log");
        let target = chunk_path(&root.join("chunks"), Class::LATEST, 0, 1);
        let mut cat = Catalogue::open(&path).unwrap();
        for rec in [
            Rec::Open {
                seq: 1,
                stripe: 0,
                class: Class::LATEST,
                policy: String::new(),
                next: 2,
            },
            Rec::Close {
                seq: 1,
                bytes: 10,
                next: 2,
            },
        ] {
            cat.append(&CatRecord { at_ms: 1, rec }).unwrap();
        }
        let part = |index| CatRecord {
            at_ms: 2,
            rec: Rec::RetiringPart {
                seq: 1,
                next: 2,
                index,
                count: 2,
                horizons: vec![],
                expired: vec![],
            },
        };
        cat.append(&part(0)).unwrap();
        assert_eq!(catalogue_state(&root, &target).unwrap(), "Closed");
        cat.append(&CatRecord {
            at_ms: 2,
            rec: Rec::Next(2),
        })
        .unwrap();
        cat.append(&part(1)).unwrap();
        assert_eq!(catalogue_state(&root, &target).unwrap(), "Closed");
        cat.append(&part(0)).unwrap();
        cat.append(&part(1)).unwrap();
        assert_eq!(catalogue_state(&root, &target).unwrap(), "Retiring");
    }

    #[cfg(windows)]
    #[test]
    fn metrics_require_named_unique_fields() {
        let h = membus::Health2 {
            retire_blocked: 1,
            retired_chunks: 2,
            read_only: true,
            expiry_degraded: true,
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
        assert_eq!(
            &values[2..],
            &[
                ("read_only", 1),
                ("storage_error", 0),
                ("expiry_degraded", 1)
            ]
        );
        let mut complete_duplicate = body.clone();
        let count = u16::from_le_bytes(body[..2].try_into().unwrap());
        complete_duplicate[..2].copy_from_slice(&(count + 1).to_le_bytes());
        membus::lp(&mut complete_duplicate, b"retire_blocked=9");
        assert!(retirement_fields(&complete_duplicate).is_err());
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
    #[cfg(windows)]
    #[test]
    fn ordinary_file_is_refused_before_any_hello_write() {
        let root = root();
        let path = root.join("ordinary-file.bin");
        let bytes = b"owned canary must remain exact";
        std::fs::write(&path, bytes).unwrap();
        assert!(retirement_command(&[path.to_string_lossy().into_owned()]).is_err());
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}
