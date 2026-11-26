//! Administrative CLI commands: resident health, policy inspection/updates, and expiry resume.
use std::{
    fs,
    io::{self, Read, Write},
    path::PathBuf,
};

#[cfg(windows)]
pub(super) fn health_command(args: &[String]) -> io::Result<()> {
    if !(args.len() == 3 && args[1] == "--state-dir") {
        return Err(io::Error::other(
            "usage: membus health PIPE --state-dir PATH",
        ));
    }
    let state = args[2].as_str();
    let (mut pipe, pid) =
        membus::transport::local_pipe::connect_admin_pipe_with_pid(&args[0], 2000)?;
    let mut payload = (membus::MAX_FRAME as u32).to_le_bytes().to_vec();
    payload.push(0);
    membus::lp(&mut payload, b"resident-health");
    let hello = health_exchange(&mut pipe, membus::HELLO, 1, &payload)?;
    if hello.len() < 8 {
        return Err(io::Error::other("truncated hello"));
    }
    let epoch = u64::from_le_bytes(hello[..8].try_into().unwrap());
    let mut payload = (membus::now_ms() + 2000).to_le_bytes().to_vec();
    membus::lp(&mut payload, b"resident-health");
    membus::lp(&mut payload, b"ledger");
    let reply = health_exchange(&mut pipe, membus::HEALTH, 2, &payload)?;
    if reply.len() != 50 {
        return Err(io::Error::other("unexpected health shape"));
    }
    let path = PathBuf::from(state).join("heartbeat.json");
    let size = fs::metadata(&path)?.len();
    if size > 1024 {
        return Err(io::Error::other("oversize heartbeat"));
    }
    let heartbeat = fs::read_to_string(path)?;
    let heartbeat_pid = heartbeat_number(&heartbeat, "pid")?;
    let heartbeat_epoch = heartbeat_number(&heartbeat, "epoch")?;
    let stamp = heartbeat_number(&heartbeat, "unix_ms")?;
    let now = membus::now_ms();
    if heartbeat_pid != pid as u64
        || heartbeat_epoch != epoch
        || stamp > now
        || now - stamp >= 90000
    {
        return Err(io::Error::other(
            "heartbeat pid/epoch/freshness does not match pipe",
        ));
    }
    println!("healthy pid={pid} epoch={epoch}");
    Ok(())
}

/// `membus expiry resume STATE_DIR`: end a clock pause (5.4); the running server's next tick reads the file.
pub(super) fn expiry_command(args: &[String]) -> io::Result<()> {
    if args.len() != 2 || args[0] != "resume" {
        return Err(io::Error::other("usage: membus expiry resume STATE_DIR"));
    }
    let file = PathBuf::from(&args[1]).join("expiry.resume");
    fs::write(&file, membus::now_ms().to_string())?;
    println!("expiry resume requested: {}", file.display());
    Ok(())
}

/// `membus policy PIPE get NS | set NS PREFIX CLASS | clear NS PREFIX` (5.1), over POLICY_GET and POLICY_SET.
#[cfg(windows)]
pub(super) fn policy_command(args: &[String]) -> io::Result<()> {
    let usage = || {
        io::Error::other(
            "usage: membus policy PIPE get NS | set NS PREFIX forever|latest|expire:MS | clear NS PREFIX",
        )
    };
    let (pipe_name, verb) = (
        args.first().ok_or_else(usage)?,
        args.get(1).ok_or_else(usage)?.as_str(),
    );
    let ns = args.get(2).ok_or_else(usage)?;
    let class = match (verb, args.len()) {
        ("get", 3) => None,
        ("clear", 4) => Some((255u8, 0u64)),
        ("set", 5) => Some(match args[4].as_str() {
            "forever" => (0, 0),
            "latest" => (1, 0),
            other => {
                let ms = other.strip_prefix("expire:").ok_or_else(usage)?;
                (2, ms.parse::<u64>().map_err(io::Error::other)?)
            }
        }),
        _ => return Err(usage()),
    };
    let (mut pipe, _pid) =
        membus::transport::local_pipe::connect_admin_pipe_with_pid(pipe_name, 2000)?;
    let mut payload = (membus::MAX_FRAME as u32).to_le_bytes().to_vec();
    payload.push(0);
    membus::lp(&mut payload, b"membus-policy");
    let hello = health_exchange(&mut pipe, membus::HELLO, 1, &payload)?;
    let negotiated = u32::from_le_bytes(
        hello
            .get(8..12)
            .ok_or_else(|| io::Error::other("short policy HELLO"))?
            .try_into()
            .unwrap(),
    ) as usize;
    if !(1024..=membus::MAX_FRAME).contains(&negotiated) {
        return Err(io::Error::other("invalid policy frame limit"));
    }
    let mut payload = (membus::now_ms() + 5000).to_le_bytes().to_vec();
    membus::lp(
        &mut payload,
        format!("policy-cli-{}", membus::now_ms()).as_bytes(),
    );
    membus::lp(&mut payload, ns.as_bytes());
    match class {
        None => {
            let reply =
                health_exchange_limit(&mut pipe, membus::POLICY_GET, 2, &payload, negotiated)?;
            let mut c = membus::Cursor::new(&reply);
            let n = c
                .u16()
                .map_err(|code| io::Error::other(format!("reply code {code}")))?;
            for _ in 0..n {
                let row = (|| -> Result<_, u16> { Ok((c.bound()?, c.u8()?, c.u64()?)) })()
                    .map_err(|code| io::Error::other(format!("reply code {code}")))?;
                let class = match row.1 {
                    0 => "forever".to_owned(),
                    1 => "latest".to_owned(),
                    _ => format!("expire:{}", row.2),
                };
                println!("{ns}\t{}\t{class}", row.0);
            }
            c.end()
                .map_err(|code| io::Error::other(format!("reply code {code}")))?;
        }
        Some((kind, retention)) => {
            membus::lp(&mut payload, args[3].as_bytes());
            payload.push(kind);
            payload.extend(retention.to_le_bytes());
            let reply = health_exchange(&mut pipe, membus::POLICY_SET, 2, &payload)?;
            let version = u64::from_le_bytes(
                reply
                    .get(..8)
                    .ok_or_else(|| io::Error::other("short reply"))?
                    .try_into()
                    .unwrap(),
            );
            println!("policy {verb} {ns} {} version={version}", args[3]);
        }
    }
    Ok(())
}

#[cfg(not(windows))]
pub(super) fn policy_command(_: &[String]) -> io::Result<()> {
    Err(io::Error::other("membus policy requires a Windows pipe"))
}

#[cfg(not(windows))]
pub(super) fn health_command(_: &[String]) -> io::Result<()> {
    Err(io::Error::other("resident health requires Windows pipe"))
}

#[cfg(windows)]
pub(super) fn health_exchange<S: Read + Write>(
    stream: &mut S,
    op: u8,
    id: u64,
    payload: &[u8],
) -> io::Result<Vec<u8>> {
    health_exchange_limit(stream, op, id, payload, 1024)
}

#[cfg(windows)]
pub(super) fn health_exchange_limit<S: Read + Write>(
    stream: &mut S,
    op: u8,
    id: u64,
    payload: &[u8],
    limit: usize,
) -> io::Result<Vec<u8>> {
    stream.write_all(&membus::frame(op, id, payload))?;
    let body = membus::read_frame_limit(stream, limit)?;
    if body.len() < 15
        || body[..2] != 1u16.to_le_bytes()
        || body[2..4] != 0u16.to_le_bytes()
        || body[4] != op | 128
        || body[5..13] != id.to_le_bytes()
        || body[13..15] != [0, 0]
    {
        return Err(io::Error::other("health protocol association/refusal"));
    }
    Ok(body[15..].to_vec())
}

#[cfg(windows)]
fn heartbeat_number(text: &str, key: &str) -> io::Result<u64> {
    let prefix = format!("\"{key}\":");
    let tail = text
        .split_once(&prefix)
        .ok_or_else(|| io::Error::other("missing heartbeat field"))?
        .1;
    let digits: String = tail.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().map_err(io::Error::other)
}

#[cfg(all(test, windows))]
mod policy_reply_tests {
    use super::*;
    #[test]
    fn policy_get_refuses_ordinary_file_without_writing() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".test-output")
            .join(format!(
                "policy-file-{}-{}",
                std::process::id(),
                membus::now_ms()
            ));
        fs::create_dir_all(&root).unwrap();
        let endpoint = root.join("endpoint");
        let canary = vec![0x5a; 2048];
        fs::write(&endpoint, &canary).unwrap();
        let result = policy_command(&[
            endpoint.to_string_lossy().into_owned(),
            "get".into(),
            "example".into(),
        ]);
        assert!(result.is_err(), "ordinary files must be refused");
        assert_eq!(
            fs::read(&endpoint).unwrap(),
            canary,
            "refusal must precede every write"
        );
    }
    struct Peer(std::io::Cursor<Vec<u8>>);
    impl Read for Peer {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.0.read(b)
        }
    }
    impl Write for Peer {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn policy_reply_uses_selected_bound_while_health_keeps_its_small_bound() {
        let payload = vec![42; 2048];
        let response = membus::reply(membus::POLICY_GET, 7, 0, &payload);
        let mut peer = Peer(std::io::Cursor::new(response.clone()));
        assert_eq!(
            health_exchange_limit(&mut peer, membus::POLICY_GET, 7, &[], 4096).unwrap(),
            payload
        );
        let mut peer = Peer(std::io::Cursor::new(response.clone()));
        assert!(health_exchange_limit(&mut peer, membus::POLICY_GET, 7, &[], 1024).is_err());
        let mut peer = Peer(std::io::Cursor::new(response));
        assert!(health_exchange(&mut peer, membus::POLICY_GET, 7, &[]).is_err());
    }
}
