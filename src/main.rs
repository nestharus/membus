use membus::{ResidentDispatch, Store};
mod cli;
mod qualification;
#[cfg(test)]
mod resident_request_tests;
#[cfg(windows)]
use membus::pipe_io;
use std::{
    env, fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
};
struct Permit(Arc<AtomicUsize>);
struct FramePermit(Arc<AtomicUsize>, usize);
impl Drop for FramePermit {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::AcqRel);
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
fn serve<S: Read + Write>(
    mut stream: S,
    resident: ResidentDispatch,
    ingress: Arc<AtomicUsize>,
) -> io::Result<()> {
    let mut connection = membus::ConnectionDispatch::default();
    loop {
        let mut head = [0u8; 4];
        stream.read_exact(&mut head)?;
        let n = u32::from_le_bytes(head) as usize;
        if n < 13 || n > connection.max_frame() {
            stream.write_all(&membus::reply(0, 0, if n < 13 { 3 } else { 4 }, &[]))?;
            return Ok(());
        }
        if ingress
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(n)
                    .filter(|total| *total <= 64 * 1024 * 1024)
            })
            .is_err()
        {
            stream.write_all(&membus::reply(0, 0, 6, &[]))?;
            return Ok(());
        }
        let _frame = FramePermit(ingress.clone(), n);
        let mut b = vec![0; n];
        stream.read_exact(&mut b)?;
        if !resident.dispatch_reply(&mut connection, &b, &mut stream)? {
            return Ok(());
        }
    }
}
#[cfg(windows)]
fn pipes(address: &str, resident: ResidentDispatch, idle_ms: u32) -> io::Result<()> {
    use std::{ffi::c_void, fs::File, os::windows::io::FromRawHandle};
    type Handle = *mut c_void;
    #[link(name = "kernel32")]
    extern "system" {
        fn CreateNamedPipeW(
            n: *const u16,
            mode: u32,
            flags: u32,
            instances: u32,
            out: u32,
            input: u32,
            timeout: u32,
            security: *mut c_void,
        ) -> Handle;
        fn GetCurrentProcess() -> Handle;
        fn CloseHandle(h: Handle) -> i32;
        fn LocalFree(p: *mut c_void) -> *mut c_void;
    }
    #[link(name = "advapi32")]
    extern "system" {
        fn OpenProcessToken(process: Handle, access: u32, token: *mut Handle) -> i32;
        fn GetTokenInformation(
            token: Handle,
            class: u32,
            buffer: *mut c_void,
            len: u32,
            needed: *mut u32,
        ) -> i32;
        fn ConvertSidToStringSidW(sid: *mut c_void, text: *mut *mut u16) -> i32;
        fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
            text: *const u16,
            version: u32,
            descriptor: *mut *mut c_void,
            size: *mut u32,
        ) -> i32;
    }
    #[repr(C)]
    struct SecurityAttributes {
        length: u32,
        descriptor: *mut c_void,
        inherit: i32,
    }
    struct Descriptor(*mut c_void);
    impl Drop for Descriptor {
        fn drop(&mut self) {
            unsafe {
                LocalFree(self.0);
            }
        }
    }
    let mut token = std::ptr::null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), 8, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut needed = 0;
    unsafe {
        GetTokenInformation(token, 1, std::ptr::null_mut(), 0, &mut needed);
    }
    if needed == 0 || needed > 65536 {
        unsafe {
            CloseHandle(token);
        }
        return Err(io::Error::other("invalid token length"));
    }
    let mut user = vec![0u8; needed as usize];
    let ok =
        unsafe { GetTokenInformation(token, 1, user.as_mut_ptr().cast(), needed, &mut needed) };
    let error = io::Error::last_os_error();
    unsafe {
        CloseHandle(token);
    }
    if ok == 0 {
        return Err(error);
    }
    let sid = unsafe { std::ptr::read_unaligned(user.as_ptr().cast::<*mut c_void>()) };
    let mut sid_text = std::ptr::null_mut();
    if unsafe { ConvertSidToStringSidW(sid, &mut sid_text) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut sid_len = 0;
    while unsafe { *sid_text.add(sid_len) } != 0 {
        sid_len += 1;
    }
    let sid_string =
        String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(sid_text, sid_len) });
    unsafe {
        LocalFree(sid_text.cast());
    }
    let sddl: Vec<u16> = format!("D:P(A;;GA;;;{sid_string})(A;;GA;;;SY)")
        .encode_utf16()
        .chain(Some(0))
        .collect();
    let mut descriptor = std::ptr::null_mut();
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            1,
            &mut descriptor,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let _descriptor = Descriptor(descriptor);
    let mut security = SecurityAttributes {
        length: std::mem::size_of::<SecurityAttributes>() as u32,
        descriptor,
        inherit: 0,
    };
    let active = Arc::new(AtomicUsize::new(0));
    let ingress = Arc::new(AtomicUsize::new(0));
    let mut first = true;
    let wide: Vec<u16> = address.encode_utf16().chain(Some(0)).collect();
    loop {
        while active.load(Ordering::Acquire) >= 64 {
            thread::sleep(std::time::Duration::from_millis(5));
        }
        let h = unsafe {
            CreateNamedPipeW(
                wide.as_ptr(),
                if first {
                    3 | 0x80000 | 0x40000000
                } else {
                    3 | 0x40000000
                },
                8,
                255,
                65536,
                65536,
                1000,
                (&mut security as *mut SecurityAttributes).cast(),
            )
        };
        if h as isize == -1 {
            return Err(io::Error::last_os_error());
        }
        first = false;
        let mut f = pipe_io::Pipe::new(unsafe { File::from_raw_handle(h) }, idle_ms);
        if let Err(error) = f.connect() {
            if error.raw_os_error() == Some(232) {
                continue;
            }
            return Err(error);
        }
        let connection = resident.clone();
        active.fetch_add(1, Ordering::AcqRel);
        let permit = Permit(active.clone());
        let budget = ingress.clone();
        thread::spawn(move || {
            let _permit = permit;
            let _ = serve(f, connection, budget);
        });
    }
}
fn tcp(address: &str, resident: ResidentDispatch, idle_ms: u32) -> io::Result<()> {
    let listener = TcpListener::bind(address)?;
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(io::Error::other("TCP must bind loopback"));
    }
    let active = Arc::new(AtomicUsize::new(0));
    let ingress = Arc::new(AtomicUsize::new(0));
    for incoming in listener.incoming() {
        let stream: TcpStream = match incoming {
            Ok(s) => s,
            Err(_) => continue,
        };
        if active.load(Ordering::Acquire) >= 64 {
            drop(stream);
            continue;
        }
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(std::time::Duration::from_millis(idle_ms as u64)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_millis(idle_ms as u64)))?;
        let connection = resident.clone();
        active.fetch_add(1, Ordering::AcqRel);
        let permit = Permit(active.clone());
        let budget = ingress.clone();
        thread::spawn(move || {
            let _permit = permit;
            let _ = serve(stream, connection, budget);
        });
    }
    Ok(())
}
fn main() -> io::Result<()> {
    let args: Vec<_> = env::args().collect();
    cli::dispatch(&args, |command| match command {
        cli::Command::Admin(admin, args) => match admin {
            cli::Admin::Retirement => qualification::retirement_command(args),
            cli::Admin::Catalogue => qualification::catalogue_command(args),
            cli::Admin::Health => health_command(args),
            cli::Admin::Policy => policy_command(args),
            cli::Admin::Expiry => expiry_command(args),
        },
        cli::Command::Server(command) => start_server(command),
    })
}

fn start_server(command: cli::Server) -> io::Result<()> {
    let cli::Server {
        path,
        transport,
        address,
        mut config,
        idle_ms,
        heartbeat_ms,
    } = command;
    // Test seam (T11 on a production copy): the import leaves one key out, so its check refuses the switch.
    if env::var_os("MEMBUS_TEST_IMPORT_DIFFERENCE").is_some() {
        config.import_inject_difference = true;
    }
    // Explicit owned-fixture seams; neither is enabled by an ordinary launch.
    config.fail_after_append = env::var_os("MEMBUS_TEST_FAIL_AFTER_APPEND").is_some();
    config.fail_recovery_sync = env::var_os("MEMBUS_TEST_FAIL_RECOVERY_SYNC").is_some();
    let store = Arc::new(Store::open(&path, config)?);
    // membus.log's one-line replay summary; state/replay.json holds the whole record (CHUNK-RETIREMENT.md 6).
    eprintln!("{}", store.replay_report().summary());
    let workers = store.start_resident_workers(heartbeat_ms)?;
    eprintln!(
        "ready epoch={} pid={} state={}",
        store.epoch,
        std::process::id(),
        path.display()
    );
    let result = match transport {
        cli::Transport::Tcp => tcp(&address, workers.wire_dispatch(&store)?, idle_ms),
        #[cfg(windows)]
        cli::Transport::Pipe => pipes(&address, workers.wire_dispatch(&store)?, idle_ms),
    };
    let settlement = workers.join();
    if let Err(error) = &settlement {
        eprintln!("resident worker settlement: {error}");
    }
    // Keep the original transport result when worker settlement also failed.
    // A remaining connection or failed join leaves the lock artifact in custody.
    #[cfg(not(windows))]
    if settlement.is_ok() && Arc::try_unwrap(store).is_ok() {
        let _ = fs::remove_file(path.join("owner.lock"));
    }
    result.and(settlement)
}

#[cfg(windows)]
fn health_command(args: &[String]) -> io::Result<()> {
    use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
    if !(args.len() == 3 && args[1] == "--state-dir") {
        return Err(io::Error::other(
            "usage: membus health PIPE --state-dir PATH",
        ));
    }
    let state = args[2].as_str();
    let connect_until = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let file = loop {
        match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(0x40000000)
            .open(&args[0])
        {
            Ok(file) => break file,
            Err(error)
                if error.raw_os_error() == Some(231)
                    && std::time::Instant::now() < connect_until =>
            {
                thread::sleep(std::time::Duration::from_millis(5))
            }
            Err(error) => return Err(error),
        }
    };
    #[link(name = "kernel32")]
    extern "system" {
        fn GetNamedPipeServerProcessId(h: *mut std::ffi::c_void, pid: *mut u32) -> i32;
    }
    let mut pid = 0u32;
    if unsafe { GetNamedPipeServerProcessId(file.as_raw_handle(), &mut pid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut pipe = pipe_io::Pipe::new(file, 2000);
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
fn expiry_command(args: &[String]) -> io::Result<()> {
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
fn policy_command(args: &[String]) -> io::Result<()> {
    use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
    let usage = || io::Error::other("usage: membus policy PIPE get NS | set NS PREFIX forever|latest|expire:MS | clear NS PREFIX");
    let (pipe_name, verb) = (args.first().ok_or_else(usage)?, args.get(1).ok_or_else(usage)?.as_str());
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
    let connect_until = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let file = loop {
        match fs::OpenOptions::new().read(true).write(true).custom_flags(0x40000000).open(pipe_name) {
            Ok(file) => break file,
            Err(error) if error.raw_os_error() == Some(231) && std::time::Instant::now() < connect_until => {
                thread::sleep(std::time::Duration::from_millis(5))
            }
            Err(error) => return Err(error),
        }
    };
    #[link(name = "kernel32")]
    extern "system" {
        fn GetNamedPipeServerProcessId(handle: *mut std::ffi::c_void, pid: *mut u32) -> i32;
    }
    let mut pid = 0;
    if unsafe { GetNamedPipeServerProcessId(file.as_raw_handle(), &mut pid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut pipe = pipe_io::Pipe::new(file, 2000);
    let mut payload = (membus::MAX_FRAME as u32).to_le_bytes().to_vec();
    payload.push(0);
    membus::lp(&mut payload, b"membus-policy");
    let hello = health_exchange(&mut pipe, membus::HELLO, 1, &payload)?;
    let negotiated = u32::from_le_bytes(hello.get(8..12)
        .ok_or_else(|| io::Error::other("short policy HELLO"))?.try_into().unwrap()) as usize;
    if !(1024..=membus::MAX_FRAME).contains(&negotiated) {
        return Err(io::Error::other("invalid policy frame limit"));
    }
    let mut payload = (membus::now_ms() + 5000).to_le_bytes().to_vec();
    membus::lp(&mut payload, format!("policy-cli-{}", membus::now_ms()).as_bytes());
    membus::lp(&mut payload, ns.as_bytes());
    match class {
        None => {
            let reply = health_exchange_limit(&mut pipe, membus::POLICY_GET, 2, &payload, negotiated)?;
            let mut c = membus::Cursor::new(&reply);
            let n = c.u16().map_err(|code| io::Error::other(format!("reply code {code}")))?;
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
            c.end().map_err(|code| io::Error::other(format!("reply code {code}")))?;
        }
        Some((kind, retention)) => {
            membus::lp(&mut payload, args[3].as_bytes());
            payload.push(kind);
            payload.extend(retention.to_le_bytes());
            let reply = health_exchange(&mut pipe, membus::POLICY_SET, 2, &payload)?;
            let version = u64::from_le_bytes(reply.get(..8).ok_or_else(|| io::Error::other("short reply"))?.try_into().unwrap());
            println!("policy {verb} {ns} {} version={version}", args[3]);
        }
    }
    Ok(())
}
#[cfg(not(windows))]
fn policy_command(_: &[String]) -> io::Result<()> {
    Err(io::Error::other("membus policy requires a Windows pipe"))
}
#[cfg(not(windows))]
fn health_command(_: &[String]) -> io::Result<()> {
    Err(io::Error::other("resident health requires Windows pipe"))
}
#[cfg(windows)]
fn health_exchange<S: Read + Write>(
    stream: &mut S,
    op: u8,
    id: u64,
    payload: &[u8],
) -> io::Result<Vec<u8>> {
    health_exchange_limit(stream, op, id, payload, 1024)
}
#[cfg(windows)]
fn health_exchange_limit<S: Read + Write>(stream: &mut S, op: u8, id: u64, payload: &[u8], limit: usize) -> io::Result<Vec<u8>> {
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
#[cfg(all(test, windows))]
mod policy_reply_tests {
    use super::*;
    #[test]
    fn policy_get_refuses_ordinary_file_without_writing() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".test-output")
            .join(format!("policy-file-{}-{}", std::process::id(), membus::now_ms()));
        fs::create_dir_all(&root).unwrap();
        let endpoint = root.join("endpoint");
        let canary = vec![0x5a; 2048];
        fs::write(&endpoint, &canary).unwrap();
        let result = policy_command(&[endpoint.to_string_lossy().into_owned(), "get".into(), "example".into()]);
        assert!(result.is_err(), "ordinary files must be refused");
        assert_eq!(fs::read(&endpoint).unwrap(), canary, "refusal must precede every write");
    }
    struct Peer(std::io::Cursor<Vec<u8>>);
    impl Read for Peer { fn read(&mut self,b:&mut[u8])->io::Result<usize>{self.0.read(b)} }
    impl Write for Peer {
        fn write(&mut self,b:&[u8])->io::Result<usize>{Ok(b.len())}
        fn flush(&mut self)->io::Result<()>{Ok(())}
    }
    #[test]
    fn policy_reply_uses_selected_bound_while_health_keeps_its_small_bound() {
        let payload=vec![42;2048];
        let response=membus::reply(membus::POLICY_GET,7,0,&payload);
        let mut peer=Peer(std::io::Cursor::new(response.clone()));
        assert_eq!(health_exchange_limit(&mut peer,membus::POLICY_GET,7,&[],4096).unwrap(),payload);
        let mut peer=Peer(std::io::Cursor::new(response.clone()));
        assert!(health_exchange_limit(&mut peer,membus::POLICY_GET,7,&[],1024).is_err());
        let mut peer=Peer(std::io::Cursor::new(response));
        assert!(health_exchange(&mut peer,membus::POLICY_GET,7,&[]).is_err());
    }
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
