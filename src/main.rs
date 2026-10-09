use membus::{Config, Store};
#[cfg(windows)]
mod pipe_io;
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
    store: Arc<Store>,
    ingress: Arc<AtomicUsize>,
) -> io::Result<()> {
    let mut hello = false;
    let mut allow_mem = false;
    let mut max_frame = membus::MAX_FRAME;
    loop {
        let mut head = [0u8; 4];
        stream.read_exact(&mut head)?;
        let n = u32::from_le_bytes(head) as usize;
        if n < 13 || n > max_frame {
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
        if !hello && b[4] != membus::HELLO {
            stream.write_all(&membus::reply(
                b[4],
                u64::from_le_bytes(b[5..13].try_into().unwrap()),
                3,
                &[],
            ))?;
            return Ok(());
        }
        if hello && b[4] == membus::HELLO {
            stream.write_all(&membus::reply(
                b[4],
                u64::from_le_bytes(b[5..13].try_into().unwrap()),
                3,
                &[],
            ))?;
            continue;
        }
        let mut response = store.dispatch_for(&b, allow_mem);
        if response.len() - 4 > max_frame {
            response = membus::reply(
                b[4],
                u64::from_le_bytes(b[5..13].try_into().unwrap()),
                4,
                &[],
            );
        }
        let code = u16::from_le_bytes(response[17..19].try_into().unwrap());
        stream.write_all(&response)?;
        if code == 1 {
            return Ok(());
        }
        if !hello {
            if code != 0 {
                return Ok(());
            }
            allow_mem = b[17] != 0;
            max_frame =
                (u32::from_le_bytes(b[13..17].try_into().unwrap()) as usize).min(membus::MAX_FRAME);
            hello = true
        }
    }
}
#[cfg(windows)]
fn pipes(address: &str, store: Arc<Store>, idle_ms: u32) -> io::Result<()> {
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
        let s = store.clone();
        active.fetch_add(1, Ordering::AcqRel);
        let permit = Permit(active.clone());
        let budget = ingress.clone();
        thread::spawn(move || {
            let _permit = permit;
            let _ = serve(f, s, budget);
        });
    }
}
fn tcp(address: &str, store: Arc<Store>, idle_ms: u32) -> io::Result<()> {
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
        let s = store.clone();
        active.fetch_add(1, Ordering::AcqRel);
        let permit = Permit(active.clone());
        let budget = ingress.clone();
        thread::spawn(move || {
            let _permit = permit;
            let _ = serve(stream, s, budget);
        });
    }
    Ok(())
}
fn main() -> io::Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.get(1).map(String::as_str) == Some("health") {
        return health_command(&args[2..]);
    }
    if args.len() < 4 {
        return Err(io::Error::other("usage: membus STATE_DIR tcp|pipe ADDRESS [--idle-ms N] [--heartbeat-ms N] [--state-limit-bytes N] [--flush-ms N] [--retry-ms N] [--quota N] [--index-cap N] [--capacity N] [--queue-bytes N] [--dedup-cap N] [--delay-ms N] [--allow-mem]; membus health PIPE [--state-dir PATH]"));
    }
    let path = PathBuf::from(&args[1]);
    let mut config = Config::default();
    let mut idle_ms = 60000u32;
    let mut heartbeat_ms = 30000u64;
    let mut i = 4;
    while i < args.len() {
        if args[i] == "--allow-mem" {
            config.allow_mem = true;
            i += 1;
            continue;
        }
        let n = args
            .get(i + 1)
            .ok_or_else(|| io::Error::other("missing value"))?
            .parse::<u64>()
            .map_err(io::Error::other)?;
        match args[i].as_str() {
            "--state-limit-bytes" => {
                if n == 0 {
                    return Err(io::Error::other("state-limit-bytes must be positive"));
                }
                config.state_limit_bytes = n;
            }
            "--idle-ms" => {
                idle_ms = u32::try_from(n)
                    .ok()
                    .filter(|n| *n > 0 && *n < u32::MAX)
                    .ok_or_else(|| io::Error::other("idle-ms must be 1..4294967294"))?
            }
            "--heartbeat-ms" => {
                if n != 0 && n < 100 {
                    return Err(io::Error::other("heartbeat-ms must be zero or >=100"));
                }
                heartbeat_ms = n;
            }
            "--flush-ms" => config.flush_ms = n,
            "--retry-ms" => config.retry_ms = n,
            "--quota" => config.quota = n as usize,
            "--index-cap" => config.index_cap = n as usize,
            "--capacity" => config.capacity = n as usize,
            "--queue-bytes" => config.queue_bytes = n as usize,
            "--dedup-cap" => config.dedup_cap = n as usize,
            "--delay-ms" => config.write_delay_ms = n,
            _ => return Err(io::Error::other("unknown option")),
        }
        i += 2
    }
    let state_limit = config.state_limit_bytes;
    let store = Arc::new(Store::open(&path, config)?);
    if heartbeat_ms != 0 {
        start_heartbeat(path.clone(), store.clone(), state_limit, heartbeat_ms);
    }
    eprintln!(
        "ready epoch={} pid={} state={}",
        store.epoch,
        std::process::id(),
        path.display()
    );
    let result = match args[2].as_str() {
        "tcp" => tcp(&args[3], store, idle_ms),
        #[cfg(windows)]
        "pipe" => pipes(&args[3], store, idle_ms),
        _ => Err(io::Error::other("unsupported transport")),
    };
    let _ = fs::remove_file(path.join("owner.lock"));
    result
}

#[cfg(windows)]
fn health_command(args: &[String]) -> io::Result<()> {
    use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
    if args.len() != 1 && !(args.len() == 3 && args[1] == "--state-dir") {
        return Err(io::Error::other(
            "usage: membus health PIPE [--state-dir PATH]",
        ));
    }
    let state = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("D:/projects/membus/state");
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
    stream.write_all(&membus::frame(op, id, payload))?;
    let body = membus::read_frame_limit(stream, 1024)?;
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

fn start_heartbeat(path: PathBuf, store: Arc<Store>, state_limit: u64, interval_ms: u64) {
    thread::spawn(move || {
        let mut writes = 0u64;
        let mut bytes = 0u64;
        let mut attempts = 0u64;
        let mut attempted_bytes = 0u64;
        let mut failures = 0u64;
        loop {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            attempts += 1;
            let state_bytes = store.state_bytes();
            let state_high_water = state_bytes >= state_limit - state_limit / 4;
            let format = |successful_bytes, tried_bytes| {
                format!("{{\"pid\":{},\"epoch\":{},\"unix_ms\":{},\"heartbeat_writes\":{},\"heartbeat_bytes\":{},\"heartbeat_attempts\":{},\"heartbeat_bytes_attempted\":{},\"heartbeat_failures\":{},\"state_bytes\":{},\"state_limit_bytes\":{},\"state_high_water\":{}}}\n", std::process::id(), store.epoch, now, writes + 1, successful_bytes, attempts, tried_bytes, failures,state_bytes,state_limit,state_high_water)
            };
            let mut content = format(0, 0);
            // Byte counter includes the current object; converge across decimal widths.
            loop {
                let next = format(
                    bytes + content.len() as u64,
                    attempted_bytes + content.len() as u64,
                );
                if next.len() == content.len() {
                    content = next;
                    break;
                }
                content = next;
            }
            let temporary = path.join("heartbeat.tmp");
            // Count write attempts too: failed replacements can still write a temp.
            // Attempted bytes are a bound, not a claim of successful/physical I/O.
            attempted_bytes += content.len() as u64;
            let result = fs::write(&temporary, content.as_bytes())
                .and_then(|_| fs::rename(&temporary, path.join("heartbeat.json")));
            if result.is_ok() {
                writes += 1;
                bytes += content.len() as u64;
            } else {
                failures += 1;
            }
            thread::sleep(std::time::Duration::from_millis(interval_ms));
        }
    });
}
