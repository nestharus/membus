use membus::{Config, Store};
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
fn pipes(address: &str, store: Arc<Store>) -> io::Result<()> {
    use std::{
        ffi::c_void,
        fs::File,
        os::windows::io::{AsRawHandle, FromRawHandle},
    };
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
        fn ConnectNamedPipe(h: Handle, o: *mut c_void) -> i32;
        fn GetLastError() -> u32;
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
                if first { 3 | 0x80000 } else { 3 },
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
        let f = unsafe { File::from_raw_handle(h) };
        if unsafe { ConnectNamedPipe(f.as_raw_handle(), std::ptr::null_mut()) } == 0 {
            let error = unsafe { GetLastError() };
            if error == 232 {
                continue;
            }
            if error != 535 {
                return Err(io::Error::last_os_error());
            }
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
fn tcp(address: &str, store: Arc<Store>) -> io::Result<()> {
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
        stream.set_read_timeout(Some(std::time::Duration::from_secs(60)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_secs(60)))?;
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
    if args.len() < 4 {
        return Err(io::Error::other("usage: membus STATE_DIR tcp|pipe ADDRESS [--flush-ms N] [--retry-ms N] [--quota N] [--index-cap N] [--capacity N] [--queue-bytes N] [--delay-ms N] [--allow-mem]"));
    }
    let path = PathBuf::from(&args[1]);
    let mut config = Config::default();
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
    let store = Arc::new(Store::open(&path, config)?);
    eprintln!(
        "ready epoch={} pid={} state={}",
        store.epoch,
        std::process::id(),
        path.display()
    );
    let result = match args[2].as_str() {
        "tcp" => tcp(&args[3], store),
        #[cfg(windows)]
        "pipe" => pipes(&args[3], store),
        _ => Err(io::Error::other("unsupported transport")),
    };
    let _ = fs::remove_file(path.join("owner.lock"));
    result
}
