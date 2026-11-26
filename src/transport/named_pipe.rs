#[cfg(windows)]
use std::ffi::c_void;
#[cfg(windows)]
use std::io;
#[cfg(windows)]
use std::mem::size_of;
#[cfg(windows)]
use std::ptr;
#[cfg(windows)]
use std::sync::Arc;
#[cfg(windows)]
use std::time::{Duration, Instant};

#[cfg(windows)]
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};
#[cfg(windows)]
use tokio::sync::watch;

#[cfg(windows)]
use crate::transport::connection::serve_connection;
#[cfg(windows)]
use crate::transport::Endpoint;
use crate::transport::{ServerContext, ServerHandle};
use crate::types::MembusError;

#[cfg(windows)]
const PIPE_IO_BUFFER_BYTES: u32 = 64 * 1024;
#[cfg(windows)]
const ERROR_PIPE_BUSY: i32 = 231;

/// Bind a Windows IOCP Named Pipe server supporting unlimited concurrent connections,
/// protected by a current-user-SID + LocalSystem DACL and `reject_remote_clients`.
#[cfg(windows)]
pub async fn spawn_named_pipe_listener(
    pipe_name: String,
    ctx: ServerContext,
) -> Result<ServerHandle, MembusError> {
    if !pipe_name.starts_with(r"\\.\pipe\") {
        return Err(MembusError::InvalidArgument(format!(
            "Windows named pipe must begin with '\\\\.\\pipe\\': got '{pipe_name}'"
        )));
    }

    let dacl = OwnedPipeDacl::current_user_and_system()?;
    let mut current_instance = create_pipe_instance(&pipe_name, true, &dacl)?;

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let active_connections = Arc::clone(&ctx.active_connections);
    let endpoint_name = pipe_name.clone();

    let accept_task = tokio::spawn(async move {
        let mut backoff_ms = 5u64;
        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                connected_res = current_instance.connect() => {
                    match connected_res {
                        Ok(()) => {
                            backoff_ms = 5;
                            let connected_pipe = current_instance;
                            let conn_ctx = ctx.clone();
                            let conn_shutdown = shutdown_rx.clone();
                            tokio::spawn(serve_connection(
                                connected_pipe,
                                conn_ctx,
                                conn_shutdown,
                            ));
                            match create_next_instance_with_retry(&pipe_name, &dacl, &mut shutdown_rx).await {
                                Some(next_instance) => current_instance = next_instance,
                                None => break,
                            }
                        }
                        Err(_) => {
                            // Recreate a clean pipe instance instead of spinning on a poisoned handle.
                            if !sleep_or_shutdown(Duration::from_millis(backoff_ms), &mut shutdown_rx).await {
                                break;
                            }
                            backoff_ms = (backoff_ms * 2).min(50);
                            match create_next_instance_with_retry(&pipe_name, &dacl, &mut shutdown_rx).await {
                                Some(next_instance) => current_instance = next_instance,
                                None => break,
                            }
                        }
                    }
                }
            }
        }
    });

    Ok(ServerHandle::new(
        Endpoint::NamedPipe(endpoint_name),
        shutdown_tx,
        accept_task,
        active_connections,
    ))
}

#[cfg(windows)]
async fn create_next_instance_with_retry(
    pipe_name: &str,
    dacl: &OwnedPipeDacl,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Option<NamedPipeServer> {
    let mut delay_ms = 5u64;
    loop {
        if *shutdown_rx.borrow() {
            return None;
        }
        match create_pipe_instance(pipe_name, false, dacl) {
            Ok(inst) => return Some(inst),
            Err(_) => {
                if !sleep_or_shutdown(Duration::from_millis(delay_ms), shutdown_rx).await {
                    return None;
                }
                delay_ms = (delay_ms * 2).min(50);
            }
        }
    }
}

#[cfg(windows)]
async fn sleep_or_shutdown(dur: Duration, shutdown_rx: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => !*shutdown_rx.borrow(),
        _ = tokio::time::sleep(dur) => !*shutdown_rx.borrow(),
    }
}

#[cfg(windows)]
fn create_pipe_instance(
    pipe_name: &str,
    first_instance: bool,
    dacl: &OwnedPipeDacl,
) -> io::Result<NamedPipeServer> {
    let mut attrs = SecurityAttributes {
        n_length: size_of::<SecurityAttributes>() as u32,
        lp_security_descriptor: dacl.as_ptr(),
        b_inherit_handle: 0,
    };
    let mut opts = ServerOptions::new();
    opts.first_pipe_instance(first_instance)
        .reject_remote_clients(true)
        .in_buffer_size(PIPE_IO_BUFFER_BYTES)
        .out_buffer_size(PIPE_IO_BUFFER_BYTES);
    // SAFETY: `attrs` points to a valid `SECURITY_ATTRIBUTES` with an immutable
    // self-relative security descriptor alive for the duration of `CreateNamedPipeW`.
    unsafe {
        opts.create_with_security_attributes_raw(pipe_name, (&mut attrs as *mut SecurityAttributes).cast::<c_void>())
    }
}

#[cfg(not(windows))]
pub async fn spawn_named_pipe_listener(
    _pipe_name: String,
    _ctx: ServerContext,
) -> Result<ServerHandle, MembusError> {
    Err(MembusError::InvalidArgument(
        "Windows named pipes are only supported on Windows".to_owned(),
    ))
}

/// Dial a Windows IOCP Named Pipe endpoint and wrap it as a [`crate::transport::TransportStream`].
#[cfg(windows)]
pub async fn connect_named_pipe_stream(
    pipe_name: &str,
) -> Result<crate::transport::TransportStream, MembusError> {
    let client = connect_named_pipe(pipe_name).await?;
    Ok(crate::transport::TransportStream::NamedPipe(client))
}

#[cfg(not(windows))]
pub async fn connect_named_pipe_stream(
    _pipe_name: &str,
) -> Result<crate::transport::TransportStream, MembusError> {
    Err(MembusError::InvalidArgument(
        "Windows named pipes are only supported on Windows".to_owned(),
    ))
}

/// Dial a Windows IOCP Named Pipe endpoint, retrying transient `ERROR_PIPE_BUSY`
/// races during simultaneous multi-client bursts.
#[cfg(windows)]
pub async fn connect_named_pipe(pipe_name: &str) -> Result<NamedPipeClient, MembusError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match ClientOptions::new().open(pipe_name) {
            Ok(client) => return Ok(client),
            Err(err) if err.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                if Instant::now() >= deadline {
                    return Err(MembusError::Io(format!(
                        "timed out waiting for busy named pipe '{pipe_name}': {err}"
                    )));
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(err) => return Err(MembusError::Io(err.to_string())),
        }
    }
}

#[cfg(windows)]
#[repr(C)]
struct SecurityAttributes {
    n_length: u32,
    lp_security_descriptor: *mut c_void,
    b_inherit_handle: i32,
}

#[cfg(windows)]
#[repr(C)]
struct SidAndAttributes {
    sid: *mut c_void,
    attributes: u32,
}

#[cfg(windows)]
#[repr(C)]
struct TokenUser {
    user: SidAndAttributes,
}

#[cfg(windows)]
struct HandleGuard(isize);

#[cfg(windows)]
impl Drop for HandleGuard {
    fn drop(&mut self) {
        if self.0 != 0 {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

#[cfg(windows)]
struct LocalAllocGuard(*mut c_void);

#[cfg(windows)]
impl Drop for LocalAllocGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                LocalFree(self.0);
            }
        }
    }
}

/// RAII self-relative Windows security descriptor granting access exclusively to
/// the resident process's user SID and LocalSystem (`SY`).
#[cfg(windows)]
struct OwnedPipeDacl {
    sd: *mut c_void,
}

#[cfg(windows)]
// SAFETY: A self-relative `SECURITY_DESCRIPTOR` allocated by Windows `LocalAlloc`
// is immutable once created and safe to share/drop across threads.
unsafe impl Send for OwnedPipeDacl {}
#[cfg(windows)]
unsafe impl Sync for OwnedPipeDacl {}

#[cfg(windows)]
impl OwnedPipeDacl {
    fn current_user_and_system() -> Result<Self, MembusError> {
        let user_sid = current_user_sid_string()?;
        let sddl = format!("D:P(A;;GA;;;{user_sid})(A;;GA;;;SY)\0");
        let sddl_wide: Vec<u16> = sddl.encode_utf16().collect();
        let mut sd: *mut c_void = ptr::null_mut();
        const SDDL_REVISION_1: u32 = 1;
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl_wide.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                ptr::null_mut(),
            )
        };
        if ok == 0 || sd.is_null() {
            return Err(MembusError::Io(format!(
                "failed to construct named-pipe security descriptor: {}",
                io::Error::last_os_error()
            )));
        }
        Ok(Self { sd })
    }

    fn as_ptr(&self) -> *mut c_void {
        self.sd
    }
}

#[cfg(windows)]
impl Drop for OwnedPipeDacl {
    fn drop(&mut self) {
        if !self.sd.is_null() {
            unsafe {
                LocalFree(self.sd);
            }
        }
    }
}

#[cfg(windows)]
fn current_user_sid_string() -> Result<String, MembusError> {
    const TOKEN_QUERY: u32 = 0x0008;
    const TOKEN_USER_CLASS: u32 = 1;

    let mut token: isize = 0;
    let opened = unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) };
    if opened == 0 || token == 0 {
        return Err(MembusError::Io(format!(
            "OpenProcessToken failed: {}",
            io::Error::last_os_error()
        )));
    }
    let _token_guard = HandleGuard(token);

    let mut needed: u32 = 0;
    unsafe {
        GetTokenInformation(
            token,
            TOKEN_USER_CLASS,
            ptr::null_mut(),
            0,
            &mut needed,
        );
    }
    if (needed as usize) < size_of::<TokenUser>() {
        return Err(MembusError::Io(format!(
            "GetTokenInformation size query returned insufficient buffer ({needed} bytes): {}",
            io::Error::last_os_error()
        )));
    }

    let word_count = (needed as usize).div_ceil(size_of::<usize>());
    let mut aligned_buf = vec![0usize; word_count];
    let buf_byte_len = aligned_buf.len() * size_of::<usize>();
    let mut returned_len: u32 = 0;
    let ok = unsafe {
        GetTokenInformation(
            token,
            TOKEN_USER_CLASS,
            aligned_buf.as_mut_ptr().cast::<c_void>(),
            buf_byte_len as u32,
            &mut returned_len,
        )
    };
    if ok == 0 || (returned_len as usize) < size_of::<TokenUser>() {
        return Err(MembusError::Io(format!(
            "GetTokenInformation failed (returned {returned_len} bytes): {}",
            io::Error::last_os_error()
        )));
    }

    // SAFETY: `aligned_buf` is `usize`-aligned and verified to hold at least `size_of::<TokenUser>()` bytes.
    let token_user = unsafe { &*(aligned_buf.as_ptr().cast::<TokenUser>()) };
    if token_user.user.sid.is_null() {
        return Err(MembusError::Io(
            "GetTokenInformation returned null user SID".to_owned(),
        ));
    }
    let mut sid_wstr: *mut u16 = ptr::null_mut();
    let converted = unsafe { ConvertSidToStringSidW(token_user.user.sid, &mut sid_wstr) };
    if converted == 0 || sid_wstr.is_null() {
        return Err(MembusError::Io(format!(
            "ConvertSidToStringSidW failed: {}",
            io::Error::last_os_error()
        )));
    }
    let _sid_guard = LocalAllocGuard(sid_wstr.cast::<c_void>());

    let mut len = 0usize;
    unsafe {
        while *sid_wstr.add(len) != 0 {
            len += 1;
        }
        let slice = std::slice::from_raw_parts(sid_wstr, len);
        String::from_utf16(slice)
            .map_err(|err| MembusError::Io(format!("invalid UTF-16 user SID: {err}")))
    }
}

#[cfg(windows)]
#[link(name = "kernel32")]
extern "system" {
    fn GetCurrentProcess() -> isize;
    fn CloseHandle(handle: isize) -> i32;
    fn LocalFree(hmem: *mut c_void) -> *mut c_void;
}

#[cfg(windows)]
#[link(name = "advapi32")]
extern "system" {
    fn OpenProcessToken(
        process_handle: isize,
        desired_access: u32,
        token_handle: *mut isize,
    ) -> i32;
    fn GetTokenInformation(
        token_handle: isize,
        token_information_class: u32,
        token_information: *mut c_void,
        token_information_length: u32,
        return_length: *mut u32,
    ) -> i32;
    fn ConvertSidToStringSidW(sid: *mut c_void, string_sid: *mut *mut u16) -> i32;
    fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
        string_security_descriptor: *const u16,
        string_sd_revision: u32,
        security_descriptor: *mut *mut c_void,
        security_descriptor_size: *mut u32,
    ) -> i32;
}
