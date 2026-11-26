//! Shared local named-pipe address validation and client/admin/signal pipe connectors.
use crate::MAX_ID;
use std::io;
#[cfg(windows)]
use std::{
    path::Path,
    thread,
    time::{Duration, Instant},
};

/// Strict validation of explicit local Windows named-pipe names (`\\.\pipe\<name>`).
pub fn is_valid_local_pipe_name(raw: &str, enforce_max_id: bool) -> bool {
    let low = raw.to_ascii_lowercase();
    low.starts_with(r"\\.\pipe\")
        && raw.len() > 9
        && (!enforce_max_id || raw.len() <= MAX_ID)
        && !raw.contains('\0')
        && !raw[9..].contains(['/', ':'])
        && raw[9..]
            .split('\\')
            .all(|p| !p.is_empty() && p != "." && p != ".." && !p.ends_with(['.', ' ']))
}

#[cfg(windows)]
pub type SignalConnection = super::pipe_io::Pipe;
#[cfg(not(windows))]
pub type SignalConnection = ();

/// Deliver a framed signal payload over a persistent local named-pipe connection.
#[cfg(windows)]
pub fn deliver_signal_pipe(
    endpoint: &str,
    payload: &[u8],
    timeout: u32,
    connection: &mut Option<SignalConnection>,
    clock: &crate::chunks::Clock,
) -> io::Result<()> {
    let deadline = clock.mono_ms().saturating_add(timeout as u64);
    if connection.is_none() {
        let file = loop {
            match crate::platform::open_signal_pipe_writer(endpoint) {
                Ok(file) => break file,
                Err(error)
                    if error.raw_os_error() == Some(231) && clock.mono_ms() < deadline =>
                {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => return Err(error),
            }
        };
        *connection = Some(super::pipe_io::Pipe::new(file, timeout));
    }
    let result = connection.as_mut().unwrap().write_before(payload, deadline, clock);
    if result.is_err() {
        *connection = None;
    }
    result
}

#[cfg(not(windows))]
pub fn deliver_signal_pipe(
    _: &str,
    _: &[u8],
    _: u32,
    _: &mut Option<SignalConnection>,
    _: &crate::chunks::Clock,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "signals require Windows pipes",
    ))
}

/// Open an overlapped local named pipe for an admin CLI command and return its server PID.
#[cfg(windows)]
pub fn connect_admin_pipe_with_pid(
    pipe_name: &str,
    timeout_ms: u32,
) -> io::Result<(super::pipe_io::Pipe, u32)> {
    let connect_until = Instant::now() + Duration::from_millis(timeout_ms as u64);
    let file = loop {
        match crate::platform::open_overlapped_pipe(Path::new(pipe_name), false) {
            Ok(file) => break file,
            Err(error) if error.raw_os_error() == Some(231) && Instant::now() < connect_until => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(error),
        }
    };
    let pid = crate::platform::pipe_server_pid(&file)?;
    Ok((super::pipe_io::Pipe::new(file, timeout_ms), pid))
}

/// Open an overlapped local named pipe for read-only inspection after verifying kernel pipe type.
#[cfg(windows)]
pub fn connect_admin_pipe_checked(
    pipe_name: &str,
    timeout_ms: u32,
) -> io::Result<super::pipe_io::Pipe> {
    let until = Instant::now() + Duration::from_millis(timeout_ms as u64);
    let file = loop {
        match crate::platform::open_overlapped_pipe(Path::new(pipe_name), false) {
            Ok(file) => break file,
            Err(e) if e.raw_os_error() == Some(231) && Instant::now() < until => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(e) => return Err(e),
        }
    };
    if !crate::platform::is_pipe_handle(&file) {
        return Err(io::Error::other("endpoint is not a pipe; no request sent"));
    }
    Ok(super::pipe_io::Pipe::new(file, timeout_ms))
}
