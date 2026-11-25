//! Windows overlapped pipe I/O: timeout cancellation always drains completion.
use std::{
    ffi::c_void,
    fs::File,
    io::{self, Read, Write},
    os::windows::io::AsRawHandle,
};
type Handle = *mut c_void;
#[repr(C)]
struct Overlapped {
    internal: usize,
    internal_high: usize,
    offset: u32,
    offset_high: u32,
    event: Handle,
}
#[link(name = "kernel32")]
extern "system" {
    fn CreateEventW(attributes: Handle, manual: i32, initial: i32, name: *const u16) -> Handle;
    fn CloseHandle(h: Handle) -> i32;
    fn ReadFile(
        h: Handle,
        buffer: Handle,
        count: u32,
        done: *mut u32,
        operation: *mut Overlapped,
    ) -> i32;
    fn WriteFile(
        h: Handle,
        buffer: Handle,
        count: u32,
        done: *mut u32,
        operation: *mut Overlapped,
    ) -> i32;
    fn ConnectNamedPipe(h: Handle, operation: *mut Overlapped) -> i32;
    fn WaitForSingleObject(h: Handle, timeout: u32) -> u32;
    fn GetOverlappedResult(h: Handle, operation: *mut Overlapped, done: *mut u32, wait: i32)
        -> i32;
    fn CancelIoEx(h: Handle, operation: *mut Overlapped) -> i32;
}
struct Event(Handle);
impl Drop for Event {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}
pub struct Pipe {
    file: File,
    idle_ms: u32,
}
impl Pipe {
    pub(crate) fn raw_handle(&self) -> Handle {
        self.file.as_raw_handle()
    }
    /// Set the remaining whole-call budget before each bounded I/O operation.
    pub fn set_deadline(&mut self, deadline: std::time::Instant) -> io::Result<()> {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "pipe deadline"));
        }
        self.idle_ms = remaining.as_millis().clamp(1, u32::MAX as u128 - 1) as u32;
        Ok(())
    }
    pub fn new(file: File, idle_ms: u32) -> Self {
        Self { file, idle_ms }
    }
    fn operation(&mut self, buffer: Handle, count: usize, kind: u8) -> io::Result<usize> {
        let event = Event(unsafe { CreateEventW(std::ptr::null_mut(), 1, 0, std::ptr::null()) });
        if event.0.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut operation = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            event: event.0,
        };
        let h = self.file.as_raw_handle();
        let mut done = 0;
        let ok = unsafe {
            match kind {
                0 => ReadFile(
                    h,
                    buffer,
                    count.min(65536) as u32,
                    std::ptr::null_mut(),
                    &mut operation,
                ),
                1 => WriteFile(
                    h,
                    buffer,
                    count.min(65536) as u32,
                    std::ptr::null_mut(),
                    &mut operation,
                ),
                _ => ConnectNamedPipe(h, &mut operation),
            }
        };
        if ok != 0 {
            if kind < 2 && unsafe { GetOverlappedResult(h, &mut operation, &mut done, 0) } == 0 {
                return Err(io::Error::last_os_error());
            }
            return Ok(done as usize);
        }
        let error = io::Error::last_os_error();
        if kind >= 2 && error.raw_os_error() == Some(535) {
            return Ok(0);
        }
        if error.raw_os_error() != Some(997) {
            return Err(error);
        }
        let wait = unsafe {
            WaitForSingleObject(event.0, if kind == 2 { u32::MAX } else { self.idle_ms })
        };
        if wait != 0 {
            // Keep event, OVERLAPPED and buffer alive until the kernel is done.
            let completed = unsafe {
                CancelIoEx(h, &mut operation);
                GetOverlappedResult(h, &mut operation, &mut done, 1)
            };
            // Completion may race cancellation: never discard consumed bytes.
            if completed != 0 {
                return Ok(done as usize);
            }
            if done != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "cancelled I/O consumed bytes",
                ));
            }
            return Err(io::Error::new(io::ErrorKind::TimedOut, "pipe idle timeout"));
        }
        if unsafe { GetOverlappedResult(h, &mut operation, &mut done, 0) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(done as usize)
    }
    pub fn connect(&mut self) -> io::Result<()> {
        self.operation(std::ptr::null_mut(), 0, 2).map(|_| ())
    }
    /// Bounded accept; the original server-side connect remains indefinite.
    pub fn connect_until(&mut self, deadline: std::time::Instant) -> io::Result<()> {
        self.set_deadline(deadline)?;
        self.operation(std::ptr::null_mut(), 0, 3).map(|_| ())
    }
    pub fn write_until(
        &mut self,
        bytes: &[u8],
        deadline: std::time::Instant,
    ) -> io::Result<()> {
        self.write_with_budget(bytes, || deadline.saturating_duration_since(std::time::Instant::now()))
    }
    pub(crate) fn write_before(&mut self, bytes: &[u8], deadline: u64, clock: &crate::chunks::Clock) -> io::Result<()> {
        self.write_with_budget(bytes, || std::time::Duration::from_millis(deadline.saturating_sub(clock.mono_ms())))
    }
    fn write_with_budget(&mut self, mut bytes: &[u8], budget: impl Fn() -> std::time::Duration) -> io::Result<()> {
        while !bytes.is_empty() {
            let remaining = budget();
            if remaining.is_zero() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "signal deadline"));
            }
            self.idle_ms = remaining.as_millis().clamp(1, u32::MAX as u128 - 1) as u32;
            let n = self.write(bytes)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "signal zero write",
                ));
            }
            bytes = &bytes[n..];
        }
        Ok(())
    }
}
impl Read for Pipe {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        self.operation(buffer.as_mut_ptr().cast(), buffer.len(), 0)
    }
}
impl Write for Pipe {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        self.operation(buffer.as_ptr().cast_mut().cast(), buffer.len(), 1)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::windows::{ffi::OsStrExt, fs::OpenOptionsExt, io::FromRawHandle};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[link(name = "kernel32")]
    extern "system" {
        fn CreateNamedPipeW(
            name: *const u16,
            open: u32,
            mode: u32,
            instances: u32,
            out: u32,
            input: u32,
            timeout: u32,
            security: Handle,
        ) -> Handle;
    }

    fn pipe_pair(label: &str) -> (Pipe, Pipe) {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!(r"\\.\pipe\membus-pipeio-{label}-{}-{stamp}", std::process::id());
        let wide: Vec<u16> = std::ffi::OsStr::new(&name)
            .encode_wide()
            .chain(Some(0))
            .collect();
        let handle = unsafe {
            CreateNamedPipeW(
                wide.as_ptr(),
                3 | 0x40000000 | 0x00080000,
                8,
                1,
                4096,
                4096,
                0,
                std::ptr::null_mut(),
            )
        };
        assert_ne!(handle as isize, -1);
        let mut server = Pipe::new(unsafe { File::from_raw_handle(handle) }, 50);
        let client_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(0x40000000)
            .open(&name)
            .unwrap();
        server
            .connect_until(Instant::now() + Duration::from_secs(5))
            .unwrap();
        (server, Pipe::new(client_file, 50))
    }

    #[test]
    fn quiet_timeout_keeps_subsequent_pipe_bytes_readable() {
        let (mut server, mut client) = pipe_pair("timeout");
        let mut buf = [0u8; 16];
        let err = server.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);

        client
            .write_until(b"hello-pipe", Instant::now() + Duration::from_secs(5))
            .unwrap();
        server
            .set_deadline(Instant::now() + Duration::from_secs(5))
            .unwrap();
        let n = server.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello-pipe");
    }

    #[test]
    fn broken_pipe_reports_error_after_peer_close() {
        let (mut server, client) = pipe_pair("close");
        drop(client);
        let mut buf = [0u8; 8];
        let err = server.read(&mut buf).unwrap_err();
        assert_ne!(err.kind(), io::ErrorKind::TimedOut);
    }
}
