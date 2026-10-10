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
            if kind != 2 && unsafe { GetOverlappedResult(h, &mut operation, &mut done, 0) } == 0 {
                return Err(io::Error::last_os_error());
            }
            return Ok(done as usize);
        }
        let error = io::Error::last_os_error();
        if kind == 2 && error.raw_os_error() == Some(535) {
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
            unsafe {
                CancelIoEx(h, &mut operation);
                GetOverlappedResult(h, &mut operation, &mut done, 1);
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
    pub fn write_until(&mut self, mut bytes: &[u8], deadline: std::time::Instant) -> io::Result<()> {
        while !bytes.is_empty() {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "signal deadline"));
            }
            self.idle_ms = remaining.as_millis().clamp(1, u32::MAX as u128 - 1) as u32;
            let n = self.write(bytes)?;
            if n == 0 { return Err(io::Error::new(io::ErrorKind::WriteZero, "signal zero write")); }
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
