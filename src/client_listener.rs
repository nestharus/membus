//! Bounded local MBC1 receiving at the shared client boundary.
use super::*;
use std::{fs::File, os::windows::io::FromRawHandle};
type Handle = *mut std::ffi::c_void;
#[link(name = "kernel32")]
extern "system" {
    fn CreateNamedPipeW(
        name: *const u16,
        access: u32,
        mode: u32,
        instances: u32,
        out: u32,
        input: u32,
        timeout: u32,
        security: Handle,
    ) -> Handle;
    fn DisconnectNamedPipe(pipe: Handle) -> i32;
}
#[derive(Debug)]
pub enum ReceiveError {
    QuietTimeout,
    Interrupted(io::Error),
    Protocol(&'static str),
    Closed,
}
impl std::fmt::Display for ReceiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ReceiveError {}
pub struct ChangeListener {
    endpoint: String,
    pipe: Option<crate::pipe_io::Pipe>,
    connected: bool,
    max_frame: usize,
    #[cfg(test)]
    closing_accepts: usize,
}
impl ChangeListener {
    pub fn bind(endpoint: &str, max_frame: usize) -> Result<Self> {
        let low = endpoint.to_ascii_lowercase();
        if !low.starts_with(r"\\.\pipe\")
            || endpoint.len() <= 9
            || endpoint.len() > MAX_ID
            || endpoint.contains('\0')
            || endpoint[9..].contains(['/', ':'])
            || endpoint[9..]
                .split('\\')
                .any(|p| p.is_empty() || p == "." || p == ".." || p.ends_with(['.', ' ']))
            || !(1024..=MAX_FRAME).contains(&max_frame)
        {
            return Err(Error::Invalid("listener endpoint/bounds"));
        }
        let name: Vec<u16> = endpoint.encode_utf16().chain(Some(0)).collect();
        // Inbound/overlapped/FIRST_PIPE_INSTANCE; byte stream, reject remote.
        let h = unsafe {
            CreateNamedPipeW(
                name.as_ptr(),
                1 | 0x40000000 | 0x00080000,
                0x8,
                1,
                65536,
                65536,
                0,
                std::ptr::null_mut(),
            )
        };
        if h as isize == -1 {
            return Err(Error::Transport(io::Error::last_os_error()));
        }
        Ok(Self {
            endpoint: endpoint.into(),
            pipe: Some(crate::pipe_io::Pipe::new(
                unsafe { File::from_raw_handle(h) },
                1,
            )),
            connected: false,
            max_frame,
            #[cfg(test)]
            closing_accepts: 0,
        })
    }
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    /// Every overlapped I/O has been drained before this synchronous close.
    pub fn close(&mut self) {
        self.pipe.take();
        self.connected = false;
    }
    pub fn cancel(&mut self) {
        self.close();
    }
    fn retire(&mut self) {
        let reset = self
            .pipe
            .as_ref()
            .is_some_and(|p| unsafe { DisconnectNamedPipe(p.raw_handle()) != 0 });
        self.connected = false;
        // A failed reset cannot promise that this handle accepts a new writer.
        if !reset {
            self.pipe.take();
        }
    }
    fn read_part(
        &mut self,
        b: &mut [u8],
        d: Instant,
        progress: &mut usize,
    ) -> std::result::Result<(), ReceiveError> {
        let mut at = 0;
        while at < b.len() {
            let p = self.pipe.as_mut().ok_or(ReceiveError::Closed)?;
            if let Err(e) = p.set_deadline(d) {
                return Err(if *progress == 0 && e.kind() == io::ErrorKind::TimedOut {
                    ReceiveError::QuietTimeout
                } else {
                    ReceiveError::Interrupted(e)
                });
            }
            // Establish header consumption with one byte: a canceled first read
            // cannot hide a partially filled multi-byte header as quiet time.
            let end = if *progress == 0 { at + 1 } else { b.len() };
            match p.read(&mut b[at..end]) {
                Ok(0) => {
                    return Err(ReceiveError::Interrupted(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "change truncated",
                    )))
                }
                Ok(n) => {
                    at += n;
                    *progress += n;
                }
                Err(e) => {
                    return Err(if *progress == 0 && e.kind() == io::ErrorKind::TimedOut {
                        ReceiveError::QuietTimeout
                    } else {
                        ReceiveError::Interrupted(e)
                    })
                }
            }
        }
        Ok(())
    }
    /// Complete buffered frames can be read after their writer closes. Terminal
    /// input errors retire the connection, or close it if reset fails. Remote
    /// subscription cleanup and reconciliation remain caller responsibilities.
    pub fn receive(
        &mut self,
        subscription: &Subscription,
        ns: &str,
        prefix: &str,
        until: Instant,
    ) -> std::result::Result<Change, ReceiveError> {
        if self.pipe.is_none() {
            return Err(ReceiveError::Closed);
        }
        if !self.connected {
            if let Err(e) = self.pipe.as_mut().unwrap().connect_until(until) {
                if e.raw_os_error() == Some(232) {
                    // ERROR_NO_DATA can leave complete buffered frames. It is
                    // not a successful Change: normal bounded decode follows.
                    #[cfg(test)]
                    {
                        self.closing_accepts += 1;
                    }
                } else if e.kind() == io::ErrorKind::TimedOut {
                    return Err(ReceiveError::QuietTimeout);
                } else {
                    self.retire();
                    return Err(ReceiveError::Interrupted(e));
                }
            }
            self.connected = true;
        }
        let mut progress = 0;
        let result = (|| {
            let mut h = [0; 4];
            self.read_part(&mut h, until, &mut progress)?;
            let size = u32::from_le_bytes(h) as usize;
            if !(38..=self.max_frame).contains(&size) {
                return Err(ReceiveError::Protocol("change frame bound"));
            }
            let mut body = vec![0; size];
            self.read_part(&mut body, until, &mut progress)?;
            let c = Change::decode(&body).map_err(|_| ReceiveError::Protocol("change payload"))?;
            if c.subscription != subscription.token
                || c.namespace != ns
                || !c.key.starts_with(prefix)
                || c.cursor[..8] != subscription.cursor[..8]
            {
                return Err(ReceiveError::Protocol("change association"));
            }
            Ok(c)
        })();
        if !matches!(&result, Ok(_) | Err(ReceiveError::QuietTimeout)) {
            self.retire();
        }
        result
    }
}
#[cfg(test)]
#[path = "../tests/support/listener_regression.rs"]
mod regression;
