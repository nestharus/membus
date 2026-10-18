//! Owned finite Windows children: suspend, retain, assign Job, then resume.
use std::{
    ffi::c_void,
    io,
    os::windows::{io::AsRawHandle, process::CommandExt},
    process::{Child, Command, ExitStatus},
    time::{Duration, Instant},
};
type Handle = *mut c_void;
#[repr(C)]
#[derive(Default)]
struct BasicLimits {
    process_time: i64,
    job_time: i64,
    flags: u32,
    minimum: usize,
    maximum: usize,
    active_limit: u32,
    affinity: usize,
    priority: u32,
    scheduling: u32,
}
#[repr(C)]
#[derive(Default)]
struct Limits {
    basic: BasicLimits,
    io: [u64; 6],
    process_memory: usize,
    job_memory: usize,
    peak_process: usize,
    peak_job: usize,
}
#[repr(C)]
#[derive(Default)]
struct Accounting {
    user: i64,
    kernel: i64,
    period_user: i64,
    period_kernel: i64,
    faults: u32,
    total: u32,
    active: u32,
    terminated: u32,
}
#[link(name = "kernel32")]
unsafe extern "system" {
    fn CreateJobObjectW(attributes: Handle, name: *const u16) -> Handle;
    fn SetInformationJobObject(job: Handle, class: u32, info: *const c_void, bytes: u32) -> i32;
    fn AssignProcessToJobObject(job: Handle, process: Handle) -> i32;
    fn QueryInformationJobObject(
        job: Handle,
        class: u32,
        info: *mut c_void,
        bytes: u32,
        returned: *mut u32,
    ) -> i32;
    fn TerminateJobObject(job: Handle, code: u32) -> i32;
    fn CloseHandle(handle: Handle) -> i32;
}
#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtResumeProcess(process: Handle) -> i32;
}
struct Job(Handle);
impl Job {
    fn new() -> io::Result<Self> {
        let handle = unsafe { CreateJobObjectW(std::ptr::null_mut(), std::ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Self(handle);
        let mut limits = Limits::default();
        limits.basic.flags = 0x2000;
        if unsafe {
            SetInformationJobObject(
                handle,
                9,
                (&limits as *const Limits).cast(),
                std::mem::size_of::<Limits>() as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }
    fn active(&self) -> io::Result<u32> {
        let mut value = Accounting::default();
        if unsafe {
            QueryInformationJobObject(
                self.0,
                1,
                (&mut value as *mut Accounting).cast(),
                std::mem::size_of::<Accounting>() as u32,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(value.active)
    }
    fn stop(&self) -> io::Result<()> {
        if unsafe { TerminateJobObject(self.0, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    fn collect(&self) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.active()? != 0 {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "owned Job settlement",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }
}
impl Drop for Job {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}
pub struct Owned {
    pub child: Child,
    job: Job,
    waited: bool,
}
impl Owned {
    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        let job = Job::new()?;
        command.creation_flags(0x08000004);
        // Retain immediately; assignment/resume errors unwind through this guard.
        let mut owner = Self {
            child: command.spawn()?,
            job,
            waited: false,
        };
        if unsafe { AssignProcessToJobObject(owner.job.0, owner.child.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let status = unsafe { NtResumeProcess(owner.child.as_raw_handle()) };
        if status < 0 {
            return Err(io::Error::other(format!("resume failed {status}")));
        }
        // Keep a mutable owner for subsequent stdin/try_wait operations.
        owner.waited = false;
        Ok(owner)
    }
    pub fn wait(&mut self, timeout: Duration) -> io::Result<(ExitStatus, bool)> {
        let start = Instant::now();
        let mut stopped = false;
        let status = loop {
            match self.child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if start.elapsed() < timeout => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                _ => {
                    stopped = true;
                    self.job.stop()?;
                    break self.child.wait()?;
                }
            }
        };
        self.waited = true;
        // A completed direct child does not authorize detached descendants.
        if self.job.active()? != 0 {
            self.job.stop()?;
        }
        self.job.collect()?;
        Ok((status, stopped))
    }
}
impl Drop for Owned {
    fn drop(&mut self) {
        if !self.waited {
            let _ = self.job.stop();
            let _ = self.child.kill();
            match self.child.wait() {
                Ok(status) => {
                    self.waited = true;
                    eprintln!("owned child Drop waited: {status}");
                }
                Err(error) => eprintln!("owned child wait failure: {error}"),
            }
        }
        if let Err(error) = self.job.stop().and_then(|_| self.job.collect()) {
            eprintln!("owned Job collection failure: {error}");
        }
    }
}
