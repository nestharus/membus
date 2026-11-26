//! Windows host platform implementations and FFI bindings.
use std::{
    ffi::c_void,
    fs::{self, File, OpenOptions},
    io,
    os::windows::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt},
        io::{AsRawHandle, FromRawHandle},
    },
    path::Path,
};

pub const CHECKS_ALTERNATE_STREAMS: bool = true;

/// Persist file contents. Under `sync-witness`, optionally records FlushFileBuffers timing.
#[cfg(feature = "sync-witness")]
pub fn sync_file(file: &File) -> io::Result<()> {
    use std::io::Write;
    use std::sync::Mutex;
    #[link(name = "kernel32")]
    extern "system" {
        fn FlushFileBuffers(handle: *mut c_void) -> i32;
        fn GetFinalPathNameByHandleW(
            handle: *mut c_void,
            path: *mut u16,
            size: u32,
            flags: u32,
        ) -> u32;
    }
    let marks = std::env::var_os("MEMBUS_SYNC_WITNESS").map(std::path::PathBuf::from);
    let witness = marks.as_ref().map(|_| {
        let mut target = vec![0u16; 1024];
        let mut n = unsafe {
            GetFinalPathNameByHandleW(
                file.as_raw_handle(),
                target.as_mut_ptr(),
                target.len() as u32,
                0,
            )
        };
        if n as usize >= target.len() {
            target.resize(n as usize + 1, 0);
            n = unsafe {
                GetFinalPathNameByHandleW(
                    file.as_raw_handle(),
                    target.as_mut_ptr(),
                    target.len() as u32,
                    0,
                )
            };
        }
        target.truncate((n as usize).min(target.len()));
        (target, file.metadata().map(|m| m.len()).ok())
    });
    if let Some(path) = &marks {
        if let Err(error) = fs::write(path.with_extension("entering"), b"entering") {
            eprintln!("sync-witness entering mark failed: {error}");
        }
    }
    let started = std::time::Instant::now();
    let ok = unsafe { FlushFileBuffers(file.as_raw_handle()) };
    let elapsed = started.elapsed().as_nanos();
    let result = if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    };
    if let Some(path) = &marks {
        if let Err(error) = fs::write(
            path.with_extension("returned"),
            format!("{{\"call_ns\":{elapsed},\"ok\":{}}}", ok != 0),
        ) {
            eprintln!("sync-witness returned mark failed: {error}");
        }
        if let Some((target, bytes)) = witness {
            static LOG: Mutex<()> = Mutex::new(());
            let _guard = LOG.lock().unwrap();
            let record = format!(
                "{{\"call_ns\":{elapsed},\"ok\":{},\"target_utf16\":{target:?},\"bytes\":{}}}\n",
                ok != 0,
                bytes.map_or_else(|| "null".into(), |n| n.to_string())
            );
            if let Err(error) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path.with_extension("calls"))
                .and_then(|mut f| f.write_all(record.as_bytes()))
            {
                eprintln!("sync-witness calls mark failed: {error}");
            }
        }
    }
    result
}

/// Persist file contents.
#[cfg(not(feature = "sync-witness"))]
pub fn sync_file(file: &File) -> io::Result<()> {
    file.sync_all()
}

/// Persist directory-entry changes via BACKUP_SEMANTICS + FlushFileBuffers.
pub fn sync_dir(path: &Path) -> io::Result<()> {
    #[link(name = "kernel32")]
    extern "system" {
        fn FlushFileBuffers(handle: *mut c_void) -> i32;
    }
    let dir = OpenOptions::new()
        .write(true)
        .share_mode(7)
        .custom_flags(0x02000000) // FILE_FLAG_BACKUP_SEMANTICS
        .open(path)?;
    if unsafe { FlushFileBuffers(dir.as_raw_handle()) } == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Open the store's exclusive owner lock file.
pub fn open_exclusive_owner(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(path)
}

/// Open the catalogue's sibling owner lock file exclusively.
pub fn open_catalogue_owner(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(path)
        .map_err(|e| io::Error::other(format!("catalogue writer ownership refused: {e}")))
}

/// Open the catalogue file for exclusive writing while permitting read/delete sharing.
pub fn open_catalogue_writer(path: &Path, create: bool) -> io::Result<File> {
    // Permit snapshots and our atomic rename; deny a second writable alias, including hardlinks.
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .share_mode(1 | 4)
        .open(path)
}

/// Open a read-only input denying concurrent writers and deletes.
pub fn open_read_only_exclusive(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .share_mode(1) // FILE_SHARE_READ only
        .open(path)
}

/// Windows owner.lock uses share_mode(0), so a copied file needs no pre-removal.
pub fn prepare_quarantined_lock(
    _target: &Path,
    _sync: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    Ok(())
}

/// Reject reparse, encrypted, offline, and device files outside the regular-byte image contract.
pub fn validate_regular_metadata(meta: &fs::Metadata) -> io::Result<()> {
    if meta.file_attributes() & (0x400 | 0x4000 | 0x1000 | 0x40) != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "image layout contains unsupported Windows attributes",
        ));
    }
    Ok(())
}

/// Windows final-path representation is at most 32768 UTF-16 code units.
pub fn canonical_path_workspace_max(_path: &Path) -> usize {
    4 * 32768
}

/// Verify that a regular file contains only the default unnamed `::$DATA` stream.
pub fn check_regular_file_streams(file: &File) -> io::Result<()> {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetFileInformationByHandleEx(
            handle: *mut c_void,
            class: u32,
            buffer: *mut c_void,
            size: u32,
        ) -> i32;
    }
    let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg);
    let mut words = vec![0u64; 8192];
    let bytes: &mut [u8] =
        unsafe { std::slice::from_raw_parts_mut(words.as_mut_ptr().cast::<u8>(), words.len() * 8) };
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            7,
            bytes.as_mut_ptr().cast(),
            bytes.len() as u32,
        )
    } == 0
    {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(
            error.kind(),
            format!("GetFileInformationByHandleEx(FileStreamInfo) refused: {error}"),
        ));
    }
    let mut offset = 0usize;
    loop {
        let header = bytes
            .get(offset..offset + 24)
            .ok_or_else(|| invalid("invalid filesystem stream inventory"))?;
        let next = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        if !length.is_multiple_of(2) {
            return Err(invalid("invalid filesystem stream name"));
        }
        let name = bytes
            .get(offset + 24..offset + 24 + length)
            .ok_or_else(|| invalid("oversized filesystem stream name"))?;
        if name != b":\0:\0$\0D\0A\0T\0A\0" {
            return Err(invalid(
                "alternate filesystem streams are unsupported by the regular-byte image",
            ));
        }
        if next == 0 {
            break;
        }
        if next < 24 {
            return Err(invalid("invalid filesystem stream offset"));
        }
        offset = offset
            .checked_add(next)
            .ok_or_else(|| invalid("filesystem stream offset overflow"))?;
    }
    Ok(())
}

/// Verify that a directory has no alternate named data streams.
pub fn check_directory_streams(path: &Path) -> io::Result<()> {
    #[repr(C)]
    struct Stream {
        size: i64,
        name: [u16; 296],
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn FindFirstStreamW(
            path: *const u16,
            level: u32,
            data: *mut Stream,
            flags: u32,
        ) -> *mut c_void;
        fn FindClose(handle: *mut c_void) -> i32;
    }
    let name: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut stream = Stream {
        size: 0,
        name: [0; 296],
    };
    let handle = unsafe { FindFirstStreamW(name.as_ptr(), 0, &mut stream, 0) };
    if handle as isize == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(38) {
            return Ok(());
        }
        return Err(io::Error::new(
            error.kind(),
            format!(
                "FindFirstStreamW(directory) refused ({}): {error}",
                path.display()
            ),
        ));
    }
    unsafe {
        FindClose(handle);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "alternate directory streams are unsupported by the regular-byte image",
    ))
}

/// Reject Windows alternate stream selectors (`:`) in leaf file names.
pub fn validate_default_stream_leaf(path: &Path) -> io::Result<()> {
    if !path
        .file_name()
        .is_some_and(|leaf| !leaf.encode_wide().any(|c| c == b':' as u16))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected a file name without a stream selector",
        ));
    }
    Ok(())
}

/// OpenOptions for held custody files: GENERIC_WRITE | FILE_READ_ATTRIBUTES, FILE_SHARE_READ only.
pub fn open_held_custody_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.access_mode(0x4000_0080).share_mode(1);
    options
}

/// Windows FILE_ID_INFO: 64-bit little-endian volume serial followed by 128-bit file ID.
pub fn file_id(file: &File) -> io::Result<[u8; 24]> {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetFileInformationByHandleEx(
            handle: *mut c_void,
            class: u32,
            buffer: *mut c_void,
            size: u32,
        ) -> i32;
    }
    #[repr(C)]
    #[derive(Default)]
    struct FileIdInfo {
        volume: u64,
        identifier: [u8; 16],
    }
    let mut info = FileIdInfo::default();
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            18,
            (&raw mut info).cast(),
            size_of::<FileIdInfo>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let mut id = [0; 24];
    id[..8].copy_from_slice(&info.volume.to_le_bytes());
    id[8..].copy_from_slice(&info.identifier);
    Ok(id)
}

/// Check whether an open file handle refers to a Windows named pipe (`FILE_TYPE_PIPE == 3`).
pub fn is_pipe_handle(file: &File) -> bool {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetFileType(handle: *mut c_void) -> u32;
    }
    unsafe { GetFileType(file.as_raw_handle()) == 3 }
}

/// Query the local server process ID hosting a connected named pipe handle.
pub fn pipe_server_pid(file: &File) -> io::Result<u32> {
    #[link(name = "kernel32")]
    extern "system" {
        fn GetNamedPipeServerProcessId(handle: *mut c_void, pid: *mut u32) -> i32;
    }
    let mut pid = 0u32;
    if unsafe { GetNamedPipeServerProcessId(file.as_raw_handle(), &mut pid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(pid)
}

/// Open an overlapped read/write handle to a Windows named pipe.
pub fn open_overlapped_pipe(path: &Path, exclusive: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).custom_flags(0x40000000);
    if exclusive {
        options.share_mode(0);
    }
    options.open(path)
}

/// Open a write-only overlapped signal pipe handle with anonymous SQOS.
pub fn open_signal_pipe_writer(endpoint: &str) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .custom_flags(0x40000000)
        .security_qos_flags(0x100000 | 0x10000)
        .open(endpoint)
}

/// Create an exclusive first-instance inbound overlapped named pipe for change listening.
pub fn create_inbound_pipe(endpoint: &str) -> io::Result<File> {
    type Handle = *mut c_void;
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
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_handle(h) })
}

/// Disconnect a connected server-side named pipe handle for reuse.
pub fn disconnect_pipe(pipe: &crate::transport::pipe_io::Pipe) -> bool {
    #[link(name = "kernel32")]
    extern "system" {
        fn DisconnectNamedPipe(pipe: *mut c_void) -> i32;
    }
    unsafe { DisconnectNamedPipe(pipe.raw_handle()) != 0 }
}

/// Named-pipe server instance factory scoped by a DACL allowing only the current user SID and SYSTEM.
pub struct SecuredPipeListener {
    wide_name: Vec<u16>,
    descriptor: *mut c_void,
    first: bool,
}

impl Drop for SecuredPipeListener {
    fn drop(&mut self) {
        #[link(name = "kernel32")]
        extern "system" {
            fn LocalFree(p: *mut c_void) -> *mut c_void;
        }
        if !self.descriptor.is_null() {
            unsafe {
                LocalFree(self.descriptor);
            }
        }
    }
}

impl SecuredPipeListener {
    pub fn bind(address: &str) -> io::Result<Self> {
        type Handle = *mut c_void;
        #[link(name = "kernel32")]
        extern "system" {
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
        let ok = unsafe {
            GetTokenInformation(token, 1, user.as_mut_ptr().cast(), needed, &mut needed)
        };
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
        Ok(Self {
            wide_name: address.encode_utf16().chain(Some(0)).collect(),
            descriptor,
            first: true,
        })
    }

    pub fn next_instance(&mut self) -> io::Result<File> {
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
        }
        #[repr(C)]
        struct SecurityAttributes {
            length: u32,
            descriptor: *mut c_void,
            inherit: i32,
        }
        let mut security = SecurityAttributes {
            length: std::mem::size_of::<SecurityAttributes>() as u32,
            descriptor: self.descriptor,
            inherit: 0,
        };
        let h = unsafe {
            CreateNamedPipeW(
                self.wide_name.as_ptr(),
                if self.first {
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
        self.first = false;
        Ok(unsafe { File::from_raw_handle(h) })
    }
}
