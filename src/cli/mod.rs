//! Binary command routing, resident server startup, and transport accept loops.
mod admin;
mod admission;
mod qualification;

#[cfg(test)]
#[path = "../unit_tests/cli/resident_request_tests.rs"]
mod resident_request_tests;

#[cfg(windows)]
use membus::pipe_io;
use membus::{ResidentDispatch, Store};
use std::{
    env,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
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
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
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
    let mut listener = membus::platform::SecuredPipeListener::bind(address)?;
    let active = Arc::new(AtomicUsize::new(0));
    let ingress = Arc::new(AtomicUsize::new(0));
    loop {
        while active.load(Ordering::Acquire) >= 64 {
            thread::sleep(std::time::Duration::from_millis(5));
        }
        let file = listener.next_instance()?;
        let mut f = pipe_io::Pipe::new(file, idle_ms);
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

pub(crate) fn run() -> io::Result<()> {
    let args: Vec<_> = env::args().collect();
    admission::dispatch(&args, |command| match command {
        admission::Command::Admin(admin, args) => match admin {
            admission::Admin::Retirement => qualification::retirement_command(args),
            admission::Admin::Catalogue => qualification::catalogue_command(args),
            admission::Admin::Health => admin::health_command(args),
            admission::Admin::Policy => admin::policy_command(args),
            admission::Admin::Expiry => admin::expiry_command(args),
        },
        admission::Command::Server(command) => start_server(command),
    })
}

fn start_server(command: admission::Server) -> io::Result<()> {
    let admission::Server {
        path,
        transport,
        address,
        mut config,
        idle_ms,
        heartbeat_ms,
    } = command;
    #[cfg(any(test, feature = "test-seams"))]
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
        admission::Transport::Tcp => tcp(&address, workers.wire_dispatch(&store)?, idle_ms),
        #[cfg(windows)]
        admission::Transport::Pipe => pipes(&address, workers.wire_dispatch(&store)?, idle_ms),
    };
    let settlement = workers.join();
    if let Err(error) = &settlement {
        eprintln!("resident worker settlement: {error}");
    }
    #[cfg(not(windows))]
    if settlement.is_ok() && Arc::try_unwrap(store).is_ok() {
        let _ = std::fs::remove_file(path.join("owner.lock"));
    }
    result.and(settlement)
}
