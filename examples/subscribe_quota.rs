//! Value-bearing quota change waiter using the public resident client.
//!
//! Windows: `cargo run --example subscribe_quota -- <resident-pipe> <listener-prefix>`.
//! Both endpoint locations are explicit; this example starts no service. Each
//! registration uses a fresh name below the listener prefix. Add `--events N`
//! for a finite run, or `--cursor HEX` to resume a previously printed cursor.
//! The caller must establish that the cursor belongs to the intended store and
//! finish any outstanding reconciliation; numeric cursor bytes prove neither.
//! Successful output handling precedes cursor advancement. Persist that cursor in
//! a real application's own durable checkpoint after its useful work succeeds.
//!
//! A gap starts a live subscription before observing `quota/open` and
//! `quota/resets`. These separate GETs are current state, not an atomic snapshot
//! or recovery of every missed event. Quiet time alone is not a disconnect:
//! with responsive output, each quiet interval closes and cancels the known
//! lifetime before renewal on the SAME control connection. `--quiet-ms N` sets
//! that interval (default 10000). Control idle limits must allow the intervening
//! output and work; synchronous writes/flushes have no example-owned deadline.
//! Control loss or failed cleanup stops automatic continuation. A new store at
//! the same address can reuse token/cursor bytes, so this CLI never retries them
//! through a new connection. Unknown registration also stops automatic attempts.
//! The caller receives unresolved identities/failures and owes their continuing
//! disposition; printed diagnostics are not durable custody. Local endpoint
//! closure never proves remote settlement. A printed starting checkpoint before
//! failed gap reconciliation is not proof of completed application restoration.
#[cfg(windows)]
pub mod quota {
    use membus::client::{
        ChangeListener, Client, Deadline, Error, Options, ReceiveError, SubscribeOptions,
        Subscription, Transport,
    };
    use std::{
        io::Write,
        path::Path,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    const NAMESPACE: &str = "quota";
    const PREFIX: &str = "event/";

    pub fn deadline(budget: Duration) -> Deadline {
        Deadline {
            unix_ms: membus::now_ms().saturating_add(budget.as_millis() as u64),
            local: Instant::now() + budget,
        }
    }

    pub fn connect(path: &Path, budget: Duration) -> Result<Client<membus::pipe_io::Pipe>, Error> {
        let mut options = Options::new("quota-example", membus::MAX_FRAME);
        options.subscriptions = true;
        Client::connect_pipe(path, options, deadline(budget).local)
    }

    pub fn hex(bytes: &[u8; 16]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[derive(Debug)]
    pub enum Cleanup {
        NotRegistered,
        Confirmed {
            token: [u8; 16],
        },
        KnownUnresolved {
            token: [u8; 16],
            endpoint: String,
            error: String,
        },
        UnknownRegistration {
            endpoint: String,
            error: String,
        },
    }
    impl Cleanup {
        pub fn unresolved(&self) -> bool {
            matches!(
                self,
                Self::KnownUnresolved { .. } | Self::UnknownRegistration { .. }
            )
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub enum End {
        Complete,
        Renew,
        Reconnect(String),
        Stop(String),
    }
    #[derive(Debug)]
    pub struct Report {
        pub end: End,
        pub cleanup: Cleanup,
        pub handled: usize,
        pub quiet_timeouts: usize,
    }
    #[derive(Clone, Copy)]
    pub struct Limits {
        pub quiet: Duration,
        pub budget: Duration,
        pub events: Option<usize>,
        /// Optional receive-phase bound. Client exchanges have separate budgets;
        /// synchronous handler writes and flushing have no supplied deadline.
        pub until: Option<Instant>,
    }

    fn register<T: Transport>(
        client: &mut Client<T>,
        listener: &ChangeListener,
        cursor: Option<[u8; 16]>,
        budget: Duration,
    ) -> Result<Subscription, Error> {
        client.subscribe_with(
            NAMESPACE,
            PREFIX,
            listener.endpoint(),
            SubscribeOptions {
                include_values: true,
                resume: cursor,
                max_frame: membus::MAX_FRAME,
            },
            "quota-register",
            deadline(budget),
        )
    }

    /// One registration lifetime. The caller retains the cursor and every
    /// unresolved cleanup report. Reusing a cursor requires intended store lineage;
    /// a fresh control connection does not establish that relationship.
    /// `output` is this example's handler: write AND flush must both succeed.
    pub fn session<T: Transport>(
        client: &mut Client<T>,
        listener: &mut ChangeListener,
        cursor: &mut Option<[u8; 16]>,
        output: &mut impl Write,
        limits: Limits,
    ) -> Report {
        let budget = limits.budget;
        let mut gap = false;
        let registration = match register(client, listener, *cursor, budget) {
            Err(Error::Refused(r)) if r.code == 23 && cursor.is_some() => {
                gap = true;
                register(client, listener, None, budget)
            }
            other => other,
        };
        let sub = match registration {
            Ok(sub) => sub,
            Err(error) => {
                // Invalid is pre-send; Refused is an associated definitive reply.
                // Every other error conservatively retains an unknown outcome.
                let cleanup = if matches!(error, Error::Invalid(_) | Error::Refused(_)) {
                    Cleanup::NotRegistered
                } else {
                    Cleanup::UnknownRegistration {
                        endpoint: listener.endpoint().into(),
                        error: error.to_string(),
                    }
                };
                listener.close();
                return Report {
                    end: End::Stop(format!("registration: {error}")),
                    cleanup,
                    handled: 0,
                    quiet_timeouts: 0,
                };
            }
        };
        // The returned cursor is the accepted starting position, including after
        // a refused resume. No change cursor is accepted before handler success.
        *cursor = Some(sub.cursor);
        let mut handled = 0;
        let mut quiet_timeouts = 0;
        let action = (|| -> Result<End, Box<dyn std::error::Error>> {
            writeln!(
                output,
                "registered token={} cursor={} live endpoint={}",
                hex(&sub.token),
                hex(&sub.cursor),
                listener.endpoint()
            )?;
            output.flush()?;
            if gap {
                let open = client.get(NAMESPACE, "open", "quota-open", deadline(budget))?;
                let resets = client.get(NAMESPACE, "resets", "quota-resets", deadline(budget))?;
                writeln!(
                    output,
                    "reconcile current-state open={open:?} resets={resets:?}"
                )?;
                output.flush()?;
            }
            while limits.events.is_none_or(|limit| handled < limit) {
                if limits.until.is_some_and(|end| Instant::now() >= end) {
                    return Ok(End::Stop("session deadline".into()));
                }
                let receive_until = limits.until.map_or(Instant::now() + limits.quiet, |end| {
                    end.min(Instant::now() + limits.quiet)
                });
                match listener.receive(&sub, NAMESPACE, PREFIX, receive_until) {
                    Ok(change) => {
                        writeln!(
                            output,
                            "change key={} version={} value={:?} cursor={}",
                            change.key,
                            change.version,
                            change.value,
                            hex(&change.cursor)
                        )?;
                        output.flush()?;
                        *cursor = Some(change.cursor);
                        handled += 1;
                    }
                    Err(ReceiveError::QuietTimeout) => {
                        quiet_timeouts += 1;
                        if limits.until.is_some_and(|end| Instant::now() >= end) {
                            return Ok(End::Stop("session deadline".into()));
                        }
                        if limits.until == Some(receive_until) {
                            continue; // A rounded I/O timeout can precede the owner bound.
                        }
                        // Quiet is not proof of failure. End this known lifetime
                        // explicitly, then resume on a fresh registration.
                        return Ok(End::Renew);
                    }
                    Err(ReceiveError::Interrupted(error)) => {
                        return Ok(End::Reconnect(error.to_string()));
                    }
                    Err(error) => return Ok(End::Stop(format!("listener: {error}"))),
                }
            }
            Ok(End::Complete)
        })();
        // Local close drains owned listener I/O, independently of unsubscribe.
        let endpoint = listener.endpoint().to_owned();
        listener.close();
        let cleanup = match client.unsubscribe(
            NAMESPACE,
            &sub.token,
            "quota-unsubscribe",
            deadline(budget),
        ) {
            Ok(()) => Cleanup::Confirmed { token: sub.token },
            Err(error) => Cleanup::KnownUnresolved {
                token: sub.token,
                endpoint,
                error: error.to_string(),
            },
        };
        Report {
            end: action
                .unwrap_or_else(|error| End::Stop(format!("handling/reconciliation: {error}"))),
            cleanup,
            handled,
            quiet_timeouts,
        }
    }

    /// The CLI's outer policy. `true` permits another lifetime on the same
    /// connection only; an unresolved report is returned intact in the error.
    pub fn continue_on_original(
        report: &Report,
        remaining: &mut Option<usize>,
    ) -> Result<bool, String> {
        if report.cleanup.unresolved() {
            return Err(format!(
                "automatic continuation stopped; end={:?}; unresolved obligation={:?}",
                report.end, report.cleanup
            ));
        }
        if let Some(left) = remaining.as_mut() {
            *left -= report.handled;
        }
        match &report.end {
            End::Complete => Ok(false),
            End::Renew => Ok(true),
            End::Stop(error) => Err(error.clone()),
            End::Reconnect(error) => Err(format!(
                "interrupted: {error}; automatic continuation stopped; no fresh control connection opened"
            )),
        }
    }

    pub fn main() -> Result<(), String> {
        let mut args = std::env::args().skip(1);
        let usage = "usage: subscribe_quota <resident-pipe> <listener-prefix> [--events N] [--cursor 32-hex-digits] [--quiet-ms N]";
        let resident = args.next().ok_or(usage)?;
        let listener_prefix = args.next().ok_or(usage)?;
        let mut quiet_ms = 10_000;
        let mut remaining = None;
        let mut cursor = None;
        while let Some(flag) = args.next() {
            let value = args.next().ok_or(usage)?;
            match flag.as_str() {
                "--events" => {
                    let n: usize = value.parse().map_err(|_| usage)?;
                    if n == 0 {
                        return Err("--events must be positive".into());
                    }
                    remaining = Some(n);
                }
                "--cursor" => {
                    if value.len() != 32 || !value.is_ascii() {
                        return Err(usage.into());
                    }
                    let mut bytes = [0; 16];
                    for (i, b) in bytes.iter_mut().enumerate() {
                        *b = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16).map_err(|_| usage)?;
                    }
                    cursor = Some(bytes);
                }
                "--quiet-ms" => {
                    quiet_ms = value.parse::<u64>().map_err(|_| usage)?;
                    if !(1..=3_600_000).contains(&quiet_ms) {
                        return Err("--quiet-ms must be between 1 and 3600000".into());
                    }
                }
                _ => return Err(usage.into()),
            }
        }
        let budget = Duration::from_secs(5);
        // Keep this exact connection for every permitted registration lifetime.
        // Reopening the same address cannot authenticate issuing-store continuity.
        let mut client = connect(Path::new(&resident), budget)
            .map_err(|error| format!("connect: {error}; no registration attempted"))?;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos();
        let mut generation = 0u64;
        loop {
            generation = generation
                .checked_add(1)
                .ok_or("listener generation exhausted")?;
            let endpoint = format!(
                "{listener_prefix}-{}-{stamp}-{generation}",
                std::process::id()
            );
            let mut listener = ChangeListener::bind(&endpoint, membus::MAX_FRAME)
                .map_err(|error| format!("bind: {error}; no new registration attempted"))?;
            let report = session(
                &mut client,
                &mut listener,
                &mut cursor,
                &mut std::io::stdout().lock(),
                Limits {
                    quiet: Duration::from_millis(quiet_ms),
                    budget,
                    events: remaining,
                    until: None,
                },
            );
            eprintln!(
                "local listener closed; remote cleanup={:?}; checkpoint={}",
                report.cleanup,
                cursor.as_ref().map(hex).unwrap_or_else(|| "none".into())
            );
            if !continue_on_original(&report, &mut remaining)? {
                return Ok(());
            }
            eprintln!(
                "quiet; continuing through known-registration renewal on original control connection"
            );
        }
    }
}

#[cfg(windows)]
fn main() {
    if let Err(error) = quota::main() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
#[cfg(not(windows))]
fn main() {
    eprintln!("subscribe_quota requires Windows named pipes");
    std::process::exit(1);
}
