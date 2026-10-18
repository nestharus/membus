//! Value-bearing quota change waiter using the public resident client.
//!
//! Windows: `cargo run --example subscribe_quota -- <resident-pipe> <listener-pipe>`.
//! Both endpoints are explicit; this example starts no service. Add `--events N`
//! for a finite run, or `--cursor HEX` to resume a previously printed cursor.
//! Successful output handling precedes cursor advancement. Persist that cursor in
//! a real application's own durable checkpoint after its useful work succeeds.
//!
//! A gap starts a live subscription before observing `quota/open` and
//! `quota/resets`. These separate GETs are current state, not an atomic snapshot
//! or recovery of every missed event. Quiet time alone is not a disconnect.
//! Unknown registration stops automatic attempts. Failed known-token cleanup is
//! retained across reconnects; endpoint closure never proves remote settlement.
#[cfg(windows)]
pub mod quota {
    use membus::client::{
        ChangeListener, Client, Deadline, Error, Options, ReceiveError, SubscribeOptions,
        Subscription, Transport,
    };
    use std::{
        io::Write,
        path::Path,
        time::{Duration, Instant},
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
        Reconnect(String),
        Stop(String),
    }
    #[derive(Debug)]
    pub struct Report {
        pub end: End,
        pub cleanup: Cleanup,
        pub handled: usize,
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
    /// unresolved cleanup report, including when starting another connection.
    /// `output` is this example's handler: write AND flush must both succeed.
    pub fn session<T: Transport>(
        client: &mut Client<T>,
        listener: &mut ChangeListener,
        cursor: &mut Option<[u8; 16]>,
        output: &mut impl Write,
        quiet: Duration,
        budget: Duration,
        events: Option<usize>,
    ) -> Report {
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
                };
            }
        };
        // The returned cursor is the accepted starting position, including after
        // a refused resume. No change cursor is accepted before handler success.
        *cursor = Some(sub.cursor);
        let mut handled = 0;
        let action = (|| -> Result<End, Box<dyn std::error::Error>> {
            writeln!(
                output,
                "registered token={} cursor={} live",
                hex(&sub.token),
                hex(&sub.cursor)
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
            while events.is_none_or(|limit| handled < limit) {
                match listener.receive(&sub, NAMESPACE, PREFIX, Instant::now() + quiet) {
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
                    Err(ReceiveError::QuietTimeout) => continue,
                    Err(ReceiveError::Interrupted(error)) => {
                        return Ok(End::Reconnect(error.to_string()))
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
        }
    }

    pub fn main() -> Result<(), String> {
        let mut args = std::env::args().skip(1);
        let usage = "usage: subscribe_quota <resident-pipe> <listener-pipe> [--events N] [--cursor 32-hex-digits]";
        let resident = args.next().ok_or(usage)?;
        let endpoint = args.next().ok_or(usage)?;
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
                _ => return Err(usage.into()),
            }
        }
        let budget = Duration::from_secs(5);
        let mut obligations = Vec::new();
        loop {
            let mut client = match connect(Path::new(&resident), budget) {
                Ok(c) => c,
                Err(Error::Transport(error)) => {
                    eprintln!("connect: {error}; waiting before reconnect; cursor={cursor:?}");
                    std::thread::sleep(Duration::from_secs(1));
                    continue;
                }
                Err(Error::Invalid("connect deadline")) => {
                    eprintln!("resident unavailable; waiting before reconnect; cursor={cursor:?}; retained obligations={obligations:?}");
                    std::thread::sleep(Duration::from_secs(1));
                    continue;
                }
                Err(error) => {
                    return Err(format!(
                        "connect: {error}; retained obligations={obligations:?}"
                    ))
                }
            };
            let mut listener = ChangeListener::bind(&endpoint, membus::MAX_FRAME)
                .map_err(|error| format!("bind: {error}; retained obligations={obligations:?}"))?;
            let report = session(
                &mut client,
                &mut listener,
                &mut cursor,
                &mut std::io::stdout().lock(),
                Duration::from_secs(60),
                budget,
                remaining,
            );
            eprintln!(
                "local listener closed; remote cleanup={:?}; checkpoint={}",
                report.cleanup,
                cursor.as_ref().map(hex).unwrap_or_else(|| "none".into())
            );
            if report.cleanup.unresolved() {
                obligations.push(report.cleanup);
            }
            if let Some(left) = remaining.as_mut() {
                *left -= report.handled;
            }
            match report.end {
                End::Complete if obligations.is_empty() => return Ok(()),
                End::Complete => {
                    return Err(format!(
                        "handled requested events; unresolved obligations={obligations:?}"
                    ))
                }
                End::Stop(error) => {
                    return Err(format!("{error}; unresolved obligations={obligations:?}"))
                }
                End::Reconnect(error) => {
                    eprintln!("interrupted: {error}; retained obligations={obligations:?}");
                    drop(client);
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
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
