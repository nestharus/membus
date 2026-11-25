use super::*;

fn argv(words: &[&str]) -> Vec<String> {
    words.iter().map(|s| (*s).to_owned()).collect()
}
fn server(extra: &[&str]) -> Vec<String> {
    let mut words = argv(&["membus", "unused-state", "tcp", "127.0.0.1:0"]);
    words.extend(extra.iter().map(|s| (*s).to_owned()));
    words
}
fn admitted(words: &[String]) -> Server {
    dispatch(words, |command| match command {
        Command::Server(command) => Ok(command),
        Command::Admin(_, _) => panic!("expected a server command"),
    })
    .unwrap()
}

#[allow(dead_code)] // Event vocabulary also names downstream native acquisitions.
#[derive(Debug, PartialEq, Eq)]
enum Event {
    StoreFactory,
    Timer,
    Heartbeat,
    Security,
    Listener,
    Worker,
    ReplaySummary,
    Ready,
}
#[derive(Default, Debug)]
struct Spy {
    calls: usize,
    events: Vec<Event>,
    stdout: Vec<u8>,
}
impl Spy {
    fn enter(&mut self) {
        self.calls += 1;
        self.events.push(Event::StoreFactory);
    }
    fn sentinel(&mut self, _: Command<'_>) -> io::Result<()> {
        self.enter();
        Err(io::Error::other("resource sentinel"))
    }
}
fn zero_resource_oracle(result: &io::Result<()>, spy: &Spy) -> bool {
    result.is_err() && spy.calls == 0 && spy.events.is_empty() && spy.stdout.is_empty()
}
fn rejected(words: &[String]) -> io::Error {
    let mut spy = Spy::default();
    // This is the same dispatch function used by main, replacing only its
    // effect adapter. A factory invocation is already a failure of admission.
    let result = dispatch(words, |command| spy.sentinel(command));
    assert!(zero_resource_oracle(&result, &spy), "{words:?}: {spy:?}");
    result.unwrap_err()
}

#[test]
fn valid_command_reaches_one_factory_only_after_complete_admission() {
    let words = server(&[
        "--heartbeat-ms",
        "0",
        "--flush-ms",
        "0",
        "--idle-ms",
        "5000",
    ]);
    let mut spy = Spy::default();
    let result = dispatch(&words, |command| {
        let Command::Server(command) = command else {
            panic!("server")
        };
        assert_eq!(command.config.flush_ms, 0);
        assert_eq!(command.heartbeat_ms, 0);
        assert_eq!(command.idle_ms, 5000);
        spy.sentinel(Command::Server(command))
    });
    assert_eq!(result.unwrap_err().to_string(), "resource sentinel");
    assert_eq!(spy.calls, 1);
    assert_eq!(spy.events, [Event::StoreFactory]);
    assert!(spy.stdout.is_empty());
}

#[test]
fn unsupported_transport_never_enters_the_resource_factory() {
    for transport in ["bogus", "TCP", "PIPE", ""] {
        let words = argv(&["membus", "unused-state", transport, "unused-address"]);
        assert_eq!(rejected(&words).to_string(), "unsupported transport");
    }
}

#[cfg(not(windows))]
#[test]
fn unavailable_pipe_platform_refuses_before_any_factory() {
    let words = argv(&["membus", "unused-state", "pipe", "unused-pipe"]);
    assert_eq!(rejected(&words).to_string(), "unsupported transport");
}

#[cfg(windows)]
#[test]
fn maintained_windows_caller_and_delegated_pipe_names_remain_admissible() {
    let words = argv(&[
        "membus",
        "unused-state",
        "pipe",
        r"\\.\pipe\unused-example",
        "--heartbeat-ms",
        "0",
        "--flush-ms",
        "0",
        "--idle-ms",
        "5000",
        "--delay-ms",
        "7",
    ]);
    let command = admitted(&words);
    assert_eq!(command.transport, Transport::Pipe);
    assert_eq!(command.address, r"\\.\pipe\unused-example");
    assert_eq!(command.config.write_delay_ms, 7);
    // No new name parser is substituted for CreateNamedPipeW. Admission here
    // does not claim that the OS will accept or make this endpoint available.
    let delegated = argv(&["membus", "unused-state", "pipe", "native-name-validation"]);
    assert_eq!(admitted(&delegated).address, "native-name-validation");
}

#[test]
fn defaults_match_the_maintained_server_contract() {
    let command = admitted(&server(&[]));
    let c = &command.config;
    assert_eq!((command.idle_ms, command.heartbeat_ms), (60000, 30000));
    assert_eq!(
        (c.state_limit_bytes, c.maintenance_reserve_bytes),
        (8589934592, 1073741824)
    );
    assert_eq!(
        (c.quota, c.index_cap, c.capacity),
        (500000, 917000, 536870912)
    );
    assert_eq!(
        (c.cache_bytes, c.queue_bytes, c.dedup_cap),
        (134217728, 67108864, 262144)
    );
    assert_eq!(
        (c.snapshot_bytes, c.snapshot_max, c.snapshot_ttl_ms),
        (268435456, 8, 30000)
    );
    assert_eq!((c.signal_history, c.signal_bytes), (4096, 16777216));
    assert_eq!(
        (c.retry_ms, c.flush_ms, c.write_delay_ms),
        (86400000, 50, 0)
    );
    assert_eq!(
        (c.chunk_bytes, c.rotation_ms, c.retire_ms),
        (8388608, 3600000, 10000)
    );
    assert!(!c.allow_mem);
    assert!(!c.fail_after_append && !c.fail_recovery_sync && !c.import_inject_difference);
    assert_eq!(
        c.cache_rules
            .iter()
            .map(|r| r.label.as_str())
            .collect::<Vec<_>>(),
        ["ledger", "metrics", "logs", "evals"]
    );
}

#[test]
fn every_scalar_option_maps_to_its_existing_field() {
    let words = server(&[
        "--signal-history",
        "1",
        "--signal-bytes",
        "2",
        "--state-limit-bytes",
        "100",
        "--idle-ms",
        "3",
        "--heartbeat-ms",
        "100",
        "--flush-ms",
        "4",
        "--retry-ms",
        "5",
        "--quota",
        "6",
        "--index-cap",
        "7",
        "--capacity",
        "8",
        "--cache-bytes",
        "9",
        "--queue-bytes",
        "10",
        "--dedup-cap",
        "11",
        "--delay-ms",
        "12",
        "--snapshot-bytes",
        "13",
        "--snapshot-max",
        "14",
        "--snapshot-ttl-ms",
        "15",
        "--maintenance-reserve-bytes",
        "16",
        "--chunk-bytes",
        "17",
        "--rotation-ms",
        "18",
        "--retire-ms",
        "19",
        "--allow-mem",
    ]);
    let command = admitted(&words);
    let c = &command.config;
    assert_eq!(
        (c.signal_history, c.signal_bytes, c.state_limit_bytes),
        (1, 2, 100)
    );
    assert_eq!((command.idle_ms, command.heartbeat_ms), (3, 100));
    assert_eq!((c.flush_ms, c.retry_ms, c.quota, c.index_cap), (4, 5, 6, 7));
    assert_eq!(
        (c.capacity, c.cache_bytes, c.queue_bytes, c.dedup_cap),
        (8, 9, 10, 11)
    );
    assert_eq!(
        (
            c.write_delay_ms,
            c.snapshot_bytes,
            c.snapshot_max,
            c.snapshot_ttl_ms
        ),
        (12, 13, 14, 15)
    );
    assert_eq!(
        (
            c.maintenance_reserve_bytes,
            c.chunk_bytes,
            c.rotation_ms,
            c.retire_ms
        ),
        (16, 17, 18, 19)
    );
    assert!(c.allow_mem);
    // Store still owns signal-budget normalization; admission changes no value.
    assert_eq!(c.signal_bytes, 2);
}

#[test]
fn arity_help_and_usage_remain_errors_without_acquisition() {
    for words in [
        argv(&[]),
        argv(&["membus"]),
        argv(&["membus", "state"]),
        argv(&["membus", "state", "tcp"]),
        argv(&["membus", "--help"]),
    ] {
        assert_eq!(rejected(&words).to_string(), USAGE);
    }
    for text in [
        "--cache-policy",
        "--cache-bytes",
        "--signal-history",
        "--signal-bytes",
        "health PIPE --state-dir PATH",
        "retirement-status PIPE",
        "catalogue-state",
    ] {
        assert!(USAGE.contains(text));
    }
    // Neither spelling becomes a forced new command or success-help alias.
    for path in ["serve", "--help", "./health"] {
        let words = argv(&["membus", path, "tcp", "127.0.0.1:0"]);
        assert_eq!(admitted(&words).path, PathBuf::from(path));
    }
}

#[test]
fn unknown_missing_and_numeric_error_precedence_is_preserved() {
    for suffix in [
        &["--unknown"][..],
        &["--"][..],
        &["--quota=7"][..],
        &["trailing"][..],
    ] {
        assert_eq!(rejected(&server(suffix)).to_string(), "missing value");
    }
    assert_eq!(
        rejected(&server(&["--unknown", "7"])).to_string(),
        "unknown option"
    );
    let e = rejected(&server(&["--unknown", "not-u64"]));
    assert!(e.get_ref().unwrap().is::<std::num::ParseIntError>());
    assert_eq!(
        rejected(&server(&["--cache-policy"])).to_string(),
        "missing cache policy"
    );
    assert!(rejected(&server(&["--quota", "--allow-mem"]))
        .get_ref()
        .unwrap()
        .is::<std::num::ParseIntError>());
}

#[test]
fn eager_numeric_edges_and_legacy_zero_values() {
    for n in ["1", "4294967294"] {
        admitted(&server(&["--idle-ms", n]));
    }
    for n in ["0", "4294967295", "18446744073709551615"] {
        assert_eq!(
            rejected(&server(&["--idle-ms", n])).to_string(),
            "idle-ms must be 1..4294967294"
        );
    }
    for n in ["0", "100", "18446744073709551615"] {
        admitted(&server(&["--heartbeat-ms", n]));
    }
    assert_eq!(
        rejected(&server(&["--heartbeat-ms", "99"])).to_string(),
        "heartbeat-ms must be zero or >=100"
    );
    for flag in ["--state-limit-bytes", "--chunk-bytes"] {
        rejected(&server(&[flag, "0"]));
        admitted(&server(&[flag, "1"]));
    }
    for flag in [
        "--flush-ms",
        "--retry-ms",
        "--quota",
        "--index-cap",
        "--capacity",
        "--cache-bytes",
        "--queue-bytes",
        "--dedup-cap",
        "--delay-ms",
        "--snapshot-bytes",
        "--snapshot-max",
        "--signal-history",
        "--signal-bytes",
        "--rotation-ms",
        "--retire-ms",
    ] {
        admitted(&server(&[flag, "0"]));
    }
    for n in ["-1", "18446744073709551616", "1ms", " 1"] {
        assert!(rejected(&server(&["--retry-ms", n]))
            .get_ref()
            .unwrap()
            .is::<std::num::ParseIntError>());
    }
    assert_eq!(
        admitted(&server(&["--retry-ms", "+0001"])).config.retry_ms,
        1
    );
}

#[test]
fn checked_platform_width_and_existing_casts_are_distinct() {
    for flag in [
        "--cache-bytes",
        "--signal-history",
        "--signal-bytes",
        "--snapshot-bytes",
        "--snapshot-max",
    ] {
        #[cfg(target_pointer_width = "32")]
        rejected(&server(&[flag, "4294967296"]));
        #[cfg(target_pointer_width = "64")]
        admitted(&server(&[flag, "18446744073709551615"]));
    }
    let c = admitted(&server(&[
        "--quota",
        "4294967296",
        "--index-cap",
        "4294967296",
        "--capacity",
        "4294967296",
        "--queue-bytes",
        "4294967296",
        "--dedup-cap",
        "4294967296",
    ]))
    .config;
    #[cfg(target_pointer_width = "32")]
    assert_eq!(
        (c.quota, c.index_cap, c.capacity, c.queue_bytes, c.dedup_cap),
        (0, 0, 0, 0, 0)
    );
    #[cfg(target_pointer_width = "64")]
    assert_eq!(
        (c.quota, c.index_cap, c.capacity, c.queue_bytes, c.dedup_cap),
        (4294967296, 4294967296, 4294967296, 4294967296, 4294967296)
    );
}

#[test]
fn scalar_and_allow_mem_repeats_keep_the_last_valid_value() {
    let c = admitted(&server(&[
        "--quota",
        "3",
        "--quota",
        "4",
        "--allow-mem",
        "--allow-mem",
    ]))
    .config;
    assert_eq!(c.quota, 4);
    assert!(c.allow_mem);
    for suffix in [
        &["--idle-ms", "0", "--idle-ms", "1"][..],
        &["--heartbeat-ms", "99", "--heartbeat-ms", "100"][..],
        &["--state-limit-bytes", "0", "--state-limit-bytes", "8"][..],
        &["--chunk-bytes", "0", "--chunk-bytes", "8"][..],
    ] {
        rejected(&server(suffix));
    }
    // TTL has always been a final Config check, not an eager occurrence check.
    assert_eq!(
        admitted(&server(&[
            "--snapshot-ttl-ms",
            "0",
            "--snapshot-ttl-ms",
            "1"
        ]))
        .config
        .snapshot_ttl_ms,
        1
    );
}

#[test]
fn reserve_uses_final_values_and_the_small_limit_default() {
    let c = admitted(&server(&[
        "--state-limit-bytes",
        "8",
        "--maintenance-reserve-bytes",
        "7",
    ]))
    .config;
    assert_eq!((c.state_limit_bytes, c.maintenance_reserve_bytes), (8, 7));
    rejected(&server(&[
        "--maintenance-reserve-bytes",
        "8",
        "--state-limit-bytes",
        "8",
    ]));
    let c = admitted(&server(&[
        "--state-limit-bytes",
        "8",
        "--state-limit-bytes",
        "16",
    ]))
    .config;
    assert_eq!(c.maintenance_reserve_bytes, 2);
    assert_eq!(
        admitted(&server(&["--state-limit-bytes", "1"]))
            .config
            .maintenance_reserve_bytes,
        0
    );
}

#[test]
fn snapshot_lifetime_and_cache_config_refuse_before_store_marker_access() {
    assert_eq!(
        rejected(&server(&["--snapshot-ttl-ms", "0"])).to_string(),
        "snapshot lifetime must be positive"
    );
    for label in ["", "a=b", "a\nb", "a\rb"] {
        let rule = format!("logs,,*,{label},1,2,true,false");
        assert_eq!(
            rejected(&server(&["--cache-policy", &rule])).to_string(),
            "invalid cache rules or more than 64 rules"
        );
    }
}

#[test]
fn cache_policy_repeats_append_in_the_existing_order() {
    let c = admitted(&server(&[
        "--cache-policy",
        "logs,event/,*,first,1,2,true,false",
        "--cache-policy",
        "logs,event/,expire:7,last,3,4,false,true",
    ]))
    .config;
    assert_eq!(
        c.cache_rules
            .iter()
            .map(|r| r.label.as_str())
            .collect::<Vec<_>>(),
        ["ledger", "metrics", "logs", "evals", "first", "last"]
    );
    let last = c.cache_rules.last().unwrap();
    assert_eq!(last.namespace.as_deref(), Some("logs"));
    assert_eq!(last.prefix, "event/");
    assert_eq!(last.class, Some(membus::chunks::Class::expire(7)));
    assert_eq!((last.policy.initial_ttl, last.policy.active_ttl), (3, 4));
    assert!(!last.policy.refresh_on_read && last.policy.never_cache);
}

#[test]
fn cache_syntax_is_not_replaced_by_another_parser() {
    for rule in [
        "short",
        "logs,,bad,c,1,2,true,false",
        "logs,,expire:0,c,1,2,true,false",
        "logs,,*,c,x,2,true,false",
        "logs,,*,c,1,2,once,false",
    ] {
        rejected(&server(&["--cache-policy", rule]));
    }
    let c = admitted(&server(&["--cache-policy", ",,*,c,0,0,false,false"])).config;
    assert_eq!(c.cache_rules.last().unwrap().namespace.as_deref(), Some(""));
}

#[test]
fn cache_aggregate_boundary_counts_defaults_and_preserves_repeats() {
    let mut words = server(&[]);
    for _ in 0..60 {
        words.extend(argv(&["--cache-policy", "*,x,*,repeated,0,0,true,false"]));
    }
    assert_eq!(admitted(&words).config.cache_rules.len(), 64);
    words.extend(argv(&["--cache-policy", "*,x,*,repeated,0,0,true,false"]));
    assert_eq!(
        rejected(&words).to_string(),
        "invalid cache rules or more than 64 rules"
    );
}

#[test]
fn cache_selector_bounds_are_bytes_and_accept_the_exact_boundary() {
    for field in 0..3 {
        for (text, valid) in [
            ("a".repeat(1024), true),
            ("a".repeat(1025), false),
            ("\u{00e9}".repeat(512), true),
            ("\u{00e9}".repeat(513), false),
        ] {
            let mut fields = ["logs".to_owned(), "prefix".to_owned(), "label".to_owned()];
            fields[field] = text;
            let rule = format!("{},{},*,{},1,2,true,false", fields[0], fields[1], fields[2]);
            let words = server(&["--cache-policy", &rule]);
            if valid {
                admitted(&words);
            } else {
                rejected(&words);
            }
        }
    }
}

#[test]
fn literal_loopback_policy_is_early_but_resolution_is_delegated_once() {
    for address in [
        "127.0.0.1:0",
        "127.1.2.3:65535",
        "[::1]:0",
        "localhost:0",
        "prospective-hostname:17",
        "::1:0",
    ] {
        let words = argv(&["membus", "unused-state", "tcp", address]);
        assert_eq!(admitted(&words).address, address);
    }
    for address in ["0.0.0.0:0", "192.0.2.1:1", "[::]:0", "[2001:db8::1]:9"] {
        let words = argv(&["membus", "unused-state", "tcp", address]);
        assert_eq!(rejected(&words).to_string(), "TCP must bind loopback");
    }
}

#[cfg(not(all(target_vendor = "fortanix", target_env = "sgx")))]
#[test]
fn default_std_address_syntax_refuses_without_resolving_or_binding() {
    for address in ["", "localhost", "127.0.0.1"] {
        let words = argv(&["membus", "unused-state", "tcp", address]);
        let e = rejected(&words);
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(e.to_string(), "invalid socket address");
    }
    for address in [
        "localhost:",
        "localhost:65536",
        "localhost:-1",
        "localhost:service",
    ] {
        let words = argv(&["membus", "unused-state", "tcp", address]);
        let e = rejected(&words);
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(e.to_string(), "invalid port value");
    }
    assert_eq!(
        admitted(&argv(&["membus", "unused-state", "tcp", "localhost:+0"])).address,
        "localhost:+0"
    );
}

#[test]
fn all_five_admin_routes_keep_their_exact_borrowed_arguments() {
    for (name, admin) in [
        ("retirement-status", Admin::Retirement),
        ("catalogue-state", Admin::Catalogue),
        ("health", Admin::Health),
        ("policy", Admin::Policy),
        ("expiry", Admin::Expiry),
    ] {
        let words = argv(&["membus", name, "verbatim", "--unknown", "untouched"]);
        dispatch(&words, |command| {
            let Command::Admin(actual, args) = command else {
                panic!("admin")
            };
            assert_eq!(actual, admin);
            assert_eq!(args, &words[2..]);
            assert_eq!(args.as_ptr(), words[2..].as_ptr());
            Ok(())
        })
        .unwrap();
        // Existing command owners, including non-Windows refusal ordering,
        // retain their own arity checks; server validation must not intercept.
        dispatch(&argv(&["membus", name]), |command| {
            assert!(matches!(command, Command::Admin(_, [])));
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn dynamic_owner_failure_is_preserved_after_admission() {
    let words = server(&[]);
    for message in [
        "port unavailable",
        "permission denied",
        "quarantined recovery",
        "required recovery sync failed",
        "unknown prior outcome",
    ] {
        let mut calls = 0;
        let result: io::Result<()> = dispatch(&words, |_| {
            calls += 1;
            Err(io::Error::other(message))
        });
        assert_eq!(calls, 1);
        assert_eq!(result.unwrap_err().to_string(), message);
    }
}

#[test]
fn the_same_zero_resource_oracle_rejects_an_old_order_counterexample() {
    let words = argv(&["membus", "unused-state", "bogus", "unused-address"]);
    let mut old_order = Spy::default();
    // A bounded harmless stand-in for the old order: enter the resource
    // adapter before transport validation, then return the same refusal.
    old_order.enter();
    old_order.events.extend([
        Event::Worker,
        Event::ReplaySummary,
        Event::Timer,
        Event::Heartbeat,
        Event::Ready,
    ]);
    let result = dispatch(&words, |_| Ok(()));
    assert_eq!(
        result.as_ref().unwrap_err().to_string(),
        "unsupported transport"
    );
    assert!(!zero_resource_oracle(&result, &old_order));
    // The production order must satisfy that identical oracle for this input.
    assert_eq!(rejected(&words).to_string(), "unsupported transport");
}
