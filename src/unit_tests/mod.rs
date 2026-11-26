//! Crate-level white-box unit tests consolidated under `src/unit_tests/`.
#[allow(unused_imports)]
use crate::{
    append_owner::{Append, AppendHandle, AppendOwner},
    cache, chunks,
    codec::{self, mutation_bytes},
    flush_owner::{enqueue_flush, FlushHandle, FlushOwner, FlushWait},
    log_io::{self, sync_file},
    maintenance::Maint,
    read_cohort::{self, tests as read_work_tests},
    replay,
    routing::{self, stripe, ChunkRouter, FixedRoute, Route},
    snapshots::{self, ScanState, ScanUsage},
    store::{start_owners, Flags, Key, State},
    *,
};
#[allow(unused_imports)]
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc, Arc, Condvar, Mutex, RwLock,
    },
    time::{Duration, Instant},
};

#[path = "store/flush_owner_tests.rs"]
mod flush_policy_tests;

#[path = "store/namespace_release_tests.rs"]
mod namespace_release_tests;

#[path = "store/snapshots_tests.rs"]
mod scan_retention_tests;

#[path = "maintenance/tests.rs"]
mod retirement_failure_tests;

#[path = "store/receiving_correction_tests.rs"]
mod receiving_correction_tests;

#[path = "store/policy_recovery_tests.rs"]
mod policy_recovery_tests;

#[path = "store/cold_load_tests.rs"]
mod cold_load_tests;

#[path = "store/value_disk_tests.rs"]
mod cold_identity_tests;
