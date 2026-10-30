use super::*;
use crate::{Config, Mutation};
use std::{
    path::PathBuf,
    sync::{Condvar, Mutex},
    time::Duration,
};

struct Unblock(Arc<(Mutex<(bool, bool)>, Condvar)>);
impl Drop for Unblock {
    fn drop(&mut self) {
        self.0 .0.lock().unwrap().1 = true;
        self.0 .1.notify_all();
    }
}

#[test]
fn pending_flush_and_release_retain_actual_data_charge_through_uncertainty() {
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let path = PathBuf::from(std::env::var_os("MEMBUS_NAMESPACE_RESERVE_TEST_ROOT").unwrap()).join(
        format!("pending-{}-{}", std::process::id(), crate::now_ms()),
    );
    let s = Store::open(
        &path,
        Config {
            cache_bytes: 0,
            signal_bytes: 0,
            state_limit_bytes: 500,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            flush_ms: 0,
            sync_test_gate: Some(gate.clone()),
            fail_sync: true,
            ..Config::default()
        },
    )
    .unwrap();
    let lease = s
        .reserve_namespace(
            "owned",
            ReserveRequest {
                bytes: 300,
                appends: 2,
                lifetime_ms: 60_000,
            },
        )
        .unwrap();
    std::thread::scope(|scope| {
        let _unblock_on_panic = Unblock(gate.clone());
        let store = &s;
        let job = scope.spawn(move || {
            store.mutate(Mutation {
                ns: "owned".into(),
                key: "k".into(),
                op_id: "grant".into(),
                deadline: crate::now_ms() + 30_000,
                ack: 2,
                expected: None,
                bytes: vec![1; 231],
                remove: false,
            })
        });
        let entered = gate
            .1
            .wait_timeout_while(gate.0.lock().unwrap(), Duration::from_secs(5), |v| !v.0)
            .unwrap();
        assert!(entered.0 .0, "actual flush seam was not reached");
        drop(entered);
        assert_eq!(lease.status().unwrap().admitted_bytes, 300);
        {
            let state = s.state.lock().unwrap();
            assert_eq!((state.pending_disk, state.persisted_bytes), (300, 0));
        }
        lease.release().unwrap();
        assert_eq!(
            s.mutate(Mutation {
                ns: "other".into(),
                key: "k".into(),
                op_id: "other".into(),
                deadline: crate::now_ms() + 30_000,
                ack: 1,
                expected: None,
                bytes: vec![2; 132],
                remove: false,
            })
            .code,
            7,
            "release cannot free pending append bytes"
        );
        gate.0.lock().unwrap().1 = true;
        gate.1.notify_all();
        assert_eq!(job.join().unwrap().code, 16);
    });
    assert_eq!(s.state_bytes(), 300);
    assert!(s.health2().read_only);
}
