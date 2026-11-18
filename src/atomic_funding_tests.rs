use super::*;

fn original_bytes(count: usize) -> Vec<u8> {
    let mut b = b"MAR1".to_vec();
    crate::lp(&mut b, b"authored-population");
    crate::lp(&mut b, b"enqueue-1");
    b.extend(19u64.to_le_bytes());
    b.extend(1200u64.to_le_bytes());
    b.push(2);
    b.extend((count as u32).to_le_bytes());
    for i in 0..count {
        crate::lp(&mut b, format!("retained-{i:08}").as_bytes());
        b.extend((i as u64 + 1).to_le_bytes());
    }
    b.extend(4u16.to_le_bytes());
    for (k, v) in [
        ("allocator", "4162"),
        ("ticket-4162", "waiting"),
        ("native-4162", "unsettled"),
        ("launch-4162", "not-started"),
    ] {
        crate::lp(&mut b, k.as_bytes());
        b.push(0);
        crate::lp(&mut b, v.as_bytes());
    }
    b
}

#[test]
fn complete_4161_original_is_conserved_through_public_decode() {
    let b = original_bytes(4161);
    let r = AtomicRequest::from_bytes(&b).expect("finite complete original");
    assert_eq!(r.to_bytes(), b);
}

fn authored_rows(count: usize) -> Vec<(String, Value)> {
    (0..count)
        .map(|i| {
            (
                format!("retained-{i:08}"),
                Value {
                    version: i as u64 + 1,
                    ack: 2,
                    bytes: format!("retained id {i}").into_bytes(),
                    tombstone: false,
                },
            )
        })
        .collect()
}
fn captured(
    rows: &[(String, Value)],
    budget: OriginalBudget,
) -> Result<NamespaceView, PublicationError> {
    capture_complete(
        "authored-population",
        19,
        1200,
        budget,
        rows.iter().map(|(k, v)| (k.as_str(), v.bytes.len(), v)),
        |_, v| Ok(v.clone()),
    )
}
fn four_changes(id: usize) -> Vec<FinalChange> {
    vec![
        FinalChange::put("allocator", id.to_string().into_bytes()),
        FinalChange::put(&format!("ticket-{id}"), b"waiting".to_vec()),
        FinalChange::put(&format!("native-{id}"), b"unsettled".to_vec()),
        FinalChange::put(&format!("launch-{id}"), b"not-started".to_vec()),
    ]
}
#[test]
fn full_4161_capture_four_changes_and_next_enqueue() {
    let mut rows = authored_rows(4161);
    // Historical allocator authority is an authored domain fact, not invented
    // from missing live IDs. It is independently larger than all retained IDs.
    rows.push((
        "allocator".into(),
        Value {
            version: 5000,
            ack: 2,
            bytes: b"9000".to_vec(),
            tombstone: false,
        },
    ));
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let budget = OriginalBudget::new(4 * 1024 * 1024);
    let view = captured(&rows, budget.clone()).unwrap();
    assert_eq!(view.rows().len(), 4162);
    assert!(view.rows().iter().any(|(k, _)| k == "retained-00004160"));
    let next = std::str::from_utf8(&view.rows()[0].1.bytes)
        .unwrap()
        .parse::<usize>()
        .unwrap()
        + 1;
    assert_eq!(next, 9001);
    let r = view
        .prepare("enqueue-1", AtomicAck::Flush, four_changes(next))
        .unwrap();
    assert!(r.matches_population(rows.iter().map(|(k, v)| (k.as_str(), v.version))));
    let bytes = r.to_bytes();
    let b = BorrowedOriginal::from_bytes(&bytes).unwrap();
    assert_eq!(b.population().count(), 4162);
    assert_eq!(b.changes().count(), 4);
    assert_eq!(b.epoch(), 19);
    assert_eq!(b.expires_mono(), 1200);
    assert_eq!(b.requested_ack(), AtomicAck::Flush);
    // Authored committed-final oracle, not Store execution or append evidence.
    for (i, c) in r.data.changes.iter().enumerate() {
        rows.retain(|(k, _)| k != &c.key);
        rows.push((
            c.key.clone(),
            Value {
                version: 6000 + i as u64,
                ack: 2,
                bytes: c.value.clone().unwrap(),
                tombstone: false,
            },
        ));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let next_view = captured(&rows, budget).unwrap();
    assert_eq!(next_view.rows().len(), 4165);
    let next = std::str::from_utf8(&next_view.rows()[0].1.bytes)
        .unwrap()
        .parse::<usize>()
        .unwrap()
        + 1;
    assert_eq!(next, 9002);
    assert_eq!(
        next_view
            .prepare("enqueue-2", AtomicAck::Os, four_changes(next))
            .unwrap()
            .data
            .changes
            .len(),
        4
    );
}
#[test]
fn insufficient_capture_refuses_before_read_and_releases_all_reservations() {
    let rows = authored_rows(4161);
    let budget = OriginalBudget::new(10);
    let calls = std::cell::Cell::new(0);
    let result = capture_complete(
        "authored",
        1,
        100,
        budget.clone(),
        rows.iter().map(|(k, v)| (k.as_str(), v.bytes.len(), v)),
        |_, v| {
            calls.set(calls.get() + 1);
            Ok(v.clone())
        },
    );
    assert!(matches!(result, Err(PublicationError::Resource(7))));
    assert_eq!(calls.get(), 0);
    assert_eq!(budget.used(), 0);
}
#[test]
fn insufficient_owned_decode_is_atomic_and_invalid_counts_allocate_no_owner() {
    let bytes = original_bytes(4161);
    let borrowed = BorrowedOriginal::from_bytes(&bytes).unwrap();
    let required = borrowed.allocation_bytes().unwrap();
    let budget = OriginalBudget::new(required - 1);
    assert!(matches!(
        AtomicRequest::from_bytes_funded(&bytes, &budget),
        Err(PublicationError::Resource(7))
    ));
    assert_eq!(budget.used(), 0);
    let budget = OriginalBudget::new(required);
    let r = AtomicRequest::from_bytes_funded(&bytes, &budget).unwrap();
    assert_eq!(budget.used(), required);
    let clone = r.clone();
    drop(r);
    assert_eq!(budget.used(), required);
    assert_eq!(clone.to_bytes(), bytes);
    drop(clone);
    assert_eq!(budget.used(), 0);
}
#[test]
fn store_reservation_survives_view_original_and_last_clone() {
    let usage = Arc::new(crate::snapshots::ScanUsage::default());
    let limit = 2 * 1024 * 1024;
    usage.bytes.store(limit, Ordering::Release);
    usage.count.store(1, Ordering::Release);
    let budget = OriginalBudget::backed(
        limit,
        Some(crate::snapshots::ScanCharge {
            usage: usage.clone(),
            bytes: limit,
        }),
    );
    let rows = authored_rows(4161);
    let view = captured(&rows, budget.clone()).unwrap();
    let original = view
        .prepare("original", AtomicAck::Flush, four_changes(9001))
        .unwrap();
    let clone = original.clone();
    assert!(Arc::ptr_eq(&original.data, &clone.data));
    let full = budget.used();
    drop(view);
    assert!(budget.used() < full);
    drop(original);
    drop(budget);
    assert_eq!(usage.bytes.load(Ordering::Acquire), limit);
    assert_eq!(usage.count.load(Ordering::Acquire), 1);
    assert_eq!(
        BorrowedOriginal::from_bytes(&clone.to_bytes())
            .unwrap()
            .population()
            .count(),
        4161
    );
    drop(clone);
    assert_eq!(usage.bytes.load(Ordering::Acquire), 0);
    assert_eq!(usage.count.load(Ordering::Acquire), 0);
}
#[test]
fn multiple_originals_share_guards_but_reserve_each_body() {
    let rows = authored_rows(10);
    let (_, row_bytes, guard_bytes) = capture_measure(
        "authored-population",
        rows.iter().map(|(k, v)| (k.as_str(), v.bytes.len())),
    )
    .unwrap();
    let changes = four_changes(9001);
    let body = request_charge(
        "authored-population",
        "one",
        changes.iter().map(|c| (c.key.as_str(), c.value.as_deref())),
    )
    .unwrap();
    let budget = OriginalBudget::new(row_bytes + guard_bytes + 2 * body);
    let view = captured(&rows, budget.clone()).unwrap();
    let first = view
        .prepare("one", AtomicAck::Os, four_changes(9001))
        .unwrap();
    let second = view
        .prepare("two", AtomicAck::Os, four_changes(9002))
        .unwrap();
    assert!(Arc::ptr_eq(&first.data.population, &second.data.population));
    assert!(!Arc::ptr_eq(&first.data, &second.data));
    assert!(matches!(
        view.prepare("three", AtomicAck::Os, four_changes(9003)),
        Err(PublicationError::Resource(7))
    ));
    drop(first);
    assert!(view
        .prepare("new", AtomicAck::Os, four_changes(9003))
        .is_ok());
}
#[test]
fn full_guard_rejects_drift_collision_and_all_phantoms() {
    let original = AtomicRequest::from_bytes(&original_bytes(4161)).unwrap();
    let population = original.data.population.rows.clone();
    assert!(original.matches_population(population.iter().map(|(k, v)| (k.as_str(), *v))));
    let mut changed = population.clone();
    changed[4000].1 += 1;
    assert!(!original.matches_population(changed.iter().map(|(k, v)| (k.as_str(), *v))));
    changed = population.clone();
    changed.pop();
    assert!(!original.matches_population(changed.iter().map(|(k, v)| (k.as_str(), *v))));
    changed = population.clone();
    changed.push(("ticket-4162".into(), 9000));
    changed.sort();
    assert!(!original.matches_population(changed.iter().map(|(k, v)| (k.as_str(), *v))));
    changed = population.clone();
    changed[4000].0 = "replacement".into();
    changed.sort();
    assert!(!original.matches_population(changed.iter().map(|(k, v)| (k.as_str(), *v))));
}
#[test]
fn concealed_retained_identity_cannot_pass_complete_guard() {
    let bytes = original_bytes(4161);
    let original = AtomicRequest::from_bytes(&bytes).unwrap();
    let mut actual = original.data.population.rows.clone();
    actual.push(("retained-99999999".into(), 9000));
    assert!(
        !original.matches_population(actual.iter().map(|(k, v)| (k.as_str(), *v))),
        "hidden retained identity must conflict"
    );
}
#[test]
fn borrowed_envelope_route_replay_and_every_cold_selection_agree() {
    let bytes = original_bytes(4161);
    let r = AtomicRequest::from_bytes(&bytes).unwrap();
    let envelope = envelope_bytes(&r, 100, 200);
    let e = decode_envelope(&envelope).unwrap();
    assert_eq!(e.request.fingerprint(), r.fingerprint());
    assert_eq!(e.request.population().count(), 4161);
    assert_eq!(e.request.bytes, bytes);
    let routed = route_records(&envelope).unwrap();
    assert_eq!(routed.len(), 4);
    let replayed: Vec<_> = e.request.mutations().collect();
    for (i, m) in replayed.iter().enumerate() {
        let selected = subrecord(&envelope, i as u16).unwrap();
        let (cold, version, at) = codec::parse_record(&selected).unwrap();
        assert_eq!(
            (
                &cold.ns,
                &cold.key,
                &cold.op_id,
                &cold.bytes,
                cold.remove,
                cold.ack
            ),
            (&m.ns, &m.key, &m.op_id, &m.bytes, m.remove, m.ack)
        );
        assert_eq!((version, at), (100 + i as u64, 200));
        assert_eq!(routed[i], (m.ns.clone(), m.key.clone(), version));
    }
    assert!(subrecord(&envelope, 4).is_err());
    let mut bad = envelope.clone();
    bad[16] = 1;
    assert!(decode_envelope(&bad).is_err());
    assert!(route_records(&bad).is_err());
    assert!(subrecord(&bad, 0).is_err());
    let mut bad = envelope.clone();
    bad[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(decode_envelope(&bad).is_err());
    let mut bad = envelope.clone();
    let key = b"launch-4162";
    let offset = bad.windows(key.len()).position(|s| s == key).unwrap() + key.len();
    bad[offset] = 2;
    assert!(
        subrecord(&bad, 0).is_err(),
        "a corrupt unselected peer must refuse before returning selected bytes"
    );
}
#[test]
fn malicious_count_order_duplicate_truncation_and_overflow_refuse() {
    let bytes = original_bytes(3);
    let offset = 4 + 4 + b"authored-population".len() + 4 + b"enqueue-1".len() + 8 + 8 + 1;
    for count in [u32::MAX, 400_000] {
        let mut b = bytes.clone();
        b[offset..offset + 4].copy_from_slice(&count.to_le_bytes());
        assert!(BorrowedOriginal::from_bytes(&b).is_err());
    }
    for n in [0, 1, 4, offset + 3, bytes.len() - 1] {
        assert!(BorrowedOriginal::from_bytes(&bytes[..n]).is_err());
    }
    let mut bad = bytes.clone();
    let row = 4 + 17 + 8;
    let start = offset + 4;
    let duplicate = bad[start..start + row].to_vec();
    bad[start + row..start + 2 * row].copy_from_slice(&duplicate);
    assert!(BorrowedOriginal::from_bytes(&bad).is_err());
    assert_eq!(
        capture_measure("authored", [("a", usize::MAX)].into_iter()),
        Err(PublicationError::Resource(7))
    );
    let budget = OriginalBudget::new(usize::MAX);
    let held = budget.reserve(usize::MAX).unwrap();
    assert!(budget.reserve(1).is_err());
    drop(held);
    assert_eq!(budget.used(), 0);
}
#[test]
fn small_compatibility_ack_remove_empty_and_identity_uncertainty_are_preserved() {
    let rows = authored_rows(2);
    let budget = OriginalBudget::new(64 * 1024);
    let view = captured(&rows, budget).unwrap();
    let r = view
        .prepare(
            "same",
            AtomicAck::Os,
            vec![
                FinalChange::remove("gone"),
                FinalChange::put("empty", vec![]),
            ],
        )
        .unwrap();
    let bytes = r.to_bytes();
    let decoded = AtomicRequest::from_bytes(&bytes).unwrap();
    assert_eq!(decoded.fingerprint(), r.fingerprint());
    let borrowed = BorrowedOriginal::from_bytes(&bytes).unwrap();
    assert_eq!(borrowed.requested_ack(), AtomicAck::Os);
    assert_eq!(
        borrowed.changes().collect::<Vec<_>>(),
        vec![("gone", None), ("empty", Some(&[][..]))]
    );
    let fp = r.fingerprint();
    let mut record = Dedup {
        fingerprint: fp,
        at: 100,
        until_mono: 900,
        replayed_chunk: 1,
        outcome: None,
    };
    assert_eq!(
        retained_outcome(&fp, Some(&record), false, 100).unwrap_err(),
        PublicationError::Uncertain
    );
    record.outcome = Some(Outcome::error(16));
    assert_eq!(
        retained_outcome(&fp, Some(&record), false, 100).unwrap_err(),
        PublicationError::Uncertain
    );
    record.outcome = Some(Outcome {
        code: 0,
        version: 50,
        ack: 1,
    });
    let outcome = retained_outcome(&fp, Some(&record), false, 100)
        .unwrap()
        .unwrap();
    assert_eq!((outcome.version, outcome.ack), (50, 1));
    assert_eq!(
        retained_outcome(&[0; 32], Some(&record), false, 100).unwrap_err(),
        PublicationError::IdentityMismatch
    );
    assert_eq!(
        retained_outcome(&fp, Some(&record), false, 901).unwrap_err(),
        PublicationError::RetryExpired
    );
    assert_eq!(
        retained_outcome(&fp, None, true, 100).unwrap_err(),
        PublicationError::RetryExpired
    );
    assert!(retained_outcome(&fp, None, false, 100).unwrap().is_none());
}

#[test]
fn failed_materialization_and_invalid_prepare_release_without_an_original() {
    let rows = authored_rows(3);
    let budget = OriginalBudget::new(64 * 1024);
    let result = capture_complete(
        "authored-population",
        19,
        1200,
        budget.clone(),
        rows.iter().map(|(k, v)| (k.as_str(), v.bytes.len(), v)),
        |_, _| Err(PublicationError::Uncertain),
    );
    assert!(matches!(result, Err(PublicationError::Uncertain)));
    assert_eq!(budget.used(), 0);
    let view = captured(&rows, budget.clone()).unwrap();
    let used = budget.used();
    assert!(matches!(
        view.prepare("", AtomicAck::Flush, four_changes(9001)),
        Err(PublicationError::Invalid)
    ));
    assert!(matches!(
        view.prepare(
            "same",
            AtomicAck::Flush,
            vec![FinalChange::remove("a"), FinalChange::put("a", vec![])]
        ),
        Err(PublicationError::Invalid)
    ));
    assert_eq!(budget.used(), used);
}
#[test]
fn concurrent_account_admission_cannot_exceed_its_actual_allowance() {
    let budget = OriginalBudget::new(64);
    let barrier = std::sync::Barrier::new(9);
    let successes = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let budget = budget.clone();
            let barrier = &barrier;
            let successes = &successes;
            scope.spawn(move || {
                let held = budget.reserve(32).ok();
                if held.is_some() {
                    successes.fetch_add(1, Ordering::SeqCst);
                }
                barrier.wait();
                barrier.wait();
                drop(held);
            });
        }
        barrier.wait();
        assert_eq!(budget.used(), 64);
        assert_eq!(successes.load(Ordering::SeqCst), 2);
        barrier.wait();
    });
    assert_eq!(budget.used(), 0);
}
