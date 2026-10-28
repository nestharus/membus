use super::*;
use original::{
    recover_original, CustodyError, CustodyRecord, DomainOriginalCustody, OriginalScope,
};
use std::cell::Cell;

#[derive(Clone)]
struct Draft {
    namespace: String,
    operation_id: String,
    epoch: u64,
    expires_mono: u64,
    population: Vec<(String, u64)>,
    ack: AtomicAck,
    changes: Vec<FinalChange>,
}
impl Draft {
    fn build(self) -> AtomicRequest {
        let budget = OriginalBudget::new(1 << 20);
        let pop_charge = budget.reserve(1).unwrap();
        let req_charge = budget.reserve(1).unwrap();
        AtomicRequest {
            data: Arc::new(OriginalData {
                namespace: self.namespace,
                operation_id: self.operation_id,
                epoch: self.epoch,
                expires_mono: self.expires_mono,
                population: Arc::new(Population {
                    rows: self.population,
                    guard: None,
                    _charge: pop_charge,
                }),
                ack: self.ack,
                changes: self.changes,
                _charge: req_charge,
            }),
        }
    }
}

fn draft() -> Draft {
    Draft {
        namespace: "authored-queue".into(),
        operation_id: "enqueue-unique".into(),
        epoch: 19,
        expires_mono: 1200,
        population: vec![("allocator".into(), 81), ("unrelated".into(), 75)],
        ack: AtomicAck::Flush,
        changes: vec![
            FinalChange::put("allocator", b"42".to_vec()),
            FinalChange::put("ticket-42", b"waiting".to_vec()),
            FinalChange::put("native-42", b"unsettled".to_vec()),
            FinalChange::put("launch-42", b"not-started".to_vec()),
        ],
    }
}
fn request() -> AtomicRequest {
    draft().build()
}
fn scope() -> OriginalScope<'static> {
    OriginalScope {
        domain: "authored-dataset",
        generation: 7,
        kind: "authored-enqueue-v1",
    }
}
// This independent authored custodian predates the offered recovery candidate.
// It stands for an authenticated owner port, not a production durable adapter.
struct Keeper {
    record: CustodyRecord,
    calls: Cell<usize>,
}
impl Keeper {
    fn new() -> Self {
        Self::with_original(request())
    }
    fn with_original(original: AtomicRequest) -> Self {
        Self {
            record: CustodyRecord {
                domain: scope().domain.into(),
                generation: 7,
                kind: scope().kind.into(),
                original,
            },
            calls: Cell::new(0),
        }
    }
}
impl DomainOriginalCustody for Keeper {
    fn load_original(&self, _: OriginalScope<'_>, _: &str) -> Result<CustodyRecord, CustodyError> {
        self.calls.set(self.calls.get() + 1);
        Ok(self.record.clone())
    }
}

#[test]
fn complete_inspection_conserves_four_records_guards_ack_and_original_bytes() {
    let r = request();
    let before = r.to_bytes();
    let v = r.inspect();
    assert_eq!(v.namespace, "authored-queue");
    assert_eq!(v.operation_id, "enqueue-unique");
    assert_eq!((v.epoch, v.expires_mono), (19, 1200));
    assert_eq!(
        v.population,
        &[("allocator".into(), 81), ("unrelated".into(), 75)]
    );
    assert_eq!(v.requested_ack, AtomicAck::Flush);
    assert_eq!(v.changes.len(), 4);
    assert_eq!(
        v.changes.iter().map(|c| c.key.as_str()).collect::<Vec<_>>(),
        ["allocator", "ticket-42", "native-42", "launch-42"]
    );
    assert_eq!(
        v.changes
            .iter()
            .map(|c| c.value.as_deref())
            .collect::<Vec<_>>(),
        [
            Some(b"42".as_slice()),
            Some(b"waiting".as_slice()),
            Some(b"unsettled".as_slice()),
            Some(b"not-started".as_slice())
        ]
    );
    assert_eq!(r.to_bytes(), before);
}

#[test]
fn exact_custodied_original_survives_caller_object_loss_without_redecision() {
    let keeper = Keeper::new();
    let bytes = {
        let caller = request();
        caller.to_bytes()
    };
    let offered = AtomicRequest::from_bytes(&bytes).unwrap();
    let recovered = recover_original(&keeper, scope(), &offered).unwrap();
    assert_eq!(recovered.original().to_bytes(), bytes);
    assert_eq!(recovered.scope(), scope());
    assert_eq!(keeper.calls.get(), 1);
    // No Store, process, recapture, new operation, send, or outcome inference.
}

#[test]
fn syntactically_valid_forged_originals_cannot_replace_retained_domain_intent() {
    let keeper = Keeper::new();
    let mut variants = Vec::new();
    let mut r = draft();
    r.operation_id.push('x');
    variants.push(r.build());
    let mut r = draft();
    r.namespace.push('x');
    variants.push(r.build());
    let mut r = draft();
    r.epoch += 1;
    variants.push(r.build());
    let mut r = draft();
    r.expires_mono += 1;
    variants.push(r.build());
    let mut r = draft();
    r.population[0].1 += 1;
    variants.push(r.build());
    let mut r = draft();
    r.population.pop();
    variants.push(r.build());
    let mut r = draft();
    r.ack = AtomicAck::Os;
    variants.push(r.build());
    let mut r = draft();
    r.changes.pop();
    variants.push(r.build());
    let mut r = draft();
    r.changes[1].value = Some(b"running".to_vec());
    variants.push(r.build());
    let mut r = draft();
    r.changes[1].value = None;
    variants.push(r.build());
    for r in variants {
        let decoded = AtomicRequest::from_bytes(&r.to_bytes()).unwrap();
        assert!(matches!(
            recover_original(&keeper, scope(), &decoded),
            Err(CustodyError::OriginalMismatch)
        ));
    }
}

#[test]
fn mismatched_domain_generation_and_origin_kind_refuse() {
    let r = request();
    for expected in [
        OriginalScope {
            domain: "different",
            ..scope()
        },
        OriginalScope {
            generation: 8,
            ..scope()
        },
        OriginalScope {
            kind: "fence-v1",
            ..scope()
        },
    ] {
        assert!(matches!(
            recover_original(&Keeper::new(), expected, &r),
            Err(CustodyError::ScopeMismatch)
        ));
    }
}

struct Missing(CustodyError);
impl DomainOriginalCustody for Missing {
    fn load_original(&self, _: OriginalScope<'_>, _: &str) -> Result<CustodyRecord, CustodyError> {
        Err(self.0.clone())
    }
}
#[test]
fn missing_original_or_provenance_never_becomes_an_empty_or_recreated_original() {
    for e in [
        CustodyError::UnknownOriginal,
        CustodyError::ProvenanceUnavailable,
        CustodyError::Unavailable,
    ] {
        assert!(
            matches!(recover_original(&Missing(e.clone()), scope(), &request()), Err(actual) if actual == e)
        );
    }
}

#[test]
fn invalid_scope_and_invalid_request_refuse_before_custodian_call() {
    let keeper = Keeper::new();
    for expected in [
        OriginalScope {
            domain: "",
            ..scope()
        },
        OriginalScope {
            generation: 0,
            ..scope()
        },
        OriginalScope {
            kind: "bad\0kind",
            ..scope()
        },
    ] {
        assert!(matches!(
            recover_original(&keeper, expected, &request()),
            Err(CustodyError::Invalid)
        ));
    }
    let mut r = draft();
    r.changes.clear();
    assert!(matches!(
        recover_original(&keeper, scope(), &r.build()),
        Err(CustodyError::Invalid)
    ));
    assert_eq!(keeper.calls.get(), 0);
}

#[test]
fn corrupt_authoritative_record_is_refused_and_never_repaired_from_candidate() {
    let mut d = draft();
    d.population[0].1 = 0;
    let keeper = Keeper::with_original(d.build());
    assert!(matches!(
        recover_original(&keeper, scope(), &request()),
        Err(CustodyError::Invalid)
    ));
}

#[test]
fn same_instance_encoding_remove_empty_value_and_os_ack_are_unchanged() {
    let mut d = draft();
    d.ack = AtomicAck::Os;
    d.changes[2] = FinalChange::remove("native-42");
    d.changes[3] = FinalChange::put("launch-42", Vec::new());
    let r = d.build();
    let keeper = Keeper::with_original(r.clone());
    let decoded = AtomicRequest::from_bytes(&r.to_bytes()).unwrap();
    let recovered = recover_original(&keeper, scope(), &decoded).unwrap();
    assert_eq!(recovered.original().inspect().requested_ack, AtomicAck::Os);
    assert!(recovered.original().inspect().changes[2].value.is_none());
    assert_eq!(
        recovered.original().inspect().changes[3].value,
        Some(Vec::new())
    );
    assert_eq!(r.to_bytes(), recovered.original().to_bytes());
}
