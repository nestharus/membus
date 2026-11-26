//! Authored SDK controls, using real account/capture code and inert owned rows.
//! These fixtures do not open a Store or certify a caller/allocator ABI.
use super::*;
use crate::atomic::{capture_complete, AtomicAck, FinalChange, OriginalBudget};
use crate::Value;
use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

fn captured(budget: OriginalBudget) -> NamespaceView {
    let value = Value {
        version: 19,
        ack: 2,
        tombstone: false,
        bytes: b"retained".to_vec(),
    };
    capture_complete(
        "authored",
        7,
        1234,
        budget,
        [("a", value.bytes.len(), &value)].into_iter(),
        |_, v| Ok(v.clone()),
    )
    .unwrap()
}

#[test]
fn complete_layout_counts_headers_capacity_and_alignment_without_allocation() {
    let header = Layout::from_size_align(1, 1).unwrap();
    let (whole, offset) = header
        .extend(Layout::from_size_align(8, 8).unwrap())
        .unwrap();
    assert_eq!(offset, 8);
    let layout = BackingLayout::empty()
        .allocation(whole)
        .unwrap()
        .array::<u16>(3)
        .unwrap()
        .allocations(Layout::from_size_align(3, 4).unwrap(), 2)
        .unwrap();
    assert_eq!(layout.bytes(), 16 + 6 + 8);
    assert_ne!(
        layout.bytes(),
        1 + 8 + 6 + 6,
        "payload lengths omit padding"
    );
    assert_eq!(BackingLayout::empty().array::<u64>(0).unwrap().bytes(), 0);
    assert_eq!(
        BackingLayout::empty()
            .array::<()>(usize::MAX)
            .unwrap()
            .bytes(),
        0
    );
    // ZST capacity does not account for an owner's separately allocated data.
    assert!(Layout::from_size_align(1, 3).is_err());
}

#[test]
fn layout_overflow_and_single_allocation_bound_refuse_without_wrapping() {
    assert_eq!(
        BackingLayout::empty().array::<u64>(usize::MAX),
        Err(PublicationError::Resource(7))
    );
    assert_eq!(
        BackingLayout::empty().allocations(Layout::from_size_align(1, 8).unwrap(), usize::MAX),
        Err(PublicationError::Resource(7))
    );
    let almost = BackingLayout::empty()
        .allocations(Layout::new::<u8>(), usize::MAX)
        .unwrap();
    assert_eq!(
        almost.allocation(Layout::new::<u8>()),
        Err(PublicationError::Resource(7))
    );
}

#[test]
fn additional_backing_uses_the_retained_account_after_the_view_drops() {
    let budget = OriginalBudget::new(8192);
    let view = captured(budget.clone());
    let bytes = |n| BackingLayout::empty().array::<u8>(n).unwrap();
    let retained = view.reserve_backing(bytes(256)).unwrap();
    drop(view);
    assert_eq!(budget.used(), 256);

    assert!(matches!(
        retained.reserve_additional(bytes(7937)),
        Err(PublicationError::Resource(7))
    ));
    assert_eq!(budget.used(), 256);

    let later = retained.reserve_additional(bytes(7936)).unwrap();
    assert_eq!(budget.used(), 8192);
    drop(retained);
    assert_eq!(budget.used(), 7936);
    drop(later);
    assert_eq!(budget.used(), 0);
}

#[test]
fn side_backing_competes_with_original_preparation_on_the_same_pool() {
    let budget = OriginalBudget::new(8192);
    let view = captured(budget.clone());
    assert_eq!(view.rows()[0].1.bytes, b"retained");
    let baseline = budget.used();
    let layout = BackingLayout::empty().array::<u8>(8192 - baseline).unwrap();
    let charge = view.reserve_backing(layout).unwrap();
    assert!(Arc::ptr_eq(&charge.reservation.budget.0, &view.budget.0));
    assert_eq!(budget.used(), 8192);
    assert!(matches!(
        view.prepare(
            "original",
            AtomicAck::Flush,
            vec![FinalChange::put("b", b"B".to_vec())]
        ),
        Err(PublicationError::Resource(7))
    ));
    drop(charge);
    assert_eq!(budget.used(), baseline);
    let original = view
        .prepare(
            "original",
            AtomicAck::Flush,
            vec![FinalChange::put("b", b"B".to_vec())],
        )
        .unwrap();
    let bytes = original.to_bytes();
    let used = budget.used();
    let remaining = BackingLayout::empty().array::<u8>(8192 - used).unwrap();
    let backing = view.reserve_backing(remaining).unwrap();
    assert!(matches!(
        view.reserve_backing(BackingLayout::empty().array::<u8>(1).unwrap()),
        Err(PublicationError::Resource(7))
    ));
    assert_eq!(original.to_bytes(), bytes);
    assert_eq!(
        crate::atomic::BorrowedOriginal::from_bytes(&bytes)
            .unwrap()
            .requested_ack(),
        AtomicAck::Flush
    );
    assert!(original.matches_population([("a", 19)].into_iter()));
    drop(backing);
    drop(original);
    assert_eq!(budget.used(), baseline);
}

#[test]
fn exhausted_charge_refuses_before_caller_copy_and_does_not_consume_the_account() {
    fn retain(
        view: &NamespaceView,
        plan: BackingLayout,
        copies: &Cell<usize>,
    ) -> Result<(Box<[u8; 32]>, BackingCharge), PublicationError> {
        let charge = view.reserve_backing(plan)?;
        copies.set(copies.get() + 1);
        Ok((Box::new([65; 32]), charge))
    }
    let budget = OriginalBudget::new(8192);
    let view = captured(budget.clone());
    let before = budget.used();
    let copies = Cell::new(0);
    let too_large = BackingLayout::empty().array::<u8>(8193 - before).unwrap();
    assert!(matches!(
        retain(&view, too_large, &copies),
        Err(PublicationError::Resource(7))
    ));
    assert_eq!(copies.get(), 0);
    assert_eq!(budget.used(), before);
    assert_eq!(view.rows()[0].1.version, 19);
}

#[test]
fn caller_error_before_attachment_returns_only_the_new_backing_debit() {
    let budget = OriginalBudget::new(8192);
    let view = captured(budget.clone());
    let original = view
        .prepare(
            "kept",
            AtomicAck::Os,
            vec![FinalChange::put("b", b"B".to_vec())],
        )
        .unwrap();
    let before = budget.used();
    let bytes = original.to_bytes();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _charge = view
            .reserve_backing(BackingLayout::empty().array::<u8>(32).unwrap())
            .unwrap();
        assert_eq!(budget.used(), before + 32);
        panic!("authored construction error before allocating/attaching context");
    }));
    assert!(result.is_err());
    assert_eq!(budget.used(), before);
    assert_eq!(original.to_bytes(), bytes);
    assert_eq!(
        crate::atomic::BorrowedOriginal::from_bytes(&bytes)
            .unwrap()
            .requested_ack(),
        AtomicAck::Os
    );
}

#[test]
fn zero_charge_is_no_capacity_grant_but_retains_the_actual_scan_owner() {
    let usage = Arc::new(crate::snapshots::ScanUsage::default());
    usage.bytes.store(8192, Ordering::Release);
    usage.count.store(1, Ordering::Release);
    let budget = OriginalBudget::backed(
        8192,
        Some(crate::snapshots::ScanCharge {
            usage: usage.clone(),
            bytes: 8192,
        }),
    );
    let view = captured(budget.clone());
    let before = budget.used();
    let charge = view.reserve_backing(BackingLayout::empty()).unwrap();
    assert_eq!(charge.bytes(), 0);
    assert_eq!(budget.used(), before);
    drop(view);
    assert_eq!(budget.used(), 0);
    drop(budget);
    assert_eq!(usage.bytes.load(Ordering::Acquire), 8192);
    assert_eq!(usage.count.load(Ordering::Acquire), 1);
    drop(charge);
    assert_eq!(usage.bytes.load(Ordering::Acquire), 0);
    assert_eq!(usage.count.load(Ordering::Acquire), 0);
}

#[test]
fn nonzero_charge_alone_keeps_scan_ownership_after_view_and_account_handles_drop() {
    let usage = Arc::new(crate::snapshots::ScanUsage::default());
    usage.bytes.store(8192, Ordering::Release);
    usage.count.store(1, Ordering::Release);
    let budget = OriginalBudget::backed(
        8192,
        Some(crate::snapshots::ScanCharge {
            usage: usage.clone(),
            bytes: 8192,
        }),
    );
    let view = captured(budget.clone());
    let charge = view
        .reserve_backing(BackingLayout::empty().array::<u64>(8).unwrap())
        .unwrap();
    assert_eq!(charge.bytes(), 64);
    drop(view);
    assert_eq!(budget.used(), 64);
    drop(budget);
    assert_eq!(usage.bytes.load(Ordering::Acquire), 8192);
    assert_eq!(usage.count.load(Ordering::Acquire), 1);
    drop(charge);
    assert_eq!(usage.bytes.load(Ordering::Acquire), 0);
    assert_eq!(usage.count.load(Ordering::Acquire), 0);
    assert!(usage.released.load(Ordering::Acquire));
}

struct Payload {
    bytes: Box<[u8; 32]>,
    budget: OriginalBudget,
    minimum: usize,
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
}
impl Drop for Payload {
    fn drop(&mut self) {
        assert_eq!(self.bytes[0], 65);
        assert!(
            self.budget.used() >= self.minimum,
            "payload destruction precedes charge release"
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic_on_drop, "authored payload drop failure");
    }
}
struct CreationData {
    payload: Payload,
    // Last, so field cleanup/unwind destroys owned payload before this charge.
    _backing: BackingCharge,
}
struct Owner(Option<Arc<CreationData>>);
impl Clone for Owner {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().unwrap().clone()))
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        // All strong owners use into_inner, not racing try_unwrap then drop.
        // No Weak/raw Arc escapes this private owner type.
        drop(Arc::into_inner(self.0.take().unwrap()));
    }
}
fn owned(
    view: &NamespaceView,
    budget: &OriginalBudget,
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
) -> Owner {
    // Authored declared header bound for this fixture, NOT a std::Arc ABI claim.
    // A real caller must qualify its actual control-block/allocation bound.
    let (root, _) = Layout::array::<usize>(2)
        .unwrap()
        .extend(Layout::new::<CreationData>())
        .unwrap();
    let plan = BackingLayout::empty()
        .allocation(root)
        .unwrap()
        .allocation(Layout::new::<[u8; 32]>())
        .unwrap();
    let charge = view.reserve_backing(plan).unwrap();
    let minimum = charge.bytes();
    Owner(Some(Arc::new(CreationData {
        payload: Payload {
            bytes: Box::new([65; 32]),
            budget: budget.clone(),
            minimum,
            drops,
            panic_on_drop,
        },
        _backing: charge,
    })))
}

#[test]
fn same_creation_arc_aliases_release_only_after_final_owner_and_payload_cleanup() {
    let budget = OriginalBudget::new(8192);
    let view = captured(budget.clone());
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = owned(&view, &budget, drops.clone(), false);
    let alias = owner.clone();
    assert!(Arc::ptr_eq(
        owner.0.as_ref().unwrap(),
        alias.0.as_ref().unwrap()
    ));
    let charged = owner.0.as_ref().unwrap()._backing.bytes();
    let data = owner.0.as_ref().unwrap();
    assert_eq!(data.payload.bytes.as_ref(), &[65; 32]);
    drop(view);
    assert_eq!(budget.used(), charged);
    drop(owner);
    assert_eq!(budget.used(), charged);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(alias);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(budget.used(), 0);
}

#[test]
fn custody_error_and_owner_unwind_retain_surviving_alias_then_release_once() {
    let budget = OriginalBudget::new(8192);
    let view = captured(budget.clone());
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = owned(&view, &budget, drops.clone(), false);
    let alias = owner.clone();
    let charged = alias.0.as_ref().unwrap()._backing.bytes();
    drop(view);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _retained_through_error = owner;
        panic!("authored custody failure; not a filesystem/provider result");
    }));
    assert!(result.is_err());
    assert_eq!(budget.used(), charged);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(alias);
    assert_eq!(budget.used(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn final_payload_unwind_still_releases_its_last_field_charge() {
    let budget = OriginalBudget::new(8192);
    let view = captured(budget.clone());
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = owned(&view, &budget, drops.clone(), true);
    drop(view);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(budget.used(), 0);
}
