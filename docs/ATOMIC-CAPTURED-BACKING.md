# Caller backing on a captured account

`NamespaceView::reserve_backing(BackingLayout)` acquires an owned `BackingCharge`
from that view's **existing** private allocation account. It competes with the
captured population and originals prepared through the same view. It does not
create another `OriginalBudget`, original, frame, publication or callback.

The charge is opaque, non-clone and not serialized. Moving it does not spend
again. Its Drop returns precisely its debit through existing account ownership.
It retains the account's underlying Store scan reservation even after the view
is dropped. That reservation remains conservatively held until the last genuine
account owner drops. Neither view expiry nor a caller error transfers the charge
to a fresh account. Holding it supplies no renewed publication horizon or rights
after Store loss. Original identity, requested Ack, comparison population,
deadline and known/unknown/uncertain outcomes are unchanged.

## Declare complete backing before growth

`BackingLayout::empty()` starts an allocation-free plan. `allocation(Layout)`
adds one complete allocation padded to its alignment; `allocations(Layout, n)`
adds separate allocations. `array::<T>(capacity)` adds one full-capacity element
array. `bytes()` reports the checked total. All methods return a new plan, so a
failed extension does not alter a prior plan. Checked overflow and invalid array
layout return `PublicationError::Resource(7)` without allocating backing.

The caller must include every allocation in its declared footprint, including:

- the full retained root object, inline container headers, shared control-block
  metadata, alignment and tail padding;
- allocated collection capacity, each element's separately owned backing, native
  string buffers, and copies of request/context/command fields;
- overlapping construction buffers and error/retention paths covered by the
  promise, rather than just the final payload's logical length.

Use `Layout::extend` to compose an actual header-plus-tail allocation before
adding it; separate unpadded payload lengths omit internal alignment. Collection
capacity is not length. A zero-sized array does not cover owner metadata or
element-owned allocations. Zero total is valid: it spends zero, but the resulting
guard still retains this actual account lifetime. It is not capacity permission.

This is a **declared managed-backing** contract. The API does not inspect caller
types, certify an allocator ABI, observe allocations or measure RSS/free memory.
`Layout::new::<Arc<T>>()` measures the handle, not its heap control block and T.
Standard Arc headers, BTreeMap nodes and native-string growth cannot be guessed
from a payload count. Select and qualify complete representation/capacity bounds;
if the caller cannot establish them, refuse before copying. The allocator may
have additional unobservable bookkeeping/usable storage outside this managed
promise. A future capacity increase or extra copy requires its own genuine charge
before growth; a successful reservation is not unlimited future admission.

Only after the plan is complete may the caller reserve it. Exhaustion or checked
account overflow returns Resource(7), leaves existing charges/originals intact,
and invokes no copy/allocation callback. Successful reservation only debits the
actual account. Subsequent allocation can independently fail or abort; it is not
proof of RAM, native admission or caller authority. On caller construction error,
destroy partial backing before dropping its charge. Never replace a prior
uncertain original merely to obtain more budget.

## Keep the charge through the final owner

Move the charge into the same immutable context root as its covered data, after
acquiring it and before any variable-sized copies. Keep it as the **last field**
to destroy other owned fields first, including during unwinding. Every alias,
custody/cohort holder and retained error path must keep that same root/account.
Public bytes, equal getter values or a successful callback cannot recreate it.

There is an additional shared-allocation obligation. Ordinary Arc drops its
payload before releasing its control-block storage. A surviving Weak can retain
that storage after the payload (and a charge field) is gone. Therefore a guard
field in an arbitrary Arc is not sufficient for a promise covering that heap
allocation. One caller-owned pattern, when no Weak or raw Arc may escape, is:

```rust,no_run
use membus::atomic::BackingCharge;
use std::{ops::Deref, sync::Arc};

struct ContextData {
    // All covered owned data precedes the charge.
    payload: Box<[u8]>,
    _backing: BackingCharge,
}
struct ContextOwner(Option<Arc<ContextData>>);
impl Clone for ContextOwner {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().unwrap().clone()))
    }
}
impl Deref for ContextOwner {
    type Target = ContextData;
    fn deref(&self) -> &ContextData { self.0.as_ref().unwrap().as_ref() }
}
impl Drop for ContextOwner {
    fn drop(&mut self) {
        // Every strong owner consumes its Arc this way. Exactly the final one
        // gets the data, after releasing the unaliased shared allocation.
        drop(Arc::into_inner(self.0.take().unwrap()));
    }
}
```

All actual handle paths must use the private owner, including custody errors and
cohort retention. Expose borrowed data only; do not expose the raw Arc, a Weak,
an extraction method or an uncharged deep clone. Do not substitute racing
`try_unwrap(...).ok()` calls: their failure/drop paths do not supply the same
final-owner guarantee. If the caller permits Weak or other escaping backing,
it needs another qualified lifetime design and must not claim this recipe.

The caller still has to establish a complete allocation plan before constructing
that ContextData. This example establishes an ownership shape, not an Arc layout
bound, allocator, fallible-copy implementation or application integration.
An explicit early `drop(charge)` does not destroy unrelated caller data; callers
must enforce the ownership association. Deliberately forgotten/leaked guards
retain the debit conservatively. Normal errors/unwind release only after the
covered backing's destruction; process abort is outside in-process cleanup.

No resident opcode, tenant entitlement, recovery grant, arbitrary context
serialization or new allocation subsystem is introduced. Future compiler,
application integration, allocator and Store/native controls require their own
actual qualification.
