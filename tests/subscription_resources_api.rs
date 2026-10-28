//! Public schema and callable signatures, without constructing a Store.
use membus::{Store,subscription_resources::*};

#[test]
fn real_store_methods_have_typed_grant_and_growth_signatures() {
    let _: fn(&Store,SubscriptionResources)->Result<SubscriptionReservation,ReservationError>
        = Store::reserve_subscription_resources;
    let _: fn(&Store,&mut SubscriptionReservation,SubscriptionResources)->Result<(),ReservationError>
        = Store::extend_subscription_resources;
    let _: fn(&Store)->SubscriptionResourceProfile = Store::subscription_resource_profile;
}
#[test]
fn prospective_layouts_preserve_every_capacity_dimension_and_overflow() {
    let a=SubscriptionResources::registration(3,5,7,11).unwrap();
    let b=SubscriptionResources::registration(13,5,7,11).unwrap();
    assert_eq!(b.metadata-a.metadata,10);assert!(a.in_flight>=std::mem::size_of::<SubscriptionReservation>()+7);
    assert_eq!(SubscriptionResources::registration(3,5,17,11).unwrap().in_flight-a.in_flight,10);
    let c=SubscriptionResources::change(2,3,17).unwrap();
    let d=SubscriptionResources::change(2,3,117).unwrap();
    assert_eq!(d.retained-c.retained,100);
    assert_eq!(SubscriptionResources::publication(2,3,17).unwrap().publication,c.retained);
    assert_eq!(SubscriptionResources::encoding(31,43).unwrap().encoding,74);
    assert_eq!(SubscriptionResources::registration(usize::MAX,1,0,0),Err(ReservationError::InvalidDemand));
    assert_eq!(SubscriptionResources::change(0,1,usize::MAX),Err(ReservationError::InvalidDemand));
    assert_eq!(SubscriptionResources::encoding(usize::MAX,1),Err(ReservationError::InvalidDemand));
}
