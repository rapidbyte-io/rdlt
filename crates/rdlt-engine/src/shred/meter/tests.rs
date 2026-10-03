use super::{Columns, Meter, Over};
use crate::shred::ShredError;

#[test]
fn a_meter_takes_its_room_and_trips_on_a_charge_past_it() {
    let meter = Meter::new(10);
    assert_eq!(meter.charge(4), Ok(()));
    assert_eq!(meter.charge(6), Ok(()));
    assert_eq!(meter.spent(), 10);
    assert!(!meter.tripped());
    assert_eq!(meter.charge(1), Err(Over));
    assert!(meter.tripped());
    assert_eq!(meter.spent(), 10, "a refused charge takes nothing");
    assert_eq!(meter.text_bytes(3), 24);
}

#[test]
fn a_reserved_meter_never_trips_and_presizes_no_text() {
    let meter = Meter::reserved();
    assert_eq!(meter.charge(u64::MAX), Ok(()));
    assert!(!meter.tripped());
    assert_eq!(meter.text_bytes(1_000), 0);
}

#[test]
fn columns_are_counted_to_their_limit_and_refused_one_past_it() {
    let columns = Columns::new(2);
    assert_eq!(columns.add(), Ok(()));
    assert_eq!(columns.add(), Ok(()));
    assert_eq!(columns.add(), Err(ShredError::TooManyColumns(3, 2)));
    assert_eq!(columns.add(), Err(ShredError::TooManyColumns(3, 2)));
}

#[test]
fn an_observation_takes_past_its_room_only_what_the_room_lacks() {
    let beyond = super::Beyond::new(1_000);
    let meter = Meter::observing(100, &beyond);
    // A hundred within the room and fifty beyond it; then all that is left beyond.
    assert_eq!(meter.charge(150), Ok(()));
    assert_eq!(meter.spent(), 150);
    assert_eq!(meter.charge(950), Ok(()));
    assert_eq!(meter.spent(), 1_100);
    assert_eq!(meter.charge(1), Err(Over));
    assert_eq!(meter.beyond_limit(), 1_000);
    assert_eq!(beyond.limit(), 1_000);
}
