use rdlt_connector::LogicalType;
use rdlt_testkit::drawn::Scalar;

use super::{Arrival, pushed};

#[test]
fn pushed_scalars_are_inferred_as_json_holds_them() {
    let typed = Arrival::Typed;
    assert_eq!(pushed(&Scalar::Float64(1.5)), typed(LogicalType::Float64));
    assert_eq!(pushed(&Scalar::Int(3)), typed(LogicalType::Int64));
    assert_eq!(pushed(&Scalar::Bool(true)), typed(LogicalType::Bool));
    assert_eq!(
        pushed(&Scalar::Float64(f64::NAN)),
        typed(LogicalType::Utf8),
        "a float JSON cannot hold is pushed as its name"
    );
    assert_eq!(pushed(&Scalar::Null), typed(LogicalType::Null));
    assert_eq!(pushed(&Scalar::List(Vec::new())), Arrival::Container);
}

#[test]
fn a_push_mixing_types_arrives_as_their_join() {
    let typed = Arrival::Typed;
    let int = typed(LogicalType::Int64);
    assert_eq!(
        int.clone().join(typed(LogicalType::Utf8)),
        typed(LogicalType::Json)
    );
    assert_eq!(int.clone().join(typed(LogicalType::Null)), int);
    assert_eq!(
        Arrival::Container.join(int),
        typed(LogicalType::Json),
        "a container joins a scalar as JSON"
    );
    assert_eq!(
        Arrival::Container.join(typed(LogicalType::Null)),
        Arrival::Container
    );
}

#[test]
fn only_json_surely_holds_a_pushed_container() {
    assert_eq!(Arrival::Container.fits(&LogicalType::Json), Some(true));
    assert_eq!(Arrival::Container.fits(&LogicalType::Int64), Some(false));
    let list = LogicalType::List(Box::new(rdlt_connector::Field::new(
        "item",
        LogicalType::Int64,
        true,
    )));
    assert_eq!(Arrival::Container.fits(&list), None);
}
