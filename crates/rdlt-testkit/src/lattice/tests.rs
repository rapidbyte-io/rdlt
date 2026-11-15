use proptest::prelude::*;
use rdlt_connector::strategy::{cut, logical_type, respelled};
use rdlt_connector::{Field, Fields, LogicalType};

use super::{as_modeled, holds};

fn structure(fields: &[(&str, LogicalType, bool)]) -> LogicalType {
    let fields = fields
        .iter()
        .map(|(name, logical, nullable)| Field::new(*name, logical.clone(), *nullable))
        .collect();
    LogicalType::Struct(Fields::new(fields).unwrap())
}

#[test]
fn a_struct_holds_its_fields_in_any_order_and_those_it_may_leave_null_lacking() {
    use LogicalType as T;
    let column = structure(&[("zip", T::Utf8, true), ("city", T::Utf8, false)]);
    assert!(holds(&column, &structure(&[("city", T::Utf8, false)])));
    assert!(holds(
        &column,
        &structure(&[("city", T::Utf8, false), ("zip", T::Null, true)])
    ));
    assert!(!holds(&column, &structure(&[("zip", T::Utf8, true)])));
    assert!(!holds(
        &column,
        &structure(&[("city", T::Utf8, true), ("zip", T::Utf8, true)])
    ));
    assert!(!holds(
        &column,
        &structure(&[("city", T::Utf8, false), ("street", T::Utf8, true)])
    ));
}

proptest! {
    #[test]
    fn the_model_holds_what_the_lattice_holds(a in logical_type(), b in logical_type()) {
        let modeled = as_modeled(&a);
        prop_assert_eq!(holds(&a, &b), modeled.join(&b) == modeled);
        let joined = a.join(&b);
        prop_assert!(holds(&joined, &a) && holds(&joined, &b));
    }

    #[test]
    fn a_type_holds_whatever_is_cut_from_it_however_spelled(
        (a, cut, respelled) in logical_type()
            .prop_flat_map(|a| (cut(&a), respelled(&a), Just(a)))
            .prop_map(|(cut, respelled, a)| (a, cut, respelled)),
    ) {
        prop_assert!(holds(&a, &cut));
        prop_assert!(holds(&a, &respelled) && holds(&respelled, &a));
        prop_assert_eq!(as_modeled(&respelled), as_modeled(&a));
        prop_assert_eq!(as_modeled(&a), a.canonical());
    }
}
