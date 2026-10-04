use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Date64Array, Decimal128Array, Float32Array, Int64Array};

use super::{checked, denotations, edges, typed};
use crate::capabilities::{Capabilities, SchemaChanges};
use crate::types::{DecimalType, LogicalType, TimeUnit, TypeKind};

#[test]
fn only_pairs_of_kinds_the_destination_stores_are_widened() {
    use TypeKind as K;
    let mut capabilities = Capabilities::minimal();
    capabilities.types = BTreeSet::from([K::Int32, K::Int64, K::Float32, K::Float64, K::Date]);
    capabilities.schema_changes = SchemaChanges::all();
    let pairs = checked(&capabilities);
    let kinds: BTreeSet<(TypeKind, TypeKind)> = pairs
        .iter()
        .map(|(from, to)| (from.kind(), to.kind()))
        .collect();
    let expected = BTreeSet::from([
        (K::Int32, K::Int64),
        (K::Int32, K::Float64),
        (K::Float32, K::Float64),
    ]);
    assert_eq!(kinds, expected);
}

#[test]
fn every_scalar_pair_the_lattice_widens_is_typed_as_a_widening() {
    let all = SchemaChanges::all().widenings;
    for (from, to) in &all {
        let pair = typed((*from, *to));
        if matches!(from, TypeKind::Struct | TypeKind::List) {
            assert!(pair.is_none(), "{from:?}");
            continue;
        }
        let (narrow, wide) = pair.unwrap_or_else(|| panic!("{from:?} to {to:?}"));
        assert_eq!((narrow.kind(), wide.kind()), (*from, *to));
        assert_eq!(narrow.join(&wide), wide, "{narrow} joins {wide}");
        // The edges are values of the narrower type, none null, none alike.
        let values = edges(&narrow);
        assert_eq!(values.data_type(), &narrow.to_arrow());
        assert_eq!(values.null_count(), 0, "{narrow}");
        let shown: BTreeSet<String> = denotations(values.as_ref(), &narrow).into_iter().collect();
        assert_eq!(shown.len(), values.len(), "{narrow}: {shown:?}");
    }
}

#[test]
fn a_32_bit_float_among_the_edges_shows_another_64_bit_float() {
    let values = edges(&LogicalType::Float32);
    let floats = values.as_any().downcast_ref::<Float32Array>().unwrap();
    assert!(floats.values().contains(&0.1_f32));
}

#[test]
fn values_are_denoted_exactly_and_counts_as_their_unit() {
    let decimal = LogicalType::Decimal(DecimalType::new(5, 2).unwrap());
    let decimals: ArrayRef = Arc::new(
        Decimal128Array::from(vec![12_345, -5, 0, 100])
            .with_precision_and_scale(5, 2)
            .unwrap(),
    );
    assert_eq!(
        denotations(decimals.as_ref(), &decimal),
        ["123.45", "-0.05", "0", "1"]
    );
    let floats: ArrayRef = Arc::new(Float32Array::from(vec![0.1_f32]));
    assert_eq!(
        denotations(floats.as_ref(), &LogicalType::Float64),
        ["0.10000000149011612"]
    );
    let millis = LogicalType::Timestamp(TimeUnit::Millisecond, None);
    let counted: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    assert_eq!(denotations(counted.as_ref(), &millis), ["1000000 ns"]);
    assert_eq!(denotations(counted.as_ref(), &LogicalType::Int64), ["1"]);
    // A date of milliseconds is the day it is within, as a timestamp column holds it.
    let within: ArrayRef = Arc::new(Date64Array::from(vec![1]));
    assert_eq!(denotations(within.as_ref(), &millis), ["0 ns"]);
}
