//! Normalizing is blind to how Arrow encodes a batch: a batch in any encodings normalizes as the
//! same batch in plain ones does, into the same parts, ids and values.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use proptest::prelude::*;
use rdlt_connector::Field;

use super::{Part, Shape, normalize};
use crate::table::plain;
use rdlt_testkit::drawn::{Drawn, Encoding, Scalar, Shape as Drawing, array, field, values};

/// `shape` with every encoding plain.
fn plainly(shape: &Drawing) -> Drawing {
    Drawing {
        logical: shape.logical.clone(),
        encoding: Encoding::Plain,
        children: shape.children.iter().map(plainly).collect(),
    }
}

/// `drawn` as a batch, each column drawn by `shape_of`.
fn batch((columns, rows): &Drawn, shape_of: impl Fn(&Drawing) -> Drawing) -> RecordBatch {
    let shapes: Vec<Drawing> = columns.iter().map(|(_, shape)| shape_of(shape)).collect();
    let arrays: Vec<ArrayRef> = shapes
        .iter()
        .enumerate()
        .map(|(column, shape)| {
            let values: Vec<&Scalar> = rows.iter().map(|row| &row[column]).collect();
            array(shape, &values)
        })
        .collect();
    let fields: Vec<_> = columns
        .iter()
        .zip(&shapes)
        .zip(&arrays)
        .map(|(((name, _), shape), array)| field(name, shape, array, true))
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(
        Arc::new(arrow_schema::Schema::new(fields)),
        arrays,
        &options,
    )
    .expect("the drawn batch is valid")
}

/// `array` in the plain Arrow type of its logical type.
fn plained(field: &arrow_schema::Field, array: &ArrayRef) -> ArrayRef {
    let logical = Field::from_arrow(field).expect("a logical type");
    plain(array, logical.logical_type()).expect("the plain type holds the values")
}

/// `value` with its dates within the days a `Date32` holds: a `Date64` beyond them has no other
/// encoding to normalize alike.
fn within_date32(value: Scalar) -> Scalar {
    match value {
        Scalar::Date(days) => Scalar::Date(days.clamp(i32::MIN.into(), i32::MAX.into())),
        Scalar::Struct(fields) => Scalar::Struct(
            fields
                .into_iter()
                .map(|(name, inner)| (name, within_date32(inner)))
                .collect(),
        ),
        Scalar::List(items) => Scalar::List(items.into_iter().map(within_date32).collect()),
        other => other,
    }
}

/// A part's path, its columns' paths, its values and its lineage.
type Held = (Vec<Arc<str>>, Vec<String>, Vec<ArrayRef>, Vec<ArrayRef>);

/// What `parts` hold, every array in its plain type.
fn held(parts: &[Part]) -> Vec<Held> {
    parts
        .iter()
        .map(|part| {
            let columns = part.columns.iter().map(ToString::to_string).collect();
            let schema = part.batch.schema();
            let values = schema
                .fields()
                .iter()
                .zip(part.batch.columns())
                .map(|(field, array)| plained(field, array))
                .collect();
            let mut lineage = vec![
                Arc::clone(&part.lineage.id),
                Arc::clone(&part.lineage.root_row),
            ];
            if let Some(parent) = &part.lineage.parent {
                lineage.extend([
                    Arc::clone(&parent.id),
                    Arc::clone(&parent.root),
                    Arc::clone(&parent.idx),
                    Arc::clone(&parent.row),
                ]);
            }
            (part.path.clone(), columns, values, lineage)
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(512)))]

    #[test]
    fn normalizing_a_batch_does_not_depend_on_its_encodings(
        drawn in values::drawn().prop_map(|(columns, rows)| {
            let rows = rows.into_iter().map(|row| row.into_iter().map(within_date32).collect());
            (columns, rows.collect())
        }),
        max_depth in 0_u8..4,
        keyed in proptest::collection::vec(any::<bool>(), 3),
    ) {
        let key = drawn
            .0
            .iter()
            .zip(&keyed)
            .filter(|(_, keyed)| **keyed)
            .map(|((name, _), _)| Arc::from(name.as_str()))
            .collect();
        let shape = Shape { max_depth, whole: std::collections::BTreeSet::new(), key };
        let encoded = normalize(&batch(&drawn, Clone::clone), &shape);
        let plain = normalize(&batch(&drawn, plainly), &shape);
        match (encoded, plain) {
            (Ok(encoded), Ok(plain)) => {
                let (encoded, plain) = (held(&encoded), held(&plain));
                prop_assert_eq!(encoded.len(), plain.len(), "parts");
                for (encoded, plain) in encoded.iter().zip(&plain) {
                    prop_assert_eq!(&encoded.0, &plain.0, "paths");
                    prop_assert_eq!(&encoded.1, &plain.1, "columns of {:?}", plain.0);
                    for (index, (left, right)) in encoded.3.iter().zip(&plain.3).enumerate() {
                        prop_assert_eq!(left, right, "lineage {} of {:?}", index, plain.0);
                    }
                    for (column, (left, right)) in encoded.2.iter().zip(&plain.2).enumerate() {
                        prop_assert_eq!(left, right, "column {} of {:?}", plain.1[column], plain.0);
                    }
                }
            }
            (encoded, plain) => prop_assert!(
                false,
                "normalizing failed: encoded {:?}, plain {:?}",
                encoded.err(),
                plain.err()
            ),
        }
    }
}
