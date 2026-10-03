//! The keys a merge table's rows carry: refused before any destination sees them where a key
//! holds no value to match by, a NaN, or a change claims it unchanged, in every encoding.

use std::sync::Arc;

use arrow_array::types::{Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type};
use arrow_array::types::{UInt32Type, UInt64Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, RunArray, StringArray, StructArray,
};
use arrow_schema::DataType;
use rdlt_connector::{ChangeOp, LogicalType, TableSchema};

use super::{batch, capabilities, created, plan, resolver, stamp, table};
use crate::table::TableView;
use crate::table::lowering::{ChangeRows, LoweringPlan};
use crate::table::resolve::Incoming;

/// The code of the error preparing `batch` for a merge table keyed by `id`, its rows `changes`
/// say, gives; `None` where it prepares.
fn refusal(batch: &RecordBatch, changes: Option<&ChangeRows>) -> Option<String> {
    let resolver = resolver(capabilities(), plan(), &["id"]);
    let incoming = Incoming::declared(TableSchema::from_arrow(&batch.schema()).unwrap());
    let model = created(&resolver, &[("v", LogicalType::Utf8)]);
    let resolution = resolver.resolve(&model, &incoming).unwrap();
    let view = Arc::new(TableView::new(&table("t"), resolution.model, &resolver).unwrap());
    LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
        .prepare(batch, None, &stamp(), changes)
        .err()
        .map(|error| error.code().unwrap_or("uncoded").to_owned())
}

/// `values` as a dictionary whose keys are of `key`, each value once.
fn dictionary(values: &ArrayRef, key: &DataType) -> ArrayRef {
    let positions: Vec<i64> = (0..i64::try_from(values.len()).unwrap()).collect();
    let keys: ArrayRef = Arc::new(Int64Array::from(positions));
    let keys = arrow_cast::cast(&keys, key).unwrap();
    macro_rules! of {
        ($type:ty) => {
            Arc::new(
                DictionaryArray::<$type>::try_new(
                    keys.as_any().downcast_ref().cloned().unwrap(),
                    Arc::clone(values),
                )
                .unwrap(),
            )
        };
    }
    match key {
        DataType::Int8 => of!(Int8Type),
        DataType::Int16 => of!(Int16Type),
        DataType::Int32 => of!(Int32Type),
        DataType::Int64 => of!(Int64Type),
        DataType::UInt8 => of!(UInt8Type),
        DataType::UInt16 => of!(UInt16Type),
        DataType::UInt32 => of!(UInt32Type),
        _ => of!(UInt64Type),
    }
}

/// `values` run-end encoded, a run each, by every run-end type.
fn runs(values: &ArrayRef) -> Vec<ArrayRef> {
    let ends: Vec<i64> = (1..=i64::try_from(values.len()).unwrap()).collect();
    let narrow = |end: &i64| i16::try_from(*end).unwrap();
    let middle = |end: &i64| i32::try_from(*end).unwrap();
    vec![
        Arc::new(
            RunArray::<Int16Type>::try_new(
                &Int16Array::from(ends.iter().map(narrow).collect::<Vec<_>>()),
                values,
            )
            .unwrap(),
        ),
        Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from(ends.iter().map(middle).collect::<Vec<_>>()),
                values,
            )
            .unwrap(),
        ),
        Arc::new(RunArray::<Int64Type>::try_new(&Int64Array::from(ends), values).unwrap()),
    ]
}

/// `values` in every encoding a key column may arrive in: plain, sliced, a dictionary of every
/// key type and run-end encoded by every run-end type.
fn encodings(values: &ArrayRef) -> Vec<ArrayRef> {
    let n = values.len();
    let wider = arrow_select::concat::concat(&[values.as_ref(), values.as_ref()]).unwrap();
    let mut encoded = vec![Arc::clone(values), wider.slice(n, n)];
    let key_types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];
    encoded.extend(key_types.iter().map(|key| dictionary(values, key)));
    encoded.extend(runs(values));
    encoded
}

/// A batch of key `id` and a text `v` of as many rows.
fn keyed(id: ArrayRef) -> RecordBatch {
    let values: Vec<&str> = (0..id.len()).map(|_| "x").collect();
    batch(vec![
        ("id", id),
        ("v", Arc::new(StringArray::from(values)) as _),
    ])
}

#[test]
fn a_key_holding_nan_in_any_float_width_and_encoding_is_refused() {
    let floats: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![1.5, f64::NAN, 2.5])),
        Arc::new(Float32Array::from(vec![1.5, f32::NAN, 2.5])),
        arrow_cast::cast(
            &(Arc::new(Float32Array::from(vec![1.5, f32::NAN, 2.5])) as ArrayRef),
            &DataType::Float16,
        )
        .unwrap(),
    ];
    for float in &floats {
        for encoded in encodings(float) {
            let kind = encoded.data_type().to_string();
            assert_eq!(
                refusal(&keyed(encoded), None).as_deref(),
                Some("merge_key_nan"),
                "{kind}"
            );
        }
        let whole = arrow_select::filter::filter(
            float.as_ref(),
            &arrow_array::BooleanArray::from(vec![true, false, true]),
        )
        .unwrap();
        for encoded in encodings(&whole) {
            let kind = encoded.data_type().to_string();
            assert_eq!(refusal(&keyed(encoded), None), None, "{kind}");
        }
    }
}

#[test]
fn a_key_holding_nan_within_a_struct_is_refused() {
    let inner: ArrayRef = Arc::new(Float64Array::from(vec![1.0, f64::NAN]));
    let fields =
        arrow_schema::Fields::from(vec![arrow_schema::Field::new("x", DataType::Float64, true)]);
    let key: ArrayRef = Arc::new(StructArray::try_new(fields, vec![inner], None).unwrap());
    assert_eq!(refusal(&keyed(key), None).as_deref(), Some("merge_key_nan"));
}

#[test]
fn a_change_flagging_its_key_unchanged_is_refused_before_any_destination_sees_it() {
    let rows = keyed(Arc::new(Int64Array::from(vec![9, 10])));
    let changes = |flags: Vec<Option<&[u8]>>| ChangeRows {
        op: Int8Array::from(vec![ChangeOp::Update.code(); 2]),
        seq: BinaryArray::from_iter_values([[0_u8; 16], [1_u8; 16]]),
        unchanged: Some(BinaryArray::from(flags)),
    };
    // Bit 0 is `id`, the key; bit 1 is `v`.
    let key = changes(vec![None, Some(&[0b01])]);
    assert_eq!(
        refusal(&rows, Some(&key)).as_deref(),
        Some("merge_key_unchanged")
    );
    let value = changes(vec![Some(&[0b10]), None]);
    assert_ne!(
        refusal(&rows, Some(&value)).as_deref(),
        Some("merge_key_unchanged")
    );
}
