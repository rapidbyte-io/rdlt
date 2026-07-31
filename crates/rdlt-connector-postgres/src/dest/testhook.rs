//! Encoding seam, exposed ONLY for the byte-identity pin and the gated
//! encoder bench. Not a public API.
//!
//! The pin exists because the wire encoder is replaceable but its OUTPUT is
//! not: Postgres binary COPY carries no per-field type tag, so a value encoded
//! one byte differently is either a loud server-side format error or, worse, a
//! silently different value. The fixture captures what the encoder emits for
//! every wire kind at boundary values, so any rewrite is checked against bytes
//! rather than against intent.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow_schema::{Field, Schema};
use bytes::BytesMut;
use rdlt_connector::core::{ColumnType, LogicalType};
use std::sync::Arc;

use super::encode::{ColumnEncoder, column_wire};

/// One column of the pin/bench batch: name, the LOGICAL type when the
/// arrow representation alone would not reach the wire kind (Utf8 covers
/// text, jsonb and uuid), and values chosen to include NULL plus whatever
/// the type's edges are.
struct PinColumn {
    name: &'static str,
    logical: Option<ColumnType>,
    array: Arc<dyn Array>,
}

fn scalar(t: LogicalType) -> Option<ColumnType> {
    Some(ColumnType::Scalar { scalar: t })
}

/// Every `ColumnWire` variant, in declaration order.
fn columns() -> Vec<PinColumn> {
    let col = |name, logical, array| PinColumn {
        name,
        logical,
        array,
    };
    vec![
        col(
            "c_bool",
            None,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as Arc<dyn Array>,
        ),
        col(
            "c_int8",
            None,
            Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
        ),
        col(
            "c_float8",
            None,
            Arc::new(Float64Array::from(vec![
                Some(f64::MIN_POSITIVE),
                Some(-0.0),
                None,
            ])),
        ),
        col(
            "c_text",
            None,
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("héllo\u{1F600}"),
                None,
            ])),
        ),
        col(
            "c_bytea",
            None,
            Arc::new(BinaryArray::from_opt_vec(vec![
                Some(&b""[..]),
                Some(&b"\x00\xff\x7f"[..]),
                None,
            ])),
        ),
        col(
            "c_timestamptz",
            None,
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(0), Some(-1), None]).with_timezone("UTC"),
            ),
        ),
        col(
            "c_timestamp",
            None,
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(0),
                Some(-1),
                None,
            ])),
        ),
        col(
            "c_date",
            None,
            Arc::new(Date32Array::from(vec![Some(0), Some(-1), None])),
        ),
        col(
            "c_time",
            None,
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(0),
                Some(86_399_999_999),
                None,
            ])),
        ),
        col(
            "c_numeric",
            scalar(LogicalType::Decimal {
                precision: 38,
                scale: 9,
            }),
            Arc::new(
                Decimal128Array::from(vec![Some(0i128), Some(-123_456_789_012_345_678), None])
                    .with_precision_and_scale(38, 9)
                    .expect("precision/scale"),
            ),
        ),
        col(
            "c_jsonb",
            scalar(LogicalType::Json),
            Arc::new(StringArray::from(vec![
                Some("{}"),
                Some(r#"{"a":[1,null,"é"]}"#),
                None,
            ])),
        ),
        col(
            "c_uuid",
            scalar(LogicalType::Uuid),
            Arc::new(StringArray::from(vec![
                Some("00000000-0000-0000-0000-000000000000"),
                Some("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                None,
            ])),
        ),
    ]
}

/// Arrow field for a pin column: the field the destination would see.
fn field_of(c: &PinColumn) -> Field {
    Field::new(c.name, c.array.data_type().clone(), true)
}

/// The batch the pin and the bench both encode. `rows` cycles the value
/// vectors so the bench has volume; the pin uses the natural length.
pub fn bench_batch(rows: usize) -> RecordBatch {
    let cols = columns();
    let schema = Arc::new(Schema::new(cols.iter().map(field_of).collect::<Vec<_>>()));
    let arrays: Vec<Arc<dyn Array>> = cols
        .iter()
        .map(|c| {
            let idx = UInt32Array::from(
                (0..rows)
                    .map(|i| u32::try_from(i % c.array.len()).expect("index fits"))
                    .collect::<Vec<u32>>(),
            );
            arrow_select::take::take(c.array.as_ref(), &idx, None).expect("take")
        })
        .collect();
    RecordBatch::try_new(schema, arrays).expect("bench batch")
}

/// Encode the pin batch's VALUES through the shipping path, returning
/// `(column, per-row wire bytes or NULL)`.
///
/// The encoder emits length-prefixed FIELDS, so the prefix is read back
/// off each one here: `-1` is NULL, otherwise the declared length must
/// account for exactly the bytes that follow — which pins the prefix as
/// well as the value. What is rendered stays value-bytes-only, so the
/// fixture is a stable oracle across the encoder rewrite.
pub fn encode_pin_values() -> Vec<(String, Vec<Option<Vec<u8>>>)> {
    let cols = columns();
    let rows = cols[0].array.len();
    cols.iter()
        .map(|c| {
            let wire =
                column_wire(c.logical.as_ref(), c.array.data_type()).expect("supported wire");
            let encoder =
                ColumnEncoder::new(wire, c.array.as_ref(), c.name).expect("column encodable");
            let cells = (0..rows)
                .map(|row| {
                    let mut buf = BytesMut::new();
                    encoder
                        .encode_field(row, c.name, &mut buf)
                        .expect("encodable");
                    let (prefix, value) = buf.split_at(4);
                    let declared = i32::from_be_bytes(prefix.try_into().expect("4 bytes"));
                    if declared < 0 {
                        assert_eq!(declared, -1, "{}: NULL is spelled -1", c.name);
                        assert!(value.is_empty(), "{}: NULL carries no bytes", c.name);
                        return None;
                    }
                    assert_eq!(
                        declared as usize,
                        value.len(),
                        "{}: field length prefix disagrees with the bytes written",
                        c.name
                    );
                    Some(value.to_vec())
                })
                .collect();
            (c.name.to_string(), cells)
        })
        .collect()
}

/// The gated encoder hot path (bench body): a whole batch through the
/// production encoding path — downcast once per column, fields appended
/// into one reused buffer. Returns the byte count so the work cannot be
/// optimized away.
///
/// Note when comparing against the recorded pre-rewrite baseline: this
/// body also writes the 4-byte length prefix the old one did not, so it
/// does slightly MORE work per cell. Any improvement it shows is a
/// conservative lower bound.
pub fn bench_encode(batch: &RecordBatch) -> u64 {
    let schema = batch.schema();
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut bytes = 0u64;
    // Wires come from the SAME logical types the pin uses. Resolving them
    // from the arrow type alone would bench `c_jsonb` and `c_uuid` as
    // plain text — so the jsonb version byte and the uuid parser, both
    // per-cell work in production, would never appear in the instruction
    // count and a regression in either would pass the 3% gate untouched.
    let logical = columns();
    let encoders: Vec<ColumnEncoder<'_>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let column = logical
                .iter()
                .find(|c| c.name == field.name())
                .expect("bench batch columns come from `columns()`");
            let wire =
                column_wire(column.logical.as_ref(), field.data_type()).expect("supported wire");
            ColumnEncoder::new(wire, batch.column(idx).as_ref(), field.name())
                .expect("column encodable")
        })
        .collect();
    for row in 0..batch.num_rows() {
        for (idx, encoder) in encoders.iter().enumerate() {
            buf.clear();
            encoder
                .encode_field(row, schema.field(idx).name(), &mut buf)
                .expect("encodable");
            bytes += buf.len() as u64;
        }
    }
    bytes
}
