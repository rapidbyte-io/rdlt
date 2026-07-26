//! Capability-driven lowering at the destination seam.
//!
//! Inside the engine, nested objects are struct columns. Destinations without native
//! structs receive a FLATTENED schema and batches: `profile.city` becomes column
//! `profile__city`, collision-safe (distinct source paths never merge — hash-suffixed
//! by `UniqueNamer`). Decimals lower to canonical text when unsupported.
//! Names are a pure function of the engine schema (first-seen order, append-only), so
//! they are stable across batches and runs.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::record_batch::RecordBatch;
use rdlt_connector::DestinationCapabilities;
use rdlt_core::naming::UniqueNamer;
use rdlt_core::{ColumnDef, ColumnType, LogicalType, RdltError, TableSchema};

fn needs_lowering(caps: &DestinationCapabilities) -> bool {
    !caps.structs || !caps.decimal
}

/// Lower a schema for the destination's capabilities.
pub(crate) fn lower_schema(schema: &TableSchema, caps: &DestinationCapabilities) -> TableSchema {
    if !needs_lowering(caps) {
        return schema.clone();
    }
    let mut namer = UniqueNamer::new(caps.ident_rules);
    let mut columns: Vec<ColumnDef> = Vec::new();
    for column in &schema.columns {
        lower_column(column, &[], caps, &mut namer, &mut columns);
    }
    TableSchema {
        table: schema.table.clone(),
        parent: schema.parent.clone(),
        columns,
    }
}

fn lower_column(
    column: &ColumnDef,
    path: &[&str],
    caps: &DestinationCapabilities,
    namer: &mut UniqueNamer,
    out: &mut Vec<ColumnDef>,
) {
    let mut full_path: Vec<&str> = path.to_vec();
    full_path.push(&column.name);
    match &column.column_type {
        ColumnType::Struct { fields } if !caps.structs => {
            for field in fields {
                lower_column(field, &full_path, caps, namer, out);
            }
        }
        ty => {
            let lowered_ty = match ty {
                ColumnType::Scalar {
                    scalar: LogicalType::Decimal { .. },
                } if !caps.decimal => ColumnType::scalar(LogicalType::Utf8),
                other => other.clone(),
            };
            out.push(ColumnDef {
                name: namer.name_for(&full_path.join("__")),
                column_type: lowered_ty,
                nullable: column.nullable || !path.is_empty(),
                provenance: column.provenance,
            });
        }
    }
}

/// Lower a batch. Driven by the batch's OWN arrow schema (which mirrors the engine
/// schema), so replay can lower without engine-schema bookkeeping.
/// Field names/paths are identical to `lower_schema`'s inputs → identical names out.
pub(crate) fn lower_batch(
    batch: &RecordBatch,
    caps: &DestinationCapabilities,
) -> Result<RecordBatch, RdltError> {
    if !needs_lowering(caps) {
        return Ok(batch.clone());
    }
    let mut namer = UniqueNamer::new(caps.ident_rules);
    let mut fields: Vec<arrow::datatypes::Field> = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        flatten_array(
            field,
            &[],
            Arc::clone(batch.column(idx)),
            caps,
            &mut namer,
            &mut fields,
            &mut arrays,
        )?;
    }
    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), arrays)
        .map_err(|e| RdltError::internal(format!("lowering produced invalid batch: {e}")))
}

fn flatten_array(
    field: &arrow::datatypes::Field,
    path: &[&str],
    array: ArrayRef,
    caps: &DestinationCapabilities,
    namer: &mut UniqueNamer,
    fields: &mut Vec<arrow::datatypes::Field>,
    out: &mut Vec<ArrayRef>,
) -> Result<(), RdltError> {
    use arrow::datatypes::DataType;
    let mut full_path: Vec<&str> = path.to_vec();
    full_path.push(field.name());
    match field.data_type() {
        DataType::Struct(children) if !caps.structs => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| RdltError::internal("schema says struct, array is not"))?;
            let parent_nulls = struct_array.nulls().cloned();
            for (i, child_field) in children.iter().enumerate() {
                let child = with_merged_nulls(struct_array.column(i), parent_nulls.as_ref());
                flatten_array(child_field, &full_path, child, caps, namer, fields, out)?;
            }
            Ok(())
        }
        DataType::Decimal128(_, scale) if !caps.decimal => {
            let decimals = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| RdltError::internal("schema says decimal, array is not"))?;
            let scale = (*scale).max(0) as u8;
            let rendered: StringArray = decimals
                .iter()
                .map(|v| v.map(|raw| render_decimal(raw, scale)))
                .collect();
            fields.push(arrow::datatypes::Field::new(
                namer.name_for(&full_path.join("__")),
                DataType::Utf8,
                true,
            ));
            out.push(Arc::new(rendered));
            Ok(())
        }
        other => {
            fields.push(arrow::datatypes::Field::new(
                namer.name_for(&full_path.join("__")),
                other.clone(),
                field.is_nullable() || !path.is_empty(),
            ));
            out.push(array);
            Ok(())
        }
    }
}

/// A struct-null row must null its flattened children too.
fn with_merged_nulls(child: &ArrayRef, parent: Option<&NullBuffer>) -> ArrayRef {
    let Some(parent) = parent else {
        return Arc::clone(child);
    };
    let merged = match child.nulls() {
        Some(own) => NullBuffer::union(Some(own), Some(parent)),
        None => Some(parent.clone()),
    };
    let data = child
        .to_data()
        .into_builder()
        .nulls(merged)
        .build()
        .expect("null-merge preserves layout");
    arrow::array::make_array(data)
}

fn render_decimal(raw: i128, scale: u8) -> String {
    if scale == 0 {
        return raw.to_string();
    }
    let negative = raw < 0;
    let digits = raw.unsigned_abs().to_string();
    let scale = scale as usize;
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int_part, frac_part) = padded.split_at(padded.len() - scale);
    format!("{}{int_part}.{frac_part}", if negative { "-" } else { "" })
}

#[cfg(test)]
mod tests {
    // Mutation-report closure: the whole lowering seam ran
    // assertion-free — the suite only ever used full-capability destinations.
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use rdlt_core::{Provenance, TableName};

    fn caps(structs: bool, decimal: bool) -> DestinationCapabilities {
        DestinationCapabilities {
            structs,
            decimal,
            ..DestinationCapabilities::default()
        }
    }

    fn col(name: &str, ty: ColumnType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            column_type: ty,
            nullable: false,
            provenance: Provenance::Inferred,
        }
    }

    fn schema_with_struct_and_decimal() -> TableSchema {
        TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns: vec![
                col("id", ColumnType::scalar(LogicalType::Int64)),
                col(
                    "profile",
                    ColumnType::Struct {
                        fields: vec![
                            col("city", ColumnType::scalar(LogicalType::Utf8)),
                            col(
                                "geo",
                                ColumnType::Struct {
                                    fields: vec![col(
                                        "lat",
                                        ColumnType::scalar(LogicalType::Float64),
                                    )],
                                },
                            ),
                        ],
                    },
                ),
                col(
                    "price",
                    ColumnType::scalar(LogicalType::Decimal {
                        precision: 10,
                        scale: 2,
                    }),
                ),
            ],
        }
    }

    #[test]
    fn needs_lowering_tracks_each_capability() {
        assert!(!needs_lowering(&caps(true, true)));
        assert!(needs_lowering(&caps(false, true)));
        assert!(needs_lowering(&caps(true, false)));
    }

    #[test]
    fn full_capabilities_lower_to_identity() {
        let schema = schema_with_struct_and_decimal();
        assert_eq!(lower_schema(&schema, &caps(true, true)), schema);
    }

    #[test]
    fn structs_off_flattens_recursively_and_children_go_nullable() {
        let lowered = lower_schema(&schema_with_struct_and_decimal(), &caps(false, true));
        let names: Vec<&str> = lowered.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "profile__city", "profile__geo__lat", "price"]
        );
        let city = &lowered.columns[1];
        assert_eq!(city.column_type, ColumnType::scalar(LogicalType::Utf8));
        assert!(city.nullable, "flattened children must be nullable");
        // Decimal untouched: only structs were lowered.
        assert_eq!(
            lowered.columns[3].column_type,
            ColumnType::scalar(LogicalType::Decimal {
                precision: 10,
                scale: 2
            })
        );
    }

    #[test]
    fn decimal_off_lowers_to_utf8_leaving_structs() {
        let lowered = lower_schema(&schema_with_struct_and_decimal(), &caps(true, false));
        assert_eq!(
            lowered.columns[2].column_type,
            ColumnType::scalar(LogicalType::Utf8)
        );
        assert!(matches!(
            lowered.columns[1].column_type,
            ColumnType::Struct { .. }
        ));
    }

    #[test]
    fn batch_lowering_flattens_structs_and_propagates_parent_nulls() {
        use arrow::array::StructArray;
        use arrow::buffer::NullBuffer;
        use std::sync::Arc;
        let inner = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
        let struct_array = StructArray::new(
            vec![Field::new("lat", DataType::Int64, true)].into(),
            vec![Arc::new(inner) as ArrayRef],
            Some(NullBuffer::from(vec![true, false, true])), // row 1 struct-null
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "geo",
                struct_array.data_type().clone(),
                true,
            )])),
            vec![Arc::new(struct_array)],
        )
        .expect("batch");

        let lowered = lower_batch(&batch, &caps(false, true)).expect("lower");
        assert_eq!(lowered.schema().field(0).name(), "geo__lat");
        let column = lowered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int column");
        assert_eq!(column.value(0), 1);
        assert!(column.is_null(1), "parent struct-null must null the child");
        assert_eq!(column.value(2), 3);
    }

    #[test]
    fn batch_lowering_renders_decimals_as_canonical_text() {
        use arrow::array::Decimal128Array;
        use std::sync::Arc;
        let decimals = Decimal128Array::from(vec![Some(1234i128), Some(-50i128), None])
            .with_precision_and_scale(10, 2)
            .expect("decimal array");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "price",
                decimals.data_type().clone(),
                true,
            )])),
            vec![Arc::new(decimals)],
        )
        .expect("batch");
        let lowered = lower_batch(&batch, &caps(true, false)).expect("lower");
        let strings = lowered
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        assert_eq!(strings.value(0), "12.34");
        assert_eq!(strings.value(1), "-0.50");
        assert!(strings.is_null(2));
        // Full capabilities: the batch passes through unchanged.
        let same = lower_batch(&batch, &caps(true, true)).expect("identity");
        assert_eq!(same, batch);
    }

    #[test]
    fn render_decimal_scale_zero_and_padding() {
        assert_eq!(render_decimal(42, 0), "42");
        assert_eq!(render_decimal(-42, 0), "-42");
        assert_eq!(render_decimal(7, 3), "0.007");
    }

    /// A batch carrying BOTH a struct and a decimal column, which is the only
    /// shape that reaches `flatten_array` with one capability on and the other
    /// off. Every prior batch test used a single-kind batch, so each match guard
    /// was only ever evaluated with its capability already false — negating it
    /// changed nothing. Here each guard is exercised in its FALSE state, where
    /// the mutant lowers a column the destination said it supports.
    fn mixed_batch() -> RecordBatch {
        use arrow::array::Decimal128Array;
        let lat = Int64Array::from(vec![Some(7i64)]);
        let struct_array = StructArray::new(
            vec![Field::new("lat", DataType::Int64, true)].into(),
            vec![Arc::new(lat) as ArrayRef],
            None,
        );
        let price = Decimal128Array::from(vec![Some(1234i128)])
            .with_precision_and_scale(10, 2)
            .expect("decimal array");
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("geo", struct_array.data_type().clone(), true),
                Field::new("price", price.data_type().clone(), true),
            ])),
            vec![Arc::new(struct_array), Arc::new(price)],
        )
        .expect("mixed batch")
    }

    #[test]
    fn mixed_batch_lowers_only_the_unsupported_kind() {
        // Structs supported, decimal NOT: the struct must survive intact.
        let lowered = lower_batch(&mixed_batch(), &caps(true, false)).expect("lower");
        assert_eq!(lowered.num_columns(), 2, "the struct must NOT be flattened");
        assert_eq!(lowered.schema().field(0).name(), "geo");
        assert!(
            matches!(lowered.schema().field(0).data_type(), DataType::Struct(_)),
            "caps.structs = true: the struct column stays a struct"
        );
        assert_eq!(
            lowered.schema().field(1).data_type(),
            &DataType::Utf8,
            "caps.decimal = false: only the decimal lowers"
        );

        // Decimal supported, structs NOT: the decimal must survive intact.
        let lowered = lower_batch(&mixed_batch(), &caps(false, true)).expect("lower");
        assert_eq!(lowered.num_columns(), 2, "struct flattens to one child");
        assert_eq!(lowered.schema().field(0).name(), "geo__lat");
        assert_eq!(
            lowered.schema().field(1).data_type(),
            &DataType::Decimal128(10, 2),
            "caps.decimal = true: the decimal is NOT rendered to text"
        );
    }

    /// Lowered nullability is `field.is_nullable() || !path.is_empty()`, and the
    /// two halves need cases that DISAGREE — a batch of all-nullable fields
    /// cannot distinguish `||` from `&&`. A non-nullable top level (false, false)
    /// catches dropping the `!`, which would report it nullable; a non-nullable
    /// struct child (false, true) catches `||`→`&&`, which would report it
    /// non-nullable even though flattening leaves nowhere to record a
    /// struct-null row.
    #[test]
    fn lowered_nullability_pins_both_halves_of_the_or() {
        let child = Int64Array::from(vec![Some(1i64)]);
        let struct_array = StructArray::new(
            vec![Field::new("lat", DataType::Int64, false)].into(), // non-nullable CHILD
            vec![Arc::new(child) as ArrayRef],
            None,
        );
        let top = Int64Array::from(vec![Some(9i64)]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false), // non-nullable TOP LEVEL
                Field::new("geo", struct_array.data_type().clone(), true),
            ])),
            vec![Arc::new(top), Arc::new(struct_array)],
        )
        .expect("batch");

        let lowered = lower_batch(&batch, &caps(false, true)).expect("lower");
        assert_eq!(lowered.schema().field(0).name(), "id");
        assert!(
            !lowered.schema().field(0).is_nullable(),
            "a non-nullable top-level field stays non-nullable (its path is empty)"
        );
        assert_eq!(lowered.schema().field(1).name(), "geo__lat");
        assert!(
            lowered.schema().field(1).is_nullable(),
            "a flattened child is nullable regardless of its own flag: the parent \
             struct can be null and flattening has nowhere else to record that"
        );
    }

    /// Boundaries chosen so each mutant CHANGES the output. `raw < 0` → `<=`
    /// differs only at exactly zero. `len - scale` → `len / scale` agrees on the
    /// short values the test above uses ("12.34": 4-2 == 4/2), so it needs a
    /// value where they diverge: 6 digits at scale 2 splits at 4 under
    /// subtraction and at 3 under division.
    #[test]
    fn render_decimal_boundaries_zero_and_split_point() {
        assert_eq!(render_decimal(0, 2), "0.00", "zero is not negative");
        assert_eq!(render_decimal(123456, 2), "1234.56", "split at len - scale");
        assert_eq!(render_decimal(-123456, 2), "-1234.56");
        assert_eq!(render_decimal(1, 2), "0.01", "padding to scale + 1");
        assert_eq!(render_decimal(i128::MAX, 0), i128::MAX.to_string());
    }
}
