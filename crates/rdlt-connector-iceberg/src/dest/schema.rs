//! The CLOSED engine-type → Iceberg-type mapping. Unmappable columns are
//! typed at ensure-table naming the column. Field IDs are assigned
//! SEQUENTIALLY at creation time only — the
//! catalog normalizes them on create, and post-creation evolution goes
//! through UpdateSchema (which assigns fresh IDs); this crate never
//! renumbers or reuses an ID.

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use rdlt_connector::DestinationError;
use rdlt_connector::core::{ColumnDef, ColumnType, LogicalType, TableSchema};

use super::config::PartitionTransform;
use super::errors::fatal;

/// The config partition vocabulary → Iceberg transform. ONE mapping shared by
/// spec construction (below) and the live-spec drift check (`ensure`), so the
/// seven-arm match is never duplicated.
pub(crate) fn to_transform(transform: PartitionTransform) -> iceberg::spec::Transform {
    match transform {
        PartitionTransform::Identity => iceberg::spec::Transform::Identity,
        PartitionTransform::Year => iceberg::spec::Transform::Year,
        PartitionTransform::Month => iceberg::spec::Transform::Month,
        PartitionTransform::Day => iceberg::spec::Transform::Day,
        PartitionTransform::Hour => iceberg::spec::Transform::Hour,
        PartitionTransform::Bucket(n) => iceberg::spec::Transform::Bucket(n),
        PartitionTransform::Truncate(w) => iceberg::spec::Transform::Truncate(w),
    }
}

/// Map one scalar logical type. `Json` maps to string — Iceberg v2 has no
/// JSON type (documented; the variant type is future work).
fn scalar_type(table: &str, column: &str, scalar: LogicalType) -> Result<Type, DestinationError> {
    Ok(Type::Primitive(match scalar {
        LogicalType::Bool => PrimitiveType::Boolean,
        LogicalType::Int64 => PrimitiveType::Long,
        LogicalType::Float64 => PrimitiveType::Double,
        LogicalType::Decimal { precision, scale } => PrimitiveType::Decimal {
            precision: precision as u32,
            scale: scale as u32,
        },
        LogicalType::Utf8 => PrimitiveType::String,
        LogicalType::Binary => PrimitiveType::Binary,
        LogicalType::TimestampTz => PrimitiveType::Timestamptz,
        LogicalType::TimestampNaive => PrimitiveType::Timestamp,
        LogicalType::Date => PrimitiveType::Date,
        LogicalType::Time => PrimitiveType::Time,
        LogicalType::Uuid => PrimitiveType::Uuid,
        // The typed escape hatch stays a STRING in Iceberg (no JSON type
        // in format v2) — documented in the README type table.
        LogicalType::Json => PrimitiveType::String,
        // A future LogicalType variant lands here as a typed error, never
        // a silent guess (the enum is non_exhaustive upstream).
        #[allow(unreachable_patterns)]
        other => {
            return Err(fatal(format!(
                "table `{table}` column `{column}`: engine type {other:?} has no \
                 iceberg mapping (the engine-type → iceberg-type mapping is closed)"
            )));
        }
    }))
}

/// Build one field, assigning IDs from the running counter.
fn build_field(
    table: &str,
    column: &ColumnDef,
    next_id: &mut i32,
) -> Result<NestedField, DestinationError> {
    let id = *next_id;
    *next_id += 1;
    let field_type = match &column.column_type {
        ColumnType::Scalar { scalar } => scalar_type(table, &column.name, *scalar)?,
        ColumnType::ScalarList { item } => {
            let element = scalar_type(table, &column.name, *item)?;
            let element_id = *next_id;
            *next_id += 1;
            Type::List(iceberg::spec::ListType {
                element_field: NestedField::list_element(element_id, element, false).into(),
            })
        }
        ColumnType::Struct { fields } => {
            let mut nested = Vec::with_capacity(fields.len());
            for field in fields {
                nested.push(build_field(table, field, next_id)?.into());
            }
            Type::Struct(iceberg::spec::StructType::new(nested))
        }
    };
    Ok(if column.nullable {
        NestedField::optional(id, &column.name, field_type)
    } else {
        NestedField::required(id, &column.name, field_type)
    })
}

/// The full mapping: engine `TableSchema` → iceberg `Schema` (creation
/// time; sequential IDs).
pub(crate) fn to_iceberg_schema(schema: &TableSchema) -> Result<Schema, DestinationError> {
    let table = schema.table.as_str();
    let mut next_id = 1i32;
    let mut fields = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
        fields.push(build_field(table, column, &mut next_id)?.into());
    }
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| fatal(format!("table `{table}`: building iceberg schema: {e}")))
}

/// Map the config partition vocabulary onto an Iceberg partition spec
/// against the MAPPED schema (unknown columns typed — never guessed).
/// Identity fields keep the column name; temporal transforms get the
/// `{column}_{transform}` convention.
pub(crate) fn to_partition_spec(
    context: &str,
    schema: &Schema,
    fields: &[super::config::PartitionField],
) -> Result<Option<iceberg::spec::UnboundPartitionSpec>, DestinationError> {
    if fields.is_empty() {
        return Ok(None);
    }
    // Polaris parses the create payload STRICTLY: spec-id and per-field
    // field-id must be present (probed live — omitting them is a 400).
    // Partition field ids follow the Iceberg convention, starting at 1000.
    let mut builder = iceberg::spec::UnboundPartitionSpec::builder().with_spec_id(0);
    for (next_field_id, field) in (1000..).zip(fields.iter()) {
        let source = schema.field_by_name(&field.column).ok_or_else(|| {
            fatal(format!(
                "{context}: partition_by names unknown column `{}`",
                field.column
            ))
        })?;
        let transform = to_transform(field.transform);
        // Field NAME convention: identity keeps the column name, temporal
        // transforms append `_{unit}`, Java-convention `col_bucket`/`col_trunc`.
        let name = match field.transform {
            PartitionTransform::Identity => field.column.clone(),
            PartitionTransform::Year => format!("{}_year", field.column),
            PartitionTransform::Month => format!("{}_month", field.column),
            PartitionTransform::Day => format!("{}_day", field.column),
            PartitionTransform::Hour => format!("{}_hour", field.column),
            PartitionTransform::Bucket(_) => format!("{}_bucket", field.column),
            PartitionTransform::Truncate(_) => format!("{}_trunc", field.column),
        };
        let unbound = iceberg::spec::UnboundPartitionField::builder()
            .source_id(source.id)
            .field_id(next_field_id)
            .name(name)
            .transform(transform)
            .build();
        builder = builder.add_partition_fields([unbound]).map_err(|e| {
            fatal(format!(
                "{context}: partition field `{}` ({}): {e}",
                field.column, transform
            ))
        })?;
    }
    Ok(Some(builder.build()))
}

#[cfg(test)]
mod partition_tests {
    use super::super::config::{PartitionField, PartitionTransform};
    use super::*;

    fn schema() -> Schema {
        to_iceberg_schema(&TableSchema {
            table: rdlt_connector::core::TableName::new("t"),
            parent: None,
            columns: vec![
                ColumnDef {
                    name: "region".into(),
                    column_type: ColumnType::scalar(LogicalType::Utf8),
                    nullable: false,
                    provenance: rdlt_connector::core::Provenance::Inferred,
                },
                ColumnDef {
                    name: "ts".into(),
                    column_type: ColumnType::scalar(LogicalType::TimestampTz),
                    nullable: false,
                    provenance: rdlt_connector::core::Provenance::Inferred,
                },
            ],
        })
        .expect("schema")
    }

    #[test]
    fn identity_and_temporal_transforms_build() {
        let spec = to_partition_spec(
            "t",
            &schema(),
            &[
                PartitionField::new("region", PartitionTransform::Identity),
                PartitionField::new("ts", PartitionTransform::Day),
            ],
        )
        .expect("builds")
        .expect("present");
        let fields = spec.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "region");
        assert_eq!(fields[0].transform, iceberg::spec::Transform::Identity);
        assert_eq!(fields[1].name, "ts_day");
        assert_eq!(fields[1].transform, iceberg::spec::Transform::Day);
    }

    #[test]
    fn empty_partition_by_is_none() {
        assert!(
            to_partition_spec("t", &schema(), &[])
                .expect("ok")
                .is_none()
        );
    }

    #[test]
    fn bucket_and_truncate_transforms_build() {
        let spec = to_partition_spec(
            "t",
            &schema(),
            &[
                PartitionField::new("region", PartitionTransform::Truncate(2)),
                PartitionField::new("region", PartitionTransform::Bucket(16)),
            ],
        )
        .expect("builds")
        .expect("present");
        let fields = spec.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "region_trunc");
        assert_eq!(fields[0].transform, iceberg::spec::Transform::Truncate(2));
        assert_eq!(fields[1].name, "region_bucket");
        assert_eq!(fields[1].transform, iceberg::spec::Transform::Bucket(16));
    }

    #[test]
    fn unknown_partition_column_is_typed() {
        let err = to_partition_spec(
            "table `x`",
            &schema(),
            &[PartitionField::new("nope", PartitionTransform::Identity)],
        )
        .expect_err("must fail");
        let text = format!("{err}");
        assert!(
            text.contains("nope") && text.contains("unknown column"),
            "{text}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_connector::core::{Provenance, TableName};

    fn column(name: &str, ty: ColumnType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            column_type: ty,
            nullable,
            provenance: Provenance::Inferred,
        }
    }

    #[test]
    fn closed_table_maps_every_scalar() {
        let schema = TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns: vec![
                column("b", ColumnType::scalar(LogicalType::Bool), false),
                column("i", ColumnType::scalar(LogicalType::Int64), false),
                column("f", ColumnType::scalar(LogicalType::Float64), true),
                column(
                    "d",
                    ColumnType::scalar(LogicalType::Decimal {
                        precision: 10,
                        scale: 2,
                    }),
                    true,
                ),
                column("s", ColumnType::scalar(LogicalType::Utf8), true),
                column("by", ColumnType::scalar(LogicalType::Binary), true),
                column("ts", ColumnType::scalar(LogicalType::TimestampTz), true),
                column("tn", ColumnType::scalar(LogicalType::TimestampNaive), true),
                column("da", ColumnType::scalar(LogicalType::Date), true),
                column("ti", ColumnType::scalar(LogicalType::Time), true),
                column("u", ColumnType::scalar(LogicalType::Uuid), true),
                column("j", ColumnType::scalar(LogicalType::Json), true),
            ],
        };
        let iceberg = to_iceberg_schema(&schema).expect("maps");
        assert_eq!(iceberg.as_struct().fields().len(), 12);
        // Json → string (documented), timestamps split tz/naive.
        let by_name = |n: &str| {
            iceberg
                .as_struct()
                .fields()
                .iter()
                .find(|f| f.name == n)
                .unwrap()
                .field_type
                .clone()
        };
        assert_eq!(*by_name("j"), Type::Primitive(PrimitiveType::String));
        assert_eq!(*by_name("ts"), Type::Primitive(PrimitiveType::Timestamptz));
        assert_eq!(*by_name("tn"), Type::Primitive(PrimitiveType::Timestamp));
    }

    #[test]
    fn nested_shapes_map_recursively_with_unique_ids() {
        let schema = TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns: vec![
                column(
                    "profile",
                    ColumnType::Struct {
                        fields: vec![
                            column("city", ColumnType::scalar(LogicalType::Utf8), true),
                            column("zip", ColumnType::scalar(LogicalType::Int64), true),
                        ],
                    },
                    true,
                ),
                column(
                    "tags",
                    ColumnType::ScalarList {
                        item: LogicalType::Utf8,
                    },
                    true,
                ),
            ],
        };
        let iceberg = to_iceberg_schema(&schema).expect("maps");
        // Unique, gapless-from-1 ids across nesting.
        let mut ids: Vec<i32> = iceberg
            .as_struct()
            .fields()
            .iter()
            .flat_map(|f| collect_ids(f))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 5, "profile, city, zip, tags, tags.element");
    }

    fn collect_ids(field: &NestedField) -> Vec<i32> {
        let mut ids = vec![field.id];
        match field.field_type.as_ref() {
            Type::Struct(s) => {
                for f in s.fields() {
                    ids.extend(collect_ids(f));
                }
            }
            Type::List(l) => ids.extend(collect_ids(&l.element_field)),
            _ => {}
        }
        ids
    }
}

/// How a live column differs from the one the stream wants, when it differs at
/// all.
#[derive(Debug, PartialEq)]
pub(crate) enum Drift {
    /// Different primitive types, or one is nested and the other is not.
    Type { wanted: String, live: String },
    /// Same shape, different children — a struct gained, lost or renamed a field.
    NestedFields { wanted: String, live: String },
    /// The table demands a value the stream may not supply.
    Nullability,
}

/// Compare what the stream wants against what the table holds, IGNORING field
/// ids.
///
/// Ids must be ignored because they are the catalog's to assign, not ours. We
/// number depth-first; a REST catalog renumbers level-order when it creates the
/// table, so for any schema with a nested column followed by another column the
/// two disagree from the moment of creation — and `Type`'s own `PartialEq`
/// compares nested `NestedField`s including their ids. Comparing types directly
/// therefore reports drift for a stream that has not changed at all, and the
/// second load of a struct-bearing table fails as contradictory.
///
/// Nullability is compared ASYMMETRICALLY: a table that REQUIRES a value the
/// stream says is optional cannot accept the stream's writes, so that is drift.
/// The reverse — the table permits null where the stream promises a value — is
/// something the table can hold, and additive evolution creates it deliberately
/// (a new column is added optional even when the stream declares it required).
pub(crate) fn compare_column(wanted: &NestedField, live: &NestedField) -> Result<(), Drift> {
    if live.required && !wanted.required {
        return Err(Drift::Nullability);
    }
    compare_type(&wanted.field_type, &live.field_type)
}

fn compare_type(wanted: &Type, live: &Type) -> Result<(), Drift> {
    let mismatch = || Drift::Type {
        wanted: wanted.to_string(),
        live: live.to_string(),
    };
    match (wanted, live) {
        (Type::Primitive(a), Type::Primitive(b)) => {
            if a == b {
                Ok(())
            } else {
                Err(mismatch())
            }
        }
        (Type::Struct(a), Type::Struct(b)) => {
            if a.fields().len() != b.fields().len() {
                return Err(Drift::NestedFields {
                    wanted: wanted.to_string(),
                    live: live.to_string(),
                });
            }
            // Matched by NAME, not by position: a catalog is free to reorder,
            // and a field that exists under a different name is a real change.
            for field in a.fields() {
                let counterpart = b.fields().iter().find(|f| f.name == field.name).ok_or(
                    Drift::NestedFields {
                        wanted: wanted.to_string(),
                        live: live.to_string(),
                    },
                )?;
                compare_column(field, counterpart)?;
            }
            Ok(())
        }
        (Type::List(a), Type::List(b)) => compare_column(&a.element_field, &b.element_field),
        (Type::Map(a), Type::Map(b)) => {
            compare_column(&a.key_field, &b.key_field)?;
            compare_column(&a.value_field, &b.value_field)
        }
        _ => Err(mismatch()),
    }
}

#[cfg(test)]
mod comparison_tests {
    use super::*;
    use rdlt_connector::core::{Provenance, TableName};

    fn column(name: &str, ty: ColumnType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            column_type: ty,
            nullable,
            provenance: Provenance::Inferred,
        }
    }

    fn nested_schema(child_columns: Vec<ColumnDef>) -> TableSchema {
        TableSchema {
            table: TableName::new("events"),
            parent: None,
            columns: vec![
                column(
                    "profile",
                    ColumnType::Struct {
                        fields: child_columns,
                    },
                    true,
                ),
                column("id", ColumnType::scalar(LogicalType::Int64), true),
            ],
        }
    }

    fn field<'a>(schema: &'a Schema, name: &str) -> &'a NestedField {
        schema
            .as_struct()
            .fields()
            .iter()
            .find(|f| f.name == name)
            .expect("field present")
    }

    /// The defect, container-free: a catalog renumbers field ids LEVEL-ORDER on
    /// create while this crate assigns them DEPTH-FIRST, so for any schema with
    /// a nested column followed by another column the two disagree from the
    /// moment the table exists. Comparing `field_type` directly then reports
    /// drift for a stream that has not changed, and the SECOND load of a
    /// struct-bearing table fails as contradictory.
    #[tokio::test]
    async fn an_unchanged_nested_column_is_not_drift_after_the_catalog_renumbers_ids() {
        let schema = nested_schema(vec![
            column("city", ColumnType::scalar(LogicalType::Utf8), true),
            column("zip", ColumnType::scalar(LogicalType::Int64), true),
        ]);
        let wanted = to_iceberg_schema(&schema).expect("maps");
        let table = crate::dest::test_support::table_with_schema(wanted.clone());
        let live = table.metadata().current_schema();

        // The ids really do diverge — otherwise this test proves nothing.
        assert_ne!(
            field(&wanted, "profile").field_type,
            field(live, "profile").field_type,
            "the catalog must have renumbered, or the pin is vacuous"
        );

        for name in ["profile", "id"] {
            compare_column(field(&wanted, name), field(live, name))
                .unwrap_or_else(|drift| panic!("`{name}` is unchanged, got {drift:?}"));
        }
    }

    /// Ignoring ids must not become ignoring CHANGES.
    #[tokio::test]
    async fn a_genuine_nested_change_is_still_refused() {
        let live_schema = to_iceberg_schema(&nested_schema(vec![column(
            "city",
            ColumnType::scalar(LogicalType::Utf8),
            true,
        )]))
        .expect("maps");
        let table = crate::dest::test_support::table_with_schema(live_schema);
        let live = table.metadata().current_schema();

        // A child field ADDED to the struct.
        let widened = to_iceberg_schema(&nested_schema(vec![
            column("city", ColumnType::scalar(LogicalType::Utf8), true),
            column("zip", ColumnType::scalar(LogicalType::Int64), true),
        ]))
        .expect("maps");
        assert_eq!(
            compare_column(field(&widened, "profile"), field(live, "profile")),
            Err(Drift::NestedFields {
                wanted: field(&widened, "profile").field_type.to_string(),
                live: field(live, "profile").field_type.to_string(),
            })
        );

        // A child field whose TYPE changed.
        let retyped = to_iceberg_schema(&nested_schema(vec![column(
            "city",
            ColumnType::scalar(LogicalType::Int64),
            true,
        )]))
        .expect("maps");
        assert!(matches!(
            compare_column(field(&retyped, "profile"), field(live, "profile")),
            Err(Drift::Type { .. })
        ));
    }

    /// Nullability is asymmetric: a table that REQUIRES a value the stream may
    /// not supply cannot take the stream's writes, and today that surfaces far
    /// away as a batching failure. The reverse is what additive evolution
    /// deliberately creates and must stay silent.
    #[tokio::test]
    async fn nullability_drift_is_asymmetric() {
        let required = NestedField::required(1, "v", Type::Primitive(PrimitiveType::Long));
        let optional = NestedField::optional(1, "v", Type::Primitive(PrimitiveType::Long));

        assert_eq!(
            compare_column(&optional, &required),
            Err(Drift::Nullability),
            "the table demands a value the stream may not supply"
        );
        assert_eq!(
            compare_column(&required, &optional),
            Ok(()),
            "a table that permits null can hold a stream that never sends one"
        );
    }
}
