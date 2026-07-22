//! The CLOSED engine-type → Iceberg-type mapping (contract ID4,
//! data-model §2). Unmappable columns are typed at ensure-table naming the
//! column. Field IDs are assigned SEQUENTIALLY at creation time only — the
//! catalog normalizes them on create, and post-creation evolution goes
//! through UpdateSchema (which assigns fresh IDs); this crate never
//! renumbers or reuses an ID.

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use rdlt_connector::DestError;
use rdlt_connector::core::{ColumnDef, ColumnType, LogicalType, TableSchema};

fn fatal(message: impl std::fmt::Display) -> DestError {
    DestError::fatal(message.to_string())
}

/// Map one scalar logical type. `Json` maps to string — Iceberg v2 has no
/// JSON type (documented; the variant type is future work).
fn scalar_type(table: &str, column: &str, scalar: LogicalType) -> Result<Type, DestError> {
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
                 iceberg mapping (closed table, contract ID4)"
            )));
        }
    }))
}

/// Build one field, assigning IDs from the running counter.
fn build_field(
    table: &str,
    column: &ColumnDef,
    next_id: &mut i32,
) -> Result<NestedField, DestError> {
    let id = *next_id;
    *next_id += 1;
    let field_type = match &column.ty {
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
pub(crate) fn to_iceberg_schema(schema: &TableSchema) -> Result<Schema, DestError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_connector::core::{Provenance, TableName};

    fn column(name: &str, ty: ColumnType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            ty,
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
