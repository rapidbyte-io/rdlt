//! THE ONE logical↔arrow type map: logical → arrow for every batch the
//! engine assembles (`arrow_column_type`, `arrow_schema`), arrow → logical
//! for every structured batch a source pushes (`column_type_from_arrow`),
//! and the cross-batch join over the shared widening lattice
//! (`join_column_types`). The engine's own batches take their arrow types
//! from here; only the destination-seam lowering renders past it (a decimal
//! to text for a destination without decimals).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use rdlt_connector::channel::MAX_ARROW_DEPTH;
use rdlt_core::schema::{Column, ColumnType, Provenance, TableSchema};
use rdlt_core::types::LogicalType;

/// Arrow physical type for a logical type.
pub(super) fn arrow_scalar_type(ty: LogicalType) -> DataType {
    match ty {
        LogicalType::Bool => DataType::Boolean,
        LogicalType::Int64 => DataType::Int64,
        LogicalType::Float64 => DataType::Float64,
        LogicalType::Decimal { precision, scale } => DataType::Decimal128(precision, scale as i8),
        LogicalType::Utf8 | LogicalType::Uuid | LogicalType::Json => DataType::Utf8,
        LogicalType::Binary => DataType::Binary,
        LogicalType::TimestampTz => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        }
        LogicalType::TimestampNaive => DataType::Timestamp(TimeUnit::Microsecond, None),
        LogicalType::Date => DataType::Date32,
        LogicalType::Time => DataType::Time64(TimeUnit::Microsecond),
    }
}

pub(crate) fn arrow_column_type(ty: &ColumnType) -> DataType {
    match ty {
        ColumnType::Scalar { scalar } => arrow_scalar_type(*scalar),
        ColumnType::Struct { fields } => DataType::Struct(arrow_fields(fields)),
        ColumnType::ScalarList { item } => {
            DataType::List(Arc::new(Field::new("item", arrow_scalar_type(*item), true)))
        }
    }
}

pub(super) fn arrow_fields(columns: &[Column]) -> Fields {
    columns
        .iter()
        .map(|c| Field::new(&c.name, arrow_column_type(&c.column_type), c.nullable))
        .collect()
}

pub(crate) fn arrow_schema(schema: &TableSchema) -> Schema {
    Schema::new(arrow_fields(&schema.columns))
}

/// Least upper bound of two column types for cross-batch evolution: scalars
/// join on the widening lattice, lists join item-wise, structs join field-wise
/// (new fields append), and shape conflicts land on Json — the same outcomes
/// the shredder's observation states produce.
///
/// Depth-capped like every walk over connector-controlled nesting — belt to
/// `column_type_from_arrow`'s own gate, since both join inputs already passed
/// it (or the arena parser's depth bound): a stack overflow is an abort, so
/// the walk refuses rather than trusts.
pub(crate) fn join_column_types(a: &ColumnType, b: &ColumnType) -> Result<ColumnType, String> {
    join_column_types_at(a, b, 0)
}

fn join_column_types_at(
    a: &ColumnType,
    b: &ColumnType,
    depth: usize,
) -> Result<ColumnType, String> {
    use rdlt_core::types::widen;
    if depth >= MAX_ARROW_DEPTH {
        return Err(format!(
            "column nesting exceeds the {MAX_ARROW_DEPTH}-level cap — refused before \
             the cross-batch join can overflow the stack"
        ));
    }
    Ok(match (a, b) {
        _ if a == b => a.clone(),
        (ColumnType::Scalar { scalar: x }, ColumnType::Scalar { scalar: y }) => {
            ColumnType::scalar(widen(*x, *y))
        }
        (ColumnType::ScalarList { item: x }, ColumnType::ScalarList { item: y }) => {
            ColumnType::ScalarList {
                item: widen(*x, *y),
            }
        }
        (ColumnType::Struct { fields: xs }, ColumnType::Struct { fields: ys }) => {
            let mut joined = xs.clone();
            for y in ys {
                match joined.iter_mut().find(|x| x.name == y.name) {
                    Some(x) => {
                        x.column_type =
                            join_column_types_at(&x.column_type, &y.column_type, depth + 1)?;
                    }
                    None => joined.push(y.clone()),
                }
            }
            ColumnType::Struct { fields: joined }
        }
        // Shape conflict: preserved verbatim, never dropped (lattice top).
        _ => ColumnType::scalar(LogicalType::Json),
    })
}

/// Map one declared arrow type onto the logical lattice. Depth-capped: the
/// nesting of a structured stream's schema is entirely CONNECTOR-controlled,
/// and an uncapped recursive map turns a deep declaration into a stack
/// overflow — an ABORT, not a catchable panic, so `spawn_blocking`
/// containment cannot absorb it.
pub(crate) fn column_type_from_arrow(dt: &DataType) -> Result<ColumnType, String> {
    column_type_from_arrow_at(dt, 0)
}

fn column_type_from_arrow_at(dt: &DataType, depth: usize) -> Result<ColumnType, String> {
    // `>=`, aligned with the arena parser's ingest gate and — one level
    // at a time — with the lowering walk's path-length bound: the
    // deepest schema this door admits (63 struct levels, the leaf
    // visited at depth 63) is exactly the deepest a structs-off
    // destination can lower (a leaf's path of 64 names). A door one
    // level looser admits a schema the destination seam can only refuse
    // internal AFTER the delta is durable.
    if depth >= MAX_ARROW_DEPTH {
        return Err(format!(
            "schema nesting exceeds the {MAX_ARROW_DEPTH}-level cap — refused before \
             the mapping walk can overflow the stack"
        ));
    }
    let scalar = |t| Ok(ColumnType::scalar(t));
    match dt {
        DataType::Boolean => scalar(LogicalType::Bool),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => scalar(LogicalType::Int64),
        DataType::UInt64 => Err("UInt64 can exceed Int64; re-encode upstream".into()),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => scalar(LogicalType::Float64),
        DataType::Utf8 | DataType::LargeUtf8 => scalar(LogicalType::Utf8),
        DataType::Binary | DataType::LargeBinary => scalar(LogicalType::Binary),
        DataType::Timestamp(_, Some(_)) => scalar(LogicalType::TimestampTz),
        DataType::Timestamp(_, None) => scalar(LogicalType::TimestampNaive),
        DataType::Date32 | DataType::Date64 => scalar(LogicalType::Date),
        DataType::Time32(_) | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => {
            scalar(LogicalType::Time)
        }
        DataType::Decimal128(precision, scale)
            if (1..=38).contains(precision) && *scale >= 0 && *scale <= *precision as i8 =>
        {
            Ok(ColumnType::scalar(LogicalType::Decimal {
                precision: *precision,
                scale: *scale as u8,
            }))
        }
        DataType::Struct(fields) => {
            let mapped: Result<Vec<Column>, String> = fields
                .iter()
                .map(|f| {
                    Ok(Column {
                        name: f.name().clone(),
                        column_type: column_type_from_arrow_at(f.data_type(), depth + 1)?,
                        nullable: true,
                        provenance: Provenance::Inferred,
                    })
                })
                .collect();
            Ok(ColumnType::Struct { fields: mapped? })
        }
        DataType::List(item) | DataType::LargeList(item) => {
            match column_type_from_arrow_at(item.data_type(), depth + 1)? {
                ColumnType::Scalar { scalar } => Ok(ColumnType::ScalarList { item: scalar }),
                _ => Err("nested lists / lists of structs are not supported in v1".into()),
            }
        }
        other => Err(format!("no logical mapping for {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A negative decimal scale must be a typed error, never a silent mapping.
    #[test]
    fn negative_decimal_scale_is_a_typed_error() {
        assert!(column_type_from_arrow(&DataType::Decimal128(10, 2)).is_ok());
        let err = column_type_from_arrow(&DataType::Decimal128(10, -2))
            .expect_err("negative scale must not map");
        assert!(err.contains("no logical mapping"), "got: {err}");
    }

    /// The Arrow path enforces the same decimal domain the JSON hint path
    /// and the destination-facing logical schema require.
    #[test]
    fn invalid_decimal_precision_and_scale_are_typed_errors() {
        for invalid in [
            DataType::Decimal128(0, 0),
            DataType::Decimal128(39, 0),
            DataType::Decimal128(10, 11),
        ] {
            assert!(
                column_type_from_arrow(&invalid).is_err(),
                "{invalid} must not enter the registry"
            );
        }
        assert!(column_type_from_arrow(&DataType::Decimal128(38, 38)).is_ok());
    }

    /// The List arm's inner-scalar match: a list of scalars maps to
    /// ScalarList; lists of structs/lists are typed errors.
    #[test]
    fn list_mapping_accepts_scalars_rejects_nesting() {
        let list_of = |dt| DataType::List(Arc::new(Field::new("item", dt, true)));

        assert_eq!(
            column_type_from_arrow(&list_of(DataType::Int64)).expect("scalar list"),
            ColumnType::ScalarList {
                item: LogicalType::Int64
            }
        );
        let err = column_type_from_arrow(&list_of(list_of(DataType::Int64)))
            .expect_err("nested lists are v1-unsupported");
        assert!(err.contains("not supported"), "got: {err}");
        let err = column_type_from_arrow(&list_of(DataType::Struct(
            vec![Field::new("f", DataType::Int64, true)].into(),
        )))
        .expect_err("lists of structs are v1-unsupported");
        assert!(err.contains("not supported"), "got: {err}");
    }

    /// Schema nesting is CONNECTOR-controlled on structured streams, and an
    /// uncapped mapping walk turns a deep declaration into a stack overflow
    /// — an abort no task containment absorbs. Past the shared cap the walk
    /// refuses with a typed error.
    #[test]
    fn a_schema_nested_past_the_depth_cap_refuses_instead_of_recursing() {
        let deep = |levels: usize| -> DataType {
            let mut dt = DataType::Int64;
            for _ in 0..levels {
                dt = DataType::Struct(vec![Field::new("f", dt, true)].into());
            }
            dt
        };
        let err = column_type_from_arrow(&deep(100))
            .expect_err("a 100-deep declared schema must refuse, not walk");
        assert!(err.contains("nesting"), "the refusal names the cap: {err}");
        assert!(
            column_type_from_arrow(&deep(10)).is_ok(),
            "ordinary nesting still maps"
        );
    }

    /// THE BOUNDARY, exactly: this door and the lowering walk must admit
    /// the same deepest schema. The lowering walk admits 63 struct
    /// levels (a leaf's path is levels + 1 names, refused past
    /// `MAX_ARROW_DEPTH`), so the 63rd level maps here and the 64th
    /// refuses HERE — at admission, where the refusal classifies as the
    /// connector's declaration. A door one level looser hands the
    /// destination seam a schema it must refuse as internal AFTER the
    /// delta became durable, and recovery then re-delivers the admitted
    /// record forever.
    #[test]
    fn the_door_admits_63_struct_levels_and_refuses_the_64th() {
        let deep = |levels: usize| -> DataType {
            let mut dt = DataType::Int64;
            for _ in 0..levels {
                dt = DataType::Struct(vec![Field::new("f", dt, true)].into());
            }
            dt
        };
        column_type_from_arrow(&deep(63)).expect("63 struct levels map");
        let err = column_type_from_arrow(&deep(64)).expect_err("the 64th level refuses");
        assert!(err.contains("nesting"), "the refusal names the cap: {err}");
    }

    /// The cross-batch join walks the SAME connector-controlled nesting and
    /// carries the same cap — belt to `column_type_from_arrow`'s gate, since
    /// both join inputs already passed it (or the arena parser's own depth
    /// bound).
    #[test]
    fn a_join_past_the_depth_cap_refuses_instead_of_recursing() {
        // The two sides must DIFFER at the leaf, or the equality fast path
        // answers before any recursion happens.
        let deep = |levels: usize, leaf: LogicalType| -> ColumnType {
            let mut ty = ColumnType::scalar(leaf);
            for _ in 0..levels {
                ty = ColumnType::Struct {
                    fields: vec![Column {
                        name: "f".to_owned(),
                        column_type: ty,
                        nullable: true,
                        provenance: Provenance::Inferred,
                    }],
                };
            }
            ty
        };
        let err = join_column_types(
            &deep(100, LogicalType::Int64),
            &deep(100, LogicalType::Float64),
        )
        .expect_err("a 100-deep join must refuse, not walk");
        assert!(err.contains("nesting"), "the refusal names the cap: {err}");
        assert!(
            join_column_types(
                &deep(10, LogicalType::Int64),
                &deep(10, LogicalType::Float64)
            )
            .is_ok(),
            "ordinary nesting still joins"
        );
    }
}
