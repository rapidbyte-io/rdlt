//! The read-before-write catalog image: a FRESH session's only way to
//! see what a PRIOR session already ensured. Feeds ONLY the widen
//! planner's `previous` parameter (031's S3 record; 028's
//! read-before-write catalog-image answer, re-derived here for the
//! schema shape rather than snapshot properties) — never the
//! drop/reorder guards in `load.rs::ensure_table`, which stay
//! session-scoped on purpose: cross-run column drift-BY-NAME is legal
//! (`test_conformance.rs::cross_run_column_drift_publishes_by_name`),
//! only a within-session drop or reorder is a defect.

use duckdb::params;
use rdlt_connector_sdk::spi::DestinationError;
use rdlt_connector_sdk::spi::core::{ColumnDef, ColumnType, LogicalType, Provenance, TableSchema};

use super::client::classify;

/// The live table's schema, mapped back into [`ColumnType`] — `None`
/// when the table does not exist yet (nothing for the widen planner to
/// compare against).
///
/// Column order comes from `ordinal_position`, matching how this
/// destination's own DDL only ever appends — so the image's column
/// order agrees with a session that had ensured the same table.
pub(super) fn live_schema(
    conn: &duckdb::Connection,
    table: &TableSchema,
) -> Result<Option<TableSchema>, DestinationError> {
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = ? ORDER BY ordinal_position",
        )
        .map_err(classify)?;
    let mut rows = stmt
        .query(params![table.table.as_str()])
        .map_err(classify)?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next().map_err(classify)? {
        let name: String = row.get(0).map_err(classify)?;
        let duckdb_type: String = row.get(1).map_err(classify)?;
        // An unmapped type is OMITTED, never guessed: inventing a shape
        // here could plan a spurious widen every run against a column
        // that never changed — exactly what the round-trip test below
        // guards against.
        let Some(column_type) = column_type_of(&duckdb_type) else {
            continue;
        };
        columns.push(ColumnDef {
            name,
            column_type,
            // Physically unrecoverable, and unused by the widen
            // planner either way (`ensure::schema_steps` compares
            // `column_type` alone): every column this destination's
            // own DDL creates is nullable — `create_table_sql` never
            // emits NOT NULL — and provenance is metadata the catalog
            // carries no trace of.
            nullable: true,
            provenance: Provenance::Inferred,
        });
    }
    if columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(TableSchema {
        table: table.table.clone(),
        parent: table.parent.clone(),
        columns,
    }))
}

/// The inverse of [`super::schema::sql_type`]'s target-leg rendering
/// (`is_stage: false` — a fresh session only ever reads the target
/// back). TOTAL: an unrecognized spelling returns `None` rather than
/// guessing, so a future logical type this function doesn't know yet
/// never plans a widen against a column it can't actually interpret.
///
/// `Uuid` is deliberately never RETURNED here: `sql_type` already lowers
/// it to the same `VARCHAR` as `Utf8` ("text for portability with the
/// hex `_rdlt_id` convention"), so the physical catalog cannot tell the
/// two apart. `VARCHAR` maps back to `Utf8`, the far more common shape;
/// a genuinely `Uuid`-hinted column pays a same-shaped no-op widen on
/// every run this image feeds — a narrow, pre-existing cost of an
/// already-lossy lowering, not a new one this function introduces.
fn column_type_of(duckdb_type: &str) -> Option<ColumnType> {
    if let Some(item) = duckdb_type.strip_suffix("[]") {
        return scalar_type_of(item).map(|item| ColumnType::ScalarList { item });
    }
    if let Some(body) = duckdb_type
        .strip_prefix("STRUCT(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let mut fields = Vec::new();
        for field in split_top_level(body) {
            let (name, rest) = split_identifier(field.trim())?;
            let column_type = column_type_of(rest.trim())?;
            fields.push(ColumnDef {
                name,
                column_type,
                nullable: true,
                provenance: Provenance::Inferred,
            });
        }
        return Some(ColumnType::Struct { fields });
    }
    scalar_type_of(duckdb_type)
        .map(ColumnType::scalar)
        .or_else(|| decimal_type_of(duckdb_type).map(ColumnType::scalar))
}

fn scalar_type_of(duckdb_type: &str) -> Option<LogicalType> {
    Some(match duckdb_type {
        "BOOLEAN" => LogicalType::Bool,
        "BIGINT" => LogicalType::Int64,
        "DOUBLE" => LogicalType::Float64,
        "VARCHAR" => LogicalType::Utf8,
        "BLOB" => LogicalType::Binary,
        "TIMESTAMP WITH TIME ZONE" => LogicalType::TimestampTz,
        "TIMESTAMP" => LogicalType::TimestampNaive,
        "DATE" => LogicalType::Date,
        "TIME" => LogicalType::Time,
        "JSON" => LogicalType::Json,
        _ => return None,
    })
}

fn decimal_type_of(duckdb_type: &str) -> Option<LogicalType> {
    let body = duckdb_type
        .strip_prefix("DECIMAL(")
        .and_then(|s| s.strip_suffix(')'))?;
    let (precision, scale) = body.split_once(',')?;
    Some(LogicalType::Decimal {
        precision: precision.trim().parse().ok()?,
        scale: scale.trim().parse().ok()?,
    })
}

/// Split a `STRUCT(...)` body on its top-level commas — depth- and
/// quote-aware, so a nested `STRUCT(...)` or a `DECIMAL(p,s)` field
/// type never gets cut in the middle.
fn split_top_level(body: &str) -> Vec<&str> {
    let mut depth = 0i32;
    let mut in_quotes = false;
    let mut start = 0;
    let mut parts = Vec::new();
    for (i, c) in body.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            '(' if !in_quotes => depth += 1,
            ')' if !in_quotes => depth -= 1,
            ',' if !in_quotes && depth == 0 => {
                parts.push(&body[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&body[start..]);
    parts
}

/// One `"name" TYPE` (or bare `name TYPE` when the name needs no
/// quoting) field spec's leading identifier — the mirror of
/// [`rdlt_connector_sqlcore::quote_identifier`]'s `"` doubling — and
/// the type text that follows it.
fn split_identifier(field: &str) -> Option<(String, &str)> {
    if let Some(rest) = field.strip_prefix('"') {
        let mut name = String::new();
        let mut chars = rest.char_indices();
        while let Some((i, c)) = chars.next() {
            if c != '"' {
                name.push(c);
                continue;
            }
            if rest[i + 1..].starts_with('"') {
                name.push('"');
                chars.next();
            } else {
                return Some((name, rest[i + 1..].trim_start()));
            }
        }
        None
    } else {
        let idx = field.find(char::is_whitespace)?;
        Some((field[..idx].to_owned(), field[idx..].trim_start()))
    }
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::spi::core::TableName;

    use super::super::schema::create_table_sql;
    use super::*;

    fn memdb() -> duckdb::Connection {
        duckdb::Connection::open_in_memory().expect("bundled in-memory duckdb")
    }

    fn col(name: &str, column_type: ColumnType) -> ColumnDef {
        ColumnDef {
            name: name.to_owned(),
            column_type,
            nullable: true,
            provenance: Provenance::Inferred,
        }
    }

    /// One of every [`ColumnType`] shape this connector's own DDL can
    /// produce — every [`LogicalType`] except `Uuid` (see
    /// `column_type_of`'s doc comment: it is physically indistinguishable
    /// from `Utf8` and is EXPECTED not to round-trip), plus a struct and
    /// a scalar list.
    fn schema_with_every_column_type(table: &str) -> TableSchema {
        TableSchema {
            table: TableName::new(table),
            parent: None,
            columns: vec![
                col("c_bool", ColumnType::scalar(LogicalType::Bool)),
                col("c_int64", ColumnType::scalar(LogicalType::Int64)),
                col("c_float64", ColumnType::scalar(LogicalType::Float64)),
                col(
                    "c_decimal",
                    ColumnType::scalar(LogicalType::Decimal {
                        precision: 18,
                        scale: 4,
                    }),
                ),
                col("c_utf8", ColumnType::scalar(LogicalType::Utf8)),
                col("c_binary", ColumnType::scalar(LogicalType::Binary)),
                col(
                    "c_timestamptz",
                    ColumnType::scalar(LogicalType::TimestampTz),
                ),
                col(
                    "c_timestampnaive",
                    ColumnType::scalar(LogicalType::TimestampNaive),
                ),
                col("c_date", ColumnType::scalar(LogicalType::Date)),
                col("c_time", ColumnType::scalar(LogicalType::Time)),
                col("c_json", ColumnType::scalar(LogicalType::Json)),
                col(
                    "c_list",
                    ColumnType::ScalarList {
                        item: LogicalType::Int64,
                    },
                ),
                col(
                    "c_struct",
                    ColumnType::Struct {
                        fields: vec![
                            col("a b", ColumnType::scalar(LogicalType::Int64)),
                            col("s", ColumnType::scalar(LogicalType::Utf8)),
                            col(
                                "d",
                                ColumnType::scalar(LogicalType::Decimal {
                                    precision: 5,
                                    scale: 1,
                                }),
                            ),
                        ],
                    },
                ),
            ],
        }
    }

    /// The fidelity guard: any mismatch here would plan a widen EVERY
    /// RUN against a table that never actually changed.
    #[test]
    fn every_column_type_round_trips_through_the_live_image() {
        let conn = memdb();
        let schema = schema_with_every_column_type("t");
        conn.execute_batch(&create_table_sql("t", &schema, false))
            .expect("ddl");
        let image = live_schema(&conn, &schema)
            .expect("image")
            .expect("table exists");
        assert_eq!(image.columns, schema.columns);
    }

    /// A table that was never created images to `None` — there is
    /// nothing for the widen planner to compare against, so the ensure
    /// path's own `Table { leg }` step (not a phantom widen) is what
    /// creates it.
    #[test]
    fn a_table_that_does_not_exist_images_to_none() {
        let conn = memdb();
        let schema = schema_with_every_column_type("absent");
        assert!(live_schema(&conn, &schema).expect("no error").is_none());
    }

    /// The documented Uuid/Utf8 collapse: a Uuid column images back as
    /// Utf8 (VARCHAR is all the catalog can report), never as Uuid.
    #[test]
    fn a_uuid_column_images_as_utf8() {
        let conn = memdb();
        let schema = TableSchema {
            table: TableName::new("u"),
            parent: None,
            columns: vec![col("id", ColumnType::scalar(LogicalType::Uuid))],
        };
        conn.execute_batch(&create_table_sql("u", &schema, false))
            .expect("ddl");
        let image = live_schema(&conn, &schema).expect("image").expect("exists");
        assert_eq!(
            image.columns[0].column_type,
            ColumnType::scalar(LogicalType::Utf8)
        );
    }

    /// An unmapped type spelling omits the column rather than guessing —
    /// a hostile probe on the parser, not a shape this connector's own
    /// DDL ever produces.
    #[test]
    fn an_unmapped_type_spelling_is_none() {
        assert_eq!(column_type_of("HUGEINT"), None);
        assert_eq!(column_type_of("MAP(VARCHAR, INTEGER)"), None);
    }
}
