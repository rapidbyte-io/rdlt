//! SQL assembly for the COPY subselect (research R1/R5). Injection-safe by
//! construction: identifiers come ONLY from reflection/config validated
//! against reflection, and are strictly double-quoted; COPY accepts no bind
//! parameters, so cursor literals (Phase 4) render as typed literals with
//! explicit casts — never raw user strings.

use crate::reflect::ReflectedColumn;
use crate::types::SelectPolicy;

/// PostgreSQL identifier quoting: wrap in double quotes, double any embedded
/// quote. Total — any string becomes a safe identifier.
pub(crate) fn quote_ident(ident: &str) -> String {
    let mut quoted = String::with_capacity(ident.len() + 2);
    quoted.push('"');
    for ch in ident.chars() {
        if ch == '"' {
            quoted.push('"');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    quoted
}

/// One projection item per the column's SelectPolicy; policy conversions run
/// server-side so the wire only carries the lossless decode set.
fn projection(column: &ReflectedColumn) -> String {
    let ident = quote_ident(&column.name);
    match column.mapped.select {
        SelectPolicy::Direct => ident,
        SelectPolicy::CastText => format!("({ident})::text AS {ident}"),
        SelectPolicy::CastJsonbText => format!("to_jsonb({ident})::text AS {ident}"),
    }
}

/// The snapshot SELECT for a table (schema-qualified, selected columns in
/// reflected order). `where_sql`/`order_sql` are Phase-4 hooks (already
/// rendered, or empty).
pub(crate) fn select_sql(
    schema: &str,
    table: &str,
    columns: &[&ReflectedColumn],
    where_sql: &str,
    order_sql: &str,
) -> String {
    let cols: Vec<String> = columns.iter().map(|c| projection(c)).collect();
    let mut sql = format!(
        "SELECT {} FROM {}.{}",
        cols.join(", "),
        quote_ident(schema),
        quote_ident(table)
    );
    if !where_sql.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(where_sql);
    }
    if !order_sql.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(order_sql);
    }
    sql
}

pub(crate) fn copy_sql(select: &str) -> String {
    format!("COPY ({select}) TO STDOUT (FORMAT BINARY)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reflect::ReflectedColumn;
    use crate::types::{map_type, oid, PgTypeInfo};

    fn col(name: &str, o: u32) -> ReflectedColumn {
        ReflectedColumn {
            name: name.into(),
            type_name: "t".into(),
            mapped: map_type(&PgTypeInfo {
                oid: o,
                typtype: 'b',
                typcategory: 'X',
                typmod: -1,
            }),
            not_null: false,
            is_pk: false,
        }
    }

    #[test]
    fn quotes_hostile_identifiers() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("Order Items"), "\"Order Items\"");
        // Embedded quotes double; injection attempts stay inert identifiers.
        assert_eq!(
            quote_ident(r#"x"; DROP TABLE t; --"#),
            r#""x""; DROP TABLE t; --""#
        );
    }

    #[test]
    fn projects_policies_server_side() {
        let id = col("id", oid::INT8);
        let money = col("price", 790); // money → ::text
        let tags = ReflectedColumn {
            mapped: map_type(&PgTypeInfo {
                oid: 1007,
                typtype: 'b',
                typcategory: 'A',
                typmod: -1,
            }),
            ..col("tags", 1007)
        };
        let sql = select_sql("public", "t", &[&id, &money, &tags], "", "");
        assert_eq!(
            sql,
            r#"SELECT "id", ("price")::text AS "price", to_jsonb("tags")::text AS "tags" FROM "public"."t""#
        );
    }

    #[test]
    fn where_and_order_hooks() {
        let id = col("id", oid::INT8);
        let sql = select_sql("s", "t", &[&id], r#""id" > 5"#, r#""id" ASC"#);
        assert_eq!(
            sql,
            r#"SELECT "id" FROM "s"."t" WHERE "id" > 5 ORDER BY "id" ASC"#
        );
        assert_eq!(
            copy_sql("SELECT 1"),
            "COPY (SELECT 1) TO STDOUT (FORMAT BINARY)"
        );
    }
}
