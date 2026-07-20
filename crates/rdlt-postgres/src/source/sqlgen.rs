//! SQL assembly for the COPY subselect (research R1/R5). Injection-safe by
//! construction: identifiers come ONLY from reflection/config validated
//! against reflection, and are strictly double-quoted; COPY accepts no bind
//! parameters, so cursor literals (Phase 4) render as typed literals with
//! explicit casts — never raw user strings.

use crate::source::reflect::ReflectedColumn;
use crate::source::types::SelectPolicy;

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
    match &column.mapped.select {
        SelectPolicy::Direct => ident,
        SelectPolicy::CastText => format!("({ident})::text AS {ident}"),
        SelectPolicy::CastJsonbText => format!("to_jsonb({ident})::text AS {ident}"),
        // Per-column type hint (006): strict server-side cast to the target.
        SelectPolicy::HintCast(target) => format!("({ident})::{target} AS {ident}"),
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

/// The universal query wrapper (feature 006, contract query-streams.md):
/// read-only enforcement + a stable surface for predicates and snapshots.
pub(crate) fn wrap_query(sql: &str) -> String {
    format!(
        "SELECT * FROM ( {} ) AS q",
        sql.trim().trim_end_matches(';')
    )
}

/// A SELECT over an arbitrary FROM clause (query streams); the table form
/// below delegates here.
pub(crate) fn select_sql_from(
    from: &str,
    columns: &[&ReflectedColumn],
    where_sql: &str,
    order_sql: &str,
) -> String {
    let cols: Vec<String> = columns.iter().map(|c| projection(c)).collect();
    let mut sql = format!("SELECT {} FROM {from}", cols.join(", "));
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

/// Incremental WHERE/ORDER BY rendering (research R5): the full dlt-parity
/// boundary matrix. `lower_closed` is the RESUME semantics (`>=` re-fetches
/// watermark-equal rows for dedup; `>` skips them), independent of direction.
pub(crate) struct IncrementalClauses {
    pub where_sql: String,
    pub order_sql: String,
}

pub(crate) fn incremental_clauses(
    column: &str,
    direction_max: bool,
    lower: Option<(&crate::source::cursor::Watermark, bool)>, // (value, closed?)
    // Feature 007 lag: SQL delta widening the resume window BEHIND the
    // watermark (`- delta` under max, `+ delta` under min) — read-side only.
    lag_delta: Option<&str>,
    upper: Option<&crate::source::cursor::Watermark>,
    nulls_include: bool,
    // Text cursors force COLLATE "C": the tracker compares watermarks in
    // Rust byte order, so the SQL ordering/filtering must use byte order
    // too — a column collation (ICU, en_US…) would diverge (005 review).
    collate_byte_order: bool,
) -> IncrementalClauses {
    let bare = quote_ident(column);
    let ident = if collate_byte_order {
        format!("{bare} COLLATE \"C\"")
    } else {
        bare
    };
    let mut predicates: Vec<String> = Vec::new();
    if let Some((value, closed)) = lower {
        let op = match (direction_max, closed) {
            (true, true) => ">=",
            (true, false) => ">",
            (false, true) => "<=",
            (false, false) => "<",
        };
        let literal = value.to_sql_literal();
        let bound = match lag_delta {
            Some(delta) => format!(
                "({literal} {} {delta})",
                if direction_max { "-" } else { "+" }
            ),
            None => literal,
        };
        predicates.push(format!("{ident} {op} {bound}"));
    }
    if let Some(value) = upper {
        let op = if direction_max { "<" } else { ">" };
        predicates.push(format!("{ident} {op} {}", value.to_sql_literal()));
    }
    let where_sql = match (predicates.is_empty(), nulls_include) {
        (true, true) => String::new(),
        (true, false) => format!("{ident} IS NOT NULL"),
        (false, true) => format!("(({}) OR {ident} IS NULL)", predicates.join(" AND ")),
        (false, false) => format!("{} AND {ident} IS NOT NULL", predicates.join(" AND ")),
    };
    // NULLS FIRST keeps the watermark tail at the stream end in BOTH
    // directions — checkpoint tracking depends on it.
    let order_sql = format!(
        "{ident} {} NULLS FIRST",
        if direction_max { "ASC" } else { "DESC" }
    );
    IncrementalClauses {
        where_sql,
        order_sql,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::reflect::ReflectedColumn;
    use crate::source::types::{PgTypeInfo, map_type, oid};

    fn col(name: &str, o: u32) -> ReflectedColumn {
        let info = PgTypeInfo {
            oid: o,
            typtype: 'b',
            typcategory: 'X',
            typmod: -1,
        };
        ReflectedColumn {
            name: name.into(),
            type_name: "t".into(),
            mapped: map_type(&info),
            type_info: info,
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
    fn incremental_boundary_matrix() {
        use crate::source::cursor::Watermark;
        let w = Watermark::Int(5);
        let e = Watermark::Int(9);
        // (direction_max, closed) × operators
        let c = incremental_clauses("ts", true, Some((&w, true)), None, None, false, false);
        assert_eq!(c.where_sql, r#""ts" >= 5::int8 AND "ts" IS NOT NULL"#);
        assert_eq!(c.order_sql, r#""ts" ASC NULLS FIRST"#);
        let c = incremental_clauses("ts", true, Some((&w, false)), None, Some(&e), false, false);
        assert_eq!(
            c.where_sql,
            r#""ts" > 5::int8 AND "ts" < 9::int8 AND "ts" IS NOT NULL"#
        );
        let c = incremental_clauses("ts", false, Some((&w, true)), None, Some(&e), false, false);
        assert_eq!(
            c.where_sql,
            r#""ts" <= 5::int8 AND "ts" > 9::int8 AND "ts" IS NOT NULL"#
        );
        assert_eq!(c.order_sql, r#""ts" DESC NULLS FIRST"#);
        // NULL policy include wraps with OR IS NULL; bare include has no filter.
        let c = incremental_clauses("ts", true, Some((&w, true)), None, None, true, false);
        assert_eq!(c.where_sql, r#"(("ts" >= 5::int8) OR "ts" IS NULL)"#);
        let c = incremental_clauses("ts", true, None, None, None, true, false);
        assert_eq!(c.where_sql, "");
        let c = incremental_clauses("ts", true, None, None, None, false, false);
        assert_eq!(c.where_sql, r#""ts" IS NOT NULL"#);
        // Hostile cursor string literal stays inert.
        let hostile = Watermark::Text("x'; DROP TABLE t; --".into());
        let c = incremental_clauses("v", true, Some((&hostile, true)), None, None, false, true);
        assert!(
            c.where_sql.contains("'x''; DROP TABLE t; --'::text"),
            "{}",
            c.where_sql
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

    // ---- feature 007: lag rendering (cursor-lag.md L1) ----

    #[test]
    fn lag_delta_widens_the_lower_bound_direction_aware() {
        use crate::source::cursor::Watermark;
        let w = Watermark::TimestampTz(1_000_000);
        // max direction: watermark MINUS the window, closed.
        let c = incremental_clauses(
            "ts",
            true,
            Some((&w, true)),
            Some("INTERVAL '300 seconds'"),
            None,
            false,
            false,
        );
        assert_eq!(
            c.where_sql,
            "\"ts\" >= ((TIMESTAMPTZ 'epoch' + 1000000::int8 * INTERVAL '1 microsecond') \
             - INTERVAL '300 seconds') AND \"ts\" IS NOT NULL"
        );
        // min direction: watermark PLUS the window.
        let c = incremental_clauses(
            "ts",
            false,
            Some((&w, true)),
            Some("INTERVAL '300 seconds'"),
            None,
            false,
            false,
        );
        assert!(
            c.where_sql.contains("+ INTERVAL '300 seconds'"),
            "{}",
            c.where_sql
        );
        assert!(c.where_sql.contains("<="), "{}", c.where_sql);
        // No lag: rendering byte-identical to before (state untouched).
        let c = incremental_clauses("ts", true, Some((&w, true)), None, None, false, false);
        assert!(!c.where_sql.contains("INTERVAL '300"), "{}", c.where_sql);
    }
}
