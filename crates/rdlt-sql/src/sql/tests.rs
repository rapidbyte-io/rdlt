use std::sync::Arc;
use std::thread;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdlt_engine::{Transform, codes};

use super::Sql;
use crate::contain::PANICS;
use crate::limits::PLANS;
use crate::planned::Planned;
use crate::{SQL_BYTES, SqlErrorKind, function_names};

#[test]
fn a_projection_of_input_parses_and_says_whether_it_filters() {
    let cases = [
        ("SELECT * FROM input", false),
        ("SELECT id, lower(email) AS email FROM input", false),
        ("SELECT i.id FROM input AS i WHERE i.status <> 'test'", true),
        (
            "SELECT id, CASE WHEN n > 0 THEN 'pos' ELSE 'neg' END AS sign FROM input \
             WHERE n IS NOT NULL",
            true,
        ),
        (
            "select ID, UPPER(Name) as Name from input where regexp_like(Name, '^a', 'i')",
            true,
        ),
        (
            "SELECT regexp_replace(t, '[a-z]', 'xy', 'g') AS r FROM input WHERE t ~* '^x'",
            true,
        ),
        (
            "SELECT substr(t, 2, 3) AS s, substring(t FROM 1 FOR 2) AS u FROM input",
            false,
        ),
    ];
    for (text, filters) in cases {
        let sql = Sql::parse(text).unwrap_or_else(|error| panic!("{text}: {error}"));
        assert_eq!(sql.filters, filters, "{text}");
    }
}

/// Asserts that each of `texts` is refused as `kind`.
fn refused_as(kind: SqlErrorKind, texts: &[&str]) {
    for text in texts {
        let shown: String = text.chars().take(80).collect();
        let refused = Sql::parse(text)
            .err()
            .unwrap_or_else(|| panic!("{shown} parsed"));
        assert_eq!(refused.kind(), kind, "{shown}: {refused}");
    }
}

#[test]
fn text_too_long_nested_too_deep_or_not_one_query_is_refused() {
    let long = format!("SELECT id FROM input --{}", "x".repeat(SQL_BYTES));
    refused_as(SqlErrorKind::TooLong, &[&long]);
    let nested = format!(
        "SELECT {}1{} AS x FROM input",
        "(".repeat(100),
        ")".repeat(100)
    );
    let chained = format!("SELECT {}n AS x FROM input", "n + ".repeat(15_000));
    refused_as(
        SqlErrorKind::Syntax,
        &[
            &nested,
            &chained,
            "SELECT id FROM input; SELECT id FROM input",
            "SELECT id FROM",
            "SELECT 'unterminated FROM input",
        ],
    );
}

#[test]
fn a_query_other_than_a_filtered_projection_of_input_is_refused() {
    refused_as(
        SqlErrorKind::Shape,
        &[
            "EXPLAIN SELECT id FROM input",
            "DELETE FROM input",
            "SELECT id FROM other",
            "SELECT a.id FROM input a JOIN input b ON a.id = b.id",
            "SELECT id FROM input, input",
            "SELECT id FROM input GROUP BY id",
            "SELECT id FROM input ORDER BY id",
            "SELECT id FROM input LIMIT 1",
            "SELECT DISTINCT id FROM input",
            "SELECT id FROM input UNION SELECT id FROM input",
            "WITH x AS (SELECT 1) SELECT * FROM input",
            "SELECT (SELECT 1) AS x FROM input",
            "SELECT id FROM input WHERE id IN (SELECT id FROM input)",
            "SELECT id FROM input TABLESAMPLE (10 PERCENT)",
            "SELECT id FROM input FOR UPDATE",
            "SELECT id FROM input FOR XML AUTO",
            "SELECT id FROM input SETTINGS x = 1",
            "SELECT id FROM input FORMAT JSON",
            "SELECT id FROM input PREWHERE id > 1",
            "SELECT id FROM input CONNECT BY PRIOR id = id",
            "SELECT id FROM input CLUSTER BY id",
            "SELECT id FROM input DISTRIBUTE BY id",
            "SELECT id FROM input SORT BY id",
            "SELECT id FROM input WINDOW w AS (PARTITION BY id)",
            "SELECT id FROM input QUALIFY id > 1",
            "SELECT id FROM input PARTITION (p)",
            "SELECT id FROM input WITH (NOLOCK)",
            "SELECT id FROM (SELECT id FROM input)",
            "SELECT id INTO x FROM input",
            "SELECT /*+ hint */ id FROM input",
            "SELECT id FROM input LATERAL VIEW explode(a) t AS b",
            "SELECT id FROM generate_series(1, 3)",
            "SELECT EXTRACT(year FROM t) AS y FROM input",
            "SELECT id FROM input WHERE id = $1",
            "SELECT n & 1 AS b FROM input",
            "SELECT t -> 'a' AS j FROM input",
            "SELECT ~n AS b FROM input",
            "SELECT id AS _rdlt_op FROM input",
            "SELECT *, CAST(2 AS TINYINT) AS _rdlt_op FROM input",
        ],
    );
}

#[test]
fn a_query_of_another_table_says_what_to_write() {
    for text in [
        "SELECT id FROM other",
        "SELECT a.id FROM input a JOIN input b ON a.id = b.id",
        "SELECT id FROM generate_series(1, 3)",
    ] {
        let refused = Sql::parse(text)
            .err()
            .unwrap_or_else(|| panic!("{text} parsed"));
        assert_eq!(refused.kind(), SqlErrorKind::Shape);
        assert!(
            refused.to_string().contains("write `FROM input`"),
            "{refused}"
        );
    }
}

#[test]
fn a_function_off_the_list_or_called_beyond_its_bound_is_refused() {
    refused_as(
        SqlErrorKind::Function,
        &[
            "SELECT count(*) AS n FROM input",
            "SELECT now() AS t FROM input",
            "SELECT random() AS r FROM input",
            "SELECT uuid() AS u FROM input",
            "SELECT repeat(name, 3) AS r FROM input",
            "SELECT lpad(name, n) AS r FROM input",
            "SELECT unnest(tags) AS t FROM input",
            "SELECT pg_catalog.lower(name) AS n FROM input",
            "SELECT lower(name) OVER () AS n FROM input",
            "SELECT lower(DISTINCT name) AS n FROM input",
            "SELECT lower(input => name) AS n FROM input",
            "SELECT replace(name, name, 'x') AS r FROM input",
            "SELECT replace(name, '', 'x') AS r FROM input",
            "SELECT regexp_replace(name, 'a', '$1') AS r FROM input",
        ],
    );
}

#[test]
fn a_call_missing_its_pattern_is_left_to_the_planner() {
    assert!(Sql::parse("SELECT regexp_like(name) AS m FROM input").is_ok());
}

#[test]
fn a_pattern_in_parentheses_and_flags_beside_g_are_read_as_written() {
    for text in [
        "SELECT regexp_like(name, ('a'), 'i') AS m FROM input",
        "SELECT regexp_replace(name, 'a', 'b', 'ig') AS m FROM input",
    ] {
        assert!(Sql::parse(text).is_ok(), "{text}");
    }
}

#[test]
fn a_literal_of_the_limit_is_read_and_one_byte_beyond_it_is_not() {
    let at = format!("SELECT '{}' AS x FROM input", "a".repeat(4096));
    assert!(Sql::parse(&at).is_ok());
}

#[test]
fn a_refusal_names_what_it_refuses() {
    let cases = [
        (
            "SELECT replace(name, name, 'x') AS r FROM input",
            "takes a literal there",
        ),
        (
            "SELECT id FROM (SELECT id FROM input)",
            "other than `FROM input`",
        ),
    ];
    for (text, what) in cases {
        let refused = Sql::parse(text).unwrap_err();
        assert!(refused.to_string().contains(what), "{text}: {refused}");
    }
}

#[test]
fn a_pattern_that_is_no_literal_or_compiles_too_large_is_refused() {
    refused_as(
        SqlErrorKind::Regex,
        &[
            "SELECT regexp_like(name, name) AS m FROM input",
            "SELECT id FROM input WHERE name ~ name",
            "SELECT id FROM input WHERE name LIKE name",
            "SELECT regexp_like(name, 'a{1000}{1000}') AS m FROM input",
            "SELECT regexp_like(name, '(') AS m FROM input",
            "SELECT regexp_like(name, 'a', 'q') AS m FROM input",
            "SELECT regexp_replace(name, 'a', 'b', 'gg') AS m FROM input",
            "SELECT regexp_like(name, 'a', 'g') AS m FROM input",
            "SELECT regexp_like(name, 'a', 'ig') AS m FROM input",
        ],
    );
}

#[test]
fn a_long_literal_or_an_unnamed_expression_is_refused() {
    let long = format!("SELECT '{}' AS x FROM input", "a".repeat(4097));
    refused_as(SqlErrorKind::Literal, &[&long]);
    refused_as(SqlErrorKind::Unnamed, &["SELECT lower(name) FROM input"]);
}

#[test]
fn a_refusal_never_quotes_a_literal_of_the_text() {
    let texts = [
        "SELECT id FROM input WHERE token = 'hunter2-secret' AND",
        "SELECT id FROM input WHERE token = 'hunter2-secret",
        "SELECT regexp_like(token, 'hunter2-secret(') AS m FROM input",
    ];
    for text in texts {
        let refused = Sql::parse(text).unwrap_err();
        assert!(!refused.to_string().contains("hunter2"), "{refused}");
    }
}

#[test]
fn a_refusal_shows_a_long_function_name_cut_short() {
    let name = "f".repeat(60 << 10);
    let refused = Sql::parse(&format!("SELECT {name}(n) AS x FROM input")).unwrap_err();
    assert_eq!(refused.kind(), SqlErrorKind::Function);
    assert!(
        refused.to_string().len() < 200,
        "{}",
        refused.to_string().len()
    );
    let shown = format!("function {}...", "f".repeat(64));
    assert!(refused.to_string().starts_with(&shown), "{refused}");
}

#[test]
fn a_query_shows_whether_it_filters_and_never_its_text() {
    let sql = Sql::parse("SELECT id FROM input WHERE token <> 'hunter2'").unwrap();
    assert_eq!(format!("{sql:?}"), "Sql { filters: true, .. }");
}

#[test]
fn a_schema_is_planned_once_however_many_ask_for_it_at_once_and_found_by_the_one_asked_last() {
    let sql = Sql::parse("SELECT n, n + 1 AS m FROM input").unwrap();
    let schema = || Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let asked: Vec<SchemaRef> = (0..8).map(|_| schema()).collect();
    let planned: Vec<_> = thread::scope(|scope| {
        let asking: Vec<_> = asked
            .iter()
            .map(|asked| scope.spawn(|| sql.planned(asked, Some(asked)).unwrap()))
            .collect();
        asking
            .into_iter()
            .map(|asking| asking.join().unwrap())
            .collect()
    });
    assert!(
        planned
            .windows(2)
            .all(|pair| Arc::ptr_eq(&pair[0], &pair[1]))
    );
    assert_eq!(sql.plans.lock().len(), 1);
    let again = schema();
    let found = sql.planned(&again, Some(&again)).unwrap();
    assert!(Arc::ptr_eq(&found, &planned[0]));
    assert!(Arc::ptr_eq(&sql.plans.lock()[0].input, &again));
}

#[test]
fn a_schema_asked_for_again_outlasts_those_asked_for_since_it_was_planned() {
    // Nine schemas, each with a column of its own.
    let schemas: Vec<SchemaRef> = (0..=PLANS)
        .map(|place| {
            let own = Field::new(format!("c{place}"), DataType::Int64, true);
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, true),
                own,
            ]))
        })
        .collect();
    // Found by the very schema a batch holds, and by an equal one, as a schema's output is.
    let lookups: [fn(&Sql, &SchemaRef) -> Arc<Planned>; 2] = [
        |sql, schema| sql.planned(schema, Some(schema)).unwrap(),
        |sql, schema| sql.planned(&Schema::clone(schema), None).unwrap(),
    ];
    for planned in lookups {
        let sql = Sql::parse("SELECT n FROM input").unwrap();
        let first: Vec<Arc<Planned>> = schemas[..PLANS]
            .iter()
            .map(|schema| planned(&sql, schema))
            .collect();
        assert!(Arc::ptr_eq(&planned(&sql, &schemas[0]), &first[0]));
        planned(&sql, &schemas[PLANS]);
        assert!(Arc::ptr_eq(&planned(&sql, &schemas[0]), &first[0]));
        assert!(!Arc::ptr_eq(&planned(&sql, &schemas[1]), &first[1]));
        assert_eq!(sql.plans.lock().len(), PLANS);
    }
}

#[test]
fn the_function_list_is_sorted_and_holds_no_volatile_or_unbounded_function() {
    let names: Vec<&str> = function_names().collect();
    assert!(names.windows(2).all(|pair| pair[0] < pair[1]), "{names:?}");
    let listed = [
        "abs",
        "btrim",
        "char_length",
        "character_length",
        "coalesce",
        "concat",
        "concat_ws",
        "date_part",
        "date_trunc",
        "ends_with",
        "initcap",
        "left",
        "length",
        "lower",
        "ltrim",
        "nullif",
        "octet_length",
        "regexp_like",
        "regexp_replace",
        "replace",
        "reverse",
        "right",
        "round",
        "rtrim",
        "starts_with",
        "strpos",
        "substr",
        "trunc",
        "upper",
    ];
    assert_eq!(names, listed);
    for refused in [
        "now",
        "random",
        "uuid",
        "repeat",
        "lpad",
        "rpad",
        "generate_series",
        "current_date",
    ] {
        assert!(!names.contains(&refused), "{refused}");
    }
}

#[test]
fn a_panic_parsing_the_text_refuses_it_naming_no_value() {
    let refused = Sql::parse(&format!("SELECT '{PANICS}' AS x FROM input")).unwrap_err();
    assert_eq!(
        (refused.kind(), refused.code()),
        (SqlErrorKind::Failed, codes::TRANSFORM_INVALID)
    );
    assert!(!refused.to_string().contains(PANICS), "{refused}");
}

#[test]
fn a_panic_planning_the_query_refuses_the_schema_naming_no_value() {
    let sql = Sql::parse("SELECT * FROM input").unwrap();
    let schema = Schema::new(vec![Field::new(PANICS, DataType::Int64, true)]);
    let refused = sql.output(&schema).unwrap_err();
    assert!(!refused.to_string().contains(PANICS), "{refused}");
    assert!(sql.bound(&schema).is_none());
    assert!(sql.lineage(&schema).is_none());
}
