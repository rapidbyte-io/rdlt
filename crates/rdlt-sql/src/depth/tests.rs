use std::thread;

use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;

use super::depth;
use crate::{SQL_BYTES, SQL_DEPTH, Sql, SqlErrorKind};

fn measured(text: &str) -> usize {
    let statements = Parser::parse_sql(&GenericDialect {}, text).unwrap();
    statements.iter().map(depth).max().unwrap()
}

#[test]
fn an_expression_lies_as_deep_as_the_expressions_above_it() {
    let cases = [
        ("SELECT n FROM input", 0),
        ("SELECT lower(t) AS x FROM input", 1),
        ("SELECT a + b + c AS x FROM input", 2),
        ("SELECT (((n))) AS x FROM input", 3),
        ("SELECT n FROM input WHERE status IS NOT NULL", 1),
        (
            "SELECT CASE WHEN n > 0 THEN 1 ELSE 2 END AS x FROM input",
            2,
        ),
        (
            "SELECT a, b + c AS x, lower(upper(t)) AS y FROM input WHERE n > 0",
            2,
        ),
    ];
    for (text, expected) in cases {
        assert_eq!(measured(text), expected, "{text}");
    }
}

/// `head`, as many of `unit` as fit in [`SQL_BYTES`] beside `tail`, and `tail`.
fn filled(head: &str, unit: &str, tail: &str) -> String {
    let room = SQL_BYTES - head.len() - tail.len();
    format!("{head}{}{tail}", unit.repeat(room / unit.len()))
}

/// `head`, `open` nested as many times as fits in [`SQL_BYTES`] around `core`, each closed by
/// `close`, and `tail`.
fn nested(head: &str, (open, core, close): (&str, &str, &str), tail: &str) -> String {
    let room = SQL_BYTES - head.len() - core.len() - tail.len();
    let times = room / (open.len() + close.len());
    format!(
        "{head}{}{core}{}{tail}",
        open.repeat(times),
        close.repeat(times)
    )
}

/// `work` run on a thread with a 2 MiB stack, an async worker's.
fn on_a_small_stack<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    thread::Builder::new()
        .name("a small stack".to_owned())
        .stack_size(2 << 20)
        .spawn(work)
        .unwrap()
        .join()
        .unwrap()
}

#[test]
fn a_tree_deeper_than_the_limit_is_refused_on_a_small_stack_however_its_text_reads() {
    let deeper = format!("nests deeper than {SQL_DEPTH}");
    let (deeper, broken) = (deeper.as_str(), "the text is not one query");
    let then = format!("{} + then", " + n".repeat(49));
    let cases = [
        // `then` read as a name in a chain inside a `CASE`, every 50 operators.
        (
            filled("SELECT CASE WHEN n", &then, " THEN 1 END AS x FROM input"),
            SqlErrorKind::Syntax,
            deeper,
        ),
        // A struct literal's type, whose comma is no group's, refused where it opens.
        (
            filled(
                "SELECT n",
                " + STRUCT<a INT, b INT>(1, 2)",
                " AS x FROM input",
            ),
            SqlErrorKind::Shape,
            "a nested type",
        ),
        (
            filled("SELECT n", " + n", " AS x FROM input"),
            SqlErrorKind::Syntax,
            deeper,
        ),
        // A chain of queries, as deep as it is long, which holds no deep expression.
        (
            filled("SELECT n FROM input", " UNION SELECT n FROM input", ""),
            SqlErrorKind::Shape,
            "a set operation",
        ),
        // Broken after a deep chain, so the parser drops what it built itself.
        (
            filled("SELECT n", " + n", " AS x FROM input )"),
            SqlErrorKind::Syntax,
            broken,
        ),
        (
            filled("SELECT n", " + n", " + )"),
            SqlErrorKind::Syntax,
            broken,
        ),
    ];
    for (text, kind, said) in cases {
        let refused = on_a_small_stack(move || Sql::parse(&text).map(|_| ()));
        let refused = refused.unwrap_err();
        assert_eq!(refused.kind(), kind, "{refused}");
        assert!(refused.to_string().contains(said), "{refused}");
    }
}

/// Each type the dialect nests: as it opens, the type it holds innermost, and as it closes.
const NESTING: [(&str, &str, &str); 10] = [
    ("ARRAY<", "INT", ">"),
    ("STRUCT<", "INT", ">"),
    ("STRUCT<a ", "INT", ">"),
    ("MAP(INT, ", "INT", ")"),
    ("Tuple(", "INT", ")"),
    ("Nested(a ", "INT", ")"),
    ("Nullable(", "INT", ")"),
    ("LowCardinality(", "INT", ")"),
    ("UNION(a ", "INT", ")"),
    ("TABLE(a ", "INT", ")"),
];

#[test]
fn a_type_nested_as_deep_as_the_text_allows_is_refused_on_a_small_stack() {
    let cases = [
        filled("SELECT CAST(n AS INT", "[]", ") AS x FROM input"),
        nested(
            "SELECT n FROM input WHERE ",
            ("ARRAY<", "INT", ">"),
            " '1' IS NULL",
        ),
    ];
    let cast = |opened| nested("SELECT CAST(n AS ", opened, ") AS x FROM input");
    let cases = cases.into_iter().chain(NESTING.into_iter().map(cast));
    for text in cases {
        let shown: String = text.chars().take(40).collect();
        let refused = on_a_small_stack(move || Sql::parse(&text).map(|_| ()));
        let refused = refused.expect_err(&shown);
        assert_eq!(refused.kind(), SqlErrorKind::Shape, "{shown}: {refused}");
    }
}

#[test]
fn a_long_but_flat_select_list_parses_on_a_small_stack() {
    let mut text = "SELECT n".to_owned();
    for item in (0..).map(|column| format!(", n + 1 AS c{column}")) {
        if text.len() + item.len() > SQL_BYTES - 16 {
            break;
        }
        text += &item;
    }
    text.push_str(" FROM input");
    let parsed = on_a_small_stack(move || Sql::parse(&text).map(|_| ()));
    assert_eq!(parsed, Ok(()));
}

#[test]
fn a_nest_of_queries_the_parser_reads_whole_is_refused_for_its_shape() {
    // A hundred derived tables take the parser twice as deep, past what any admitted query takes
    // and short of the parser's limit, which refuses deeper text as too deep.
    let tables = "(SELECT n FROM ".repeat(100);
    let text = format!("SELECT n FROM {tables}input{}", ")".repeat(100));
    let refused = Sql::parse(&text).unwrap_err();
    assert_eq!(refused.kind(), SqlErrorKind::Shape, "{refused}");
}
