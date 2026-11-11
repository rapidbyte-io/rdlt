use std::thread;

use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;

use super::{Nesting, nesting};
use crate::{SQL_BYTES, SQL_DEPTH, Sql, SqlErrorKind};

/// How `text`, a statement, nests.
fn nesting_of(text: &str) -> Nesting {
    let statements = Parser::parse_sql(&GenericDialect {}, text).unwrap();
    let [statement] = statements.as_slice() else {
        panic!("{text} is one statement");
    };
    nesting(statement)
}

fn measured(text: &str) -> usize {
    nesting_of(text).depth
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

/// Queries, each with the most conditions any `CASE` or `coalesce` of it lies within.
const CONDITIONED: [(&str, usize); 19] = [
    ("SELECT n FROM input", 0),
    ("SELECT CASE WHEN n > 0 THEN 1 END AS x FROM input", 0),
    (
        "SELECT CASE WHEN CASE WHEN n > 0 THEN true END THEN 1 END AS x FROM input",
        1,
    ),
    (
        "SELECT CASE WHEN NOT (CASE WHEN n > 0 THEN true END) THEN 1 END AS x FROM input",
        1,
    ),
    (
        "SELECT n FROM input WHERE CASE WHEN CASE WHEN n > 0 THEN true END THEN true END",
        1,
    ),
    (
        "SELECT CASE n WHEN CASE WHEN n > 0 THEN 1 END THEN 2 END AS x FROM input",
        1,
    ),
    // A result, an `ELSE` and an operand are no conditions.
    (
        "SELECT CASE WHEN n > 0 THEN CASE WHEN n > 1 THEN 1 END END AS x FROM input",
        0,
    ),
    (
        "SELECT CASE WHEN n > 0 THEN 1 ELSE CASE WHEN n > 1 THEN 2 END END AS x FROM input",
        0,
    ),
    (
        "SELECT CASE CASE WHEN n > 0 THEN 1 END WHEN 1 THEN 2 END AS x FROM input",
        0,
    ),
    // `coalesce` tests each argument but its last, as the `CASE` planned for it does.
    ("SELECT coalesce(n, coalesce(n, 1)) AS x FROM input", 0),
    ("SELECT coalesce(coalesce(n, 1), 2) AS x FROM input", 1),
    ("SELECT COALESCE(n, COALESCE(n, 1), 2) AS x FROM input", 1),
    ("SELECT lower(coalesce(lower(t), 'a')) AS x FROM input", 0),
    (
        "SELECT CASE WHEN coalesce(n, 1) > 0 THEN 1 END AS x FROM input",
        1,
    ),
    (
        "SELECT coalesce(CASE WHEN n > 0 THEN 1 END, 2) AS x FROM input",
        1,
    ),
    (
        "SELECT coalesce(coalesce(coalesce(n, 1), 2), 3) AS x FROM input",
        2,
    ),
    (
        "SELECT CASE WHEN lower(CASE WHEN CASE WHEN n > 0 THEN t END = 'a' THEN t END) = 'b' \
         THEN 1 END AS x FROM input",
        2,
    ),
    // Conditions side by side, and a `CASE` met after the walk leaves a condition.
    (
        "SELECT CASE WHEN CASE WHEN n > 0 THEN true END AND CASE WHEN n > 1 THEN true END \
         THEN 1 END AS x FROM input",
        1,
    ),
    (
        "SELECT CASE WHEN CASE WHEN n > 0 THEN true END THEN CASE WHEN CASE WHEN n > 1 THEN \
         true END THEN 1 END END AS x FROM input",
        1,
    ),
];

#[test]
fn a_case_lies_within_the_conditions_of_the_cases_and_coalesces_around_it() {
    for (text, expected) in CONDITIONED {
        assert_eq!(nesting_of(text).conditions, expected, "{text}");
    }
}

/// Lengths of text the deepest forms are built at: a short query's, a long one's, and the most a
/// query's text may take.
const LENGTHS: [usize; 3] = [1 << 10, 8 << 10, SQL_BYTES];

/// `head`, as many of `unit` as fit in `bytes` beside `tail`, and `tail`.
fn filled(bytes: usize, head: &str, unit: &str, tail: &str) -> String {
    let room = bytes - head.len() - tail.len();
    format!("{head}{}{tail}", unit.repeat(room / unit.len()))
}

/// `head`, `open` nested as many times as fits in `bytes` around `core`, each closed by `close`,
/// and `tail`.
fn nested(bytes: usize, head: &str, (open, core, close): (&str, &str, &str), tail: &str) -> String {
    let room = bytes - head.len() - core.len() - tail.len();
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
    for bytes in LENGTHS {
        deeper_refused(bytes);
    }
}

/// Each form of a tree deeper than the limit, at `bytes` of text, refused from a 2 MiB thread.
fn deeper_refused(bytes: usize) {
    let deeper = format!("nests deeper than {SQL_DEPTH}");
    let (deeper, broken) = (deeper.as_str(), "the text is not one query");
    let then = format!("{} + then", " + n".repeat(49));
    let filled = |head: &str, unit: &str, tail: &str| filled(bytes, head, unit, tail);
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
        assert_eq!(refused.kind(), kind, "{bytes}: {refused}");
        assert!(refused.to_string().contains(said), "{bytes}: {refused}");
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

/// Each type nested as deep as `bytes` of text allow: in a cast, as a typed string's type, and
/// each type of [`NESTING`] as a cast's.
fn types_nested(bytes: usize) -> Vec<String> {
    let cases = [
        filled(bytes, "SELECT CAST(n AS INT", "[]", ") AS x FROM input"),
        nested(
            bytes,
            "SELECT n FROM input WHERE ",
            ("ARRAY<", "INT", ">"),
            " '1' IS NULL",
        ),
    ];
    let cast = |opened| nested(bytes, "SELECT CAST(n AS ", opened, ") AS x FROM input");
    cases
        .into_iter()
        .chain(NESTING.into_iter().map(cast))
        .collect()
}

#[test]
fn a_type_nested_as_deep_as_the_text_allows_is_refused_on_a_small_stack() {
    for bytes in LENGTHS {
        for text in types_nested(bytes) {
            let shown: String = text.chars().take(40).collect();
            let refused = on_a_small_stack(move || Sql::parse(&text).map(|_| ()));
            let refused = refused.expect_err(&shown);
            assert_eq!(
                refused.kind(),
                SqlErrorKind::Shape,
                "{bytes} {shown}: {refused}"
            );
        }
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
