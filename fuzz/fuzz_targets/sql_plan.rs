//! Arbitrary text and drawn queries through `Sql::parse`, and the short among them through
//! planning, never panic or overflow their stack, and a plan rdlt-sql accepts holds only a
//! projection of input, a filter and the listed scalar functions.
//!
//! An input starting with a printable character is text, read as UTF-8 whatever its bytes; any
//! other draws a query: the shapes a SQL transform may not hold beside those it may, names that
//! are keywords, and nesting and chains past the depth limit.
//!
//! Every text is parsed, within two seconds however long the limit lets it be. Only a text of at
//! most [`PLANNED`] bytes is planned: planning what the limits admit is polynomial in its depth,
//! and slow under the fuzzer's instrumentation, so at this length a timeout is planning that grows
//! faster than the limits allow.

#![forbid(unsafe_code)]
#![no_main]

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use libfuzzer_sys::arbitrary::{Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use rdlt_sql::{SQL_DEPTH, Sql, function_names};

/// Bytes: the longest text the target plans, at which the slowest queries the limits admit that
/// were found plan within half the timeout.
const PLANNED: usize = 1 << 10;

/// The nodes an accepted plan may hold.
const NODES: [&str; 4] = ["Projection", "Filter", "SubqueryAlias", "TableScan"];

/// Columns of the input, some named as keywords.
const COLUMNS: [&str; 7] = ["t", "n", "f", "ts", "then", "when", "else"];

/// Functions the list leaves out: volatile, unbounded, aggregate and window ones, and none.
const UNLISTED: [&str; 8] = [
    "random",
    "now",
    "uuid",
    "repeat",
    "count",
    "sum",
    "row_number",
    "nothing",
];

/// What a query reads from: `input`, or what a SQL transform may not read.
const SOURCES: [&str; 6] = [
    "input",
    "input AS i",
    "other",
    "input JOIN input AS b ON input.n = b.n",
    "(SELECT * FROM input) AS s",
    "input, input AS b",
];

/// Clauses a SQL transform may not hold.
const CLAUSES: [&str; 6] = [
    " GROUP BY n",
    " ORDER BY n",
    " LIMIT 3",
    " UNION SELECT * FROM input",
    " HAVING n > 1",
    " OFFSET 1",
];

fuzz_target!(|data: &[u8]| {
    let text = match data.first() {
        Some(0x20..=0x7e) => String::from_utf8_lossy(data).into_owned(),
        _ => match query(&mut Unstructured::new(data.get(1..).unwrap_or_default())) {
            Ok(text) => text,
            Err(_) => return,
        },
    };
    let Ok(sql) = Sql::parse(&text) else {
        return;
    };
    if text.len() > PLANNED {
        return;
    }
    let mut fields: Vec<Field> = COLUMNS
        .iter()
        .map(|name| Field::new(*name, DataType::Utf8, true))
        .collect();
    fields[1] = Field::new("n", DataType::Int64, true);
    fields[2] = Field::new("f", DataType::Float64, true);
    let utc = Some("UTC".into());
    fields[3] = Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, utc), true);
    let Ok(facts) = rdlt_sql::bench::facts(&sql, &Schema::new(fields)) else {
        return;
    };
    let listed: Vec<&str> = function_names().collect();
    assert!(
        facts.nodes.iter().all(|node| NODES.contains(&node.as_str())),
        "{text}: {facts:?}"
    );
    assert!(
        facts
            .functions
            .iter()
            .all(|function| listed.contains(&function.as_str())),
        "{text}: {facts:?}"
    );
});

/// A drawn query: a projection of some source, filtered or not, with a clause or not.
fn query(drawn: &mut Unstructured<'_>) -> Result<String> {
    let deep = drawn.ratio(1, 8)?;
    let limit = if deep { 2 * SQL_DEPTH } else { 8 };
    let mut projected = Vec::new();
    for _ in 0..drawn.int_in_range(1..=3)? {
        projected.push(match drawn.int_in_range(0..=3)? {
            0 => "*".to_owned(),
            1 => (*drawn.choose(&COLUMNS)?).to_owned(),
            _ => {
                let alias = *drawn.choose(&["y", "_rdlt_y", "then", "\"x y\""])?;
                format!("{} AS {alias}", expression(drawn, 0, limit)?)
            }
        });
    }
    let distinct = if drawn.ratio(1, 8)? { "DISTINCT " } else { "" };
    let source = if drawn.ratio(3, 4)? {
        "input"
    } else {
        *drawn.choose(&SOURCES)?
    };
    let mut text = format!("SELECT {distinct}{} FROM {source}", projected.join(", "));
    if drawn.arbitrary()? {
        text = format!("{text} WHERE {}", expression(drawn, 0, limit)?);
    }
    if drawn.ratio(1, 8)? {
        text.push_str(drawn.choose(&CLAUSES)?);
    }
    Ok(text)
}

/// A drawn expression, nested at most `limit` deep.
fn expression(drawn: &mut Unstructured<'_>, depth: usize, limit: usize) -> Result<String> {
    if depth >= limit || drawn.ratio(1, 3)? {
        return leaf(drawn);
    }
    let below = depth + 1;
    Ok(match drawn.int_in_range(0..=9)? {
        0 => {
            let names: Vec<&str> = function_names().collect();
            let name = if drawn.ratio(1, 6)? {
                *drawn.choose(&UNLISTED)?
            } else {
                *drawn.choose(&names)?
            };
            let arity = drawn.int_in_range(0..=3)?;
            let arguments: Vec<String> = (0..arity)
                .map(|_| expression(drawn, below, limit))
                .collect::<Result<_>>()?;
            let over = if name == "row_number" { " OVER ()" } else { "" };
            format!("{name}({}){over}", arguments.join(", "))
        }
        1 => {
            let operators = [
                "+", "-", "*", "/", "%", "=", "<>", "<", "AND", "OR", "||", "LIKE", "~",
                "IS DISTINCT FROM",
            ];
            let operator = *drawn.choose(&operators)?;
            let (left, right) = (
                expression(drawn, below, limit)?,
                expression(drawn, below, limit)?,
            );
            format!("({left} {operator} {right})")
        }
        2 => format!(
            "CASE WHEN {} THEN {} ELSE {} END",
            expression(drawn, below, limit)?,
            expression(drawn, below, limit)?,
            expression(drawn, below, limit)?
        ),
        3 => {
            let targets = ["BIGINT", "DOUBLE", "VARCHAR", "BOOLEAN", "DATE", "TIMESTAMP"];
            let target = *drawn.choose(&targets)?;
            format!("CAST({} AS {target})", expression(drawn, below, limit)?)
        }
        4 => format!("{} IS NULL", expression(drawn, below, limit)?),
        5 => {
            let fields: Vec<String> = (0..drawn.int_in_range(1..=4)?)
                .map(|_| expression(drawn, below, limit))
                .collect::<Result<_>>()?;
            format!("STRUCT({})", fields.join(", "))
        }
        6 => {
            let subqueries = [
                "(SELECT n FROM input LIMIT 1)",
                "n IN (SELECT n FROM input)",
                "EXISTS (SELECT 1 FROM input)",
            ];
            (*drawn.choose(&subqueries)?).to_owned()
        }
        7 => chain(drawn)?,
        8 => format!("(NOT {})", expression(drawn, below, limit)?),
        _ => format!("({})", expression(drawn, below, limit)?),
    })
}

/// A column, a literal or `NULL`.
fn leaf(drawn: &mut Unstructured<'_>) -> Result<String> {
    Ok(match drawn.int_in_range(0..=4)? {
        0 => (*drawn.choose(&COLUMNS)?).to_owned(),
        1 => format!("\"{}\"", drawn.choose(&COLUMNS)?),
        2 => drawn.arbitrary::<i32>()?.to_string(),
        3 => format!("'{}'", drawn.arbitrary::<String>()?.replace('\'', "''")),
        _ => "NULL".to_owned(),
    })
}

/// A chain of one operator over many operands, as long as several depth limits.
fn chain(drawn: &mut Unstructured<'_>) -> Result<String> {
    let operator = *drawn.choose(&[" + ", " AND ", " || ", " = "])?;
    let length = drawn.int_in_range(2..=4 * SQL_DEPTH)?;
    let operand = (*drawn.choose(&COLUMNS)?).to_owned();
    Ok(vec![operand; length].join(operator))
}
