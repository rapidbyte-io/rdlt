//! The expressions every test runs, one at least for each listed function, and the batches
//! they run on.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, Float64Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, TimestampMicrosecondArray,
};
use proptest::collection::vec;
use proptest::option;
use proptest::prelude::*;

/// Expressions over `t` (text), `n` (an integer), `f` (a float) and `ts` (a time), each named
/// in a query.
pub(crate) const EXPRESSIONS: &[&str] = &[
    "abs(n)",
    "btrim(t)",
    "char_length(t)",
    "character_length(t)",
    "coalesce(t, 'none')",
    "concat(t, t, 'x')",
    "concat('', '')",
    "concat_ws('-', t, t)",
    "date_part('year', ts)",
    "date_trunc('day', ts)",
    "ends_with(t, 'a')",
    "initcap(t)",
    "left(t, 2)",
    "length(t)",
    "lower(t)",
    "ltrim(t)",
    "nullif(t, 'a')",
    "octet_length(t)",
    "regexp_like(t, '^a', 'i')",
    "regexp_replace(t, '[a-z]', 'xy', 'g')",
    "replace(t, 'a', 'bcd')",
    "reverse(t)",
    "right(t, 2)",
    "round(f, 2)",
    "rtrim(t)",
    "starts_with(t, 'a')",
    "strpos(t, 'a')",
    "substr(t, 2, 3)",
    "trunc(f)",
    "upper(t)",
    "CAST(n AS VARCHAR)",
    "t || t",
    "CASE WHEN n > 0 THEN t ELSE 'neg' END",
];

/// Predicates a query may filter by.
pub(crate) const PREDICATES: &[&str] = &[
    "n % 3 <> 0",
    "t LIKE 'a%'",
    "n BETWEEN -5 AND 5",
    "t IS NOT NULL AND n > 0",
    "t IN ('a', 'b')",
    "lower(t) <> 'x'",
];

/// The query projecting `n` and `expression`, filtered by `predicate` where there is one.
pub(crate) fn query(expression: &str, predicate: Option<&str>) -> String {
    let filter = predicate.map_or(String::new(), |predicate| format!(" WHERE {predicate}"));
    format!("SELECT n, {expression} AS y FROM input{filter}")
}

/// A row of `t`, `n`, `f` and `ts`.
type Row = (Option<String>, Option<i64>, Option<f64>, Option<i64>);

/// How a batch lays its text out.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Layout {
    Utf8,
    LargeUtf8,
    Utf8View,
}

fn layout() -> impl Strategy<Value = Layout> {
    prop_oneof![
        Just(Layout::Utf8),
        Just(Layout::LargeUtf8),
        Just(Layout::Utf8View)
    ]
}

/// Text, Unicode with characters case mappings grow among it.
fn text() -> impl Strategy<Value = String> {
    prop_oneof!["\\PC{0,24}", "[aAbBxß\u{390}\u{149}\u{130} ]{0,12}"]
}

/// Batches of `t`, `n`, `f` and `ts`, Unicode text and nulls among them, each laid out as drawn;
/// each starts with two rows that differ in every column, so moving any column's rows changes
/// it.
pub(crate) fn batches() -> impl Strategy<Value = Vec<RecordBatch>> {
    let row = (
        option::of(text()),
        option::of(-1_000_000_i64..1_000_000),
        option::of(-1e9_f64..1e9),
        option::of(0_i64..4_000_000_000_000_000),
    );
    let drawn = (vec(row, 1..62), layout());
    vec(drawn, 1..4).prop_map(|batches| {
        let batches = batches.into_iter();
        batches.map(|(rows, layout)| batch(rows, layout)).collect()
    })
}

/// Rows of each large batch, and bytes of text each holds at least.
const LARGE_ROWS: usize = 2048;
const LARGE_TEXT: usize = 512;

/// A batch of [`LARGE_ROWS`] rows, each holding [`LARGE_TEXT`] bytes of text at least, laid out
/// as drawn: a megabyte of text, far beyond what a node of text takes beside its bytes, so a
/// bound too low a byte shows once the batch is cut in eighths.
pub(crate) fn large() -> impl Strategy<Value = RecordBatch> {
    (text(), layout()).prop_map(|(unit, layout)| {
        let rows = (0..LARGE_ROWS).map(|row| {
            let value = format!("{unit}{row} ");
            let times = LARGE_TEXT / value.len() + 1;
            let n = i64::try_from(row).expect("few rows") - 1000;
            (row % 7 != 3).then(|| value.repeat(times)).map_or(
                (None, Some(n), None, Some(n)),
                |text| {
                    (
                        Some(text),
                        Some(n),
                        Some(1.5),
                        Some(n.abs() * 86_400_000_000),
                    )
                },
            )
        });
        batch(rows.collect(), layout)
    })
}

fn batch(drawn: Vec<Row>, layout: Layout) -> RecordBatch {
    let mut rows: Vec<Row> = vec![
        (Some("a\u{390}".to_owned()), Some(4), Some(1.25), Some(0)),
        (
            Some("B x".to_owned()),
            Some(-7),
            Some(-3.5),
            Some(90_061_000_000),
        ),
    ];
    rows.extend(drawn);
    let texts = rows.iter().map(|row| row.0.clone());
    let t: ArrayRef = match layout {
        Layout::Utf8 => Arc::new(texts.collect::<StringArray>()),
        Layout::LargeUtf8 => Arc::new(texts.collect::<LargeStringArray>()),
        Layout::Utf8View => Arc::new(texts.collect::<StringViewArray>()),
    };
    let n: Int64Array = rows.iter().map(|row| row.1).collect();
    let f: Float64Array = rows.iter().map(|row| row.2).collect();
    let ts: TimestampMicrosecondArray = rows.iter().map(|row| row.3).collect();
    RecordBatch::try_from_iter([
        ("t", t),
        ("n", Arc::new(n) as ArrayRef),
        ("f", Arc::new(f) as ArrayRef),
        ("ts", Arc::new(ts.with_timezone("UTC")) as ArrayRef),
    ])
    .expect("the columns are of one length")
}

#[test]
fn every_listed_function_is_called_by_an_expression() {
    for name in rdlt_sql::function_names() {
        let called = format!("{name}(");
        assert!(
            EXPRESSIONS
                .iter()
                .any(|expression| expression.starts_with(&called)),
            "{name}"
        );
    }
}
