//! Queries composed of the listed functions and the operators a SQL transform may hold, drawn
//! to a bounded depth, and batches of drawn layouts to run them on.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    StringViewArray,
};
use proptest::collection::vec;
use proptest::option;
use proptest::prelude::*;

use crate::queries::Layout;

/// How deep a drawn expression nests its calls and operators at most.
const DEPTH: u32 = 3;

/// A drawn query: a projection of `n` and two drawn expressions, and perhaps a drawn filter.
pub(crate) fn query() -> impl Strategy<Value = String> {
    let any = || prop_oneof![text(DEPTH), int(DEPTH), float(DEPTH), truth(DEPTH)];
    (any(), any(), option::of(truth(DEPTH))).prop_map(|(y, z, filter)| {
        let filter = filter.map_or(String::new(), |filter| format!(" WHERE {filter}"));
        format!("SELECT n, {y} AS y, {z} AS z FROM input{filter}")
    })
}

/// Text: the columns `t` and `u`, a literal, or a listed function or operator making text.
fn text(depth: u32) -> BoxedStrategy<String> {
    let leaf = prop_oneof![
        Just("t".to_owned()),
        Just("u".to_owned()),
        "[a-cß ]{0,3}".prop_map(|text| format!("'{text}'")),
    ];
    if depth == 0 {
        return leaf.boxed();
    }
    let (t, i, b) = (text(depth - 1), int(depth - 1), truth(depth - 1));
    let (t, i, b) = (|| t.clone(), || i.clone(), || b.clone());
    let cased = proptest::sample::select(vec![
        "lower", "upper", "initcap", "btrim", "ltrim", "rtrim", "reverse",
    ]);
    prop_oneof![
        leaf,
        (cased, t()).prop_map(|(name, a)| format!("{name}({a})")),
        (t(), 0_i64..4).prop_map(|(a, k)| format!("left({a}, {k})")),
        (t(), 0_i64..4).prop_map(|(a, k)| format!("right({a}, {k})")),
        (t(), 1_i64..3, 0_i64..4).prop_map(|(a, at, k)| format!("substr({a}, {at}, {k})")),
        (t(), t()).prop_map(|(a, b)| format!("concat({a}, {b})")),
        (t(), t(), t()).prop_map(|(a, b, c)| format!("concat({a}, {b}, {c})")),
        (t(), t()).prop_map(|(a, b)| format!("concat_ws('-', {a}, {b})")),
        (t(), t()).prop_map(|(a, b)| format!("coalesce({a}, {b})")),
        (t(), t()).prop_map(|(a, b)| format!("nullif({a}, {b})")),
        t().prop_map(|a| format!("replace({a}, 'a', 'xy')")),
        t().prop_map(|a| format!("regexp_replace({a}, '[a-b]', 'z', 'g')")),
        (t(), t()).prop_map(|(a, b)| format!("({a} || {b})")),
        (b(), t(), t()).prop_map(|(c, a, b)| format!("CASE WHEN {c} THEN {a} ELSE {b} END")),
        i().prop_map(|a| format!("CAST({a} AS VARCHAR)")),
    ]
    .boxed()
}

/// A whole number: the column `n`, a literal, or a listed function or operator making one.
fn int(depth: u32) -> BoxedStrategy<String> {
    let leaf = prop_oneof![
        Just("n".to_owned()),
        (-5_i64..5).prop_map(|k| k.to_string())
    ];
    if depth == 0 {
        return leaf.boxed();
    }
    let (t, i, b) = (text(depth - 1), int(depth - 1), truth(depth - 1));
    let (t, i, b) = (|| t.clone(), || i.clone(), || b.clone());
    let measured = proptest::sample::select(vec!["char_length", "length", "octet_length"]);
    prop_oneof![
        leaf,
        i().prop_map(|a| format!("abs({a})")),
        (i(), i()).prop_map(|(a, b)| format!("({a} + {b})")),
        (i(), i()).prop_map(|(a, b)| format!("({a} - {b})")),
        (i(), -3_i64..3).prop_map(|(a, k)| format!("({a} * {k})")),
        (measured, t()).prop_map(|(name, a)| format!("{name}({a})")),
        t().prop_map(|a| format!("strpos({a}, 'a')")),
        (i(), i()).prop_map(|(a, b)| format!("coalesce({a}, {b})")),
        (b(), i(), i()).prop_map(|(c, a, b)| format!("CASE WHEN {c} THEN {a} ELSE {b} END")),
    ]
    .boxed()
}

/// A floating-point number: the column `f`, or a listed function or operator making one.
fn float(depth: u32) -> BoxedStrategy<String> {
    let leaf = prop_oneof![Just("f".to_owned()), Just("1.5".to_owned())];
    if depth == 0 {
        return leaf.boxed();
    }
    let x = float(depth - 1);
    let x = || x.clone();
    prop_oneof![
        leaf,
        x().prop_map(|a| format!("round({a}, 2)")),
        x().prop_map(|a| format!("trunc({a})")),
        x().prop_map(|a| format!("abs({a})")),
        (x(), x()).prop_map(|(a, b)| format!("({a} + {b})")),
    ]
    .boxed()
}

/// A truth: the column `b`, or a comparison, a pattern, a listed function or a logical operator.
fn truth(depth: u32) -> BoxedStrategy<String> {
    let leaf = prop_oneof![Just("b".to_owned()), Just("TRUE".to_owned())];
    if depth == 0 {
        return leaf.boxed();
    }
    let (t, i, b) = (text(depth - 1), int(depth - 1), truth(depth - 1));
    let (t, i, b) = (|| t.clone(), || i.clone(), || b.clone());
    let compared = proptest::sample::select(vec!["=", "<>", "<", "<=", ">", ">="]);
    prop_oneof![
        leaf,
        (i(), compared.clone(), i()).prop_map(|(a, op, b)| format!("({a} {op} {b})")),
        (t(), compared, t()).prop_map(|(a, op, b)| format!("({a} {op} {b})")),
        t().prop_map(|a| format!("({a} LIKE 'a%')")),
        t().prop_map(|a| format!("({a} IN ('a', 'b', ''))")),
        i().prop_map(|a| format!("({a} BETWEEN -2 AND 2)")),
        b().prop_map(|a| format!("(NOT {a})")),
        (b(), b()).prop_map(|(a, c)| format!("({a} AND {c})")),
        (b(), b()).prop_map(|(a, c)| format!("({a} OR {c})")),
        t().prop_map(|a| format!("({a} IS NULL)")),
        (t(), t()).prop_map(|(a, c)| format!("({a} IS DISTINCT FROM {c})")),
        t().prop_map(|a| format!("starts_with({a}, 'a')")),
        t().prop_map(|a| format!("ends_with({a}, 'b')")),
        t().prop_map(|a| format!("regexp_like({a}, '^a', 'i')")),
        t().prop_map(|a| format!("({a} ~ '^b')")),
    ]
    .boxed()
}

/// A row of `t`, `u`, `n`, `f` and `b`.
type Row = (
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<f64>,
    Option<bool>,
);

/// A batch of `t` and `u`, each text in a drawn layout, `n`, `f` and `b`, nulls among them.
pub(crate) fn batch() -> impl Strategy<Value = RecordBatch> {
    let words = || option::of("[aAbBcß ]{0,6}");
    let row = (
        words(),
        words(),
        option::of(-1_000_i64..1_000),
        option::of(-1e3_f64..1e3),
        option::of(any::<bool>()),
    );
    (vec(row, 1..24), layout(), layout())
        .prop_map(|(rows, first, second)| built(&rows, first, second))
}

fn layout() -> impl Strategy<Value = Layout> {
    prop_oneof![
        Just(Layout::Utf8),
        Just(Layout::LargeUtf8),
        Just(Layout::Utf8View)
    ]
}

fn laid(texts: impl Iterator<Item = Option<String>>, layout: Layout) -> ArrayRef {
    match layout {
        Layout::Utf8 => Arc::new(texts.collect::<StringArray>()),
        Layout::LargeUtf8 => Arc::new(texts.collect::<LargeStringArray>()),
        Layout::Utf8View => Arc::new(texts.collect::<StringViewArray>()),
    }
}

fn built(rows: &[Row], first: Layout, second: Layout) -> RecordBatch {
    let numbers: Int64Array = rows.iter().map(|row| row.2).collect();
    let floats: Float64Array = rows.iter().map(|row| row.3).collect();
    let truths: BooleanArray = rows.iter().map(|row| row.4).collect();
    RecordBatch::try_from_iter([
        ("t", laid(rows.iter().map(|row| row.0.clone()), first)),
        ("u", laid(rows.iter().map(|row| row.1.clone()), second)),
        ("n", Arc::new(numbers) as ArrayRef),
        ("f", Arc::new(floats) as ArrayRef),
        ("b", Arc::new(truths) as ArrayRef),
    ])
    .expect("the columns are of one length")
}
