//! Rows a files table holds fit a wider type exactly, or the widen is refused.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampNanosecondType, TimestampSecondType};
use arrow_array::{
    ArrayRef, Int32Array, Int64Array, RecordBatch, TimestampNanosecondArray, TimestampSecondArray,
};
use rdlt_connector::{
    ConnectorError, ConnectorErrorKind, Field, LogicalType, OpenedSession, SegmentId, TableChange,
    TableSchema, TimeUnit,
};
use rdlt_connector_reference::files;

use crate::fixtures::{connect, merge_table, meta, open, tempdir};

async fn staged(session: &mut OpenedSession, segment: u64, id: i64, held: ArrayRef) {
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![id])) as ArrayRef),
        ("held", held),
        ("seq", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    ])
    .expect("a valid batch");
    let table = merge_table("events");
    let mut writer = session.session.writer(&table).await.expect("a writer");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

/// Commits one row holding `first` as `from`, widens the column to `to`, and commits another
/// row, `fitted` where the widen was taken and `kept` where it was refused; the widen's outcome
/// and the root the table is under.
async fn widened(
    format: &str,
    (from, to): (LogicalType, LogicalType),
    first: ArrayRef,
    (fitted, kept): (ArrayRef, ArrayRef),
) -> (Result<(), ConnectorError>, tempfile::TempDir) {
    let root = tempdir().expect("a root");
    let destination = connect(root.path(), format).await;
    let mut session = open(destination.as_ref()).await;
    let create = TableChange::Create {
        table: merge_table("events"),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("held", from.clone(), true),
            Field::new("seq", LogicalType::Int64, false),
        ])
        .expect("the schema is valid"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    staged(&mut session, 1, 1, first).await;
    session
        .session
        .commit(&meta(&session, 1, &[1]))
        .await
        .expect("the first row commits");
    let widen = TableChange::Widen {
        table: merge_table("events"),
        column: "held".into(),
        from,
        to,
    };
    let widening = session.session.apply_schema(&widen).await;
    let second = if widening.is_ok() { fitted } else { kept };
    staged(&mut session, 2, 2, second).await;
    session
        .session
        .commit(&meta(&session, 2, &[2]))
        .await
        .expect("the table merges at the type it has");
    (widening, root)
}

#[tokio::test]
async fn a_widen_a_published_value_does_not_fit_is_refused_and_the_table_merges_as_it_was() {
    let seconds = LogicalType::Timestamp(TimeUnit::Second, None);
    let nanos = LogicalType::Timestamp(TimeUnit::Nanosecond, None);
    // The last day of the year 9999, which nanoseconds do not reach.
    let first: ArrayRef = Arc::new(TimestampSecondArray::from(vec![253_402_214_400]));
    let fitted: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![5]));
    let kept: ArrayRef = Arc::new(TimestampSecondArray::from(vec![5]));
    let (widening, root) = widened("arrow", (seconds, nanos), first, (fitted, kept)).await;
    let error = widening.expect_err("the published value does not fit");
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("schema_conflict"));
    let mut values: Vec<Option<i64>> = files::published(root.path(), "events")
        .expect("the table reads")
        .iter()
        .flat_map(|batch| {
            let held = batch.column_by_name("held").expect("the column");
            let held = held.as_primitive::<TimestampSecondType>();
            held.iter().collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, [Some(5), Some(253_402_214_400)]);
}

#[tokio::test]
async fn a_published_value_a_wider_type_holds_is_kept_exactly() {
    let seconds = LogicalType::Timestamp(TimeUnit::Second, None);
    let nanos = LogicalType::Timestamp(TimeUnit::Nanosecond, None);
    let first: ArrayRef = Arc::new(TimestampSecondArray::from(vec![9_223_372_036]));
    let fitted: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![5]));
    let kept: ArrayRef = Arc::new(TimestampSecondArray::from(vec![5]));
    let (widening, root) = widened("arrow", (seconds, nanos), first, (fitted, kept)).await;
    widening.expect("the published value fits");
    let mut values: Vec<Option<i64>> = files::published(root.path(), "events")
        .expect("the table reads")
        .iter()
        .flat_map(|batch| {
            let held = batch.column_by_name("held").expect("the column");
            let held = held.as_primitive::<TimestampNanosecondType>();
            held.iter().collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, [Some(5), Some(9_223_372_036_000_000_000)]);
    // Lines hold a number as text, which a wider integer reads as it was.
    for format in ["jsonl", "arrow"] {
        let first: ArrayRef = Arc::new(Int32Array::from(vec![i32::MAX]));
        let fitted: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));
        let kept: ArrayRef = Arc::new(Int32Array::from(vec![5]));
        let types = (LogicalType::Int32, LogicalType::Int64);
        let (widening, root) = widened(format, types, first, (fitted, kept)).await;
        widening.expect("an integer fits a wider one");
        let mut values: Vec<i64> = files::published(root.path(), "events")
            .expect("the table reads")
            .iter()
            .flat_map(|batch| {
                let held = batch.column_by_name("held").expect("the column");
                held.as_primitive::<Int64Type>().values().to_vec()
            })
            .collect();
        values.sort_unstable();
        assert_eq!(values, [i64::from(i32::MAX), i64::MAX], "{format}");
    }
}

/// Rows of `ids` holding `held` as `kind`.
fn holding(ids: Vec<i64>, held: ArrayRef, kind: LogicalType) -> RecordBatch {
    let seqs = Int64Array::from(ids.iter().map(|_| 1).collect::<Vec<i64>>());
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            Field::new("held", kind, true).to_arrow(),
            arrow_schema::Field::new("seq", arrow_schema::DataType::Int64, false),
        ])),
        vec![Arc::new(Int64Array::from(ids)), held, Arc::new(seqs)],
    )
    .expect("a valid batch")
}

/// Stages `batch` in `segment` and commits it as commit `seq`.
async fn committed(session: &mut OpenedSession, segment: u64, seq: u64, batch: RecordBatch) {
    let table = merge_table("events");
    let mut writer = session.session.writer(&table).await.expect("a writer");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
    session
        .session
        .commit(&meta(session, seq, &[segment]))
        .await
        .expect("the rows commit");
}

/// The text of the column `held` of the table under `root`, by id.
fn held_text(root: &std::path::Path) -> Vec<(i64, Option<String>)> {
    use arrow_array::Array as _;
    let mut held: Vec<(i64, Option<String>)> = files::published(root, "events")
        .expect("the table reads")
        .iter()
        .flat_map(|batch| {
            let ids = batch.column_by_name("id").expect("the ids");
            let ids = ids.as_primitive::<Int64Type>();
            let values = batch.column_by_name("held").expect("the column");
            let values = arrow_cast::cast(values, &arrow_schema::DataType::Utf8).expect("text");
            let values = values.as_string::<i32>();
            (0..batch.num_rows())
                .map(|row| {
                    let value = values.is_valid(row).then(|| values.value(row).to_owned());
                    (ids.value(row), value)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    held.sort_unstable();
    held
}

/// Commits rows `1..` holding `first` as `from` in `format`, widens their column to `Json`, and
/// commits a row holding the JSON `{"a":1}`; the widen's outcome and the column's text by id.
async fn to_json(
    format: &str,
    from: LogicalType,
    first: ArrayRef,
) -> (Result<(), ConnectorError>, Vec<(i64, Option<String>)>) {
    let root = tempdir().expect("a root");
    let destination = connect(root.path(), format).await;
    let mut session = open(destination.as_ref()).await;
    let create = TableChange::Create {
        table: merge_table("events"),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("held", from.clone(), true),
            Field::new("seq", LogicalType::Int64, false),
        ])
        .expect("the schema is valid"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let ids: Vec<i64> = (1..=i64::try_from(first.len()).expect("few rows")).collect();
    let later = i64::try_from(ids.len()).expect("few rows") + 1;
    let rows = holding(ids, first, from.clone());
    committed(&mut session, 1, 1, rows).await;
    let widen = TableChange::Widen {
        table: merge_table("events"),
        column: "held".into(),
        from,
        to: LogicalType::Json,
    };
    let widening = session.session.apply_schema(&widen).await;
    if widening.is_ok() {
        let text: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![r#"{"a":1}"#]));
        let rows = holding(vec![later], text, LogicalType::Json);
        committed(&mut session, 2, 2, rows).await;
    }
    (widening, held_text(root.path()))
}

#[tokio::test]
async fn text_a_column_held_becomes_json_strings_when_it_widens_to_json() {
    let first = || -> ArrayRef {
        Arc::new(arrow_array::StringArray::from(vec![
            Some("false"),
            Some("4450"),
            Some("not json \"quoted\" \\ \u{1}\u{7f} / \u{2028} é"),
            None,
        ]))
    };
    let (widening, held) = to_json("arrow", LogicalType::Utf8, first()).await;
    widening.expect("text widens to JSON");
    let expected = [
        (1, Some(r#""false""#.to_owned())),
        (2, Some(r#""4450""#.to_owned())),
        (
            3,
            Some("\"not json \\\"quoted\\\" \\\\ \\u0001\u{7f} / \u{2028} é\"".to_owned()),
        ),
        (4, None),
        (5, Some(r#"{"a":1}"#.to_owned())),
    ];
    assert_eq!(held, expected);
    // A line holds no type, so its text would be read as JSON: the widen is refused.
    let (widening, held) = to_json("jsonl", LogicalType::Utf8, first()).await;
    let refused = widening.expect_err("lines hold no type");
    assert_eq!(refused.code(), Some("schema_conflict"));
    let texts: Vec<Option<&str>> = held.iter().map(|(_, text)| text.as_deref()).collect();
    let written = Some("not json \"quoted\" \\ \u{1}\u{7f} / \u{2028} é");
    assert_eq!(texts, [Some("false"), Some("4450"), written, None]);
}

#[tokio::test]
async fn booleans_numbers_and_structs_a_column_held_become_their_json_when_it_widens_to_json() {
    use arrow_array::{BooleanArray, StructArray};
    let text = |held: &[(i64, Option<String>)]| -> Vec<Option<String>> {
        held.iter().map(|(_, text)| text.clone()).collect()
    };
    let json = |values: &[&str]| -> Vec<Option<String>> {
        values
            .iter()
            .map(|value| Some((*value).to_owned()))
            .collect()
    };
    let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
    let (widening, held) = to_json("arrow", LogicalType::Bool, booleans).await;
    widening.expect("booleans widen to JSON");
    assert_eq!(text(&held), json(&["true", "false", r#"{"a":1}"#]));
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![42, i64::MIN]));
    let (widening, held) = to_json("arrow", LogicalType::Int64, numbers).await;
    widening.expect("integers widen to JSON");
    let expected = json(&["42", "-9223372036854775808", r#"{"a":1}"#]);
    assert_eq!(text(&held), expected);
    let fields = vec![
        Field::new("x", LogicalType::Bool, true),
        Field::new("n", LogicalType::Int64, true),
    ];
    let logical = LogicalType::Struct(fields.try_into().expect("distinct fields"));
    let arrow_schema::DataType::Struct(arrow_fields) = logical.to_arrow() else {
        unreachable!("a struct type is an Arrow struct")
    };
    let structs: ArrayRef = Arc::new(StructArray::new(
        arrow_fields,
        vec![
            Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef,
        ],
        None,
    ));
    let (widening, held) = to_json("arrow", logical, Arc::clone(&structs)).await;
    widening.expect("structs widen to JSON");
    assert_eq!(text(&held), json(&[r#"{"x":true,"n":null}"#, r#"{"a":1}"#]));
    let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    let (widening, held) = to_json("jsonl", LogicalType::Bool, booleans).await;
    let refused = widening.expect_err("lines hold no type");
    assert_eq!(refused.code(), Some("schema_conflict"));
    assert_eq!(text(&held), json(&["true"]));
}

#[tokio::test]
async fn a_table_of_json_lines_takes_a_widen_to_json_until_it_holds_rows_staged_or_published() {
    let root = tempdir().expect("a root");
    let destination = connect(root.path(), "jsonl").await;
    let mut session = open(destination.as_ref()).await;
    let create = TableChange::Create {
        table: merge_table("events"),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("held", LogicalType::Utf8, true),
            Field::new("other", LogicalType::Utf8, true),
            Field::new("seq", LogicalType::Int64, false),
        ])
        .expect("the schema is valid"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let widen = |column: &str| TableChange::Widen {
        table: merge_table("events"),
        column: column.into(),
        from: LogicalType::Utf8,
        to: LogicalType::Json,
    };
    session
        .session
        .apply_schema(&widen("held"))
        .await
        .expect("a table of no row takes the widen");
    let text: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["x"]));
    let mut writer = session
        .session
        .writer(&merge_table("events"))
        .await
        .expect("a writer");
    let rows = holding(vec![1], text, LogicalType::Json);
    writer
        .write(SegmentId(1), rows)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
    let refused = session.session.apply_schema(&widen("other")).await;
    let refused = refused.expect_err("staged lines hold no type");
    assert_eq!(refused.code(), Some("schema_conflict"));
}
