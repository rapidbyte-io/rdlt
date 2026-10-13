//! Rows a memory table holds are converted to a wider type exactly, or the widen is refused.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampNanosecondType, TimestampSecondType};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, RecordBatch, TimestampNanosecondArray,
    TimestampSecondArray,
};
use rdlt_connector::{
    ConnectorErrorKind, Field, LogicalType, MergeKey, OpenedSession, SegmentId, TableChange,
    TableRef, TableSchema, TimeUnit,
};
use rdlt_connector_reference::published;

use super::owned::{meta, open, store};

fn seq(value: u8) -> Vec<u8> {
    let mut seq = vec![0; 16];
    seq[15] = value;
    seq
}

fn events() -> TableRef {
    TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
        ..super::owned::table("events", "events", None)
    }
}

async fn staged(session: &mut OpenedSession, segment: u64, id: i64, until: ArrayRef) {
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![id])) as ArrayRef),
        ("until", until),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values([seq(1)])) as _,
        ),
    ])
    .expect("a valid batch");
    let mut writer = session.session.writer(&events()).await.expect("a writer");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
}

/// Commits one row whose `until` is `stored` seconds, widens `until` to nanoseconds, and commits
/// another row at the unit the column then has; returns the widen's outcome.
async fn widened(store_name: &str, stored: i64) -> Result<(), rdlt_connector::ConnectorError> {
    let destination = store(store_name).await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    let seconds = LogicalType::Timestamp(TimeUnit::Second, None);
    let nanos = LogicalType::Timestamp(TimeUnit::Nanosecond, None);
    let create = TableChange::Create {
        table: events(),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("until", seconds.clone(), true),
            Field::new("seq", LogicalType::Binary, false),
        ])
        .expect("the schema is valid"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let first: ArrayRef = Arc::new(TimestampSecondArray::from(vec![stored]));
    staged(&mut session, 1, 1, first).await;
    session
        .session
        .commit(&meta(&session, 1, 1, &[1]))
        .await
        .expect("the first row commits");
    let widen = TableChange::Widen {
        table: events(),
        column: "until".into(),
        from: seconds,
        to: nanos,
    };
    let widening = session.session.apply_schema(&widen).await;
    let second: ArrayRef = match &widening {
        Ok(()) => Arc::new(TimestampNanosecondArray::from(vec![5])),
        Err(_) => Arc::new(TimestampSecondArray::from(vec![5])),
    };
    staged(&mut session, 2, 2, second).await;
    session
        .session
        .commit(&meta(&session, 1, 2, &[2]))
        .await
        .expect("the table merges at the type it has");
    widening
}

#[tokio::test]
async fn a_widen_a_stored_value_does_not_fit_is_refused_and_the_table_merges_as_it_was() {
    // The last day of the year 9999, which nanoseconds do not reach.
    let error = widened("widened-beyond", 253_402_214_400)
        .await
        .expect_err("the stored value does not fit");
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("schema_conflict"));
    let mut values: Vec<Option<i64>> = published("widened-beyond", "events")
        .iter()
        .flat_map(|batch| {
            let until = batch.column_by_name("until").expect("the column");
            until
                .as_primitive::<TimestampSecondType>()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, [Some(5), Some(253_402_214_400)]);
}

#[tokio::test]
async fn a_stored_value_a_finer_unit_holds_is_kept_exactly() {
    widened("widened-within", 9_223_372_036)
        .await
        .expect("the stored value fits");
    let mut values: Vec<Option<i64>> = published("widened-within", "events")
        .iter()
        .flat_map(|batch| {
            let until = batch.column_by_name("until").expect("the column");
            assert_eq!(until.null_count(), 0);
            until
                .as_primitive::<TimestampNanosecondType>()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, [Some(5), Some(9_223_372_036_000_000_000)]);
}

/// A change stream's table keyed by `id`, whose deletes remove their rows.
fn changes() -> TableRef {
    let mut table = events();
    table.merge.as_mut().expect("a key").changes = Some(rdlt_connector::ChangeColumns {
        op: "op".into(),
        unchanged: None,
        deletion: rdlt_connector::Deletion::Hard,
    });
    table
}

/// One sequence `seq` as a column of `kind`.
fn sequence(kind: &LogicalType, seq: i64) -> ArrayRef {
    match kind {
        LogicalType::Int64 => Arc::new(Int64Array::from(vec![seq])),
        LogicalType::Utf8 => Arc::new(arrow_array::StringArray::from(vec![format!("{seq:04}")])),
        _ => Arc::new(BinaryArray::from_iter_values([seq.to_be_bytes()])),
    }
}

#[tokio::test]
async fn a_table_that_holds_tombstones_takes_a_widen_whatever_its_sequence_is() {
    use arrow_array::{Int8Array, Int32Array};
    use rdlt_connector::ChangeOp;
    let kinds = [LogicalType::Int64, LogicalType::Utf8, LogicalType::Binary];
    for (case, kind) in kinds.into_iter().enumerate() {
        let name = format!("widened-over-a-tombstone-{case}");
        let destination = store(&name).await;
        let mut session = open(destination.as_ref(), "p", 1).await;
        let create = TableChange::Create {
            table: changes(),
            schema: TableSchema::new(vec![
                Field::new("id", LogicalType::Int32, true),
                Field::new("seq", kind.clone(), false),
                Field::new("v", LogicalType::Int32, true),
            ])
            .expect("the schema is valid"),
        };
        session
            .session
            .apply_schema(&create)
            .await
            .expect("created");
        let mut segment = 0;
        for (id, seq, op) in [(1, 1, ChangeOp::Insert), (1, 5, ChangeOp::Delete)] {
            segment += 1;
            let batch = RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                ("seq", sequence(&kind, seq)),
                ("v", Arc::new(Int32Array::from(vec![7])) as ArrayRef),
                ("op", Arc::new(Int8Array::from(vec![op.code()])) as ArrayRef),
            ])
            .expect("a valid batch");
            let mut writer = session.session.writer(&changes()).await.expect("a writer");
            writer
                .write(SegmentId(segment), batch)
                .await
                .expect("buffers");
            writer.flush().await.expect("the flush stages");
            let commit = meta(&session, 1, segment, &[segment]);
            session.session.commit(&commit).await.expect("the commit");
        }
        for column in ["v", "id"] {
            let widen = TableChange::Widen {
                table: changes(),
                column: column.into(),
                from: LogicalType::Int32,
                to: LogicalType::Int64,
            };
            let widening = session.session.apply_schema(&widen).await;
            widening.unwrap_or_else(|error| panic!("{kind:?}: widening {column}: {error}"));
        }
    }
}

#[tokio::test]
async fn a_widen_a_staged_value_does_not_fit_is_refused() {
    let destination = store("widened-staged").await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    let seconds = LogicalType::Timestamp(TimeUnit::Second, None);
    let create = TableChange::Create {
        table: events(),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("until", seconds.clone(), true),
            Field::new("seq", LogicalType::Binary, false),
        ])
        .expect("the schema is valid"),
    };
    session
        .session
        .apply_schema(&create)
        .await
        .expect("created");
    let beyond: ArrayRef = Arc::new(TimestampSecondArray::from(vec![253_402_214_400]));
    staged(&mut session, 1, 1, beyond).await;
    let widen = TableChange::Widen {
        table: events(),
        column: "until".into(),
        from: seconds,
        to: LogicalType::Timestamp(TimeUnit::Nanosecond, None),
    };
    let refused = session.session.apply_schema(&widen).await;
    let refused = refused.expect_err("the staged value does not fit");
    assert_eq!(refused.code(), Some("schema_conflict"));
    let commit = meta(&session, 1, 1, &[1]);
    session
        .session
        .commit(&commit)
        .await
        .expect("the row commits");
}

/// Rows of `ids` holding `held` as `kind`.
fn holding(ids: Vec<i64>, held: ArrayRef, kind: LogicalType) -> RecordBatch {
    let seqs = BinaryArray::from_iter_values(ids.iter().map(|_| seq(1)));
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            Field::new("held", kind, true).to_arrow(),
            arrow_schema::Field::new("seq", arrow_schema::DataType::Binary, false),
        ])),
        vec![Arc::new(Int64Array::from(ids)), held, Arc::new(seqs)],
    )
    .expect("a valid batch")
}

/// Stages `batch` in `segment` and commits it as the commit of that number.
async fn committed(session: &mut OpenedSession, segment: u64, batch: RecordBatch) {
    let mut writer = session.session.writer(&events()).await.expect("a writer");
    writer
        .write(SegmentId(segment), batch)
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
    session
        .session
        .commit(&meta(session, 1, segment, &[segment]))
        .await
        .expect("the rows commit");
}

/// The text of the column `held` of `store_name`'s table, by id.
fn held_text(store_name: &str) -> Vec<(i64, Option<String>)> {
    let mut held: Vec<(i64, Option<String>)> = published(store_name, "events")
        .iter()
        .flat_map(|batch| {
            let ids = batch.column_by_name("id").expect("the ids");
            let ids = ids.as_primitive::<arrow_array::types::Int64Type>();
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

/// Commits rows `1..` holding `first` as `from`, widens their column to `Json`, and commits a
/// row holding the JSON `{"a":1}`; the widen's outcome and the column's text by id.
async fn to_json(
    store_name: &str,
    from: LogicalType,
    first: ArrayRef,
) -> (
    Result<(), rdlt_connector::ConnectorError>,
    Vec<(i64, Option<String>)>,
) {
    let destination = store(store_name).await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    let create = TableChange::Create {
        table: events(),
        schema: TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("held", from.clone(), true),
            Field::new("seq", LogicalType::Binary, false),
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
    committed(&mut session, 1, holding(ids, first, from.clone())).await;
    let widen = TableChange::Widen {
        table: events(),
        column: "held".into(),
        from,
        to: LogicalType::Json,
    };
    let widening = session.session.apply_schema(&widen).await;
    if widening.is_ok() {
        let text: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![r#"{"a":1}"#]));
        committed(
            &mut session,
            2,
            holding(vec![later], text, LogicalType::Json),
        )
        .await;
    }
    (widening, held_text(store_name))
}

#[tokio::test]
async fn text_a_column_held_becomes_json_strings_when_it_widens_to_json() {
    let first: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![
        Some("false"),
        Some("4450"),
        Some("not json \"quoted\" \\ \u{1}\u{7f} / \u{2028} é"),
        None,
    ]));
    let (widening, held) = to_json("widened-text-json", LogicalType::Utf8, first).await;
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
}

#[tokio::test]
async fn booleans_numbers_and_structs_a_column_held_become_their_json_when_it_widens_to_json() {
    use arrow_array::{BooleanArray, StructArray};
    let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
    let (widening, held) = to_json("widened-bool-json", LogicalType::Bool, booleans).await;
    widening.expect("booleans widen to JSON");
    let text = |held: &[(i64, Option<String>)]| -> Vec<Option<String>> {
        held.iter().map(|(_, text)| text.clone()).collect()
    };
    let json = |values: &[&str]| -> Vec<Option<String>> {
        values
            .iter()
            .map(|value| Some((*value).to_owned()))
            .collect()
    };
    assert_eq!(text(&held), json(&["true", "false", r#"{"a":1}"#]));
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![42, i64::MIN]));
    let (widening, held) = to_json("widened-int-json", LogicalType::Int64, numbers).await;
    widening.expect("integers widen to JSON");
    assert_eq!(
        text(&held),
        json(&["42", "-9223372036854775808", r#"{"a":1}"#])
    );
    let fields = vec![
        Field::new("x", LogicalType::Bool, true),
        Field::new("n", LogicalType::Int64, true),
    ];
    let logical = LogicalType::Struct(fields.clone().try_into().expect("distinct fields"));
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
    let (widening, held) = to_json("widened-struct-json", logical, structs).await;
    widening.expect("structs widen to JSON");
    assert_eq!(text(&held), json(&[r#"{"x":true,"n":null}"#, r#"{"a":1}"#]));
}

/// Row 1 holding `text` as the engine renders a value of `logical`, a type the destination does
/// not store: as text, its field naming the type.
fn lowered(text: &str, logical: &LogicalType) -> RecordBatch {
    let named = serde_json::to_string(logical).expect("a type serializes");
    let field = arrow_schema::Field::new("held", arrow_schema::DataType::Utf8, true)
        .with_metadata([(rdlt_connector::LOGICAL_TYPE_KEY.to_owned(), named)].into());
    let texts: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![text]));
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            field,
            arrow_schema::Field::new("seq", arrow_schema::DataType::Binary, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            texts,
            Arc::new(BinaryArray::from_iter_values([seq(1)])),
        ],
    )
    .expect("a valid batch")
}

#[tokio::test]
async fn text_the_engine_rendered_a_type_as_becomes_that_type_s_json_when_it_widens_to_json() {
    let decimal = LogicalType::Decimal(rdlt_connector::DecimalType::new(7, 2).expect("a decimal"));
    let cases = [
        (LogicalType::Int16, "4450", "4450"),
        (LogicalType::Bool, "true", "true"),
        (decimal, "123.40", "123.40"),
        (
            LogicalType::Timestamp(TimeUnit::Microsecond, None),
            "1970-01-01T00:00:00",
            r#""1970-01-01T00:00:00""#,
        ),
    ];
    for (case, (logical, text, json)) in cases.into_iter().enumerate() {
        let name = format!("widened-lowered-{case}");
        let destination = store(&name).await;
        let mut session = open(destination.as_ref(), "p", 1).await;
        let create = TableChange::Create {
            table: events(),
            schema: TableSchema::new(vec![
                Field::new("id", LogicalType::Int64, false),
                Field::new("held", LogicalType::Utf8, true),
                Field::new("seq", LogicalType::Binary, false),
            ])
            .expect("the schema is valid"),
        };
        session
            .session
            .apply_schema(&create)
            .await
            .expect("created");
        let batch = lowered(text, &logical);
        committed(&mut session, 1, batch).await;
        let widen = TableChange::Widen {
            table: events(),
            column: "held".into(),
            from: LogicalType::Utf8,
            to: LogicalType::Json,
        };
        session
            .session
            .apply_schema(&widen)
            .await
            .expect("text widens to JSON");
        let object: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![r#"{"a":1}"#]));
        committed(&mut session, 2, holding(vec![2], object, LogicalType::Json)).await;
        let expected = [
            (1, Some(json.to_owned())),
            (2, Some(r#"{"a":1}"#.to_owned())),
        ];
        assert_eq!(held_text(&name), expected, "{logical:?}");
    }
}
