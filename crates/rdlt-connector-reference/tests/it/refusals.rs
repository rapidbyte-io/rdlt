//! A written batch a merge table cannot take is refused where it is staged, under the same code
//! by the memory and the SQLite destinations.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int8Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use rdlt_connector::{
    ChangeColumns, ChangeOp, ConnectContext, ConnectorErrorKind, Deletion, Destination,
    HistoryColumns, LoadId, MergeKey, OpenContext, PipelineId, SchemaVersion, SegmentId,
    TableChange, TablePath, TableRef, TableSchema, destination_factory,
};
use rdlt_connector_reference::{MemoryDestination, SqliteDestination};
use serde_json::json;

/// A change stream's table keyed by `id`, a history table where `history`.
fn orders(history: bool) -> TableRef {
    TableRef {
        path: TablePath::new(["orders"]).expect("a valid table path"),
        name: "orders".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: Some(ChangeColumns {
                op: "op".into(),
                unchanged: (!history).then(|| "unchanged".into()),
                deletion: Deletion::Soft { at: "at".into() },
            }),
            history: history.then(|| HistoryColumns {
                valid_from: "from".into(),
                valid_to: "to".into(),
                is_current: "current".into(),
                row_hash: "hash".into(),
            }),
        }),
    }
}

/// The stored columns of [`orders`].
fn stored(history: bool) -> Vec<ArrowField> {
    let mut fields = vec![
        ArrowField::new("id", DataType::Int64, true),
        ArrowField::new("value", DataType::Utf8, true),
        ArrowField::new("seq", DataType::Binary, true),
        ArrowField::new("at", DataType::Int64, true),
    ];
    if history {
        fields.extend([
            ArrowField::new("from", DataType::Int64, true),
            ArrowField::new("to", DataType::Int64, true),
            ArrowField::new("current", DataType::Boolean, true),
            ArrowField::new("hash", DataType::Binary, true),
        ]);
    }
    fields
}

/// One written row of [`orders`]: its op, sequence, deletion time and unchanged flags.
struct Written {
    op: i8,
    seq: Option<u8>,
    at: Option<i64>,
    flags: Option<ArrayRef>,
}

impl Written {
    fn update() -> Self {
        Self {
            op: ChangeOp::Update.code(),
            seq: Some(1),
            at: None,
            flags: None,
        }
    }

    fn flagging(bits: u8) -> Self {
        let flags: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[bits][..])]));
        Self {
            flags: Some(flags),
            ..Self::update()
        }
    }

    fn batch(&self, history: bool) -> RecordBatch {
        let seq = self.seq.map(|seq| vec![seq; 16]);
        let mut columns: Vec<(&str, ArrayRef)> = vec![
            ("id", Arc::new(Int64Array::from(vec![1]))),
            ("value", Arc::new(StringArray::from(vec!["a"]))),
            ("seq", Arc::new(BinaryArray::from(vec![seq.as_deref()]))),
            ("at", Arc::new(Int64Array::from(vec![self.at]))),
        ];
        if history {
            columns.extend([
                ("from", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                ("to", Arc::new(Int64Array::new_null(1))),
                ("current", Arc::new(BooleanArray::from(vec![true]))),
                ("hash", Arc::new(BinaryArray::from(vec![Some(&b"h"[..])]))),
            ]);
        }
        columns.push(("op", Arc::new(Int8Array::from(vec![self.op]))));
        if !history {
            let none: ArrayRef = Arc::new(BinaryArray::from(vec![None::<&[u8]>]));
            columns.push(("unchanged", self.flags.clone().unwrap_or(none)));
        }
        RecordBatch::try_from_iter(columns).expect("a valid batch")
    }
}

/// The kind and code a flush of `written` to [`orders`] of `destination` is refused with; none
/// where it is staged.
async fn flushed(
    destination: &dyn Destination,
    written: &Written,
    history: bool,
) -> Option<(ConnectorErrorKind, Option<String>)> {
    let context = OpenContext {
        pipeline: PipelineId::parse("p").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let table = orders(history);
    let schema = Arc::new(Schema::new(stored(history)));
    let create = TableChange::Create {
        table: table.clone(),
        schema: TableSchema::from_arrow(&schema).expect("a schema"),
    };
    opened.session.apply_schema(&create).await.expect("created");
    let mut writer = opened.session.writer(&table).await.expect("a writer");
    let batch = written.batch(history);
    writer.write(SegmentId(1), batch).await.expect("buffers");
    let refused = writer.flush().await.err()?;
    Some((refused.kind(), refused.code().map(str::to_owned)))
}

/// Each destination the refusals are held on, over a fresh store.
async fn destinations(
    directory: &std::path::Path,
    case: usize,
) -> Vec<(&'static str, Box<dyn Destination>)> {
    let memory = destination_factory::<MemoryDestination>()
        .connect(
            json!({ "store": format!("refusals-{case}") }),
            ConnectContext::new(),
        )
        .await
        .expect("the memory destination connects");
    let sqlite = destination_factory::<SqliteDestination>()
        .connect(
            json!({ "path": directory.join(format!("refusals-{case}.db")) }),
            ConnectContext::new(),
        )
        .await
        .expect("the sqlite destination connects");
    vec![("memory", memory), ("sqlite", sqlite)]
}

/// Batches no merge table takes, each with the code it is refused under and whether its table
/// keeps history; the fields of a change table's batch are id, value, seq, at, op, unchanged.
fn refused() -> Vec<(&'static str, bool, Written)> {
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let op = |op: i8| Written {
        op,
        ..Written::update()
    };
    let unsequenced = || Written {
        seq: None,
        ..Written::update()
    };
    vec![
        ("flag_on_key", false, Written::flagging(0b1)),
        ("flag_on_key", false, Written::flagging(0b100)),
        ("flag_on_missing_column", false, Written::flagging(0b1_0000)),
        (
            "flag_on_missing_column",
            false,
            Written::flagging(0b10_0000),
        ),
        (
            "flags_invalid",
            false,
            Written {
                flags: Some(numbers),
                ..Written::update()
            },
        ),
        ("op_invalid", false, op(9)),
        ("op_invalid", false, op(-1)),
        ("sequence_missing", false, unsequenced()),
        ("sequence_missing", true, unsequenced()),
        ("deletion_untimed", true, op(ChangeOp::Delete.code())),
        ("deletion_untimed", true, op(ChangeOp::Truncate.code())),
    ]
}

#[tokio::test]
async fn a_batch_a_merge_table_cannot_take_is_refused_alike_where_it_is_staged() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    for (case, (code, history, written)) in refused().iter().enumerate() {
        for (name, destination) in destinations(directory.path(), case).await {
            let refused = flushed(destination.as_ref(), written, *history).await;
            let expected = Some((ConnectorErrorKind::Data, Some((*code).to_owned())));
            assert_eq!(refused, expected, "case {case} on {name}");
        }
    }
}

#[tokio::test]
async fn a_batch_a_merge_table_takes_is_staged() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let taken: Vec<(bool, Written)> = vec![
        (false, Written::update()),
        (false, Written::flagging(0b10)),
        (false, Written::flagging(0b1000)),
        // A table that is no history's takes a delete that says no time.
        (
            false,
            Written {
                op: ChangeOp::Delete.code(),
                ..Written::update()
            },
        ),
        (
            false,
            Written {
                op: ChangeOp::Truncate.code(),
                ..Written::update()
            },
        ),
        (
            true,
            Written {
                op: ChangeOp::Delete.code(),
                at: Some(5),
                ..Written::update()
            },
        ),
        (
            true,
            Written {
                op: ChangeOp::Truncate.code(),
                at: Some(5),
                ..Written::update()
            },
        ),
        (true, Written::update()),
    ];
    for (case, (history, written)) in taken.iter().enumerate() {
        for (name, destination) in destinations(directory.path(), 100 + case).await {
            let refused = flushed(destination.as_ref(), written, *history).await;
            assert_eq!(refused, None, "case {case} on {name}");
        }
    }
}
