//! The change batches a Python read pushes, and the names Python gives a change batch.

use std::num::NonZeroUsize;

use pyo3::types::PyAnyMethods as _;
use rdlt_connector::{
    ChangeOp, ConnectContext, Partition, Push, ReadRequest, SourceEvent, StreamName,
    partition_channel,
};

use super::factory;
use crate::embedded;
use crate::runtime;

/// A stream `rows` read as changes, in one partition, which reads with `read`, defined before it.
const ROWS: &str = "\
stream = types.SimpleNamespace(read=read, plan=None, spec=json.dumps({
    'spec': {'name': 'rows', 'schema': None, 'primary_key': ['id'], 'read_modes': ['cdc'],
             'checkpointing': 'natural', 'replayable': True, 'change_time': None},
    'planned': False, 'stated': False, 'committed': False, 'acknowledged': False,
    'cursor_version': 1}))
streams = [stream]";

#[test]
fn python_s_change_ops_and_columns_are_rust_s() {
    embedded::with(|py| {
        let ops: Vec<(String, i8)> = py
            .eval(
                c"[(op.name, int(op)) for op in __import__('rdlt.connector').connector.ChangeOp]",
                None,
                None,
            )
            .unwrap()
            .extract()
            .unwrap();
        let rust = [
            ("INSERT", ChangeOp::Insert),
            ("UPDATE", ChangeOp::Update),
            ("DELETE", ChangeOp::Delete),
            ("TRUNCATE", ChangeOp::Truncate),
        ];
        assert_eq!(ops, rust.map(|(name, op)| (name.to_owned(), op.code())));
        let columns: (String, String, String) = py
            .eval(
                c"(lambda sdk: (sdk.OP_COLUMN, sdk.SEQ_COLUMN, sdk.UNCHANGED_COLUMN))(__import__('rdlt.connector').connector)",
                None,
                None,
            )
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(
            columns,
            (
                rdlt_connector::OP_COLUMN.to_owned(),
                rdlt_connector::SEQ_COLUMN.to_owned(),
                rdlt_connector::UNCHANGED_COLUMN.to_owned()
            )
        );
    });
}

#[test]
fn a_change_batch_or_stream_a_read_yields_is_pushed_as_changes() {
    embedded::with(|py| {
        let body = format!(
            "\
import pyarrow as pa
import rdlt._native as n
def changed(rows):
    return pa.record_batch({{
        'id': pa.array(range(rows), pa.int64()),
        '_rdlt_op': pa.array([0] * rows, pa.int8()),
        '_rdlt_seq': pa.array([bytes(16)] * rows, pa.binary(16)),
    }})
def read(partition, cursor, context):
    yield n.Changes(changed(2))
    yield n.Changes(pa.Table.from_batches([changed(1), changed(3)]))
{ROWS}"
        );
        let source_factory = factory(py, &body).factory();
        let runtime = runtime::get(py).unwrap();
        let pushed = py.detach(|| {
            runtime.block_on(async {
                let source = source_factory
                    .connect(serde_json::json!({}), ConnectContext::new())
                    .await
                    .expect("the source connects");
                let (sink, mut feed) = partition_channel(NonZeroUsize::new(8).unwrap());
                let rows = StreamName::new("rows").unwrap();
                let request = ReadRequest::new(rows, Partition::single(), None);
                let events = async {
                    let mut events = Vec::new();
                    while let Some(event) = feed.recv().await {
                        events.push(event);
                    }
                    events
                };
                let (read, events) =
                    tokio::time::timeout(std::time::Duration::from_secs(30), async {
                        tokio::join!(source.read(request, sink), events)
                    })
                    .await
                    .expect("within 30 s");
                read.expect("the read ends clean");
                events
            })
        });
        let changes: Vec<usize> = pushed
            .iter()
            .map(|event| match event {
                SourceEvent::Push(Push::Changes(batch)) => batch.num_rows(),
                other => panic!("a change batch, not {other:?}"),
            })
            .collect();
        assert_eq!(changes, [2, 1, 3]);
    });
}

#[test]
fn a_change_batch_of_what_is_no_arrow_data_is_refused_where_it_is_made() {
    embedded::with(|py| {
        let refused = py
            .eval(
                c"__import__('rdlt._native', fromlist=['Changes']).Changes({'id': 1})",
                None,
                None,
            )
            .expect_err("a dict is no Arrow data");
        assert_eq!(super::class_of(py, &refused), "TypeError");
        py.eval(
            c"__import__('rdlt._native', fromlist=['Changes']).Changes(__import__('pyarrow').table({'id': [1]}))",
            None,
            None,
        )
        .expect("a table is Arrow data");
    });
}
