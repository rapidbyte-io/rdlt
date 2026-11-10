use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::{Int64Array, RecordBatch};
use bytes::Bytes;
use pyo3::types::{PyAnyMethods as _, PyDict};
use pyo3::{Py, PyAny, Python};
use rdlt_connector::{
    Capabilities, CommitKey, CommitMeta, ConnectContext, ConnectorErrorKind, DestinationFactory,
    Epoch, Field, LogicalType, OpenContext, PipelineId, PublishedRows, Receipt, SchemaVersion,
    SegmentId, SegmentSet, StateChange, StateRecord, TableChange, TablePath, TableRef, WriteStats,
};
use rdlt_connector_reference::MemorySource;
use serde_json::json;

use super::{NativeDestinationFactory, minimal_capabilities};
use crate::embedded;
use crate::pipeline::NativePipeline;

/// The memory destination of the package's tests as its factory, and the placement that keeps
/// its instances, which the caller holds while it is placed.
fn memory(py: Python<'_>) -> (Py<PyAny>, Py<NativeDestinationFactory>) {
    let placed = py
        .eval(
            c"__import__('rdlt.connector._destination', fromlist=['placed']).placed(__import__('tests.connectors.memory', fromlist=['Memory']).Memory)",
            None,
            None,
        )
        .unwrap();
    let factory = placed.get_item(0).unwrap().extract().unwrap();
    (placed.unbind(), factory)
}

#[test]
fn a_python_destination_loads_what_the_engine_writes_exactly_once() {
    embedded::with(|py| {
        let (_placement, factory) = memory(py);
        let host = rdlt::Host::new()
            .source::<MemorySource>()
            .destination_factory(Arc::from(factory.get().factory()));
        let rows: Vec<serde_json::Value> = (0..30).map(|n| json!({ "n": n })).collect();
        let document = json!({
            "version": 1, "pipeline": "into-python",
            "source": { "connector": "io.rapidbyte.memory", "config": { "streams": { "rows": rows }, "page_size": 7 } },
            "destination": { "connector": "io.example.memory", "config": { "store": "into-python" } },
            "streams": [{ "name": "rows" }],
            "commit": { "rows": 1 },
        });
        let builder = rdlt::Pipeline::from_text(&document.to_string(), "<python>").unwrap();
        let pipeline = NativePipeline::built(py, builder, Arc::new(host)).unwrap();
        let (_, error) = embedded::ended_within(py, &pipeline.start(py).unwrap());
        assert_eq!(error, None);
        let published: usize = py
            .eval(
                c"__import__('tests.connectors.memory', fromlist=['published']).published('into-python', 'rows')",
                None,
                None,
            )
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(published, 30);
    });
}

/// A destination that records each call it is given, as the protocol's JSON, in `calls`; its
/// `open` answers `opened`.
const RECORDER: &str = "\
import json
calls = []
class Writer:
    def write(self, segment, batch):
        import pyarrow as pa
        calls.append(['write', segment, pa.record_batch(batch).num_rows])
    def flush(self):
        calls.append(['flush'])
        return 3, 24
class Session:
    def apply_schema(self, change):
        calls.append(['apply_schema', json.loads(change)])
    def writer(self, table):
        calls.append(['writer', json.loads(table)['name']])
        return Writer()
    def discard_staged(self):
        calls.append(['discard_staged'])
    def commit(self, meta):
        meta = json.loads(meta)
        calls.append(['commit', meta])
        committed_at = {'secs_since_epoch': 5, 'nanos_since_epoch': 0}
        return json.dumps({'key': meta['key'], 'committed_at': committed_at, 'rows': 3, 'bytes': 24})
    def close(self):
        calls.append(['close'])
class Connected:
    def check(self):
        calls.append(['check'])
    def open(self, pipeline):
        calls.append(['open', pipeline])
        return Session(), opened
    def published(self, table):
        import pyarrow as pa
        calls.append(['published', json.loads(table)['name']])
        return [list(pa.record_batch({'n': [1, 2]}).__arrow_c_array__())]
def connect(config):
    calls.append(['connect', json.loads(config)])
    return Connected()
";

/// The recorder's factory, its `open` answering `opened`, and the list of its calls.
fn recorder(opened: &str) -> (NativeDestinationFactory, Py<PyAny>) {
    recorder_reading(opened, true)
}

/// The recorder's factory, which reads back where `reads_back`, and the list of its calls.
fn recorder_reading(opened: &str, reads_back: bool) -> (NativeDestinationFactory, Py<PyAny>) {
    embedded::with(|py| {
        let globals = PyDict::new(py);
        globals.set_item("opened", opened).unwrap();
        py.run(
            &std::ffi::CString::new(RECORDER).unwrap(),
            Some(&globals),
            None,
        )
        .unwrap();
        let connect = globals.get_item("connect").unwrap().unbind();
        let factory = NativeDestinationFactory::new(
            "io.example.recorder",
            "1.0.0".to_owned(),
            "{}",
            &minimal_capabilities(),
            reads_back,
            connect,
        )
        .unwrap();
        (factory, globals.get_item("calls").unwrap().unbind())
    })
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["orders"]).unwrap(),
        name: "orders".into(),
        version: SchemaVersion(2),
        generation: None,
        merge: None,
    }
}

fn record(key: &str, value: &'static [u8]) -> StateRecord {
    StateRecord {
        key: key.to_owned(),
        value: Bytes::from_static(value),
    }
}

/// `future`'s answer, within a minute, on a runtime of its own: the caller holds no GIL.
fn within<T>(future: impl Future<Output = T>) -> T {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(60), future).await })
        .expect("within a minute")
}

/// What the recorder was called with, as JSON.
fn calls(recorded: &Py<PyAny>) -> serde_json::Value {
    embedded::with(|py| {
        let text: String = py
            .import("json")
            .unwrap()
            .call_method1("dumps", (recorded.bind(py),))
            .unwrap()
            .extract()
            .unwrap();
        serde_json::from_str(&text).unwrap()
    })
}

const OPENED: &str =
    r#"{"epoch": 4, "landed": null, "state": [{"key": "k", "value": "{\"n\": 1}"}]}"#;

/// A commit of segment 7 that abandons segment 8, puts one state record and deletes another.
fn commit_meta() -> CommitMeta {
    CommitMeta {
        key: CommitKey {
            epoch: Epoch(4),
            seq: 1,
        },
        after: None,
        segments: SegmentSet::from_iter([SegmentId(7)]),
        abandoned: SegmentSet::from_iter([SegmentId(8)]),
        state_delta: vec![
            StateChange::Put(record("s", br#"{"at":[1, 2.50]}"#)),
            StateChange::Delete("gone".to_owned()),
        ],
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    }
}

/// [`table`], as the SDK's JSON tells it.
fn table_json() -> serde_json::Value {
    json!({ "path": ["orders"], "name": "orders", "version": 2, "generation": null, "merge": null })
}

/// A load into `factory`'s destination, every call of a session's made once as the engine makes
/// it, committing `meta`: the commit's receipt.
async fn loaded(factory: &dyn DestinationFactory, meta: &CommitMeta) -> Receipt {
    let destination = factory
        .connect(json!({ "given": 1 }), ConnectContext::new())
        .await
        .unwrap();
    assert_eq!(destination.capabilities(), &Capabilities::minimal());
    destination.check().await.unwrap();
    let context = OpenContext {
        pipeline: PipelineId::parse("p").unwrap(),
    };
    let opened = destination.open(&context).await.unwrap();
    assert_eq!(opened.epoch, Epoch(4));
    assert_eq!(opened.landed, None);
    // A state record's value is its JSON text, kept byte for byte.
    assert_eq!(opened.state, [record("k", br#"{"n": 1}"#)]);
    let mut session = opened.session;
    let add = TableChange::AddColumn {
        table: table(),
        field: Field::new("n", LogicalType::Int64, true),
    };
    session.apply_schema(&add).await.unwrap();
    let mut writer = session.writer(&table()).await.unwrap();
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("n", ids as _)]).unwrap();
    writer.write(SegmentId(7), batch).await.unwrap();
    let flushed = writer.flush().await.unwrap();
    assert_eq!(flushed, WriteStats { rows: 3, bytes: 24 });
    let receipt = session.commit(meta).await.unwrap();
    session.close().await.unwrap();
    receipt
}

#[test]
fn each_call_reaches_the_python_destination_as_the_sdk_s_json() {
    let (factory, recorded) = recorder(OPENED);
    let factory = factory.factory();
    assert_eq!(factory.spec().id.as_str(), "io.example.recorder");
    assert_eq!(factory.spec().version, "1.0.0");
    let meta = commit_meta();
    let receipt = within(loaded(factory.as_ref(), &meta));
    let answered = Receipt {
        key: meta.key,
        committed_at: UNIX_EPOCH + Duration::from_secs(5),
        rows: 3,
        bytes: 24,
    };
    assert_eq!(receipt, answered);
    let told = json!({
        "key": { "epoch": 4, "seq": 1 },
        "after": null,
        "segments": [{ "first": 7, "last": 7 }],
        "abandoned": [{ "first": 8, "last": 8 }],
        "state_delta": [{ "put": { "key": "s", "value": r#"{"at":[1, 2.50]}"# } }, { "delete": "gone" }],
        "finish_generations": [],
        "child_tables": [],
        "drop_tables": [],
    });
    let field = json!([{ "name": "n", "nullable": true, "type": "int64" }]);
    assert_eq!(
        calls(&recorded),
        json!([
            ["connect", { "given": 1 }],
            ["check"],
            ["open", "p"],
            ["discard_staged"],
            ["apply_schema", { "add_column": { "table": table_json(), "field": field } }],
            ["writer", "orders"],
            ["write", 7, 3],
            ["flush"],
            ["commit", told],
            ["close"],
        ])
    );
}

#[test]
fn each_schema_change_is_told_by_its_kind() {
    let (factory, recorded) = recorder(OPENED);
    let factory = factory.factory();
    let changes = [
        TableChange::Create {
            table: table(),
            schema: rdlt_connector::TableSchema::new(vec![Field::new(
                "id",
                LogicalType::Int64,
                false,
            )])
            .unwrap(),
        },
        TableChange::Widen {
            table: table(),
            column: "n".into(),
            from: LogicalType::Int32,
            to: LogicalType::Int64,
        },
    ];
    within(async {
        let destination = factory
            .connect(json!({ "given": 1 }), ConnectContext::new())
            .await
            .unwrap();
        let context = OpenContext {
            pipeline: PipelineId::parse("p").unwrap(),
        };
        let mut session = destination.open(&context).await.unwrap().session;
        for change in &changes {
            session.apply_schema(change).await.unwrap();
        }
    });
    let told: Vec<serde_json::Value> = calls(&recorded)
        .as_array()
        .unwrap()
        .iter()
        .filter(|call| call[0] == "apply_schema")
        .map(|call| call[1].clone())
        .collect();
    let table = table_json();
    assert_eq!(
        told,
        [
            json!({ "create": { "table": table, "schema": { "fields": [[{ "name": "id", "nullable": false, "type": "int64" }]] } } }),
            json!({ "widen": { "table": table, "column": "n", "from": [{ "type": "int32" }], "to": [{ "type": "int64" }] } }),
        ]
    );
}

#[test]
fn the_readable_factory_reads_back_what_the_destination_answers() {
    let (factory, recorded) = recorder(OPENED);
    assert!(!factory.factory().reads_back());
    let readable = factory.readable();
    assert!(readable.reads_back());
    let rows = within(async {
        let (_, reader) = readable
            .connect_reading(json!({ "given": 1 }), ConnectContext::new())
            .await
            .unwrap();
        let read = PublishedRows::gather(reader.as_ref(), &table()).await;
        read.unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
    });
    assert_eq!(rows, 2);
    assert_eq!(calls(&recorded)[1], json!(["published", "orders"]));
    let (unread, _) = recorder_reading(OPENED, false);
    assert!(!unread.readable().reads_back());
}

#[test]
fn an_opened_session_rdlt_cannot_read_is_an_internal_error_naming_what_it_is() {
    let (factory, _) = recorder(r#"{"epoch": 4, "landed": null}"#);
    let factory = factory.factory();
    let error = within(async {
        let destination = factory
            .connect(json!({}), ConnectContext::new())
            .await
            .unwrap();
        let context = OpenContext {
            pipeline: PipelineId::parse("p").unwrap(),
        };
        destination.open(&context).await.expect_err("refused")
    });
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    assert!(error.to_string().contains("an opened session"), "{error}");
}

#[test]
fn what_the_destination_raises_is_the_connector_s_error_of_its_kind_and_code() {
    let factory = embedded::with(|py| {
        let connect = py
            .eval(
                c"lambda config: (_ for _ in ()).throw(__import__('rdlt.connector', fromlist=['Fenced']).Fenced('a newer session holds it', code='commit_stale'))",
                None,
                None,
            )
            .unwrap()
            .unbind();
        NativeDestinationFactory::new(
            "io.example.raising",
            "1.0.0".to_owned(),
            "{}",
            &minimal_capabilities(),
            false,
            connect,
        )
        .unwrap()
        .factory()
    });
    let error = within(factory.connect(json!({}), ConnectContext::new()))
        .err()
        .expect("refused");
    assert_eq!(error.kind(), ConnectorErrorKind::Fenced);
    assert_eq!(error.code(), Some("commit_stale"));
    assert!(
        error.to_string().contains("a newer session holds it"),
        "{error}"
    );
}

#[test]
fn a_factory_refuses_what_describes_no_destination() {
    embedded::with(|py| {
        let connect = || {
            py.eval(c"lambda config: None", None, None)
                .unwrap()
                .unbind()
        };
        let capabilities = minimal_capabilities();
        let with = |pointer: &str, key: &str| {
            let mut given: serde_json::Value = serde_json::from_str(&capabilities).unwrap();
            given.pointer_mut(pointer).unwrap()[key] = json!(true);
            given.to_string()
        };
        let (in_mode, at_top) = (with("/write_modes", "upsert"), with("", "drop_table"));
        for (id, schema, capabilities, said) in [
            ("No Id", "{}", capabilities.as_str(), "is no connector id"),
            (
                "io.example.x",
                "{",
                capabilities.as_str(),
                "schema is not JSON",
            ),
            ("io.example.x", "{}", r#"{"append": true}"#, "capabilities"),
            ("io.example.x", "{}", in_mode.as_str(), "write_modes.upsert"),
            ("io.example.x", "{}", at_top.as_str(), "drop_table"),
        ] {
            let refused = NativeDestinationFactory::new(
                id,
                "1.0.0".to_owned(),
                schema,
                capabilities,
                true,
                connect(),
            );
            let error = refused.err().expect("refused");
            assert!(
                error.is_instance_of::<pyo3::exceptions::PyValueError>(py),
                "{said}"
            );
            assert!(error.to_string().contains(said), "{error}");
        }
        let made = NativeDestinationFactory::new(
            "io.example.x",
            "1.0.0".to_owned(),
            r#"{"type": "object"}"#,
            &minimal_capabilities(),
            true,
            connect(),
        )
        .unwrap();
        assert_eq!(made.id(), "io.example.x");
        assert_eq!(
            made.factory().spec().config_schema,
            json!({ "type": "object" })
        );
    });
}

/// Connects `factory`'s destination, opens a session and commits `meta` in it.
async fn committing(
    factory: &dyn DestinationFactory,
    meta: &CommitMeta,
) -> rdlt_connector::Result<Receipt> {
    let destination = factory.connect(json!({}), ConnectContext::new()).await?;
    let context = OpenContext {
        pipeline: PipelineId::parse("p").unwrap(),
    };
    destination.open(&context).await?.session.commit(meta).await
}

/// The state changes the recorder's last commit was told, and whether it was told one.
fn told_state(recorded: &Py<PyAny>) -> Option<serde_json::Value> {
    let calls = calls(recorded);
    let commit = calls
        .as_array()
        .unwrap()
        .iter()
        .rfind(|call| call[0] == "commit")?;
    Some(commit[1]["state_delta"].clone())
}

#[test]
fn a_state_value_that_is_no_json_text_reaches_python_as_it_is() {
    let (factory, recorded) = recorder(OPENED);
    let meta = CommitMeta {
        state_delta: vec![StateChange::Put(record("s", b" not json "))],
        ..commit_meta()
    };
    within(committing(factory.factory().as_ref(), &meta)).unwrap();
    assert_eq!(
        told_state(&recorded),
        Some(json!([{ "put": { "key": "s", "value": " not json " } }]))
    );
}

#[test]
fn a_state_value_that_is_not_utf8_is_refused_before_python_is_called() {
    let (factory, recorded) = recorder(OPENED);
    let meta = CommitMeta {
        state_delta: vec![StateChange::Put(record("s", b"\xff"))],
        ..commit_meta()
    };
    let refused = within(committing(factory.factory().as_ref(), &meta)).unwrap_err();
    assert_eq!(refused.kind(), ConnectorErrorKind::Data);
    assert!(
        refused.to_string().contains("state record \"s\""),
        "{refused}"
    );
    assert_eq!(told_state(&recorded), None);
}

#[test]
fn the_minimal_capabilities_are_the_sdk_s() {
    let read: Capabilities = serde_json::from_str(&minimal_capabilities()).unwrap();
    assert_eq!(read, Capabilities::minimal());
}
