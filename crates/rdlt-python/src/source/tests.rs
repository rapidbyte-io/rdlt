use std::sync::Arc;
use std::time::Duration;

use pyo3::types::{PyAnyMethods as _, PyDict, PyTypeMethods as _};
use pyo3::{Py, PyAny, Python};
use rdlt_connector::{
    ConnectContext, ConnectorSpec, Partition, ReadRequest, SourceEvent, StreamName,
    partition_channel,
};
use rdlt_connector_reference::MemoryDestination;

use super::NativeSourceFactory;
use crate::embedded;
use crate::pipeline::NativePipeline;
use crate::runtime;

/// A source object, `connect`, whose streams `body` defines.
fn connect(py: Python<'_>, body: &str) -> Py<PyAny> {
    let code = format!(
        "import json, types, threading\n{body}\n\
         connected = types.SimpleNamespace(check=lambda: None, streams=streams)\n\
         connect = lambda config: connected\n"
    );
    let globals = PyDict::new(py);
    py.run(&std::ffi::CString::new(code).unwrap(), Some(&globals), None)
        .unwrap();
    globals.get_item("connect").unwrap().unbind()
}

fn factory(py: Python<'_>, body: &str) -> NativeSourceFactory {
    NativeSourceFactory::new("io.example.py", "1.0.0".to_owned(), "{}", connect(py, body)).unwrap()
}

fn loaded(py: Python<'_>, store: &str, read: &str, body: &str) -> (String, Option<String>) {
    let host = rdlt::Host::new()
        .source_factory(Arc::from(factory(py, body).factory()))
        .destination::<MemoryDestination>();
    let document = serde_json::json!({
        "version": 1, "pipeline": store,
        "source": { "connector": "io.example.py" },
        "destination": { "connector": "io.rapidbyte.memory", "config": { "store": store } },
        "streams": [{ "name": "rows", "read": read }],
        "commit": { "rows": 1 },
    });
    let builder = rdlt::Pipeline::from_text(&document.to_string(), "<python>").unwrap();
    let pipeline = NativePipeline::built(py, builder, Arc::new(host)).unwrap();
    embedded::ended_within(py, &pipeline.start(py).unwrap())
}

fn published_rows(store: &str, table: &str) -> usize {
    rdlt_connector_reference::published(store, table)
        .iter()
        .map(arrow_array::RecordBatch::num_rows)
        .sum()
}

const ONE_STREAM: &str = "\
def read(partition, cursor, context):
    yield [{'n': n} for n in range(40)]
stream = types.SimpleNamespace(read=read, partitions=lambda: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'natural', 'partitioned': False}))
streams = [stream]";

#[test]
fn a_python_source_loads_into_the_engine() {
    embedded::with(|py| {
        let (_, error) = loaded(py, "python-loaded", "full", ONE_STREAM);
        assert_eq!(error, None);
        assert_eq!(published_rows("python-loaded", "rows"), 40);
    });
}

#[test]
fn each_partition_reads_on_a_thread_of_its_own() {
    embedded::with(|py| {
        let body = "\
import builtins
builtins.idents = set()
# Held together until all three have started: a thread's id is reused once it has ended.
barrier = threading.Barrier(3)
def read(partition, cursor, context):
    builtins.idents.add(threading.get_ident())
    barrier.wait(30)
    yield {'partition': partition}
stream = types.SimpleNamespace(read=read, partitions=lambda: ['a', 'b', 'c'], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'natural', 'partitioned': True}))
streams = [stream]";
        let (_, error) = loaded(py, "python-partitioned", "full", body);
        assert_eq!(error, None);
        assert_eq!(published_rows("python-partitioned", "rows"), 3);
        let idents: usize = py
            .eval(c"len(__import__('builtins').idents)", None, None)
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(
            idents, 3,
            "each of the three partitions read on its own thread"
        );
    });
}

#[test]
fn a_checkpoint_s_cursor_is_where_the_next_run_resumes() {
    embedded::with(|py| {
        let body = "\
import builtins
builtins.cursors = getattr(builtins, 'cursors', [])
def read(partition, cursor, context):
    builtins.cursors.append(cursor)
    start = 0 if cursor is None else json.loads(cursor)['after'] + 1
    for n in range(start, start + 10):
        yield {'n': n}
    yield __import__('rdlt._native', fromlist=['Checkpoint']).Checkpoint(json.dumps({'after': start + 9}))
stream = types.SimpleNamespace(read=read, partitions=lambda: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'], 'checkpointing': 'natural', 'partitioned': False}))
streams = [stream]";
        loaded(py, "python-resumed", "incremental", body);
        loaded(py, "python-resumed", "incremental", body);
        let cursors: Vec<Option<String>> = py
            .eval(c"__import__('builtins').cursors", None, None)
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(cursors[0], None);
        let resumed: serde_json::Value =
            serde_json::from_str(cursors[1].as_deref().expect("resumed")).unwrap();
        assert_eq!(resumed["after"], 9);
        assert_eq!(published_rows("python-resumed", "rows"), 20);
    });
}

#[test]
fn a_factory_says_its_spec_and_refuses_an_id_or_schema_that_is_not_one() {
    embedded::with(|py| {
        let made = factory(py, ONE_STREAM);
        let source_factory = made.factory();
        let spec: &ConnectorSpec = source_factory.spec();
        assert_eq!(spec.id.as_str(), "io.example.py");
        assert_eq!(made.id(), "io.example.py");
        assert_eq!(spec.version, "1.0.0");
        assert_eq!(
            NativeSourceFactory::new("not an id", "1".to_owned(), "{}", py.None())
                .err()
                .expect("refused")
                .get_type(py)
                .name()
                .unwrap()
                .to_string(),
            "ValueError"
        );
        assert!(
            NativeSourceFactory::new("io.example.py", "1".to_owned(), "not json", py.None())
                .is_err()
        );
    });
}

fn built_up(py: Python<'_>, body: &str) -> Result<NativePipeline, pyo3::PyErr> {
    let host = rdlt::Host::new()
        .source_factory(Arc::from(factory(py, body).factory()))
        .destination::<MemoryDestination>();
    let document = serde_json::json!({
        "version": 1, "pipeline": "python-checked",
        "source": { "connector": "io.example.py" },
        "destination": { "connector": "io.rapidbyte.memory", "config": { "store": "python-checked" } },
        "streams": [{ "name": "rows" }],
    });
    let builder = rdlt::Pipeline::from_text(&document.to_string(), "<python>").unwrap();
    NativePipeline::built(py, builder, Arc::new(host))
}

fn class_of(py: Python<'_>, error: &pyo3::PyErr) -> String {
    error.get_type(py).name().unwrap().to_string()
}

#[test]
fn a_source_whose_check_raises_fails_the_build_with_its_type() {
    embedded::with(|py| {
        let body = "\
def read(partition, cursor, context):
    yield {'n': 1}
stream = types.SimpleNamespace(read=read, partitions=lambda: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'natural', 'partitioned': False}))
streams = [stream]
def failing():
    raise PermissionError('no access')
connected = types.SimpleNamespace(check=failing, streams=streams)
connect = lambda config: connected";
        let globals = PyDict::new(py);
        py.run(
            &std::ffi::CString::new(format!("import json, types\n{body}")).unwrap(),
            Some(&globals),
            None,
        )
        .unwrap();
        let connect = globals.get_item("connect").unwrap().unbind();
        let source_factory =
            NativeSourceFactory::new("io.example.py", "1".to_owned(), "{}", connect).unwrap();
        let host = rdlt::Host::new()
            .source_factory(Arc::from(source_factory.factory()))
            .destination::<MemoryDestination>();
        let document = serde_json::json!({
            "version": 1, "pipeline": "python-checks",
            "source": { "connector": "io.example.py" },
            "destination": { "connector": "io.rapidbyte.memory", "config": { "store": "python-checks" } },
            "streams": [{ "name": "rows" }],
        });
        let builder = rdlt::Pipeline::from_text(&document.to_string(), "<python>").unwrap();
        let error = NativePipeline::built(py, builder, Arc::new(host))
            .err()
            .expect("the build fails");
        assert_eq!(class_of(py, &error), "SourceError");
    });
}

#[test]
fn a_stream_described_wrongly_fails_the_build() {
    embedded::with(|py| {
        let body = "\
def read(partition, cursor, context):
    yield {'n': 1}
stream = types.SimpleNamespace(read=read, partitions=lambda: [], spec='not json')
streams = [stream]";
        let error = built_up(py, body).err().expect("the build fails");
        assert_eq!(class_of(py, &error), "SourceError");
    });
}

#[test]
fn a_barrier_reaches_a_read_that_is_waiting_on_its_generator() {
    embedded::with(|py| {
        let body = "\
import rdlt._native as n
def read(partition, cursor, context):
    while not context.checkpoint_due():
        __import__('time').sleep(0.005)
    yield n.Checkpoint('{\"at\": 1}')
stream = types.SimpleNamespace(read=read, partitions=lambda: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'on_demand', 'partitioned': False}))
streams = [stream]";
        let source_factory = factory(py, body).factory();
        let runtime = runtime::get(py).unwrap();
        py.detach(|| {
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(30), async {
                    let source = source_factory
                        .connect(serde_json::json!({}), ConnectContext::new())
                        .await
                        .unwrap();
                    let (sink, mut feed) =
                        partition_channel(std::num::NonZeroUsize::new(8).unwrap());
                    let request = ReadRequest::new(
                        StreamName::new("rows").unwrap(),
                        Partition::single(),
                        None,
                    );
                    let read = source.read(request, sink);
                    feed.request_checkpoint(1);
                    let (ended, event) = tokio::join!(read, feed.recv());
                    ended.unwrap();
                    assert!(matches!(event, Some(SourceEvent::Checkpoint { .. })));
                })
                .await
                .expect("within 30 s");
            });
        });
    });
}

#[test]
fn an_exception_fails_the_run_with_its_type_and_traceback() {
    embedded::with(|py| {
        let body = "\
def read(partition, cursor, context):
    yield {'n': 1}
    raise ValueError('page 2 is malformed')
stream = types.SimpleNamespace(read=read, partitions=lambda: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'natural', 'partitioned': False}))
streams = [stream]";
        let (_, error) = loaded(py, "python-raised", "full", body);
        let report: serde_json::Value =
            serde_json::from_str(&error.expect("the run failed")).unwrap();
        assert_eq!(report["kind"], "source");
        assert_eq!(report["connector_code"], "value_error");
        let causes = report["causes"].as_array().expect("causes");
        assert!(
            causes.iter().any(|cause| cause
                .as_str()
                .is_some_and(|text| text.starts_with("Traceback (most recent call last)"))),
            "{causes:?}"
        );
    });
}

const RESUMING: &str =
    "stream = types.SimpleNamespace(read=read, partitions=lambda: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'],
    'checkpointing': 'natural', 'partitioned': False, 'resumes': True}))
streams = [stream]";

#[test]
fn a_resuming_read_that_pushes_rows_and_never_checkpoints_fails() {
    embedded::with(|py| {
        for (store, read) in [
            ("no-checkpoint-rows", "yield {'n': 1}"),
            (
                "no-checkpoint-batch",
                "yield __import__('pyarrow').record_batch({'n': [1]})",
            ),
        ] {
            let body = format!("def read(partition, cursor, context):\n    {read}\n{RESUMING}");
            let (_, error) = loaded(py, store, "incremental", &body);
            let report: serde_json::Value =
                serde_json::from_str(&error.expect("the run failed")).unwrap();
            assert_eq!(report["connector_code"], "checkpoint_missing", "{read}");
        }
    });
}

#[test]
fn a_resuming_read_that_pushes_nothing_ends_clean() {
    embedded::with(|py| {
        let body = format!(
            "def read(partition, cursor, context):\n    \
             yield __import__('pyarrow').record_batch({{'n': __import__('pyarrow').array([], __import__('pyarrow').int64())}})\n{RESUMING}"
        );
        let (_, error) = loaded(py, "no-checkpoint-empty", "incremental", &body);
        assert_eq!(error, None);
    });
}
