use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pyo3::types::{PyAnyMethods as _, PyDict, PyTypeMethods as _};
use pyo3::{Py, PyAny, Python};
use rdlt_connector::{
    ConnectContext, ConnectorSpec, GenerationId, Partition, PartitionId, PartitionState,
    ReadRequest, ReadStream as _, SourceEvent, StreamName, StreamState, partition_channel,
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
         connect = lambda config, host: connected\n"
    );
    let globals = PyDict::new(py);
    py.run(&std::ffi::CString::new(code).unwrap(), Some(&globals), None)
        .unwrap();
    globals.get_item("connect").unwrap().unbind()
}

fn factory(py: Python<'_>, body: &str) -> NativeSourceFactory {
    NativeSourceFactory::new(
        "io.example.py",
        "1.0.0".to_owned(),
        "{}",
        false,
        connect(py, body),
    )
    .unwrap()
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
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'natural', 'partitioned': False, 'stated': False, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}))
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
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [('a', False), ('b', False), ('c', False)], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'natural', 'partitioned': True, 'stated': True, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}))
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
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'], 'checkpointing': 'natural', 'partitioned': False, 'stated': False, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}))
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
fn a_partition_that_never_ends_keeps_its_last_checkpoint_when_its_read_ends_on_rows() {
    embedded::with(|py| {
        for (store, unbounded, resumed) in [
            ("python-unbounded", "True", Some(4)),
            ("python-bounded", "False", None),
        ] {
            let body = format!(
                "\
import builtins
builtins.started = getattr(builtins, 'started', {{}})
def read(partition, cursor, context):
    builtins.started.setdefault('{store}', []).append(cursor)
    start = 0 if cursor is None else json.loads(cursor)['after'] + 1
    for n in range(start, start + 10):
        yield {{'n': n}}
        if n == start + 4:
            yield __import__('rdlt._native', fromlist=['Checkpoint']).Checkpoint(json.dumps({{'after': n}}))
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [('p', {unbounded})], spec=json.dumps({{
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'], 'checkpointing': 'natural', 'partitioned': True, 'stated': True, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}}))
streams = [stream]"
            );
            loaded(py, store, "incremental", &body);
            loaded(py, store, "incremental", &body);
            let started: Vec<Option<String>> = py
                .eval(
                    &std::ffi::CString::new(format!("__import__('builtins').started['{store}']"))
                        .unwrap(),
                    None,
                    None,
                )
                .unwrap()
                .extract()
                .unwrap();
            let after = started.get(1).map(|cursor| {
                serde_json::from_str::<serde_json::Value>(cursor.as_deref().expect("resumed"))
                    .unwrap()["after"]
                    .as_i64()
                    .unwrap()
            });
            assert_eq!(after, resumed, "{store}");
        }
    });
}

#[test]
fn a_stream_that_asks_is_told_its_committed_cursors_by_partition() {
    embedded::with(|py| {
        for (store, partitions, partitioned, told) in [
            (
                "python-told-parted",
                "[('p', False)]",
                "True",
                "('p', '{\"after\": 4}')",
            ),
            (
                "python-told-whole",
                "[]",
                "False",
                "('whole', '{\"after\": 4}')",
            ),
        ] {
            let body = format!(
                "\
import builtins
builtins.told = getattr(builtins, 'told', {{}})
def read(partition, cursor, context):
    for n in range(5):
        yield {{'n': n}}
    yield __import__('rdlt._native', fromlist=['Checkpoint']).Checkpoint(json.dumps({{'after': 4}}))
def committed(cursors):
    builtins.told.setdefault('{store}', []).extend(cursors)
stream = types.SimpleNamespace(read=read, partitions=lambda *state: {partitions}, committed=committed, spec=json.dumps({{
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'], 'checkpointing': 'natural', 'partitioned': {partitioned}, 'stated': {partitioned}, 'replayable': True, 'committed': True, 'acknowledged': False, 'cursor_version': 1}}))
streams = [stream]"
            );
            let (_, error) = loaded(py, store, "incremental", &body);
            assert_eq!(error, None, "{store}");
            let last: String = py
                .eval(
                    &std::ffi::CString::new(format!(
                        "repr(__import__('builtins').told['{store}'][-1])"
                    ))
                    .unwrap(),
                    None,
                    None,
                )
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(last, told, "{store}");
        }
    });
}

#[test]
fn a_stream_that_does_not_ask_is_never_told_its_committed_cursors() {
    embedded::with(|py| {
        let body = "\
def read(partition, cursor, context):
    yield {'n': 1}
    yield __import__('rdlt._native', fromlist=['Checkpoint']).Checkpoint(json.dumps({'after': 1}))
def committed(cursors):
    raise AssertionError('told')
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], committed=committed, spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'], 'checkpointing': 'natural', 'partitioned': False, 'stated': False, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}))
streams = [stream]";
        let (_, error) = loaded(py, "python-untold", "incremental", body);
        assert_eq!(error, None);
    });
}

/// Where the stream `rows` stands, as the reader of a factory made from `body` with `acknowledges`
/// tells it, connected for certification.
fn standing(py: Python<'_>, body: &str, acknowledges: bool) -> Option<rdlt_connector::Cursor> {
    let made = NativeSourceFactory::new(
        "io.example.py",
        "1.0.0".to_owned(),
        "{}",
        acknowledges,
        connect(py, body),
    )
    .unwrap();
    let factory = made.acknowledging();
    assert!(factory.acknowledges());
    assert!(!made.factory().acknowledges(), "served, it tells nothing");
    let runtime = runtime::get(py).unwrap();
    py.detach(|| {
        runtime.block_on(async {
            let (_, reader) = factory
                .connect_acknowledging(serde_json::json!({}), ConnectContext::new())
                .await
                .unwrap();
            let rows = StreamName::new("rows").unwrap();
            reader
                .acknowledged(&rows, &PartitionId::whole())
                .await
                .unwrap()
        })
    })
}

#[test]
fn a_stream_that_tells_where_it_stands_is_asked_for_certification_alone() {
    embedded::with(|py| {
        let body = "\
def read(partition, cursor, context):
    yield {'n': 1}
def acknowledged(partition):
    return json.dumps({'after': 3, 'partition': partition})
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], acknowledged=acknowledged, spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'], 'checkpointing': 'natural', 'partitioned': False, 'stated': False, 'replayable': True, 'committed': False, 'acknowledged': True, 'cursor_version': 1}))
streams = [stream]";
        let told = standing(py, body, true).expect("it tells");
        let decoded: serde_json::Value = told.decode(1).unwrap();
        assert_eq!(
            decoded,
            serde_json::json!({ "after": 3, "partition": "whole" })
        );
        let silent = body.replace("'acknowledged': True", "'acknowledged': False");
        assert_eq!(
            standing(py, &silent, true),
            None,
            "one that does not say it tells"
        );
    });
}

#[test]
fn a_factory_whose_class_tells_nothing_refuses_to_say_where_it_stands() {
    embedded::with(|py| {
        let factory = factory(py, ONE_STREAM).acknowledging();
        assert!(!factory.acknowledges());
        let runtime = runtime::get(py).unwrap();
        let refused = py.detach(|| {
            runtime.block_on(
                factory.connect_acknowledging(serde_json::json!({}), ConnectContext::new()),
            )
        });
        assert_eq!(refused.err().expect("refused").code(), Some("acknowledged"));
    });
}

#[test]
fn a_stream_says_whether_it_can_be_read_again() {
    embedded::with(|py| {
        for replayable in [true, false] {
            let spec = serde_json::json!({
                "name": "rows", "primary_key": null, "read_modes": ["full", "incremental"],
                "checkpointing": "natural", "partitioned": false, "stated": false,
                "replayable": replayable,
                "committed": false, "acknowledged": false, "cursor_version": 1,
            });
            let stream = super::PythonStream::declared(&spec.to_string(), py.None()).unwrap();
            assert_eq!(stream.spec().is_replayable(), replayable);
        }
    });
}

#[test]
fn a_source_is_connected_for_the_host_its_context_names() {
    embedded::with(|py| {
        let code = c"import builtins, types
connected = types.SimpleNamespace(check=lambda: None, streams=[])
def connect(config, host):
    builtins.connected_for = host
    return connected
";
        let globals = PyDict::new(py);
        py.run(code, Some(&globals), None).unwrap();
        let connect = globals.get_item("connect").unwrap().unbind();
        let made =
            NativeSourceFactory::new("io.example.py", "1.0.0".to_owned(), "{}", false, connect);
        let source_factory = made.unwrap().factory();
        let runtime = runtime::get(py).unwrap();
        py.detach(|| {
            runtime.block_on(async {
                let serving = ConnectContext::serving("host-a");
                source_factory
                    .connect(serde_json::json!({}), serving)
                    .await
                    .unwrap();
            });
        });
        let host: String = py
            .eval(c"__import__('builtins').connected_for", None, None)
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(host, "host-a");
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
            NativeSourceFactory::new("not an id", "1".to_owned(), "{}", false, py.None())
                .err()
                .expect("refused")
                .get_type(py)
                .name()
                .unwrap()
                .to_string(),
            "ValueError"
        );
        assert!(
            NativeSourceFactory::new(
                "io.example.py",
                "1".to_owned(),
                "not json",
                false,
                py.None()
            )
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
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'natural', 'partitioned': False, 'stated': False, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}))
streams = [stream]
def failing():
    raise PermissionError('no access')
connected = types.SimpleNamespace(check=failing, streams=streams)
connect = lambda config, host: connected";
        let globals = PyDict::new(py);
        py.run(
            &std::ffi::CString::new(format!("import json, types\n{body}")).unwrap(),
            Some(&globals),
            None,
        )
        .unwrap();
        let connect = globals.get_item("connect").unwrap().unbind();
        let source_factory =
            NativeSourceFactory::new("io.example.py", "1".to_owned(), "{}", false, connect)
                .unwrap();
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
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], spec='not json')
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
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'on_demand', 'partitioned': False, 'stated': False, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}))
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
fn a_plan_reads_committed_cursors_only_for_partitions_that_are_told_them() {
    embedded::with(|py| {
        for stated in [false, true] {
            let body = format!(
                "\
import builtins
def read(partition, cursor, context):
    yield {{'n': 1}}
def partitions(phase, positions, generation, completed):
    builtins.told = (phase, positions, generation, completed)
    return [('b', False)]
stream = types.SimpleNamespace(read=read, partitions=partitions, spec=json.dumps({{
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'], 'checkpointing': 'natural', 'partitioned': True, 'stated': {}, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 2}}))
streams = [stream]",
                if stated { "True" } else { "False" }
            );
            let connect = Arc::new(connect(py, &body));
            let runtime = runtime::get(py).unwrap();
            let planned = py.detach(|| {
                runtime.block_on(async {
                    let source = super::connected(connect, serde_json::json!({}), None)
                        .await
                        .unwrap();
                    // A cursor in another format, of a partition the plan no longer names.
                    let older =
                        rdlt_connector::Cursor::encode(1, &serde_json::json!({ "after": 1 }))
                            .unwrap();
                    let state = StreamState {
                        phase: 3,
                        partitions: BTreeMap::from([(
                            PartitionId::parse("a").unwrap(),
                            PartitionState::Cursor(older),
                        )]),
                        generation: Some(GenerationId(7)),
                        completed: vec![GenerationId(5), GenerationId(6)],
                    };
                    source.streams[0].partitions(&source, &state).await
                })
            });
            if stated {
                let refused = planned.expect_err("a cursor of another format");
                assert_eq!(refused.code(), Some("cursor_version"));
                continue;
            }
            assert_eq!(planned.expect("planned").len(), 1);
            let told: (u16, Vec<super::Position>, Option<u64>, Vec<u64>) = py
                .eval(c"__import__('builtins').told", None, None)
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(told, (3, Vec::new(), Some(7), vec![5, 6]));
        }
    });
}

#[test]
fn a_partitioned_stream_is_told_where_each_partition_stands_as_it_plans() {
    embedded::with(|py| {
        let body = "\
import builtins
builtins.planned = getattr(builtins, 'planned', [])
import rdlt._native as n
def read(partition, cursor, context):
    if cursor is None:
        yield {'n': partition}
        if partition == 'b':
            yield n.Checkpoint(json.dumps({'after': 7}))
def partitions(phase, positions, generation, completed):
    builtins.planned.append((phase, sorted(positions)))
    return [('a', False), ('b', False)]
stream = types.SimpleNamespace(read=read, partitions=partitions, spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'], 'checkpointing': 'natural', 'partitioned': True, 'stated': True, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}))
streams = [stream]";
        for _ in 0..2 {
            let (_, error) = loaded(py, "python-planned", "incremental", body);
            assert_eq!(error, None);
        }
        let planned: Vec<(u16, Vec<super::Position>)> = py
            .eval(c"__import__('builtins').planned", None, None)
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(planned[0], (0, Vec::new()));
        let after: serde_json::Value = serde_json::json!({ "after": 7 });
        let (phase, positions) = planned.last().expect("the second run planned");
        assert_eq!(*phase, 0);
        assert_eq!(positions[0], ("a".to_owned(), true, None));
        assert_eq!((&positions[1].0, positions[1].1), (&"b".to_owned(), false));
        let told: serde_json::Value =
            serde_json::from_str(positions[1].2.as_deref().expect("a cursor")).unwrap();
        assert_eq!(told, after);
    });
}

#[test]
fn a_stream_s_cursors_are_in_its_format_and_one_of_another_is_refused() {
    embedded::with(|py| {
        let body = "\
import rdlt._native as n
def read(partition, cursor, context):
    yield n.Checkpoint(json.dumps({'after': json.loads(cursor)['after'] + 1}))
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'], 'checkpointing': 'natural', 'partitioned': False, 'stated': False, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 2}))
streams = [stream]";
        let source_factory = factory(py, body).factory();
        let runtime = runtime::get(py).unwrap();
        py.detach(|| {
            runtime.block_on(async {
                let source = source_factory
                    .connect(serde_json::json!({}), ConnectContext::new())
                    .await
                    .unwrap();
                let rows = StreamName::new("rows").unwrap();
                let at = |version| {
                    rdlt_connector::Cursor::encode(version, &serde_json::json!({ "after": 1 }))
                        .unwrap()
                };
                let (sink, _feed) = partition_channel(std::num::NonZeroUsize::MIN);
                let older = ReadRequest::new(rows.clone(), Partition::single(), Some(at(1)));
                let refused = source.read(older, sink).await.expect_err("refused");
                assert_eq!(refused.code(), Some("cursor_version"));
                let (sink, mut feed) = partition_channel(std::num::NonZeroUsize::MIN);
                let current = ReadRequest::new(rows, Partition::single(), Some(at(2)));
                let (read, event) = tokio::join!(source.read(current, sink), feed.recv());
                read.unwrap();
                let Some(SourceEvent::Checkpoint { cursor, .. }) = event else {
                    panic!("a checkpoint, not {event:?}");
                };
                assert_eq!(cursor.version(), 2);
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
stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full'], 'checkpointing': 'natural', 'partitioned': False, 'stated': False, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}))
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

const UNSPLIT: &str =
    "stream = types.SimpleNamespace(read=read, partitions=lambda *state: [], spec=json.dumps({
    'name': 'rows', 'primary_key': None, 'read_modes': ['full', 'incremental'],
    'checkpointing': 'natural', 'partitioned': False, 'stated': False, 'replayable': True, 'committed': False, 'acknowledged': False, 'cursor_version': 1}))
streams = [stream]";

#[test]
fn an_incremental_read_that_pushes_rows_and_never_checkpoints_fails_and_a_full_one_loads_them() {
    embedded::with(|py| {
        for (store, read) in [
            ("no-checkpoint-rows", "yield {'n': 1}"),
            (
                "no-checkpoint-batch",
                "yield __import__('pyarrow').record_batch({'n': [1]})",
            ),
        ] {
            let body = format!("def read(partition, cursor, context):\n    {read}\n{UNSPLIT}");
            let (_, error) = loaded(py, store, "incremental", &body);
            let report: serde_json::Value =
                serde_json::from_str(&error.expect("the run failed")).unwrap();
            assert_eq!(report["code"], "checkpoint_missing", "{read}");
            assert_eq!(report["kind"], "source", "{read}");
            let full = format!("{store}-full");
            let (_, error) = loaded(py, &full, "full", &body);
            assert_eq!(error, None, "{read}");
            assert_eq!(published_rows(&full, "rows"), 1, "{read}");
        }
    });
}

#[test]
fn an_incremental_read_that_pushes_nothing_ends_clean() {
    embedded::with(|py| {
        let body = format!(
            "def read(partition, cursor, context):\n    \
             yield __import__('pyarrow').record_batch({{'n': __import__('pyarrow').array([], __import__('pyarrow').int64())}})\n{UNSPLIT}"
        );
        let (_, error) = loaded(py, "no-checkpoint-empty", "incremental", &body);
        assert_eq!(error, None);
    });
}
