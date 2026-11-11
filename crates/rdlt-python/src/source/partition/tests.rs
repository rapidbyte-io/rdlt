use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use pyo3::types::PyAnyMethods as _;
use pyo3::{Py, PyAny, Python};
use rdlt_connector::{ConnectorError, ConnectorErrorKind, Partition, PartitionId};
use tokio::sync::mpsc;

use super::{PartitionOut, Pushed, Reading};
use crate::embedded;
use crate::json::JsonLimits;
use crate::runtime;
use crate::source::context::NativeStreamContext;

const LIMITS: JsonLimits = JsonLimits {
    push_bytes: 1 << 20,
    depth: 64,
};

/// A stream object whose `read` returns what `body`, the source of a generator function named
/// `read(partition, cursor, context)`, yields.
fn stream(py: Python<'_>, body: &str) -> Py<PyAny> {
    let code = format!("import types\n{body}\nstream = types.SimpleNamespace(read=read)\n");
    let globals = pyo3::types::PyDict::new(py);
    py.run(&std::ffi::CString::new(code).unwrap(), Some(&globals), None)
        .unwrap();
    globals.get_item("stream").unwrap().unbind()
}

fn started(py: Python<'_>, body: &str) -> Reading {
    let (context, _due) = NativeStreamContext::new(&Partition::single(), false, false);
    let stream = Arc::new(stream(py, body));
    let whole = "whole".to_owned();
    Reading::start("rdlt-test", stream, whole, None, context, LIMITS).expect("it starts")
}

fn drained(py: Python<'_>, mut reading: Reading) -> Vec<Pushed> {
    let runtime = runtime::get(py).unwrap();
    py.detach(|| {
        runtime.block_on(async {
            let mut all = Vec::new();
            while let Some(pushed) = tokio::time::timeout(Duration::from_secs(30), reading.next())
                .await
                .expect("within 30 s")
            {
                all.push(pushed);
            }
            all
        })
    })
}

#[test]
fn rows_batches_and_checkpoints_arrive_in_the_order_they_were_yielded() {
    embedded::with(|py| {
        let reading = started(
            py,
            "import pyarrow as pa\nimport rdlt._native as n\ndef read(p, c, x):\n    yield {'n': 1}\n    yield n.Checkpoint('{\"after\": 1}')\n    yield pa.record_batch({'n': [2, 3]})\n    yield [{'n': 4}, {'n': 5}]",
        );
        let kinds: Vec<&str> = drained(py, reading)
            .iter()
            .map(|pushed| match pushed {
                Pushed::Json(_) => "json",
                Pushed::Batch(_) => "batch",
                Pushed::Checkpoint(_) => "checkpoint",
                Pushed::Failed(error) => panic!("failed: {error}"),
            })
            .collect();
        assert_eq!(kinds, ["json", "checkpoint", "batch", "json"]);
    });
}

#[test]
fn a_stream_producer_arrives_one_batch_at_a_time() {
    embedded::with(|py| {
        let reading = started(
            py,
            "import pyarrow as pa\ndef read(p, c, x):\n    yield pa.Table.from_batches([pa.record_batch({'n': [1]})] * 3)",
        );
        assert_eq!(drained(py, reading).len(), 3);
    });
}

#[test]
fn an_async_generator_runs_on_its_thread_s_own_loop() {
    embedded::with(|py| {
        let reading = started(
            py,
            "import asyncio\nasync def read(p, c, x):\n    for n in range(3):\n        await asyncio.sleep(0)\n        yield {'n': n}",
        );
        assert_eq!(drained(py, reading).len(), 3);
    });
}

#[test]
fn an_exception_arrives_as_its_failure_and_ends_the_read() {
    embedded::with(|py| {
        let reading = started(
            py,
            "def read(p, c, x):\n    yield {'n': 1}\n    raise ValueError('bad page')\n    yield {'n': 2}",
        );
        let pushed = drained(py, reading);
        assert_eq!(pushed.len(), 2);
        let Pushed::Failed(error) = &pushed[1] else {
            panic!("a failure")
        };
        assert_eq!(error.code(), Some("value_error"));
    });
}

#[test]
fn what_an_async_generator_raises_arrives_as_its_failure() {
    embedded::with(|py| {
        let reading = started(
            py,
            "async def read(p, c, x):\n    yield {'n': 1}\n    raise ConnectionResetError('gone')",
        );
        let pushed = drained(py, reading);
        assert_eq!(pushed.len(), 2);
        let Pushed::Failed(error) = &pushed[1] else {
            panic!("a failure")
        };
        assert_eq!(error.code(), Some("connection_reset_error"));
        assert_eq!(error.kind(), ConnectorErrorKind::Transient);
    });
}

#[test]
fn a_base_exception_fails_the_read_and_reads_as_no_end() {
    embedded::with(|py| {
        let reading = started(
            py,
            "def read(p, c, x):\n    yield {'n': 1}\n    raise SystemExit(3)",
        );
        let pushed = drained(py, reading);
        assert_eq!(pushed.len(), 2, "the failure ends what the read sends");
        let Pushed::Failed(error) = &pushed[1] else {
            panic!("a failure")
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Internal);
        assert_eq!(error.code(), Some("system_exit"));
    });
}

#[test]
fn what_cannot_be_pushed_fails_the_read_with_its_code() {
    embedded::with(|py| {
        let reading = started(py, "def read(p, c, x):\n    yield 7");
        let pushed = drained(py, reading);
        let [Pushed::Failed(error)] = &pushed[..] else {
            panic!("one failure: {pushed:?}")
        };
        assert_eq!(error.code(), Some("json_not_object"));
    });
}

#[test]
fn a_read_that_cannot_be_opened_fails_with_what_opening_raised() {
    embedded::with(|py| {
        let reading = started(py, "def read(p, c, x):\n    raise KeyError('nope')");
        let pushed = drained(py, reading);
        let [Pushed::Failed(error)] = &pushed[..] else {
            panic!("one failure: {pushed:?}")
        };
        assert_eq!(error.code(), Some("key_error"));
    });
}

#[test]
fn what_opens_to_something_not_iterable_fails_the_read_with_its_error() {
    embedded::with(|py| {
        let reading = started(py, "def read(p, c, x):\n    return 7");
        let pushed = drained(py, reading);
        let [Pushed::Failed(error)] = &pushed[..] else {
            panic!("one failure: {pushed:?}")
        };
        assert_eq!(error.code(), Some("type_error"));
    });
}

#[test]
fn a_read_that_says_it_finished_ends_though_its_out_is_still_held() {
    embedded::with(|py| {
        // A finished read's channel closes at once, whatever Python's collector keeps alive.
        py.run(c"import builtins; builtins.held = []", None, None)
            .unwrap();
        let reading = started(
            py,
            "import builtins\ndef read(p, c, x):\n    builtins.held.append(x)\n    yield {'n': 1}",
        );
        assert_eq!(drained(py, reading).len(), 1);
    });
}

#[test]
fn a_thread_that_ends_without_saying_how_fails_the_read() {
    embedded::with(|py| {
        let (sent, received) = mpsc::channel(1);
        drop(sent);
        let reading = Reading {
            received,
            told: Arc::new(AtomicBool::new(false)),
        };
        let pushed = drained(py, reading);
        let [Pushed::Failed(error)] = &pushed[..] else {
            panic!("one failure: {pushed:?}")
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    });
}

#[test]
fn a_yield_waits_for_the_read_to_take_the_one_before_and_frees_the_gil_meanwhile() {
    embedded::with(|py| {
        py.run(c"import builtins; builtins.produced = 0", None, None)
            .unwrap();
        let reading = started(
            py,
            "import builtins\ndef read(p, c, x):\n    while True:\n        builtins.produced += 1\n        yield {'n': builtins.produced}",
        );
        let produced = || {
            Python::attach(|py| {
                py.eval(c"__import__('builtins').produced", None, None)
                    .unwrap()
                    .extract::<u64>()
                    .unwrap()
            })
        };
        py.detach(|| {
            let deadline = Instant::now() + Duration::from_secs(10);
            while produced() < 2 {
                assert!(
                    Instant::now() < deadline,
                    "the generator never yielded twice"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            std::thread::sleep(Duration::from_millis(200));
            // One push held by the channel, one waiting to be sent: the generator is not asked
            // again.
            assert_eq!(produced(), 2);
        });
        drop(reading);
    });
}

#[test]
fn a_stopped_read_returns_while_its_generator_is_blocked() {
    embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.release = threading.Event(); builtins.closed = threading.Event()",
            None,
            None,
        )
        .unwrap();
        let reading = started(
            py,
            "import builtins\ndef read(p, c, x):\n    try:\n        yield {'n': 1}\n        builtins.release.wait()\n        yield {'n': 2}\n    finally:\n        builtins.closed.set()",
        );
        let runtime = runtime::get(py).unwrap();
        let dropped_at = py.detach(|| {
            runtime.block_on(async move {
                let mut reading = reading;
                let first = tokio::time::timeout(Duration::from_secs(30), reading.next());
                assert!(matches!(
                    first.await.expect("within 30 s"),
                    Some(Pushed::Json(_))
                ));
                let started = Instant::now();
                drop(reading);
                started.elapsed()
            })
        });
        assert!(
            dropped_at < Duration::from_secs(1),
            "the drop waited for the generator"
        );
        py.run(c"import builtins; builtins.release.set()", None, None)
            .unwrap();
        let closed = py.detach(|| {
            Python::attach(|py| {
                py.eval(c"__import__('builtins').closed.wait(10)", None, None)
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            })
        });
        assert!(
            closed,
            "the generator was not closed once its thread could end"
        );
    });
}

#[test]
fn a_read_is_told_what_its_context_says_a_checkpoint_is_due() {
    embedded::with(|py| {
        let (context, due) = NativeStreamContext::new(&Partition::single(), false, false);
        let context = Py::new(py, context).unwrap().into_bound(py);
        let is_due = || -> bool {
            context
                .call_method0("checkpoint_due")
                .unwrap()
                .extract()
                .unwrap()
        };
        assert!(!is_due());
        due.set(true);
        assert!(is_due());
    });
}

#[test]
fn a_dict_that_changes_while_it_is_written_fails_the_read_rather_than_the_process() {
    embedded::with(|py| {
        // Reading a value runs its `isoformat`, which grows the dict being written: PyO3's
        // iterator panics, which reaches the read as a `PanicException`.
        let reading = started(
            py,
            "import datetime\nclass Grows(datetime.date):\n    def isoformat(self):\n        row['grown'] = 1\n        return 'x'\nrow = {'a': Grows(2026, 1, 1)}\ndef read(p, c, x):\n    yield row",
        );
        let pushed = drained(py, reading);
        let [Pushed::Failed(error)] = &pushed[..] else {
            panic!("one failure: {pushed:?}")
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    });
}

#[test]
fn a_read_s_context_says_its_partition_and_whether_it_follows_and_resumes() {
    embedded::with(|py| {
        let parted = Partition::new(PartitionId::parse("p-1").unwrap()).unbounded();
        let (context, _due) = NativeStreamContext::new(&parted, true, true);
        let context = Py::new(py, context).unwrap().into_bound(py);
        let partition: String = context.getattr("partition").unwrap().extract().unwrap();
        let unbounded: bool = context.getattr("unbounded").unwrap().extract().unwrap();
        let follows: bool = context.getattr("follows").unwrap().extract().unwrap();
        let resumes: bool = context.getattr("resumes").unwrap().extract().unwrap();
        assert_eq!(partition, "p-1");
        assert!(unbounded);
        assert!(follows);
        assert!(resumes);

        let ending = Partition::new(PartitionId::whole());
        let (context, _due) = NativeStreamContext::new(&ending, false, false);
        let context = Py::new(py, context).unwrap().into_bound(py);
        let partition: String = context.getattr("partition").unwrap().extract().unwrap();
        let unbounded: bool = context.getattr("unbounded").unwrap().extract().unwrap();
        let follows: bool = context.getattr("follows").unwrap().extract().unwrap();
        let resumes: bool = context.getattr("resumes").unwrap().extract().unwrap();
        assert_eq!(partition, "whole");
        assert!(!unbounded);
        assert!(!follows);
        assert!(!resumes);
    });
}

#[test]
fn a_checkpoint_holds_its_cursor_and_refuses_what_is_not_json() {
    embedded::with(|py| {
        let namespace = pyo3::types::PyDict::new(py);
        let module = pyo3::types::PyModule::new(py, "checkpoints").unwrap();
        crate::register(&module).unwrap();
        namespace.set_item("n", module).unwrap();
        let built = py
            .eval(c"n.Checkpoint('{\"after\": 1}')", Some(&namespace), None)
            .unwrap();
        let cursor = built
            .cast::<crate::source::context::NativeCheckpoint>()
            .unwrap();
        assert_eq!(cursor.get().cursor().get(), "{\"after\": 1}");
        let refused = py
            .eval(c"n.Checkpoint('not json')", Some(&namespace), None)
            .expect_err("refused");
        assert!(refused.is_instance_of::<pyo3::exceptions::PyValueError>(py));
    });
}

#[test]
fn a_checkpoint_keeps_its_cursor_s_numbers_as_it_was_given_them() {
    embedded::with(|py| {
        let namespace = pyo3::types::PyDict::new(py);
        let module = pyo3::types::PyModule::new(py, "checkpoints").unwrap();
        crate::register(&module).unwrap();
        namespace.set_item("n", module).unwrap();
        let text = "{\"after\": 1208925819614629174706176, \"at\": 1801439850948199.0}";
        let built = py
            .eval(
                &std::ffi::CString::new(format!("n.Checkpoint({text:?})")).unwrap(),
                Some(&namespace),
                None,
            )
            .unwrap();
        let checkpoint = built
            .cast::<crate::source::context::NativeCheckpoint>()
            .unwrap();
        assert_eq!(checkpoint.get().cursor().get(), text);
    });
}

#[test]
fn a_read_has_room_until_its_channel_fills_and_again_once_it_has_gone() {
    let (sent, mut received) = mpsc::channel(1);
    let out = PartitionOut {
        sent: Mutex::new(Some(sent.clone())),
        limits: LIMITS,
        told: Arc::new(AtomicBool::new(false)),
    };
    let push = || Pushed::Failed(ConnectorError::internal("a push"));
    assert!(out.has_room());
    sent.try_send(push()).unwrap();
    assert!(!out.has_room());
    drop(received.try_recv().unwrap());
    assert!(out.has_room());
    sent.try_send(push()).unwrap();
    drop(received);
    assert!(
        out.has_room(),
        "a push to a read that has gone answers at once"
    );
    out.finished();
    assert!(out.has_room());
}
