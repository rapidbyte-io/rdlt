use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use pyo3::types::PyAnyMethods as _;
use pyo3::{Py, PyAny, Python};
use rdlt_connector::{ConnectorErrorKind, Partition, PartitionId};
use tokio::sync::mpsc;

use super::{PartitionOut, Pushed, Reading};
use crate::embedded;
use crate::json::JsonLimits;
use crate::runtime;
use crate::source::context::{NativeStreamContext, Told};

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
    let (context, _due) = NativeStreamContext::new(&Partition::single(), Told::default());
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
                Pushed::Replan => "replan",
                Pushed::Behind { .. } => "behind",
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
            embedded::with(|py| {
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
            embedded::with(|py| {
                py.eval(c"__import__('builtins').closed.wait(10)", None, None)
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            })
        });
        assert!(
            closed,
            "the generator was not closed once its read had gone"
        );
    });
}

#[test]
fn a_replan_and_a_lag_reach_the_read_among_what_it_yielded() {
    embedded::with(|py| {
        let reading = started(
            py,
            "import datetime\nfrom rdlt._native import Notice\ndef read(p, c, x):\n    yield {'n': 1}\n    x.notify(Notice.replan())\n    x.notify(Notice.behind(3, 10, datetime.timedelta(seconds=2)))\n    yield {'n': 2}",
        );
        let pushed = drained(py, reading);
        let [
            Pushed::Json(_),
            Pushed::Replan,
            Pushed::Behind { records, headroom },
            Pushed::Json(_),
        ] = &pushed[..]
        else {
            panic!("a row, a replan, a lag and a row: {pushed:?}")
        };
        assert_eq!(*records, 3);
        assert_eq!(
            *headroom,
            rdlt_connector::Headroom {
                bytes: Some(10),
                time: Some(Duration::from_secs(2)),
            }
        );
    });
}

#[test]
fn a_read_waiting_for_data_is_told_once_its_read_has_gone() {
    embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.gone = threading.Event()",
            None,
            None,
        )
        .unwrap();
        let reading = started(
            py,
            "import builtins, time\ndef read(p, c, x):\n    builtins.stopped_at_start = x.stopped()\n    yield {'n': 1}\n    while not x.stopped():\n        time.sleep(0.005)\n    builtins.gone.set()",
        );
        let runtime = runtime::get(py).unwrap();
        py.detach(|| {
            runtime.block_on(async move {
                let mut reading = reading;
                let first = tokio::time::timeout(Duration::from_secs(30), reading.next());
                assert!(matches!(
                    first.await.expect("within 30 s"),
                    Some(Pushed::Json(_))
                ));
            });
        });
        let gone = py.detach(|| {
            embedded::with(|py| {
                py.eval(c"__import__('builtins').gone.wait(10)", None, None)
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            })
        });
        assert!(gone, "the read never saw that it had gone");
        let stopped_at_start: bool = py
            .eval(c"__import__('builtins').stopped_at_start", None, None)
            .unwrap()
            .extract()
            .unwrap();
        assert!(
            !stopped_at_start,
            "a read the engine still reads was told it stopped"
        );
    });
}

#[test]
fn a_read_is_told_what_its_context_says_a_checkpoint_is_due() {
    embedded::with(|py| {
        let (context, due) = NativeStreamContext::new(&Partition::single(), Told::default());
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
        let (context, _due) = NativeStreamContext::new(
            &parted,
            Told {
                follows: true,
                resumes: true,
                excluded: vec!["secret".to_owned()],
                limits: vec![("nesting_depth", 64)],
            },
        );
        let context = Py::new(py, context).unwrap().into_bound(py);
        let partition: String = context.getattr("partition").unwrap().extract().unwrap();
        let unbounded: bool = context.getattr("unbounded").unwrap().extract().unwrap();
        let follows: bool = context.getattr("follows").unwrap().extract().unwrap();
        let resumes: bool = context.getattr("resumes").unwrap().extract().unwrap();
        let excluded: Vec<String> = context.getattr("excluded").unwrap().extract().unwrap();
        let limits: Vec<(String, u64)> = context.getattr("limits").unwrap().extract().unwrap();
        assert_eq!(partition, "p-1");
        assert!(unbounded);
        assert!(follows);
        assert!(resumes);
        assert_eq!(excluded, ["secret"]);
        assert_eq!(limits, [("nesting_depth".to_owned(), 64)]);

        let ending = Partition::new(PartitionId::whole());
        let (context, _due) = NativeStreamContext::new(&ending, Told::default());
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

/// Whether the socket `watched` names was readable within `seconds`.
fn readable(py: Python<'_>, watched: i32, seconds: u8) -> bool {
    let code = format!("__import__('select').select([{watched}], [], [], {seconds})[0]");
    let code = std::ffi::CString::new(code).unwrap();
    py.eval(&code, None, None).unwrap().is_truthy().unwrap()
}

#[test]
fn an_async_send_answers_at_once_and_the_runtime_sends_what_found_no_room() {
    embedded::with(|py| {
        let (sent, mut received) = mpsc::channel(1);
        let outlet = super::Outlet::default();
        outlet.open(sent);
        let out = PartitionOut {
            sent: outlet,
            limits: LIMITS,
            told: Arc::new(AtomicBool::new(false)),
            room: parking_lot::Mutex::default(),
        };
        let watched = out.watched().unwrap();
        let row = |n: u8| {
            let code = std::ffi::CString::new(format!("{{'n': {n}}}")).unwrap();
            py.eval(&code, None, None).unwrap()
        };
        let notice = py
            .import("rdlt._native")
            .unwrap()
            .getattr("Notice")
            .unwrap()
            .call_method0("replan")
            .unwrap();
        let runtime = runtime::get(py).unwrap();
        let mut taken = || {
            py.detach(|| runtime.block_on(received.recv()))
                .map(|pushed| kind(&pushed))
        };

        assert_eq!(out.send(py, &row(1)), Some(true));
        assert_eq!(out.send(py, &row(2)), None, "the channel is full");
        assert_eq!(out.resume(py), None, "the row still waits for room");
        assert_eq!(taken(), Some("json"));
        assert!(
            readable(py, watched, 5),
            "the loop is woken once the row was sent"
        );
        out.woken();
        assert!(
            !readable(py, watched, 0),
            "a wake taken wakes the loop no more"
        );
        assert_eq!(out.resume(py), Some(true));
        assert_eq!(taken(), Some("json"));

        assert_eq!(out.send(py, &row(3)), Some(true));
        assert_eq!(
            out.send(py, &notice),
            None,
            "a notice is not taken without room"
        );
        assert_eq!(taken(), Some("json"));
        assert!(readable(py, watched, 5), "the loop is woken once room came");
        out.woken();
        assert!(
            !readable(py, watched, 0),
            "a wake taken wakes the loop no more"
        );
        assert_eq!(out.send(py, &notice), Some(true));
        assert_eq!(taken(), Some("replan"));

        drop(received);
        assert_eq!(out.send(py, &row(4)), Some(false), "the read has gone");
    });
}

/// What a push holds in the tests whose rows span several pushes: one row of theirs, not two.
const ROW_A_PUSH: JsonLimits = JsonLimits {
    push_bytes: 64,
    depth: 64,
};

/// A task that counts its turns of the event loop it runs on, in `builtins.ticks`.
const TICKING: &str = "import asyncio, builtins
builtins.ticks = 0
async def ticking():
    while True:
        builtins.ticks += 1
        await asyncio.sleep(0)
";

fn started_within(py: Python<'_>, body: &str, limits: JsonLimits) -> Reading {
    let (context, _due) = NativeStreamContext::new(&Partition::single(), Told::default());
    let stream = Arc::new(stream(py, body));
    let whole = "whole".to_owned();
    Reading::start("rdlt-test", stream, whole, None, context, limits).expect("it starts")
}

/// What `builtins.<name>` holds, read from this thread.
fn builtin<T>(name: &str) -> T
where
    T: for<'a, 'py> pyo3::FromPyObject<'a, 'py>,
{
    embedded::with(|py| {
        let code = std::ffi::CString::new(format!("__import__('builtins').{name}")).unwrap();
        py.eval(&code, None, None)
            .unwrap()
            .extract()
            .unwrap_or_else(|_| panic!("builtins.{name} reads"))
    })
}

/// Whether `condition`, a Python expression, came true within 5 s, the GIL released between asks.
async fn came_true(condition: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !builtin::<bool>(condition) {
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    true
}

/// The `item` pushes of a single item `reading` sends, and the read; how many were taken where
/// the loop did not turn within 5 s.
///
/// Each push but the first and the last is taken only once the read's ticking task has turned
/// since the previous take: the item's next push then waits for room, and the loop with it. Once
/// the last is in the channel the read may end, and its loop with it.
fn taken_as_the_loop_turns(
    py: Python<'_>,
    mut reading: Reading,
    item: usize,
) -> Result<(Vec<Pushed>, Reading), usize> {
    let runtime = runtime::get(py).unwrap();
    py.detach(|| {
        runtime.block_on(async move {
            let mut taken = Vec::new();
            let mut seen: Option<u64> = None;
            while taken.len() < item {
                if let Some(seen) = seen
                    && taken.len() + 1 < item
                    && !came_true(&format!("ticks > {seen}")).await
                {
                    return Err(taken.len());
                }
                let next = tokio::time::timeout(Duration::from_secs(30), reading.next());
                let next = next.await.expect("within 30 s");
                let next = next.unwrap_or_else(|| panic!("the read ended at {}", taken.len()));
                seen = Some(builtin("ticks"));
                taken.push(next);
            }
            Ok((taken, reading))
        })
    })
}

fn kind(pushed: &Pushed) -> &'static str {
    match pushed {
        Pushed::Json(_) => "json",
        Pushed::Batch(_) => "batch",
        Pushed::Checkpoint(_) => "checkpoint",
        Pushed::Replan => "replan",
        Pushed::Behind { .. } => "behind",
        Pushed::Failed(_) => "failed",
    }
}

#[test]
fn an_async_read_s_event_loop_runs_while_an_item_s_pushes_wait_for_the_engine() {
    embedded::with(|py| {
        let shapes = [
            (
                "an Arrow stream",
                "import pyarrow as pa
batches = [pa.record_batch({'n': [n]}) for n in range(6)]
item = pa.RecordBatchReader.from_batches(batches[0].schema, batches)",
                LIMITS,
                "batch",
            ),
            (
                "a list of rows",
                "item = [{'n': n, 'pad': 'x' * 20} for n in range(6)]",
                ROW_A_PUSH,
                "json",
            ),
        ];
        let mut held = Vec::new();
        for (shape, item, limits, pushes) in shapes {
            let body = format!(
                "{TICKING}{item}\nasync def read(p, c, x):\n    builtins.ticker = asyncio.create_task(ticking())\n    yield item"
            );
            let reading = started_within(py, &body, limits);
            match taken_as_the_loop_turns(py, reading, 6) {
                Ok((taken, reading)) => {
                    assert!(taken.iter().all(|pushed| kind(pushed) == pushes), "{shape}");
                    assert!(drained(py, reading).is_empty(), "{shape}: read to its end");
                }
                Err(taken) => held.push(format!("{shape}, after {taken} pushes")),
            }
        }
        assert!(
            held.is_empty(),
            "the read's event loop was held while the engine was slow: {held:?}"
        );
    });
}

#[test]
fn an_async_read_reads_an_arrow_stream_no_further_ahead_than_its_channel_holds_and_one_push() {
    embedded::with(|py| {
        let reading = started(
            py,
            "import builtins
import pyarrow as pa
builtins.made = 0
def batches():
    for n in range(40):
        builtins.made += 1
        yield pa.record_batch({'n': [n]})
async def read(p, c, x):
    yield pa.RecordBatchReader.from_batches(pa.schema([('n', pa.int64())]), batches())",
        );
        let runtime = runtime::get(py).unwrap();
        let ahead = py.detach(|| {
            runtime.block_on(async move {
                let mut reading = reading;
                let holds = reading.received.max_capacity();
                let mut taken = 0_usize;
                let mut furthest = 0_usize;
                while let Some(pushed) =
                    tokio::time::timeout(Duration::from_secs(30), reading.next())
                        .await
                        .expect("within 30 s")
                {
                    assert_eq!(kind(&pushed), "batch");
                    taken += 1;
                    // Time for a read that runs ahead to do so.
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    let made: usize = builtin("made");
                    furthest = furthest.max(made - taken);
                }
                assert_eq!(taken, 40);
                (furthest, holds)
            })
        });
        let (furthest, holds) = ahead;
        assert!(
            furthest <= holds + 1,
            "the stream was read {furthest} batches ahead of the engine"
        );
    });
}

#[test]
fn notices_said_during_and_after_a_multi_push_item_reach_the_engine_after_its_pushes() {
    embedded::with(|py| {
        let reading = started(
            py,
            "import asyncio, builtins
import pyarrow as pa
from rdlt.connector import Headroom, StreamContext
builtins.said_during = None
async def read(p, c, x):
    ctx = StreamContext(x)
    yielding = [False]
    async def noticing():
        await asyncio.sleep(0)
        builtins.said_during = yielding[0]
        ctx.replan()
    builtins.noticer = asyncio.create_task(noticing())
    batches = [pa.record_batch({'n': [n]}) for n in range(4)]
    yielding[0] = True
    yield pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    yielding[0] = False
    ctx.behind(2, Headroom())
    yield {'n': 4}
    await builtins.noticer",
        );
        let runtime = runtime::get(py).unwrap();
        // Nothing is taken until the notice was said, so the item waits for the engine meanwhile.
        py.detach(|| {
            runtime.block_on(async { came_true("said_during is not None").await });
        });
        let pushed = drained(py, reading);
        let said_during: Option<bool> = builtin("said_during");
        assert_eq!(
            said_during,
            Some(true),
            "the notice was said while the item's pushes waited for the engine"
        );
        let kinds: Vec<&str> = pushed.iter().map(kind).collect();
        assert_eq!(
            kinds,
            [
                "batch", "batch", "batch", "batch", "replan", "behind", "json"
            ]
        );
    });
}

#[test]
fn an_item_that_fails_part_way_fails_an_async_read_with_its_code() {
    embedded::with(|py| {
        let failing = [
            (
                "import pyarrow as pa
def batches():
    yield pa.record_batch({'n': [0]})
    yield pa.record_batch({'n': [1]})
    raise ValueError('torn')
item = pa.RecordBatchReader.from_batches(pa.schema([('n', pa.int64())]), batches())",
                LIMITS,
                "arrow_stream",
                ConnectorErrorKind::Data,
            ),
            (
                "item = [{'n': n, 'pad': 'x' * 20} for n in range(3)] + [{'n': float('nan')}]",
                ROW_A_PUSH,
                "json_not_finite",
                ConnectorErrorKind::Data,
            ),
        ];
        for (item, limits, code, error_kind) in failing {
            let body =
                format!("{item}\nasync def read(p, c, x):\n    yield item\n    yield {{'n': 9}}");
            let pushed = drained(py, started_within(py, &body, limits));
            let Some((Pushed::Failed(error), sent)) = pushed.split_last() else {
                panic!("{code}: a failure last: {pushed:?}")
            };
            assert!(!sent.is_empty(), "{code}: pushes sent before it failed");
            assert!(sent.iter().all(|pushed| kind(pushed) != "failed"), "{code}");
            assert_eq!(error.code(), Some(code));
            assert_eq!(error.kind(), error_kind, "{code}");
        }
    });
}

#[test]
fn an_async_read_whose_engine_stops_while_it_waits_for_room_ends_and_closes_its_loop() {
    embedded::with(|py| {
        let body = format!(
            "{TICKING}import threading
import pyarrow as pa
builtins.closed = threading.Event()
builtins.event_loop = None
def batches():
    for n in range(1000):
        yield pa.record_batch({{'n': [n]}})
async def read(p, c, x):
    builtins.event_loop = asyncio.get_running_loop()
    builtins.ticker = asyncio.create_task(ticking())
    try:
        yield pa.RecordBatchReader.from_batches(pa.schema([('n', pa.int64())]), batches())
    finally:
        builtins.closed.set()"
        );
        let reading = started(py, &body);
        let runtime = runtime::get(py).unwrap();
        py.detach(|| {
            runtime.block_on(async move {
                let mut reading = reading;
                let first = tokio::time::timeout(Duration::from_secs(30), reading.next());
                assert!(matches!(
                    first.await.expect("within 30 s"),
                    Some(Pushed::Batch(_))
                ));
                // The engine stops while the read waits for room, its loop then turning.
                let seen: u64 = builtin("ticks");
                came_true(&format!("ticks > {seen}")).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                drop(reading);
                assert!(
                    came_true("closed.is_set()").await,
                    "the read went on once its engine had stopped"
                );
                assert!(
                    came_true("event_loop.is_closed()").await,
                    "the read's event loop stayed open"
                );
            });
        });
    });
}

#[test]
fn an_async_read_cancelled_while_it_waits_for_room_says_so_and_leaves_its_channel_whole() {
    embedded::with(|py| {
        let reading = started(
            py,
            "import asyncio, builtins
async def read(p, c, x):
    read_task = asyncio.current_task()
    async def cancelling():
        await asyncio.sleep(0)
        read_task.cancel()
    yield {'n': 0}
    builtins.canceller = asyncio.create_task(cancelling())
    yield {'n': 1}
    yield {'n': 2}",
        );
        let runtime = runtime::get(py).unwrap();
        py.detach(|| {
            runtime.block_on(async move {
                let mut reading = reading;
                // The first push fills the channel, and the second waits for room until the
                // read is cancelled. Nothing is taken until the read has said how it ended, which
                // lets go of what waited first.
                let deadline = Instant::now() + Duration::from_secs(5);
                while !reading.told.load(Ordering::Acquire) {
                    assert!(Instant::now() < deadline, "the cancelled read never ended");
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let mut pushed = Vec::new();
                while let Some(next) = tokio::time::timeout(Duration::from_secs(30), reading.next())
                    .await
                    .expect("within 30 s")
                {
                    pushed.push(next);
                }
                let [Pushed::Json(_), Pushed::Failed(error)] = &pushed[..] else {
                    panic!("the first row, then the cancellation: {pushed:?}")
                };
                assert_eq!(error.code(), Some("asyncio.exceptions.cancelled_error"));
                assert_eq!(
                    reading.received.capacity(),
                    reading.received.max_capacity(),
                    "a cancelled wait for room kept a slot of the channel"
                );
            });
        });
    });
}

/// How many descriptors the process holds open.
fn descriptors() -> usize {
    let listed = if std::path::Path::new("/proc/self/fd").exists() {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    std::fs::read_dir(listed).unwrap().count()
}

/// Whether the process held no more than `before` descriptors within 5 s: what the runtime lets
/// go of once its task is aborted, it lets go of in a moment.
fn descriptors_back_to(py: Python<'_>, before: usize) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    py.detach(|| {
        while descriptors() > before {
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        true
    })
}

/// Records, in `builtins.unwatched`, whether each socket a loop stops watching was open then.
const UNWATCHING: &std::ffi::CStr = c"import asyncio.selector_events as events, builtins, os
builtins.unwatched = []
removing = events.BaseSelectorEventLoop.remove_reader
def remove_reader(loop, fd):
    try:
        os.fstat(fd)
    except OSError:
        builtins.unwatched.append(False)
    else:
        builtins.unwatched.append(True)
    return removing(loop, fd)
events.BaseSelectorEventLoop.remove_reader = remove_reader
";

/// Each way an async read ends, and a read that ends so.
const ENDINGS: [(&str, &str); 5] = [
    (
        "finished",
        "async def read(p, c, x):\n    yield {'n': 1}\n    yield {'n': 2}",
    ),
    (
        "failed",
        "async def read(p, c, x):\n    yield {'n': 1}\n    raise ValueError('bad page')",
    ),
    (
        "failed part-way through an item",
        "import pyarrow as pa
def batches():
    yield pa.record_batch({'n': [0]})
    yield pa.record_batch({'n': [1]})
    raise ValueError('torn')
async def read(p, c, x):
    yield pa.RecordBatchReader.from_batches(pa.schema([('n', pa.int64())]), batches())",
    ),
    (
        "cancelled while it waited",
        "import asyncio, builtins
builtins.event_loop = None
async def read(p, c, x):
    builtins.event_loop = asyncio.get_running_loop()
    read_task = asyncio.current_task()
    async def cancelling():
        await asyncio.sleep(0)
        read_task.cancel()
    yield {'n': 0}
    builtins.canceller = asyncio.create_task(cancelling())
    yield {'n': 1}",
    ),
    (
        "stopped by its engine while it waited",
        "import asyncio, builtins
import pyarrow as pa
builtins.event_loop = None
def batches():
    for n in range(1000):
        yield pa.record_batch({'n': [n]})
async def read(p, c, x):
    builtins.event_loop = asyncio.get_running_loop()
    yield pa.RecordBatchReader.from_batches(pa.schema([('n', pa.int64())]), batches())",
    ),
];

/// Has the engine read `reading` as `ending` says it ends: to its end, once its loop has closed
/// where the read cancels itself, or a push and then no more where its engine stops.
fn ended_as(py: Python<'_>, ending: &str, mut reading: Reading) {
    let runtime = runtime::get(py).unwrap();
    py.detach(|| {
        runtime.block_on(async {
            if ending.starts_with("cancelled") {
                let closed = "event_loop is not None and event_loop.is_closed()";
                assert!(came_true(closed).await, "{ending}");
            }
            if ending.starts_with("stopped") {
                let first = tokio::time::timeout(Duration::from_secs(30), reading.next());
                assert!(first.await.expect("within 30 s").is_some());
                drop(reading);
                assert!(came_true("event_loop.is_closed()").await, "{ending}");
                return;
            }
            while tokio::time::timeout(Duration::from_secs(30), reading.next())
                .await
                .expect("within 30 s")
                .is_some()
            {}
        });
    });
}

#[test]
fn an_async_read_unwatches_and_closes_the_socket_it_waits_for_room_with_however_it_ends() {
    embedded::with(|py| {
        // Everything a first read makes for good, the runtime's, the interpreter's and pyarrow's,
        // is open.
        let warming = "import pyarrow as pa
async def read(p, c, x):
    yield pa.Table.from_batches([pa.record_batch({'n': [1]})] * 3)";
        drop(drained(py, started(py, warming)));
        py.run(UNWATCHING, None, None).unwrap();
        let before = descriptors();
        let mut left = Vec::new();
        for (unwatched_before, (ending, body)) in ENDINGS.into_iter().enumerate() {
            ended_as(py, ending, started(py, body));
            if !descriptors_back_to(py, before) {
                left.push(format!("{ending}: {} open", descriptors() - before));
            }
            let unwatched: Vec<bool> = builtin("unwatched");
            assert_eq!(
                unwatched.len(),
                unwatched_before + 1,
                "{ending}: unwatched once"
            );
            assert_eq!(
                unwatched.last(),
                Some(&true),
                "{ending}: unwatched while open"
            );
        }
        assert!(left.is_empty(), "descriptors left open: {left:?}");
    });
}

#[test]
fn an_async_read_cancelled_mid_item_makes_no_more_of_it_while_it_cleans_up() {
    embedded::with(|py| {
        let reading = started(
            py,
            "import asyncio, builtins
import pyarrow as pa
builtins.made = 0
builtins.cancelled_at = None
def batches():
    for n in range(1000):
        builtins.made += 1
        yield pa.record_batch({'n': [n]})
async def read(p, c, x):
    read_task = asyncio.current_task()
    async def cancelling():
        await asyncio.sleep(0)
        read_task.cancel()
        builtins.cancelled_at = builtins.made
    builtins.canceller = asyncio.create_task(cancelling())
    try:
        yield pa.RecordBatchReader.from_batches(pa.schema([('n', pa.int64())]), batches())
    finally:
        # A cleanup that awaits, as closing a client session does, while the engine takes on.
        await asyncio.sleep(0.2)",
        );
        let runtime = runtime::get(py).unwrap();
        let pushed = py.detach(|| {
            runtime.block_on(async move {
                let mut reading = reading;
                // Nothing is taken until the read is cancelled, its item waiting for room.
                assert!(came_true("cancelled_at is not None").await);
                let mut pushed = Vec::new();
                while let Some(next) = tokio::time::timeout(Duration::from_secs(30), reading.next())
                    .await
                    .expect("within 30 s")
                {
                    pushed.push(next);
                }
                pushed
            })
        });
        let Some((Pushed::Failed(error), _)) = pushed.split_last() else {
            panic!("the cancellation last: {pushed:?}")
        };
        assert_eq!(error.code(), Some("asyncio.exceptions.cancelled_error"));
        let made: usize = builtin("made");
        let cancelled_at: usize = builtin("cancelled_at");
        assert_eq!(made, cancelled_at, "batches were made after the cancel");
    });
}

#[test]
fn a_notice_said_while_an_item_is_made_reaches_the_engine_after_the_whole_item() {
    embedded::with(|py| {
        // Said as the item's first batch is made, in the send that begins it, and as its third
        // is, once the item has waited for room.
        let said_at = [
            ("first", 0, "    pass"),
            (
                "waited",
                2,
                "    builtins.ticker = asyncio.create_task(ticking())",
            ),
        ];
        for (case, at, starting) in said_at {
            let body = format!(
                "{TICKING}import pyarrow as pa
from rdlt.connector import Headroom, StreamContext
def batches(ctx):
    for n in range(4):
        if n == {at}:
            ctx.behind(n, Headroom())
        yield pa.record_batch({{'n': [n]}})
async def read(p, c, x):
{starting}
    ctx = StreamContext(x)
    yield pa.RecordBatchReader.from_batches(pa.schema([('n', pa.int64())]), batches(ctx))
    yield {{'n': 9}}"
            );
            let reading = started(py, &body);
            if case == "waited" {
                // Nothing is taken until the item waits for room, its loop then turning.
                let runtime = runtime::get(py).unwrap();
                py.detach(|| {
                    runtime.block_on(async {
                        assert!(came_true("ticks > 0").await, "{case}");
                    });
                });
            }
            let kinds: Vec<&str> = drained(py, reading).iter().map(kind).collect();
            assert_eq!(
                kinds,
                ["batch", "batch", "batch", "batch", "behind", "json"],
                "{case}"
            );
        }
    });
}
