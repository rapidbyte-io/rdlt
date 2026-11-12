use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array as _, BooleanArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use pyo3::types::PyAnyMethods as _;
use rdlt::Transform as _;
use rdlt_connector_reference::{ChangesSource, MemoryDestination};

use super::NativeTransform;
use crate::calls::tests::{GIVEN_BACK, Unpatch, Within};
use crate::{embedded, runtime};

fn input() -> RecordBatch {
    RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as _)]).unwrap()
}

fn native(
    py: pyo3::Python<'_>,
    code: &str,
    filters: bool,
    output: Option<&str>,
) -> NativeTransform {
    let function = py
        .eval(&std::ffi::CString::new(code).unwrap(), None, None)
        .unwrap()
        .unbind();
    let output = output.map(|code| {
        py.eval(&std::ffi::CString::new(code).unwrap(), None, None)
            .unwrap()
            .unbind()
    });
    NativeTransform::new(py, "under_test".to_owned(), function, filters, output).unwrap()
}

#[test]
fn a_transform_that_finds_no_daemon_thread_says_why_the_last_did_not_start() {
    embedded::with(|py| {
        py.run(
            c"import _thread, builtins, threading\n\
              builtins.entered = threading.Event()\n\
              builtins.leave = threading.Event()\n\
              builtins.started_by = _thread.start_new_thread\n\
              def refused(*_):\n    raise RuntimeError('no threads left')\n\
              _thread.start_new_thread = refused\n",
            None,
            None,
        )
    })
    .unwrap();
    let given_back = Unpatch(GIVEN_BACK);
    let (holding, quick) = embedded::with(|py| {
        let holding = "lambda batch: (__import__('builtins').entered.set(), __import__('builtins').leave.wait(30), batch)[-1]";
        (
            native(py, holding, false, None).transform(),
            native(py, "lambda batch: batch", false, None).transform(),
        )
    });
    std::thread::scope(|scope| {
        // Takes the only daemon thread, whose spare is refused before it calls.
        let held = scope.spawn(|| holding.apply(input()));
        let entered = embedded::with(|py| {
            py.eval(c"__import__('builtins').entered.wait(30)", None, None)
                .unwrap()
                .extract::<bool>()
                .unwrap()
        });
        assert!(entered, "the first call holds its daemon thread");
        let failed = quick.apply(input()).expect_err("no daemon thread is idle");
        assert!(failed.to_string().contains("no threads left"), "{failed}");
        // Gives starts back and lets the held call go; where an assertion above fails, the guard
        // does so as the scope unwinds, before the scope waits for that call.
        drop(given_back);
        held.join().unwrap().expect("the first call is answered");
    });
}

const DOUBLED: &str = "lambda batch: batch.append_column('doubled', __import__('pyarrow.compute', fromlist=['multiply']).multiply(batch.column('n'), 2))";

#[test]
fn a_transform_reshapes_a_batch_and_takes_its_output_from_an_empty_one() {
    let transform = embedded::with(|py| native(py, DOUBLED, false, None).transform());
    let output = transform
        .output(&input().schema())
        .expect("an output schema");
    assert_eq!(output.field(1).name(), "doubled");
    let applied = transform.apply(input()).expect("it applies");
    assert_eq!(applied.num_rows(), 4);
    let doubled = applied
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(doubled.values(), &[2, 4, 6, 8]);
    assert!(!transform.filters());
}

#[test]
fn a_batch_returned_as_it_came_shares_its_buffers_with_rust() {
    let transform = embedded::with(|py| native(py, "lambda batch: batch", false, None).transform());
    let batch = input();
    let address = batch.column(0).to_data().buffers()[0].as_ptr().addr();
    let applied = transform.apply(batch).expect("it applies");
    assert_eq!(
        applied.column(0).to_data().buffers()[0].as_ptr().addr(),
        address
    );
}

#[test]
fn a_given_output_is_taken_without_calling_the_function() {
    let transform = embedded::with(|py| {
        native(
            py,
            "lambda batch: 1 / 0",
            false,
            Some("__import__('pyarrow').schema([('m', __import__('pyarrow').int64())])"),
        )
        .transform()
    });
    let declared = Schema::new(vec![Field::new("m", DataType::Int64, true)]);
    assert_eq!(*transform.output(&input().schema()).unwrap(), declared);
}

#[test]
fn a_transform_is_known_by_its_name() {
    let (native_name, name) = embedded::with(|py| {
        let made = native(py, "lambda batch: batch", false, None);
        (made.name(), made.transform().name().to_owned())
    });
    assert_eq!(native_name, "under_test");
    assert_eq!(name, "under_test");
}

#[test]
fn a_filter_keeps_what_its_mask_says_and_reshapes_nothing() {
    let transform = embedded::with(|py| {
        native(
            py,
            "lambda batch: __import__('pyarrow.compute', fromlist=['greater']).greater(batch.column('n'), 2)",
            true,
            None,
        )
        .transform()
    });
    assert!(transform.filters());
    assert_eq!(
        *transform.output(&input().schema()).unwrap(),
        *input().schema()
    );
    assert_eq!(
        transform.keep(&input()).unwrap(),
        BooleanArray::from(vec![false, false, true, true])
    );
    assert_eq!(transform.apply(input()).unwrap(), input());
}

#[test]
fn an_exception_fails_with_its_type_message_and_traceback() {
    let transform =
        embedded::with(|py| native(py, "lambda batch: {}['missing']", false, None).transform());
    let error = transform.apply(input()).expect_err("it fails");
    assert!(error.to_string().contains("KeyError"), "{error}");
    let cause = std::error::Error::source(&error).expect("the traceback is its cause");
    assert!(
        cause
            .to_string()
            .starts_with("Traceback (most recent call last):")
    );
}

#[test]
fn a_mask_that_is_not_boolean_is_refused() {
    let transform =
        embedded::with(|py| native(py, "lambda batch: batch.column('n')", true, None).transform());
    assert!(transform.keep(&input()).is_err());
}

#[test]
fn a_transform_runs_on_a_daemon_thread_with_only_python_beneath_it() {
    let transform = embedded::with(|py| {
        native(
            py,
            "lambda batch: batch.append_column('daemon', __import__('pyarrow').array([__import__('threading').current_thread().daemon] * batch.num_rows))",
            false,
            None,
        )
        .transform()
    });
    let applied = transform.apply(input()).unwrap();
    let daemon = applied
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(daemon.iter().all(|value| value == Some(true)));
}

#[test]
fn a_partial_update_that_meets_a_python_filter_fails_as_transform_partial() {
    embedded::with(|py| {
        let filter = native(
            py,
            "lambda batch: __import__('pyarrow').array([True] * batch.num_rows)",
            true,
            None,
        )
        .transform();
        let host = rdlt::Host::new()
            .source::<ChangesSource>()
            .destination::<MemoryDestination>()
            .transform("kept", filter);
        let document = serde_json::json!({
            "version": 1,
            "pipeline": "partial",
            "source": {
                "connector": "io.rapidbyte.changes",
                "config": { "seed": 7, "streams": [{ "name": "rows", "keys": 4, "changes": 200 }] },
            },
            "destination": {
                "connector": "io.rapidbyte.memory",
                "config": { "store": "python-partial" },
            },
            "streams": [{
                "name": "rows",
                "read": "cdc",
                "write": { "merge": { "key": ["id"] } },
                "transform": [{ "code": "kept" }],
            }],
        });
        let builder = rdlt::Pipeline::from_text(&document.to_string(), "<python>").unwrap();
        let runtime = runtime::get(py).unwrap();
        let outcome = py.detach(|| {
            runtime.block_on(async move {
                let pipeline = builder.build(&host).await.expect("it builds");
                tokio::time::timeout(Duration::from_secs(60), pipeline.start())
                    .await
                    .expect("within 60 s")
            })
        });
        let report = outcome.error.expect("the run fails").report();
        assert_eq!(
            report.code.map(rdlt::Code::as_str),
            Some("transform_partial")
        );
    });
}

#[test]
fn a_blocked_output_leaves_the_runtime_its_worker() {
    let transform = embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.entered = threading.Event(); builtins.leave = threading.Event()",
            None,
            None,
        )
        .unwrap();
        native(
            py,
            "lambda batch: (__import__('builtins').entered.set(), __import__('builtins').leave.wait(120), batch)[2]",
            false,
            None,
        )
        .transform()
    });
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let schema = input().schema();
    let inferred = runtime.spawn(async move { transform.output(&schema).is_ok() });
    embedded::with(|py| {
        assert!(
            py.eval(c"__import__('builtins').entered.wait(30)", None, None)
                .unwrap()
                .extract::<bool>()
                .unwrap()
        );
    });
    // One worker: this task runs only because the inference left it. Read here on a channel,
    // since a blocked worker would stop the runtime's own timers too.
    let has_run = Within::new();
    let ran = has_run.clone();
    drop(runtime.spawn(async move { ran.give(7) }));
    let answered = has_run
        .wait(Duration::from_secs(20))
        .expect("the worker was not held");
    assert_eq!(answered, 7);
    embedded::with(|py| {
        py.eval(c"__import__('builtins').leave.set()", None, None)
            .unwrap();
    });
    let inferred = runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(30), inferred).await })
        .expect("the inference ended")
        .unwrap();
    assert!(inferred);
}
