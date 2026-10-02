//! What a source in the engine's process may send is what the budget admits: one that keeps to
//! the limits it is held to where it emits is refused nothing for the budget's sake.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use rdlt_engine::{EngineConfig, RunStatus};

use crate::support::destinations::null;
use crate::support::making::{Step, Steps, making};
use crate::support::{commit_every, engine, pipeline, stream};

#[tokio::test(start_paused = true)]
async fn a_source_sending_all_the_default_budget_admits_is_refused_nothing() {
    let limits = EngineConfig::default().limits();
    let (json, frame, cursor) = (
        usize::try_from(limits.json_push_bytes).expect("a size"),
        usize::try_from(limits.frame_bytes).expect("a size"),
        usize::try_from(limits.cursor_bytes).expect("a size"),
    );
    assert_eq!((json, frame, cursor), (29_360_128, 33_306_271, 123_361));
    // A JSON push of one record as long as a push may be, a batch keeping alive all a batch
    // may, and a cursor as long as a cursor may be, each sealed by a checkpoint.
    let steps: Steps = Arc::new(move |step| match step {
        0 => {
            let pad = "x".repeat(json - 12);
            let record = format!(r#"{{"pad":"{pad}"}}"#);
            assert_eq!(record.len(), json - 2);
            Some(Step::Json(bytes::Bytes::from(record)))
        }
        1 | 3 => Some(Step::Checkpoint(cursor)),
        2 => {
            // The schema, the offsets and the validity share the frame's bytes with the value.
            let value = "y".repeat(frame - 4_096);
            let column: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
            Some(Step::Batch(
                RecordBatch::try_from_iter([("n", column)]).expect("a batch"),
            ))
        }
        _ => None,
    });
    let outcome = engine(commit_every(1_000_000_000).lanes(1))
        .run(
            pipeline("admitted", [stream("events")]),
            making("admitted", steps).await,
            null().await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 2);
}

#[tokio::test(start_paused = true)]
async fn a_source_sending_beyond_what_the_budget_admits_is_refused_where_it_emits() {
    let limits = EngineConfig::default().limits();
    // A byte beyond what JSON may take of the budget, which the wire would carry: the source
    // is refused where it emits, before the budget is asked.
    let json = usize::try_from(limits.json_push_bytes).expect("a size") + 1;
    let steps: Steps = Arc::new(move |step| {
        (step < 1).then(|| {
            let record = format!(r#"{{"pad":"{}"}}"#, "x".repeat(json - 10));
            Step::Json(bytes::Bytes::from(record))
        })
    });
    let outcome = engine(commit_every(1_000_000_000).lanes(1))
        .run(
            pipeline("admitted_beyond", [stream("events")]),
            making("admitted_beyond", steps).await,
            null().await,
        )
        .await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(error.code(), Some("limit_exceeded"));
    let said = format!("{:?}", error.report());
    let expected = format!("json push bytes is {json}, over the limit of {}", json - 1);
    assert!(said.contains(&expected), "{said}");
    assert_eq!(outcome.report.peak_memory, 0);
}
