//! Tables belong to the pipeline that created them: another pipeline loading into one is refused
//! before any of its rows lands, and the owner's rows stay.

use rdlt_engine::{ErrorKind, RunStatus};

use crate::schema::{batch, ints};
use crate::support::batches::{BatchStream, batches};
use crate::support::targets::Target;
use crate::support::{commit_every, each, engine, pipeline, stream};

#[tokio::test(start_paused = true)]
async fn a_pipeline_loading_into_another_pipeline_s_table_is_refused_and_its_rows_stay() {
    each(Target::IN_PROCESS, |target| async move {
        let store = target.name("owned");
        let run = |name: &'static str, ids: &'static [i64]| {
            let store = store.clone();
            async move {
                let pushed = vec![batch(vec![("id", ints(ids))])];
                engine(commit_every(1))
                    .run(
                        pipeline(name, [stream("events")]),
                        batches(&store, vec![BatchStream::new("events", pushed)]).await,
                        target.destination("owned").await,
                    )
                    .await
            }
        };
        let first = run("owner", &[1, 2]).await;
        assert_eq!(first.report.status, RunStatus::Succeeded, "{target:?}");
        let second = run("intruder", &[3]).await;
        assert_eq!(second.report.status, RunStatus::Failed, "{target:?}");
        let error = second.error.expect("the intruding run failed");
        assert_eq!(error.kind(), ErrorKind::Config, "{target:?}: {error}");
        assert_eq!(error.code(), Some("table_owned"), "{target:?}: {error}");
        assert_eq!(target.ids("owned", "events"), [1, 2], "{target:?}");
        let again = run("owner", &[4]).await;
        assert_eq!(again.report.status, RunStatus::Succeeded, "{target:?}");
        assert_eq!(target.ids("owned", "events"), [1, 2, 4], "{target:?}");
    })
    .await;
}
