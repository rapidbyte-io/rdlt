use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::Mutex;
use rdlt_connector::PipelineId;

use super::{Announcer, Event, EventKind, Listed};
use crate::env::{Env, InlineEnv, Sleep};
use crate::limits::REPORTED_LISTED;

#[test]
fn a_list_names_its_first_entries_and_counts_the_rest() {
    let listed = Listed::first(0..REPORTED_LISTED + 3);
    assert_eq!(listed.entries, (0..REPORTED_LISTED).collect::<Vec<_>>());
    assert_eq!(listed.unlisted, 3);
    let short = Listed::first(["a", "b"]);
    assert_eq!((short.entries, short.unlisted), (vec!["a", "b"], 0));
}

/// An environment keeping what it is told.
#[derive(Default)]
struct Kept(Mutex<Vec<Event>>);

impl Env for Kept {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(7)
    }

    fn instant(&self) -> std::time::Instant {
        InlineEnv.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        InlineEnv.sleep(duration)
    }

    fn random(&self) -> u64 {
        4
    }

    fn compute(&self) -> &dyn crate::compute::ComputePool {
        &crate::compute::Inline
    }

    fn cores(&self) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::MIN
    }

    fn observe(&self, event: &Event) {
        self.0.lock().push(event.clone());
    }

    fn observes(&self) -> bool {
        true
    }
}

#[test]
fn an_announcer_tells_its_environment_of_its_attempt_at_the_environment_s_time() {
    let kept = Arc::new(Kept::default());
    let pipeline = PipelineId::parse("told").unwrap();
    let env: Arc<dyn Env> = Arc::clone(&kept) as Arc<dyn Env>;
    Announcer::new(env, pipeline.clone(), 3).announce(|| EventKind::AttemptStarted);
    let told = kept.0.lock().clone();
    assert_eq!(
        told,
        [Event {
            at: SystemTime::UNIX_EPOCH + Duration::from_secs(7),
            pipeline,
            attempt: 3,
            kind: EventKind::AttemptStarted,
        }]
    );
}

#[test]
fn an_environment_told_nothing_has_no_event_built_for_it() {
    let pipeline = PipelineId::parse("untold").unwrap();
    let env: Arc<dyn Env> = Arc::new(InlineEnv);
    assert!(!env.observes());
    Announcer::new(env, pipeline, 1).announce(|| unreachable!("no event is built"));
}

#[test]
fn an_announcer_shows_its_pipeline_and_attempt_when_debugged() {
    let pipeline = PipelineId::parse("debugged").unwrap();
    let announcer = Announcer::new(Arc::new(InlineEnv), pipeline, 42);
    let shown = format!("{announcer:?}");
    assert!(
        shown.contains("debugged") && shown.contains("42"),
        "{shown}"
    );
}
