use std::num::NonZeroUsize;
use std::ptr;
use std::sync::Arc;
use std::time::SystemTime;

use rdlt_connector::PipelineId;
use rdlt_engine::{ComputePool, Cores, Env, Event, EventKind, SystemEnv};
use tokio::sync::broadcast;

use super::Observed;

#[tokio::test]
async fn a_run_s_environment_is_the_host_s_and_tells_each_event_to_its_ring() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let base = Arc::new(SystemEnv::try_new(cores).unwrap());
    let (events, mut told) = broadcast::channel(4);
    let observed = Observed {
        base: Arc::clone(&base),
        events,
    };
    assert_eq!(observed.cores(), base.cores());
    assert!(observed.observes());
    let draws = [observed.random(), observed.random(), observed.random()];
    assert!(draws[0] != draws[1] || draws[1] != draws[2], "{draws:?}");
    let before = SystemTime::now();
    assert!(observed.now() >= before);
    let started = observed.instant();
    observed.sleep(std::time::Duration::from_millis(1)).await;
    assert!(observed.instant() > started);
    let pool = |env: &dyn Env| ptr::from_ref::<dyn ComputePool>(env.compute()).cast::<()>();
    assert_eq!(pool(&observed), pool(base.as_ref()));
    let event = Event {
        seq: 1,
        at: before,
        pipeline: PipelineId::parse("told").unwrap(),
        attempt: 1,
        kind: EventKind::AttemptStarted,
    };
    observed.observe(&event);
    assert_eq!(told.try_recv().expect("the event is in the ring"), event);
}
