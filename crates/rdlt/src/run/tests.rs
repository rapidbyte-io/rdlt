use std::num::NonZeroUsize;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use rdlt_connector::PipelineId;
use rdlt_engine::{ComputePool, Cores, Env, Event, EventKind, RunStatus, SystemEnv};
use tokio::sync::broadcast;

use super::Observed;
use crate::Events;

#[tokio::test]
async fn a_run_s_environment_is_the_host_s_and_tells_each_event_to_its_ring() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let base = Arc::new(SystemEnv::try_new(cores).unwrap());
    let (events, mut told) = broadcast::channel(4);
    let ended = Arc::new(AtomicBool::new(false));
    let observed = Observed {
        base: Arc::clone(&base),
        events,
        ended: Arc::clone(&ended),
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
    assert!(!ended.load(Ordering::SeqCst), "the run goes on");
    let end = EventKind::RunEnded {
        status: RunStatus::Succeeded,
    };
    observed.observe(&Event { kind: end, ..event });
    assert!(ended.load(Ordering::SeqCst), "the run has ended");
}

#[tokio::test]
async fn a_subscriber_come_late_reads_what_it_holds_and_ends() {
    let (events, receiver) = broadcast::channel(4);
    let event = Event {
        seq: 1,
        at: SystemTime::now(),
        pipeline: PipelineId::parse("late").unwrap(),
        attempt: 1,
        kind: EventKind::AttemptStarted,
    };
    events.send(event.clone()).expect("a subscriber holds it");
    let mut late = Events::new(receiver, true);
    assert_eq!(
        late.next().await.expect("it holds one").expect("kept up"),
        event
    );
    assert!(late.next().await.is_none(), "the channel is still open");
    assert!(late.next().await.is_none(), "it stays ended");
    assert_eq!(events.receiver_count(), 1);
}
