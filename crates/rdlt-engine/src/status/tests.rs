use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use rdlt_connector::{PartitionId, PipelineId, StreamName};

use super::clock::{stamp, unstamp};
use super::{Call, LaneCalls, Live, Partitions, Phase, Place, Quiet, RunClock, Step};
use crate::budget::budget;
use crate::env::{Env, InlineEnv};
use crate::report::{RunStatus, Tally};

fn live() -> Live {
    let env: Arc<dyn Env> = Arc::new(InlineEnv);
    let pipeline = PipelineId::parse("lively").unwrap();
    Live::new(env, pipeline, budget(1 << 30), Arc::new(Tally::default()))
}

fn place(stream: &str, id: &str, stamp: u64) -> Place {
    Place {
        stream: StreamName::new(stream).unwrap(),
        id: PartitionId::parse(id).unwrap(),
        stamp: Arc::new(AtomicU64::new(stamp)),
    }
}

#[tokio::test(start_paused = true)]
async fn a_stamp_reads_back_as_the_instant_it_was_taken_and_is_never_zero() {
    let origin = InlineEnv.instant();
    assert_eq!(stamp(origin, origin), 1);
    tokio::time::advance(Duration::from_millis(1_500)).await;
    let at = InlineEnv.instant();
    assert_eq!(unstamp(origin, stamp(origin, at)), at);
    // An instant before the origin is the origin.
    assert_eq!(stamp(at, origin), 1);
}

#[test]
fn a_stamp_past_the_latest_is_the_latest_which_leaves_room_for_a_call_beside_it() {
    let origin = InlineEnv.instant();
    let centuries = origin + Duration::from_hours(200 * 365 * 24);
    let latest = stamp(origin, centuries);
    assert_eq!(latest, (1 << 62) - 1);
    assert_eq!(
        latest << 2 >> 2,
        latest,
        "a lane's call fits in the two bits left"
    );
}

#[tokio::test(start_paused = true)]
async fn a_run_s_clock_reads_an_instant_as_its_start_s_wall_time_and_the_time_since() {
    let clock = RunClock::new(&InlineEnv);
    tokio::time::advance(Duration::from_secs(4)).await;
    let later = InlineEnv.instant();
    assert_eq!(
        clock.wall(later),
        clock.wall(clock.began()) + Duration::from_secs(4)
    );
}

#[tokio::test(start_paused = true)]
async fn a_lane_s_call_shows_until_it_returns_with_when_it_was_made() {
    let clock = RunClock::new(&InlineEnv);
    let calls = LaneCalls::new(clock.began(), 2);
    tokio::time::advance(Duration::from_secs(3)).await;
    let at = InlineEnv.instant();
    calls.calling(1, Call::Flush, at);
    let read = calls.read(&clock);
    assert_eq!(read[0], None);
    let call = read[1].expect("lane 1 calls");
    assert_eq!((call.call, call.since), (Call::Flush, clock.wall(at)));
    calls.calling(0, Call::Open, at);
    assert_eq!(
        calls.read(&clock)[0].map(|call| call.call),
        Some(Call::Open)
    );
    calls.calling(0, Call::Write, at);
    assert_eq!(
        calls.read(&clock)[0].map(|call| call.call),
        Some(Call::Write)
    );
    calls.returned(1);
    assert_eq!(calls.read(&clock)[1], None);
    // A lane the calls do not know is no lane of theirs.
    calls.calling(7, Call::Write, at);
    assert_eq!(calls.read(&clock).len(), 2);
}

#[tokio::test(start_paused = true)]
async fn partitions_count_by_where_they_stand_and_the_quietest_reading_is_named() {
    let live = live();
    let clock = live.clock();
    let early = stamp(clock.began(), clock.began() + Duration::from_secs(1));
    let late = stamp(clock.began(), clock.began() + Duration::from_secs(9));
    live.places(vec![
        place("a", "p0", late),
        place("a", "p1", Place::WAITING),
        place("b", "p0", early),
        place("b", "p1", Place::ENDED),
    ]);
    let status = live.status();
    let expected = Partitions {
        reading: 2,
        waiting: 1,
        ended: 1,
    };
    assert_eq!(status.partitions, expected);
    let quiet = Quiet {
        stream: "b".to_owned(),
        partition: "p0".to_owned(),
        since: clock.wall(clock.began() + Duration::from_secs(1)),
    };
    assert_eq!(status.quietest, Some(quiet));
    // A place taken by another partition counts as that one.
    live.place(2, place("b", "p2", Place::ENDED));
    live.place(4, place("b", "p3", late));
    let status = live.status();
    assert_eq!((status.partitions.reading, status.partitions.ended), (2, 2));
    // Of two quiet as long, the first place is named.
    let quiet = Quiet {
        stream: "a".to_owned(),
        partition: "p0".to_owned(),
        since: clock.wall(clock.began() + Duration::from_secs(9)),
    };
    assert_eq!(status.quietest, Some(quiet));
}

#[tokio::test(start_paused = true)]
async fn a_status_names_the_phase_and_when_it_began_and_the_end_carries_the_report() {
    let live = live();
    tokio::time::advance(Duration::from_secs(2)).await;
    let at = InlineEnv.instant();
    let committing = Phase::Committing {
        seq: 3,
        step: Step::Commit,
        tries: 2,
    };
    live.phase(committing, at);
    let status = live.status();
    assert_eq!(
        (status.phase, status.since),
        (committing, live.clock().wall(at))
    );
    assert_eq!(status.report.status, RunStatus::Running);
    assert_eq!(status.report.elapsed, Duration::from_secs(2));
    let report = live.end(RunStatus::Stopped);
    let ended = live.status();
    assert_eq!(
        ended.phase,
        Phase::Ended {
            status: RunStatus::Stopped
        }
    );
    assert_eq!(ended.report, report);
    // A status of the ended run stays its end's.
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(live.status().report, report);
}

#[tokio::test(start_paused = true)]
async fn a_run_s_clock_starts_again_when_the_run_begins() {
    let live = live();
    tokio::time::advance(Duration::from_secs(5)).await;
    live.begin();
    let begun = live.status();
    assert_eq!(begun.report.elapsed, Duration::ZERO);
    tokio::time::advance(Duration::from_secs(2)).await;
    let later = live.status();
    assert_eq!(later.report.elapsed, Duration::from_secs(2));
    assert_eq!(later.since, begun.since, "it opens from when it began");
}
