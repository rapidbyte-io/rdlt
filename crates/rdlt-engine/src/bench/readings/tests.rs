use std::num::NonZeroUsize;
use std::time::Duration;

use super::Readings;
use crate::report::{Counters, Flushes, LaneCounters, PoolCounters};

#[test]
fn readings_are_shares_of_the_run_and_counts_a_batch() {
    let mut counters = Counters::default();
    counters.waits.work.time = Duration::from_millis(50);
    counters.waits.intake.time = Duration::from_millis(10);
    counters.waits.decode.time = Duration::from_millis(30);
    counters.pool = PoolCounters {
        jobs: 8,
        queued: Duration::from_millis(20),
        running: Duration::from_millis(400),
        returned: Duration::from_millis(30),
    };
    let wrote = LaneCounters {
        writing: Duration::from_millis(1),
        flushes: Flushes {
            pressure: 3,
            commit: 2,
            retired: 1,
        },
        ..LaneCounters::default()
    };
    counters.lanes = vec![wrote, LaneCounters::default(), wrote];
    counters.commits.interval = 1;
    counters.commits.size = 3;
    counters.commits.cursors = 2;
    counters.commits.log = 4;
    counters.commits.end = 1;
    counters.signals = 64;
    let threads = NonZeroUsize::new(4).unwrap();
    let readings = Readings::of(Duration::from_secs(2), &counters, 16, threads);
    assert_eq!(
        readings.to_string(),
        "commits 11 (interval 1, size 3, cursors 2, log 4, end 1), work waits 2.5 % of the run, \
         intake waits 0.5 %, decode waits 1.5 %, pool idle 95.0 %, pool queue and way back 2.5 \
         %, lanes written 2, writer flushes 12 (pressure 6, commit 4, retired 2), signals 4.0 a \
         batch"
    );
}

#[test]
fn a_run_of_no_time_and_no_batches_reads_as_nothing_rather_than_dividing_by_zero() {
    let threads = NonZeroUsize::MIN;
    let text = Readings::of(Duration::ZERO, &Counters::default(), 0, threads).to_string();
    assert_eq!(
        text,
        "commits 0 (interval 0, size 0, cursors 0, log 0, end 0), work waits 0.0 % of the run, \
         intake waits 0.0 %, decode waits 0.0 %, pool idle 0.0 %, pool queue and way back 0.0 \
         %, lanes written 0, writer flushes 0 (pressure 0, commit 0, retired 0), signals 0.0 a \
         batch"
    );
}
