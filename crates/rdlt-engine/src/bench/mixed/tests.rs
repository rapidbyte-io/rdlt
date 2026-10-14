use std::num::NonZeroUsize;

use super::Mixed;
use crate::Cores;
use crate::bench::memory_wal;

fn cores() -> Cores {
    Cores::from_count(NonZeroUsize::new(2).unwrap())
}

#[test]
#[expect(clippy::cast_precision_loss, reason = "a share of two byte counts")]
fn a_stream_its_source_cannot_read_again_logs_every_stream_s_batches() {
    let mixed = Mixed::logged(cores(), Mixed::CELL.tested(), memory_wal()).unwrap();
    let (replayable, all) = mixed.frame_bytes();
    let share = replayable as f64 / all as f64;
    assert!((0.45..0.55).contains(&share), "{share}");
    for _ in 0..2 {
        let report = mixed.run();
        let log = &report.counters.log;
        assert!(log.appended >= all, "{log:?}");
        assert_eq!(log.carried, 0, "{log:?}");
    }
}

#[test]
fn streams_their_source_can_each_read_again_log_nothing() {
    let mixed = Mixed::unlogged(cores(), Mixed::CELL.tested()).unwrap();
    let report = mixed.run();
    assert_eq!(report.counters.log.appended, 0);
    assert_eq!(report.rows, u64::from(Mixed::CELL.tested().rows) * 4);
}
