use std::num::NonZeroUsize;
use std::time::Duration;

use rdlt_connector::{ConnectContext, PipelineId};

use super::{Schedule, Tables, TablesCell, phases};
use crate::bench::{sink_factory, written};
use crate::report::Report;
use crate::{Cores, EngineConfig};

/// A schedule of the timed one's shape a tenth of a second long, so a test runs a cell quickly.
const QUICK: Schedule = Schedule {
    pace: Duration::from_millis(10),
    interval: Duration::from_millis(20),
    quiet: Duration::from_millis(300),
    span: Duration::from_millis(500),
};

/// Pushes so far apart that a stream's last is due long after a span of 150 ms.
const LATE: Schedule = Schedule {
    pace: Duration::from_millis(200),
    interval: Duration::from_millis(20),
    quiet: Duration::from_millis(50),
    span: Duration::from_millis(150),
};

fn cores() -> Cores {
    Cores::new(NonZeroUsize::new(2).unwrap())
}

fn followed(streams: usize) -> TablesCell {
    TablesCell::Followed(NonZeroUsize::new(streams).unwrap())
}

#[test]
fn a_followed_cell_reads_a_partition_beyond_its_streams_within_the_default_budget() {
    let default = EngineConfig::default().memory().get();
    for streams in [16, 64, 192, 256] {
        let tables = Tables::try_new(cores(), followed(streams), Schedule::TESTED).unwrap();
        assert_eq!(tables.partitions().get(), streams + 1, "{streams}");
        assert_eq!(tables.memory(), default, "{streams}");
        let least = EngineConfig::least_memory(streams + 1);
        assert_eq!(tables.least_memory(), least, "{streams}");
        assert_eq!(tables.kept_share(), default / 4, "{streams}");
        assert_eq!(tables.writers().get(), 2, "a writer a core");
    }
}

#[test]
fn a_cell_s_sink_declares_a_writer_a_core() {
    let tables = Tables::try_new(cores(), followed(16), Schedule::TESTED).unwrap();
    let factory = sink_factory();
    let connecting = factory.connect(tables.sink_config(), ConnectContext::new());
    let sink = tables.block_on(connecting).unwrap();
    assert_eq!(sink.capabilities().max_parallel_writers, tables.writers());
}

#[test]
fn the_snapshot_reads_its_tables_at_the_engine_s_defaults() {
    let tables = Tables::try_new(cores(), TablesCell::Snapshot, Schedule::TESTED).unwrap();
    let defaults = EngineConfig::default();
    assert_eq!(tables.partitions(), defaults.partitions());
    assert_eq!(tables.memory(), defaults.memory().get());
    assert_eq!(TablesCell::Snapshot.streams().get(), 256);
    assert_eq!(TablesCell::Snapshot.rows(), 256 * 4 * 1_000);
    assert!(tables.replay_config().get("pace").is_none());
}

#[test]
fn the_cells_are_named_by_their_streams_and_replay_alike_in_every_process() {
    let names: Vec<String> = TablesCell::ALL.iter().map(|cell| cell.name()).collect();
    assert_eq!(
        names,
        [
            "followed/16",
            "followed/64",
            "followed/192",
            "followed/256",
            "snapshot/256"
        ]
    );
    for (index, cell) in TablesCell::ALL.into_iter().enumerate() {
        assert_eq!(cell.index(), index);
        assert_eq!(TablesCell::named(&cell.replay()), Some(cell));
        let replayed = cell.replayed();
        assert_eq!(replayed.pushes(), cell.pushes(), "{}", cell.name());
        assert_eq!(replayed, cell.replayed(), "{}", cell.name());
    }
    assert_eq!(TablesCell::named("followed_17"), None);
    assert_eq!(followed(16).rows(), 16 * 5 * 1_000);
}

#[test]
fn a_followed_run_lands_every_push_before_its_streams_fall_quiet_and_nothing_while_they_are() {
    for streams in [3, 16] {
        let tables = Tables::try_new(cores(), followed(streams), QUICK).unwrap();
        let (source, destination) = tables.block_on(tables.connectors());
        let before = written();
        // The rows a run lands are checked against the pushes inside.
        let metered = tables.run(source, destination, written);
        let [quiet, ended] = metered.readings;
        let pushes = u64::try_from(followed(streams).pushes()).unwrap();
        assert_eq!(quiet - before, pushes, "every push is written by the quiet");
        assert_eq!(
            ended, quiet,
            "nothing is written while the streams are quiet"
        );
        assert!(
            metered.window >= QUICK.span.saturating_sub(QUICK.quiet) && metered.window < QUICK.span,
            "{:?}",
            metered.window
        );
        let report = &metered.report;
        assert_eq!(report.streams.len(), streams);
        assert!(report.counters.commits.interval > 0, "{report:?}");
    }
}

#[test]
fn a_followed_run_still_pushing_at_its_span_s_end_follows_on_until_every_row_lands() {
    let tables = Tables::try_new(cores(), followed(2), LATE).unwrap();
    let (source, destination) = tables.block_on(tables.connectors());
    let before = written();
    // The rows a run lands are checked against the pushes inside.
    let metered = tables.run(source, destination, written);
    let [_, ended] = metered.readings;
    let pushes = u64::try_from(followed(2).pushes()).unwrap();
    assert!(
        ended - before < pushes,
        "the window ends at the span, before the last pushes are due: {} of {pushes} written",
        ended - before
    );
}

#[test]
fn a_snapshot_run_lands_every_row_and_reads_its_meter_around_the_run() {
    let tables = Tables::try_new(cores(), TablesCell::Snapshot, QUICK).unwrap();
    let (source, destination) = tables.block_on(tables.connectors());
    let before = written();
    let metered = tables.run(source, destination, written);
    let [started, ended] = metered.readings;
    assert_eq!(started, before);
    let pushes = u64::try_from(TablesCell::Snapshot.pushes()).unwrap();
    assert_eq!(ended - started, pushes);
    assert_eq!(metered.report.streams.len(), 256);
    assert_eq!(metered.report.commits, 1);
}

#[test]
fn each_phase_is_a_commit_s_share_of_its_time_over_the_runs() {
    let report = |commits: u64, flush: u64, ack: u64| {
        let mut report = Report::new(PipelineId::parse("tables").unwrap());
        report.commits = commits;
        report.counters.phases.flush = Duration::from_millis(flush);
        report.counters.phases.ack = Duration::from_millis(ack);
        report
    };
    let runs = [report(2, 4, 1), report(4, 4, 6), report(1, 3, 0)];
    assert_eq!(
        phases(&runs.iter().collect::<Vec<_>>()),
        "flush 2.000 (1.000 to 3.000), commit 0.000 (0.000 to 0.000), acknowledgement 0.500 \
         (0.000 to 1.500)"
    );
    let none = phases(&[&report(0, 0, 0)]);
    assert!(none.starts_with("flush unmeasured"), "{none}");
}

#[test]
fn a_cell_says_what_it_reads_and_within_what_budget() {
    #[expect(clippy::cast_precision_loss, reason = "a budget far below 2^52")]
    let mebibytes = |partitions| EngineConfig::least_memory(partitions) as f64 / f64::from(1 << 20);
    let least = mebibytes(257);
    let followed = Tables::try_new(cores(), followed(256), Schedule::TIMED).unwrap();
    assert_eq!(
        followed.to_string(),
        format!(
            "256 streams of an unbounded partition each, 5 pushes of 1000 rows a stream 1s apart, \
         committed every 2s, followed for 10s, quiet from 7s; 257 partitions read at once within \
         256.0 MiB, the least that admits them {least:.1} MiB; 2 writers"
        )
    );
    let snapshot = Tables::try_new(cores(), TablesCell::Snapshot, Schedule::TIMED).unwrap();
    let least = mebibytes(16);
    assert_eq!(
        snapshot.to_string(),
        format!(
            "256 tables of a bounded partition and 4 pushes of 1000 rows each; 16 partitions read \
             at once within 256.0 MiB, the least that admits them {least:.1} MiB; 2 writers"
        )
    );
}

#[test]
fn a_served_cell_follows_fewer_streams_than_its_connection_carries_calls() {
    let served: Vec<String> = TablesCell::served().map(TablesCell::name).collect();
    assert_eq!(
        served,
        ["followed/16", "followed/64", "followed/192", "snapshot/256"]
    );
}
