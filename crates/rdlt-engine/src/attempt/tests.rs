use std::num::{NonZeroU16, NonZeroUsize};

use rdlt_connector::{BoxFuture, Capabilities, Destination, OpenContext, OpenedSession, Result};

use super::{held_writers, lane_count};
use crate::config::{EngineConfig, GrowthLimits};
use crate::error::codes;

/// A destination of its capabilities that carries its calls at once.
struct Writers(Capabilities, Option<NonZeroUsize>);

impl Destination for Writers {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn open<'a>(&'a self, _context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        unreachable!("lane counts never open a session")
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.1
    }
}

fn destination(writers: u16) -> Writers {
    carrying(writers, None)
}

/// A destination of `writers` parallel writers that carries `calls` at once.
fn carrying(writers: u16, calls: Option<usize>) -> Writers {
    let mut capabilities = Capabilities::minimal();
    capabilities.max_parallel_writers = NonZeroU16::new(writers).unwrap();
    Writers(
        capabilities,
        calls.map(|calls| NonZeroUsize::new(calls).unwrap()),
    )
}

fn cores(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}

#[test]
fn configured_lanes_never_exceed_the_destinations_writers() {
    let config = EngineConfig::builder().lanes(5).build().unwrap();
    assert_eq!(lane_count(&config, cores(16), &destination(2)).get(), 2);
    assert_eq!(lane_count(&config, cores(1), &destination(8)).get(), 5);
}

#[test]
fn default_lanes_are_one_per_declared_core_up_to_the_destinations_limit() {
    let config = EngineConfig::builder().build().unwrap();
    for (count, writers, lanes) in [
        (1, u16::MAX, 1),
        (6, u16::MAX, 6),
        (64, u16::MAX, 64),
        (6, 2, 2),
        (6, 1, 1),
        (3, 8, 3),
    ] {
        assert_eq!(
            lane_count(&config, cores(count), &destination(writers)).get(),
            lanes,
            "{count} cores, {writers} writers"
        );
    }
}

#[test]
fn an_attempt_holds_no_more_writers_open_than_its_destination_carries_beside_a_sessions_call() {
    let growth = GrowthLimits::new(1024, 300).unwrap();
    let config = EngineConfig::builder().growth(growth).build().unwrap();
    for (calls, writers) in [
        (None, 300),
        (Some(199), 198),
        (Some(300), 299),
        (Some(301), 300),
        (Some(1000), 300),
        (Some(1), 1),
    ] {
        let destination = carrying(u16::MAX, calls);
        assert_eq!(
            held_writers(&config, &destination).get(),
            writers,
            "{calls:?} calls"
        );
    }
}

#[test]
fn lanes_never_exceed_the_writers_their_destination_carries() {
    let config = EngineConfig::builder().lanes(250).build().unwrap();
    let growth = GrowthLimits::new(1024, 300).unwrap();
    let grown = EngineConfig::builder()
        .lanes(250)
        .growth(growth)
        .build()
        .unwrap();
    assert_eq!(
        lane_count(&config, cores(1), &carrying(1000, None)).get(),
        128
    );
    assert_eq!(
        lane_count(&grown, cores(1), &carrying(1000, None)).get(),
        250
    );
    assert_eq!(
        lane_count(&grown, cores(1), &carrying(1000, Some(101))).get(),
        100
    );
}

mod keys {
    use rdlt_connector::{ColumnPath, StreamName, StreamSpec};

    use super::super::streams::merge_key;
    use crate::error::codes;
    use crate::plan::{StreamPlan, WriteMode};

    fn spec(key: Option<ColumnPath>) -> StreamSpec {
        let spec = StreamSpec::new(StreamName::new("s").unwrap());
        match key {
            Some(column) => spec.with_primary_key([column]),
            None => spec,
        }
    }

    fn merging() -> StreamPlan {
        StreamPlan::new(StreamName::new("s").unwrap()).write(WriteMode::Merge)
    }

    #[test]
    fn a_merge_key_comes_from_the_plan_then_the_catalog() {
        let catalog = spec(Some(ColumnPath::from("id")));
        assert_eq!(
            merge_key(&merging(), &catalog).unwrap(),
            [ColumnPath::from("id")]
        );
        let planned = merging().key(["code"]);
        assert_eq!(
            merge_key(&planned, &catalog).unwrap(),
            [ColumnPath::from("code")]
        );
        let appending = StreamPlan::new(StreamName::new("s").unwrap());
        assert!(merge_key(&appending, &catalog).unwrap().is_empty());
    }

    #[test]
    fn a_missing_or_nested_merge_key_is_refused() {
        let missing = merge_key(&merging(), &spec(None)).unwrap_err();
        assert_eq!(missing.code(), Some(codes::MERGE_KEY_MISSING));
        let nested = spec(Some(ColumnPath::new(["a", "b"]).unwrap()));
        let error = merge_key(&merging(), &nested).unwrap_err();
        assert_eq!(error.code(), Some(codes::PLAN_COLUMN_NESTED));
    }
}

#[test]
fn an_attempt_reads_as_many_partitions_at_once_as_a_plan_may_name_and_no_more() {
    use rdlt_connector::limits::MAX_PLAN_PARTITIONS;
    assert!(super::within_partition_limit(MAX_PLAN_PARTITIONS).is_ok());
    let refused = super::within_partition_limit(MAX_PLAN_PARTITIONS + 1).unwrap_err();
    assert_eq!(refused.code(), Some(codes::PLAN_INVALID));
}

#[test]
fn lanes_never_exceed_the_writers_an_attempt_holds_open() {
    let growth = GrowthLimits::new(1, 3).unwrap();
    let config = EngineConfig::builder()
        .lanes(5)
        .growth(growth)
        .build()
        .unwrap();
    assert_eq!(lane_count(&config, cores(16), &destination(8)).get(), 3);
}

#[test]
fn the_least_memory_s_share_for_logs_holds_a_carry_s_read() {
    for partitions in [1, 16, 17, 600_000] {
        let least = EngineConfig::least_memory(partitions).expect("a least memory");
        let shares = crate::budget::Shares::of(least);
        assert!(
            shares.log >= crate::limits::LOG_COPY_BYTES,
            "{partitions} partitions: {shares:?}"
        );
    }
}
