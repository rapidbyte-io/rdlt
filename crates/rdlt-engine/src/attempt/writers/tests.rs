use std::num::{NonZeroU16, NonZeroUsize};

use rdlt_connector::{BoxFuture, Capabilities, Destination, OpenContext, OpenedSession, Result};

use super::{held_table_writers, writer_count};
use crate::config::{EngineConfig, GrowthLimits};

/// A destination of its capabilities that carries its calls at once.
struct Declaring(Capabilities, Option<NonZeroUsize>);

impl Destination for Declaring {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn open<'a>(&'a self, _context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        unreachable!("writer counts never open a session")
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.1
    }
}

fn destination(writers: u16) -> Declaring {
    carrying(writers, None)
}

/// A destination of `writers` parallel writers that carries `calls` at once.
fn carrying(writers: u16, calls: Option<usize>) -> Declaring {
    let mut capabilities = Capabilities::minimal();
    capabilities.max_parallel_writers = NonZeroU16::new(writers).unwrap();
    Declaring(
        capabilities,
        calls.map(|calls| NonZeroUsize::new(calls).unwrap()),
    )
}

fn cores(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}

#[test]
fn configured_writers_never_exceed_what_the_destination_runs_at_once() {
    let config = EngineConfig::builder().writers(5).build().unwrap();
    assert_eq!(writer_count(&config, cores(16), &destination(2)).get(), 2);
    assert_eq!(writer_count(&config, cores(1), &destination(8)).get(), 5);
}

#[test]
fn default_writers_are_one_per_declared_core_up_to_the_destinations_limit() {
    let config = EngineConfig::builder().build().unwrap();
    for (count, declared, expected) in [
        (1, u16::MAX, 1),
        (6, u16::MAX, 6),
        (64, u16::MAX, 64),
        (6, 2, 2),
        (6, 1, 1),
        (3, 8, 3),
    ] {
        assert_eq!(
            writer_count(&config, cores(count), &destination(declared)).get(),
            expected,
            "{count} cores, {declared} writers"
        );
    }
}

#[test]
fn an_attempt_holds_no_more_table_writers_open_than_its_destination_carries_beside_a_sessions_call()
{
    let growth = GrowthLimits::new(1024, 300).unwrap();
    let config = EngineConfig::builder().growth(growth).build().unwrap();
    for (calls, table_writers) in [
        (None, 300),
        (Some(199), 198),
        (Some(300), 299),
        (Some(301), 300),
        (Some(1000), 300),
        (Some(1), 1),
    ] {
        let destination = carrying(u16::MAX, calls);
        assert_eq!(
            held_table_writers(&config, &destination).get(),
            table_writers,
            "{calls:?} calls"
        );
    }
}

#[test]
fn writers_never_exceed_the_table_writers_a_destination_carries() {
    let config = EngineConfig::builder().writers(250).build().unwrap();
    let growth = GrowthLimits::new(1024, 300).unwrap();
    let grown = EngineConfig::builder()
        .writers(250)
        .growth(growth)
        .build()
        .unwrap();
    assert_eq!(
        writer_count(&config, cores(1), &carrying(1000, None)).get(),
        128
    );
    assert_eq!(
        writer_count(&grown, cores(1), &carrying(1000, None)).get(),
        250
    );
    assert_eq!(
        writer_count(&grown, cores(1), &carrying(1000, Some(101))).get(),
        100
    );
}

#[test]
fn writers_never_exceed_the_table_writers_an_attempt_holds_open() {
    let growth = GrowthLimits::new(1, 3).unwrap();
    let config = EngineConfig::builder()
        .writers(5)
        .growth(growth)
        .build()
        .unwrap();
    assert_eq!(writer_count(&config, cores(16), &destination(8)).get(), 3);
}
