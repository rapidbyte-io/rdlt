use std::num::NonZeroU16;

use rdlt_connector::{BoxFuture, Capabilities, Destination, OpenContext, OpenedSession, Result};

use super::lane_count;
use crate::config::EngineConfig;

struct Writers(Capabilities);

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
}

fn destination(writers: u16) -> Writers {
    let mut capabilities = Capabilities::minimal();
    capabilities.max_parallel_writers = NonZeroU16::new(writers).unwrap();
    Writers(capabilities)
}

#[test]
fn configured_lanes_win_over_the_destinations_limit() {
    let config = EngineConfig::builder().lanes(5).build().unwrap();
    assert_eq!(lane_count(&config, &destination(2)).get(), 5);
}

#[test]
fn default_lanes_are_one_per_core_up_to_the_destinations_limit() {
    let config = EngineConfig::builder().build().unwrap();
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    assert_eq!(lane_count(&config, &destination(1)).get(), 1);
    assert_eq!(lane_count(&config, &destination(u16::MAX)).get(), cores);
    assert_eq!(lane_count(&config, &destination(2)).get(), cores.min(2));
}
