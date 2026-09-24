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
fn configured_lanes_never_exceed_the_destinations_writers() {
    let config = EngineConfig::builder().lanes(5).build().unwrap();
    assert_eq!(lane_count(&config, &destination(2)).get(), 2);
    assert_eq!(lane_count(&config, &destination(8)).get(), 5);
}

#[test]
fn default_lanes_are_one_per_core_up_to_the_destinations_limit() {
    let config = EngineConfig::builder().build().unwrap();
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    assert_eq!(lane_count(&config, &destination(1)).get(), 1);
    assert_eq!(lane_count(&config, &destination(u16::MAX)).get(), cores);
    assert_eq!(lane_count(&config, &destination(2)).get(), cores.min(2));
}

mod keys {
    use rdlt_connector::{ColumnPath, StreamName, StreamSpec};

    use super::super::streams::merge_key;
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
        assert_eq!(missing.code(), Some("merge_key_missing"));
        let nested = spec(Some(ColumnPath::new(["a", "b"]).unwrap()));
        let error = merge_key(&merging(), &nested).unwrap_err();
        assert_eq!(error.code(), Some("plan_column_nested"));
    }
}
