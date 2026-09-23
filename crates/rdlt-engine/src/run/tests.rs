use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, Cursor, Destination, OpenContext, OpenedSession, Partition,
    PartitionId, PartitionSink, PipelineId, ReadRequest, Result, Source, StreamName, StreamState,
};

use super::{Engine, StopMode};
use crate::compute::RayonPool;
use crate::config::EngineConfig;
use crate::env::SystemEnv;
use crate::plan::{PipelinePlan, StreamPlan};

fn engine() -> Engine {
    let pool = RayonPool::new(NonZeroUsize::MIN).unwrap();
    let config = EngineConfig::builder().lanes(3).build().unwrap();
    Engine::new(config, Arc::new(SystemEnv::new(pool)))
}

/// A connector the tests never poll a run far enough to use.
struct Idle(Capabilities);

impl Source for Idle {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        unreachable!("the run is never polled")
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        unreachable!("the run is never polled")
    }

    fn plan<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a StreamState,
    ) -> BoxFuture<'a, Result<Vec<Partition>>> {
        unreachable!("the run is never polled")
    }

    fn read(&self, _: ReadRequest, _: PartitionSink) -> BoxFuture<'_, Result<()>> {
        unreachable!("the run is never polled")
    }

    fn committed<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        unreachable!("the run is never polled")
    }
}

impl Destination for Idle {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        unreachable!("the run is never polled")
    }

    fn open<'a>(&'a self, _: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        unreachable!("the run is never polled")
    }
}

#[test]
fn engines_and_handles_describe_themselves() {
    let engine = engine();
    let rendered = format!("{engine:?}");
    assert!(rendered.starts_with("Engine"), "{rendered}");
    assert!(rendered.contains("lanes: Some(3)"), "{rendered}");
    let plan = PipelinePlan::new(
        PipelineId::parse("p").unwrap(),
        [StreamPlan::new(StreamName::new("s").unwrap())],
    )
    .unwrap();
    let idle = Arc::new(Idle(Capabilities::minimal()));
    let handle = engine.run(plan, Arc::clone(&idle) as _, idle);
    assert_eq!(format!("{handle:?}"), "RunHandle { .. }");
    assert!(format!("{:?}", handle.control()).starts_with("RunControl"));
}

#[test]
fn stop_modes_are_distinct() {
    assert_ne!(StopMode::AfterCommit, StopMode::Now);
}
