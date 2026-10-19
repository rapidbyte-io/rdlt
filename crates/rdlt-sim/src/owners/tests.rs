use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, Partition, PartitionId, ReadRequest, Source, SourceConnector, StreamName,
    partition_channel, source_factory,
};
use serde_json::json;

use super::Held;
use crate::changes::{CHANGES_PARTITION, SimChangeSource};
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::source::SimSource;
use crate::world::World;

/// Bounds every wait of a test: a call that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

/// What the connectors of the world `name` hold while a read of `request` from a source `S`
/// runs, after it is stopped, while a second one runs and after that one is dropped.
///
/// Each read sends into a channel of one event that nothing takes from, or follows a partition
/// that never ends, so it lives until it is stopped or dropped.
fn reads_of<S: SourceConnector>(name: &str, request: fn() -> ReadRequest) -> [Held; 4] {
    let world = World::named(name).expect("the world is registered");
    run(Seed::new(1), |_env| async move {
        let factory = source_factory::<S>();
        let connected = factory.connect(json!({ "world": name }), ConnectContext::new());
        let source: Arc<dyn Source> = Arc::from(
            tokio::time::timeout(WAIT, connected)
                .await
                .expect("the connect ends")
                .expect("the source connects"),
        );
        let reading = async |read: &tokio::task::JoinHandle<rdlt_connector::Result<()>>| {
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(!read.is_finished(), "the read lives until it is stopped");
            world.owners.held()
        };
        let (sink, feed) = partition_channel(NonZeroUsize::MIN);
        let read = tokio::spawn({
            let source = Arc::clone(&source);
            async move { source.read(request(), sink).await }
        });
        let first = reading(&read).await;
        feed.stop();
        tokio::time::timeout(WAIT, read)
            .await
            .expect("the stopped read ends")
            .expect("the read does not panic")
            .expect("a stopped read succeeds");
        let stopped = world.owners.held();
        let (sink, _feed) = partition_channel(NonZeroUsize::MIN);
        let read = tokio::spawn(async move { source.read(request(), sink).await });
        let second = reading(&read).await;
        read.abort();
        let aborted = tokio::time::timeout(WAIT, read)
            .await
            .expect("the abort ends");
        assert!(aborted.is_err_and(|error| error.is_cancelled()));
        [first, stopped, second, world.owners.held()]
    })
}

fn assert_counted([first, stopped, second, dropped]: [Held; 4]) {
    assert_eq!(first.reads, 1, "{first}");
    assert!(stopped.is_empty(), "{stopped}");
    assert_eq!(second.reads, 1, "{second}");
    assert!(dropped.is_empty(), "{dropped}");
}

#[test]
fn a_read_counts_until_it_is_stopped_or_dropped() {
    let registered = World::register("held-reads", &mut SplitMix64::new(1));
    let held = reads_of::<SimSource>("held-reads", || {
        let partition = Partition::new(PartitionId::parse("p0").unwrap()).unbounded();
        let mut request = ReadRequest::new(StreamName::new("s0").unwrap(), partition, None);
        request.follow = true;
        request
    });
    drop(registered);
    assert_counted(held);
}

#[test]
fn a_read_of_changes_counts_until_it_is_stopped_or_dropped() {
    let registered = World::register_changes(
        "held-change-reads",
        &mut SplitMix64::new(1),
        &mut SplitMix64::new(2),
    );
    let held = reads_of::<SimChangeSource>("held-change-reads", || {
        let partition = Partition::new(PartitionId::parse(CHANGES_PARTITION).unwrap());
        ReadRequest::new(StreamName::new("c0").unwrap(), partition, None)
    });
    drop(registered);
    assert_counted(held);
}
