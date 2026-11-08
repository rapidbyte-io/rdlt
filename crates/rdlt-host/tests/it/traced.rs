//! The host's trace crosses the wire: each call carries `traceparent`, and the served
//! connector's span for the call names the host's trace.

use std::num::NonZeroUsize;
use std::time::Duration;

use rdlt_connector::serve::Served;
use rdlt_connector::{
    Partition, ReadRequest, Role, Source as _, StreamName, partition_channel, source_factory,
};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{Connection, Options, RemoteSource, TraceParent};
use rdlt_testkit::spans::Spans;

use crate::support::piped;

#[tokio::test(start_paused = true)]
async fn the_host_sends_its_trace_and_serve_continues_it() {
    assert!(rdlt_host::trace::propagate(|| TraceParent::new(
        [1; 16], [2; 8], 1
    )));
    let spans = Spans::default();
    let _traced = tracing::subscriber::set_default(spans.clone());
    let io = piped(Served::new().with_source(source_factory::<MemorySource>()));
    let config = serde_json::json!({ "streams": {} });
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("connected");
    drop(connection);
    let calls = spans.named("rdlt.serve");
    assert!(!calls.is_empty(), "each served call opens a span");
    assert!(
        calls.iter().all(
            |fields| fields.contains("trace_id=01010101010101010101010101010101")
                && fields.contains("parent_id=0202020202020202")
        ),
        "{calls:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_read_of_the_data_plane_carries_the_trace_too() {
    assert!(rdlt_host::trace::propagate(|| TraceParent::new(
        [1; 16], [2; 8], 1
    )));
    let spans = Spans::default();
    let _traced = tracing::subscriber::set_default(spans.clone());
    let io = piped(Served::new().with_source(source_factory::<MemorySource>()));
    let config = serde_json::json!({ "streams": { "rows": [{ "id": 1 }] } });
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("connected");
    let source = RemoteSource::new(connection);
    let (sink, _feed) = partition_channel(NonZeroUsize::new(4).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("rows").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    tokio::time::timeout(Duration::from_secs(60), source.read(request, sink))
        .await
        .expect("the read ends")
        .expect("it reads");
    let reads: Vec<String> = spans
        .named("rdlt.serve")
        .into_iter()
        .filter(|fields| fields.contains("Read"))
        .collect();
    assert!(!reads.is_empty(), "the read is served in a span");
    assert!(
        reads
            .iter()
            .all(|fields| fields.contains("trace_id=01010101010101010101010101010101")),
        "{reads:?}"
    );
}
