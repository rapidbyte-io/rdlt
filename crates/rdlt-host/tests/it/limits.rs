//! A served connector refuses what goes beyond the limits it declares, on receive, with
//! `limit_exceeded`, whatever else would refuse it.

use std::num::NonZeroUsize;

use rdlt_connector::serve::Served;
use rdlt_connector::{
    Cursor, Partition, ReadRequest, Role, Source as _, StreamName, partition_channel,
    source_factory,
};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{Connection, Options, RemoteSource};
use rdlt_wire::Limits;

use crate::support::served_within;

#[tokio::test]
async fn a_cursor_beyond_the_sources_limit_is_refused_as_exceeding_it() {
    let limits = Limits {
        cursor_bytes: 64,
        ..Limits::default()
    };
    let served = Served::new().with_source(source_factory::<MemorySource>());
    let config = serde_json::json!({ "streams": { "items": [{ "id": 1 }] } });
    let connection = Connection::connect(
        served_within(served, limits),
        Role::Source,
        &config,
        Options::default(),
    )
    .await
    .expect("the source handshakes");
    let (sink, _feed) = partition_channel(NonZeroUsize::new(4).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("items").expect("a valid stream name"),
        Partition::single(),
        Some(Cursor::new(1, vec![b'x'; 65].into()).expect("a cursor within the host's")),
    );
    let error = RemoteSource::new(connection)
        .read(request, sink)
        .await
        .expect_err("the read is refused");
    assert_eq!(error.code(), Some("limit_exceeded"), "{error}");
    assert_eq!(
        error.limit().map(|limit| (limit.name, limit.limit)),
        Some(("cursor bytes", 64))
    );
}
