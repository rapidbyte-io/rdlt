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
        Some(Cursor::new(1, &[b'x'; 65]).expect("a cursor within the host's")),
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

/// Limits a byte, a row or a value under each of the protocol's minimums, with the limit's name.
fn below_the_minimums() -> [(Limits, &'static str, u64); 3] {
    use rdlt_wire::limits::{MIN_BATCH_ROWS, MIN_BATCH_VALUES, MIN_FRAME_BYTES};
    [
        (
            Limits {
                frame_bytes: MIN_FRAME_BYTES - 1,
                ..Limits::default()
            },
            "frame bytes",
            MIN_FRAME_BYTES,
        ),
        (
            Limits {
                batch_rows: MIN_BATCH_ROWS - 1,
                ..Limits::default()
            },
            "batch rows",
            MIN_BATCH_ROWS,
        ),
        (
            Limits {
                batch_values: MIN_BATCH_VALUES - 1,
                ..Limits::default()
            },
            "batch values",
            MIN_BATCH_VALUES,
        ),
    ]
}

/// The limits at every minimum.
fn at_the_minimums() -> Limits {
    use rdlt_wire::limits::{MIN_BATCH_ROWS, MIN_BATCH_VALUES, MIN_FRAME_BYTES};
    Limits {
        frame_bytes: MIN_FRAME_BYTES,
        batch_rows: MIN_BATCH_ROWS,
        batch_values: MIN_BATCH_VALUES,
        ..Limits::default()
    }
}

#[tokio::test]
async fn a_connector_whose_limits_are_below_the_protocols_minimums_is_refused_at_the_handshake() {
    let config = serde_json::json!({ "streams": { "items": [] } });
    for (limits, name, minimum) in below_the_minimums() {
        let served = Served::new().with_source(source_factory::<MemorySource>());
        let io = served_within(served, limits);
        let error = Connection::connect(io, Role::Source, &config, Options::default())
            .await
            .expect_err("the handshake is refused");
        assert_eq!(error.code(), Some("limit_below_minimum"), "{error}");
        assert_eq!(
            error
                .limit()
                .map(|limit| (limit.name, limit.limit, limit.actual)),
            Some((name, minimum, minimum - 1))
        );
    }
    let served = Served::new().with_source(source_factory::<MemorySource>());
    let io = served_within(served, at_the_minimums());
    Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("limits at the minimums are admitted");
}

#[tokio::test]
async fn a_host_whose_limits_are_below_the_protocols_minimums_is_refused_at_the_handshake() {
    let config = serde_json::json!({ "streams": { "items": [] } });
    let within = |limits| Options {
        limits,
        ..Options::default()
    };
    for (limits, name, minimum) in below_the_minimums() {
        let served = Served::new().with_source(source_factory::<MemorySource>());
        let io = served_within(served, Limits::default());
        let error = Connection::connect(io, Role::Source, &config, within(limits))
            .await
            .expect_err("the handshake is refused");
        assert_eq!(error.code(), Some("limit_below_minimum"), "{error}");
        assert_eq!(
            error
                .limit()
                .map(|limit| (limit.name, limit.limit, limit.actual)),
            Some((name, minimum, minimum - 1))
        );
    }
    let served = Served::new().with_source(source_factory::<MemorySource>());
    let io = served_within(served, Limits::default());
    Connection::connect(io, Role::Source, &config, within(at_the_minimums()))
        .await
        .expect("limits at the minimums are admitted");
}

/// The fake source answering bloated, connected with `limits`.
async fn bloated(limits: Limits) -> RemoteSource {
    use crate::support::fake::{Fake, Fault, serve_fake};
    let options = Options {
        limits,
        ..Options::default()
    };
    // 64 KiB of entries on the wire, each of which decodes to tens of bytes.
    let io = serve_fake(Fake(Fault::Bloats(32 * 1024)));
    let connection = Connection::connect(io, Role::Source, &serde_json::json!({}), options)
        .await
        .expect("the fake handshakes");
    RemoteSource::new(connection)
}

#[tokio::test]
async fn answers_beyond_their_class_limit_are_refused_before_they_are_decoded() {
    let stream = StreamName::new("items").expect("a valid stream name");
    let state = rdlt_connector::StreamState::default();
    // Within its limit, an answer is decoded, and its first empty entry refused.
    let source = bloated(Limits::default()).await;
    let decoded = source
        .discover()
        .await
        .expect_err("an empty stream is refused");
    assert_eq!(decoded.code(), Some("invalid_message"), "{decoded}");
    let decoded = source.plan(&stream, &state).await.expect_err("refused");
    assert_eq!(decoded.code(), Some("invalid_message"), "{decoded}");
    let small = Limits {
        catalog_bytes: 32 * 1024,
        state_bytes: 32 * 1024,
        ..Limits::default()
    };
    let source = bloated(small).await;
    let refused = source.discover().await.expect_err("the catalog is refused");
    assert!(refused.to_string().contains("too large"), "{refused}");
    let refused = source
        .plan(&stream, &state)
        .await
        .expect_err("the plan is refused");
    assert!(refused.to_string().contains("too large"), "{refused}");
}

#[tokio::test]
async fn a_request_beyond_its_class_limit_is_refused_by_the_served_connector() {
    use rdlt_wire::v1;
    let limits = Limits {
        state_bytes: 32 * 1024,
        ..Limits::default()
    };
    let served = Served::new().with_source(source_factory::<MemorySource>());
    let mut client = crate::support::raw_client(served_within(served, limits)).await;
    let cursors = (0..4096)
        .map(|partition| v1::CommittedCursor {
            partition: format!("p{partition}"),
            cursor: None,
        })
        .collect();
    let request = v1::CommittedRequest {
        stream: None,
        cursors,
    };
    let refused = client.committed(request).await.expect_err("refused");
    assert_eq!(refused.code(), tonic::Code::OutOfRange, "{refused}");
    assert!(refused.message().contains("too large"), "{refused}");
}

#[tokio::test]
async fn a_state_key_beyond_the_control_string_limit_is_refused_where_it_is_received() {
    use crate::support::fake::{Fake, Fault, serve_fake};
    use rdlt_connector::{Destination as _, LoadId, OpenContext, PipelineId};
    let opened = |key: fn() -> String| async move {
        let io = serve_fake(Fake(Fault::Keys(key)));
        let connection = Connection::connect(
            io,
            Role::Destination,
            &serde_json::json!({}),
            Options::default(),
        )
        .await
        .expect("the fake connects");
        let destination =
            rdlt_host::RemoteDestination::new(connection).expect("the fake declares capabilities");
        let context = OpenContext {
            pipeline: PipelineId::parse("keyed").expect("a pipeline id"),
            load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
        };
        destination
            .open(&context)
            .await
            .map(|opened| opened.state.len())
    };
    assert_eq!(opened(|| "k".repeat(64 * 1024)).await.ok(), Some(1));
    let refused = opened(|| "k".repeat(64 * 1024 + 1))
        .await
        .expect_err("the key is refused");
    assert_eq!(refused.code(), Some("limit_exceeded"), "{refused}");
    assert_eq!(
        refused.limit().map(|limit| limit.name),
        Some("control string bytes")
    );
}
