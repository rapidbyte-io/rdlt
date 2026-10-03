//! A message within its class's bytes on the wire that would decode to far more is refused
//! before it is decoded, on either end, and what the refusal held at its peak is within the
//! bound its class states.

use bytes::Bytes;
use rdlt_connector::{Destination as _, LoadId, OpenContext, PipelineId, Role, Source as _};
use rdlt_host::{Connection, Options, RemoteDestination, RemoteSource};
use rdlt_wire::Limits;
use rdlt_wire::limits::Class;

use crate::HEAP;
use crate::support::fake::Fault;
use crate::support::raw::{answering, called};

/// `entry`, an empty entry of a repeated field, as many times as `bytes` take.
fn entries(entry: [u8; 2], bytes: usize) -> Bytes {
    Bytes::from(entry.repeat(bytes / 2))
}

/// A length-delimited field `number` of `payload`, its key a byte.
fn field(number: u8, payload: &[u8]) -> Vec<u8> {
    let mut field = vec![number << 3 | 2];
    let mut length = payload.len();
    while length >= 0x80 {
        field.push(u8::try_from(length & 0x7f).expect("seven bits") | 0x80);
        length >>= 7;
    }
    field.push(u8::try_from(length).expect("seven bits"));
    field.extend_from_slice(payload);
    field
}

/// An empty group of field 1000, which protocol buffers' decoder skips, then `message`.
fn grouped(message: &[u8]) -> Bytes {
    [&[0xc3, 0x3e, 0xc4, 0x3e][..], message].concat().into()
}

/// Runs `call`, and returns what it ended with and what the heap held at its peak meanwhile,
/// beyond what it held before.
async fn peaked<T>(call: impl Future<Output = T>) -> (T, usize) {
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let ended = call.await;
    (ended, HEAP.peak_usage().saturating_sub(before))
}

/// Asserts that `refused` failed as too large, holding no more than a message of `class` takes
/// on the wire and may hold decoded together.
fn within(refused: &str, peak: usize, class: Class) {
    assert!(refused.contains("too large"), "{refused}");
    let limits = Limits::default();
    let bound = limits.decoding(class) + limits.decoded(class);
    assert!(peak <= bound, "held {peak} bytes, beyond {bound}");
}

/// The fake source answering `method` with `payload`.
async fn source(method: &'static str, payload: Bytes) -> RemoteSource {
    let io = answering(Fault::Bloats(0), method, &payload);
    let config = serde_json::json!({});
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("the fake handshakes");
    RemoteSource::new(connection)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_of_empty_streams_at_its_bytes_is_refused_within_its_bound() {
    let limit = usize::try_from(Limits::default().catalog_bytes).unwrap();
    let source = source("Discover", entries([0x0a, 0x00], limit)).await;
    let (refused, peak) = peaked(source.discover()).await;
    let refused = refused.expect_err("the catalog is refused");
    within(&refused.to_string(), peak, Class::Catalog);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_behind_a_group_the_decoder_skips_is_refused_within_its_bound() {
    let limit = usize::try_from(Limits::default().catalog_bytes).unwrap();
    let streams = entries([0x0a, 0x00], limit - 4);
    let source = source("Discover", grouped(&streams)).await;
    let (refused, peak) = peaked(source.discover()).await;
    let refused = refused.expect_err("the catalog is refused");
    within(&refused.to_string(), peak, Class::Catalog);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_frame_counted_beyond_its_bound_is_refused_within_it() {
    let limit = Limits::default().decoding(Class::Data);
    // A schema frame whose schema comes again and again, empty.
    let schema = field(1, &[0x12, 0x00].repeat((limit - 16) / 2));
    let source = source("Read", schema.into()).await;
    let (sink, _feed) = rdlt_connector::partition_channel(std::num::NonZeroUsize::MIN);
    let request = rdlt_connector::ReadRequest::new(
        rdlt_connector::StreamName::new("s").expect("a name"),
        rdlt_connector::Partition::single(),
        None,
    );
    let (refused, peak) = peaked(source.read(request, sink)).await;
    let refused = refused.expect_err("the frame is refused");
    within(&refused.to_string(), peak, Class::Data);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_plan_of_empty_starts_at_its_bytes_is_refused_within_its_bound() {
    let limit = usize::try_from(Limits::default().state_bytes).unwrap();
    let source = source("Plan", entries([0x1a, 0x00], limit)).await;
    let stream = rdlt_connector::StreamName::new("s").expect("a name");
    let state = rdlt_connector::StreamState::default();
    let (refused, peak) = peaked(source.plan(&stream, &state)).await;
    let refused = refused.expect_err("the plan is refused");
    within(&refused.to_string(), peak, Class::State);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_open_answering_empty_state_at_its_bytes_is_refused_within_its_bound() {
    let limit = usize::try_from(Limits::default().state_bytes).unwrap();
    let io = answering(
        Fault::Keys(String::new),
        "Open",
        &entries([0x1a, 0x00], limit),
    );
    let config = serde_json::json!({});
    let connection = Connection::connect(io, Role::Destination, &config, Options::default())
        .await
        .expect("the fake handshakes");
    let destination = RemoteDestination::new(connection).expect("its capabilities are taken");
    let context = OpenContext {
        pipeline: PipelineId::parse("bloated").expect("a pipeline id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let (refused, peak) = peaked(destination.open(&context)).await;
    let Err(refused) = refused else {
        panic!("the open is refused");
    };
    within(&refused.to_string(), peak, Class::State);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_commit_of_empty_child_tables_is_refused_by_the_served_connector_within_its_bound() {
    let limit = usize::try_from(Limits::default().state_bytes).unwrap();
    // A session, and the commit's meta: child tables, each empty, near the state's bytes.
    let children = entries([0x3a, 0x00], limit - 16);
    let io = destination(Limits::default());
    let (refused, peak) = peaked(called(io, "Commit", commit(&children))).await;
    let refused = refused.expect_err("the commit is refused");
    assert_eq!(refused.code(), tonic::Code::OutOfRange, "{refused}");
    within(refused.message(), peak, Class::State);
}

/// The memory destination, served, with `limits`.
fn destination(limits: Limits) -> tokio::net::UnixStream {
    use rdlt_connector::serve::Served;
    let served = Served::new().with_destination(rdlt_connector::destination_factory::<
        rdlt_connector_reference::MemoryDestination,
    >());
    crate::support::served_within(served, limits)
}

/// A commit of a session, its meta `meta`.
fn commit(meta: &[u8]) -> Bytes {
    [&[0x08, 0x01][..], &field(2, meta)].concat().into()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_commit_behind_a_group_the_decoder_skips_is_refused_within_its_bound() {
    let limit = usize::try_from(Limits::default().state_bytes).unwrap();
    let children = entries([0x3a, 0x00], limit - 32);
    let payload = grouped(&commit(&children));
    let io = destination(Limits::default());
    let (refused, peak) = peaked(called(io, "Commit", payload)).await;
    let refused = refused.expect_err("the commit is refused");
    assert_eq!(refused.code(), tonic::Code::OutOfRange, "{refused}");
    within(refused.message(), peak, Class::State);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_starting_with_a_table_path_of_empty_segments_is_refused_within_its_bound() {
    let limit = Limits::default().decoding(Class::Data);
    let path = field(1, &[0x0a, 0x00].repeat((limit - 32) / 2));
    let start = [&[0x08, 0x01][..], &field(2, &path)].concat();
    let frame = field(1, &start);
    let io = destination(Limits::default());
    let (refused, peak) = peaked(crate::support::raw::streamed(io, "Write", frame.into())).await;
    let refused = refused.expect_err("the write is refused");
    assert_eq!(refused.code(), tonic::Code::OutOfRange, "{refused}");
    within(refused.message(), peak, Class::Data);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_state_message_beyond_the_largest_frame_is_taken_where_the_state_limit_allows_it() {
    let least = rdlt_wire::limits::MIN_FRAME_BYTES;
    let default = rdlt_wire::limits::FRAME_BYTES;
    // The least frame and twice it of state, and the default frame and more state than that.
    for (frame, state) in [(least, 2 * least), (default, default + (16 << 20))] {
        let limits = Limits {
            frame_bytes: frame,
            state_bytes: state,
            ..Limits::default()
        };
        let beyond = usize::try_from(frame).unwrap() + (1 << 20);
        // A report of committed positions of a stream whose name is larger than a frame.
        let report = field(1, &field(2, &vec![b's'; beyond])).into();
        let answered = called(destination(limits), "Committed", report).await;
        // It reaches the connector, which refuses a report before any handshake: not its size.
        let refused = answered.expect_err("no handshake came first");
        assert_ne!(refused.code(), tonic::Code::OutOfRange, "{refused}");
        assert!(!refused.message().contains("too large"), "{refused}");
    }
}
