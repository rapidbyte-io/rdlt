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

/// Runs `call`, and returns what it ended with and what the heap held at its peak meanwhile,
/// beyond what it held before.
async fn peaked<T>(call: impl Future<Output = T>) -> (T, usize) {
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let ended = call.await;
    (ended, HEAP.peak_usage().saturating_sub(before))
}

/// Asserts that `refused` failed as too large, holding no more than `class` may hold decoded.
fn within(refused: &str, peak: usize, class: Class) {
    assert!(refused.contains("too large"), "{refused}");
    let bound = Limits::default().decoded(class).expect("a bound");
    assert!(peak <= bound, "held {peak} bytes, beyond {bound}");
}

/// The fake source answering `method` with `payload`.
async fn source(method: &'static str, payload: Bytes) -> RemoteSource {
    let io = answering(Fault::Bloats(0), method, payload);
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
        entries([0x1a, 0x00], limit),
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
    use rdlt_connector::serve::Served;
    let limit = usize::try_from(Limits::default().state_bytes).unwrap();
    // A session, and the commit's meta: child tables, each empty, near the state's bytes.
    let children = entries([0x3a, 0x00], limit - 16);
    let mut payload = vec![0x08, 0x01, 0x12];
    let mut length = children.len();
    while length >= 0x80 {
        payload.push(u8::try_from(length & 0x7f).unwrap() | 0x80);
        length >>= 7;
    }
    payload.push(u8::try_from(length).unwrap());
    payload.extend_from_slice(&children);
    let served = Served::new().with_destination(rdlt_connector::destination_factory::<
        rdlt_connector_reference::MemoryDestination,
    >());
    let io = crate::support::served(served);
    let (refused, peak) = peaked(called(io, "Commit", payload.into())).await;
    let refused = refused.expect_err("the commit is refused");
    assert_eq!(refused.code(), tonic::Code::OutOfRange, "{refused}");
    within(refused.message(), peak, Class::State);
}
