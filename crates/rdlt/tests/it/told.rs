//! What a placed connector is told: the limits the engine admits within for its pipeline's
//! resources, whether it is spawned or reached remotely, in discovery as in a run.

#[cfg(feature = "local")]
use std::path::Path;

use rdlt::arrow::array::AsArray as _;
use rdlt::arrow::datatypes::Int64Type;
use rdlt::{Code, Connector, Host, Pipeline, PipelineBuilder, Resources, Side, Stream};
use rdlt_connector_reference::MemoryDestination;
use rdlt_engine::EngineConfig;
use rdlt_wire::Limits;

use crate::support::{example, within};

/// Bytes: the memory the pipelines here are given, with more partitions than the engine's
/// default, so what the engine admits is below what the wire and the engine's defaults admit.
const MEMORY: u64 = 64 << 20;

/// Partitions the pipelines here read at once.
const PARTITIONS: usize = 32;

/// The limits the engine admits within for the pipelines here: neither the wire's defaults nor
/// the engine's, so a connector told either fails the tests.
fn admitted() -> Limits {
    let config = EngineConfig::builder()
        .memory(MEMORY)
        .partitions(PARTITIONS);
    let admitted = config.build().expect("a valid configuration").limits();
    assert_ne!(admitted, Limits::default());
    assert_ne!(admitted, EngineConfig::default().limits());
    admitted
}

/// Pipeline `name` loading `source`'s `limits` into memory store `name`, its catalog padded
/// with `padding` streams, with the resources above.
fn told(name: &str, source: Connector, padding: usize) -> PipelineBuilder {
    let source = Side::new(source).config(serde_json::json!({ "padding": padding }));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": name }));
    Pipeline::builder(name, source, destination)
        .stream(Stream::new("limits"))
        .resources(Resources::new().memory(MEMORY).partitions(PARTITIONS))
}

/// The limits pipeline `name`'s read was told on `host`, as the row it loaded.
async fn told_in_a_run(host: &Host, name: &str, source: Connector) -> Limits {
    let pipeline = within(told(name, source, 0).build(host))
        .await
        .expect("built");
    let outcome = within(pipeline.start()).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let published = rdlt_connector_reference::published(name, "limits");
    let [row] = &published[..] else {
        panic!("one batch: {published:?}");
    };
    let told = |limit: &str| {
        let column = row.column_by_name(limit).expect("a column a limit");
        u64::try_from(column.as_primitive::<Int64Type>().value(0)).expect("a limit")
    };
    Limits {
        frame_bytes: told("frame_bytes"),
        batch_rows: told("batch_rows"),
        schema_columns: told("schema_columns"),
        nesting_depth: told("nesting_depth"),
        json_push_bytes: told("json_push_bytes"),
        cursor_bytes: told("cursor_bytes"),
        config_bytes: told("config_bytes"),
        control_string_bytes: told("control_string_bytes"),
        batch_values: told("batch_values"),
        schema_bytes: told("schema_bytes"),
        dictionary_bytes: told("dictionary_bytes"),
        catalog_bytes: told("catalog_bytes"),
        state_bytes: told("state_bytes"),
        control_message_bytes: told("control_message_bytes"),
    }
}

/// A host spawning the told connector, trusted, from a copy of it in `dir`, and running the
/// memory destination in process.
#[cfg(feature = "local")]
fn spawning(dir: &Path) -> Host {
    let copy = dir.join("rdlt-connector-told");
    std::fs::copy(example("told_connector"), copy).expect("the connector is copied");
    Host::new()
        .trust_binaries()
        .connectors(dir)
        .destination::<MemoryDestination>()
}

#[cfg(feature = "local")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spawned_connector_is_told_the_limits_its_pipeline_admits() {
    let dir = tempfile::tempdir().unwrap();
    let host = spawning(dir.path());
    let told = told_in_a_run(&host, "told-spawned", Connector::new("test.told")).await;
    assert_eq!(told, admitted());
}

#[cfg(feature = "remote")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_connector_is_told_the_limits_its_pipeline_admits() {
    let pki = rdlt_testkit::tls::Pki::new("ca");
    let binary = example("told_connector");
    let (_connector, endpoint) = within(rdlt_testkit::tls::listening(&pki, &binary)).await;
    let client = pki.client("host");
    let tls = rdlt::Tls::from_files(&client.cert, &client.key, pki.ca())
        .await
        .expect("the host's TLS files open");
    let host = Host::new().remote(tls).destination::<MemoryDestination>();
    let source = Connector::new("test.told").endpoint(&endpoint);
    let told = told_in_a_run(&host, "told-remote", source).await;
    assert_eq!(told, admitted());
}

#[cfg(feature = "local")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_holds_a_catalog_to_the_limits_its_pipeline_admits() {
    let dir = tempfile::tempdir().unwrap();
    let host = spawning(dir.path());
    // Streams of 256-byte names, more bytes than the pipeline admits in a catalog and fewer than
    // the engine's defaults admit.
    let padding = usize::try_from(admitted().catalog_bytes).unwrap() / 256 + 1;
    let source = || Connector::new("test.told");
    let refused = within(told("told-discovered", source(), padding).discover(&host)).await;
    let refused = refused.expect_err("the catalog is beyond what the pipeline admits");
    assert_eq!(
        refused.code().map(Code::as_str),
        Some("connector_failed"),
        "{refused}"
    );
    let defaults = told("told-discovered", source(), padding).resources(Resources::new());
    within(defaults.discover(&host))
        .await
        .expect("the catalog is within what the engine's defaults admit");
}
