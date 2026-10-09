//! The reference connectors' binaries, spawned as a host places them.

use rdlt_connector::wire::v1;
use rdlt_connector::{ConnectorId, RoleFactory, Serve};
use rdlt_connector_reference::{GeneratorSource, SqliteDestination};
use rdlt_host::remote::client;
use rdlt_host::{ConnectorRef, Local, Options, Provider as _};
use serde_json::json;

fn reference(id: &str, binary: &str) -> ConnectorRef {
    ConnectorRef::new(ConnectorId::parse(id).expect("a valid id")).path(binary)
}

#[tokio::test(flavor = "multi_thread")]
async fn each_binary_serves_its_connectors_in_their_roles() {
    let local = Local::trusting_binaries().env_passthrough("LLVM_PROFILE_FILE");
    let dir = crate::fixtures::tempdir().expect("a temporary directory");
    let sources = [
        (
            "io.rapidbyte.memory",
            env!("CARGO_BIN_EXE_rdlt-connector-memory"),
            json!({ "streams": {} }),
        ),
        (
            "io.rapidbyte.files",
            env!("CARGO_BIN_EXE_rdlt-connector-files"),
            json!({ "root": dir.path() }),
        ),
        (
            "io.rapidbyte.generator",
            env!("CARGO_BIN_EXE_rdlt-connector-generator"),
            json!({ "seed": 1, "streams": [] }),
        ),
    ];
    for (id, binary, config) in sources {
        let placed = local.source(&reference(id, binary), &config).await;
        let placed = placed.unwrap_or_else(|error| panic!("{id}: {error}"));
        assert_eq!(placed.spec.id.as_str(), id);
        placed
            .connector
            .check()
            .await
            .unwrap_or_else(|error| panic!("{id}: {error}"));
    }
    let destinations = [
        (
            "io.rapidbyte.memory",
            env!("CARGO_BIN_EXE_rdlt-connector-memory"),
            json!({ "store": "binaries" }),
        ),
        (
            "io.rapidbyte.files",
            env!("CARGO_BIN_EXE_rdlt-connector-files"),
            json!({ "root": dir.path(), "format": "jsonl" }),
        ),
        (
            "io.rapidbyte.sqlite",
            env!("CARGO_BIN_EXE_rdlt-connector-sqlite"),
            json!({ "path": dir.path().join("binaries.db") }),
        ),
    ];
    for (id, binary, config) in destinations {
        let placed = local.destination(&reference(id, binary), &config).await;
        let placed = placed.unwrap_or_else(|error| panic!("{id}: {error}"));
        assert_eq!(placed.spec.id.as_str(), id);
        placed
            .connector
            .check()
            .await
            .unwrap_or_else(|error| panic!("{id}: {error}"));
    }
}

#[test]
fn a_connector_is_servable_by_its_type_in_its_role() {
    assert_eq!(
        format!("{:?}", GeneratorSource::factory()),
        "Source(ConnectorId(\"io.rapidbyte.generator\"))"
    );
    assert_eq!(
        format!("{:?}", SqliteDestination::factory()),
        "Destination(ConnectorId(\"io.rapidbyte.sqlite\"))"
    );
    assert!(matches!(
        SqliteDestination::factory(),
        RoleFactory::Destination(_)
    ));
}

/// A handshake as a destination's host that offers the feature certification reads back with.
fn offering_read_back() -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        protocol_major: rdlt_wire::PROTOCOL_MAJOR,
        features: vec![rdlt_wire::PUBLISHED.to_owned()],
        role: v1::Role::Destination as i32,
        limits: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shipped_destination_binary_reads_nothing_back_whatever_its_host_offers() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let destinations = [
        (
            "io.rapidbyte.sqlite",
            env!("CARGO_BIN_EXE_rdlt-connector-sqlite"),
            json!({ "path": dir.path().join("shipped.db") }),
        ),
        (
            "io.rapidbyte.files",
            env!("CARGO_BIN_EXE_rdlt-connector-files"),
            json!({ "root": dir.path(), "format": "jsonl" }),
        ),
    ];
    for (id, binary, config) in destinations {
        let local = Local::trusting_binaries().env_passthrough("LLVM_PROFILE_FILE");
        let wire = local
            .wire(&reference(id, binary))
            .await
            .expect("the binary starts");
        let mut client = client(wire, Options::default())
            .await
            .expect("the binary connects");
        let agreed = client.rpc.handshake(offering_read_back()).await;
        let agreed = agreed.expect("the handshake succeeds").into_inner();
        assert!(agreed.accepted_features.is_empty(), "{id}");
        let configured = v1::ConfigureRequest {
            config_json: config.to_string(),
        };
        client
            .rpc
            .configure(configured)
            .await
            .expect("it is configured");
        let table = v1::TableRef {
            path: Some(v1::TablePath {
                segments: vec!["rows".to_owned()],
            }),
            name: "rows".to_owned(),
            version: 1,
            ..v1::TableRef::default()
        };
        let request = v1::ReadPublishedRequest { table: Some(table) };
        let Err(refused) = client.read_published(request).await else {
            panic!("{id} read a table back");
        };
        assert_eq!(
            refused.code(),
            rdlt_wire::tonic::Code::Unimplemented,
            "{id}"
        );
    }
}

#[test]
fn the_test_connectors_binaries_are_built_only_on_request() {
    let manifest = include_str!("../../Cargo.toml");
    let binaries: Vec<&str> = manifest.split("[[bin]]").skip(1).collect();
    for name in ["rdlt-connector-generator", "rdlt-connector-memory"] {
        let binary = binaries
            .iter()
            .find(|binary| binary.contains(&format!("name = \"{name}\"")));
        let binary = binary.unwrap_or_else(|| panic!("{name} is declared"));
        let declared = binary.split("\n\n").next().expect("its table");
        assert!(
            declared.contains("required-features = [\"test-connectors\"]"),
            "{name}: {declared}"
        );
    }
    // The binaries a deployment runs are those left: built without a feature.
    for shipped in ["rdlt-connector-sqlite", "rdlt-connector-files"] {
        assert!(
            !manifest.contains(&format!("name = \"{shipped}\"")),
            "{shipped}"
        );
    }
}
