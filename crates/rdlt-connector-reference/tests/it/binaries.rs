//! The reference connectors' binaries, spawned as a host places them.

use rdlt_connector::{ConnectorId, RoleFactory, Serve};
use rdlt_connector_reference::{GeneratorSource, SqliteDestination};
use rdlt_host::{ConnectorRef, Local, Provider as _};
use serde_json::json;

fn reference(id: &str, binary: &str) -> ConnectorRef {
    ConnectorRef::new(ConnectorId::parse(id).expect("a valid id")).path(binary)
}

#[tokio::test(flavor = "multi_thread")]
async fn each_binary_serves_its_connectors_in_their_roles() {
    let local = Local::new().env_passthrough("LLVM_PROFILE_FILE");
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
