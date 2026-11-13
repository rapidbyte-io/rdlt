//! The document beside the example places the connector's own binary in the sandbox, granted the
//! network, as `rdlt run` places it.

use std::path::Path;

use rdlt::{Access, Host, Pipeline};
use rdlt_connector_reference::SqliteDestination;
use serde_json::json;

use crate::runs::count;
use crate::server::Api;

/// The codes a build fails with where no sandbox can be made here.
const NO_SANDBOX: [&str; 3] = [
    "sandbox_missing",
    "sandbox_unavailable",
    "sandbox_unsupported",
];

#[tokio::test]
async fn the_document_spawns_the_binary_sandboxed_with_the_network() {
    let api = Api::serve(30);
    let directory = tempfile::tempdir().unwrap();
    let (connectors, data) = (
        directory.path().join("connectors"),
        directory.path().join("data"),
    );
    std::fs::create_dir(&connectors).unwrap();
    std::fs::create_dir(&data).unwrap();
    let binary = env!("CARGO_BIN_EXE_rdlt-connector-pokeapi");
    std::fs::copy(binary, connectors.join("rdlt-connector-pokeapi")).unwrap();
    let example = Path::new(env!("CARGO_MANIFEST_DIR"));
    let built = Pipeline::from_path(example.join("rdlt.yaml"))
        .await
        .unwrap();
    // The document as written, but for its root and its database, which the test serves and keeps.
    let mut document = built.document().clone();
    document.source.config["base_url"] = json!(api.url);
    document.destination.config["path"] = json!(data.join("pokeapi.db"));
    // Compiled in here, the destination is given no sandbox's access.
    document.destination.access = Access::new();
    let host = Host::new()
        .connectors(&connectors)
        .allow_network()
        .destination::<SqliteDestination>();
    // macOS has no sandbox: the binary runs there with the test's own access.
    let host = if cfg!(target_os = "macos") {
        host.trust_binaries()
    } else {
        host
    };
    let pipeline = match Pipeline::from_document(document).build(&host).await {
        Ok(pipeline) => pipeline,
        Err(error)
            if error
                .code()
                .is_some_and(|code| NO_SANDBOX.contains(&code.as_str())) =>
        {
            return without_sandbox(&error);
        }
        Err(error) => panic!("{}", caused(&error)),
    };
    let outcome = pipeline.start().await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    // Each pokemon's one type, two abilities and three stats landed in child tables.
    let database = data.join("pokeapi.db");
    let rows = |table: &str| count(&database, &format!(r#"SELECT count(*) FROM "{table}""#));
    let tables = [
        "pokemon",
        "pokemon__types",
        "pokemon__abilities",
        "pokemon__stats",
        "types",
    ];
    assert_eq!(tables.map(rows), [30, 30, 60, 90, 21]);
}

/// Says why the test does not run here, or fails it where `RDLT_REQUIRE_SANDBOX` asks for a
/// sandbox, as CI does on Linux.
fn without_sandbox(why: &rdlt::Error) {
    let required = std::env::var_os("RDLT_REQUIRE_SANDBOX").is_some_and(|set| set != "0");
    assert!(
        !required,
        "a sandbox is required here, and none can be made: {why}"
    );
    eprintln!("skipped: no sandbox can be made here: {why}");
}

/// `error` and every error that caused it, each after the error it caused.
fn caused(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(error) = cause {
        text = format!("{text}: {error}");
        cause = error.source();
    }
    text
}
