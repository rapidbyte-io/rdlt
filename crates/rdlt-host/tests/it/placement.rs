//! Placement as policy: a reference states what it requires, and each provider honours every
//! requirement it is given or refuses the reference, before anything is spawned, dialed or
//! connected.

use rdlt_connector::ConnectorId;
use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{
    Connect, ConnectorRef, Digest, Isolation, Kills, Local, Provider, ProviderError, Registry,
    Remote, Stream,
};
use rdlt_testkit::tls::Pki;
use rdlt_wire::Limits;

use crate::network::{identity, listening, port};
use crate::process::{example, local, scripted};

/// Each requirement a reference may state beyond its id and version, on `reference`.
fn requiring(reference: &ConnectorRef) -> [(&'static str, ConnectorRef); 6] {
    let from = || reference.clone();
    [
        ("a path", from().path(example("scripted_connector"))),
        ("an endpoint", from().endpoint("grpcs://localhost:1")),
        ("a digest", from().digest(Digest([7; 32]))),
        ("an isolation", from().isolation(Isolation::Process)),
        ("an isolation", from().isolation(Isolation::Sandbox)),
        ("an isolation", from().isolation(Isolation::Remote)),
    ]
}

/// Whether `placed` was refused for requiring `requirement` of `placement`.
fn unsupported<T>(
    placed: Result<T, ProviderError>,
    requirement: &str,
    placement: &str,
) -> Result<(), String> {
    match placed {
        Err(error @ ProviderError::Unsupported { .. }) => {
            let ProviderError::Unsupported {
                requirement: found,
                placement: by,
                ..
            } = &error
            else {
                unreachable!("matched above");
            };
            if (*found, *by) != (requirement, placement) || error.code() != "placement_unsupported"
            {
                return Err(format!("refused for {found} by {by}"));
            }
            Ok(())
        }
        Err(other) => Err(format!("failed otherwise: {other}")),
        Ok(_) => Err("placed".to_owned()),
    }
}

#[tokio::test]
async fn in_process_placement_refuses_every_requirement_it_cannot_honour() {
    let registry = Registry::trusted().trusted_source::<MemorySource>();
    let memory = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("a valid id"));
    let config = serde_json::json!({ "streams": {} });
    registry
        .source(&memory, &config)
        .await
        .expect("a bare reference is placed");
    for (requirement, reference) in requiring(&memory) {
        let placed = registry.source(&reference, &config).await;
        unsupported(placed, requirement, "in-process")
            .unwrap_or_else(|why| panic!("{requirement}: {why}"));
    }
}

#[tokio::test]
async fn process_placement_refuses_an_endpoint_and_an_isolation_it_does_not_give() {
    let config = serde_json::json!({});
    let refused = [
        ("an endpoint", scripted().endpoint("grpcs://localhost:1")),
        ("an isolation", scripted().isolation(Isolation::Remote)),
        // Binaries stated to be trusted run in no sandbox.
        ("an isolation", scripted().isolation(Isolation::Sandbox)),
    ];
    for (requirement, reference) in refused {
        let placed = local().source(&reference, &config).await;
        unsupported(placed, requirement, "process")
            .unwrap_or_else(|why| panic!("{requirement}: {why}"));
        let wired = local().wire(&reference).await;
        unsupported(wired, requirement, "process")
            .unwrap_or_else(|why| panic!("{requirement}: {why}"));
        let resolved = local().resolve(&reference);
        unsupported(resolved, requirement, "process")
            .unwrap_or_else(|why| panic!("{requirement}: {why}"));
    }
    let process = scripted().isolation(Isolation::Process);
    local()
        .source(&process, &config)
        .await
        .expect("a process is what it gives");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_raw_wire_spawns_only_a_binary_of_the_digest_its_reference_requires() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let pid_file = dir.path().join("pid");
    let pinned = scripted().digest(Digest([0; 32]));
    let wired = local().wire(&pinned).await;
    assert!(
        matches!(wired, Err(ProviderError::DigestMismatch { .. })),
        "{wired:?}"
    );
    assert_eq!(wired.expect_err("refused").code(), "digest_mismatch");
    // Nothing ran: a connector that had would have a process to list.
    assert!(rdlt_host::spawned().is_empty() && !pid_file.exists());
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn a_digest_is_refused_where_an_open_file_cannot_be_executed() {
    // Hashing a path and then executing it would check one file and run another.
    let pinned = scripted().digest(Digest([0; 32]));
    let config = serde_json::json!({});
    let placed = local().source(&pinned, &config).await;
    unsupported(placed, "a digest", "process").expect("refused");
    let wired = local().wire(&pinned).await;
    unsupported(wired, "a digest", "process").expect("refused");
    // And none is reported for a binary run by its path.
    let placed = local()
        .source(&scripted(), &config)
        .await
        .expect("it starts");
    assert_eq!(placed.digest, None);
}

#[tokio::test]
async fn remote_placement_refuses_a_path_a_digest_and_an_isolation_it_does_not_give() {
    let pki = Pki::new("ca");
    let (_connector, address) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let listening = crate::network::scripted(&endpoint);
    let config = serde_json::json!({});
    let refused = [
        (
            "a path",
            listening.clone().path(example("scripted_connector")),
        ),
        ("a digest", listening.clone().digest(Digest([7; 32]))),
        (
            "an isolation",
            listening.clone().isolation(Isolation::Process),
        ),
        (
            "an isolation",
            listening.clone().isolation(Isolation::Sandbox),
        ),
    ];
    for (requirement, reference) in refused {
        let placed = remote.source(&reference, &config).await;
        unsupported(placed, requirement, "remote")
            .unwrap_or_else(|why| panic!("{requirement}: {why}"));
        let wired = remote.wire(&reference).await;
        unsupported(wired, requirement, "remote")
            .unwrap_or_else(|why| panic!("{requirement}: {why}"));
    }
    let isolated = listening.isolation(Isolation::Remote);
    remote
        .source(&isolated, &config)
        .await
        .expect("another machine is what it gives");
}

#[tokio::test]
async fn a_connector_reached_through_a_function_is_placed_for_a_bare_reference_alone() {
    let served = std::sync::Arc::new(
        Served::new().with_source(rdlt_connector::source_factory::<MemorySource>()),
    );
    let connect = Connect::new(move || {
        let served = std::sync::Arc::clone(&served);
        Box::pin(async move {
            let (host, connector) = tokio::net::UnixStream::pair()?;
            tokio::spawn(serve_connection(served, connector, Limits::default()));
            Ok(Box::new(host) as Box<dyn Stream>)
        })
    });
    let memory = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("a valid id"));
    let config = serde_json::json!({ "streams": {} });
    connect
        .source(&memory, &config)
        .await
        .expect("a bare reference is placed");
    for (requirement, reference) in requiring(&memory) {
        let placed = connect.source(&reference, &config).await;
        unsupported(placed, requirement, "connected")
            .unwrap_or_else(|why| panic!("{requirement}: {why}"));
    }
}

#[tokio::test]
async fn a_requirement_is_refused_before_a_fallback_or_a_kill_is_reached() {
    // The registry knows the connector, so the fallback that could honour a path is not asked.
    let registry = Registry::trusted()
        .trusted_source::<MemorySource>()
        .fallback(Local::trusting_binaries().kills(&Kills::new()));
    let memory = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("a valid id"));
    let reference = memory.path(example("scripted_connector"));
    let placed = registry
        .source(&reference, &serde_json::json!({ "streams": {} }))
        .await;
    unsupported(placed, "a path", "in-process").expect("refused");
    assert!(rdlt_host::spawned().is_empty());
}
