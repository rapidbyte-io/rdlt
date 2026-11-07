use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, DestinationFactory, Role, Secret, SourceFactory, destination_factory, source_factory,
};
use rdlt_connector_reference::{MemoryDestination, MemorySource};
use rdlt_engine::{Code, ErrorKind, Origin};
use rdlt_host::{
    Placement, Provider as _, SecretFault, SecretKind, SecretReference, SecretResolver,
};

use super::Host;
use crate::{Access, Connector, Side};

/// A store holding one secret, `mask_key`.
#[derive(Debug)]
pub(crate) struct Vault;

impl SecretResolver for Vault {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            match (reference.kind, reference.name.as_str()) {
                (SecretKind::Named, "mask_key") => Ok(Secret::new("CANARY-mask".to_owned())),
                _ => Err(SecretFault::Missing),
            }
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_registered_connector_is_placed_in_process() {
    let host = Host::new()
        .source::<MemorySource>()
        .destination::<MemoryDestination>();
    let side = Side::new(Connector::new("io.rapidbyte.memory"));
    let reference = crate::lower::reference(&side, Role::Source).unwrap();
    let placed = host
        .provider()
        .source(&reference, &serde_json::json!({ "streams": {} }))
        .await
        .expect("placed");
    assert_eq!(placed.placement, Placement::InProcess);
    let placed = host
        .provider()
        .destination(&reference, &serde_json::json!({ "store": "host-placed" }))
        .await
        .expect("placed");
    assert_eq!(placed.placement, Placement::InProcess);
}

#[tokio::test(start_paused = true)]
async fn a_connector_given_as_a_runtime_factory_is_placed_in_process_by_its_spec_s_id() {
    let factory: Arc<dyn SourceFactory> = Arc::from(source_factory::<MemorySource>());
    let host = Host::new().source_factory(factory);
    let memory = Connector::new("io.rapidbyte.memory");
    let spoken = host.spec(&memory, Role::Source).await.expect("registered");
    assert_eq!(spoken.id.as_str(), "io.rapidbyte.memory");
    let destination: Arc<dyn DestinationFactory> =
        Arc::from(destination_factory::<MemoryDestination>());
    let host = host.destination_factory(destination);
    host.spec(&memory, Role::Destination)
        .await
        .expect("registered");
}

#[tokio::test(start_paused = true)]
async fn a_reference_with_an_endpoint_is_refused_where_the_host_has_no_remote() {
    let far = Connector::new("io.example.far").endpoint("grpcs://far.example:7443");
    let refused = Host::new()
        .spec(&far, Role::Source)
        .await
        .expect_err("refused");
    assert_eq!(refused.code(), Some(Code::new("placement_refused")));
    assert_eq!(
        (refused.kind(), refused.origin()),
        (ErrorKind::Config, Origin::Source)
    );
}

#[tokio::test(start_paused = true)]
async fn a_registered_connector_refuses_what_running_in_process_cannot_honour() {
    let host = Host::new().source::<MemorySource>();
    let memory = Connector::new("io.rapidbyte.memory");
    let versioned = host
        .spec(&memory.clone().version(">=99"), Role::Source)
        .await;
    let refused = versioned.expect_err("no such version");
    assert_eq!(refused.code(), Some(Code::new("version_mismatch")));
    let side = Side::new(memory).access(Access::new().write("/data"));
    let reference = crate::lower::reference(&side, Role::Source).unwrap();
    let placing = host
        .provider()
        .source(&reference, &serde_json::json!({ "streams": {} }))
        .await;
    let refused =
        crate::Error::placement(Role::Source, placing.err().expect("no access in process"));
    assert_eq!(refused.code(), Some(Code::new("placement_unsupported")));
}

#[test]
fn a_host_shows_what_it_allows() {
    let host = Host::new()
        .source::<MemorySource>()
        .destination::<MemoryDestination>()
        .allow_env(["DB_PASSWORD"]);
    let shown = format!("{host:?}");
    for part in ["io.rapidbyte.memory", "DB_PASSWORD"] {
        assert!(shown.contains(part), "{shown}");
    }
}

/// What `host` places local connectors with, as its `Debug` shows it.
#[cfg(feature = "local")]
fn local(host: &Host) -> String {
    format!("{:?}", host.local())
}

#[cfg(feature = "local")]
#[test]
fn a_host_runs_local_connectors_in_the_sandbox_unless_it_trusts_their_binaries() {
    assert!(
        local(&Host::new()).contains("sandbox: Some(Bubblewrap"),
        "{}",
        local(&Host::new())
    );
    let trusted = Host::new().trust_binaries();
    assert!(
        local(&trusted).contains("sandbox: None"),
        "{}",
        local(&trusted)
    );
}

#[cfg(feature = "local")]
#[test]
fn a_host_gives_local_placement_the_directories_and_roots_it_allows() {
    let host = Host::new()
        .connectors("/opt/rdlt/bin")
        .connectors("/opt/rdlt/more")
        .writable("/data")
        .readable("/in")
        .always_readable("/etc/ssl");
    let shown = local(&host);
    for part in [
        r#"dirs: ["/opt/rdlt/bin", "/opt/rdlt/more"]"#,
        r#"roots: Roots { read: ["/in"], write: ["/data"] }"#,
        r#"shared_reads: ["/etc/ssl"]"#,
        "sandbox: Some(Bubblewrap",
    ] {
        assert!(shown.contains(part), "{part} in {shown}");
    }
    let bare = local(&Host::new());
    for part in ["dirs: []", "read: []", "write: []", "shared_reads: []"] {
        assert!(bare.contains(part), "{part} in {bare}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_configuration_reaches_a_private_file_only_beneath_a_directory_the_host_allows() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("token");
    std::fs::write(&file, "CANARY-token").unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    let rows = serde_json::json!([{ "token": format!("${{file:{}}}", file.display()) }]);
    let config = serde_json::json!({ "streams": { "users": rows } });
    let side = Side::new(Connector::new("io.rapidbyte.memory"));
    let reference = crate::lower::reference(&side, Role::Source).unwrap();
    let allowed = Host::new()
        .allow_files([directory.path()])
        .source::<MemorySource>();
    let placed = allowed.provider().source(&reference, &config).await;
    assert!(placed.is_ok(), "the file is beneath an allowed directory");
    let unallowed = Host::new().source::<MemorySource>();
    let placing = unallowed.provider().source(&reference, &config).await;
    let refused = crate::Error::placement(Role::Source, placing.err().expect("refused"));
    assert_eq!(refused.code(), Some(Code::new("secret_refused")));
    assert!(!refused.report().causes.join(" ").contains("CANARY"));
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn a_host_given_tls_dials_a_reference_with_an_endpoint_presenting_its_files() {
    use std::time::Duration;

    let pki = rdlt_testkit::tls::Pki::new("ca");
    let identity = pki.client("host");
    let missing = pki.dir().join("absent.pem");
    let unread = super::Tls::from_files(&identity.cert, &identity.key, &missing);
    let unread = unread.expect_err("a file that is not there");
    assert_eq!(unread.code(), Some(Code::new("tls_unreadable")));
    assert_eq!(unread.kind(), ErrorKind::Config);
    let tls = super::Tls::from_files(&identity.cert, &identity.key, pki.ca());
    let host = Host::new().remote(tls.expect("the files open"));
    let far = Connector::new("io.example.far").endpoint("grpcs://127.0.0.1:1");
    let placing = tokio::time::timeout(Duration::from_secs(60), host.spec(&far, Role::Source));
    let refused = placing
        .await
        .expect("it answers")
        .expect_err("nothing listens there");
    assert_eq!(
        refused.code(),
        Some(Code::new("unreachable")),
        "{refused:?}"
    );
    assert_eq!(
        (refused.kind(), refused.origin()),
        (ErrorKind::Source, Origin::Source)
    );
}

#[tokio::test(start_paused = true)]
async fn a_mask_key_resolves_through_what_the_host_allows_and_nothing_else() {
    let host = Host::new().secret_store(Vault);
    let key = host.key("${secret:mask_key}").await.expect("resolved");
    assert_eq!(key.expose(), "CANARY-mask");
    let refused = host.key("${env:HOME}").await.expect_err("refused");
    assert_eq!(refused.code(), Some(Code::new("secret_refused")));
    assert_eq!(refused.kind(), ErrorKind::Config);
    assert!(refused.to_string().contains("key"), "{refused}");
    let missing = host
        .key("${secret:other}")
        .await
        .expect_err("not in the store");
    assert_eq!(missing.code(), Some(Code::new("secret_unresolved")));
    assert!(!format!("{host:?}").contains("CANARY"));
}

#[tokio::test(start_paused = true)]
async fn a_mask_key_written_out_is_refused_and_never_shown() {
    let host = Host::new().secret_store(Vault);
    for literal in [
        "CANARY-literal",
        "$${secret:mask_key}",
        "CANARY ${secret:mask_key}",
    ] {
        let refused = host.key(literal).await.expect_err("a key written out");
        assert_eq!(
            refused.code(),
            Some(Code::new("mask_key_literal")),
            "{literal}"
        );
        assert_eq!(refused.kind(), ErrorKind::Config);
        let said = format!("{refused} {refused:?} {:?}", refused.report());
        assert!(!said.contains("CANARY"), "{said}");
        assert!(
            said.contains("${secret:name}"),
            "the refusal says what to write: {said}"
        );
    }
}

#[test]
fn a_code_transform_is_the_host_s_by_its_name() {
    let host = Host::new()
        .transform("kept", rdlt_engine::Op::rename([("a", "b")]))
        .transform("also", rdlt_engine::Op::rename([("c", "d")]));
    assert!(host.code("kept").is_some());
    assert!(host.code("other").is_none());
    assert_eq!(host.codes().collect::<Vec<_>>(), ["also", "kept"]);
}
