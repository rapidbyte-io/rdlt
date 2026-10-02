use rdlt_connector::{BoxFuture, ConnectorErrorKind, ConnectorId, Destination, Source};
use rdlt_connector_reference::{MemoryDestination, MemorySource};

use super::Registry;
use crate::provider::{ConnectorRef, Placed, Placement, Provider, ProviderError};

fn memory() -> ConnectorRef {
    ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("a valid id"))
}

/// A provider that places nothing, and says which reference it was asked for.
struct Refusing;

impl Provider for Refusing {
    fn source<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        _config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Source>>, ProviderError>> {
        Box::pin(async move { Err(refused(reference)) })
    }

    fn destination<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        _config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Destination>>, ProviderError>> {
        Box::pin(async move { Err(refused(reference)) })
    }
}

fn refused(reference: &ConnectorRef) -> ProviderError {
    ProviderError::NotFound {
        id: reference.id.clone(),
        source: Some(std::io::Error::other("asked the fallback")),
    }
}

fn asked_the_fallback(error: &ProviderError) -> bool {
    matches!(
        error,
        ProviderError::NotFound {
            source: Some(_),
            ..
        }
    )
}

#[tokio::test]
async fn a_linked_connector_is_placed_in_process_before_the_fallback_is_asked() {
    let registry = Registry::trusted()
        .trusted_source::<MemorySource>()
        .trusted_destination::<MemoryDestination>()
        .fallback(Refusing);
    let config = serde_json::json!({ "streams": {} });
    let source = Provider::source(&registry, &memory(), &config)
        .await
        .expect("placed");
    assert_eq!(
        (source.placement, source.digest, source.spec.id),
        (Placement::InProcess, None, memory().id)
    );
    let config = serde_json::json!({ "store": "registry" });
    let destination = Provider::destination(&registry, &memory(), &config)
        .await
        .expect("placed");
    assert_eq!(destination.placement, Placement::InProcess);
}

#[tokio::test]
async fn any_other_connector_goes_to_the_fallback_or_is_not_found() {
    let other = ConnectorRef::new(ConnectorId::parse("io.example.other").expect("a valid id"));
    let config = serde_json::json!({});
    let with = Registry::trusted()
        .trusted_source::<MemorySource>()
        .fallback(Refusing);
    assert!(asked_the_fallback(
        &Provider::source(&with, &other, &config)
            .await
            .err()
            .expect("refused")
    ));
    assert!(asked_the_fallback(
        &Provider::destination(&with, &other, &config)
            .await
            .err()
            .expect("refused")
    ));
    let without = Registry::trusted();
    for refused in [
        Provider::source(&without, &other, &config)
            .await
            .err()
            .expect("refused"),
        Provider::destination(&without, &other, &config)
            .await
            .err()
            .expect("refused"),
    ] {
        assert!(
            matches!(refused, ProviderError::NotFound { source: None, .. }),
            "{refused}"
        );
    }
}

#[tokio::test]
async fn a_linked_connector_of_another_version_or_whose_connect_fails_is_refused() {
    let registry = Registry::trusted()
        .trusted_source::<MemorySource>()
        .trusted_destination::<MemoryDestination>();
    let newer = memory().version(semver::VersionReq::parse(">=9").expect("a valid requirement"));
    let config = serde_json::json!({ "streams": {} });
    let refused = Provider::source(&registry, &newer, &config)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(refused, ProviderError::VersionMismatch { .. }),
        "{refused}"
    );
    let refused = Provider::destination(&registry, &newer, &config)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(refused, ProviderError::VersionMismatch { .. }),
        "{refused}"
    );
    let bad = serde_json::json!({ "unknown": true });
    for refused in [
        Provider::source(&registry, &memory(), &bad)
            .await
            .err()
            .expect("refused"),
        Provider::destination(&registry, &memory(), &bad)
            .await
            .err()
            .expect("refused"),
    ] {
        let ProviderError::HandshakeFailed { source, .. } = &refused else {
            panic!("{refused}");
        };
        assert_eq!(source.kind(), ConnectorErrorKind::Config);
    }
}

#[test]
fn a_registry_debugs_its_connectors_by_id() {
    let registry = Registry::trusted()
        .trusted_source::<MemorySource>()
        .trusted_destination::<MemoryDestination>()
        .fallback(Refusing);
    assert_eq!(
        format!("{registry:?}"),
        "Registry { sources: [\"io.rapidbyte.memory\"], destinations: \
         [\"io.rapidbyte.memory\"], fallback: true, .. }"
    );
}

/// Counts how often it is asked, and resolves nothing.
#[derive(Debug, Default)]
struct Asked(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl crate::secrets::SecretResolver for Asked {
    fn resolve<'a>(
        &'a self,
        _reference: &'a crate::secrets::SecretReference,
    ) -> BoxFuture<'a, Result<rdlt_connector::Secret<String>, crate::secrets::SecretFault>> {
        Box::pin(async move {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(crate::secrets::SecretFault::Missing)
        })
    }
}

#[tokio::test]
async fn a_linked_connectors_secrets_are_resolved_only_once_its_reference_is_accepted() {
    let asked = Asked::default();
    let count = std::sync::Arc::clone(&asked.0);
    let registry = Registry::trusted()
        .trusted_source::<MemorySource>()
        .trusted_destination::<MemoryDestination>()
        .secrets(asked);
    let config = serde_json::json!({ "store": "${secret:store}", "streams": {} });
    let newer = memory().version(semver::VersionReq::parse(">=9").expect("a valid requirement"));
    let elsewhere = memory().endpoint("grpcs://localhost:1");
    for unaccepted in [newer, elsewhere] {
        assert!(
            Provider::source(&registry, &unaccepted, &config)
                .await
                .is_err()
        );
        assert!(
            Provider::destination(&registry, &unaccepted, &config)
                .await
                .is_err()
        );
    }
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
    // Accepted, its reference is resolved, and one that does not resolve fails the placement.
    for refused in [
        Provider::source(&registry, &memory(), &config).await.err(),
        Provider::destination(&registry, &memory(), &config)
            .await
            .err(),
    ] {
        let refused = refused.expect("the secret does not resolve");
        assert!(matches!(refused, ProviderError::Secret { .. }), "{refused}");
        assert_eq!(refused.code(), "secret_unresolved");
    }
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 2);
}

/// An error of each kind a connector that was found fails to be placed with, and its code.
fn unplaced() -> Vec<(ProviderError, &'static str)> {
    let id = || memory().id;
    let io = || std::io::Error::other("it failed");
    let (endpoint, path) = (|| "e".to_owned(), || std::path::PathBuf::from("/x"));
    let source = Box::new(rdlt_connector::ConnectorError::config("no"));
    let (expected, found) = (
        crate::provider::Digest([0; 32]),
        crate::provider::Digest([1; 32]),
    );
    let digests = ProviderError::DigestMismatch {
        id: id(),
        path: path(),
        expected,
        found,
    };
    vec![
        (
            ProviderError::SpawnFailed {
                id: id(),
                path: path(),
                source: io(),
            },
            "spawn_failed",
        ),
        (
            ProviderError::Unreachable {
                id: id(),
                endpoint: endpoint(),
                source: io(),
            },
            "unreachable",
        ),
        (
            ProviderError::Tls {
                id: id(),
                endpoint: endpoint(),
                source: Box::new(io()),
            },
            "tls",
        ),
        (digests, "digest_mismatch"),
        (
            ProviderError::HandshakeFailed { id: id(), source },
            "handshake_failed",
        ),
    ]
}

/// An error of each kind a reference is refused with before anything runs, and its code.
fn refused_references() -> Vec<(ProviderError, &'static str)> {
    let id = || memory().id;
    let (required, found) = (semver::VersionReq::STAR, "1".to_owned());
    let unsupported = ProviderError::Unsupported {
        id: id(),
        requirement: "a path",
        placement: "remote",
    };
    let shared = ProviderError::Shared {
        id: id(),
        path: "/x".into(),
        owner: 7,
        mode: 0o777,
    };
    let sandbox = crate::local::SandboxError::Unsupported;
    let secret = crate::secrets::SecretError::NotJson;
    let endpoint = crate::network::Endpoint::parse("no endpoint").expect_err("no endpoint");
    vec![
        (
            ProviderError::Endpoint {
                id: id(),
                source: endpoint,
            },
            "endpoint_invalid",
        ),
        (
            ProviderError::NotFound {
                id: id(),
                source: None,
            },
            "connector_not_found",
        ),
        (unsupported, "placement_unsupported"),
        (
            ProviderError::VersionMismatch {
                id: id(),
                required,
                found,
            },
            "version_mismatch",
        ),
        (shared, "binary_shared"),
        (
            ProviderError::Sandbox {
                id: id(),
                source: sandbox,
            },
            "sandbox_unsupported",
        ),
        (
            ProviderError::Secret {
                id: id(),
                source: secret,
            },
            "config_invalid",
        ),
    ]
}

#[test]
fn every_provider_error_has_a_code_of_its_own_kind() {
    let mut errors = unplaced();
    errors.append(&mut refused_references());
    let mut codes: Vec<&str> = errors.iter().map(|(error, _)| error.code()).collect();
    for (error, code) in &errors {
        assert_eq!(error.code(), *code, "{error}");
    }
    codes.sort_unstable();
    codes.dedup();
    assert_eq!(codes.len(), errors.len());
}

#[test]
fn a_connectors_own_word_for_its_version_is_shown_in_the_refusal_of_it() {
    let reference = memory().version(semver::VersionReq::parse(">=9").expect("valid"));
    let refused =
        crate::provider::accepts(&reference, "1.0\u{1b}[2J\u{202e}").expect_err("refused");
    let ProviderError::VersionMismatch { found, .. } = &refused else {
        panic!("{refused}");
    };
    assert_eq!(found, r"1.0\u{1b}[2J\u{202e}");
    assert!(crate::provider::accepts(&memory(), "anything at all").is_ok());
}

#[tokio::test]
async fn a_linked_connector_s_configuration_beyond_its_limit_is_refused() {
    use crate::secrets::SecretError;
    let registry = Registry::trusted()
        .trusted_source::<MemorySource>()
        .trusted_destination::<MemoryDestination>();
    let limit = usize::try_from(rdlt_connector::limits::MAX_CONFIG_BYTES).expect("a size");
    assert_eq!(limit, crate::limits::CONFIG_BYTES);
    // One string's JSON, its quotes and the object around it, of `bytes` in all.
    let config = |bytes: usize| serde_json::json!({ "store": "x".repeat(bytes - 12) });
    assert_eq!(config(limit + 1).to_string().len(), limit + 1);
    let refused = Provider::destination(&registry, &memory(), &config(limit + 1))
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(
            refused,
            ProviderError::Secret {
                source: SecretError::TooLarge { limit: found },
                ..
            } if found == limit
        ),
        "{refused}"
    );
    let placed = Provider::destination(&registry, &memory(), &config(limit)).await;
    assert!(
        placed.is_ok(),
        "{:?}",
        placed.err().map(|error| error.to_string())
    );
}
