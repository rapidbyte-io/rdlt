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
    let registry = Registry::new()
        .source::<MemorySource>()
        .destination::<MemoryDestination>()
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
    let with = Registry::new().source::<MemorySource>().fallback(Refusing);
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
    let without = Registry::new();
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
    let registry = Registry::new()
        .source::<MemorySource>()
        .destination::<MemoryDestination>();
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
    let registry = Registry::new()
        .source::<MemorySource>()
        .destination::<MemoryDestination>()
        .fallback(Refusing);
    assert_eq!(
        format!("{registry:?}"),
        "Registry { sources: [\"io.rapidbyte.memory\"], destinations: \
         [\"io.rapidbyte.memory\"], fallback: true }"
    );
}
