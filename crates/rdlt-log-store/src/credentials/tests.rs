use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rdlt_connector::{BoxFuture, Secret};
use rdlt_host::{SecretFault, SecretKind, SecretReference, SecretResolver};

use super::SecretCredentials;
use crate::config::References;
use crate::limits::CREDENTIALS_FRESH;

/// Resolves every name to itself and the times it was asked, refusing `refused`.
#[derive(Debug, Default)]
struct Counting {
    asked: AtomicUsize,
    refused: Option<&'static str>,
}

impl SecretResolver for Counting {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        let asked = self.asked.fetch_add(1, Ordering::SeqCst);
        let refused = self.refused == Some(reference.name.as_str());
        Box::pin(async move {
            if refused {
                return Err(SecretFault::Refused);
            }
            Ok(Secret::new(format!("{}-{asked}", reference.name)))
        })
    }
}

fn named(name: &str) -> SecretReference {
    SecretReference {
        kind: SecretKind::Named,
        name: name.to_owned(),
    }
}

fn references(token: bool) -> References {
    References {
        key_id: named("id"),
        secret_key: named("key"),
        token: token.then(|| named("token")),
    }
}

#[tokio::test(start_paused = true)]
async fn credentials_are_resolved_again_once_they_have_aged() {
    let secrets = Arc::new(Counting::default());
    let credentials = SecretCredentials::new(references(true), Arc::clone(&secrets) as _);
    let first = credentials.fresh().await.expect("resolves");
    assert_eq!(first.token.as_deref(), Some("token-0"));
    assert_eq!(first.key_id, "id-1");
    assert_eq!(first.secret_key, "key-2");
    let almost = CREDENTIALS_FRESH
        .checked_sub(Duration::from_secs(1))
        .expect("fresh for longer");
    tokio::time::advance(almost).await;
    let held = credentials.fresh().await.expect("held");
    assert!(Arc::ptr_eq(&first, &held), "not resolved again while fresh");
    tokio::time::advance(Duration::from_secs(1)).await;
    let again = credentials.fresh().await.expect("resolves again");
    assert_eq!(again.key_id, "id-4");
    assert_eq!(secrets.asked.load(Ordering::SeqCst), 6);
    let shown = format!("{credentials:?}");
    for secret in ["id-1", "key-2", "token-0", "id-4"] {
        assert!(!shown.contains(secret), "{shown}");
    }
}

#[tokio::test]
async fn a_credential_the_operator_lets_no_configuration_reach_is_refused_naming_its_field() {
    for (refused, field) in [
        ("id", "access_key_id"),
        ("key", "secret_access_key"),
        ("token", "session_token"),
    ] {
        let secrets = Arc::new(Counting {
            refused: Some(refused),
            ..Counting::default()
        });
        let credentials = SecretCredentials::new(references(true), secrets);
        let error = credentials.fresh().await.expect_err("refused");
        assert_eq!(error.code(), "secret_refused");
        assert!(
            matches!(error, crate::LogStoreError::Secret { field: named, .. } if named == field)
        );
    }
    let credentials = SecretCredentials::new(references(false), Arc::new(Counting::default()));
    let fresh = credentials.fresh().await.expect("resolves");
    assert!(fresh.token.is_none());
    let unresolved = SecretCredentials::new(references(false), Arc::new(rdlt_host::Secrets::new()));
    let error = object_store::CredentialProvider::get_credential(&unresolved)
        .await
        .expect_err("nothing resolves");
    assert!(matches!(error, object_store::Error::Unauthenticated { .. }));
}
