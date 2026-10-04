mod served;

use std::sync::Arc;

use rdlt_connector::{BoxFuture, Secret};
use rdlt_engine::SystemClock;
use rdlt_host::{SecretFault, SecretReference, SecretResolver, Secrets};
use serde_json::json;
use tokio::net::TcpListener;

use self::served::{Answer, serve};
use super::{Connector, open_through};
use crate::{LogStoreConfig, LogStoreError, LogStoreErrorKind, S3Config};

/// Resolves every reference to its name.
#[derive(Debug)]
struct Named;

impl SecretResolver for Named {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move { Ok(Secret::new(reference.name.clone())) })
    }
}

fn reaching(endpoint: &str, path_style: bool) -> LogStoreConfig {
    LogStoreConfig::parse(&json!({ "s3": {
        "bucket": "rdlt-logs",
        "prefix": "pipelines/logs",
        "region": "us-east-1",
        "endpoint": endpoint,
        "path_style": path_style,
        "access_key_id": "${secret:id}",
        "secret_access_key": "${secret:key}",
    }}))
    .expect("parses")
}

fn s3(config: &LogStoreConfig) -> &S3Config {
    match config {
        LogStoreConfig::S3(config) => config,
        LogStoreConfig::Local { .. } => panic!("an S3 configuration"),
    }
}

/// Opens logs at `endpoint` through `connector`, the store named `host` reached on `served`.
async fn opened_through(config: &LogStoreConfig, connector: Connector) -> LogStoreError {
    let config = s3(config);
    let checked = config.checked().expect("valid");
    let opened = open_through(
        config,
        checked,
        (Arc::new(Named), Arc::new(SystemClock)),
        connector,
    )
    .await;
    opened.expect_err("the store fails or refuses")
}

#[tokio::test]
async fn a_refused_secret_is_reported_before_the_store_is_asked_anything() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let endpoint = format!("http://{}", listener.local_addr().expect("an address"));
    let error = reaching(&endpoint, true)
        .open(Arc::new(Secrets::new()), Arc::new(SystemClock))
        .await
        .expect_err("refused");
    assert_eq!(error.code(), "secret_refused");
    assert_eq!(error.kind(), LogStoreErrorKind::Secret);
    assert!(!error.is_retryable());
    let accepted = tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept());
    assert!(accepted.await.is_err(), "nothing was asked of the store");
}

#[tokio::test]
async fn a_store_on_a_loopback_address_is_reached_without_tls_in_the_bucket_s_path() {
    let served = serve(None, Answer::Busy).await;
    let endpoint = format!("http://{}", served.address);
    let error = reaching(&endpoint, true)
        .open(Arc::new(Named), Arc::new(SystemClock))
        .await
        .expect_err("the store fails every attempt");
    assert_eq!(error.code(), "wal_storage_unavailable");
    assert!(error.is_retryable());
    let heard = served.heard();
    // Four creates racing, each tried five times by the log and never again by the client.
    let probe = "PUT /rdlt-logs/pipelines/logs/probe/";
    let puts = heard.iter().filter(|(line, _)| line.starts_with(probe));
    assert_eq!(puts.count(), 20, "{heard:?}");
    assert!(
        heard.iter().all(|(line, _)| line.contains(" /rdlt-logs")),
        "{heard:?}"
    );
}

#[tokio::test]
async fn a_request_the_store_takes_as_malformed_is_refused_for_good_and_not_tried_again() {
    let served = serve(None, Answer::Malformed).await;
    let endpoint = format!("http://{}", served.address);
    let error = reaching(&endpoint, true)
        .open(Arc::new(Named), Arc::new(SystemClock))
        .await
        .expect_err("refused");
    assert_eq!(error.code(), "wal_storage_refused");
    assert!(!error.is_retryable());
    let heard = served.heard();
    let creates = heard.iter().filter(|(line, _)| line.starts_with("PUT "));
    assert_eq!(creates.count(), 4, "each racing create once: {heard:?}");
}

#[tokio::test]
async fn a_store_whose_certificate_no_trusted_root_signs_is_refused_for_good() {
    let pki = rdlt_testkit::tls::Pki::new("untrusted");
    let served = serve(Some(pki.server("store", &["127.0.0.1"])), Answer::Busy).await;
    let endpoint = format!("https://{}", served.address);
    let error = reaching(&endpoint, true)
        .open(Arc::new(Named), Arc::new(SystemClock))
        .await
        .expect_err("refused");
    assert_eq!(error.code(), "wal_storage_refused");
    assert!(!error.is_retryable());
    let (completed, failed) = (&served.completed, &served.failed);
    assert_eq!(completed.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(failed.load(std::sync::atomic::Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn a_trusted_store_is_reached_over_tls_at_the_bucket_s_host_and_one_named_otherwise_is_not() {
    let pki = rdlt_testkit::tls::Pki::new("trusted");
    let roots = || {
        use rustls::pki_types::pem::PemObject as _;
        vec![rustls::pki_types::CertificateDer::from_pem_file(pki.ca()).expect("the CA")]
    };
    for (named, reached) in [("rdlt-logs.store.test", true), ("other.test", false)] {
        let served = serve(Some(pki.server(named, &[named])), Answer::Busy).await;
        let endpoint = format!("https://store.test:{}", served.address.port());
        let host = ("rdlt-logs.store.test".to_owned(), served.address);
        let connector = Connector::trusting(roots(), vec![host]);
        let error = opened_through(&reaching(&endpoint, false), connector).await;
        let heard = served.heard();
        if reached {
            assert_eq!(error.code(), "wal_storage_unavailable", "{error:?}");
            let host = format!("rdlt-logs.store.test:{}", served.address.port());
            assert!(!heard.is_empty());
            for (line, said) in &heard {
                assert_eq!(said.as_deref(), Some(host.as_str()), "{line}");
                assert!(
                    !line.contains("rdlt-logs/"),
                    "the bucket is in the host: {line}"
                );
            }
        } else {
            assert_eq!(error.code(), "wal_storage_refused", "{error:?}");
            assert_eq!(heard, Vec::new());
        }
    }
}
