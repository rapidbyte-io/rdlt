use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rdlt_connector::{BoxFuture, Secret};
use rdlt_engine::SystemClock;
use rdlt_host::{SecretFault, SecretReference, SecretResolver, Secrets};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde_json::json;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

use crate::{LogStoreConfig, LogStoreErrorKind};

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

fn reaching(endpoint: &str) -> LogStoreConfig {
    LogStoreConfig::parse(&json!({ "s3": {
        "bucket": "rdlt-logs",
        "prefix": "pipelines/logs",
        "region": "us-east-1",
        "endpoint": endpoint,
        "path_style": true,
        "access_key_id": "${secret:id}",
        "secret_access_key": "${secret:key}",
    }}))
    .expect("parses")
}

#[tokio::test]
async fn a_refused_secret_is_reported_before_the_store_is_asked_anything() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let endpoint = format!("http://{}", listener.local_addr().expect("an address"));
    let error = reaching(&endpoint)
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
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let endpoint = format!("http://{}", listener.local_addr().expect("an address"));
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let heard = Arc::clone(&requests);
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut request = vec![0; 4096];
            let read = stream.read(&mut request).await.unwrap_or(0);
            let line = String::from_utf8_lossy(&request[..read]);
            heard
                .lock()
                .expect("not poisoned")
                .push(line.lines().next().unwrap_or("").to_owned());
            let answer =
                b"HTTP/1.1 503 Slow Down\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
            stream.write_all(answer).await.ok();
        }
    });
    let error = reaching(&endpoint)
        .open(Arc::new(Named), Arc::new(SystemClock))
        .await
        .expect_err("the store fails every attempt");
    server.abort();
    assert_eq!(error.code(), "wal_storage_unavailable");
    assert!(error.is_retryable());
    let heard = requests.lock().expect("not poisoned").clone();
    // The probe's create, then the deletion of its markers, each tried five times by the log
    // and never again by the client; the bucket in each path.
    let puts = heard
        .iter()
        .filter(|line| line.starts_with("PUT /rdlt-logs/pipelines/logs/probe/"));
    assert_eq!(puts.count(), 5, "{heard:?}");
    assert!(
        heard.iter().all(|line| line.contains(" /rdlt-logs")),
        "{heard:?}"
    );
}

#[tokio::test]
async fn a_store_whose_certificate_no_trusted_root_signs_is_never_spoken_to() {
    let pki = rdlt_testkit::tls::Pki::new("untrusted");
    let files = pki.server("store", &["127.0.0.1"]);
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&files.cert)
        .expect("reads")
        .collect::<Result<_, _>>()
        .expect("certificates");
    let key = PrivateKeyDer::from_pem_file(&files.key).expect("a key");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let tls = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("TLS versions")
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .expect("a server configuration");
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let endpoint = format!("https://{}", listener.local_addr().expect("an address"));
    let (failed, completed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let counts = (Arc::clone(&failed), Arc::clone(&completed));
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            match acceptor.accept(stream).await {
                Ok(_) => counts.1.fetch_add(1, Ordering::SeqCst),
                Err(_) => counts.0.fetch_add(1, Ordering::SeqCst),
            };
        }
    });
    let error = reaching(&endpoint)
        .open(Arc::new(Named), Arc::new(SystemClock))
        .await
        .expect_err("refused");
    server.abort();
    assert_eq!(error.kind(), LogStoreErrorKind::Store);
    assert_eq!(
        completed.load(Ordering::SeqCst),
        0,
        "no handshake completed"
    );
    assert!(
        failed.load(Ordering::SeqCst) > 0,
        "the client refused the certificate"
    );
}
