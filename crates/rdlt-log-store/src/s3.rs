//! Logs in an S3 bucket: the client its requests go through, and the log opened on it.

use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::client::{ClientOptions, HttpClient, HttpConnector};
use object_store::{BackoffConfig, RetryConfig};
use rdlt_engine::{Clock, ObjectStoreOptions, ObjectStoreWal};
use rdlt_host::SecretResolver;
use reqwest::redirect;
use rustls::ClientConfig;

use crate::config::S3Config;
use crate::credentials::SecretCredentials;
use crate::error::LogStoreError;
use crate::limits::CONNECT;

/// Makes the client an S3 log's requests go through: TLS 1.2 or 1.3 checked against the
/// system's trusted roots, HTTP/1.1, no redirect followed and no proxy, and plaintext only where
/// the configuration allowed it.
#[derive(Debug)]
pub(crate) struct Connector {
    tls: ClientConfig,
    plaintext: bool,
}

impl Connector {
    /// A connector reaching a store through TLS, or in plaintext where `plaintext`.
    pub(crate) fn new(plaintext: bool) -> Result<Self, LogStoreError> {
        let client = |error: rustls::Error| LogStoreError::Client(Box::new(error));
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier =
            rustls_platform_verifier::Verifier::new(Arc::clone(&provider)).map_err(client)?;
        let mut tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(client)?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self { tls, plaintext })
    }
}

impl HttpConnector for Connector {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        let client = reqwest::Client::builder()
            .tls_backend_preconfigured(self.tls.clone())
            .https_only(!self.plaintext)
            .redirect(redirect::Policy::none())
            .no_proxy()
            .http1_only()
            .connect_timeout(CONNECT)
            .build()
            .map_err(|error| object_store::Error::Generic {
                store: "S3",
                source: Box::new(error),
            })?;
        Ok(HttpClient::new(client))
    }
}

/// Opens logs in the bucket `config` names, its credentials resolved through `secrets`, each
/// request tried on `clock`.
pub(crate) async fn open(
    config: &S3Config,
    secrets: Arc<dyn SecretResolver>,
    clock: Arc<dyn Clock>,
) -> Result<ObjectStoreWal, LogStoreError> {
    let checked = config.checked()?;
    let credentials = SecretCredentials::new(checked.credentials, secrets);
    // Resolved before anything is asked of the store, so a refused secret says so.
    credentials.fresh().await?;
    let plaintext = checked
        .endpoint
        .as_ref()
        .is_some_and(|endpoint| endpoint.plaintext);
    // The log tries each request itself, on its clock; the client tries each once.
    let once = RetryConfig {
        backoff: BackoffConfig::default(),
        max_retries: 0,
        retry_timeout: std::time::Duration::ZERO,
    };
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .with_virtual_hosted_style_request(!config.path_style)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_retry(once)
        .with_allow_http(plaintext)
        .with_credentials(Arc::new(credentials))
        .with_http_connector(Connector::new(plaintext)?);
    if let Some(endpoint) = &checked.endpoint {
        builder = builder.with_endpoint(&endpoint.url);
    }
    let s3 = builder
        .build()
        .map_err(|error| LogStoreError::Client(Box::new(error)))?;
    let options = match checked.part_bytes {
        Some(part) => ObjectStoreOptions::default().with_part_bytes(part),
        None => ObjectStoreOptions::default(),
    };
    ObjectStoreWal::open(Arc::new(s3), &config.prefix, clock, options)
        .await
        .map_err(LogStoreError::Store)
}

#[cfg(test)]
mod tests;
