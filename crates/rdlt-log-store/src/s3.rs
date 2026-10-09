//! Logs in an S3 bucket: the client its requests go through, and the log opened on it.

use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::client::{ClientOptions, HttpClient, HttpConnector};
use object_store::{BackoffConfig, RetryConfig};
use rdlt_engine::{Clock, ObjectStoreOptions, ObjectStoreWal};
use rdlt_host::SecretResolver;
use reqwest::redirect;
use rustls::ClientConfig;

use crate::config::{Checked, S3Config};
use crate::credentials::SecretCredentials;
use crate::error::LogStoreError;
use crate::limits::CONNECT;
use crate::refusing::Refusing;

/// Makes the client an S3 log's requests go through: TLS 1.2 or 1.3 checked against the
/// system's trusted roots, HTTP/1.1, no redirect followed and no proxy, and plaintext only where
/// the configuration allowed it.
#[derive(Debug)]
pub(crate) struct Connector {
    tls: ClientConfig,
    plaintext: bool,
    /// Names reached at addresses of their own, as a test's store is.
    #[cfg(test)]
    resolves: Vec<(String, std::net::SocketAddr)>,
}

impl Connector {
    /// A connector reaching a store through TLS, or in plaintext where `plaintext`.
    pub(crate) fn new(plaintext: bool) -> Result<Self, LogStoreError> {
        let provider = rdlt_wire::tls::provider();
        let verifier = rustls_platform_verifier::Verifier::new(Arc::clone(&provider))
            .map_err(|error| LogStoreError::Client(Box::new(error)))?;
        Self::verifying(provider, verifier, plaintext)
    }

    /// A connector trusting `roots` beside the system's, reaching each name of `resolves` at
    /// its address, for a test's store.
    #[cfg(test)]
    pub(crate) fn trusting(
        roots: Vec<rustls::pki_types::CertificateDer<'static>>,
        resolves: Vec<(String, std::net::SocketAddr)>,
    ) -> Self {
        let provider = rdlt_wire::tls::provider();
        let verifier =
            rustls_platform_verifier::Verifier::new_with_extra_roots(roots, Arc::clone(&provider))
                .expect("a verifier of the test's roots");
        let connector = Self::verifying(provider, verifier, false).expect("a connector");
        Self {
            resolves,
            ..connector
        }
    }

    fn verifying(
        provider: Arc<rustls::crypto::CryptoProvider>,
        verifier: rustls_platform_verifier::Verifier,
        plaintext: bool,
    ) -> Result<Self, LogStoreError> {
        let mut tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|error| LogStoreError::Client(Box::new(error)))?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            tls,
            plaintext,
            #[cfg(test)]
            resolves: Vec::new(),
        })
    }
}

impl HttpConnector for Connector {
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        let builder = reqwest::Client::builder();
        #[cfg(test)]
        let builder = self
            .resolves
            .iter()
            .fold(builder, |builder, (name, address)| {
                builder.resolve(name, *address)
            });
        let client = builder
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
        Ok(HttpClient::new(Refusing(client)))
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
    let plaintext = checked
        .endpoint
        .as_ref()
        .is_some_and(|endpoint| endpoint.plaintext);
    let connector = Connector::new(plaintext)?;
    open_through(config, checked, (secrets, clock), connector).await
}

/// Opens logs in the bucket `config` names, as `checked`, through `connector`.
pub(crate) async fn open_through(
    config: &S3Config,
    checked: Checked,
    (secrets, clock): (Arc<dyn SecretResolver>, Arc<dyn Clock>),
    connector: Connector,
) -> Result<ObjectStoreWal, LogStoreError> {
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
        .with_http_connector(connector);
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
