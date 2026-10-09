//! S3 servers in containers, each image named by its digest, with a bucket made in each.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use object_store::aws::{AwsAuthorizer, AwsCredential};
use object_store::client::{HttpClient, HttpRequestBody};
use rdlt_connector::{BoxFuture, Secret};
use rdlt_engine::{SystemClock, WalStore};
use rdlt_host::{SecretFault, SecretReference, SecretResolver};
use rdlt_log_store::{LogStoreConfig, LogStoreError};
use serde_json::{Value, json};
use testcontainers::bollard::models::{HostConfig, PortBinding};
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{ContainerAsync, GenericImage, ImageExt as _};

/// The access key every server is started with.
pub(crate) const KEY_ID: &str = "rdltaccess";

/// The secret key every server is started with.
pub(crate) const SECRET_KEY: &str = "rdltsecretkey123";

/// The bucket made in every server.
pub(crate) const BUCKET: &str = "rdlt-logs";

/// An S3 server a test starts.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Server {
    /// `RustFS`, an S3 server written in Rust.
    RustFs,
    /// `MinIO`, as Pigsty builds it from its source: its makers publish no image of it.
    Minio,
}

/// A server running in its container, removed when it is dropped.
pub(crate) struct Running {
    container: ContainerAsync<GenericImage>,
    /// Where it answers: `http://127.0.0.1:<port>`.
    pub(crate) endpoint: String,
}

impl Server {
    fn image(self) -> testcontainers::ContainerRequest<GenericImage> {
        match self {
            Self::RustFs => GenericImage::new(
                "rustfs/rustfs",
                "latest@sha256:1803faef57627e2d9c2e7d89d655d712ddded5389040054987163043fecb6a3c",
            )
            .with_exposed_port(9000.tcp())
            .with_wait_for(WaitFor::message_on_either_std("Starting:"))
            .with_env_var("RUSTFS_ACCESS_KEY", KEY_ID)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY),
            Self::Minio => GenericImage::new(
                "pgsty/minio",
                "latest@sha256:b6bfe7239bfc83fb90d31612d9704d86039dd714f7904b3f1ad68f211e602372",
            )
            .with_exposed_port(9000.tcp())
            .with_wait_for(WaitFor::message_on_either_std("API:"))
            .with_env_var("MINIO_ROOT_USER", KEY_ID)
            .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .with_cmd(["server", "/data"]),
        }
    }

    /// The server started, with [`BUCKET`] made in it.
    pub(crate) async fn start(self) -> Running {
        Arc::unwrap_or_clone(rdlt_wire::tls::provider())
            .install_default()
            .ok();
        let container = self
            .image()
            .with_host_config_modifier(on_loopback)
            .start()
            .await
            .expect("the server starts");
        let port = container
            .get_host_port_ipv4(9000.tcp())
            .await
            .expect("the server's port is published");
        let endpoint = format!("http://127.0.0.1:{port}");
        made(&endpoint).await;
        Running {
            container,
            endpoint,
        }
    }
}

/// Publishes the server's port on the loopback address alone, out of reach of other machines.
fn on_loopback(config: &mut HostConfig) {
    let binding = PortBinding {
        host_ip: Some("127.0.0.1".to_owned()),
        host_port: None,
    };
    config.publish_all_ports = Some(false);
    config.port_bindings = Some(HashMap::from([(
        "9000/tcp".to_owned(),
        Some(vec![binding]),
    )]));
}

impl Running {
    /// The host addresses its ports are published on, as Docker reports them.
    pub(crate) async fn published_on(&self) -> Vec<String> {
        let docker = testcontainers::bollard::Docker::connect_with_defaults().expect("Docker");
        let inspected = docker
            .inspect_container(self.container.id(), None)
            .await
            .expect("the container is inspected");
        let ports = inspected
            .network_settings
            .and_then(|settings| settings.ports)
            .unwrap_or_default();
        ports
            .into_values()
            .flatten()
            .flatten()
            .map(|binding| binding.host_ip.unwrap_or_default())
            .collect()
    }
}

/// Makes [`BUCKET`] at `endpoint`, waiting for the server to take requests.
async fn made(endpoint: &str) {
    let credential = AwsCredential {
        key_id: KEY_ID.to_owned(),
        secret_key: SECRET_KEY.to_owned(),
        token: None,
    };
    let client = HttpClient::new(reqwest::Client::new());
    for _ in 0..100 {
        let mut request = http::Request::builder()
            .method(http::Method::PUT)
            .uri(format!("{endpoint}/{BUCKET}"))
            .body(HttpRequestBody::empty())
            .expect("a request");
        AwsAuthorizer::new(&credential, "s3", "us-east-1")
            .try_authorize(&mut request, None)
            .expect("the request is signed");
        if let Ok(response) = client.execute(request).await
            && response.status().is_success()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("the bucket was not made at {endpoint}");
}

/// Resolves the references the configurations name to the servers' keys.
#[derive(Debug)]
pub(crate) struct Keys(BTreeMap<&'static str, &'static str>);

impl Keys {
    pub(crate) fn new() -> Self {
        Self(BTreeMap::from([("id", KEY_ID), ("key", SECRET_KEY)]))
    }
}

impl SecretResolver for Keys {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        let secret = self
            .0
            .get(reference.name.as_str())
            .map(|value| (*value).to_owned());
        Box::pin(async move { secret.map(Secret::new).ok_or(SecretFault::Missing) })
    }
}

/// The configuration of logs at `endpoint` beneath `prefix`, credentials named for [`Keys`].
pub(crate) fn config(endpoint: &str, prefix: &str) -> Value {
    json!({ "s3": {
        "bucket": BUCKET, "prefix": prefix, "region": "us-east-1",
        "endpoint": endpoint, "path_style": true,
        "access_key_id": "${secret:id}", "secret_access_key": "${secret:key}",
    }})
}

/// Logs at `endpoint` beneath `prefix`, opened.
pub(crate) async fn opened(
    endpoint: &str,
    prefix: &str,
) -> Result<Arc<dyn WalStore>, LogStoreError> {
    LogStoreConfig::parse(&config(endpoint, prefix))
        .expect("parses")
        .open(Arc::new(Keys::new()), Arc::new(SystemClock))
        .await
}
