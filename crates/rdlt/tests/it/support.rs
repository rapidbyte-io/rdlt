//! What the facade's tests share: a host of the reference memory connectors, and pipelines of
//! them.

use std::future::IntoFuture;
#[cfg(any(feature = "local", feature = "remote"))]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rdlt::{
    BoxFuture, Connector, Host, Pipeline, PipelineBuilder, Secret, SecretFault, SecretKind,
    SecretReference, SecretResolver, Side, Stream,
};
use rdlt_connector_reference::{MemoryDestination, MemorySource};

/// The example binary `name`, which the test build builds.
#[cfg(any(feature = "local", feature = "remote"))]
pub(crate) fn example(name: &str) -> PathBuf {
    let tests = std::env::current_exe().expect("the test binary has a path");
    tests
        .ancestors()
        .map(|dir| dir.join("examples").join(name))
        .find(|example| example.is_file())
        .expect("the test build builds the examples")
}

/// A host running the reference memory connectors in process.
pub(crate) fn memory_host() -> Host {
    Host::new()
        .source::<MemorySource>()
        .destination::<MemoryDestination>()
}

/// Pipeline `name`, whose one stream `users` holds `rows`, into memory store `store`.
pub(crate) fn memory_pipeline(
    name: &str,
    store: &str,
    rows: &serde_json::Value,
) -> PipelineBuilder {
    let source = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "streams": { "users": rows } }));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": store }));
    Pipeline::builder(name, source, destination).stream(Stream::new("users"))
}

/// A store holding one secret, `mask_key`, counting how often it is asked for it.
#[derive(Debug)]
pub(crate) struct Vault(Arc<AtomicUsize>);

impl Vault {
    /// A vault counting each resolution into `resolved`.
    pub(crate) fn counting(resolved: Arc<AtomicUsize>) -> Self {
        Self(resolved)
    }
}

impl SecretResolver for Vault {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            match (reference.kind, reference.name.as_str()) {
                (SecretKind::Named, "mask_key") => Ok(Secret::new("CANARY-mask".to_owned())),
                _ => Err(SecretFault::Missing),
            }
        })
    }
}

/// `work`, which must end within a minute.
///
/// A run computes on its host's pool, beyond the runtime, so tests of runs keep the real clock,
/// which a paused one would leap past while a job runs; each of their waits is bounded instead.
pub(crate) async fn within<F: IntoFuture>(work: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), work)
        .await
        .expect("it ends within a minute")
}
