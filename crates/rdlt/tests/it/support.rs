//! What the facade's tests share: a host of the reference memory connectors, and pipelines of
//! them.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rdlt::{
    BoxFuture, Connector, Host, Pipeline, PipelineBuilder, Secret, SecretFault, SecretKind,
    SecretReference, SecretResolver, Side, Stream,
};
use rdlt_connector_reference::{MemoryDestination, MemorySource};

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
