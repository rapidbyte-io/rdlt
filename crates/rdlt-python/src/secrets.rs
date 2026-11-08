//! The operator's secret store: a Python callable from a secret's name to its text, or `None`.

#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;

use pyo3::types::PyAnyMethods as _;
use pyo3::{Py, PyAny};
use rdlt::{BoxFuture, Secret, SecretFault, SecretKind, SecretReference, SecretResolver};

use crate::{calls, errors};

/// Resolves `${secret:name}` through the operator's callable; it resolves no other kind.
pub(crate) struct PythonSecrets {
    store: Arc<Py<PyAny>>,
}

impl PythonSecrets {
    /// The store `store` is: called with a name, it answers the secret's text or `None`.
    pub(crate) fn new(store: Py<PyAny>) -> Self {
        Self {
            store: Arc::new(store),
        }
    }
}

/// Shows nothing of the store: the callable's own text may hold the secrets it serves.
impl fmt::Debug for PythonSecrets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PythonSecrets")
    }
}

/// The store raised; its type alone is said, since its message may hold what it was asked for.
#[derive(Debug, thiserror::Error)]
#[error("the secret store raised {0}")]
struct Raised(String);

impl SecretResolver for PythonSecrets {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            if reference.kind != SecretKind::Named {
                return Err(SecretFault::Unsupported);
            }
            let name = reference.name.clone();
            let answered = calls::blocking(move |py| match store.bind(py).call1((name,)) {
                Ok(value) if value.is_none() => Err(SecretFault::Missing),
                Ok(value) => value
                    .extract::<String>()
                    .map(Secret::new)
                    .map_err(|_| SecretFault::NotText),
                Err(error) => {
                    let raised = Raised(errors::type_name(py, &error));
                    Err(SecretFault::Failed(Box::new(raised)))
                }
            })
            .await;
            answered.map_err(|joined| SecretFault::Failed(Box::new(joined)))?
        })
    }
}
