//! A store's credentials, resolved from the secrets the operator lets the configuration reach,
//! and resolved again as they age.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use object_store::CredentialProvider;
use object_store::aws::AwsCredential;
use rdlt_host::{SecretReference, SecretResolver};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::config::References;
use crate::error::LogStoreError;
use crate::limits::CREDENTIALS_FRESH;

/// Credentials resolved through `secrets` from their references, held for
/// [`CREDENTIALS_FRESH`] and then resolved again, so a rotated secret is taken up.
pub(crate) struct SecretCredentials {
    references: References,
    secrets: Arc<dyn SecretResolver>,
    held: Mutex<Option<(Arc<AwsCredential>, Instant)>>,
}

impl fmt::Debug for SecretCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretCredentials")
            .field("references", &self.references)
            .finish_non_exhaustive()
    }
}

impl SecretCredentials {
    pub(crate) fn new(references: References, secrets: Arc<dyn SecretResolver>) -> Self {
        Self {
            references,
            secrets,
            held: Mutex::new(None),
        }
    }

    /// The credentials, resolved where none are held or those held have aged.
    ///
    /// # Errors
    ///
    /// [`LogStoreError::Secret`] naming the field whose reference did not resolve.
    pub(crate) async fn fresh(&self) -> Result<Arc<AwsCredential>, LogStoreError> {
        let mut held = self.held.lock().await;
        if let Some((credential, at)) = &*held
            && at.elapsed() < CREDENTIALS_FRESH
        {
            return Ok(Arc::clone(credential));
        }
        let references = &self.references;
        let token = match &references.token {
            Some(token) => Some(self.resolved("session_token", token).await?),
            None => None,
        };
        let credential = Arc::new(AwsCredential {
            key_id: self.resolved("access_key_id", &references.key_id).await?,
            secret_key: self
                .resolved("secret_access_key", &references.secret_key)
                .await?,
            token,
        });
        *held = Some((Arc::clone(&credential), Instant::now()));
        Ok(credential)
    }

    async fn resolved(
        &self,
        field: &'static str,
        reference: &SecretReference,
    ) -> Result<String, LogStoreError> {
        let secret = self
            .secrets
            .resolve(reference)
            .await
            .map_err(|source| LogStoreError::Secret { field, source })?;
        Ok(secret.expose().clone())
    }
}

#[async_trait]
impl CredentialProvider for SecretCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        self.fresh()
            .await
            .map_err(|error| object_store::Error::Unauthenticated {
                path: String::new(),
                source: Box::new(error),
            })
    }
}

#[cfg(test)]
mod tests;
