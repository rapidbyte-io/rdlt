//! A connector's configuration as its host holds it: secret material, whose secret values are
//! references resolved at the last moment before the connector, once verified, is sent it.

mod redact;
mod reference;
mod resolve;
#[cfg(test)]
mod tests;

use std::fmt;

use zeroize::{Zeroize as _, Zeroizing};

pub(crate) use redact::DROPPED;
pub use redact::Redactions;
pub use reference::ReferenceFault;
pub use resolve::{
    EnvSecrets, FileSecrets, SecretFault, SecretKind, SecretReference, SecretResolver, Secrets,
};

use crate::limits::{CONFIG_BYTES, SECRET_REFERENCES};

/// A connector's configuration, held as secret material: it has no `Debug` that shows it, is
/// not `Clone`, and is wiped from memory when dropped.
///
/// A text value may hold references, `${env:NAME}`, `${file:/absolute/path}` and
/// `${secret:name}`, each replaced by what a [`SecretResolver`] resolves it to when the
/// configuration is sent to its connector; `$${` is a literal `${`.
pub struct Config {
    json: Zeroizing<String>,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Config(***)")
    }
}

impl From<&serde_json::Value> for Config {
    fn from(document: &serde_json::Value) -> Self {
        Self {
            json: Zeroizing::new(document.to_string()),
        }
    }
}

impl Config {
    /// The configuration `json` holds, a JSON document of [`CONFIG_BYTES`] at most.
    ///
    /// # Errors
    ///
    /// [`SecretError::NotJson`] for text that is no JSON document, and
    /// [`SecretError::TooLarge`] for one beyond the limit: neither says what the text holds.
    pub fn parse(json: impl Into<String>) -> Result<Self, SecretError> {
        let json = Zeroizing::new(json.into());
        if json.len() > CONFIG_BYTES {
            return Err(SecretError::TooLarge {
                limit: CONFIG_BYTES,
            });
        }
        let mut document: serde_json::Value =
            serde_json::from_str(&json).map_err(|_| SecretError::NotJson)?;
        wipe(&mut document);
        Ok(Self { json })
    }

    /// A second copy of the configuration, wiped when dropped as this one is.
    #[must_use]
    pub fn duplicate(&self) -> Self {
        Self {
            json: self.json.clone(),
        }
    }

    /// The configuration as the JSON a connector is sent, each reference replaced by what
    /// `secrets` resolves it to, and each resolved value added to `redactions`.
    ///
    /// # Errors
    ///
    /// A [`SecretError`] naming the field whose reference is malformed or did not resolve,
    /// never a value.
    pub async fn resolved(
        &self,
        secrets: &dyn SecretResolver,
        redactions: &Redactions,
    ) -> Result<Zeroizing<String>, SecretError> {
        let mut document: serde_json::Value =
            serde_json::from_str(&self.json).map_err(|_| SecretError::NotJson)?;
        let outcome = self.resolve(&mut document, secrets, redactions).await;
        wipe(&mut document);
        outcome
    }

    async fn resolve(
        &self,
        document: &mut serde_json::Value,
        secrets: &dyn SecretResolver,
        redactions: &Redactions,
    ) -> Result<Zeroizing<String>, SecretError> {
        let mut texts = Vec::new();
        reference::texts(document, &mut String::new(), &mut texts);
        let (mut references, mut grown) = (0_usize, 0_usize);
        for (field, text) in texts {
            let pieces = reference::pieces(text).map_err(|fault| SecretError::Reference {
                field: field.clone(),
                fault,
            })?;
            references = references.saturating_add(reference::count(&pieces));
            if references > SECRET_REFERENCES {
                return Err(SecretError::TooMany {
                    limit: SECRET_REFERENCES,
                });
            }
            let resolving = reference::resolved(&pieces, text, secrets, redactions);
            let Some(resolved) = resolving.await else {
                continue;
            };
            let resolved = resolved.map_err(|(kind, source)| match source {
                SecretFault::Refused => SecretError::Refused {
                    field: field.clone(),
                    kind,
                },
                source => SecretError::Unresolved {
                    field: field.clone(),
                    kind,
                    source,
                },
            })?;
            grown = grown.saturating_add(resolved.len());
            text.zeroize();
            text.push_str(&resolved);
        }
        // Room for the whole document at once: a buffer that grows leaves copies behind.
        let room = self.json.len().saturating_add(grown.saturating_mul(2));
        let mut json = Zeroizing::new(Vec::with_capacity(room));
        serde_json::to_writer(&mut *json, document).map_err(|_| SecretError::NotJson)?;
        if json.len() > CONFIG_BYTES {
            return Err(SecretError::TooLarge {
                limit: CONFIG_BYTES,
            });
        }
        let text = std::str::from_utf8(&json).map_err(|_| SecretError::NotJson)?;
        let mut sent = Zeroizing::new(String::with_capacity(text.len()));
        sent.push_str(text);
        Ok(sent)
    }
}

/// Wipes every text `document` holds.
fn wipe(document: &mut serde_json::Value) {
    match document {
        serde_json::Value::String(text) => text.zeroize(),
        serde_json::Value::Array(items) => items.iter_mut().for_each(wipe),
        serde_json::Value::Object(fields) => fields.values_mut().for_each(wipe),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

/// Why a configuration could not be held, or its secrets not resolved: the field, never its
/// value.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    /// The configuration is no JSON document.
    #[error("the configuration is not a JSON document")]
    NotJson,
    /// The configuration is larger than a configuration may be.
    #[error("the configuration is larger than {limit} bytes")]
    TooLarge {
        /// Bytes a configuration may take.
        limit: usize,
    },
    /// The configuration holds more references than one may.
    #[error("the configuration holds more than {limit} secret references")]
    TooMany {
        /// References a configuration may hold.
        limit: usize,
    },
    /// A reference is malformed.
    #[error("config field {field}: its secret reference {fault}")]
    Reference {
        /// The field, as a path of keys and indexes.
        field: String,
        /// What is wrong with the reference.
        fault: ReferenceFault,
    },
    /// A reference names what the host's operator lets no configuration reach.
    #[error("config field {field}: its {kind} reference is to nothing this host resolves")]
    Refused {
        /// The field, as a path of keys and indexes.
        field: String,
        /// The reference's kind.
        kind: SecretKind,
    },
    /// A reference did not resolve.
    #[error("config field {field}: its {kind} reference did not resolve")]
    Unresolved {
        /// The field, as a path of keys and indexes.
        field: String,
        /// The reference's kind.
        kind: SecretKind,
        /// Why it did not resolve.
        #[source]
        source: SecretFault,
    },
}

impl SecretError {
    /// The error's stable code: `config_invalid` for a document that cannot be held,
    /// `secret_reference` for a malformed reference, `secret_unresolved` for one that did not
    /// resolve.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotJson | Self::TooLarge { .. } | Self::TooMany { .. } => "config_invalid",
            Self::Reference { .. } => "secret_reference",
            Self::Refused { .. } => "secret_refused",
            Self::Unresolved { .. } => "secret_unresolved",
        }
    }
}
