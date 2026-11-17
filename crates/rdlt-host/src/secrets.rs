//! A connector's configuration as its host holds it: secret material, whose secret values are
//! references resolved at the last moment before the connector, once verified, is sent it.

mod named;
mod redact;
mod reference;
mod resolve;
#[cfg(test)]
mod tests;

use std::{fmt, mem};

use zeroize::{Zeroize as _, Zeroizing};

pub use named::NAMED_BY_SECRET;
pub(crate) use named::{catalog as named_catalog, plan as named_plan};
pub(crate) use redact::DROPPED;
pub use redact::Redactions;
pub use reference::ReferenceFault;
pub use resolve::{EnvSecrets, SecretFault, SecretKind, SecretReference, SecretResolver, Secrets};

use crate::limits::SECRET_REFERENCES;
use rdlt_wire::limits::{CONFIG_BYTES, count};

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
        if count(json.len()) > CONFIG_BYTES {
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
        let wiping = Wiping(&mut document);
        resolution(&mut *wiping.0, secrets, redactions).await
    }
}

/// `document` with each reference in it replaced by what `secrets` resolves it to, and each
/// resolved value added to `redactions`, as the JSON a connector is sent.
async fn resolution(
    document: &mut serde_json::Value,
    secrets: &dyn SecretResolver,
    redactions: &Redactions,
) -> Result<Zeroizing<String>, SecretError> {
    let mut texts = Vec::new();
    reference::texts(document, &mut String::new(), &mut texts);
    let mut references = 0_usize;
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
        let resolving = reference::resolved(&pieces, &field, secrets, redactions);
        let Some(resolved) = resolving.await else {
            continue;
        };
        let resolved = resolved?;
        text.zeroize();
        text.push_str(&resolved);
    }
    let mut json = written(document)?;
    // The text takes the buffer whole: nothing is copied.
    String::from_utf8(mem::take(&mut *json))
        .map(Zeroizing::new)
        .map_err(|unread| {
            drop(Zeroizing::new(unread.into_bytes()));
            SecretError::NotJson
        })
}

/// `document` as JSON, in a buffer made as large as it is: one that grows leaves copies behind.
fn written(document: &serde_json::Value) -> Result<Zeroizing<Vec<u8>>, SecretError> {
    let mut counted = Counted(0);
    serde_json::to_writer(&mut counted, document).map_err(|_| SecretError::NotJson)?;
    if count(counted.0) > CONFIG_BYTES {
        return Err(SecretError::TooLarge {
            limit: CONFIG_BYTES,
        });
    }
    let mut json = Zeroizing::new(Vec::with_capacity(counted.0));
    serde_json::to_writer(&mut *json, document).map_err(|_| SecretError::NotJson)?;
    Ok(json)
}

/// What counts the bytes written to it, and keeps none.
struct Counted(usize);

impl std::io::Write for Counted {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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

/// A document under resolution, wiped once its resolution ends, however it ends: one dropped
/// at its deadline holds the secrets resolved before it.
struct Wiping<'a>(&'a mut serde_json::Value);

impl Drop for Wiping<'_> {
    fn drop(&mut self) {
        wipe(self.0);
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
        limit: u64,
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
    /// A reference resolved to a secret of nothing but `*` and whitespace, which the `***` that
    /// replaces a secret in a text would hold.
    #[error("config field {field}: its {kind} reference is to a secret `***` cannot hide")]
    Unredactable {
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
    /// `secret_reference` for a malformed reference, `secret_refused` for one the operator lets
    /// no configuration reach, `secret_unredactable` for one whose secret `***` cannot hide,
    /// and `secret_unresolved` for one that did not resolve.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotJson | Self::TooLarge { .. } | Self::TooMany { .. } => "config_invalid",
            Self::Reference { .. } => "secret_reference",
            Self::Refused { .. } => "secret_refused",
            Self::Unredactable { .. } => "secret_unredactable",
            Self::Unresolved { .. } => "secret_unresolved",
        }
    }
}
