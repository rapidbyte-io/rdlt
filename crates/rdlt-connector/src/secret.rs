//! [`Secret`] — a credential string that never renders.
//!
//! One shared type for every connector. A `Secret` is serde-transparent, so
//! config documents keep plain strings in and out; its `Debug` and `Display`
//! both print `***`, so a credential can be nested anywhere in a config struct
//! that derives `Debug` (or lands in an error via `{:?}`/`{}`) without leaking.
//!
//! [`Secret::reveal`] is the ONLY way to the underlying value, and that makes
//! the set of `reveal()` call sites the entire audit surface: every place a
//! credential is exposed is a `.reveal()` a reader can grep for. The rule the
//! connectors follow is to call it at the exact point the credential attaches
//! to an outbound request (a header, a query param, a catalog property) and
//! never to place its result into a log line, an error message, or any other
//! rendered output.
//!
//! The [`schemars::JsonSchema`] impl (a plain `{"type": "string"}` placeholder)
//! lives behind the crate's `schema` feature so the SPI carries no mandatory
//! schemars dependency; connectors that generate config schemas enable it.

use serde::{Deserialize, Serialize};

/// A credential that cannot be printed by accident.
///
/// `Debug` and `Display` render `***`, so a secret survives being interpolated
/// into an error, a log line, or a rendered config without leaking. The only way
/// to obtain the value is [`Secret::reveal`], which makes that call site the
/// entire audit surface — the test suite greps rendered output to prove no
/// secret substring ever reaches it.
///
/// `#[serde(transparent)]`: it deserializes from a plain string, so wrapping a
/// config field in `Secret` changes nothing about the document.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// Wrap a credential.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The raw credential — the sole accessor, and therefore the audit
    /// surface. Call it only at the point the value attaches to a request,
    /// never into a log, error, or other rendered output.
    pub fn reveal(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("***")
    }
}

impl std::fmt::Display for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("***")
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for Secret {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Secret")
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type": "string", "description": "secret value; never rendered"})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_and_display_never_render_the_value() {
        let secret = Secret::new("hunter2-super-secret");
        assert_eq!(format!("{secret:?}"), "***");
        assert_eq!(format!("{secret}"), "***");
        assert_eq!(secret.reveal(), "hunter2-super-secret");
    }

    #[test]
    fn serde_is_transparent() {
        let secret: Secret = serde_json::from_str("\"tok\"").unwrap();
        assert_eq!(secret.reveal(), "tok");
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"tok\"");
    }

    #[test]
    fn from_str_and_string_construct_equal_secrets() {
        assert_eq!(Secret::from("k"), Secret::from(String::from("k")));
    }
}
