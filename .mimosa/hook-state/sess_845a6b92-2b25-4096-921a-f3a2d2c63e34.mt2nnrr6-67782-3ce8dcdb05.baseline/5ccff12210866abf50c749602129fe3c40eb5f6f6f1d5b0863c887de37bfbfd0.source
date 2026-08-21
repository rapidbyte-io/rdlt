//! [`Secret`] — a credential that cannot be printed by accident.
//!
//! One shared type for every connector. Serde-transparent, so config
//! documents keep plain strings in and out; `Debug` and `Display` both
//! render `***`, so a credential nested anywhere in a config struct that
//! derives `Debug` — or interpolated into an error — never leaks.
//!
//! [`Secret::reveal`] is the ONLY accessor, which makes the set of
//! `reveal()` call sites the entire audit surface: every exposure is a
//! grep away. The rule connectors follow: reveal at the exact point the
//! credential attaches to an outbound request (a header, a form field, a
//! connection property) and never into anything rendered.
//!
//! The `schemars::JsonSchema` impl (a plain string placeholder) rides the
//! crate's `schema` feature so the SPI carries no mandatory schemars
//! dependency.

use serde::{Deserialize, Serialize};

/// A credential string that renders as `***` from both `Debug` and
/// `Display`; [`Secret::reveal`] is the sole way to the value.
///
/// `#[serde(transparent)]`: wrapping a config field in `Secret` changes
/// nothing about the document. A config document crossing the local
/// connector socket therefore carries secrets in the clear — the spawned
/// connector inherits the operator's trust like any child process;
/// secret references are the recorded future for network transports.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// Wrap a credential.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The raw credential — the sole accessor, and therefore the audit
    /// surface. Call it only where the value attaches to a request; never
    /// into a log, an error, or any other rendered output.
    pub fn reveal(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("***")
    }
}

impl std::fmt::Display for Secret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("***")
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
    fn both_renderings_mask_and_reveal_is_the_only_way_through() {
        let secret = Secret::new("hunter2-credential");
        assert_eq!(format!("{secret:?}"), "***");
        assert_eq!(format!("{secret}"), "***");
        assert_eq!(secret.reveal(), "hunter2-credential");
    }

    /// The mask survives nesting: a struct deriving Debug delegates here.
    #[test]
    fn the_mask_survives_a_derived_debug() {
        #[derive(Debug)]
        struct Config {
            token: Secret,
        }
        let config = Config {
            token: Secret::new("leakable"),
        };
        assert!(!format!("{config:?}").contains("leakable"));
        // Masking is a rendering property, not storage: the credential
        // still reads back in full where it is meant to.
        assert_eq!(config.token.reveal(), "leakable");
    }

    #[test]
    fn serde_is_transparent_in_both_directions() {
        let secret: Secret = serde_json::from_str("\"tok\"").expect("a plain string parses");
        assert_eq!(secret.reveal(), "tok");
        assert_eq!(serde_json::to_string(&secret).expect("renders"), "\"tok\"");
    }

    #[test]
    fn string_and_str_conversions_agree() {
        assert_eq!(Secret::from("k"), Secret::from(String::from("k")));
    }

    /// The generated schema must say STRING: a platform renders a form
    /// from it and validates input against it, so an untyped schema would
    /// stop rejecting wrong-typed values.
    #[cfg(feature = "schema")]
    #[test]
    fn the_schema_is_a_string_carrying_the_never_rendered_warning() {
        let schema = schemars::schema_for!(Secret);
        let json = serde_json::to_value(&schema).expect("schema serializes");
        assert_eq!(json.get("type").and_then(|t| t.as_str()), Some("string"));
        assert!(
            json.get("description")
                .and_then(|d| d.as_str())
                .is_some_and(|d| d.contains("never rendered")),
            "{json}"
        );
    }
}
