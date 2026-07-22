//! `Secret` — a string that never renders (contract RS4).
//!
//! Serde-transparent (config documents stay plain strings); Debug/Display
//! print `***`; the generated JSON schema shows an ordinary string. The only
//! way to read the value is [`Secret::reveal`], which every call site uses
//! at the exact point the credential attaches to a request — never in an
//! error message.

use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The raw credential — use ONLY at the attachment point.
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

impl schemars::JsonSchema for Secret {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Secret".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "credential — redacted from all output"
        })
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
}
