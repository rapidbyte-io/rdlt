//! Secret-wrapped credential values (the 014 discipline): Debug/Display
//! render `***`, serde-transparent for config parsing, schemars placeholder.
//! The grep-proof cell asserts no rendering ever contains the value.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The only way to the value — call sites are the audit surface.
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

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl schemars::JsonSchema for Secret {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("Secret")
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type": "string", "description": "secret value; never rendered"})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_and_display_never_render_the_value() {
        let secret = Secret::new("hunter2-key");
        assert_eq!(format!("{secret:?}"), "***");
        assert_eq!(format!("{secret}"), "***");
        assert_eq!(secret.reveal(), "hunter2-key");
    }

    #[test]
    fn serde_is_transparent() {
        let secret: Secret = serde_json::from_str("\"s3-cred\"").unwrap();
        assert_eq!(secret.reveal(), "s3-cred");
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"s3-cred\"");
    }
}
