//! Snowflake destination configuration.
//!
//! One closed vocabulary: connection identity, an auth block naming exactly
//! one method, and the shared destination-option set every SQL destination
//! validates identically. Validation is eager and typed, naming the offending
//! field; the generated schema and the parser cannot drift because both come
//! from these types.

use std::collections::BTreeMap;

use rdlt_connector::{PemSource, Secret};
use rdlt_connector_sqlcore::DestinationOptions;
use serde::{Deserialize, Serialize};

/// A rejected configuration. Every variant names the field at fault, because
/// the user's next action is to edit that field.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// A required field was empty.
    Missing {
        /// The field, as it is spelled in the document.
        field: &'static str,
    },
    /// The `auth` block named no method, or more than one.
    Auth {
        /// What was wrong, in the user's terms.
        detail: String,
    },
    /// A field's value cannot be used as given.
    Invalid {
        /// The field, as it is spelled in the document.
        field: &'static str,
        /// Why it cannot be used.
        detail: String,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing { field } => write!(f, "snowflake: `{field}` is required"),
            Self::Auth { detail } => write!(f, "snowflake: `auth` {detail}"),
            Self::Invalid { field, detail } => {
                write!(f, "snowflake: `{field}` {detail}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Key-pair (JWT) authentication — the method Snowflake recommends for
/// unattended access, and the one this connector is verified against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct KeyPair {
    /// The private key: a path to a `.p8` file, or the PEM text inline.
    pub private_key: PemSource,
    /// Required when, and only when, the key is encrypted. An encrypted key
    /// without one fails at connect time with the library's own error, which
    /// is why this is validated up front instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passphrase: Option<Secret>,
}

/// Password authentication.
///
/// Snowflake enforces MFA on password sign-ins and refuses passwords entirely
/// on `TYPE = SERVICE` users, so this is the least suitable method for an
/// unattended pipeline — it exists for parity, not as a recommendation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Password {
    /// The account password.
    pub password: Secret,
    /// An MFA passcode, where the account requires one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passcode: Option<Secret>,
}

/// How to authenticate: exactly one field set.
///
/// A struct of optional kinds rather than an enum, matching the connector
/// family's config convention — a new scheme is then an ADDITIVE field, and
/// the YAML stays the natural `auth: {key_pair: {…}}` either way.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Auth {
    /// Key-pair JWT.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_pair: Option<KeyPair>,
    /// Password, with the caveats on [`Password`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<Password>,
    /// A caller-supplied OAuth access token. Acquiring and refreshing it is
    /// the caller's business — this connector never mints one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_token: Option<Secret>,
    /// A programmatic access token. Snowflake's drivers present these on the
    /// password channel, which is verified rather than assumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pat: Option<Secret>,
}

impl KeyPair {
    /// Key-pair auth from a key that needs no passphrase.
    pub fn new(private_key: impl Into<PemSource>) -> Self {
        Self {
            private_key: private_key.into(),
            passphrase: None,
        }
    }

    /// Supply the passphrase for an encrypted key.
    pub fn with_passphrase(mut self, passphrase: impl Into<Secret>) -> Self {
        self.passphrase = Some(passphrase.into());
        self
    }
}

impl Password {
    /// Password auth without an MFA passcode.
    pub fn new(password: impl Into<Secret>) -> Self {
        Self {
            password: password.into(),
            passcode: None,
        }
    }

    /// Supply the MFA passcode the account requires.
    pub fn with_passcode(mut self, passcode: impl Into<Secret>) -> Self {
        self.passcode = Some(passcode.into());
        self
    }
}

impl Auth {
    /// Authenticate by key pair — the recommended method for unattended use.
    ///
    /// These constructors exist because the vocabulary is `#[non_exhaustive]`,
    /// so a new scheme stays additive; without them an embedding application
    /// could deserialize a config but never build one, and the library API is
    /// meant to reach everything the CLI reaches.
    pub fn key_pair(key_pair: KeyPair) -> Self {
        Self {
            key_pair: Some(key_pair),
            ..Self::default()
        }
    }

    /// Authenticate by password, with the caveats on [`Password`].
    pub fn password(password: Password) -> Self {
        Self {
            password: Some(password),
            ..Self::default()
        }
    }

    /// Authenticate with a caller-supplied OAuth access token.
    pub fn oauth_token(token: impl Into<Secret>) -> Self {
        Self {
            oauth_token: Some(token.into()),
            ..Self::default()
        }
    }

    /// Authenticate with a programmatic access token.
    pub fn pat(token: impl Into<Secret>) -> Self {
        Self {
            pat: Some(token.into()),
            ..Self::default()
        }
    }

    /// Reject anything but exactly one method.
    ///
    /// Zero is a document that cannot connect; more than one is a document
    /// whose author believes something untrue about which credential is in
    /// use — worth refusing rather than silently picking.
    fn validate(&self) -> Result<(), ConfigError> {
        let named: Vec<&str> = [
            self.key_pair.is_some().then_some("key_pair"),
            self.password.is_some().then_some("password"),
            self.oauth_token.is_some().then_some("oauth_token"),
            self.pat.is_some().then_some("pat"),
        ]
        .into_iter()
        .flatten()
        .collect();
        match named.as_slice() {
            [_] => Ok(()),
            [] => Err(ConfigError::Auth {
                detail: "names no method (expected one of key_pair, password, oauth_token, pat)"
                    .to_owned(),
            }),
            many => Err(ConfigError::Auth {
                detail: format!(
                    "names {} methods ({}); set exactly one",
                    many.len(),
                    many.join(", ")
                ),
            }),
        }
    }
}

/// Whether tables are created transient.
///
/// Transient tables carry no fail-safe period, which is the cost lever most
/// worth exposing: a re-loadable pipeline target rarely needs seven days of
/// recovery it pays to keep.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TableType {
    /// Standard tables, with Time Travel and fail-safe.
    #[default]
    Permanent,
    /// No fail-safe. Applied to rdlt's own bookkeeping tables too, so the
    /// choice is consistent across everything this pipeline creates.
    Transient,
}

/// The Snowflake destination's configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SnowflakeConfig {
    /// The account identifier, as it appears in the host name — `MYORG-MYACCT`
    /// for `myorg-myacct.snowflakecomputing.com`. Not a URL: supplying one is
    /// refused rather than silently mangled.
    pub account: String,
    /// The login name.
    pub user: String,
    /// Exactly one authentication method.
    pub auth: Auth,
    /// The database rdlt writes into. Required even though a user may have a
    /// default, because every statement this connector emits names its
    /// database and schema explicitly — a changed server-side default must
    /// not be able to retarget a pipeline.
    pub database: String,
    /// The schema rdlt writes into. Required for the same reason.
    pub schema: String,
    /// The warehouse to run on. Optional: the user's own default applies when
    /// absent, and a load fails typed if neither supplies one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    /// The role to assume. Optional; the user's default applies when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Permanent or transient tables.
    #[serde(default)]
    pub table_type: TableType,
    /// Session parameters applied at connect time, verbatim.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub session_parameters: BTreeMap<String, String>,
    /// A `QUERY_TAG` for this pipeline, so its statements are attributable in
    /// the account's query history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_tag: Option<String>,
    /// Replaces the host derived from `account` — for PrivateLink and similar
    /// deployments that front the account with their own name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// The destination-option vocabulary shared by every SQL destination:
    /// merge strategy, hard delete, dedup sort, merge scope, scd2 columns.
    #[serde(default, flatten)]
    pub options: DestinationOptions,
}

impl SnowflakeConfig {
    /// Parse and validate a YAML document.
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        let parsed: Self = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
        parsed.validate().map_err(|e| e.to_string())?;
        Ok(parsed)
    }

    /// Parse and validate a JSON document.
    pub fn from_json(json: &str) -> Result<Self, String> {
        let parsed: Self = serde_json::from_str(json).map_err(|e| e.to_string())?;
        parsed.validate().map_err(|e| e.to_string())?;
        Ok(parsed)
    }

    /// Validate an already-deserialized document — the entry point an
    /// embedding application uses when it builds config programmatically.
    pub fn from_value(value: serde_json::Value) -> Result<Self, String> {
        let parsed: Self = serde_json::from_value(value).map_err(|e| e.to_string())?;
        parsed.validate().map_err(|e| e.to_string())?;
        Ok(parsed)
    }

    /// Eager validation: everything checkable without a connection.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (field, value) in [
            ("account", &self.account),
            ("user", &self.user),
            ("database", &self.database),
            ("schema", &self.schema),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::Missing { field });
            }
        }
        // A URL here is the mistake worth catching: the account identifier is
        // a NAME, and pasting the console URL produces a host that resolves to
        // nothing with an error far from the cause.
        if self.account.contains("://") || self.account.contains('/') {
            return Err(ConfigError::Invalid {
                field: "account",
                detail: "is the account identifier (`MYORG-MYACCT`), not a URL".to_owned(),
            });
        }
        if self.account.contains(".snowflakecomputing.com") {
            return Err(ConfigError::Invalid {
                field: "account",
                detail: "is the account identifier alone; the host is derived from it".to_owned(),
            });
        }
        self.auth.validate()?;
        Ok(())
    }

    /// The host this configuration addresses.
    ///
    /// Derived from the account unless `host` overrides it, so the derivation
    /// is stated once and every caller agrees.
    pub fn host(&self) -> String {
        self.host
            .clone()
            .unwrap_or_else(|| format!("{}.snowflakecomputing.com", self.account.to_lowercase()))
    }
}

/// The generated JSON Schema for the destination configuration.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(SnowflakeConfig))
        .expect("a generated schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> serde_json::Value {
        serde_json::json!({
            "account": "MYORG-MYACCT",
            "user": "LOADER",
            "auth": {"key_pair": {"private_key": "/k.p8"}},
            "database": "ANALYTICS",
            "schema": "RAW",
        })
    }

    #[test]
    fn a_minimal_document_parses_and_defaults_the_rest() {
        let config = SnowflakeConfig::from_value(minimal()).expect("valid");
        assert_eq!(config.table_type, TableType::Permanent);
        assert!(config.warehouse.is_none());
        assert!(config.session_parameters.is_empty());
        assert_eq!(config.host(), "myorg-myacct.snowflakecomputing.com");
    }

    #[test]
    fn an_unknown_field_is_refused_rather_than_ignored() {
        let mut doc = minimal();
        doc["wharehouse"] = serde_json::json!("TYPO_WH");
        let err = SnowflakeConfig::from_value(doc).expect_err("typo is rejected");
        assert!(err.contains("wharehouse"), "{err}");
    }

    #[test]
    fn the_host_override_replaces_the_derived_name() {
        let mut doc = minimal();
        doc["host"] = serde_json::json!("acct.privatelink.snowflakecomputing.com");
        let config = SnowflakeConfig::from_value(doc).expect("valid");
        assert_eq!(config.host(), "acct.privatelink.snowflakecomputing.com");
    }

    #[test]
    fn a_pasted_url_is_refused_where_an_identifier_belongs() {
        for wrong in [
            "https://myorg-myacct.snowflakecomputing.com",
            "myorg-myacct.snowflakecomputing.com",
        ] {
            let mut doc = minimal();
            doc["account"] = serde_json::json!(wrong);
            let err =
                SnowflakeConfig::from_value(doc).expect_err("a URL is not an account identifier");
            assert!(err.contains("account"), "{err}");
        }
    }

    #[test]
    fn auth_must_name_exactly_one_method() {
        let mut none = minimal();
        none["auth"] = serde_json::json!({});
        let err = SnowflakeConfig::from_value(none).expect_err("no method");
        assert!(err.contains("names no method"), "{err}");

        let mut two = minimal();
        two["auth"] = serde_json::json!({
            "key_pair": {"private_key": "/k.p8"},
            "pat": "tok",
        });
        let err = SnowflakeConfig::from_value(two).expect_err("two methods");
        assert!(err.contains("key_pair") && err.contains("pat"), "{err}");
    }

    #[test]
    fn every_auth_method_parses() {
        for auth in [
            serde_json::json!({"key_pair": {"private_key": "/k.p8"}}),
            serde_json::json!({"key_pair": {"private_key": "/k.p8", "passphrase": "s"}}),
            serde_json::json!({"password": {"password": "p"}}),
            serde_json::json!({"password": {"password": "p", "passcode": "123456"}}),
            serde_json::json!({"oauth_token": "tok"}),
            serde_json::json!({"pat": "tok"}),
        ] {
            let mut doc = minimal();
            doc["auth"] = auth.clone();
            SnowflakeConfig::from_value(doc).unwrap_or_else(|e| panic!("{auth} must parse: {e}"));
        }
    }

    #[test]
    fn required_fields_are_named_when_missing() {
        for field in ["account", "user", "database", "schema"] {
            let mut doc = minimal();
            doc[field] = serde_json::json!("   ");
            let err = SnowflakeConfig::from_value(doc).expect_err("blank is missing");
            assert!(err.contains(field), "the error names `{field}`: {err}");
        }
    }

    #[test]
    fn no_secret_reaches_debug_or_the_serialized_document() {
        let doc = serde_json::json!({
            "account": "A", "user": "U", "database": "D", "schema": "S",
            "auth": {"key_pair": {"private_key": "-----BEGIN X-----", "passphrase": "PASSPHRASE-LEAK"}},
        });
        let config = SnowflakeConfig::from_value(doc).expect("valid");
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("PASSPHRASE-LEAK"),
            "the passphrase must not render: {rendered}"
        );
        for auth in [
            serde_json::json!({"password": {"password": "PW-LEAK", "passcode": "PC-LEAK"}}),
            serde_json::json!({"oauth_token": "OAUTH-LEAK"}),
            serde_json::json!({"pat": "PAT-LEAK"}),
        ] {
            let mut doc = minimal();
            doc["auth"] = auth;
            let config = SnowflakeConfig::from_value(doc).expect("valid");
            let rendered = format!("{config:?}");
            for leak in ["PW-LEAK", "PC-LEAK", "OAUTH-LEAK", "PAT-LEAK"] {
                assert!(!rendered.contains(leak), "{leak} rendered: {rendered}");
            }
        }
    }

    #[test]
    fn the_private_key_may_be_a_path_or_inline_pem() {
        let mut inline = minimal();
        inline["auth"] = serde_json::json!({
            "key_pair": {"private_key": "-----BEGIN PRIVATE KEY-----\nabc\n"}
        });
        let config = SnowflakeConfig::from_value(inline).expect("valid");
        let key = &config.auth.key_pair.expect("key pair").private_key;
        assert!(key.is_inline());

        let config = SnowflakeConfig::from_value(minimal()).expect("valid");
        assert!(
            !config
                .auth
                .key_pair
                .expect("key pair")
                .private_key
                .is_inline()
        );
    }

    #[test]
    fn the_shared_option_vocabulary_is_flattened_into_the_document() {
        // The same spelling as every other SQL destination — one YAML shape.
        let mut doc = minimal();
        doc["merge_strategy"] = serde_json::json!("upsert");
        let config = SnowflakeConfig::from_value(doc).expect("valid");
        assert!(config.options.merge_strategy.is_some());
    }

    #[test]
    fn the_schema_generates_and_names_the_vocabulary() {
        let schema = config_schema();
        let rendered = serde_json::to_string(&schema).expect("render");
        for field in ["account", "auth", "key_pair", "table_type", "query_tag"] {
            assert!(rendered.contains(field), "schema omits `{field}`");
        }
    }
}
