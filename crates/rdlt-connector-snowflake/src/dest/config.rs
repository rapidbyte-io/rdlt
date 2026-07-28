//! Snowflake destination configuration.
//!
//! One closed vocabulary: connection identity, an auth block naming exactly
//! one method, and the shared destination-option set every SQL destination
//! validates identically. Validation is eager and typed, naming the offending
//! field; the generated schema and the parser cannot drift because both come
//! from these types.

use std::collections::BTreeMap;

use rdlt_connector::{PemSource, Secret};
use rdlt_connector_sqlcore::options::DestOptions;
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

/// Where bulk parts are staged for `COPY INTO`.
///
/// A struct of optional kinds rather than an enum, matching the connector
/// family's `location:` convention — a second store is then an ADDITIVE field.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Stage {
    /// An S3 bucket the pipeline can write and the account can read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3Stage>,
}

/// An S3 bucket used as an external stage.
///
/// The credentials are needed on BOTH sides: this process writes the parts,
/// and the account reads them back. `storage_integration` changes only the
/// second half — the client still writes with these keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct S3Stage {
    /// The bucket.
    pub bucket: String,
    /// A key prefix inside it. rdlt scopes its own keys beneath this, so a
    /// bucket shared with other data stays safe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// The region. Defaults to `us-east-1`, which is what the signing code
    /// falls back to when a bucket's region is not stated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// An endpoint override, for S3-compatible storage.
    ///
    /// Snowflake reads such a bucket only after the endpoint is allowlisted
    /// for the account, which only Snowflake Support can do — there is no
    /// self-service parameter. Absent that, `CREATE STAGE` is refused and the
    /// load fails with the service's own reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Path-style addressing (`host/bucket/key`).
    ///
    /// Defaults OFF, unlike the file family's own S3 block: that one defaults
    /// on because it is usually pointed at a local test server, while a stage
    /// is by construction a cloud bucket the service must also reach.
    #[serde(default)]
    pub path_style: bool,
    /// Access key for writing the parts, and for the account to read them
    /// unless `storage_integration` supersedes that half.
    pub access_key: Secret,
    /// The matching secret key.
    pub secret_key: Secret,
    /// An account-level `STORAGE INTEGRATION` to read the bucket with.
    ///
    /// Preferred where it exists: without it the stage definition carries the
    /// key pair, which then lives in the account's metadata and in its query
    /// history. Creating one needs privileges a pipeline does not have, so it
    /// is referenced, never created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_integration: Option<String>,
}

impl S3Stage {
    /// A stage on a bucket reachable with these keys.
    pub fn new(
        bucket: impl Into<String>,
        access_key: impl Into<Secret>,
        secret_key: impl Into<Secret>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: None,
            region: None,
            endpoint: None,
            path_style: false,
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            storage_integration: None,
        }
    }

    /// Scope rdlt's keys under a prefix inside the bucket.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Name the bucket's region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Read the bucket through an account-level storage integration, keeping
    /// the key pair out of the stage definition.
    pub fn with_storage_integration(mut self, name: impl Into<String>) -> Self {
        self.storage_integration = Some(name.into());
        self
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.bucket.trim().is_empty() {
            return Err(ConfigError::Missing {
                field: "stage.s3.bucket",
            });
        }
        // A bucket NAME, for the same reason `account` is an identifier: the
        // URL form would be pasted into the stage definition and fail on the
        // service side with a message far from the cause.
        if self.bucket.contains("://") || self.bucket.contains('/') {
            return Err(ConfigError::Invalid {
                field: "stage.s3.bucket",
                detail: "is the bucket name alone, not a URL or a path".to_owned(),
            });
        }
        if let Some(endpoint) = &self.endpoint
            && !endpoint.contains("://")
        {
            return Err(ConfigError::Invalid {
                field: "stage.s3.endpoint",
                detail: "must name a scheme (`https://…`)".to_owned(),
            });
        }
        Ok(())
    }
}

impl Stage {
    /// A stage on S3.
    pub fn s3(s3: S3Stage) -> Self {
        Self { s3: Some(s3) }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let Some(s3) = &self.s3 else {
            return Err(ConfigError::Invalid {
                field: "stage",
                detail: "names no storage kind (expected `s3`)".to_owned(),
            });
        };
        s3.validate()
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
    /// A bucket to stage bulk parts in. Absent, rows are inserted directly.
    ///
    /// Optional because it is infrastructure the user supplies: with a bucket
    /// the rows travel as parquet and the service loads them itself; without
    /// one they travel inside statements, which needs nothing but works the
    /// same.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<Stage>,
    /// The destination-option vocabulary shared by every SQL destination:
    /// merge strategy, hard delete, dedup sort, merge scope, scd2 columns.
    #[serde(default, flatten)]
    pub options: DestOptions,
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
        if let Some(stage) = &self.stage {
            stage.validate()?;
        }
        Ok(())
    }

    /// The S3 stage this configuration names, if any.
    pub(super) fn s3_stage(&self) -> Option<&S3Stage> {
        self.stage.as_ref()?.s3.as_ref()
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
    fn a_stage_is_optional_and_absent_means_no_bulk_path() {
        let config = SnowflakeConfig::from_value(minimal()).expect("valid");
        assert!(
            config.s3_stage().is_none(),
            "no bucket configured, so no bulk path"
        );
    }

    #[test]
    fn a_configured_stage_parses_with_its_optional_parts_defaulted() {
        let mut doc = minimal();
        doc["stage"] = serde_json::json!({
            "s3": {"bucket": "parts", "access_key": "AK", "secret_key": "SK"}
        });
        let config = SnowflakeConfig::from_value(doc).expect("valid");
        let s3 = config.s3_stage().expect("stage");
        assert_eq!(s3.bucket, "parts");
        assert!(s3.prefix.is_none());
        assert!(
            !s3.path_style,
            "a stage is a cloud bucket the service must reach, so virtual-hosted by default"
        );
        assert!(s3.storage_integration.is_none());
    }

    #[test]
    fn a_stage_that_names_no_kind_is_refused_rather_than_ignored() {
        // An empty `stage:` block reads as "use the bulk path" but configures
        // nothing; silently falling back to inserts would hide the mistake.
        let mut doc = minimal();
        doc["stage"] = serde_json::json!({});
        let err = SnowflakeConfig::from_value(doc).expect_err("no kind");
        assert!(err.contains("stage"), "{err}");
    }

    #[test]
    fn a_bucket_url_is_refused_where_a_bucket_name_belongs() {
        for wrong in ["s3://parts", "parts/prefix"] {
            let mut doc = minimal();
            doc["stage"] = serde_json::json!({
                "s3": {"bucket": wrong, "access_key": "AK", "secret_key": "SK"}
            });
            let err = SnowflakeConfig::from_value(doc).expect_err("not a bucket name");
            assert!(err.contains("bucket"), "{err}");
        }
    }

    #[test]
    fn an_endpoint_without_a_scheme_is_refused() {
        let mut doc = minimal();
        doc["stage"] = serde_json::json!({
            "s3": {"bucket": "parts", "access_key": "AK", "secret_key": "SK",
                   "endpoint": "storage.example.com"}
        });
        let err = SnowflakeConfig::from_value(doc).expect_err("no scheme");
        assert!(err.contains("endpoint"), "{err}");
    }

    #[test]
    fn stage_keys_never_render() {
        let mut doc = minimal();
        doc["stage"] = serde_json::json!({
            "s3": {"bucket": "parts", "access_key": "AK-LEAK", "secret_key": "SK-LEAK"}
        });
        let config = SnowflakeConfig::from_value(doc).expect("valid");
        let rendered = format!("{config:?}");
        for leak in ["AK-LEAK", "SK-LEAK"] {
            assert!(!rendered.contains(leak), "{leak} rendered: {rendered}");
        }
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
