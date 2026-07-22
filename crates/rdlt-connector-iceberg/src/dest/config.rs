//! Iceberg destination configuration (contract ID1/ID6, data-model §1):
//! catalog connection (uri/warehouse/auth), namespace, optional storage
//! override sharing the FAMILY S3 spelling, per-stream table options with
//! partition specs. All credentials are `Secret`-wrapped; validation is
//! eager and typed; the generated schema and the parser cannot drift.
//!
//! NOTE (recorded): this is the third crate-local `Secret` copy
//! (014 rest, 015 file, 016 here) — the extraction trigger has FIRED;
//! unification into a shared home is a dedicated follow-up, not smuggled
//! into this feature.

use std::borrow::Cow;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// ---- Secret --------------------------------------------------------------

/// Secret-wrapped credential value: Debug/Display render `***`,
/// serde-transparent, schemars placeholder (the 014 discipline).
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

// ---- Auth ----------------------------------------------------------------

/// `auth: {oauth2_client_credentials: …}` | `auth: {bearer: …}` — a struct
/// of optional kinds (exactly one set, validated) so YAML stays natural and
/// future schemes (sigv4, phase-2) are ADDITIVE fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct AuthOptions {
    #[serde(default)]
    pub oauth2_client_credentials: Option<Oauth2ClientCredentials>,
    #[serde(default)]
    pub bearer: Option<BearerAuth>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Oauth2ClientCredentials {
    /// Defaults to the catalog's own token endpoint.
    #[serde(default)]
    pub token_url: Option<String>,
    pub client_id: String,
    pub client_secret: Secret,
    #[serde(default)]
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct BearerAuth {
    pub token: Secret,
}

impl AuthOptions {
    pub fn oauth2(
        client_id: impl Into<String>,
        client_secret: impl Into<Secret>,
        scopes: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            oauth2_client_credentials: Some(Oauth2ClientCredentials {
                token_url: None,
                client_id: client_id.into(),
                client_secret: client_secret.into(),
                scopes: scopes.into_iter().collect(),
            }),
            bearer: None,
        }
    }

    pub fn bearer(token: impl Into<Secret>) -> Self {
        Self {
            oauth2_client_credentials: None,
            bearer: Some(BearerAuth {
                token: token.into(),
            }),
        }
    }

    fn validate(&self) -> Result<(), String> {
        match (&self.oauth2_client_credentials, &self.bearer) {
            (Some(_), Some(_)) => Err("catalog.auth declares BOTH \
                 oauth2_client_credentials and bearer — pick one"
                .into()),
            (None, None) => Err("catalog.auth declares no scheme (expected \
                 oauth2_client_credentials or bearer)"
                .into()),
            _ => Ok(()),
        }
    }
}

// ---- Catalog -------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CatalogOptions {
    /// The Iceberg REST catalog root, e.g. `https://…/api/catalog`.
    pub uri: String,
    /// Provider-defined warehouse identifier.
    pub warehouse: String,
    pub auth: AuthOptions,
    /// Escape hatch: extra catalog properties passed through verbatim.
    #[serde(default)]
    pub props: BTreeMap<String, String>,
}

// ---- Storage override (family S3 spelling; catalog-supplied otherwise) ---

/// `storage: {s3: {…}}` — the family S3 spelling (no `bucket`: table
/// locations come from the catalog). Absent = catalog-supplied
/// credentials via `/v1/config` defaults (the verified delegation path).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct StorageOptions {
    #[serde(default)]
    pub s3: Option<S3Override>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct S3Override {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    pub access_key: Secret,
    pub secret_key: Secret,
    #[serde(default = "default_path_style")]
    pub path_style: bool,
}

fn default_path_style() -> bool {
    true
}

impl S3Override {
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            endpoint: None,
            region: None,
            access_key: Secret::new(access_key),
            secret_key: Secret::new(secret_key),
            path_style: true,
        }
    }

    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    #[must_use]
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }
}

// ---- Tables & partitioning ----------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PartitionField {
    pub column: String,
    pub transform: PartitionTransform,
}

impl PartitionField {
    pub fn new(column: impl Into<String>, transform: PartitionTransform) -> Self {
        Self {
            column: column.into(),
            transform,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TableOptions {
    /// Table name (default: the stream name).
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub partition_by: Vec<PartitionField>,
}

impl TableOptions {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    #[must_use]
    pub fn with_partition(mut self, field: PartitionField) -> Self {
        self.partition_by.push(field);
        self
    }
}

// ---- The document --------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct IcebergConfig {
    pub catalog: CatalogOptions,
    /// Dot-separated namespace levels, e.g. `raw.orders`.
    pub namespace: String,
    /// Explicit create-if-missing (never silent).
    #[serde(default)]
    pub create_namespace: bool,
    #[serde(default)]
    pub storage: Option<StorageOptions>,
    /// Per-stream table options.
    #[serde(default)]
    pub tables: BTreeMap<String, TableOptions>,
}

impl IcebergConfig {
    pub fn new(
        catalog_uri: impl Into<String>,
        warehouse: impl Into<String>,
        auth: AuthOptions,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            catalog: CatalogOptions {
                uri: catalog_uri.into(),
                warehouse: warehouse.into(),
                auth,
                props: BTreeMap::new(),
            },
            namespace: namespace.into(),
            create_namespace: false,
            storage: None,
            tables: BTreeMap::new(),
        }
    }

    pub fn with_create_namespace(mut self, create: bool) -> Self {
        self.create_namespace = create;
        self
    }

    pub fn with_catalog_prop(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.catalog.props.insert(key.into(), value.into());
        self
    }

    pub fn with_storage_s3(mut self, s3: S3Override) -> Self {
        self.storage = Some(StorageOptions { s3: Some(s3) });
        self
    }

    pub fn with_table(mut self, stream: impl Into<String>, options: TableOptions) -> Self {
        self.tables.insert(stream.into(), options);
        self
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let config: IcebergConfig = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        let config: IcebergConfig = serde_json::from_str(json)?;
        config.validate()?;
        Ok(config)
    }

    /// The embedder entry point (JSON document, no string round-trip).
    pub fn from_value(value: serde_json::Value) -> Result<Self, ConfigError> {
        let config: IcebergConfig = serde_json::from_value(value)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        if !self.catalog.uri.starts_with("http://") && !self.catalog.uri.starts_with("https://") {
            return invalid(format!(
                "catalog.uri `{}` must be an http(s) URL",
                self.catalog.uri
            ));
        }
        if self.catalog.warehouse.is_empty() {
            return invalid("catalog.warehouse must not be empty".into());
        }
        self.catalog.auth.validate().map_err(ConfigError::Invalid)?;
        if let Some(oauth) = &self.catalog.auth.oauth2_client_credentials
            && let Some(url) = &oauth.token_url
            && !url.starts_with("http://")
            && !url.starts_with("https://")
        {
            return invalid(format!(
                "catalog.auth.oauth2_client_credentials.token_url `{url}` must be an http(s) URL"
            ));
        }
        if self.namespace.is_empty() || self.namespace.split('.').any(str::is_empty) {
            return invalid(format!(
                "namespace `{}` must be non-empty dot-separated levels",
                self.namespace
            ));
        }
        if let Some(storage) = &self.storage
            && storage.s3.is_none()
        {
            return invalid("`storage` block declares no kind (expected `s3`)".into());
        }
        for (stream, table) in &self.tables {
            if let Some(name) = &table.name
                && name.is_empty()
            {
                return invalid(format!("tables.{stream}.name must not be empty"));
            }
            for field in &table.partition_by {
                if field.column.is_empty() {
                    return invalid(format!(
                        "tables.{stream}.partition_by: a field names no column"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Namespace as its dot-separated levels.
    pub fn namespace_levels(&self) -> Vec<String> {
        self.namespace.split('.').map(str::to_owned).collect()
    }

    /// The Iceberg table name for a stream.
    pub fn table_name(&self, stream: &str) -> String {
        self.tables
            .get(stream)
            .and_then(|t| t.name.clone())
            .unwrap_or_else(|| stream.to_owned())
    }

    /// Declared partition fields for a stream.
    pub fn partition_fields(&self, stream: &str) -> &[PartitionField] {
        self.tables
            .get(stream)
            .map(|t| t.partition_by.as_slice())
            .unwrap_or(&[])
    }
}

/// JSON Schema GENERATED from the config structs.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(IcebergConfig)).expect("schema serializes")
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid iceberg destination YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid iceberg destination JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid iceberg destination config: {0}")]
    Invalid(String),
}
