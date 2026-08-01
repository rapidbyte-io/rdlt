//! Destination configuration: the builder embedders call, the document
//! vocabulary pipelines spell, and the shared sqlcore options both consume.
//!
//! The options vocabulary + validation live in rdlt-connector-sqlcore
//! (shared with every SQL destination) and are re-exported here under their
//! bare sqlcore names — the same spelling every SQL destination uses, so a
//! config document reads identically whichever one consumes it.

use rdlt_connector::DestinationError;
use serde::{Deserialize, Serialize};

pub use rdlt_connector_sqlcore::{
    AbsentPolicy, DedupSort, DestinationOptions, MergeStrategy, Scd2Options, SortOrder,
    TableOptions,
};

/// The Postgres destination handle: connection string, target schema, TLS
/// posture, merge options. Built either programmatically (the builder) or
/// from a configuration document ([`Config`]).
#[derive(Debug, Clone)]
pub struct Postgres {
    pub(super) connection_string: String,
    pub(super) schema: String,
    pub(super) tls: Option<crate::tls::Policy>,
    pub(super) options: DestinationOptions,
}

impl Postgres {
    /// A handle over `connection_string` (e.g. `host=localhost
    /// user=postgres password=pw dbname=raw`). Nothing connects until the
    /// engine opens a load session.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            schema: "public".into(),
            tls: None,
            options: DestinationOptions::default(),
        }
    }

    /// Target schema (the pipeline vocabulary calls it `dataset`); created
    /// if missing.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// TLS posture — the SAME policy type the source uses; the two
    /// directions share one connect path.
    pub fn tls(mut self, policy: crate::tls::Policy) -> Self {
        self.tls = Some(policy);
        self
    }

    /// Strategy/hard-delete/SCD2 options. Validated here — errors name the
    /// field.
    pub fn options(mut self, options: DestinationOptions) -> Result<Self, DestinationError> {
        options.validate().map_err(DestinationError::fatal)?;
        self.options = options;
        Ok(self)
    }

    /// Build from a parsed configuration document.
    pub fn from_config(config: Config) -> Result<Self, ConfigError> {
        let options = DestinationOptions {
            merge_strategy: config.merge_strategy,
            tables: config.tables,
        };
        options.validate().map_err(ConfigError::Invalid)?;
        Ok(Self {
            connection_string: config.connection,
            schema: config.schema,
            tls: config.tls,
            options,
        })
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        Self::from_config(Config::from_yaml(yaml)?)
    }

    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        Self::from_config(Config::from_json(json)?)
    }

    /// The embedder entry point — a `serde_json::Value` straight through,
    /// same validation as every other entry point.
    pub fn from_value(value: serde_json::Value) -> Result<Self, ConfigError> {
        Self::from_config(Config::from_value(value)?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("parsing postgres destination config: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("parsing postgres destination JSON config: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid postgres destination config: {0}")]
    Invalid(String),
}

/// The destination configuration document — the same field vocabulary the
/// pipeline YAML's `destination: postgres:` block has always carried:
/// `conn`, `dataset`, `tls`, `merge_strategy`, `tables`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// libpq-style connection string/URL.
    #[serde(rename = "conn")]
    #[schemars(rename = "conn")]
    pub connection: String,
    /// Target schema (the document vocabulary calls it `dataset`); created
    /// if missing.
    #[serde(rename = "dataset", default = "default_schema")]
    #[schemars(rename = "dataset")]
    pub schema: String,
    /// TLS posture (verify-* modes are expressible only here).
    #[serde(default)]
    pub tls: Option<crate::tls::Policy>,
    /// Default merge strategy for every merge table (sqlcore vocabulary).
    #[serde(default)]
    pub merge_strategy: Option<MergeStrategy>,
    /// Per-table strategy/hard-delete/SCD2 options (sqlcore vocabulary).
    #[serde(default)]
    pub tables: std::collections::BTreeMap<String, TableOptions>,
}

impl Config {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        Ok(serde_yaml::from_str(yaml)?)
    }

    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        Ok(serde_json::from_str(json)?)
    }

    pub fn from_value(value: serde_json::Value) -> Result<Self, ConfigError> {
        Ok(serde_json::from_value(value)?)
    }
}

fn default_schema() -> String {
    "public".into()
}

/// JSON Schema GENERATED from the config struct — the declared schema and
/// the parser cannot drift. Published through `ConnectorSpec`, so a
/// platform can render a destination form, not just a source one.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_round_trips_and_validates() {
        let destination = Postgres::from_yaml(
            r#"
conn: "host=h user=u"
dataset: raw
merge_strategy: upsert
tables:
  events:
    merge_strategy: delete_insert
"#,
        )
        .expect("parses");
        assert_eq!(destination.schema, "raw");
        assert_eq!(
            destination.options.merge_strategy,
            Some(MergeStrategy::Upsert)
        );
        // Dataset defaults to public; unknown fields refuse.
        let bare = Postgres::from_yaml("conn: \"host=h\"\n").expect("parses");
        assert_eq!(bare.schema, "public");
        assert!(Postgres::from_yaml("conn: \"host=h\"\nghost: 1\n").is_err());
    }

    #[test]
    fn invalid_options_are_refused_at_parse() {
        // scd2 options without the scd2 strategy — sqlcore's validation
        // fires through the document entry point too.
        let err = Postgres::from_yaml(
            "conn: \"host=h\"\ntables:\n  events:\n    scd2:\n      valid_from: vf\n",
        )
        .expect_err("invalid options refuse");
        assert!(matches!(err, ConfigError::Invalid(_)), "{err}");
    }

    #[test]
    fn schema_is_generated_from_the_struct() {
        let schema = config_schema();
        let properties = schema["properties"].as_object().expect("properties");
        for field in ["conn", "dataset", "tls", "merge_strategy", "tables"] {
            assert!(properties.contains_key(field), "{field}");
        }
    }
}
