//! The pipeline spec: the ONE YAML document model that both consumers of the
//! engine — the `rdlt` CLI and the `rdlt-bench` harness — parse, and its
//! construction into a runnable [`Pipeline`].
//!
//! ONE YAML document describes a whole pipeline: pipeline-wide settings, the
//! source (inline, or `config: path` to a reusable document), and the
//! destination. The CLI and the bench harness used to carry byte-identical
//! copies of these structs; they now share this one, so a destination or
//! source kind cannot be taught to one parser and forgotten in the other. The
//! shared fixture `benches/parity_specs.yaml` pins the model from both
//! consumers.
//!
//! Each variant that names a connector type is feature-gated to that
//! connector, so the facade still builds with any subset of connectors (down
//! to none): a spec that names a connector this build did not compile in fails
//! to parse (the variant does not exist), never silently.

use std::path::PathBuf;

use serde::Deserialize;

use crate::builder::{Missing, PipelineBuilder};
use crate::{Pipeline, WriteMode};

#[cfg(feature = "postgres-source")]
use crate::connector::postgres::source::PostgresConfig;

/// One pipeline, end to end.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub pipeline: String,
    #[serde(default)]
    pub workdir: Option<PathBuf>,
    // singleton_map: YAML's natural `write_mode: {merge: {key: […]}}` /
    // `source: postgres: …` singleton-map form for externally-tagged
    // enums (serde_yaml 0.9 otherwise wants `!tag` syntax).
    #[serde(default, with = "serde_yaml::with::singleton_map")]
    pub write_mode: Option<WriteModeSpec>,
    #[serde(with = "serde_yaml::with::singleton_map")]
    pub source: SourceSpec,
    #[serde(with = "serde_yaml::with::singleton_map")]
    pub destination: DestSpec,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteModeSpec {
    Append,
    Replace,
    Merge { key: Vec<String> },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceSpec {
    /// Path to the declarative REST source YAML.
    #[cfg(feature = "rest")]
    Rest { config: PathBuf },
    /// Path to the file source YAML (jsonl/parquet streams).
    #[cfg(feature = "file")]
    File { config: PathBuf },
    /// Postgres source: the config document INLINE (the natural form — the
    /// pipeline is one YAML document), or `config: path` referencing a
    /// reusable YAML/JSON file with the identical shape.
    #[cfg(feature = "postgres-source")]
    Postgres(PgSourceSpec),
}

#[cfg(feature = "postgres-source")]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum PgSourceSpec {
    /// `source: postgres: {config: source.yaml}` — tried first; strict
    /// (`deny_unknown_fields`), so `config` mixed with inline fields is a
    /// loud error, never a silently-ignored document.
    File(PgSourceFile),
    /// The full source document inline (boxed — it dwarfs the path form).
    Inline(Box<PostgresConfig>),
}

#[cfg(feature = "postgres-source")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgSourceFile {
    pub config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DestSpec {
    #[cfg(feature = "duckdb")]
    Duckdb {
        path: PathBuf,
        memory_limit: Option<String>,
        /// The SAME destination-options vocabulary as postgres — shared
        /// sqlcore types, one YAML shape.
        merge_strategy: Option<crate::connector::duckdb::dest::MergeStrategy>,
        tables: Option<
            std::collections::BTreeMap<String, crate::connector::duckdb::dest::TableOptions>,
        >,
        /// dlt-parity passthrough: extensions to LOAD and `SET` settings.
        extensions: Option<Vec<String>>,
        settings: Option<std::collections::BTreeMap<String, String>>,
    },
    #[cfg(feature = "postgres-dest")]
    Postgres {
        conn: String,
        dataset: String,
        /// Optional TLS block: `tls: {mode: verify_full, root_cert: /ca.pem}`.
        tls: Option<crate::connector::postgres::tls::TlsPolicy>,
        /// Destination-wide merge strategy
        /// ("delete_insert" | "upsert" | "scd2").
        merge_strategy: Option<crate::connector::postgres::dest::MergeStrategy>,
        /// Per-table options — `tables: <name>: {…}` with
        /// `merge_strategy`, `hard_delete`, `dedup_sort`, `merge_scope`, and
        /// `scd2: {valid_from, valid_to, absent}`.
        tables: Option<
            std::collections::BTreeMap<String, crate::connector::postgres::dest::TableOptions>,
        >,
    },
    /// The frozen `parquet:` spelling (equivalent to `file: local parquet`);
    /// the parquet destination lives in the file family.
    #[cfg(feature = "file")]
    Parquet { path: PathBuf },
    /// The full file-destination vocabulary — format (parquet|jsonl),
    /// location (local | s3), partition_by.
    #[cfg(feature = "file")]
    File {
        path: String,
        format: Option<crate::connector::file::dest::DestFormat>,
        location: Option<crate::connector::file::location::LocationOptions>,
        partition_by: Option<String>,
        /// Mirrors `FileDestConfig::parquet`. This enum is a hand-maintained
        /// mirror rebuilt field by field below, so a field added to the
        /// destination config and NOT added here compiles fine and is simply
        /// unreachable from a pipeline document — silently unconfigurable.
        /// The reverse direction is safe: adding here forces the destructure
        /// below to be updated.
        parquet: Option<crate::connector::file::dest::ParquetOptions>,
    },
    /// The Iceberg destination — the crate's full config vocabulary inline
    /// (catalog/auth, namespace, storage override, per-stream tables with
    /// partition_by).
    #[cfg(feature = "iceberg")]
    Iceberg(Box<crate::connector::iceberg::IcebergConfig>),
}

/// A spec that could not be turned into a pipeline. Two shapes so consumers
/// keep their exit-code taxonomy: [`SpecError::Resolve`] is a config/parse/IO
/// problem (the CLI's exit code 2), while [`SpecError::Build`] carries the
/// engine's own typed error from the typestate builder.
#[derive(Debug)]
pub enum SpecError {
    /// Resolving the spec into connectors failed — a missing/invalid config
    /// file, an unopenable destination, rejected destination options.
    Resolve(String),
    /// The typestate builder rejected the configuration (e.g. a destination
    /// that cannot Merge).
    Build(crate::RdltError),
}

impl SpecError {
    fn resolve(message: impl Into<String>) -> Self {
        SpecError::Resolve(message.into())
    }
}

impl std::fmt::Display for SpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpecError::Resolve(message) => f.write_str(message),
            SpecError::Build(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for SpecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SpecError::Resolve(_) => None,
            SpecError::Build(error) => Some(error),
        }
    }
}

impl From<crate::RdltError> for SpecError {
    fn from(error: crate::RdltError) -> Self {
        SpecError::Build(error)
    }
}

/// Source config files: YAML by default, JSON when the file says so — the same
/// document shape either way (the library's from_yaml/from_json share
/// validation; embedders pass serde_json::Value via from_value).
#[cfg(any(feature = "rest", feature = "file", feature = "postgres-source"))]
fn is_json(path: &std::path::Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"))
}

#[cfg(any(feature = "rest", feature = "file"))]
fn read_config(path: &std::path::Path) -> Result<String, SpecError> {
    std::fs::read_to_string(path)
        .map_err(|e| SpecError::resolve(format!("reading {}: {e}", path.display())))
}

#[cfg(feature = "postgres-source")]
impl Spec {
    /// The resolved postgres SOURCE config when this spec's source is postgres
    /// — reading the referenced file for the `config:` form, or re-validating
    /// the inline document through the shared `from_value` gate (untagged
    /// deserialization bypassed the document validation otherwise). `None` for
    /// other source kinds. The CLI inspects it for CDC-composition advisories;
    /// [`build_pipeline`] resolves it the same way for the source it builds.
    pub fn pg_source_config(&self) -> Option<Result<PostgresConfig, SpecError>> {
        match &self.source {
            SourceSpec::Postgres(pg) => Some(resolve_pg(pg)),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }
}

#[cfg(feature = "postgres-source")]
fn resolve_pg(pg: &PgSourceSpec) -> Result<PostgresConfig, SpecError> {
    match pg {
        PgSourceSpec::File(file) => {
            let path = &file.config;
            let text = std::fs::read_to_string(path)
                .map_err(|e| SpecError::resolve(format!("reading {}: {e}", path.display())))?;
            if is_json(path) {
                PostgresConfig::from_json(&text)
            } else {
                PostgresConfig::from_yaml(&text)
            }
            .map_err(|e| SpecError::resolve(e.to_string()))
        }
        PgSourceSpec::Inline(inline) => {
            let value =
                serde_json::to_value(inline).map_err(|e| SpecError::resolve(e.to_string()))?;
            PostgresConfig::from_value(value).map_err(|e| SpecError::resolve(e.to_string()))
        }
    }
}

/// Turn a parsed [`Spec`] into a runnable [`Pipeline`]. Pure construction: no
/// network or destination I/O beyond reading the referenced source-config
/// files; the typestate builder's `build` re-checks against destination
/// capabilities before any pipeline runs.
// With no source connector compiled in every real arm vanishes, leaving
// `builder` used only by the fallback error arm — inert, not a defect.
#[cfg_attr(
    not(any(feature = "rest", feature = "file", feature = "postgres-source")),
    allow(unused_variables)
)]
pub fn build_pipeline(spec: &Spec) -> Result<Pipeline, SpecError> {
    let builder = Pipeline::builder(spec.pipeline.as_str());
    let builder = match &spec.write_mode {
        None | Some(WriteModeSpec::Append) => builder.write_mode(WriteMode::Append),
        Some(WriteModeSpec::Replace) => builder.write_mode(WriteMode::Replace),
        Some(WriteModeSpec::Merge { key }) => {
            builder.write_mode(WriteMode::Merge { key: key.clone() })
        }
    };
    let builder = match &spec.workdir {
        Some(dir) => builder.workdir(dir),
        None => builder.workdir(".rdlt"),
    };

    match &spec.source {
        #[cfg(feature = "rest")]
        SourceSpec::Rest { config } => {
            let text = read_config(config)?;
            let source = if is_json(config) {
                crate::connector::rest::RestSource::from_json(&text)
            } else {
                crate::connector::rest::RestSource::from_yaml(&text)
            }
            .map_err(|e| SpecError::resolve(e.to_string()))?;
            build_with(builder.source(source), &spec.destination)
        }
        #[cfg(feature = "file")]
        SourceSpec::File { config } => {
            let text = read_config(config)?;
            let source = if is_json(config) {
                crate::connector::file::FileSource::from_json(&text)
            } else {
                crate::connector::file::FileSource::from_yaml(&text)
            }
            .map_err(|e| SpecError::resolve(e.to_string()))?;
            build_with(builder.source(source), &spec.destination)
        }
        #[cfg(feature = "postgres-source")]
        SourceSpec::Postgres(pg) => {
            let config = resolve_pg(pg)?;
            let source = crate::connector::postgres::source::PostgresSource::new(config);
            build_with(builder.source(source), &spec.destination)
        }
        #[allow(unreachable_patterns)]
        _ => Err(SpecError::resolve(
            "source connector not compiled into this build",
        )),
    }
}

/// Fix the source generic, then dispatch the destination and build. Generic
/// over the source so the typestate builder keeps its type through `build`.
// Dead when no source connector calls it; `builder` is untouched when no
// destination connector is compiled in — both are inert degenerate builds.
#[cfg_attr(
    not(any(feature = "rest", feature = "file", feature = "postgres-source")),
    allow(dead_code)
)]
#[cfg_attr(
    not(any(
        feature = "duckdb",
        feature = "postgres-dest",
        feature = "file",
        feature = "iceberg"
    )),
    allow(unused_variables)
)]
fn build_with<S: rdlt_connector::Source>(
    builder: PipelineBuilder<S, Missing>,
    dest: &DestSpec,
) -> Result<Pipeline, SpecError> {
    match dest {
        #[cfg(feature = "duckdb")]
        DestSpec::Duckdb {
            path,
            memory_limit,
            merge_strategy,
            tables,
            extensions,
            settings,
        } => {
            let mut dest = crate::connector::duckdb::dest::DuckDb::open(path)
                .map_err(|e| SpecError::resolve(format!("opening duckdb: {e}")))?;
            for ext in extensions.iter().flatten() {
                dest = dest
                    .extension(ext)
                    .map_err(|e| SpecError::resolve(e.to_string()))?;
            }
            for (key, value) in settings.iter().flatten() {
                dest = dest
                    .setting(key, value)
                    .map_err(|e| SpecError::resolve(e.to_string()))?;
            }
            if let Some(limit) = memory_limit {
                dest = dest
                    .memory_limit(limit)
                    .map_err(|e| SpecError::resolve(format!("duckdb memory_limit: {e}")))?;
            }
            if merge_strategy.is_some() || tables.is_some() {
                let options = crate::connector::duckdb::dest::DestOptions {
                    merge_strategy: *merge_strategy,
                    tables: tables.clone().unwrap_or_default(),
                };
                dest = dest
                    .options(options)
                    .map_err(|e| SpecError::resolve(format!("destination options: {e}")))?;
            }
            Ok(builder.destination(dest).build()?)
        }
        #[cfg(feature = "postgres-dest")]
        DestSpec::Postgres {
            conn,
            dataset,
            tls,
            merge_strategy,
            tables,
        } => {
            let mut dest =
                crate::connector::postgres::dest::Postgres::connect(conn).dataset(dataset);
            if let Some(policy) = tls {
                dest = dest.tls(policy.clone());
            }
            if merge_strategy.is_some() || tables.is_some() {
                let options = crate::connector::postgres::dest::DestOptions {
                    merge_strategy: *merge_strategy,
                    tables: tables.clone().unwrap_or_default(),
                };
                dest = dest
                    .options(options)
                    .map_err(|e| SpecError::resolve(format!("destination options: {e}")))?;
            }
            Ok(builder.destination(dest).build()?)
        }
        #[cfg(feature = "file")]
        DestSpec::Parquet { path } => {
            let dest = crate::connector::file::ParquetDir::open(path)
                .map_err(|e| SpecError::resolve(format!("opening parquet dir: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        #[cfg(feature = "file")]
        DestSpec::File {
            path,
            format,
            location,
            partition_by,
            parquet,
        } => {
            let mut config = crate::connector::file::dest::FileDestConfig::new(path.clone());
            if let Some(format) = format {
                config = config.with_format(*format);
            }
            if let Some(location) = location {
                config = config.with_location(location.clone());
            }
            if let Some(column) = partition_by {
                config = config.with_partition_by(column.clone());
            }
            if let Some(parquet) = parquet {
                config = config.with_parquet(parquet.clone());
            }
            let dest = crate::connector::file::dest::FileDest::from_config(config)
                .map_err(|e| SpecError::resolve(format!("file destination: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        #[cfg(feature = "iceberg")]
        DestSpec::Iceberg(config) => {
            let dest = crate::connector::iceberg::IcebergDest::from_config((**config).clone())
                .map_err(|e| SpecError::resolve(format!("iceberg destination: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        #[allow(unreachable_patterns)]
        _ => Err(SpecError::resolve(
            "destination connector not compiled into this build",
        )),
    }
}
