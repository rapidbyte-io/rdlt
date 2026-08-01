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
use crate::connector::postgres::source::Config as PostgresConfig;

/// One pipeline, end to end.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    /// The pipeline's stable name. State and cursors are persisted under it, so
    /// renaming it starts a fresh pipeline rather than continuing this one.
    pub pipeline: String,
    /// Where the write-ahead log lives. Absent means no WAL: recovery still
    /// works, but degrades to re-extracting from the last committed cursor —
    /// slower, never wrong.
    #[serde(default)]
    pub workdir: Option<PathBuf>,
    // singleton_map: YAML's natural `write_mode: {merge: {key: […]}}` /
    // `source: postgres: …` singleton-map form for externally-tagged
    // enums (serde_yaml 0.9 otherwise wants `!tag` syntax).
    /// How rows land at the destination. Absent defaults to `Append`.
    #[serde(default, with = "serde_yaml::with::singleton_map")]
    pub write_mode: Option<WriteModeSpec>,
    /// Where rows come from.
    #[serde(with = "serde_yaml::with::singleton_map")]
    pub source: SourceSpec,
    /// Where rows go.
    #[serde(with = "serde_yaml::with::singleton_map")]
    pub destination: DestSpec,
}

/// The document form of [`WriteMode`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteModeSpec {
    /// Add rows, keeping everything already there.
    Append,
    /// Replace the table's contents with this load's rows, atomically at commit.
    Replace,
    /// Merge on an identity, updating matched rows and inserting the rest.
    Merge {
        /// The columns identifying a row. Must agree with the stream's declared
        /// `primary_key` where it has one — a mismatch is refused at plan time.
        key: Vec<String>,
    },
}

/// Which source a document selects, and how it is configured.
///
/// Each variant is gated on its connector feature, so a build that excludes a
/// connector also rejects documents naming it — rather than compiling and
/// failing at run time.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceSpec {
    /// Path to the declarative REST source YAML.
    #[cfg(feature = "rest")]
    Rest {
        /// The REST source document.
        config: PathBuf,
    },
    /// Path to the file source YAML (jsonl/parquet streams).
    #[cfg(feature = "file")]
    File {
        /// The file source document.
        config: PathBuf,
    },
    /// Postgres source: the config document INLINE (the natural form — the
    /// pipeline is one YAML document), or `config: path` referencing a
    /// reusable YAML/JSON file with the identical shape.
    #[cfg(feature = "postgres-source")]
    Postgres(PgSourceSpec),
}

#[cfg(feature = "postgres-source")]
#[derive(Debug, Deserialize)]
/// The two ways a postgres source can be written in a pipeline document.
///
/// `untagged`, so the form is inferred from the shape rather than declared.
#[serde(untagged)]
pub enum PgSourceSpec {
    /// `source: postgres: {config: source.yaml}` — tried first; strict
    /// (`deny_unknown_fields`), so `config` mixed with inline fields is a
    /// loud error, never a silently-ignored document.
    File(PgSourceFile),
    /// The full source document inline (boxed — it dwarfs the path form).
    Inline(Box<PostgresConfig>),
}

/// The path form of a postgres source: `postgres: {config: source.yaml}`.
///
/// `deny_unknown_fields`, so mixing `config` with inline fields is a loud error
/// rather than a document half of which is silently ignored.
#[cfg(feature = "postgres-source")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PgSourceFile {
    /// The reusable postgres source document.
    pub config: PathBuf,
}

/// Which destination a document selects, and how it is configured.
///
/// Each variant is gated on its connector feature, so a build that excludes a
/// connector also rejects documents naming it.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DestSpec {
    /// A DuckDB database file.
    #[cfg(feature = "duckdb")]
    Duckdb {
        /// The database file. Created if absent.
        path: PathBuf,
        /// DuckDB's own `memory_limit` setting (`"4GB"`), passed through.
        memory_limit: Option<String>,
        /// The SAME destination-options vocabulary as postgres — shared
        /// sqlcore types, one YAML shape.
        merge_strategy: Option<crate::connector::duckdb::dest::MergeStrategy>,
        /// Per-table option overrides, keyed by table name.
        tables: Option<
            std::collections::BTreeMap<String, crate::connector::duckdb::dest::TableOptions>,
        >,
        /// dlt-parity passthrough: extensions to LOAD and `SET` settings.
        extensions: Option<Vec<String>>,
        /// Raw `SET <key> = <value>` settings applied to the connection.
        settings: Option<std::collections::BTreeMap<String, String>>,
    },
    /// A PostgreSQL schema.
    #[cfg(feature = "postgres-dest")]
    Postgres {
        /// libpq connection string.
        conn: String,
        /// The schema rows land in. Created if absent.
        dataset: String,
        /// Optional TLS block: `tls: {mode: verify_full, root_cert: /ca.pem}`.
        tls: Option<crate::connector::postgres::tls::Policy>,
        /// Destination-wide merge strategy
        /// ("delete_insert" | "upsert" | "scd2").
        merge_strategy: Option<crate::connector::postgres::destination::MergeStrategy>,
        /// Per-table options — `tables: <name>: {…}` with
        /// `merge_strategy`, `hard_delete`, `dedup_sort`, `merge_scope`, and
        /// `scd2: {valid_from, valid_to, absent}`.
        tables: Option<
            std::collections::BTreeMap<
                String,
                crate::connector::postgres::destination::TableOptions,
            >,
        >,
    },
    /// The frozen `parquet:` spelling (equivalent to `file: local parquet`);
    /// the parquet destination lives in the file family.
    #[cfg(feature = "file")]
    Parquet {
        /// Output directory.
        path: PathBuf,
    },
    /// The full file-destination vocabulary — format (parquet|jsonl),
    /// location (local | s3), partition_by, parquet options.
    ///
    /// The connector's own config type IS the document shape, embedded rather
    /// than mirrored. Previously this was a struct variant restating each field
    /// by hand, which failed silently in one direction: a field added to
    /// `FileDestConfig` and not added here compiled fine and was simply
    /// unreachable from any pipeline document — configurable in the library,
    /// invisible from YAML, with no error anywhere. Embedding removes the
    /// possibility rather than guarding against it. Boxed because the config
    /// dwarfs the other variants.
    #[cfg(feature = "file")]
    File(Box<crate::connector::file::dest::FileDestConfig>),
    /// The Iceberg destination — the crate's full config vocabulary inline
    /// (catalog/auth, namespace, storage override, per-stream tables with
    /// partition_by).
    #[cfg(feature = "iceberg")]
    Iceberg(Box<crate::connector::iceberg::IcebergConfig>),
    /// The Snowflake destination — the crate's full config vocabulary inline
    /// (account/auth, database, schema, warehouse, role, table type, session
    /// parameters, the optional staging bucket, and the shared merge options).
    ///
    /// Embedded, like the file and iceberg blocks: a hand-mirrored struct fails
    /// silently in one direction, leaving a field configurable from the library
    /// and invisible from YAML with no error anywhere.
    #[cfg(feature = "snowflake")]
    Snowflake(Box<crate::connector::snowflake::dest::SnowflakeConfig>),
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
                crate::connector::rest::source::Rest::from_json(&text)
            } else {
                crate::connector::rest::source::Rest::from_yaml(&text)
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
            let source = crate::connector::postgres::source::Postgres::new(config);
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
                let options = crate::connector::duckdb::dest::DestinationOptions {
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
                crate::connector::postgres::destination::Postgres::new(conn).schema(dataset);
            if let Some(policy) = tls {
                dest = dest.tls(policy.clone());
            }
            if merge_strategy.is_some() || tables.is_some() {
                let options = crate::connector::postgres::destination::DestinationOptions {
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
        DestSpec::File(config) => {
            let dest = crate::connector::file::dest::FileDest::from_config((**config).clone())
                .map_err(|e| SpecError::resolve(format!("file destination: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        #[cfg(feature = "iceberg")]
        DestSpec::Iceberg(config) => {
            let dest = crate::connector::iceberg::IcebergDest::from_config((**config).clone())
                .map_err(|e| SpecError::resolve(format!("iceberg destination: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        #[cfg(feature = "snowflake")]
        DestSpec::Snowflake(config) => {
            let dest = crate::connector::snowflake::dest::Snowflake::new((**config).clone())
                .map_err(|e| SpecError::resolve(format!("snowflake destination: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        #[allow(unreachable_patterns)]
        _ => Err(SpecError::resolve(
            "destination connector not compiled into this build",
        )),
    }
}
