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
//! Each variant that names a COMPILED-IN connector type is feature-gated to
//! that connector, so the facade still builds with any subset of connectors
//! (down to none): a spec that names a connector this build did not compile
//! in fails to parse (the variant does not exist), never silently. The
//! `connector:` variant is the exception BY DESIGN — it names an
//! out-of-process connector resolved through a [`rdlt_runtime::ConnectorProvider`]
//! at build time, so it is always present regardless of features.

use std::path::PathBuf;

use rdlt_runtime::{ConnectorProvider, ConnectorRequirement, LocalBinaryConnectorProvider};
use serde::Deserialize;

use crate::builder::{Missing, PipelineBuilder};
use crate::{CommitPolicy, Pipeline, WriteMode};

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
    /// How many rows the engine accumulates before each destination
    /// WRITE — and so, for file destinations, how many rows land in
    /// each part.
    ///
    /// Destination-agnostic: `{every_rows: 50000}` means the same to
    /// a file, a table and a warehouse. Absent writes each source
    /// batch straight through, which is what happened before this
    /// existed — so part size followed the SOURCE's paging.
    ///
    /// Distinct from `commit_policy`: this is write granularity
    /// (throughput and memory), that is durability (what a crash
    /// costs). A batch never spans a commit.
    #[serde(default)]
    pub batch_policy: Option<rdlt_core::BatchPolicy>,
    /// When accumulated rows are committed — and so, for file
    /// destinations, how many rows land in each part.
    ///
    /// Thresholds, ANY of which ends the commit unit, whichever is
    /// reached first: `{every_bytes: 104857600, every_seconds: 900}`
    /// is "100 MB or every 15 minutes". Absent defaults to committing
    /// at every source checkpoint, which is the safest cadence
    /// because a crash can then cost at most one checkpoint of work.
    #[serde(default)]
    pub commit_policy: Option<CommitPolicy>,
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
    /// The REST source: a document inline, or `config: <path>`.
    #[cfg(feature = "rest")]
    Rest(ConfigSpec<crate::connector::rest::source::Config>),
    /// The Oracle source (tables with watermark cursors).
    #[cfg(feature = "oracle")]
    Oracle(ConfigSpec<crate::connector::oracle::source::Config>),
    /// The file source (jsonl/csv/parquet streams).
    #[cfg(feature = "file")]
    File(ConfigSpec<crate::connector::file::source::Config>),
    /// The postgres source.
    #[cfg(feature = "postgres-source")]
    Postgres(ConfigSpec<PostgresConfig>),
    /// An out-of-process connector, spawned and handshaken at build:
    /// `connector: {id: io.rapidbyte.file, config: {…}}`. Always present
    /// — this is the variant that needs NO compiled-in feature.
    Connector(ConnectorRef),
}

/// The path form of a postgres source: `postgres: {config: source.yaml}`.
///
/// `deny_unknown_fields`, so mixing `config` with inline fields is a loud error
/// rather than a document half of which is silently ignored.
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
        merge_strategy: Option<crate::connector::duckdb::destination::MergeStrategy>,
        /// Per-table option overrides, keyed by table name.
        tables: Option<
            std::collections::BTreeMap<String, crate::connector::duckdb::destination::TableOptions>,
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
    /// the config and not added here compiled fine and was simply
    /// unreachable from any pipeline document — configurable in the library,
    /// invisible from YAML, with no error anywhere. Embedding removes the
    /// possibility rather than guarding against it. Boxed because the config
    /// dwarfs the other variants.
    #[cfg(feature = "file")]
    File(Box<crate::connector::file::destination::Config>),
    /// The Iceberg destination — the crate's full config vocabulary inline
    /// (catalog/auth, namespace, storage override, per-stream tables with
    /// partition_by).
    #[cfg(feature = "iceberg")]
    Iceberg(Box<crate::connector::iceberg::destination::Config>),
    /// The Snowflake destination — the crate's full config vocabulary inline
    /// (account/auth, database, schema, warehouse, role, table type, session
    /// parameters, and the shared merge options).
    ///
    /// Embedded, like the file and iceberg blocks: a hand-mirrored struct fails
    /// silently in one direction, leaving a field configurable from the library
    /// and invisible from YAML with no error anywhere.
    #[cfg(feature = "snowflake")]
    Snowflake(Box<crate::connector::snowflake::destination::Config>),
    /// An out-of-process connector, spawned and handshaken at build —
    /// the destination twin of [`SourceSpec::Connector`], same shape,
    /// same always-present rule.
    Connector(ConnectorRef),
}

/// An out-of-process connector requirement, the `connector:` document:
///
/// ```yaml
/// source:
///   connector:
///     id: io.rapidbyte.file
///     version: "0.3.0"      # optional, exact-match
///     path: /explicit/bin   # optional override
///     config: { ... }       # the connector's own document, opaque here
/// ```
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorRef {
    /// The connector id, spelled reverse-DNS: `io.rapidbyte.file`. Two
    /// things hang off it: the id's LAST `.`-segment names the binary
    /// discovery looks for (`rdlt-connector-file` on PATH), and the
    /// spawned connector must report EXACTLY this id in its handshake.
    /// A shorthand like `id: file` would therefore discover the same
    /// binary and then be REFUSED as an identity mismatch — the full
    /// reverse-DNS spelling is the id, not a long form of it.
    pub id: String,
    /// Pin the connector's version, exact-match against what its
    /// handshake reports. Absent accepts any.
    #[serde(default)]
    pub version: Option<String>,
    /// Explicit binary path, bypassing PATH discovery entirely.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// The connector's OWN config document, OPAQUE here: it crosses the
    /// wire in the handshake and the CONNECTOR's config gate validates
    /// it — the facade and CLI never learn remote vocabularies, so a
    /// refusal arrives in the connector's own wording.
    pub config: serde_json::Value,
}

/// Manual on purpose (the workspace lint demands SOME Debug): the
/// `config` document is the connector's own vocabulary and routinely
/// carries credentials — a derived Debug would print them into any
/// `{:?}` of a `Spec`, a log line, or a test failure message. The
/// other fields render normally; the config renders as `<elided>`.
impl std::fmt::Debug for ConnectorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorRef")
            .field("id", &self.id)
            .field("version", &self.version)
            .field("path", &self.path)
            .field("config", &"<elided>")
            .finish()
    }
}

impl ConnectorRef {
    /// The provider-facing half of the document — everything except the
    /// config, which travels beside it.
    fn requirement(&self) -> ConnectorRequirement {
        let mut requirement = ConnectorRequirement::new(&self.id);
        if let Some(version) = &self.version {
            requirement = requirement.with_version(version);
        }
        if let Some(path) = &self.path {
            requirement = requirement.with_path(path);
        }
        requirement
    }
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
#[cfg(any(
    feature = "rest",
    feature = "file",
    feature = "postgres-source",
    feature = "oracle"
))]
fn is_json(path: &std::path::Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"))
}

/// A connector's configuration: EITHER a path to a document, OR the
/// document written inline.
///
/// One spelling for every connector, because the alternative — some
/// taking a path and some taking a document — is a difference the
/// reader has to memorise per connector for no benefit.
///
/// Untagged, and the ORDER MATTERS: [`ConfigPath`] denies unknown
/// fields, so it matches ONLY the exact `{config: <path>}` shape and
/// every other map falls through to the inline arm.
#[cfg(any(
    feature = "rest",
    feature = "file",
    feature = "oracle",
    feature = "postgres-source"
))]
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ConfigSpec<T> {
    /// `config: path/to/document.yaml`
    Path(ConfigPath),
    /// The document itself, written where the path would go. TYPED,
    /// not a free-form value: mixing `config:` with inline keys then
    /// fails at PARSE — neither arm matches — instead of parsing as
    /// an inline document with a stray key that only the connector
    /// notices later.
    Inline(Box<T>),
}

#[cfg(any(
    feature = "rest",
    feature = "file",
    feature = "oracle",
    feature = "postgres-source"
))]
/// The path form: `config: path/to/document.yaml`.
///
/// `deny_unknown_fields` is what makes the untagged choice
/// unambiguous AND makes a half-written document loud: `config`
/// mixed with inline keys is an error rather than a document half of
/// which is silently ignored.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigPath {
    /// The document to read. YAML unless the extension is `.json`.
    pub config: PathBuf,
}

#[cfg(any(
    feature = "rest",
    feature = "file",
    feature = "oracle",
    feature = "postgres-source"
))]
impl<T> ConfigSpec<T> {
    /// Resolve to the connector's own validated document.
    ///
    /// BOTH arms go through the connector's `Document` gate, so an
    /// inline document is validated exactly as a file-backed one is —
    /// untagged deserialization alone would have skipped `validate`
    /// — and a failure carries the connector's own frozen wording
    /// rather than a facade paraphrase.
    fn document(&self) -> Result<T, SpecError>
    where
        T: rdlt_connector_sdk::config::Document + Clone,
        T::Error: std::fmt::Display,
    {
        match self {
            ConfigSpec::Path(spelled) => {
                let path = &spelled.config;
                let text = read_config(path)?;
                if is_json(path) {
                    T::from_json(&text)
                } else {
                    T::from_yaml(&text)
                }
                .map_err(|e| SpecError::resolve(e.to_string()))
            }
            // Deserialization alone does NOT validate, so the inline
            // arm is put through the connector's own gate here — the
            // same one the path arm passes through.
            ConfigSpec::Inline(document) => {
                let document = document.as_ref().clone();
                document
                    .validate()
                    .map(|()| document.clone())
                    .map_err(|e| SpecError::resolve(e.to_string()))
            }
        }
    }
}

#[cfg(any(
    feature = "rest",
    feature = "file",
    feature = "oracle",
    feature = "postgres-source"
))]
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
            SourceSpec::Postgres(spec) => Some(spec.document()),
            // Reachable in every build: the `Connector` variant is
            // always present beside any compiled-in sources.
            _ => None,
        }
    }
}

/// The engine byte budget a `connector:` spawn's dial derives its flow-control
/// windows from: the document's own batch-policy byte threshold when it names
/// one, else the engine's channel default — the SAME constant the engine's
/// byte channel uses, so the wire can never hold more in flight than the
/// engine itself would buffer.
fn engine_budget_bytes(spec: &Spec) -> u64 {
    spec.batch_policy
        .and_then(|policy| policy.every_bytes)
        .unwrap_or(rdlt_engine::DEFAULT_BYTE_BUDGET as u64)
}

/// Turn a parsed [`Spec`] into a runnable [`Pipeline`]. Construction only: no
/// destination I/O beyond reading the referenced source-config files and
/// opening compiled-in destinations — EXCEPT for `connector:` requirements,
/// which are resolved through the default
/// [`LocalBinaryConnectorProvider`]: spawn, dial, handshake (where the
/// CONNECTOR validates its own config), wrap. The typestate builder's `build`
/// re-checks against destination capabilities before any pipeline runs.
///
/// Async because of that spawn seam; embedders with their own provider (a
/// pool, a remote scheduler) use [`build_pipeline_with`].
pub async fn build_pipeline(spec: &Spec) -> Result<Pipeline, SpecError> {
    let provider =
        LocalBinaryConnectorProvider::default().with_engine_budget_bytes(engine_budget_bytes(spec));
    build_pipeline_with(spec, &provider).await
}

/// [`build_pipeline`] with the caller's own [`ConnectorProvider`] deciding how
/// `connector:` requirements become processes (or pool members, or anything
/// else) — the engine never learns which.
pub async fn build_pipeline_with(
    spec: &Spec,
    provider: &dyn ConnectorProvider,
) -> Result<Pipeline, SpecError> {
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
    let builder = match &spec.batch_policy {
        Some(policy) => builder.batch_policy(*policy),
        None => builder,
    };
    let builder = match &spec.commit_policy {
        // Refused here rather than honoured: a policy with no
        // threshold never fires, so the run would hold everything
        // uncommitted until it ended.
        Some(policy) => {
            policy.check().map_err(SpecError::resolve)?;
            builder.commit_policy(*policy)
        }
        None => builder,
    };

    match &spec.source {
        #[cfg(feature = "rest")]
        SourceSpec::Rest(spec_config) => {
            let source = crate::connector::rest::source::Shell::new(spec_config.document()?)
                .map_err(|e| SpecError::resolve(e.to_string()))?;
            build_with(builder.source(source), &spec.destination, provider).await
        }
        #[cfg(feature = "oracle")]
        SourceSpec::Oracle(spec_config) => {
            let source = crate::connector::oracle::source::Shell::new(spec_config.document()?)
                .map_err(|e| SpecError::resolve(e.to_string()))?;
            build_with(builder.source(source), &spec.destination, provider).await
        }
        #[cfg(feature = "file")]
        SourceSpec::File(spec_config) => {
            let source = crate::connector::file::source::Shell::new(spec_config.document()?)
                .map_err(|e| SpecError::resolve(e.to_string()))?;
            build_with(builder.source(source), &spec.destination, provider).await
        }
        #[cfg(feature = "postgres-source")]
        SourceSpec::Postgres(spec_config) => {
            let source = crate::connector::postgres::source::Shell::new(spec_config.document()?)
                .map_err(|e| SpecError::resolve(e.to_string()))?;
            build_with(builder.source(source), &spec.destination, provider).await
        }
        SourceSpec::Connector(reference) => {
            // The provider's typed errors render verbatim — the frozen
            // NotFound spelling, the handshake's identity/config
            // refusals — never a facade paraphrase on top.
            let source = provider
                .source(&reference.requirement(), &reference.config)
                .await
                .map_err(|e| SpecError::resolve(e.to_string()))?;
            build_with(builder.source(source), &spec.destination, provider).await
        }
    }
}

/// Fix the source generic, then dispatch the destination and build. Generic
/// over the source so the typestate builder keeps its type through `build`.
async fn build_with<S: rdlt_connector::Source>(
    builder: PipelineBuilder<S, Missing>,
    dest: &DestSpec,
    provider: &dyn ConnectorProvider,
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
            // The spec's fields ARE the connector document's fields;
            // Shell::new runs the one validation gate and opens the
            // database (settings/extensions applied eagerly).
            let mut config = crate::connector::duckdb::destination::Config::new(path);
            config.memory_limit = memory_limit.clone();
            config.merge_strategy = *merge_strategy;
            config.tables = tables.clone();
            config.extensions = extensions.clone();
            config.settings = settings.clone();
            let dest = crate::connector::duckdb::destination::Shell::new(config)
                .map_err(|e| SpecError::resolve(format!("opening duckdb: {e}")))?;
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
            Ok(builder.destination(dest.into_shell()).build()?)
        }
        #[cfg(feature = "file")]
        DestSpec::Parquet { path } => {
            // The canonical local-parquet spelling: the sdk Shell over a
            // plain-path config (Shell::new validates the document).
            let config = crate::connector::file::destination::Config::new(
                path.to_string_lossy().into_owned(),
            );
            let dest = crate::connector::file::destination::Shell::new(config)
                .map_err(|e| SpecError::resolve(format!("opening parquet dir: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        #[cfg(feature = "file")]
        DestSpec::File(config) => {
            let dest = crate::connector::file::destination::Shell::new((**config).clone())
                .map_err(|e| SpecError::resolve(format!("file destination: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        #[cfg(feature = "iceberg")]
        DestSpec::Iceberg(config) => {
            // Shell::new validates the hand-parsed document — the spec
            // enum's serde parse is not the Document gate.
            let dest = crate::connector::iceberg::destination::Shell::new((**config).clone())
                .map_err(|e| SpecError::resolve(format!("iceberg destination: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        #[cfg(feature = "snowflake")]
        DestSpec::Snowflake(config) => {
            // Shell::new validates the hand-parsed document — the spec
            // enum's serde parse is not the Document gate.
            let dest = crate::connector::snowflake::destination::Shell::new((**config).clone())
                .map_err(|e| SpecError::resolve(format!("snowflake destination: {e}")))?;
            Ok(builder.destination(dest).build()?)
        }
        DestSpec::Connector(reference) => {
            // Same verbatim rule as the source arm: the provider's and
            // handshake's typed refusals ARE the message.
            let dest = provider
                .destination(&reference.requirement(), &reference.config)
                .await
                .map_err(|e| SpecError::resolve(e.to_string()))?;
            Ok(builder.destination(dest).build()?)
        }
    }
}
