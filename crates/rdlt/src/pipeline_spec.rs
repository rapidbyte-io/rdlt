//! The pipeline spec: the ONE YAML document model that every consumer
//! of the engine — the `rdlt` CLI, the `rdlt-bench` harness, library
//! embedders — parses, and its construction into a runnable
//! [`Pipeline`].
//!
//! ONE YAML document describes a whole pipeline: pipeline-wide
//! settings, the source, and the destination. EVERY arm names an
//! OUT-OF-PROCESS connector. The rich spellings (`postgres:`, `file:`,
//! `duckdb:`, …) are sugar for the explicit `connector:` form: each
//! desugars to the same [`ConnectorRef`] through the one
//! [`connector_id`] table, and every reference — rich or explicit — is
//! resolved through a [`rdlt_runtime::ConnectorProvider`] at build
//! time into a spawned connector binary.
//!
//! The config document inside any arm is OPAQUE here: it crosses the
//! wire in the handshake and the CONNECTOR's own config gate validates
//! it — the facade and CLI never learn connector vocabularies, so a
//! refusal arrives in the connector's own wording, never a facade
//! paraphrase.

use std::path::{Path, PathBuf};

use rdlt_runtime::{ConnectorProvider, ConnectorRequirement, LocalBinaryConnectorProvider};
use serde::Deserialize;

use crate::{CommitPolicy, Pipeline, WriteMode};

/// One pipeline, end to end.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    /// The pipeline's stable name. State and cursors are persisted under it, so
    /// renaming it starts a fresh pipeline rather than continuing this one.
    pub pipeline: String,
    /// Where the write-ahead log lives. Absent defaults to
    /// `.rdlt/<pipeline>` BESIDE THE DOCUMENT — per pipeline, never
    /// shared: the engine replays and clears whatever WAL its workdir
    /// holds, so two pipelines over one directory would hand each other
    /// their crashed spans (a shape the engine now refuses typed).
    /// A relative spelling here resolves against the document's own
    /// directory, the same rule path-form configs follow.
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
/// Each rich spelling is sugar for the `connector:` form — see
/// [`SourceSpec::desugar`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceSpec {
    /// The REST source — `io.rapidbyte.rest`.
    Rest(ConfigSource),
    /// The Oracle source — `io.rapidbyte.oracle`.
    Oracle(ConfigSource),
    /// The file source — `io.rapidbyte.file`.
    File(ConfigSource),
    /// The postgres source — `io.rapidbyte.postgres`.
    Postgres(ConfigSource),
    /// The explicit form the rich spellings desugar to:
    /// `connector: {id: io.rapidbyte.file, config: {…}}` — the id
    /// spelled in full, with the optional version pin and path
    /// override the sugar cannot express.
    Connector(ConnectorRef),
}

/// Which destination a document selects, and how it is configured.
///
/// Each rich spelling is sugar for the `connector:` form — see
/// [`DestSpec::desugar`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DestSpec {
    /// The DuckDB destination — `io.rapidbyte.duckdb`.
    Duckdb(ConfigSource),
    /// The postgres destination — `io.rapidbyte.postgres`.
    Postgres(ConfigSource),
    /// The file destination — `io.rapidbyte.file`.
    File(ConfigSource),
    /// The Iceberg destination — `io.rapidbyte.iceberg`.
    Iceberg(ConfigSource),
    /// The Snowflake destination — `io.rapidbyte.snowflake`.
    Snowflake(ConfigSource),
    /// The explicit form — the destination twin of
    /// [`SourceSpec::Connector`], same shape, same rules.
    Connector(ConnectorRef),
}

/// A connector config document: an inline mapping, or a string path to
/// a YAML/JSON file read at build time. A string means a path —
/// connector documents are mappings by construction, so the untagged
/// order cannot misread an inline document.
#[derive(Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum ConfigSource {
    /// `postgres: path/to/document.yaml` — the document lives in its
    /// own file, read at resolve time (YAML unless the extension says
    /// `.json`), relative paths joined onto the caller's base.
    Path(String),
    /// The document itself, written where the path would go. Opaque:
    /// only the connector knows its vocabulary, and only the
    /// connector's own config gate validates it.
    Inline(serde_json::Value),
}

/// Manual on purpose, like [`ConnectorRef`]'s: an inline document is
/// the connector's own vocabulary and routinely carries credentials,
/// so a derived Debug would print them into any `{:?}` of a [`Spec`],
/// a log line, or a test failure message. The path form renders (a
/// path is an address, not a secret); the inline form is elided.
impl std::fmt::Debug for ConfigSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigSource::Path(path) => f.debug_tuple("Path").field(path).finish(),
            ConfigSource::Inline(_) => f.debug_tuple("Inline").field(&"<elided>").finish(),
        }
    }
}

impl ConfigSource {
    /// Resolve to the connector's opaque config document.
    ///
    /// The path form reads its file — relative paths against `base` —
    /// and parses it as YAML, or as JSON when the extension says so;
    /// absence and a malformed document each refuse with a typed
    /// [`SpecError::Resolve`] naming the path. The inline form clones.
    pub fn resolve(&self, base: &Path) -> Result<serde_json::Value, SpecError> {
        match self {
            ConfigSource::Path(spelled) => {
                let path = base.join(spelled);
                let text = std::fs::read_to_string(&path)
                    .map_err(|e| SpecError::resolve(format!("reading {}: {e}", path.display())))?;
                if is_json(&path) {
                    serde_json::from_str(&text)
                        .map_err(|e| SpecError::resolve(format!("parsing {}: {e}", path.display())))
                } else {
                    serde_yaml::from_str(&text)
                        .map_err(|e| SpecError::resolve(format!("parsing {}: {e}", path.display())))
                }
            }
            ConfigSource::Inline(value) => Ok(value.clone()),
        }
    }
}

/// Config files: YAML by default, JSON when the file says so — the same
/// document shape either way, exactly as the connectors' own
/// `from_yaml`/`from_json` treat it.
fn is_json(path: &Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"))
}

/// The ONE desugar table: rich spelling ↔ reverse-DNS connector id.
/// The spec arms resolve through it, and `rdlt schema`'s short names
/// map through [`connector_id`] over the same rows — so the document
/// language and the CLI cannot drift apart.
const DESUGAR_TABLE: &[(&str, &str)] = &[
    ("rest", "io.rapidbyte.rest"),
    ("oracle", "io.rapidbyte.oracle"),
    ("file", "io.rapidbyte.file"),
    ("postgres", "io.rapidbyte.postgres"),
    ("duckdb", "io.rapidbyte.duckdb"),
    ("iceberg", "io.rapidbyte.iceberg"),
    ("snowflake", "io.rapidbyte.snowflake"),
];

/// The reverse-DNS connector id a rich spelling resolves to — `None`
/// for anything outside the table (such a value is already an id or a
/// binary path, not a short name).
pub fn connector_id(spelling: &str) -> Option<&'static str> {
    DESUGAR_TABLE
        .iter()
        .find(|(short, _)| *short == spelling)
        .map(|(_, id)| *id)
}

impl SourceSpec {
    /// Resolve this arm to the connector requirement it names.
    ///
    /// A rich spelling maps through the desugar table — the table's
    /// id, no version pin, no path override — with its config
    /// resolved ([`ConfigSource::resolve`], path form read relative to
    /// `base`). The `connector:` arm keeps its id, version pin and
    /// path override, and resolves its config by the same rule.
    pub fn desugar(&self, base: &Path) -> Result<ConnectorRef, SpecError> {
        let (spelling, config) = match self {
            SourceSpec::Rest(config) => ("rest", config),
            SourceSpec::Oracle(config) => ("oracle", config),
            SourceSpec::File(config) => ("file", config),
            SourceSpec::Postgres(config) => ("postgres", config),
            SourceSpec::Connector(reference) => return reference.resolved(base),
        };
        ConnectorRef::desugared(spelling, config, base)
    }
}

impl DestSpec {
    /// The destination twin of [`SourceSpec::desugar`] — same table,
    /// same resolution rules.
    pub fn desugar(&self, base: &Path) -> Result<ConnectorRef, SpecError> {
        let (spelling, config) = match self {
            DestSpec::Duckdb(config) => ("duckdb", config),
            DestSpec::Postgres(config) => ("postgres", config),
            DestSpec::File(config) => ("file", config),
            DestSpec::Iceberg(config) => ("iceberg", config),
            DestSpec::Snowflake(config) => ("snowflake", config),
            DestSpec::Connector(reference) => return reference.resolved(base),
        };
        ConnectorRef::desugared(spelling, config, base)
    }
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
///
/// `config` also takes the path form (`config: source.yaml`) — the
/// same [`ConfigSource`] rule as every rich spelling's value.
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
    /// The connector's OWN config document — inline, or a path to one.
    /// Opaque here: it crosses the wire in the handshake and the
    /// CONNECTOR's config gate validates it.
    pub config: ConfigSource,
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
    /// A rich spelling's desugared form: the table's id, no version
    /// pin, no path override, the config resolved inline.
    fn desugared(spelling: &str, config: &ConfigSource, base: &Path) -> Result<Self, SpecError> {
        let id = connector_id(spelling).expect("every rich spec arm has a desugar-table row");
        Ok(ConnectorRef {
            id: id.to_owned(),
            version: None,
            path: None,
            config: ConfigSource::Inline(config.resolve(base)?),
        })
    }

    /// This requirement with its config resolved inline — the
    /// `connector:` arm's half of the desugar.
    fn resolved(&self, base: &Path) -> Result<Self, SpecError> {
        Ok(ConnectorRef {
            id: self.id.clone(),
            version: self.version.clone(),
            path: self.path.clone(),
            config: ConfigSource::Inline(self.config.resolve(base)?),
        })
    }

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
    /// file, a connector binary that could not be found or spawned, a
    /// config the connector's own gate refused.
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

/// The engine byte budget a spawned connector's dial derives its flow-control
/// windows from: the document's own batch-policy byte threshold when it names
/// one, else the engine's channel default — the SAME constant the engine's
/// byte channel uses, so the wire can never hold more in flight than the
/// engine itself would buffer.
fn engine_budget_bytes(spec: &Spec) -> u64 {
    spec.batch_policy
        .and_then(|policy| policy.every_bytes)
        .unwrap_or(rdlt_engine::DEFAULT_BYTE_BUDGET as u64)
}

/// The workdir a document runs under: the spelled path — relative
/// spellings resolved against `base`, the document's own directory,
/// exactly like path-form configs — or, when the document names none,
/// `.rdlt/<pipeline>` under `base`.
///
/// Per pipeline, deliberately: the engine replays and clears whatever
/// WAL its workdir holds, and it refuses a directory occupied by
/// another pipeline — the per-pipeline default keeps that refusal from
/// ever firing on defaulted documents. Resolving against `base` (not
/// the invoking shell's working directory) means one document owns ONE
/// workdir and one lock from wherever it is run; a spec built from a
/// string gets whatever base its embedder passes, so a bare `Path::new("")`
/// base stays working-directory-relative. The pipeline name is
/// path-sanitized through the same normalization destination
/// identifiers use, so a name carrying separators or `..` cannot
/// escape `.rdlt/`.
fn resolved_workdir(spec: &Spec, base: &Path) -> PathBuf {
    match &spec.workdir {
        Some(dir) => base.join(dir),
        None => base.join(".rdlt").join(rdlt_core::naming::normalize_ident(
            &spec.pipeline,
            rdlt_core::naming::IdentRules::default(),
        )),
    }
}

/// Turn a parsed [`Spec`] into a runnable [`Pipeline`]. Construction only —
/// no data moves — but BOTH arms desugar to connector requirements and are
/// resolved through the default [`LocalBinaryConnectorProvider`]: spawn,
/// dial, handshake (where the CONNECTOR validates its own config), wrap.
/// Reading a path-form config file is the one other I/O. The typestate
/// builder's `build` re-checks against destination capabilities before any
/// pipeline runs.
///
/// `base` anchors relative path-form configs — the include rule: a
/// `postgres: ./creds.yaml` resolves beside the document that names it,
/// so the CLI passes the pipeline file's own directory and a spec built
/// from a string names whatever directory its author means. Data paths
/// INSIDE a config document are a different story: the connector
/// process resolves those against its working directory. Async because
/// of the spawn seam; embedders with their own provider (a pool, a
/// remote scheduler) use [`build_pipeline_with`].
pub async fn build_pipeline(spec: &Spec, base: &Path) -> Result<Pipeline, SpecError> {
    let provider =
        LocalBinaryConnectorProvider::default().with_engine_budget_bytes(engine_budget_bytes(spec));
    build_pipeline_with(spec, base, &provider).await
}

/// [`build_pipeline`] with the caller's own [`ConnectorProvider`] deciding how
/// connector requirements become processes (or pool members, or anything
/// else) — the engine never learns which.
pub async fn build_pipeline_with(
    spec: &Spec,
    base: &Path,
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
    let builder = builder.workdir(resolved_workdir(spec, base));
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

    // The provider's typed errors render verbatim — the frozen
    // NotFound spelling, the handshake's identity/config refusals —
    // never a facade paraphrase on top.
    let source_ref = spec.source.desugar(base)?;
    let source_config = source_ref.config.resolve(base)?;
    let source = provider
        .source(&source_ref.requirement(), &source_config)
        .await
        .map_err(|e| SpecError::resolve(e.to_string()))?;
    let dest_ref = spec.destination.desugar(base)?;
    let dest_config = dest_ref.config.resolve(base)?;
    let dest = provider
        .destination(&dest_ref.requirement(), &dest_config)
        .await
        .map_err(|e| SpecError::resolve(e.to_string()))?;
    Ok(builder.source(source).destination(dest).build()?)
}

#[cfg(test)]
mod workdir_tests {
    use super::*;

    fn spec_from(yaml: &str) -> Spec {
        serde_yaml::from_str(yaml).expect("the fixture document parses")
    }

    const BODY: &str = "source:\n  postgres: s.yaml\ndestination:\n  duckdb: {path: out.db}\n";

    /// The default workdir is PER PIPELINE and lives beside the document:
    /// two defaulted documents in one directory get two workdirs, so
    /// neither can ever scan (or clear) the other's WAL.
    #[test]
    fn the_default_workdir_is_per_pipeline_under_the_spec_dir() {
        let orders = spec_from(&format!("pipeline: orders\n{BODY}"));
        let customers = spec_from(&format!("pipeline: customers\n{BODY}"));
        let base = Path::new("/srv/specs");
        assert_eq!(
            resolved_workdir(&orders, base),
            Path::new("/srv/specs/.rdlt/orders")
        );
        assert_eq!(
            resolved_workdir(&customers, base),
            Path::new("/srv/specs/.rdlt/customers")
        );
    }

    /// A pipeline name is a free-form string; the workdir leaf derived
    /// from it is normalized, so separators and dot-segments cannot
    /// escape `.rdlt/`.
    #[test]
    fn the_default_workdir_leaf_is_path_sanitized() {
        let spec = spec_from(&format!("pipeline: \"../Orders / prod\"\n{BODY}"));
        let dir = resolved_workdir(&spec, Path::new("/srv/specs"));
        let leaf = dir.file_name().expect("a leaf exists").to_string_lossy();
        assert_eq!(dir.parent(), Some(Path::new("/srv/specs/.rdlt")));
        assert!(
            !leaf.contains('/') && !leaf.contains(".."),
            "the leaf must not traverse: {leaf}"
        );
    }

    /// An explicit relative `workdir:` resolves against the document's
    /// directory — the include rule config paths already follow — so one
    /// document owns ONE workdir and one lock from any invoking CWD. An
    /// absolute spelling is taken as given.
    #[test]
    fn an_explicit_workdir_resolves_against_the_spec_dir() {
        let relative = spec_from(&format!("pipeline: p\nworkdir: state/wd\n{BODY}"));
        assert_eq!(
            resolved_workdir(&relative, Path::new("/srv/specs")),
            Path::new("/srv/specs/state/wd")
        );
        let absolute = spec_from(&format!("pipeline: p\nworkdir: /var/lib/rdlt\n{BODY}"));
        assert_eq!(
            resolved_workdir(&absolute, Path::new("/srv/specs")),
            Path::new("/var/lib/rdlt")
        );
    }

    /// An empty base — a spec built from a string with `Path::new("")` —
    /// stays working-directory-relative, exactly as documented for
    /// embedders.
    #[test]
    fn an_empty_base_keeps_the_default_cwd_relative() {
        let spec = spec_from(&format!("pipeline: p\n{BODY}"));
        assert_eq!(resolved_workdir(&spec, Path::new("")), Path::new(".rdlt/p"));
    }
}
