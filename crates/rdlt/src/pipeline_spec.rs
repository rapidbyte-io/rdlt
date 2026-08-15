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

use std::{
    io::Read as _,
    path::{Path, PathBuf},
};

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
    ///
    /// The memory it governs is the ENGINE's accumulation. A spawned
    /// connector's in-flight frames are byte-bounded by the connector
    /// sdk's serve budget, and the connector's own batch knobs size
    /// its frames — this knob neither bounds nor needs to bound a
    /// connector process.
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

/// The most a config or spec document may weigh before the read is
/// refused: hand-written YAML/JSON measures in kilobytes, so a
/// multi-megabyte "document" is a wrong path (a data file, a dump) —
/// better refused by size, typed, than slurped whole into memory and
/// fed to a recursive parser. Shared with the CLI's spec read AND with
/// the connector sdk's own `Document::from_yaml` gate, so every
/// document read answers to one number.
pub const MAX_DOCUMENT_BYTES: u64 = rdlt_connector_sdk::yaml::MAX_DOCUMENT_BYTES;

fn parse_yaml<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, String> {
    // The raw-text graph gate runs BEFORE deserialization: `serde_yaml`
    // expands anchors and aliases while constructing the target value,
    // so an input-byte cap cannot bound the resulting allocation.
    // Pipeline and connector config documents are trees; graph syntax
    // is refused. The gate lives in the sdk (`rdlt_connector_sdk::yaml`)
    // so this parse and every connector's `Document::from_yaml` answer
    // to the same scanner.
    rdlt_connector_sdk::yaml::reject_graph_syntax(text)?;
    serde_yaml::from_str(text).map_err(|error| error.to_string())
}

/// Parse one pipeline document through the raw-text security gates.
pub fn parse_spec(text: &str) -> Result<Spec, String> {
    parse_yaml(text)
}

/// Read a document file with the [`MAX_DOCUMENT_BYTES`] cap enforced
/// BEFORE the read — the refusal names the size and the cap.
pub fn read_document(path: &Path) -> Result<String, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    let mut text = String::new();
    file.take(MAX_DOCUMENT_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
    let len = text.len() as u64;
    if len > MAX_DOCUMENT_BYTES {
        return Err(format!(
            "reading {}: the file is {len} bytes, over the {MAX_DOCUMENT_BYTES}-byte \
             document cap — a pipeline or connector document is hand-written \
             configuration, so a file this size is almost certainly not the document \
             the path meant to name",
            path.display()
        ));
    }
    Ok(text)
}

impl ConfigSource {
    /// Resolve to the connector's opaque config document.
    ///
    /// The path form reads its file — relative paths against `base` —
    /// and parses it as YAML, or as JSON when the extension says so;
    /// absence, a malformed document, and a file over
    /// [`MAX_DOCUMENT_BYTES`] each refuse with a typed
    /// [`SpecError::Resolve`] naming the path. The inline form clones.
    pub fn resolve(&self, base: &Path) -> Result<serde_json::Value, SpecError> {
        match self {
            ConfigSource::Path(spelled) => {
                let path = base.join(spelled);
                let text = read_document(&path).map_err(SpecError::resolve)?;
                if is_json(&path) {
                    serde_json::from_str(&text)
                        .map_err(|e| SpecError::resolve(format!("parsing {}: {e}", path.display())))
                } else {
                    parse_yaml(&text)
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
/// windows from: the engine channel's OWN constant — the same number the
/// engine's byte channel is actually built with, so the wire can never hold
/// more in flight than the engine would buffer. Deliberately NOT the
/// document's `batch_policy.every_bytes` (the pre-045 reading): that knob
/// paces DESTINATION writes, and the facade never resizes the engine channel
/// from it, so deriving the wire window from it silently throttled the
/// connector wire to a write-cadence number while the engine kept buffering
/// at its default. Takes the spec so the pin below can state the
/// independence; a future spec-level budget knob would thread here AND
/// through the builder's `byte_budget`, keeping the two sides one number.
fn engine_budget_bytes(_spec: &Spec) -> u64 {
    rdlt_engine::DEFAULT_BYTE_BUDGET as u64
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
        parse_spec(yaml).expect("the fixture document parses")
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

#[cfg(test)]
mod budget_tests {
    use super::*;

    /// The dial budget is the ENGINE CHANNEL's, never the batch policy's:
    /// `every_bytes` is destination-write cadence, and a document tuning
    /// its dest-flush size must not silently shrink the connector wire's
    /// h2 windows.
    #[test]
    fn the_dial_budget_ignores_batch_policy() {
        let spec: Spec = serde_yaml::from_str(
            "pipeline: p\nbatch_policy: {every_bytes: 1048576}\n\
             source:\n  postgres: s.yaml\ndestination:\n  duckdb: {path: out.db}\n",
        )
        .expect("the fixture document parses");
        assert_eq!(
            engine_budget_bytes(&spec),
            rdlt_engine::DEFAULT_BYTE_BUDGET as u64,
            "a 1 MiB dest-write threshold must leave the wire budget at the engine default"
        );
    }
}

#[cfg(test)]
mod document_cap_tests {
    use super::*;

    /// A config path naming a file over the document cap refuses typed
    /// through a bounded `MAX+1` read (045 external findings, KIMI 5): a
    /// concurrent grow cannot race a prior metadata check and feed extra bytes
    /// to the parser. The sparse fixture proves the read itself owns the cap.
    #[test]
    fn an_oversized_config_file_refuses_before_the_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("big.yaml");
        std::fs::File::create(&path)
            .expect("fixture file")
            .set_len(MAX_DOCUMENT_BYTES + 1)
            .expect("sparse size");

        let refused = ConfigSource::Path("big.yaml".into())
            .resolve(dir.path())
            .expect_err("an oversized document must refuse");
        assert_eq!(
            refused.to_string(),
            format!(
                "reading {}: the file is {} bytes, over the {MAX_DOCUMENT_BYTES}-byte \
                 document cap — a pipeline or connector document is hand-written \
                 configuration, so a file this size is almost certainly not the document \
                 the path meant to name",
                path.display(),
                MAX_DOCUMENT_BYTES + 1
            )
        );

        // At the cap or under it, the ordinary read proceeds.
        let small = dir.path().join("small.yaml");
        std::fs::write(&small, "key: value\n").expect("write");
        assert!(
            ConfigSource::Path("small.yaml".into())
                .resolve(dir.path())
                .is_ok()
        );
    }
}

#[cfg(test)]
mod yaml_graph_tests {
    use super::*;

    #[test]
    fn pipeline_anchors_and_aliases_refuse_before_deserialization() {
        let yaml = "pipeline: p\n\
                    source:\n  connector:\n    id: io.example.source\n    config: &shared {token: secret}\n\
                    destination:\n  connector:\n    id: io.example.destination\n    config: *shared\n";
        let error = parse_spec(yaml).expect_err("YAML graph syntax must refuse");
        assert!(error.contains("anchors and aliases"), "{error}");
    }

    #[test]
    fn mid_scalar_apostrophe_does_not_blind_the_graph_guard() {
        // A plain scalar may contain an apostrophe (`john's`) — a quote
        // character is a YAML indicator only at the start of a scalar.
        // A guard that misreads it as quote-open goes blind to every
        // anchor and alias after it, restoring unbounded alias
        // expansion. The graph refusal must still fire.
        let yaml = "pipeline: john's orders\n\
                    big: &big [a, b, c]\n\
                    boom: [*big, *big]\n";
        let error = parse_yaml::<serde_json::Value>(yaml)
            .expect_err("anchors after a mid-scalar apostrophe must still refuse");
        assert!(error.contains("anchors and aliases"), "{error}");
    }

    #[test]
    fn quoted_indicators_and_comments_remain_plain_data() {
        let value: serde_json::Value = parse_yaml(
            "literal_alias: '*not-an-alias'\nliteral_anchor: \"&not-an-anchor\"\n# *comment\n",
        )
        .expect("indicators in scalars and comments are data");
        assert_eq!(value["literal_alias"], "*not-an-alias");
        assert_eq!(value["literal_anchor"], "&not-an-anchor");
    }

    /// The spellings the character-scanner generation of the guard
    /// over-rejected: indicator characters as scalar DATA. These are
    /// availability pins on the primary config surface — a graph guard
    /// that refuses `val#ue` is a parser bug wearing a security badge.
    #[test]
    fn indicator_characters_as_data_parse_on_the_config_surface() {
        let value: serde_json::Value = parse_yaml(
            "hash: val#ue\n\
             amp: a&b\n\
             star: a*b\n\
             apostrophe: don't\n\
             block: |\n  this & that\n  * bullet point\n\
             folded: >\n  more & data\n",
        )
        .expect("indicator characters as data must parse");
        assert_eq!(value["hash"], "val#ue");
        assert_eq!(value["amp"], "a&b");
        assert_eq!(value["star"], "a*b");
        assert_eq!(value["apostrophe"], "don't");
        assert_eq!(value["block"], "this & that\n* bullet point\n");
        assert_eq!(value["folded"], "more & data\n");
    }

    /// Hostile positions beyond the value slot: the refusal fires
    /// wherever the parser would see a live anchor or alias.
    #[test]
    fn anchors_and_aliases_refuse_in_sequence_flow_and_merge_positions() {
        for doc in [
            "items:\n  - &x v\n",
            "items: [a, *x]\n",
            "map: {k: &x v}\n",
            "base:\n  <<: *defaults\n",
            "--- &doc\nkey: v\n",
        ] {
            let error = parse_yaml::<serde_json::Value>(doc)
                .expect_err("graph syntax must refuse in every position");
            assert!(error.contains("anchors and aliases"), "{doc:?}: {error}");
        }
    }
}
