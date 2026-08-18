//! Construction: a parsed [`Document`] into a runnable [`Pipeline`]
//! over a runtime provider, and the typed refusal it can end in.

use std::path::{Path, PathBuf};

use rdlt_runtime::local::Local;
use rdlt_runtime::provider::Provider;

use super::model::{self, Document};
use crate::commit;
use crate::error;
use crate::pipeline::Pipeline;

/// A document that could not be turned into a pipeline. Two shapes so
/// consumers keep their exit-code taxonomy: [`Error::Resolve`] is a
/// config/parse/IO problem (the CLI's exit code 2), while
/// [`Error::Build`] carries the engine's own typed error from the
/// typestate builder.
#[derive(Debug)]
pub enum Error {
    /// Resolving the document into connectors failed — a missing/invalid
    /// config file, a connector binary that could not be found or
    /// spawned, a config the connector's own gate refused.
    Resolve(String),
    /// The typestate builder rejected the configuration (e.g. a destination
    /// that cannot Merge).
    Build(error::Error),
}

impl Error {
    pub(super) fn resolve(message: impl Into<String>) -> Self {
        Error::Resolve(message.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Resolve(message) => f.write_str(message),
            Error::Build(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Resolve(_) => None,
            Error::Build(error) => Some(error),
        }
    }
}

impl From<error::Error> for Error {
    fn from(error: error::Error) -> Self {
        Error::Build(error)
    }
}

/// The engine byte budget a spawned connector's dial derives its
/// flow-control windows from: the engine channel's OWN constant — the
/// same number the engine's byte channel is actually built with, so the
/// wire can never hold more in flight than the engine would buffer.
/// Deliberately NOT the document's `batch_policy.every_bytes`: that knob
/// paces DESTINATION writes, and the facade never resizes the engine
/// channel from it, so deriving the wire window from it would silently
/// throttle the connector wire to a write-cadence number while the
/// engine kept buffering at its default. Takes the document so the pin
/// below can state the independence; a future document-level budget
/// knob would thread here AND through the builder's `byte_budget`,
/// keeping the two sides one number.
fn engine_budget_bytes(_document: &Document) -> u64 {
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
/// workdir and one lock from wherever it is run; a document built from a
/// string gets whatever base its embedder passes, so a bare `Path::new("")`
/// base stays working-directory-relative. The pipeline name is
/// path-sanitized through the same normalization destination
/// identifiers use, so a name carrying separators or `..` cannot
/// escape `.rdlt/`.
fn resolved_workdir(document: &Document, base: &Path) -> PathBuf {
    match &document.workdir {
        Some(dir) => base.join(dir),
        None => base
            .join(".rdlt")
            .join(rdlt_engine::naming::normalize_ident(
                &document.pipeline,
                rdlt_core::schema::IdentRules::default(),
            )),
    }
}

/// Turn a parsed [`Document`] into a runnable [`Pipeline`]. Construction
/// only — no data moves — but BOTH arms' connector requirements are
/// resolved through the default [`Local`] provider: spawn, dial,
/// handshake (where the CONNECTOR validates its own config), wrap.
/// Reading a path-form config file is the one other I/O. The typestate
/// builder's `build` re-checks against destination capabilities before
/// any pipeline runs.
///
/// `base` anchors relative path-form configs — the include rule: a
/// `postgres: ./creds.yaml` resolves beside the document that names it,
/// so the CLI passes the pipeline file's own directory and a document
/// built from a string names whatever directory its author means. Data
/// paths INSIDE a config document are a different story: the connector
/// process resolves those against its working directory. Async because
/// of the spawn seam; embedders with their own provider (a pool, a
/// remote scheduler) use [`build_with`].
pub async fn build(document: &Document, base: &Path) -> Result<Pipeline, Error> {
    let provider = Local::default().with_budget_bytes(engine_budget_bytes(document));
    build_with(document, base, &provider).await
}

/// [`build`] with the caller's own [`Provider`] deciding how connector
/// requirements become processes (or pool members, or anything else) —
/// the engine never learns which.
pub async fn build_with(
    document: &Document,
    base: &Path,
    provider: &dyn Provider,
) -> Result<Pipeline, Error> {
    let builder = Pipeline::builder(document.pipeline.as_str());
    let builder = match &document.write_mode {
        None | Some(model::WriteMode::Append) => builder.write_mode(commit::WriteMode::Append),
        Some(model::WriteMode::Replace) => builder.write_mode(commit::WriteMode::Replace),
        Some(model::WriteMode::Merge { key }) => {
            builder.write_mode(commit::WriteMode::Merge { key: key.clone() })
        }
    };
    let builder = builder.workdir(resolved_workdir(document, base));
    let builder = match &document.batch_policy {
        Some(policy) => builder.batch_policy(*policy),
        None => builder,
    };
    let builder = match &document.commit_policy {
        // Refused here rather than honoured: a policy with no
        // threshold never fires, so the run would hold everything
        // uncommitted until it ended.
        Some(policy) => {
            policy.check().map_err(Error::resolve)?;
            builder.commit_policy(*policy)
        }
        None => builder,
    };

    // The provider's typed errors render verbatim — the frozen
    // NotFound spelling, the handshake's identity/config refusals —
    // never a facade paraphrase on top.
    let source_config = document.source.config.resolve(base)?;
    let source = provider
        .source(&document.source.requirement(), &source_config)
        .await
        .map_err(|e| Error::resolve(e.to_string()))?;
    let dest_config = document.destination.config.resolve(base)?;
    let dest = provider
        .destination(&document.destination.requirement(), &dest_config)
        .await
        .map_err(|e| Error::resolve(e.to_string()))?;
    Ok(builder.source(source).destination(dest).build()?)
}

#[cfg(test)]
mod workdir_tests {
    use super::*;

    fn document_from(yaml: &str) -> Document {
        model::parse(yaml).expect("the fixture document parses")
    }

    const BODY: &str = "source:\n  postgres: s.yaml\ndestination:\n  duckdb: {path: out.db}\n";

    /// The default workdir is PER PIPELINE and lives beside the document:
    /// two defaulted documents in one directory get two workdirs, so
    /// neither can ever scan (or clear) the other's WAL.
    #[test]
    fn the_default_workdir_is_per_pipeline_under_the_document_dir() {
        let orders = document_from(&format!("pipeline: orders\n{BODY}"));
        let customers = document_from(&format!("pipeline: customers\n{BODY}"));
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
        let document = document_from(&format!("pipeline: \"../Orders / prod\"\n{BODY}"));
        let dir = resolved_workdir(&document, Path::new("/srv/specs"));
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
    fn an_explicit_workdir_resolves_against_the_document_dir() {
        let relative = document_from(&format!("pipeline: p\nworkdir: state/wd\n{BODY}"));
        assert_eq!(
            resolved_workdir(&relative, Path::new("/srv/specs")),
            Path::new("/srv/specs/state/wd")
        );
        let absolute = document_from(&format!("pipeline: p\nworkdir: /var/lib/rdlt\n{BODY}"));
        assert_eq!(
            resolved_workdir(&absolute, Path::new("/srv/specs")),
            Path::new("/var/lib/rdlt")
        );
    }

    /// An empty base — a document built from a string with
    /// `Path::new("")` — stays working-directory-relative, exactly as
    /// documented for embedders.
    #[test]
    fn an_empty_base_keeps_the_default_cwd_relative() {
        let document = document_from(&format!("pipeline: p\n{BODY}"));
        assert_eq!(
            resolved_workdir(&document, Path::new("")),
            Path::new(".rdlt/p")
        );
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
        let document: Document = serde_yaml_ng::from_str(
            "pipeline: p\nbatch_policy: {every_bytes: 1048576}\n\
             source:\n  postgres: s.yaml\ndestination:\n  duckdb: {path: out.db}\n",
        )
        .expect("the fixture document parses");
        assert_eq!(
            engine_budget_bytes(&document),
            rdlt_engine::DEFAULT_BYTE_BUDGET as u64,
            "a 1 MiB dest-write threshold must leave the wire budget at the engine default"
        );
    }
}
