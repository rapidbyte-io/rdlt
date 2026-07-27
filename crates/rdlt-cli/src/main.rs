//! # rdlt — thin development CLI
//!
//! `rdlt run <pipeline.yaml> [--report <path>]`
//!
//! ONE YAML document describes the whole pipeline: pipeline-wide settings,
//! the source (inline, or `config: path` to a reusable document), and the
//! destination — one file, one format, end to end. The document model and its
//! construction into a pipeline are the shared [`rdlt::pipeline_spec`]; this
//! binary parses the file, renders the event feed, and emits the report.
//!
//! Everything the CLI does, the library does — the CLI adds zero engine
//! capability. Events stream to stderr (human-readable); the
//! `RunReport` JSON goes to stdout or `--report`.
//!
//! Exit codes mirror `RdltError` variants (stable, scriptable):
//! 0 success · 2 config · 3 schema contract · 4 source · 5 destination · 6 WAL/disk ·
//! 7 cancelled · 64 usage · 70 internal defect · 74 file I/O.
//!
//! 70 is also the code for any variant this build does not know: `RdltError` is
//! `#[non_exhaustive]`, and "this binary cannot classify what happened" is a bug
//! to report, never an instruction to go and edit the pipeline configuration.

mod cdc;

use std::path::PathBuf;
use std::process::ExitCode;

use rdlt::pipeline_spec::{self, Spec, SpecError};
use rdlt::prelude::*;

/// Bound glibc's allocator retention: data movement churns
/// large short-lived buffers (slabs, arenas, arrow builds), and glibc retains
/// them as RSS long after free.
///
/// What each call actually does, measured as a 2x2 factorial (7 interleaved
/// runs per arm, two cells, quiet machine):
///
/// - `M_TRIM_THRESHOLD` is set to 128 KiB, which IS glibc's default — so the
///   value changes nothing. The call's real effect is its documented side
///   effect: it DISABLES glibc's dynamic growth of the mmap/trim thresholds.
///   Without it, glibc raises those thresholds as it sees large frees, so more
///   big allocations come from the retained heap. That is the knob doing the
///   memory work: dropping it costs +29% peak RSS on a 1M-row relational copy
///   and +32% on the nested-JSONL cell.
/// - `M_ARENA_MAX = 2` bounds per-thread arena growth. Its effect is small and
///   NOT consistent: it improved both wall and RSS on the relational copy and
///   cost ~4% wall on the JSONL cell, buying no RSS there.
///
/// Wall-clock effects of the pair are within a few percent in BOTH directions
/// depending on the cell, so neither "free" nor "costly" is an honest summary;
/// the memory reduction is the reason it is here, and it is large.
///
/// CLI-only: library embedders own their allocator policy. The workspace denies
/// unsafe; this single libc FFI call (no pointers, no invariants — two integer
/// knobs) is the deliberate exception.
#[allow(unsafe_code)]
fn bound_allocator_retention() {
    #[cfg(target_env = "gnu")]
    // SAFETY: mallopt takes two ints, touches no memory we own, and is called
    // before any pipeline threads exist.
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, 2);
        libc::mallopt(libc::M_TRIM_THRESHOLD, 128 * 1024);
    }
}

fn usage() -> ExitCode {
    eprintln!("usage: rdlt run <pipeline.yaml> [--report <path>]");
    ExitCode::from(64)
}

fn main() -> ExitCode {
    bound_allocator_retention();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let (spec_path, report_path) = match args.as_slice() {
        [cmd, spec] if cmd == "run" => (PathBuf::from(spec), None),
        [cmd, spec, flag, report] if cmd == "run" && flag == "--report" => {
            (PathBuf::from(spec), Some(PathBuf::from(report)))
        }
        _ => return usage(),
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: starting runtime: {e}");
            return ExitCode::from(2);
        }
    };
    match runtime.block_on(run(spec_path, report_path)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(CliError::Io(message)) => {
            eprintln!("error: {message}");
            ExitCode::from(74) // EX_IOERR: the invocation was fine, the filesystem was not
        }
        Err(CliError::Usage(message)) => {
            eprintln!("error: {message}");
            ExitCode::from(2)
        }
        Err(CliError::Run(error)) => {
            eprintln!("error: {error}");
            ExitCode::from(match error {
                RdltError::Config { .. } => 2,
                RdltError::Schema(_) => 3,
                RdltError::Source { .. } => 4,
                RdltError::Destination { .. } => 5,
                RdltError::Wal { .. } => 6,
                RdltError::Cancelled => 7,
                RdltError::Internal { .. } => 70,
                // NOT 2. Falling back to the config code tells a scripting
                // caller to fix their YAML for something the engine could not
                // classify, and a future variant would silently join it.
                _ => 70,
            })
        }
    }
}

enum CliError {
    Usage(String),
    /// A file the CLI itself could not read or write. Distinct from `Usage`:
    /// the invocation was well-formed, the filesystem refused.
    Io(String),
    Run(RdltError),
}

impl From<RdltError> for CliError {
    fn from(e: RdltError) -> Self {
        CliError::Run(e)
    }
}

impl From<SpecError> for CliError {
    fn from(e: SpecError) -> Self {
        match e {
            // A spec-resolution problem is a config error (exit 2), the same
            // taxonomy the loud parse/IO paths below use.
            SpecError::Resolve(message) => CliError::Usage(message),
            // The builder's own typed error keeps its exit-code mapping.
            SpecError::Build(error) => CliError::Run(error),
        }
    }
}

async fn run(spec_path: PathBuf, report_path: Option<PathBuf>) -> Result<(), CliError> {
    let raw = std::fs::read_to_string(&spec_path)
        .map_err(|e| CliError::Io(format!("reading {}: {e}", spec_path.display())))?;
    let spec: Spec =
        serde_yaml::from_str(&raw).map_err(|e| CliError::Usage(format!("parsing spec: {e}")))?;

    // The exactly-once CDC composition advisories need the resolved postgres
    // SOURCE config; `pg_source_config` is `None` for other source kinds.
    if let Some(config) = spec.pg_source_config() {
        let config = config?;
        for warning in cdc::cdc_composition_warnings(&spec, &config) {
            eprintln!("warning: {warning}");
        }
    }

    let pipeline = pipeline_spec::build_pipeline(&spec)?;
    drive(pipeline, report_path).await
}

/// Event feed + run + report emission (shared tail after the pipeline is built).
async fn drive(pipeline: rdlt::Pipeline, report_path: Option<PathBuf>) -> Result<(), CliError> {
    let mut events = pipeline.events();
    let feed = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                PipelineEvent::StreamStarted { stream } => eprintln!("-> stream {stream} started"),
                PipelineEvent::BatchLoaded { table, rows, .. } => {
                    eprintln!("  {table}: +{rows} rows")
                }
                PipelineEvent::SchemaEvolved { delta } => {
                    eprintln!(
                        "  schema: {} -> {} changes",
                        delta.table,
                        delta.changes.len()
                    )
                }
                PipelineEvent::Committed { commit_seq, .. } => {
                    eprintln!("commit {commit_seq} ok")
                }
                PipelineEvent::Discarded {
                    table,
                    rows,
                    values,
                    ..
                } => {
                    eprintln!("! {table}: discarded {rows} rows / {values} values")
                }
                PipelineEvent::Retried { stream, attempt } => {
                    eprintln!("! retry attempt {attempt} ({stream:?})")
                }
                PipelineEvent::StreamFinished { stream } => {
                    eprintln!("-> stream {stream} finished")
                }
                _ => {}
            }
        }
    });

    let report = pipeline.run().await?;
    // A failed renderer is reported but never fails the run: by this point the
    // load has succeeded and the report is in hand, so exiting non-zero would
    // misreport the outcome.
    if let Err(e) = feed.await {
        eprintln!("warning: event feed stopped: {e}");
    }

    let json = serde_json::to_string_pretty(&report)
        .map_err(|e| CliError::Usage(format!("encoding report: {e}")))?;
    match report_path {
        Some(path) => std::fs::write(&path, json)
            .map_err(|e| CliError::Io(format!("writing {}: {e}", path.display())))?,
        None => println!("{json}"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rdlt::pipeline_spec::{DestSpec, PgSourceSpec, SourceSpec, Spec, WriteModeSpec};
    use serde::Deserialize;

    fn spec(yaml: &str) -> Spec {
        serde_yaml::from_str(yaml).expect("spec parses")
    }

    /// Every document in the shared parity fixture must parse as a Spec. The
    /// bench harness pins the SAME file against the SAME shared model
    /// (`rdlt::pipeline_spec`), so this fixture — the one place a destination
    /// or source kind is added first — is exercised from both consumers.
    #[test]
    fn shared_parity_specs_all_parse() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../benches/parity_specs.yaml"
        ));
        let mut parsed = 0usize;
        for document in serde_yaml::Deserializer::from_str(raw) {
            let parsed_spec = Spec::deserialize(document).expect("parity spec parses in the CLI");
            assert!(!parsed_spec.pipeline.is_empty());
            parsed += 1;
        }
        assert_eq!(parsed, 5, "fixture covers every destination kind");
    }

    /// The iceberg destination block parses the crate's full vocabulary from
    /// the pipeline YAML with zero CLI code — and validation errors are typed
    /// at spec load.
    #[test]
    fn iceberg_spec_parses_from_the_yaml() {
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  iceberg:
    catalog:
      uri: https://polaris.example/api/catalog
      warehouse: rdlt
      auth:
        oauth2_client_credentials: {client_id: cid, client_secret: hunter2-cli}
    namespace: raw.orders
    create_namespace: true
    tables:
      events:
        partition_by: [{column: region, transform: identity}]
"#,
        );
        let DestSpec::Iceberg(config) = parsed.destination else {
            panic!("expected iceberg dest");
        };
        assert_eq!(config.namespace_levels(), vec!["raw", "orders"]);
        assert_eq!(config.partition_fields("events").len(), 1);
        assert!(config.validate().is_ok());
        assert!(
            !format!("{config:?}").contains("hunter2-cli"),
            "secret redacted"
        );
    }

    /// The per-table destination options ride the pipeline YAML with zero CLI
    /// code, and the duckdb destination block accepts the SAME options
    /// vocabulary as postgres — one YAML shape.
    #[test]
    fn duckdb_options_pass_through_the_yaml() {
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  duckdb:
    path: out.duckdb
    merge_strategy: upsert
    extensions: [httpfs]
    settings: {threads: "4"}
    tables:
      events:
        hard_delete: deleted
        dedup_sort: {column: seq, order: desc}
"#,
        );
        let DestSpec::Duckdb {
            merge_strategy,
            tables,
            extensions,
            settings,
            ..
        } = &parsed.destination
        else {
            panic!("duckdb dest");
        };
        assert_eq!(extensions.as_deref(), Some(&["httpfs".to_string()][..]));
        assert_eq!(
            settings
                .as_ref()
                .and_then(|s| s.get("threads"))
                .map(String::as_str),
            Some("4")
        );
        assert_eq!(
            *merge_strategy,
            Some(rdlt::connector::duckdb::dest::MergeStrategy::Upsert)
        );
        let events = tables.as_ref().expect("tables")["events"].clone();
        assert_eq!(events.hard_delete.as_deref(), Some("deleted"));
        assert!(events.dedup_sort.is_some());
    }

    #[test]
    fn refinement_options_pass_through_the_yaml() {
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  postgres:
    conn: host=x
    dataset: d
    tables:
      events:
        hard_delete: deleted
        dedup_sort: {column: seq, order: desc}
        merge_scope: [day, tenant]
"#,
        );
        let DestSpec::Postgres { tables, .. } = &parsed.destination else {
            panic!("postgres dest");
        };
        let events = tables.as_ref().expect("tables")["events"].clone();
        let dedup = events.dedup_sort.expect("dedup_sort");
        assert_eq!(dedup.column, "seq");
        assert_eq!(
            dedup.order,
            rdlt::connector::postgres::dest::SortOrder::Desc
        );
        assert_eq!(
            events.merge_scope.as_deref(),
            Some(&["day".to_string(), "tenant".to_string()][..])
        );
    }

    /// ONE pipeline YAML: the source document inline is the natural form
    /// and is held to the SAME validation as file configs (from_value).
    #[test]
    fn inline_postgres_source_parses_and_validates() {
        let parsed = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres:
    conn: host=localhost
    tables:
      - name: orders
        cursor: {column: updated_at, lag: "5m"}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
        );
        let SourceSpec::Postgres(PgSourceSpec::Inline(inline)) = &parsed.source else {
            panic!("inline postgres source");
        };
        let table = &inline.tables.as_ref().expect("tables")[0];
        assert_eq!(table.name, "orders");
        assert_eq!(table.cursor.as_ref().expect("cursor").column, "updated_at");
        // The run() path re-validates through from_value — prove the gate
        // holds for inline documents too.
        let value = serde_json::to_value(inline).expect("serialize");
        rdlt::connector::postgres::source::PostgresConfig::from_value(value).expect("valid inline");

        // …and rejects invalid shapes identically — cdc and cursor are
        // mutually exclusive on the same table.
        let bad = spec(
            r#"
pipeline: p
source:
  postgres:
    conn: host=localhost
    cdc: {slot: s, publication: p}
    tables:
      - name: t
        cursor: {column: id}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
        );
        let SourceSpec::Postgres(PgSourceSpec::Inline(inline)) = &bad.source else {
            panic!("inline postgres source");
        };
        let value = serde_json::to_value(inline).expect("serialize");
        let err = rdlt::connector::postgres::source::PostgresConfig::from_value(value)
            .expect_err("cdc + cursor rejected inline")
            .to_string();
        assert!(err.contains("mutually exclusive"), "{err}");

        // `config:` mixed with inline fields is a LOUD error, not a
        // silently-ignored document.
        let mixed: Result<Spec, _> = serde_yaml::from_str(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml, conn: host=localhost}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
        );
        assert!(mixed.is_err(), "config + inline fields must not parse");

        // The file form still parses.
        let file = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
        );
        assert!(matches!(
            file.source,
            SourceSpec::Postgres(PgSourceSpec::File(_))
        ));
    }

    /// Every pipeline-spec form parses — write_mode's three shapes, all
    /// destination kinds, workdir default vs custom.
    #[test]
    fn pipeline_spec_forms_parse() {
        for (mode, want_merge) in [
            ("write_mode: append\n", false),
            ("write_mode: replace\n", false),
            ("write_mode: {merge: {key: [id]}}\n", true),
            ("", false), // absent = append default
        ] {
            let parsed = spec(&format!(
                "pipeline: p\n{mode}source:\n  postgres: {{config: s.yaml}}\n\
                 destination:\n  duckdb: {{path: out.db}}\n"
            ));
            assert_eq!(
                matches!(parsed.write_mode, Some(WriteModeSpec::Merge { .. })),
                want_merge,
                "{mode}"
            );
        }
        // Destination kinds + workdir.
        let parquet = spec(
            "pipeline: p\nworkdir: /tmp/x\nsource:\n  postgres: {config: s.yaml}\n\
             destination:\n  parquet: {path: out}\n",
        );
        assert!(matches!(parquet.destination, DestSpec::Parquet { .. }));
        assert_eq!(
            parquet.workdir.as_deref(),
            Some(std::path::Path::new("/tmp/x"))
        );
        let duck = spec(
            "pipeline: p\nsource:\n  postgres: {config: s.yaml}\n\
             destination:\n  duckdb: {path: out.db, memory_limit: \"1GB\"}\n",
        );
        assert!(matches!(duck.destination, DestSpec::Duckdb { .. }));
        assert!(
            duck.workdir.is_none(),
            "workdir defaults downstream to .rdlt"
        );
        // 015: the file destination's full vocabulary.
        let file = spec(
            r#"
pipeline: p
source:
  postgres: {config: s.yaml}
destination:
  file:
    path: warehouse
    format: jsonl
    partition_by: day
    location:
      s3:
        endpoint: "http://x:9000"
        bucket: lake
        access_key: k
        secret_key: s
"#,
        );
        match &file.destination {
            DestSpec::File(config) => {
                // The document deserializes straight into the connector's own
                // config, so these assertions now read the SAME type the
                // destination is built from — there is no mirror in between to
                // disagree with it.
                assert!(matches!(
                    config.format,
                    rdlt::connector::file::dest::DestFormat::Jsonl
                ));
                assert!(config.location.is_some());
                assert_eq!(config.partition_by.as_deref(), Some("day"));
            }
            other => panic!("expected file destination, got {other:?}"),
        }
    }
}
