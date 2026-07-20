//! # rdlt — thin development CLI
//!
//! `rdlt run <pipeline.toml> [--report <path>]`
//!
//! Everything the CLI does, the library does (contract: embedder-api.md — the CLI
//! adds zero engine capability). Events stream to stderr (human-readable); the
//! `RunReport` JSON goes to stdout or `--report`.
//!
//! Exit codes mirror `RdltError` variants (stable, scriptable):
//! 0 success · 2 config · 3 schema contract · 4 source · 5 destination · 6 WAL/disk ·
//! 7 cancelled · 64 usage.

use std::path::PathBuf;
use std::process::ExitCode;

use rdlt::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    pipeline: String,
    #[serde(default)]
    workdir: Option<PathBuf>,
    #[serde(default)]
    write_mode: Option<WriteModeSpec>,
    source: SourceSpec,
    destination: DestSpec,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WriteModeSpec {
    Append,
    Replace,
    Merge { key: Vec<String> },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum SourceSpec {
    /// Path to the declarative REST source YAML.
    Rest { config: PathBuf },
    /// Path to the file source YAML (jsonl/parquet streams).
    File { config: PathBuf },
    /// Path to the Postgres source YAML (tables, cursors, batching).
    Postgres { config: PathBuf },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum DestSpec {
    Duckdb {
        path: PathBuf,
        memory_limit: Option<String>,
    },
    Postgres {
        conn: String,
        dataset: String,
    },
    Parquet {
        path: PathBuf,
    },
}

/// Bound glibc's allocator retention (feature 003 T024): data movement churns
/// large short-lived buffers (slabs, arenas, arrow builds), and glibc's default
/// per-thread arenas retain them as RSS long after free. Two arenas + a low trim
/// threshold returns memory to the OS with no measured wall-time cost (642 MB →
/// ~370 MB peak on the flagship bench). CLI-only: library embedders own their
/// allocator policy. The workspace denies unsafe; this single libc FFI call
/// (no pointers, no invariants — two integer knobs) is the deliberate exception.
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
    eprintln!("usage: rdlt run <pipeline.toml> [--report <path>]");
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
                _ => 2,
            })
        }
    }
}

enum CliError {
    Usage(String),
    Run(RdltError),
}

impl From<RdltError> for CliError {
    fn from(e: RdltError) -> Self {
        CliError::Run(e)
    }
}

async fn run(spec_path: PathBuf, report_path: Option<PathBuf>) -> Result<(), CliError> {
    let raw = std::fs::read_to_string(&spec_path)
        .map_err(|e| CliError::Usage(format!("reading {}: {e}", spec_path.display())))?;
    let spec: Spec =
        toml::from_str(&raw).map_err(|e| CliError::Usage(format!("parsing spec: {e}")))?;

    // The source and destination arms each fix the builder's generic type, so the
    // whole tail expands per combination via a macro (typestate-friendly).
    macro_rules! run_with {
        ($source:expr) => {{
            let builder = Pipeline::builder(spec.pipeline.as_str()).source($source);
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
            let mut pipeline = match &spec.destination {
                DestSpec::Duckdb { path, memory_limit } => {
                    let mut dest = rdlt::duckdb::DuckDb::open(path)
                        .map_err(|e| CliError::Usage(format!("opening duckdb: {e}")))?;
                    if let Some(limit) = memory_limit {
                        dest = dest
                            .memory_limit(limit)
                            .map_err(|e| CliError::Usage(format!("duckdb memory_limit: {e}")))?;
                    }
                    builder.destination(dest).build()?
                }
                DestSpec::Postgres { conn, dataset } => {
                    let dest = rdlt::postgres::Postgres::connect(conn).dataset(dataset);
                    builder.destination(dest).build()?
                }
                DestSpec::Parquet { path } => {
                    let dest = rdlt::parquet::ParquetDir::open(path)
                        .map_err(|e| CliError::Usage(format!("opening parquet dir: {e}")))?;
                    builder.destination(dest).build()?
                }
            };
            drive(&mut pipeline, report_path).await
        }};
    }

    // Source config files: YAML by default, JSON when the file says so —
    // the same document shape either way (the library's from_yaml/from_json
    // share validation; embedders pass serde_json::Value via from_value).
    let is_json = |path: &PathBuf| {
        path.extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("json"))
    };
    match &spec.source {
        SourceSpec::Rest { config } => {
            let text = std::fs::read_to_string(config)
                .map_err(|e| CliError::Usage(format!("reading {}: {e}", config.display())))?;
            let source = if is_json(config) {
                rdlt::rest::RestSource::from_json(&text)
            } else {
                rdlt::rest::RestSource::from_yaml(&text)
            }
            .map_err(|e| CliError::Usage(e.to_string()))?;
            run_with!(source)
        }
        SourceSpec::File { config } => {
            let text = std::fs::read_to_string(config)
                .map_err(|e| CliError::Usage(format!("reading {}: {e}", config.display())))?;
            let source = if is_json(config) {
                rdlt::file::FileSource::from_json(&text)
            } else {
                rdlt::file::FileSource::from_yaml(&text)
            }
            .map_err(|e| CliError::Usage(e.to_string()))?;
            run_with!(source)
        }
        SourceSpec::Postgres { config } => {
            let text = std::fs::read_to_string(config)
                .map_err(|e| CliError::Usage(format!("reading {}: {e}", config.display())))?;
            let source = if is_json(config) {
                rdlt::postgres_source::PostgresSource::from_json(&text)
            } else {
                rdlt::postgres_source::PostgresSource::from_yaml(&text)
            }
            .map_err(|e| CliError::Usage(e.to_string()))?;
            run_with!(source)
        }
    }
}

/// Event feed + run + report emission (shared tail after the pipeline is built).
async fn drive(
    pipeline: &mut rdlt::Pipeline,
    report_path: Option<PathBuf>,
) -> Result<(), CliError> {
    let mut events = pipeline.events().expect("events available before run");
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
    let _ = feed.await;

    let json = serde_json::to_string_pretty(&report)
        .map_err(|e| CliError::Usage(format!("encoding report: {e}")))?;
    match report_path {
        Some(path) => std::fs::write(&path, json)
            .map_err(|e| CliError::Usage(format!("writing {}: {e}", path.display())))?,
        None => println!("{json}"),
    }
    Ok(())
}
