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
    /// Postgres source: either `config` (path to the YAML/JSON document) or
    /// the same document INLINE under `[source.postgres.inline]` — exactly
    /// one of the two (parity with the inline destination block). Boxed:
    /// the inline document dwarfs the path-only variants.
    Postgres {
        #[serde(default)]
        config: Option<PathBuf>,
        #[serde(default)]
        inline: Option<Box<rdlt::postgres_source::PostgresConfig>>,
    },
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
        /// Optional TLS block: `tls = { mode = "verify_full", root_cert = "/ca.pem" }`.
        tls: Option<rdlt::postgres_tls::TlsPolicy>,
        /// Feature 008: destination-wide merge strategy
        /// ("delete_insert" | "upsert" | "scd2").
        merge_strategy: Option<rdlt::postgres::MergeStrategy>,
        /// Feature 008: per-table options —
        /// `[destination.postgres.tables.<name>]` with `merge_strategy`,
        /// `hard_delete`, and `[….scd2]` `{valid_from, valid_to, absent}`.
        tables: Option<std::collections::BTreeMap<String, rdlt::postgres::PgTableOptions>>,
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
                DestSpec::Postgres {
                    conn,
                    dataset,
                    tls,
                    merge_strategy,
                    tables,
                } => {
                    let mut dest = rdlt::postgres::Postgres::connect(conn).dataset(dataset);
                    if let Some(policy) = tls {
                        dest = dest.tls(policy.clone());
                    }
                    if merge_strategy.is_some() || tables.is_some() {
                        let options = rdlt::postgres::PgDestOptions {
                            merge_strategy: merge_strategy.unwrap_or_default(),
                            tables: tables.clone().unwrap_or_default(),
                        };
                        dest = dest
                            .options(options)
                            .map_err(|e| CliError::Usage(format!("destination options: {e}")))?;
                    }
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
        SourceSpec::Postgres { config, inline } => {
            let parsed = match (config, inline) {
                (Some(path), None) => {
                    let text = std::fs::read_to_string(path)
                        .map_err(|e| CliError::Usage(format!("reading {}: {e}", path.display())))?;
                    if is_json(path) {
                        rdlt::postgres_source::PostgresConfig::from_json(&text)
                    } else {
                        rdlt::postgres_source::PostgresConfig::from_yaml(&text)
                    }
                    .map_err(|e| CliError::Usage(e.to_string()))?
                }
                (None, Some(inline)) => {
                    // TOML deserialization bypassed the document validation;
                    // route through the shared from_value gate so inline and
                    // file configs are held to identical rules.
                    let value =
                        serde_json::to_value(inline).map_err(|e| CliError::Usage(e.to_string()))?;
                    rdlt::postgres_source::PostgresConfig::from_value(value)
                        .map_err(|e| CliError::Usage(e.to_string()))?
                }
                (Some(_), Some(_)) => {
                    return Err(CliError::Usage(
                        "source.postgres: `config` and `inline` are mutually \
                         exclusive — provide one"
                            .into(),
                    ));
                }
                (None, None) => {
                    return Err(CliError::Usage(
                        "source.postgres: provide `config` (path to the YAML/JSON \
                         document) or `inline` (the same document inline)"
                            .into(),
                    ));
                }
            };
            for warning in cdc_composition_warnings(&spec, &parsed) {
                eprintln!("warning: {warning}");
            }
            let source = rdlt::postgres_source::PostgresSource::new(parsed);
            run_with!(source)
        }
    }
}

/// C3 (feature 009, contract cdc-config.md): the exactly-once-outcome CDC
/// composition is `write_mode = merge{key}` + destination
/// `merge_strategy = upsert` + `hard_delete = <flag column>`. Its absence
/// WARNS, never blocks — other shapes are documented at-least-once /
/// soft-delete.
fn cdc_composition_warnings(
    spec: &Spec,
    config: &rdlt::postgres_source::PostgresConfig,
) -> Vec<String> {
    let Some(cdc) = &config.cdc else {
        return Vec::new();
    };
    let mut warnings = Vec::new();
    if !matches!(spec.write_mode, Some(WriteModeSpec::Merge { .. })) {
        warnings.push(
            "cdc: write_mode is not merge — changed rows will append instead of \
             converging; set write_mode = { merge = { key = [...] } } (contract C3)"
                .to_string(),
        );
    }
    match &spec.destination {
        DestSpec::Postgres {
            merge_strategy,
            tables,
            ..
        } => {
            if !matches!(merge_strategy, Some(rdlt::postgres::MergeStrategy::Upsert)) {
                warnings.push(
                    "cdc: destination merge_strategy is not upsert — the \
                     recommended composition is merge_strategy = \"upsert\" \
                     (contract C3)"
                        .to_string(),
                );
            }
            match &config.tables {
                // Schema-wide discovery: the table set is unknown here, but
                // the C3 warning must not go silent — one generic notice.
                None => warnings.push(format!(
                    "cdc: schema-wide discovery (no `tables:` list) — give every \
                     CDC table hard_delete = \"{}\" in the destination options, \
                     or deletes land as flagged rows (soft delete) instead of \
                     removals (contract C3)",
                    cdc.flag_column
                )),
                Some(listed) => {
                    for table in listed {
                        let has_flag = tables
                            .as_ref()
                            .and_then(|t| t.get(&table.name))
                            .and_then(|t| t.hard_delete.as_deref())
                            == Some(cdc.flag_column.as_str());
                        if !has_flag {
                            warnings.push(format!(
                                "cdc: table `{}` has no hard_delete = \"{}\" — \
                                 deletes will land as flagged rows (soft delete) \
                                 instead of removals (contract C3)",
                                table.name, cdc.flag_column
                            ));
                        }
                    }
                }
            }
        }
        DestSpec::Duckdb { .. } | DestSpec::Parquet { .. } => {
            warnings.push(format!(
                "cdc: this destination has no hard-delete support — the \
                 deletion flag `{}` lands as data (documented soft delete, \
                 contract C3/P8)",
                cdc.flag_column
            ));
        }
    }
    warnings
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

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(toml_text: &str) -> Spec {
        toml::from_str(toml_text).expect("spec parses")
    }

    fn cdc_config() -> rdlt::postgres_source::PostgresConfig {
        rdlt::postgres_source::PostgresConfig::from_yaml(
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
             tables:\n  - name: orders\n",
        )
        .expect("config")
    }

    /// Feature 010 (MR7): the per-table destination options carry the
    /// refinement fields through the toml passthrough with zero CLI code.
    #[test]
    fn refinement_options_pass_through_the_toml() {
        let spec = spec(
            "pipeline = \"p\"\n\
             [source.postgres]\nconfig = \"src.yaml\"\n\
             [destination.postgres]\nconn = \"host=x\"\ndataset = \"d\"\n\
             [destination.postgres.tables.events]\nhard_delete = \"deleted\"\n\
             dedup_sort = { column = \"seq\", order = \"desc\" }\n\
             merge_key = [\"day\", \"tenant\"]\n",
        );
        let DestSpec::Postgres { tables, .. } = &spec.destination else {
            panic!("postgres dest");
        };
        let events = tables.as_ref().expect("tables")["events"].clone();
        let dedup = events.dedup_sort.expect("dedup_sort");
        assert_eq!(dedup.column, "seq");
        assert_eq!(dedup.order, rdlt::postgres::SortOrder::Desc);
        assert_eq!(
            events.merge_key.as_deref(),
            Some(&["day".to_string(), "tenant".to_string()][..])
        );
    }

    /// Inline source config (parity with the inline destination): the full
    /// document rides the pipeline TOML and is held to the SAME validation
    /// as file configs (the from_value gate).
    #[test]
    fn inline_postgres_source_parses_and_validates() {
        let parsed = spec(
            "pipeline = \"p\"\n\
             [write_mode.merge]\nkey = [\"id\"]\n\
             [source.postgres.inline]\nconn = \"host=localhost\"\n\
             [[source.postgres.inline.tables]]\nname = \"orders\"\n\
             [source.postgres.inline.tables.cursor]\ncolumn = \"updated_at\"\n\
             lag = \"5m\"\n\
             [destination.postgres]\nconn = \"host=x\"\ndataset = \"d\"\n",
        );
        let SourceSpec::Postgres { config, inline } = &parsed.source else {
            panic!("postgres source");
        };
        assert!(config.is_none());
        let inline = inline.as_ref().expect("inline config");
        let table = &inline.tables.as_ref().expect("tables")[0];
        assert_eq!(table.name, "orders");
        assert_eq!(table.cursor.as_ref().expect("cursor").column, "updated_at");
        // The run() path re-validates through from_value — prove the gate
        // holds for inline documents too.
        let value = serde_json::to_value(inline).expect("serialize");
        rdlt::postgres_source::PostgresConfig::from_value(value).expect("valid inline");

        // …and rejects invalid shapes identically (cdc + cursor, C1).
        let bad = spec(
            "pipeline = \"p\"\n\
             [source.postgres.inline]\nconn = \"host=localhost\"\n\
             [source.postgres.inline.cdc]\nslot = \"s\"\npublication = \"p\"\n\
             [[source.postgres.inline.tables]]\nname = \"t\"\n\
             [source.postgres.inline.tables.cursor]\ncolumn = \"id\"\n\
             [destination.postgres]\nconn = \"host=x\"\ndataset = \"d\"\n",
        );
        let SourceSpec::Postgres { inline, .. } = &bad.source else {
            panic!("postgres source");
        };
        let value = serde_json::to_value(inline.as_ref().unwrap()).expect("serialize");
        let err = rdlt::postgres_source::PostgresConfig::from_value(value)
            .expect_err("C1 holds inline")
            .to_string();
        assert!(err.contains("mutually exclusive"), "{err}");
    }

    /// C3 capture matrix: the recommended composition is silent; every
    /// missing leg warns with the fix; non-merge destinations warn soft
    /// delete.
    #[test]
    fn cdc_composition_warning_matrix() {
        let recommended = spec(
            "pipeline = \"p\"\n\
             [write_mode.merge]\nkey = [\"id\"]\n\
             [source.postgres]\nconfig = \"src.yaml\"\n\
             [destination.postgres]\nconn = \"host=x\"\ndataset = \"d\"\n\
             merge_strategy = \"upsert\"\n\
             [destination.postgres.tables.orders]\nhard_delete = \"_rdlt_deleted\"\n",
        );
        assert!(cdc_composition_warnings(&recommended, &cdc_config()).is_empty());

        let append = spec(
            "pipeline = \"p\"\n\
             [source.postgres]\nconfig = \"src.yaml\"\n\
             [destination.postgres]\nconn = \"host=x\"\ndataset = \"d\"\n",
        );
        let warnings = cdc_composition_warnings(&append, &cdc_config());
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        assert!(warnings[0].contains("write_mode"), "{warnings:?}");
        assert!(warnings[1].contains("upsert"), "{warnings:?}");
        assert!(
            warnings[2].contains("`orders`") && warnings[2].contains("hard_delete"),
            "{warnings:?}"
        );

        let duckdb = spec(
            "pipeline = \"p\"\n\
             [write_mode.merge]\nkey = [\"id\"]\n\
             [source.postgres]\nconfig = \"src.yaml\"\n\
             [destination.duckdb]\npath = \"out.db\"\n",
        );
        let warnings = cdc_composition_warnings(&duckdb, &cdc_config());
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("soft delete"), "{warnings:?}");

        // Schema-wide discovery (no tables list): the hard_delete leg still
        // warns — once, generically (review F9).
        let schema_wide = rdlt::postgres_source::PostgresConfig::from_yaml(
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n",
        )
        .expect("config");
        let recommended_no_tables = spec(
            "pipeline = \"p\"\n\
             [write_mode.merge]\nkey = [\"id\"]\n\
             [source.postgres]\nconfig = \"src.yaml\"\n\
             [destination.postgres]\nconn = \"host=x\"\ndataset = \"d\"\n\
             merge_strategy = \"upsert\"\n",
        );
        let warnings = cdc_composition_warnings(&recommended_no_tables, &schema_wide);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("schema-wide") && warnings[0].contains("hard_delete"),
            "{warnings:?}"
        );

        // No cdc block: silent regardless of shape.
        let plain = rdlt::postgres_source::PostgresConfig::from_yaml("conn: host=localhost\n")
            .expect("config");
        assert!(cdc_composition_warnings(&append, &plain).is_empty());
    }
}
