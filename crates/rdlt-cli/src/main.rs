//! # rdlt — thin development CLI
//!
//! `rdlt run <pipeline.yaml> [--report <path>]`
//!
//! ONE YAML document describes the whole pipeline: pipeline-wide settings,
//! the source (inline, or `config: path` to a reusable document), and the
//! destination — one file, one format, end to end.
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
    // singleton_map: YAML's natural `write_mode: {merge: {key: […]}}` /
    // `source: postgres: …` singleton-map form for externally-tagged
    // enums (serde_yaml 0.9 otherwise wants `!tag` syntax).
    #[serde(default, with = "serde_yaml::with::singleton_map")]
    write_mode: Option<WriteModeSpec>,
    #[serde(with = "serde_yaml::with::singleton_map")]
    source: SourceSpec,
    #[serde(with = "serde_yaml::with::singleton_map")]
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
    /// Postgres source: the config document INLINE (the natural form — the
    /// pipeline is one YAML document), or `config: path` referencing a
    /// reusable YAML/JSON file with the identical shape.
    Postgres(PgSourceSpec),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PgSourceSpec {
    /// `source: postgres: {config: source.yaml}` — tried first; strict
    /// (`deny_unknown_fields`), so `config` mixed with inline fields is a
    /// loud error, never a silently-ignored document.
    File(PgSourceFile),
    /// The full source document inline (boxed — it dwarfs the path form).
    Inline(Box<rdlt::connector::postgres::source::PostgresConfig>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PgSourceFile {
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum DestSpec {
    Duckdb {
        path: PathBuf,
        memory_limit: Option<String>,
        /// Feature 013: the SAME destination-options vocabulary as postgres
        /// (shared sqlcore types — one YAML shape, contract SM5).
        merge_strategy: Option<rdlt::connector::duckdb::dest::MergeStrategy>,
        tables:
            Option<std::collections::BTreeMap<String, rdlt::connector::duckdb::dest::TableOptions>>,
    },
    Postgres {
        conn: String,
        dataset: String,
        /// Optional TLS block: `tls: {mode: verify_full, root_cert: /ca.pem}`.
        tls: Option<rdlt::connector::postgres::tls::TlsPolicy>,
        /// Feature 008: destination-wide merge strategy
        /// ("delete_insert" | "upsert" | "scd2").
        merge_strategy: Option<rdlt::connector::postgres::dest::MergeStrategy>,
        /// Feature 008/010: per-table options — `tables: <name>: {…}` with
        /// `merge_strategy`, `hard_delete`, `dedup_sort`, `merge_key`, and
        /// `scd2: {valid_from, valid_to, absent}`.
        tables: Option<
            std::collections::BTreeMap<String, rdlt::connector::postgres::dest::PgTableOptions>,
        >,
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
        serde_yaml::from_str(&raw).map_err(|e| CliError::Usage(format!("parsing spec: {e}")))?;

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
                DestSpec::Duckdb {
                    path,
                    memory_limit,
                    merge_strategy,
                    tables,
                } => {
                    let mut dest = rdlt::connector::duckdb::dest::DuckDb::open(path)
                        .map_err(|e| CliError::Usage(format!("opening duckdb: {e}")))?;
                    if let Some(limit) = memory_limit {
                        dest = dest
                            .memory_limit(limit)
                            .map_err(|e| CliError::Usage(format!("duckdb memory_limit: {e}")))?;
                    }
                    if merge_strategy.is_some() || tables.is_some() {
                        let options = rdlt::connector::duckdb::dest::DestOptions {
                            merge_strategy: *merge_strategy,
                            tables: tables.clone().unwrap_or_default(),
                        };
                        dest = dest
                            .options(options)
                            .map_err(|e| CliError::Usage(format!("destination options: {e}")))?;
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
                    let mut dest =
                        rdlt::connector::postgres::dest::Postgres::connect(conn).dataset(dataset);
                    if let Some(policy) = tls {
                        dest = dest.tls(policy.clone());
                    }
                    if merge_strategy.is_some() || tables.is_some() {
                        let options = rdlt::connector::postgres::dest::PgDestOptions {
                            merge_strategy: *merge_strategy,
                            tables: tables.clone().unwrap_or_default(),
                        };
                        dest = dest
                            .options(options)
                            .map_err(|e| CliError::Usage(format!("destination options: {e}")))?;
                    }
                    builder.destination(dest).build()?
                }
                DestSpec::Parquet { path } => {
                    let dest = rdlt::connector::parquet::ParquetDir::open(path)
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
                rdlt::connector::rest::RestSource::from_json(&text)
            } else {
                rdlt::connector::rest::RestSource::from_yaml(&text)
            }
            .map_err(|e| CliError::Usage(e.to_string()))?;
            run_with!(source)
        }
        SourceSpec::File { config } => {
            let text = std::fs::read_to_string(config)
                .map_err(|e| CliError::Usage(format!("reading {}: {e}", config.display())))?;
            let source = if is_json(config) {
                rdlt::connector::file::FileSource::from_json(&text)
            } else {
                rdlt::connector::file::FileSource::from_yaml(&text)
            }
            .map_err(|e| CliError::Usage(e.to_string()))?;
            run_with!(source)
        }
        SourceSpec::Postgres(pg) => {
            let parsed = match pg {
                PgSourceSpec::File(file) => {
                    let path = &file.config;
                    let text = std::fs::read_to_string(path)
                        .map_err(|e| CliError::Usage(format!("reading {}: {e}", path.display())))?;
                    if is_json(path) {
                        rdlt::connector::postgres::source::PostgresConfig::from_json(&text)
                    } else {
                        rdlt::connector::postgres::source::PostgresConfig::from_yaml(&text)
                    }
                    .map_err(|e| CliError::Usage(e.to_string()))?
                }
                PgSourceSpec::Inline(inline) => {
                    // Untagged deserialization bypassed the document
                    // validation; route through the shared from_value gate
                    // so inline and file configs are held to identical rules.
                    let value =
                        serde_json::to_value(inline).map_err(|e| CliError::Usage(e.to_string()))?;
                    rdlt::connector::postgres::source::PostgresConfig::from_value(value)
                        .map_err(|e| CliError::Usage(e.to_string()))?
                }
            };
            for warning in cdc_composition_warnings(&spec, &parsed) {
                eprintln!("warning: {warning}");
            }
            let source = rdlt::connector::postgres::source::PostgresSource::new(parsed);
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
    config: &rdlt::connector::postgres::source::PostgresConfig,
) -> Vec<String> {
    let Some(cdc) = &config.cdc else {
        return Vec::new();
    };
    let mut warnings = Vec::new();
    if !matches!(spec.write_mode, Some(WriteModeSpec::Merge { .. })) {
        warnings.push(
            "cdc: write_mode is not merge — changed rows will append instead of \
             converging; set write_mode: {merge: {key: [...]}} (contract C3)"
                .to_string(),
        );
    }
    match &spec.destination {
        DestSpec::Postgres {
            merge_strategy,
            tables,
            ..
        } => {
            if !matches!(
                merge_strategy,
                Some(rdlt::connector::postgres::dest::MergeStrategy::Upsert)
            ) {
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

    fn spec(yaml: &str) -> Spec {
        serde_yaml::from_str(yaml).expect("spec parses")
    }

    fn cdc_config() -> rdlt::connector::postgres::source::PostgresConfig {
        rdlt::connector::postgres::source::PostgresConfig::from_yaml(
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
             tables:\n  - name: orders\n",
        )
        .expect("config")
    }

    /// Feature 010 (MR7): the per-table destination options ride the
    /// pipeline YAML with zero CLI code.
    /// Feature 013 (SM5): the duckdb destination block accepts the SAME
    /// options vocabulary as postgres — one YAML shape.
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
    tables:
      events:
        hard_delete: deleted
        dedup_sort: {column: seq, order: desc}
"#,
        );
        let DestSpec::Duckdb {
            merge_strategy,
            tables,
            ..
        } = &parsed.destination
        else {
            panic!("duckdb dest");
        };
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
        merge_key: [day, tenant]
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
            events.merge_key.as_deref(),
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

        // …and rejects invalid shapes identically (cdc + cursor, C1).
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
            .expect_err("C1 holds inline")
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

    /// Feature 011 (PM1): every pipeline-spec form parses — write_mode's
    /// three shapes, all destination kinds, workdir default vs custom.
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
    }

    /// C3 capture matrix: the recommended composition is silent; every
    /// missing leg warns with the fix; non-merge destinations warn soft
    /// delete.
    #[test]
    fn cdc_composition_warning_matrix() {
        let recommended = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  postgres:
    conn: host=x
    dataset: d
    merge_strategy: upsert
    tables:
      orders: {hard_delete: _rdlt_deleted}
"#,
        );
        assert!(cdc_composition_warnings(&recommended, &cdc_config()).is_empty());

        let append = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
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
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  duckdb: {path: out.db}
"#,
        );
        let warnings = cdc_composition_warnings(&duckdb, &cdc_config());
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("soft delete"), "{warnings:?}");

        // Schema-wide discovery (no tables list): the hard_delete leg still
        // warns — once, generically (review F9).
        let schema_wide = rdlt::connector::postgres::source::PostgresConfig::from_yaml(
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n",
        )
        .expect("config");
        let recommended_no_tables = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres: {config: src.yaml}
destination:
  postgres: {conn: host=x, dataset: d, merge_strategy: upsert}
"#,
        );
        let warnings = cdc_composition_warnings(&recommended_no_tables, &schema_wide);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("schema-wide") && warnings[0].contains("hard_delete"),
            "{warnings:?}"
        );

        // No cdc block: silent regardless of shape.
        let plain =
            rdlt::connector::postgres::source::PostgresConfig::from_yaml("conn: host=localhost\n")
                .expect("config");
        assert!(cdc_composition_warnings(&append, &plain).is_empty());
    }
}
