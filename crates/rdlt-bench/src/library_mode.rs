//! Library-mode runs: the pipeline in-process via the `rdlt` crate. RunReport
//! gives EXACT rows/bytes; the `events()` seam gives per-stream attribution.
//! Scoreboard detail only — gated numbers bind to the subprocess mode, which
//! is the measured, release-CLI configuration. CPU/RSS are subprocess-mode
//! metrics: in-process /proc/self readings would accumulate across runs, so
//! they are null here with a stated reason — a metric is never fabricated;
//! an absent number is null with a reason.
//!
//! The `Spec` below deliberately duplicates the CLI's YAML spec structs
//! (crates/rdlt-cli/src/main.rs): both are thin consumers of the same library
//! surface, and this harness must parse the SAME pipeline templates the
//! subprocess mode feeds the CLI. The duplication is a deliberate constraint,
//! not an oversight — the shared fixture `benches/parity_specs.yaml` pins both
//! parsers so the two struct sets can never drift apart.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rdlt::prelude::*;
use serde::Deserialize;

use crate::artifact::{CpuStats, RdltSide, RssStats, StreamAttribution, VerifyOutcome};
use crate::cells::Cell;
use crate::protocol::{self, Sample};
use crate::runner::{Paths, substitute};
use crate::{BenchError, Result};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    pipeline: String,
    #[serde(default)]
    workdir: Option<PathBuf>,
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
    Rest { config: PathBuf },
    File { config: PathBuf },
    Postgres(PgSourceSpec),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PgSourceSpec {
    File(PgSourceFile),
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
        /// The SAME destination-options vocabulary as postgres — shared
        /// sqlcore types, one YAML shape.
        merge_strategy: Option<rdlt::connector::duckdb::dest::MergeStrategy>,
        tables:
            Option<std::collections::BTreeMap<String, rdlt::connector::duckdb::dest::TableOptions>>,
        /// dlt-parity passthrough: extensions to LOAD and `SET` settings.
        extensions: Option<Vec<String>>,
        settings: Option<std::collections::BTreeMap<String, String>>,
    },
    Postgres {
        conn: String,
        dataset: String,
        tls: Option<rdlt::connector::postgres::tls::TlsPolicy>,
        merge_strategy: Option<rdlt::connector::postgres::dest::MergeStrategy>,
        tables: Option<BTreeMap<String, rdlt::connector::postgres::dest::PgTableOptions>>,
    },
    Parquet {
        path: PathBuf,
    },
    /// The full file-destination vocabulary — format (parquet|jsonl),
    /// location (local | s3), partition_by. The `parquet:` spelling above
    /// stays frozen (≡ file: local parquet).
    File {
        path: String,
        format: Option<rdlt::connector::file::dest::DestFormat>,
        location: Option<rdlt::connector::file::location::LocationOptions>,
        partition_by: Option<String>,
    },
    /// The Iceberg destination — the crate's full config vocabulary inline.
    Iceberg(Box<rdlt::connector::iceberg::IcebergConfig>),
}

/// Timestamped event log of one run.
#[derive(Debug)]
pub struct RunOutcome {
    pub report: RunReport,
    pub events: Vec<(u64, PipelineEvent)>,
}

fn err(e: impl std::fmt::Display) -> BenchError {
    BenchError(e.to_string())
}

async fn drive(spec: Spec) -> Result<RunOutcome> {
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
                    extensions,
                    settings,
                } => {
                    let mut dest =
                        rdlt::connector::duckdb::dest::DuckDb::open(path).map_err(err)?;
                    for ext in extensions.iter().flatten() {
                        dest = dest.extension(ext).map_err(err)?;
                    }
                    for (key, value) in settings.iter().flatten() {
                        dest = dest.setting(key, value).map_err(err)?;
                    }
                    if let Some(limit) = memory_limit {
                        dest = dest.memory_limit(limit).map_err(err)?;
                    }
                    if merge_strategy.is_some() || tables.is_some() {
                        let options = rdlt::connector::duckdb::dest::DestOptions {
                            merge_strategy: *merge_strategy,
                            tables: tables.clone().unwrap_or_default(),
                        };
                        dest = dest.options(options).map_err(err)?;
                    }
                    builder.destination(dest).build().map_err(err)?
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
                        dest = dest.options(options).map_err(err)?;
                    }
                    builder.destination(dest).build().map_err(err)?
                }
                DestSpec::Parquet { path } => {
                    let dest = rdlt::connector::file::ParquetDir::open(path).map_err(err)?;
                    builder.destination(dest).build().map_err(err)?
                }
                DestSpec::File {
                    path,
                    format,
                    location,
                    partition_by,
                } => {
                    let mut config = rdlt::connector::file::dest::FileDestConfig::new(path.clone());
                    if let Some(format) = format {
                        config = config.with_format(*format);
                    }
                    if let Some(location) = location {
                        config = config.with_location(location.clone());
                    }
                    if let Some(column) = partition_by {
                        config = config.with_partition_by(column.clone());
                    }
                    let dest =
                        rdlt::connector::file::dest::FileDest::from_config(config).map_err(err)?;
                    builder.destination(dest).build().map_err(err)?
                }
                DestSpec::Iceberg(config) => {
                    let dest =
                        rdlt::connector::iceberg::IcebergDest::from_config((**config).clone())
                            .map_err(err)?;
                    builder.destination(dest).build().map_err(err)?
                }
            };

            let mut stream = pipeline
                .events()
                .ok_or_else(|| BenchError("pipeline already ran".into()))?;
            let started = Instant::now();
            let collector = tokio::spawn(async move {
                let mut log = Vec::new();
                while let Some(event) = stream.recv().await {
                    log.push((started.elapsed().as_millis() as u64, event));
                }
                log
            });
            let report = pipeline.run().await.map_err(err)?;
            drop(pipeline); // close the broadcast sender so the collector ends
            let events = collector.await.map_err(err)?;
            Ok(RunOutcome { report, events })
        }};
    }

    let is_json = |path: &PathBuf| {
        path.extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("json"))
    };
    match &spec.source {
        SourceSpec::Rest { config } => {
            let text = std::fs::read_to_string(config).map_err(err)?;
            let source = if is_json(config) {
                rdlt::connector::rest::RestSource::from_json(&text)
            } else {
                rdlt::connector::rest::RestSource::from_yaml(&text)
            }
            .map_err(err)?;
            run_with!(source)
        }
        SourceSpec::File { config } => {
            let text = std::fs::read_to_string(config).map_err(err)?;
            let source = if is_json(config) {
                rdlt::connector::file::FileSource::from_json(&text)
            } else {
                rdlt::connector::file::FileSource::from_yaml(&text)
            }
            .map_err(err)?;
            run_with!(source)
        }
        SourceSpec::Postgres(pg) => {
            let parsed = match pg {
                PgSourceSpec::File(f) => {
                    let text = std::fs::read_to_string(&f.config).map_err(err)?;
                    if is_json(&f.config) {
                        rdlt::connector::postgres::source::PostgresConfig::from_json(&text)
                    } else {
                        rdlt::connector::postgres::source::PostgresConfig::from_yaml(&text)
                    }
                    .map_err(err)?
                }
                // Same rule as the CLI: untagged inline bypasses document
                // validation, so route through the shared from_value gate.
                PgSourceSpec::Inline(inline) => {
                    let value = serde_json::to_value(inline).map_err(err)?;
                    rdlt::connector::postgres::source::PostgresConfig::from_value(value)
                        .map_err(err)?
                }
            };
            let source = rdlt::connector::postgres::source::PostgresSource::new(parsed);
            run_with!(source)
        }
    }
}

/// One library-mode run: render the template, run in-process, log events.
pub fn run_once(
    cell: &Cell,
    subs: &BTreeMap<String, String>,
    paths: &Paths,
    run_dir: &Path,
) -> Result<Sample<RunOutcome>> {
    let template = cell
        .pipeline
        .as_ref()
        .ok_or_else(|| BenchError(format!("library cell `{}` needs `pipeline`", cell.id)))?;
    let mut subs = subs.clone();
    subs.insert("workdir".into(), run_dir.display().to_string());
    let raw = std::fs::read_to_string(paths.benches.join(template)).map_err(|e| {
        BenchError(format!(
            "cell `{}`: reading {}: {e}",
            cell.id,
            template.display()
        ))
    })?;
    let spec: Spec = serde_yaml::from_str(&substitute(&raw, &subs))
        .map_err(|e| BenchError(format!("cell `{}`: parsing pipeline spec: {e}", cell.id)))?;

    let runtime = tokio::runtime::Runtime::new().map_err(err)?;
    let started = Instant::now();
    let outcome = runtime.block_on(drive(spec))?;
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(Sample {
        wall_ms,
        detail: outcome,
    })
}

/// Attribution: BatchLoaded rows/bytes roll up to the stream whose name
/// prefixes the table (children like `events__tags` land on `events`).
fn attribute(events: &[(u64, PipelineEvent)]) -> Vec<StreamAttribution> {
    let mut streams: Vec<StreamAttribution> = Vec::new();
    for (at, event) in events {
        match event {
            PipelineEvent::StreamStarted { stream } => {
                if !streams.iter().any(|s| s.stream == stream.as_str()) {
                    streams.push(StreamAttribution {
                        stream: stream.as_str().to_owned(),
                        first_batch_ms: None,
                        last_batch_ms: None,
                        finished_ms: None,
                        rows: 0,
                        bytes: 0,
                    });
                }
            }
            PipelineEvent::BatchLoaded { table, rows, bytes } => {
                if let Some(s) = streams
                    .iter_mut()
                    .filter(|s| table.as_str().starts_with(s.stream.as_str()))
                    .max_by_key(|s| s.stream.len())
                {
                    s.first_batch_ms.get_or_insert(*at);
                    s.last_batch_ms = Some(*at);
                    s.rows += rows;
                    s.bytes += bytes;
                }
            }
            PipelineEvent::StreamFinished { stream } => {
                if let Some(s) = streams.iter_mut().find(|s| s.stream == stream.as_str()) {
                    s.finished_ms = Some(*at);
                }
            }
            _ => {}
        }
    }
    streams
}

pub fn side_from(samples: &[Sample<RunOutcome>]) -> RdltSide {
    let runs_ms: Vec<f64> = samples.iter().map(|s| s.wall_ms).collect();
    let median_ms = protocol::median(&runs_ms);
    let p95_ms = protocol::p95(&runs_ms);
    let last = samples.last().expect("protocol guarantees >= 1 run");
    let rows: u64 = last.detail.report.tables.values().map(|t| t.rows).sum();
    let bytes: u64 = last.detail.report.tables.values().map(|t| t.bytes).sum();
    let secs = median_ms / 1000.0;
    let note = "in-process run — CPU/RSS are subprocess-mode metrics, null here";
    RdltSide {
        median_ms,
        p95_ms,
        rows: Some(rows),
        bytes: Some(bytes),
        rows_per_s: Some(rows as f64 / secs),
        mb_per_s: Some(bytes as f64 / (1024.0 * 1024.0) / secs),
        cpu: CpuStats {
            note: Some(note.into()),
            ..CpuStats::default()
        },
        rss: RssStats {
            peak_bytes: None,
            note: Some(note.into()),
        },
        streams: attribute(&last.detail.events),
        runs_ms,
    }
}

pub fn verify_from(cell: &Cell, samples: &[Sample<RunOutcome>]) -> Result<Option<VerifyOutcome>> {
    let Some(verify) = &cell.verify else {
        return Ok(None);
    };
    let report = &samples
        .last()
        .expect("protocol guarantees >= 1 run")
        .detail
        .report;
    let actual = report
        .tables
        .iter()
        .find(|(table, _)| table.as_str() == verify.table)
        .map_or(0, |(_, t)| t.rows);
    if actual != verify.expected_rows {
        return Err(BenchError(format!(
            "cell `{}`: verify FAILED — table `{}` has {actual} rows, expected {}",
            cell.id, verify.table, verify.expected_rows
        )));
    }
    Ok(Some(VerifyOutcome {
        table: verify.table.clone(),
        expected_rows: verify.expected_rows,
        actual_rows: actual,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every document in the shared parity fixture must parse as a Spec.
    /// The CLI pins the SAME file against its own spec parser, so a
    /// destination or source kind added to one parser without the other
    /// fails one of the two pins instead of drifting silently.
    #[test]
    fn shared_parity_specs_all_parse() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../benches/parity_specs.yaml"
        ));
        let mut parsed = 0usize;
        for document in serde_yaml::Deserializer::from_str(raw) {
            let spec = Spec::deserialize(document).expect("parity spec parses in library mode");
            assert!(!spec.pipeline.is_empty());
            parsed += 1;
        }
        assert_eq!(parsed, 5, "fixture covers every destination kind");
    }

    /// A tiny file→parquet pipeline in-process, asserting non-estimated
    /// totals and attribution ordering.
    #[test]
    fn in_process_run_reports_exact_totals_and_attribution() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        std::fs::write(
            data.join("rows.jsonl"),
            "{\"id\":1,\"tags\":[{\"label\":\"a\"},{\"label\":\"b\"}]}\n{\"id\":2,\"tags\":[{\"label\":\"c\"}]}\n",
        )
        .unwrap();
        std::fs::write(
            data.join("files.yaml"),
            format!(
                "streams:\n  - name: events\n    format: jsonl\n    path: \"{}/rows.jsonl\"\n",
                data.display()
            ),
        )
        .unwrap();
        let template = data.join("pipe.yaml");
        std::fs::write(
            &template,
            format!(
                "pipeline: lib-test\nworkdir: {{{{workdir}}}}/.rdlt\nsource:\n  file: {{config: {}/files.yaml}}\ndestination:\n  parquet: {{path: {{{{workdir}}}}/out}}\n",
                data.display()
            ),
        )
        .unwrap();

        let cell = crate::cells::Cell {
            id: "lib-test".into(),
            class: crate::cells::Class::Scoreboard,
            mode: crate::cells::Mode::Library,
            fixture: "none".into(),
            pipeline: Some(template.clone()),
            command: None,
            prepare_sh: None,
            timing: crate::cells::Timing::Wall,
            workload: BTreeMap::new(),
            warmups: 0,
            runs: 2,
            competitors: vec![],
            verify: Some(crate::cells::Verify {
                table: "events".into(),
                expected_rows: 2,
            }),
            suite: "test".into(),
        };
        // benches dir join with an absolute template path stays absolute.
        let paths = Paths {
            repo: PathBuf::from("/"),
            benches: PathBuf::from("/"),
            cells_dir: PathBuf::from("/"),
            fixtures_toml: PathBuf::from("/"),
            bars_toml: PathBuf::from("/"),
            results: PathBuf::from("/"),
            cli: PathBuf::from("/nonexistent"),
        };

        let subs = BTreeMap::new();
        let mut samples = Vec::new();
        for i in 0..2 {
            let run_dir = dir.path().join(format!("run{i}"));
            std::fs::create_dir_all(&run_dir).unwrap();
            samples.push(run_once(&cell, &subs, &paths, &run_dir).unwrap());
        }

        let side = side_from(&samples);
        // 2 parent rows + 3 shredded children — exact, from the RunReport.
        assert_eq!(side.rows, Some(5));
        assert!(side.bytes.unwrap() > 0);
        assert!(side.rows_per_s.unwrap() > 0.0);
        // resource metrics explicitly null in library mode
        assert!(side.cpu.mean_util.is_none());
        assert!(side.rss.peak_bytes.is_none());
        // attribution: the events stream saw batches, then finished
        let events = side
            .streams
            .iter()
            .find(|s| s.stream == "events")
            .expect("attributed");
        assert_eq!(events.rows, 5);
        assert!(events.first_batch_ms.is_some());
        assert!(events.finished_ms.is_some());
        // Causal order that actually holds: first batch <= last batch. The
        // source-side StreamFinished may precede the destination's tail load.
        assert!(events.first_batch_ms <= events.last_batch_ms);

        // verify passes on exact totals (a mismatch would have been an Err)
        let outcome = verify_from(&cell, &samples).unwrap().unwrap();
        assert_eq!(outcome.actual_rows, outcome.expected_rows);
    }
}
