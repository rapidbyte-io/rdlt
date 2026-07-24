//! Library-mode runs: the pipeline in-process via the `rdlt` crate. RunReport
//! gives EXACT rows/bytes; the `events()` seam gives per-stream attribution.
//! Scoreboard detail only — gated numbers bind to the subprocess mode, which
//! is the measured, release-CLI configuration. CPU/RSS are subprocess-mode
//! metrics: in-process /proc/self readings would accumulate across runs, so
//! they are null here with a stated reason — a metric is never fabricated;
//! an absent number is null with a reason.
//!
//! The pipeline templates this harness renders are the SAME documents the
//! subprocess mode feeds the CLI, so both parse them through the one shared
//! model, [`rdlt::pipeline_spec`]. The shared fixture
//! `benches/parity_specs.yaml` pins that model from both consumers.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use rdlt::pipeline_spec::{self, Spec};
use rdlt::prelude::*;

use crate::artifact::{CpuStats, RdltSide, RssStats, StreamAttribution, VerifyOutcome};
use crate::cells::Cell;
use crate::paths::Paths;
use crate::protocol::{self, Sample};
use crate::template::substitute;
use crate::{BenchError, Result};

/// Timestamped event log of one run.
#[derive(Debug)]
pub struct RunOutcome {
    pub report: RunReport,
    pub events: Vec<(u64, PipelineEvent)>,
}

fn err(e: impl std::fmt::Display) -> BenchError {
    BenchError(e.to_string())
}

/// Build the pipeline from the shared spec model, then run it in-process while
/// a collector timestamps every event (the attribution detail scoreboards use).
async fn drive(spec: Spec) -> Result<RunOutcome> {
    let pipeline = pipeline_spec::build_pipeline(&spec).map_err(err)?;
    let mut stream = pipeline.events();
    let started = Instant::now();
    let collector = tokio::spawn(async move {
        let mut log = Vec::new();
        while let Some(event) = stream.recv().await {
            log.push((started.elapsed().as_millis() as u64, event));
        }
        log
    });
    // `run` consumes the pipeline, dropping the event sender, which ends the collector.
    let report = pipeline.run().await.map_err(err)?;
    let events = collector.await.map_err(err)?;
    Ok(RunOutcome { report, events })
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
    use std::path::PathBuf;

    use super::*;
    use serde::Deserialize;

    /// Every document in the shared parity fixture must parse as a Spec. The
    /// CLI pins the SAME file against the SAME shared model
    /// (`rdlt::pipeline_spec`), so this fixture is exercised from both
    /// consumers — a destination or source kind added here forces both.
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
