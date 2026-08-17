//! Competitors: the baseline arms measured beside the product from the same
//! seeded fixtures. `variant` is the registry, `container` and `driver` the
//! two ways an arm executes, `summary` the one self-reported line both
//! print. Wall time is the baseline's own in-process timing — continuity
//! with every recorded multiple. A baseline that cannot run is a LOUD
//! `Missing{reason}`, never an error: the product side must still run.

pub mod container;
pub mod driver;
pub mod summary;
pub mod variant;

use std::collections::BTreeMap;

use crate::artifact::{CompetitorSide, CpuStats, RssStats};
use crate::cell::CompetitorRef;
use crate::competitor::variant::{Kind, Variant};
use crate::{fixture, measure};

/// One counted run of an arm, as its kind measured it.
pub(crate) struct Run {
    pub(crate) seconds: f64,
    pub(crate) cpu: CpuStats,
    pub(crate) rss: RssStats,
    pub(crate) extra: Option<serde_json::Value>,
}

/// Run one competitor reference for a cell: warmup-free (the baselines have
/// always been measured cold-process, warm-cache), N runs with every fixture
/// reset before each, medians. Run-count precedence: the cell's competitor
/// entry > the variant's own override > the cell default.
pub(crate) fn run(
    variant: &Variant,
    reference: &CompetitorRef,
    cell_runs: u32,
    subs: &BTreeMap<String, String>,
    fixtures: &[&fixture::Live],
) -> CompetitorSide {
    let runs = reference.runs.or(variant.runs).unwrap_or(cell_runs).max(1);
    match measure(variant, reference, runs, subs, fixtures) {
        Ok(side) => side,
        Err(reason) => CompetitorSide::Missing { reason },
    }
}

fn measure(
    variant: &Variant,
    reference: &CompetitorRef,
    runs: u32,
    subs: &BTreeMap<String, String>,
    fixtures: &[&fixture::Live],
) -> std::result::Result<CompetitorSide, String> {
    let engine = match variant.kind {
        Kind::Container => Some(container::preflight(variant)?),
        Kind::Driver => driver::preflight(variant).map(|()| None)?,
    };
    let mut self_timed_ms = Vec::with_capacity(runs as usize);
    let mut last: Option<Run> = None;
    for seq in 0..runs {
        // Same discipline as the product side: every store the cell uses is
        // reset between runs, source and destination alike.
        for fixture in fixtures {
            fixture
                .reset()
                .map_err(|e| format!("fixture reset failed: {e}"))?;
        }
        let run = match &engine {
            Some(engine) => container::run_once(engine, variant, reference, subs, seq),
            None => driver::run_once(variant, reference, subs),
        }
        .map_err(|e| e.to_string())?;
        self_timed_ms.push(run.seconds * 1000.0);
        last = Some(run);
    }
    let last = last.expect("runs >= 1");
    Ok(CompetitorSide::Ok {
        median_ms: measure::median(&self_timed_ms),
        runs_ms: self_timed_ms,
        self_timed: true,
        cpu: last.cpu,
        rss: last.rss,
        ratio_vs_rdlt: None,  // filled once the rdlt median exists
        artifact_bytes: None, // filled by the caller, which knows the arm's sizer
        extra: last.extra,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(variant: &str, runs: Option<u32>) -> CompetitorRef {
        CompetitorRef {
            artifact_bytes_sh: None,
            variant: variant.into(),
            args: vec!["x.py".into()],
            mounts: vec![],
            network: None,
            runs,
        }
    }

    fn driver_variant(dir: &std::path::Path, prerequisite_sh: Option<&str>) -> Variant {
        Variant {
            id: "airbyte".into(),
            pin: "airbyte 2.1.1".into(),
            kind: Kind::Driver,
            image: None,
            driver: Some(dir.join("driver.py")),
            prerequisite_sh: prerequisite_sh.map(str::to_owned),
            runs: None,
            module_dir: dir.to_path_buf(),
        }
    }

    #[test]
    fn missing_image_is_loud_not_silent() {
        let variant = Variant {
            id: "ghost".into(),
            pin: "dlt 0.0.0".into(),
            kind: Kind::Container,
            image: Some("rdlt-bench-definitely-not-built".into()),
            driver: None,
            prerequisite_sh: None,
            runs: None,
            module_dir: ".".into(),
        };
        // With no engine OR no image, both paths must produce Missing{reason}.
        let fixture = fixture::start(&fixture::tests::none_def("none"), &BTreeMap::new()).unwrap();
        let side = run(
            &variant,
            &reference("ghost", None),
            1,
            &BTreeMap::new(),
            &[&fixture],
        );
        match side {
            CompetitorSide::Missing { reason } => {
                assert!(!reason.is_empty());
            }
            CompetitorSide::Ok { .. } => panic!("ghost image cannot run"),
        }
    }

    #[test]
    fn failed_prerequisite_is_missing_with_the_probe_output_as_reason() {
        let dir = tempfile::tempdir().unwrap();
        let variant = driver_variant(dir.path(), Some("echo 'abctl cluster not running'; exit 1"));
        let side = run(
            &variant,
            &reference("airbyte", None),
            1,
            &BTreeMap::new(),
            &[],
        );
        match side {
            CompetitorSide::Missing { reason } => {
                assert!(
                    reason.contains("prerequisite failed") && reason.contains("abctl cluster"),
                    "{reason}"
                );
            }
            CompetitorSide::Ok { .. } => panic!("failed prerequisite cannot measure"),
        }
    }

    #[test]
    fn driver_summary_line_yields_seconds_rss_and_extra_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("driver.py"),
            "import json\nprint('noise')\nprint(json.dumps({'seconds': 42.5, 'rows': 5, 'peak_rss_kb': 1024, 'extra': {'sync_s': 40.0}}))\n",
        )
        .unwrap();
        let variant = driver_variant(dir.path(), None);
        let side = run(
            &variant,
            &reference("airbyte", Some(1)),
            5,
            &BTreeMap::new(),
            &[],
        );
        match side {
            CompetitorSide::Ok {
                runs_ms,
                median_ms,
                rss,
                extra,
                ..
            } => {
                assert_eq!(runs_ms.len(), 1); // the reference's runs override won
                assert_eq!(median_ms, 42500.0);
                assert_eq!(rss.peak_bytes, Some(1024 * 1024));
                assert_eq!(extra.unwrap()["sync_s"], 40.0);
            }
            CompetitorSide::Missing { reason } => panic!("driver run failed: {reason}"),
        }
    }
}
