//! Versioned result artifacts (data-model.md §4, contract BH5). One committed
//! JSON per cell under `benches/results/`; raw sampler series under
//! `results/raw/` (gitignored). Serde-stable: `format_version` gates readers.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{BenchError, Result};

pub const ARTIFACT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Fingerprint {
    pub cpu_model: String,
    pub kernel: String,
    pub rustc: String,
    /// Competitor pin (e.g. "dlt 1.29.0") when any competitor ran.
    pub competitor_pin: Option<String>,
    pub dataset_hashes: BTreeMap<String, String>,
    pub loadavg_at_start: f64,
    /// Present when the quiet-machine guard annotated the run (BH2).
    pub quiet_note: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CpuStats {
    pub mean_util: Option<f64>,
    pub peak_util: Option<f64>,
    pub user_sys_ms: Option<u64>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RssStats {
    pub peak_bytes: Option<u64>,
    pub note: Option<String>,
}

/// Per-stream attribution (library mode only; from the events seam).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamAttribution {
    pub stream: String,
    pub first_batch_ms: Option<u64>,
    pub last_batch_ms: Option<u64>,
    /// Source-side finish (`StreamFinished`) — can PRECEDE `last_batch_ms`:
    /// the source drains before the destination loads its tail.
    pub finished_ms: Option<u64>,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RdltSide {
    pub runs_ms: Vec<f64>,
    pub median_ms: f64,
    pub p95_ms: f64,
    /// From the RunReport's own accounting — never estimated (BH3).
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
    pub rows_per_s: Option<f64>,
    pub mb_per_s: Option<f64>,
    pub cpu: CpuStats,
    pub rss: RssStats,
    #[serde(default)]
    pub streams: Vec<StreamAttribution>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CompetitorSide {
    Ok {
        runs_ms: Vec<f64>,
        median_ms: f64,
        /// Baselines self-time in-process (continuity with every recorded
        /// multiple); the harness records that number, not its own clock.
        self_timed: bool,
        cpu: CpuStats,
        rss: RssStats,
        /// competitor_median / rdlt_median — >1 means rdlt is faster.
        ratio_vs_rdlt: Option<f64>,
    },
    /// BH4: never silently skipped.
    Missing { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VerifyOutcome {
    pub table: String,
    pub expected_rows: u64,
    pub actual_rows: u64,
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Artifact {
    pub format_version: u32,
    pub cell_id: String,
    pub class: crate::cells::Class,
    pub mode: crate::cells::Mode,
    pub suite: String,
    /// ISO date of the session (from `date -I` at run time — the harness has
    /// no clock dependency worth adding for this).
    pub recorded_at: String,
    pub fingerprint: Fingerprint,
    pub workload: BTreeMap<String, toml::Value>,
    pub rdlt: RdltSide,
    #[serde(default)]
    pub competitors: BTreeMap<String, CompetitorSide>,
    pub verify: Option<VerifyOutcome>,
}

pub fn fingerprint(
    dataset_hashes: BTreeMap<String, String>,
    competitor_pin: Option<String>,
    quiet_note: Option<String>,
) -> Fingerprint {
    let cpu_model = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .and_then(|l| l.split(':').nth(1))
                .map(|m| m.trim().to_owned())
        })
        .unwrap_or_else(|| "unknown".into());
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|_| "unknown".into());
    let rustc = std::process::Command::new("rustc")
        .arg("-V")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|| "unknown".into());
    Fingerprint {
        cpu_model,
        kernel,
        rustc,
        competitor_pin,
        dataset_hashes,
        loadavg_at_start: crate::protocol::loadavg_1min().unwrap_or(-1.0),
        quiet_note,
    }
}

pub fn recorded_at() -> String {
    std::process::Command::new("date")
        .arg("-I")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|| "unknown".into())
}

pub fn write(results_dir: &Path, artifact: &Artifact) -> Result<std::path::PathBuf> {
    std::fs::create_dir_all(results_dir)?;
    let path = results_dir.join(format!("{}.json", artifact.cell_id));
    let json = serde_json::to_string_pretty(artifact)
        .map_err(|e| BenchError(format!("serializing artifact: {e}")))?;
    std::fs::write(&path, json + "\n")?;
    Ok(path)
}

pub fn read(results_dir: &Path, cell_id: &str) -> Result<Artifact> {
    let path = results_dir.join(format!("{cell_id}.json"));
    let raw = std::fs::read_to_string(&path).map_err(|e| {
        BenchError(format!(
            "no artifact for `{cell_id}` ({}): {e}",
            path.display()
        ))
    })?;
    let artifact: Artifact = serde_json::from_str(&raw)
        .map_err(|e| BenchError(format!("parsing {}: {e}", path.display())))?;
    if artifact.format_version != ARTIFACT_FORMAT_VERSION {
        return Err(BenchError(format!(
            "{}: format_version {} (this harness reads {ARTIFACT_FORMAT_VERSION})",
            path.display(),
            artifact.format_version
        )));
    }
    Ok(artifact)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::cells::{Class, Mode};

    pub(crate) fn minimal(cell_id: &str, class: Class) -> Artifact {
        Artifact {
            format_version: ARTIFACT_FORMAT_VERSION,
            cell_id: cell_id.into(),
            class,
            mode: Mode::Subprocess,
            suite: "test".into(),
            recorded_at: "2026-07-21".into(),
            fingerprint: Fingerprint {
                cpu_model: "test-cpu".into(),
                kernel: "6.0".into(),
                rustc: "rustc test".into(),
                competitor_pin: None,
                dataset_hashes: BTreeMap::new(),
                loadavg_at_start: 0.1,
                quiet_note: None,
            },
            workload: BTreeMap::new(),
            rdlt: RdltSide {
                runs_ms: vec![10.0, 11.0, 12.0],
                median_ms: 11.0,
                p95_ms: 12.0,
                rows: Some(100),
                bytes: Some(1000),
                rows_per_s: Some(9090.9),
                mb_per_s: Some(0.09),
                cpu: CpuStats::default(),
                rss: RssStats::default(),
                streams: vec![],
            },
            competitors: BTreeMap::new(),
            verify: None,
        }
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let mut art = minimal("rt-cell", Class::Scoreboard);
        art.competitors.insert(
            "dlt-pyarrow".into(),
            CompetitorSide::Missing {
                reason: "image not built".into(),
            },
        );
        write(dir.path(), &art).unwrap();
        let back = read(dir.path(), "rt-cell").unwrap();
        assert_eq!(art, back);
    }

    #[test]
    fn future_format_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut art = minimal("v2-cell", Class::Scoreboard);
        art.format_version = 99;
        write(dir.path(), &art).unwrap();
        let err = read(dir.path(), "v2-cell").unwrap_err().to_string();
        assert!(err.contains("format_version 99"), "{err}");
    }

    #[test]
    fn missing_artifact_names_the_cell() {
        let dir = tempfile::tempdir().unwrap();
        let err = read(dir.path(), "ghost").unwrap_err().to_string();
        assert!(err.contains("ghost"), "{err}");
    }

    #[test]
    fn fingerprint_reads_the_live_machine() {
        let fp = fingerprint(BTreeMap::new(), Some("dlt 1.29.0".into()), None);
        assert!(!fp.kernel.is_empty());
        assert!(fp.loadavg_at_start >= 0.0);
        assert_eq!(fp.competitor_pin.as_deref(), Some("dlt 1.29.0"));
    }
}
