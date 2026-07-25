//! Versioned result artifacts. One committed
//! JSON per cell under `benches/results/`; raw sampler series under
//! `results/raw/` (gitignored). Serde-stable: `format_version` gates readers.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{BenchError, Result};

/// The archive commit where every retired v1 cell/fixture/artifact/bar stays
/// checkout-able (feature 018 migration, BR1). Named in the v1-rejection
/// message so a stale artifact points a reader at its recorded history.
pub const ARCHIVE_COMMIT: &str = "40841ab";

pub const ARTIFACT_FORMAT_VERSION: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Fingerprint {
    pub cpu_model: String,
    pub kernel: String,
    pub rustc: String,
    /// Every competitor that ran, keyed by variant id → pin (e.g.
    /// `{"dlt": "dlt 1.29.0"}`) — not only the first. Empty when no competitor
    /// ran.
    #[serde(default)]
    pub competitor_pins: BTreeMap<String, String>,
    pub dataset_hashes: BTreeMap<String, String>,
    pub loadavg_at_start: f64,
    /// Present when the quiet-machine guard annotated the run.
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RdltSide {
    pub runs_ms: Vec<f64>,
    pub median_ms: f64,
    pub p95_ms: f64,
    /// From the RunReport's own accounting — never estimated.
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
    pub rows_per_s: Option<f64>,
    pub mb_per_s: Option<f64>,
    pub cpu: CpuStats,
    pub rss: RssStats,
    /// Bytes this arm LEFT BEHIND — the size of the output it wrote, not the
    /// `bytes` above, which is what it moved.
    ///
    /// The distinction is the point. Two arms can move identical rows and
    /// write wildly different artifacts: rdlt wrote 210.0 MB where dlt wrote
    /// 73.7 MB for the same million rows, because one compressed and the
    /// other did not. A timing comparison between them was comparing
    /// different work, and nothing in the artifact said so.
    ///
    /// Measured per arm by the cell's `artifact_bytes_sh`, absent when the
    /// cell declares none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_bytes: Option<u64>,
}

/// One competitor arm's outcome. The size skew between the variants is
/// accepted: a handful of these exist per artifact, never large collections.
#[allow(clippy::large_enum_variant)]
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
        /// Driver pass-through, carried verbatim from the summary line's
        /// `extra{}` object (e.g. a platform-reported `sync_s` as labeled
        /// context next to the headline wall). Absent for container runs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        extra: Option<serde_json::Value>,
        /// Bytes this arm left behind — see [`RdltSide::artifact_bytes`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        artifact_bytes: Option<u64>,
    },
    /// A competitor that could not run — recorded explicitly, never silently
    /// skipped.
    Missing { reason: String },
}

/// The verified destination shape: every table the cell declared, mapped to the
/// row count it actually held. `verify_outcome` returns an error (never this)
/// on any mismatch, so a recorded outcome is always a full match.
///
/// The whole SET is recorded, not just one table, because the set is the
/// evidence: two arms are comparable only if they moved the same tables, and a
/// record holding a single table's count cannot show that.
pub type VerifyOutcome = std::collections::BTreeMap<String, u64>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Artifact {
    pub format_version: u32,
    pub cell_id: String,
    /// ISO date of the session (from `date -I` at run time — the harness has
    /// no clock dependency worth adding for this).
    pub recorded_at: String,
    pub fingerprint: Fingerprint,
    pub workload: BTreeMap<String, toml::Value>,
    pub rdlt: RdltSide,
    #[serde(default)]
    pub competitors: BTreeMap<String, CompetitorSide>,
    pub verify: Option<VerifyOutcome>,
    /// Set when `RDLT_BENCH_FORCE=1` overrode the quiet guard on a loaded
    /// machine — the number is context, not evidence, and says so.
    #[serde(default)]
    pub forced: bool,
    /// Driver pass-through context (e.g. `sync_s` for the Airbyte kind),
    /// carried verbatim into the record. Empty for container/rdlt runs.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

pub fn fingerprint(
    dataset_hashes: BTreeMap<String, String>,
    competitor_pins: BTreeMap<String, String>,
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
        competitor_pins,
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
    // Read the version before full deserialization: a retired v1 artifact must
    // fail loudly and point at its archived history, not error on a since-removed
    // field with a confusing serde message.
    let version = serde_json::from_str::<serde_json::Value>(&raw)
        .ok()
        .and_then(|v| v.get("format_version").and_then(serde_json::Value::as_u64));
    if version != Some(ARTIFACT_FORMAT_VERSION as u64) {
        return Err(BenchError(format!(
            "artifact {} is format v{} (this harness reads v{ARTIFACT_FORMAT_VERSION} only); \
             re-record it with a measurement session — v1 history lives at commit \
             {ARCHIVE_COMMIT}, and v2 artifacts predate the delivered-vs-declared \
             table check, so their timings may cover tables the cell never declared",
            path.display(),
            version.map_or_else(|| "?".to_owned(), |v| v.to_string()),
        )));
    }
    let artifact: Artifact = serde_json::from_str(&raw)
        .map_err(|e| BenchError(format!("parsing {}: {e}", path.display())))?;
    Ok(artifact)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn minimal(cell_id: &str) -> Artifact {
        Artifact {
            format_version: ARTIFACT_FORMAT_VERSION,
            cell_id: cell_id.into(),
            recorded_at: "2026-07-24".into(),
            fingerprint: Fingerprint {
                cpu_model: "test-cpu".into(),
                kernel: "6.0".into(),
                rustc: "rustc test".into(),
                competitor_pins: BTreeMap::new(),
                dataset_hashes: BTreeMap::new(),
                loadavg_at_start: 0.1,
                quiet_note: None,
            },
            workload: BTreeMap::new(),
            rdlt: RdltSide {
                artifact_bytes: None,
                runs_ms: vec![10.0, 11.0, 12.0],
                median_ms: 11.0,
                p95_ms: 12.0,
                rows: Some(100),
                bytes: Some(1000),
                rows_per_s: Some(9090.9),
                mb_per_s: Some(0.09),
                cpu: CpuStats::default(),
                rss: RssStats::default(),
            },
            competitors: BTreeMap::new(),
            verify: None,
            forced: false,
            extra: serde_json::Map::new(),
        }
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let mut art = minimal("rt-cell");
        art.competitors.insert(
            "dlt".into(),
            CompetitorSide::Missing {
                reason: "image not built".into(),
            },
        );
        write(dir.path(), &art).unwrap();
        let back = read(dir.path(), "rt-cell").unwrap();
        assert_eq!(art, back);
    }

    #[test]
    fn v1_artifact_is_refused_naming_the_archive_commit() {
        // A retired v1 artifact (any pre-migration shape) must fail loudly and
        // point the reader at the archived history, never half-parse.
        let dir = tempfile::tempdir().unwrap();
        // A v1-shaped artifact: the reader rejects on `format_version` before
        // touching the since-removed fields, so the discriminator is enough.
        std::fs::write(
            dir.path().join("old-cell.json"),
            r#"{"format_version":1,"cell_id":"old-cell"}"#,
        )
        .unwrap();
        let err = read(dir.path(), "old-cell").unwrap_err().to_string();
        assert!(err.contains("format v1"), "{err}");
        assert!(err.contains(ARCHIVE_COMMIT), "{err}");
    }

    #[test]
    fn future_format_version_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut art = minimal("v99-cell");
        art.format_version = 99;
        write(dir.path(), &art).unwrap();
        let err = read(dir.path(), "v99-cell").unwrap_err().to_string();
        assert!(err.contains("format v99"), "{err}");
    }

    #[test]
    fn missing_artifact_names_the_cell() {
        let dir = tempfile::tempdir().unwrap();
        let err = read(dir.path(), "ghost").unwrap_err().to_string();
        assert!(err.contains("ghost"), "{err}");
    }

    #[test]
    fn fingerprint_reads_the_live_machine() {
        let pins = BTreeMap::from([("dlt".to_owned(), "dlt 1.29.0".to_owned())]);
        let fp = fingerprint(BTreeMap::new(), pins, None);
        assert!(!fp.kernel.is_empty());
        assert!(fp.loadavg_at_start >= 0.0);
        assert_eq!(
            fp.competitor_pins.get("dlt").map(String::as_str),
            Some("dlt 1.29.0")
        );
    }
}
