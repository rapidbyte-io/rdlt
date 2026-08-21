//! Bars and their judgment: `benches/bars.toml` names a threshold per cell,
//! and the gate evaluates each against the cell's recorded artifact. A
//! violation names cell, bar, measured value and the bar's policy — the
//! sentence that says why the bar exists. A ratio bar over a MISSING
//! baseline FAILS: there is nothing to compare against, so the gate cannot
//! be satisfied. Only wall-median and peak-RSS bars exist; CPU and
//! throughput are recorded, not gated.

use std::fmt;
use std::path::Path;

use serde::Deserialize;

use crate::artifact::{self, Artifact, CompetitorSide};
use crate::cell::Cell;
use crate::error::{Error, Result, load_toml};
use crate::paths::Paths;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Kind {
    /// rdlt must be at least `min_ratio`× faster than the named competitor's
    /// wall median.
    RatioVs { competitor: String, min_ratio: f64 },
    /// Absolute wall-median bound on reference hardware — a competitor
    /// release can never flip it.
    AbsoluteMs { max_ms: f64 },
    /// rdlt peak RSS must be at most `max_rss_ratio` of the competitor's.
    RssRatioVs {
        competitor: String,
        max_rss_ratio: f64,
    },
}

// No `deny_unknown_fields` here: serde cannot combine it with `flatten` (the
// tag+payload fields would register as unknown). The flattened `Kind` enum
// still rejects unknown fields inside each variant.
#[derive(Debug, Clone, Deserialize)]
pub struct Bar {
    pub cell: String,
    #[serde(flatten)]
    pub kind: Kind,
    /// Jitter allowance (percent) before a violation is declared.
    #[serde(default)]
    pub tolerance_pct: f64,
    /// Why the bar exists — the recorded session floors it was set below.
    /// Printed with every failing verdict so a violation reads with its
    /// provenance.
    pub policy: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BarsFile {
    #[serde(default, rename = "bar")]
    bars: Vec<Bar>,
}

pub fn load(path: &Path) -> Result<Vec<Bar>> {
    let file: BarsFile = load_toml(path)?;
    Ok(file.bars)
}

/// Every bar references an existing cell. Enforcement is measurement-first —
/// a bar is added only after a recorded session — so the only structural
/// rule is that its cell exists.
pub fn cross_validate(cells: &[Cell], bars: &[Bar]) -> Result<()> {
    for bar in bars {
        if !cells.iter().any(|c| c.id == bar.cell) {
            return Err(Error(format!(
                "bars.toml names unknown cell `{}`",
                bar.cell
            )));
        }
    }
    Ok(())
}

/// One bar's judgment over one artifact. `Fail` carries the bar's policy so
/// the printed line says why the bar exists.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Verdict {
    Pass { detail: String },
    Fail { detail: String, policy: String },
}

impl Verdict {
    pub(crate) fn passed(&self) -> bool {
        matches!(self, Verdict::Pass { .. })
    }
}

/// The verdict line: `[PASS] detail`, or `[FAIL] detail — policy: …`. The
/// one place the tag wording lives — `gate` and the run summary print
/// through here.
impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verdict::Pass { detail } => write!(f, "[PASS] {detail}"),
            Verdict::Fail { detail, policy } => write!(f, "[FAIL] {detail} — policy: {policy}"),
        }
    }
}

fn competitor_median(artifact: &Artifact, competitor: &str) -> std::result::Result<f64, String> {
    match artifact.competitors.get(competitor) {
        Some(CompetitorSide::Ok { median_ms, .. }) => Ok(*median_ms),
        Some(CompetitorSide::Missing { reason }) => {
            Err(format!("baseline `{competitor}` MISSING ({reason})"))
        }
        None => Err(format!(
            "baseline `{competitor}` not present in the artifact"
        )),
    }
}

fn competitor_rss(artifact: &Artifact, competitor: &str) -> std::result::Result<u64, String> {
    match artifact.competitors.get(competitor) {
        Some(CompetitorSide::Ok { rss, .. }) => rss
            .peak_bytes
            .ok_or_else(|| format!("baseline `{competitor}` has no peak-RSS reading")),
        Some(CompetitorSide::Missing { reason }) => {
            Err(format!("baseline `{competitor}` MISSING ({reason})"))
        }
        None => Err(format!(
            "baseline `{competitor}` not present in the artifact"
        )),
    }
}

/// Evaluate one bar against its cell's artifact.
pub(crate) fn evaluate(bar: &Bar, artifact: &Artifact) -> Verdict {
    let tol = bar.tolerance_pct / 100.0;
    let cell = &bar.cell;
    let judge = |held: bool, detail: String| {
        if held {
            Verdict::Pass { detail }
        } else {
            Verdict::Fail {
                detail,
                policy: bar.policy.clone(),
            }
        }
    };
    match &bar.kind {
        Kind::RatioVs {
            competitor,
            min_ratio,
        } => match competitor_median(artifact, competitor) {
            Err(reason) => judge(
                false,
                format!("{cell}: ratio bar >= {min_ratio}x vs {competitor} — {reason}"),
            ),
            Ok(comp_ms) => {
                let ratio = comp_ms / artifact.rdlt.median_ms;
                let floor = min_ratio * (1.0 - tol);
                let detail = format!(
                    "{cell}: {ratio:.1}x vs {competitor} ({comp_ms:.0} ms / {:.0} ms), bar >= {min_ratio}x (tol {}%)",
                    artifact.rdlt.median_ms, bar.tolerance_pct
                );
                judge(ratio >= floor, detail)
            }
        },
        Kind::AbsoluteMs { max_ms } => {
            let measured = artifact.rdlt.median_ms;
            let ceiling = max_ms * (1.0 + tol);
            let detail = format!(
                "{cell}: {measured:.1} ms, bar <= {max_ms} ms absolute (tol {}%)",
                bar.tolerance_pct
            );
            judge(measured <= ceiling, detail)
        }
        Kind::RssRatioVs {
            competitor,
            max_rss_ratio,
        } => {
            let Some(rdlt_rss) = artifact.rdlt.rss.peak_bytes else {
                return judge(
                    false,
                    format!("{cell}: RSS bar but the rdlt side has no peak-RSS reading"),
                );
            };
            match competitor_rss(artifact, competitor) {
                Err(reason) => judge(
                    false,
                    format!(
                        "{cell}: RSS bar <= 1/{:.0} vs {competitor} — {reason}",
                        1.0 / max_rss_ratio
                    ),
                ),
                Ok(comp_rss) => {
                    let ratio = rdlt_rss as f64 / comp_rss as f64;
                    let ceiling = max_rss_ratio * (1.0 + tol);
                    let detail = format!(
                        "{cell}: peak RSS 1/{:.1} of {competitor} ({} MB / {} MB), bar <= 1/{:.0} (tol {}%)",
                        1.0 / ratio,
                        rdlt_rss / (1 << 20),
                        comp_rss / (1 << 20),
                        1.0 / max_rss_ratio,
                        bar.tolerance_pct
                    );
                    judge(ratio <= ceiling, detail)
                }
            }
        }
    }
}

/// Judge every bar against the recorded ledger; a barred cell with no
/// artifact fails its bar — nothing to compare against is a violation,
/// never a skip.
pub(crate) fn run(paths: &Paths, bars: &[Bar]) -> Vec<Verdict> {
    bars.iter()
        .map(|bar| match artifact::read(&paths.results, &bar.cell) {
            Ok(artifact) => evaluate(bar, &artifact),
            Err(e) => Verdict::Fail {
                detail: format!("{}: no artifact — {e}", bar.cell),
                policy: bar.policy.clone(),
            },
        })
        .collect()
}

pub(crate) fn passed(verdicts: &[Verdict]) -> bool {
    verdicts.iter().all(Verdict::passed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{CpuStats, RssStats};

    fn artifact_with(median_ms: f64, comp_ms: Option<f64>) -> Artifact {
        let mut artifact = crate::artifact::tests::minimal("cell-x");
        artifact.rdlt.median_ms = median_ms;
        if let Some(comp) = comp_ms {
            artifact.competitors.insert(
                "dlt-pyarrow".into(),
                CompetitorSide::Ok {
                    artifact_bytes: None,
                    runs_ms: vec![comp],
                    median_ms: comp,
                    self_timed: true,
                    cpu: CpuStats::default(),
                    rss: RssStats {
                        peak_bytes: Some(1000 << 20),
                        note: None,
                    },
                    ratio_vs_rdlt: None,
                    extra: None,
                },
            );
        }
        artifact
    }

    fn ratio_bar(min_ratio: f64, tolerance_pct: f64) -> Bar {
        Bar {
            cell: "cell-x".into(),
            kind: Kind::RatioVs {
                competitor: "dlt-pyarrow".into(),
                min_ratio,
            },
            tolerance_pct,
            policy: "test".into(),
        }
    }

    fn bars_file(dir: &tempfile::TempDir, body: &str) -> std::path::PathBuf {
        let p = dir.path().join("bars.toml");
        std::fs::write(&p, body).unwrap();
        p
    }

    const BAR_RATIO: &str = r#"
[[bar]]
cell = "a-cell"
kind = "ratio_vs"
competitor = "dlt-pyarrow"
min_ratio = 10.0
tolerance_pct = 3.0
policy = "two recorded sessions cleared 12x"
"#;

    #[test]
    fn bar_kinds_parse() {
        let dir = tempfile::tempdir().unwrap();
        let p = bars_file(
            &dir,
            &format!(
                "{BAR_RATIO}\n[[bar]]\ncell='b'\nkind='absolute_ms'\nmax_ms=40.0\npolicy='x'\n\n[[bar]]\ncell='c'\nkind='rss_ratio_vs'\ncompetitor='dlt-pyarrow'\nmax_rss_ratio=0.2\npolicy='y'\n"
            ),
        );
        let bars = load(&p).unwrap();
        assert_eq!(bars.len(), 3);
        assert!(
            matches!(bars[0].kind, Kind::RatioVs { ref competitor, min_ratio } if competitor == "dlt-pyarrow" && min_ratio == 10.0)
        );
        assert_eq!(bars[0].policy, "two recorded sessions cleared 12x");
        assert!(matches!(bars[1].kind, Kind::AbsoluteMs { max_ms } if max_ms == 40.0));
        assert!(
            matches!(bars[2].kind, Kind::RssRatioVs { max_rss_ratio, .. } if max_rss_ratio == 0.2)
        );
    }

    #[test]
    fn every_bar_must_reference_an_existing_cell() {
        let cells_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            cells_dir.path().join("a.toml"),
            "[[cell]]\nid='a-cell'\nfixtures=['none']\ncommand=['true']\n",
        )
        .unwrap();
        let cells = crate::cell::load(cells_dir.path()).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        // `a-cell` exists → a bar over it validates.
        let bars = load(&bars_file(&tmp, BAR_RATIO)).unwrap();
        cross_validate(&cells, &bars).unwrap();
        // A bar over an unknown cell is a loud load-time error.
        let mut ghost = bars.clone();
        ghost[0].cell = "ghost".into();
        let err = cross_validate(&cells, &ghost).unwrap_err().to_string();
        assert!(err.contains("unknown cell `ghost`"), "{err}");
    }

    #[test]
    fn ratio_bar_passes_and_a_tightened_bar_fails_naming_the_cell() {
        let artifact = artifact_with(1000.0, Some(12_000.0)); // 12x
        let pass = evaluate(&ratio_bar(10.0, 0.0), &artifact);
        assert!(pass.passed(), "{pass}");
        // Tighten the bar above the measured ratio → loud failure.
        let fail = evaluate(&ratio_bar(15.0, 0.0), &artifact);
        assert!(!fail.passed());
        assert!(fail.to_string().contains("cell-x"), "{fail}");
        assert!(fail.to_string().contains("12.0x"), "{fail}");
    }

    /// The printed line: a pass is the tag and the detail; a failure adds
    /// the bar's policy, so the violation reads with why the bar exists.
    #[test]
    fn a_failing_verdict_prints_the_bars_policy() {
        let artifact = artifact_with(1000.0, Some(12_000.0));
        let mut bar = ratio_bar(15.0, 0.0);
        bar.policy = "two-session floors 12.1x, 12.4x".into();
        let fail = evaluate(&bar, &artifact);
        assert_eq!(
            fail.to_string(),
            "[FAIL] cell-x: 12.0x vs dlt-pyarrow (12000 ms / 1000 ms), bar >= 15x (tol 0%) — policy: two-session floors 12.1x, 12.4x"
        );
        let pass = evaluate(&ratio_bar(10.0, 0.0), &artifact);
        assert_eq!(
            pass.to_string(),
            "[PASS] cell-x: 12.0x vs dlt-pyarrow (12000 ms / 1000 ms), bar >= 10x (tol 0%)"
        );
    }

    #[test]
    fn tolerance_absorbs_jitter_at_the_boundary() {
        let artifact = artifact_with(1030.0, Some(10_000.0)); // 9.7x vs 10x bar
        assert!(!evaluate(&ratio_bar(10.0, 0.0), &artifact).passed());
        assert!(evaluate(&ratio_bar(10.0, 5.0), &artifact).passed());
    }

    #[test]
    fn missing_baseline_fails_never_passes_silently() {
        let artifact = artifact_with(1000.0, None);
        let verdict = evaluate(&ratio_bar(10.0, 0.0), &artifact);
        assert!(!verdict.passed());
        assert!(verdict.to_string().contains("not present"), "{verdict}");

        let mut with_missing = artifact.clone();
        with_missing.competitors.insert(
            "dlt-pyarrow".into(),
            CompetitorSide::Missing {
                reason: "no image".into(),
            },
        );
        let verdict = evaluate(&ratio_bar(10.0, 0.0), &with_missing);
        assert!(!verdict.passed());
        assert!(verdict.to_string().contains("MISSING"), "{verdict}");
    }

    #[test]
    fn absolute_bar_is_competitor_independent() {
        let bar = Bar {
            cell: "cell-x".into(),
            kind: Kind::AbsoluteMs { max_ms: 40.0 },
            tolerance_pct: 0.0,
            policy: "cold-start record".into(),
        };
        assert!(evaluate(&bar, &artifact_with(23.6, None)).passed());
        let fail = evaluate(&bar, &artifact_with(44.0, None));
        assert!(!fail.passed());
        assert!(fail.to_string().contains("44.0 ms"), "{fail}");
    }

    #[test]
    fn rss_bar_needs_both_sides_and_compares_ratios() {
        let mut artifact = artifact_with(1000.0, Some(10_000.0));
        let bar = Bar {
            cell: "cell-x".into(),
            kind: Kind::RssRatioVs {
                competitor: "dlt-pyarrow".into(),
                max_rss_ratio: 0.2,
            },
            tolerance_pct: 0.0,
            policy: "test".into(),
        };
        // no rdlt RSS reading → fail loudly
        assert!(!evaluate(&bar, &artifact).passed());
        // 100 MB vs the competitor's 1000 MB = 1/10 ≤ 1/5 → pass
        artifact.rdlt.rss.peak_bytes = Some(100 << 20);
        let verdict = evaluate(&bar, &artifact);
        assert!(verdict.passed(), "{verdict}");
        // 300 MB = 1/3.3 > 1/5 → fail
        artifact.rdlt.rss.peak_bytes = Some(300 << 20);
        assert!(!evaluate(&bar, &artifact).passed());
    }

    /// A barred cell with NO artifact in a ledger that has recordings (an
    /// entirely empty ledger refuses upstream) FAILS its bar, policy
    /// attached — nothing to compare against is a violation, never a skip.
    #[test]
    fn gate_over_results_dir_reports_missing_artifacts() {
        let root = tempfile::tempdir().unwrap();
        let paths = Paths::rooted(root.path().to_path_buf(), root.path().join("target"));
        crate::artifact::write(
            &paths.results,
            &crate::artifact::tests::minimal("other-cell"),
        )
        .unwrap();
        let bars = vec![ratio_bar(10.0, 0.0)];
        let verdicts = run(&paths, &bars);
        assert!(!passed(&verdicts));
        assert!(
            verdicts[0].to_string().contains("no artifact"),
            "{}",
            verdicts[0].to_string()
        );
        assert!(verdicts[0].to_string().ends_with("— policy: test"));
    }
}
