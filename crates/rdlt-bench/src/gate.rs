//! Bar enforcement: bars.toml vs latest artifacts. A violation names cell,
//! bar, and measured value and exits nonzero. Only wall-median bars exist so
//! far — CPU/RSS/throughput are recorded, not gated. A ratio bar over a
//! MISSING baseline FAILS: there is nothing to compare against, so the gate
//! cannot be satisfied.

use std::path::Path;

use crate::Result;
use crate::artifact::{self, Artifact, CompetitorSide};
use crate::cells::{Bar, BarKind};

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Pass { detail: String },
    Fail { detail: String },
}

impl Verdict {
    pub fn passed(&self) -> bool {
        matches!(self, Verdict::Pass { .. })
    }
    pub fn detail(&self) -> &str {
        match self {
            Verdict::Pass { detail } | Verdict::Fail { detail } => detail,
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
pub fn evaluate(bar: &Bar, artifact: &Artifact) -> Verdict {
    let tol = bar.tolerance_pct / 100.0;
    let cell = &bar.cell;
    match &bar.kind {
        BarKind::RatioVs {
            competitor,
            min_ratio,
        } => match competitor_median(artifact, competitor) {
            Err(reason) => Verdict::Fail {
                detail: format!("{cell}: ratio bar >= {min_ratio}x vs {competitor} — {reason}"),
            },
            Ok(comp_ms) => {
                let ratio = comp_ms / artifact.rdlt.median_ms;
                let floor = min_ratio * (1.0 - tol);
                let detail = format!(
                    "{cell}: {ratio:.1}x vs {competitor} ({comp_ms:.0} ms / {:.0} ms), bar >= {min_ratio}x (tol {}%)",
                    artifact.rdlt.median_ms, bar.tolerance_pct
                );
                if ratio >= floor {
                    Verdict::Pass { detail }
                } else {
                    Verdict::Fail { detail }
                }
            }
        },
        BarKind::AbsoluteMs { max_ms } => {
            let measured = artifact.rdlt.median_ms;
            let ceiling = max_ms * (1.0 + tol);
            let detail = format!(
                "{cell}: {measured:.1} ms, bar <= {max_ms} ms absolute (tol {}%)",
                bar.tolerance_pct
            );
            if measured <= ceiling {
                Verdict::Pass { detail }
            } else {
                Verdict::Fail { detail }
            }
        }
        BarKind::RssRatioVs {
            competitor,
            max_rss_ratio,
        } => {
            let Some(rdlt_rss) = artifact.rdlt.rss.peak_bytes else {
                return Verdict::Fail {
                    detail: format!("{cell}: RSS bar but the rdlt side has no peak-RSS reading"),
                };
            };
            match competitor_rss(artifact, competitor) {
                Err(reason) => Verdict::Fail {
                    detail: format!(
                        "{cell}: RSS bar <= 1/{:.0} vs {competitor} — {reason}",
                        1.0 / max_rss_ratio
                    ),
                },
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
                    if ratio <= ceiling {
                        Verdict::Pass { detail }
                    } else {
                        Verdict::Fail { detail }
                    }
                }
            }
        }
    }
}

/// Render one verdict as its `[PASS|FAIL] detail` line; returns whether it
/// passed. The one place the tag wording lives — `gate` and the run summary
/// print through here.
pub fn print_verdict(verdict: &Verdict) -> bool {
    let tag = if verdict.passed() { "PASS" } else { "FAIL" };
    println!("[{tag}] {}", verdict.detail());
    verdict.passed()
}

/// Evaluate every bar; returns Ok(report) only if all pass, Err(report) with
/// every verdict otherwise — the caller exits nonzero.
pub fn run_gate(bars: &[Bar], results_dir: &Path) -> Result<(Vec<Verdict>, bool)> {
    let mut verdicts = Vec::new();
    let mut all_pass = true;
    for bar in bars {
        let verdict = match artifact::read(results_dir, &bar.cell) {
            Ok(artifact) => evaluate(bar, &artifact),
            Err(e) => Verdict::Fail {
                detail: format!("{}: no artifact — {e}", bar.cell),
            },
        };
        all_pass &= verdict.passed();
        verdicts.push(verdict);
    }
    Ok((verdicts, all_pass))
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
            kind: BarKind::RatioVs {
                competitor: "dlt-pyarrow".into(),
                min_ratio,
            },
            tolerance_pct,
            policy: "test".into(),
        }
    }

    #[test]
    fn ratio_bar_passes_and_a_tightened_bar_fails_naming_the_cell() {
        let artifact = artifact_with(1000.0, Some(12_000.0)); // 12x
        let pass = evaluate(&ratio_bar(10.0, 0.0), &artifact);
        assert!(pass.passed(), "{}", pass.detail());
        // Tighten the bar above the measured ratio → loud failure.
        let fail = evaluate(&ratio_bar(15.0, 0.0), &artifact);
        assert!(!fail.passed());
        assert!(fail.detail().contains("cell-x"), "{}", fail.detail());
        assert!(fail.detail().contains("12.0x"), "{}", fail.detail());
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
        assert!(
            verdict.detail().contains("not present"),
            "{}",
            verdict.detail()
        );

        let mut with_missing = artifact.clone();
        with_missing.competitors.insert(
            "dlt-pyarrow".into(),
            CompetitorSide::Missing {
                reason: "no image".into(),
            },
        );
        let verdict = evaluate(&ratio_bar(10.0, 0.0), &with_missing);
        assert!(!verdict.passed());
        assert!(verdict.detail().contains("MISSING"), "{}", verdict.detail());
    }

    #[test]
    fn absolute_bar_is_competitor_independent() {
        let bar = Bar {
            cell: "cell-x".into(),
            kind: BarKind::AbsoluteMs { max_ms: 40.0 },
            tolerance_pct: 0.0,
            policy: "cold-start record".into(),
        };
        assert!(evaluate(&bar, &artifact_with(23.6, None)).passed());
        let fail = evaluate(&bar, &artifact_with(44.0, None));
        assert!(!fail.passed());
        assert!(fail.detail().contains("44.0 ms"), "{}", fail.detail());
    }

    #[test]
    fn rss_bar_needs_both_sides_and_compares_ratios() {
        let mut artifact = artifact_with(1000.0, Some(10_000.0));
        let bar = Bar {
            cell: "cell-x".into(),
            kind: BarKind::RssRatioVs {
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
        assert!(verdict.passed(), "{}", verdict.detail());
        // 300 MB = 1/3.3 > 1/5 → fail
        artifact.rdlt.rss.peak_bytes = Some(300 << 20);
        assert!(!evaluate(&bar, &artifact).passed());
    }

    #[test]
    fn gate_over_results_dir_reports_missing_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let bars = vec![ratio_bar(10.0, 0.0)];
        let (verdicts, all_pass) = run_gate(&bars, dir.path()).unwrap();
        assert!(!all_pass);
        assert!(
            verdicts[0].detail().contains("no artifact"),
            "{}",
            verdicts[0].detail()
        );
    }
}
