//! The driver kind: a host-side `driver.py` in the variant's module
//! directory drives an external system (an Airbyte cluster), times the work
//! itself, and prints the same summary line as every other arm.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::artifact::{CpuStats, RssStats};
use crate::cell::CompetitorRef;
use crate::competitor::{Run, summary, variant::Variant};
use crate::error::{Error, Result};
use crate::template;

/// The venv convention: a module ships `.venv/` next to its driver (created
/// by its setup instructions); without one the driver runs on the system
/// `python3`.
fn driver_python(module_dir: &Path) -> PathBuf {
    let venv = module_dir.join(".venv/bin/python");
    if venv.is_file() {
        venv
    } else {
        "python3".into()
    }
}

/// The machine prerequisite ONCE before any run: an external system the
/// driver depends on that this machine may simply not have. Failure is the
/// arm's `Missing` reason, carrying the probe's output.
pub(super) fn preflight(variant: &Variant) -> std::result::Result<(), String> {
    let Some(probe) = &variant.prerequisite_sh else {
        return Ok(());
    };
    let out = Command::new("sh")
        .args(["-c", probe])
        .current_dir(&variant.module_dir)
        .output();
    let failed = match &out {
        Ok(o) => !o.status.success(),
        Err(_) => true,
    };
    if !failed {
        return Ok(());
    }
    let detail = out
        .map(|o| {
            let mut text = String::from_utf8_lossy(&o.stdout).trim().to_string();
            let err = String::from_utf8_lossy(&o.stderr).trim().to_string();
            if !err.is_empty() {
                if !text.is_empty() {
                    text.push_str("; ");
                }
                text.push_str(&err);
            }
            text
        })
        .unwrap_or_else(|e| e.to_string());
    Err(format!(
        "prerequisite failed for `{}`: {detail}",
        variant.id
    ))
}

/// One driver run: exec the script with the arm's substituted args, then
/// parse the summary line.
pub(super) fn run_once(
    variant: &Variant,
    reference: &CompetitorRef,
    subs: &BTreeMap<String, String>,
) -> Result<Run> {
    let driver = variant.driver.as_ref().expect("driver kind has a driver");
    let mut cmd = Command::new(driver_python(&variant.module_dir));
    cmd.arg(driver).current_dir(&variant.module_dir);
    cmd.args(reference.args.iter().map(|a| template::substitute(a, subs)));
    let out = cmd.output()?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        return Err(Error(format!(
            "driver {} ({}) failed: {}{}",
            variant.id,
            driver.display(),
            stdout,
            String::from_utf8_lossy(&out.stderr)
        )));
    }
    let summary = summary::parse(&stdout).ok_or_else(|| {
        Error(format!(
            "driver {}: no summary JSON line with `seconds` on stdout: {stdout}",
            variant.id
        ))
    })?;
    let peak_bytes = summary.peak_rss_bytes();
    Ok(Run {
        seconds: summary.seconds,
        cpu: CpuStats {
            mean_util: None,
            peak_util: None,
            user_sys_ms: None,
            note: Some("driver-run external system — no per-process CPU accounting".into()),
        },
        rss: RssStats {
            peak_bytes,
            note: Some(if peak_bytes.is_some() {
                "driver-reported (the module states which system statistic this is)".into()
            } else {
                "no RSS reading (nothing driver-reported) — null, not fabricated".into()
            }),
        },
        extra: summary.extra,
    })
}
