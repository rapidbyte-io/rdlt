
//! The verdict lookups the rogue suites share: find one clause's
//! entry in a report and hold it to a pinned Fail or a Pass.

use super::Report;
use crate::report::Verdict;

pub(super) fn verdict<'a>(report: &'a Report, clause: &str) -> &'a Verdict {
    &report
        .entries
        .iter()
        .find(|entry| entry.clause == clause)
        .unwrap_or_else(|| panic!("no {clause} entry:\n{}", report.render_text()))
        .verdict
}

#[track_caller]
pub(super) fn assert_fail(report: &Report, clause: &str, evidence: &str) {
    match verdict(report, clause) {
        Verdict::Fail(why) => assert_eq!(why, evidence, "clause {clause}"),
        other => panic!(
            "{clause} must Fail, got {other:?}:\n{}",
            report.render_text()
        ),
    }
}

#[track_caller]
pub(super) fn assert_pass(report: &Report, clause: &str) {
    assert!(
        matches!(verdict(report, clause), Verdict::Pass),
        "{clause} must Pass:\n{}",
        report.render_text()
    );
}
