//! The report vocabulary's public semantics: `passed()` counts only
//! `Fail` entries against certification, and the render spellings are a
//! CONTRACT — `PASS P1` / `FAIL S1: <why>` / `SKIP K-D4: <why>` are the
//! certifier bin's stdout lines, pinned full-string here before the CLI
//! exists. (`absorb`'s S/D-reuse fold is `pub(crate)` and pinned by the
//! unit tests beside it in `src/report.rs`.)

use rdlt_certify::{Entry, Report, Verdict};

/// A report is certified when nothing FAILED — skips are honest
/// non-verdicts (a clause the session could not exercise), not failures.
#[test]
fn passes_with_only_pass_and_skip_entries() {
    let report = Report {
        entries: vec![
            Entry {
                clause: "P1",
                verdict: Verdict::Pass,
            },
            Entry {
                clause: "K-D4",
                verdict: Verdict::Skip("no destination fixture in this session".to_string()),
            },
        ],
    };
    assert!(report.passed());
}

/// One `Fail` entry refuses the whole certification.
#[test]
fn one_fail_entry_refuses_certification() {
    let report = Report {
        entries: vec![
            Entry {
                clause: "P1",
                verdict: Verdict::Pass,
            },
            Entry {
                clause: "S1",
                verdict: Verdict::Fail("the resume law broke".to_string()),
            },
        ],
    };
    assert!(!report.passed());
}

/// The text render, full-string: one line per entry, in entry order,
/// each line newline-terminated. These spellings are the bin's stdout
/// contract — do not reword them.
#[test]
fn render_text_spells_the_contract_lines() {
    let report = Report {
        entries: vec![
            Entry {
                clause: "P1",
                verdict: Verdict::Pass,
            },
            Entry {
                clause: "S1",
                verdict: Verdict::Fail("the resume law broke".to_string()),
            },
            Entry {
                clause: "K-D4",
                verdict: Verdict::Skip("no fixture".to_string()),
            },
        ],
    };
    assert_eq!(
        report.render_text(),
        "PASS P1\nFAIL S1: the resume law broke\nSKIP K-D4: no fixture\n"
    );
}

/// The JSON render round-trips through `serde_json::Value` with entries
/// in stable (entry) order.
#[test]
fn render_json_round_trips_in_stable_order() {
    let report = Report {
        entries: vec![
            Entry {
                clause: "S1",
                verdict: Verdict::Fail("the resume law broke".to_string()),
            },
            Entry {
                clause: "S2",
                verdict: Verdict::Pass,
            },
        ],
    };
    let value: serde_json::Value =
        serde_json::from_str(&report.render_json()).expect("render_json emits valid JSON");
    assert_eq!(value["entries"][0]["clause"], "S1");
    assert_eq!(
        value["entries"][0]["verdict"]["Fail"],
        "the resume law broke"
    );
    assert_eq!(value["entries"][1]["clause"], "S2");
    assert_eq!(value["entries"][1]["verdict"], "Pass");
}

/// An empty report passes vacuously at `passed()` — the CALLER decides
/// whether an empty entry set is meaningful; renders are empty too.
#[test]
fn an_empty_report_renders_empty() {
    let report = Report::default();
    assert!(report.passed());
    assert_eq!(report.render_text(), "");
    let value: serde_json::Value =
        serde_json::from_str(&report.render_json()).expect("render_json emits valid JSON");
    assert_eq!(value["entries"], serde_json::json!([]));
}
