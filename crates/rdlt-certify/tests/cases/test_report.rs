//! The report vocabulary's public semantics: `passed()` counts `Fail`
//! and `NotReached` entries against certification (a clause the suite
//! died before reaching proves nothing), and the render spellings are a
//! CONTRACT — `PASS P1 (<title>)` / `FAIL S1 (<title>): <why>` /
//! `SKIP K-D4 (<title>): <why>` / `NOT-REACHED D4 (<title>): <why>`
//! are the certifier bin's stdout lines, pinned full-string here.
//! Every clause id carries its fixed short title from the one
//! vocabulary table (end users never see specs, so a bare `FAIL P3`
//! would be unactionable); an id outside the table renders
//! bare rather than inventing a title. (`absorb`'s S/D-reuse fold is
//! `pub(crate)` and pinned by the unit tests beside it in
//! `src/report.rs`.)

use rdlt_certify::report::{CLAUSES, Entry, Report, Verdict, clause_title};

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
/// each line newline-terminated, every clause id followed by its fixed
/// title in parentheses. These spellings are the bin's stdout contract
/// — do not reword them.
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
        "PASS P1 (one handshake line on stdout)\n\
         FAIL S1 (checkpoint resume law): the resume law broke\n\
         SKIP K-D4 (SIGKILL between write and publish, then exactly-once on re-run): \
         no fixture\n"
    );
}

/// A `NotReached` entry REFUSES certification — unlike `Skip`, nobody
/// chose not to exercise the clause: the suite died first, and nothing
/// was proven. Its render line is part of the stdout contract.
#[test]
fn a_not_reached_entry_refuses_certification_and_renders_the_contract_line() {
    let report = Report {
        entries: vec![
            Entry {
                clause: "P1",
                verdict: Verdict::Pass,
            },
            Entry {
                clause: "D4",
                verdict: Verdict::NotReached(
                    "not run — the suite aborted at D1 before reaching it".to_string(),
                ),
            },
        ],
    };
    assert!(!report.passed());
    assert_eq!(
        report.render_text(),
        "PASS P1 (one handshake line on stdout)\n\
         NOT-REACHED D4 (dead-predecessor staging teardown): not run — the suite aborted \
         at D1 before reaching it\n"
    );
}

/// An id outside the vocabulary renders bare — the fold keeps foreign
/// clause ids (a testkit failure naming a clause this crate does not
/// know) rather than dropping them, and the render must not invent a
/// title for one.
#[test]
fn an_unknown_clause_id_renders_without_a_title() {
    assert_eq!(clause_title("Z9"), None);
    let report = Report {
        entries: vec![Entry {
            clause: "Z9",
            verdict: Verdict::Fail("a clause outside the vocabulary".to_string()),
        }],
    };
    assert_eq!(
        report.render_text(),
        "FAIL Z9: a clause outside the vocabulary\n"
    );
}

/// The vocabulary table is well-formed for its consumers: ids unique,
/// titles one short phrase (never empty, never sentence-cased prose),
/// definitions full sentences — and `clause_title` answers from it.
#[test]
fn the_clause_table_is_well_formed() {
    let mut seen = std::collections::BTreeSet::new();
    for clause in CLAUSES {
        assert!(seen.insert(clause.id), "duplicate id {}", clause.id);
        assert!(!clause.title.is_empty(), "{} has an empty title", clause.id);
        assert!(
            !clause.definition.is_empty(),
            "{} has an empty definition",
            clause.id
        );
        assert_eq!(
            clause_title(clause.id),
            Some(clause.title),
            "clause_title must answer from the table for {}",
            clause.id
        );
    }
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
            Entry {
                clause: "S4",
                verdict: Verdict::NotReached("the suite died first".to_string()),
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
    assert_eq!(
        value["entries"][2]["verdict"]["NotReached"],
        "the suite died first"
    );
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
