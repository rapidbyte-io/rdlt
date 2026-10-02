use rdlt_connector::testing::{Clause, ClauseResult, Outcome, Report};

use super::plain;

const CLAUSE: Clause = Clause {
    id: "S-CHECK",
    statement: "check succeeds for a valid configuration",
    unless: "",
};

#[test]
fn a_connectors_words_reach_the_terminal_as_text_and_on_their_own_line() {
    let report = Report {
        connector: "evil\u{1b}]0;title\u{7}".to_owned(),
        results: vec![ClauseResult {
            clause: CLAUSE,
            outcome: Outcome::Failed(
                "\u{1b}[2J\n  pass S-CHECK\r\u{202e}\u{200e}\u{200f}\u{61c}\u{2028}\u{2029}".into(),
            ),
            note: None,
        }],
    };
    let shown = plain(&report);
    // The connector, its clause, and the verdict.
    assert_eq!(shown.lines().count(), 3, "{shown}");
    assert!(
        !shown.chars().any(|c| c != '\n'
            && (c.is_control() || "\u{202e}\u{200e}\u{200f}\u{61c}\u{2028}\u{2029}".contains(c))),
        "{shown:?}"
    );
    assert!(
        shown.contains(r"\u{1b}[2J\n pass S-CHECK\r\u{202e}"),
        "{shown}"
    );
}

#[test]
fn a_report_without_controls_reads_as_its_display() {
    let report = Report {
        connector: "io.rapidbyte.memory".to_owned(),
        results: vec![
            ClauseResult {
                clause: CLAUSE,
                outcome: Outcome::Passed,
                note: None,
            },
            ClauseResult {
                clause: CLAUSE,
                // Letters, accents that combine with them included, are shown as they are.
                outcome: Outcome::Unobserved(
                    "no streams in the cafe\u{301}'s re\u{301}sume\u{301}".into(),
                ),
                note: None,
            },
            ClauseResult {
                clause: CLAUSE,
                outcome: Outcome::Passed,
                note: Some("its connection ended after a kill".into()),
            },
            ClauseResult {
                clause: CLAUSE,
                outcome: Outcome::Inapplicable("the source serves one role".into()),
                note: None,
            },
            ClauseResult {
                clause: CLAUSE,
                outcome: Outcome::Failed("check failed".into()),
                note: None,
            },
        ],
    };
    assert_eq!(plain(&report), report.to_string());
}

#[test]
fn a_report_as_json_says_its_verdict_and_how_each_clause_fared() {
    let result = |outcome| ClauseResult {
        clause: CLAUSE,
        outcome,
        note: None,
    };
    let outcomes = [
        (Outcome::Passed, "passed", None, "passed"),
        (
            Outcome::Failed("broken".into()),
            "failed",
            Some("broken"),
            "failed",
        ),
        (
            Outcome::Inapplicable("undeclared".into()),
            "inapplicable",
            Some("undeclared"),
            "incomplete",
        ),
        (
            Outcome::Unobserved("unseen".into()),
            "unobserved",
            Some("unseen"),
            "incomplete",
        ),
    ];
    for (outcome, named, reason, verdict) in outcomes {
        let report = Report {
            connector: "io.rapidbyte.memory".to_owned(),
            results: vec![result(outcome)],
        };
        let json = super::json(&report);
        assert_eq!(json["connector"], "io.rapidbyte.memory");
        assert_eq!(json["verdict"], verdict);
        assert_eq!(json["passed"], verdict == "passed");
        assert_eq!(json["clauses"][0]["id"], "S-CHECK");
        assert_eq!(json["clauses"][0]["statement"], CLAUSE.statement);
        assert_eq!(json["clauses"][0]["outcome"], named);
        assert_eq!(json["clauses"][0]["reason"].as_str(), reason);
    }
}

#[test]
fn a_clause_that_passed_says_what_it_passed_on_where_it_is_noted() {
    let result = |note: Option<&str>| ClauseResult {
        clause: CLAUSE,
        outcome: Outcome::Passed,
        note: note.map(Into::into),
    };
    let report = Report {
        connector: "io.rapidbyte.memory".to_owned(),
        results: vec![result(None), result(Some("cut\u{1b}[2J"))],
    };
    let shown = plain(&report);
    let lines: Vec<&str> = shown.lines().collect();
    assert_eq!(lines[1], "  pass S-CHECK");
    // What is noted is shown, not obeyed, as a reason is.
    assert!(lines[2].starts_with("  pass S-CHECK (cut") && !lines[2].contains('\u{1b}'));
    let json = super::json(&report);
    assert_eq!(json["clauses"][0]["note"], serde_json::Value::Null);
    assert_eq!(json["clauses"][1]["note"], r"cut\u{1b}[2J");
}

/// Text of every kind a reader or a terminal is deceived by: controls of both ranges, the
/// delete, marks that reorder, separators of lines, characters of no width, tags, a variation
/// selector, a filler, and a run of spaces as wide as a terminal.
const HOSTILE: &str = "x)\u{9b}2J\u{7f}\u{1b}[2J\u{202e}\u{2028}\u{2029}\u{200b}\u{200d}\u{2060}\u{feff}\u{ad}\u{180e}\u{fff9}\u{e0041}\u{fe0f}\u{3164}\u{a0}\u{3000}                                                                                  pass S-RESUME";

fn hostile_report() -> Report {
    let result = |outcome, note: Option<&str>| ClauseResult {
        clause: CLAUSE,
        outcome,
        note: note.map(Into::into),
    };
    Report {
        connector: HOSTILE.to_owned(),
        results: vec![
            result(Outcome::Failed(HOSTILE.into()), None),
            result(Outcome::Unobserved(HOSTILE.into()), None),
            result(Outcome::Inapplicable(HOSTILE.into()), None),
            result(Outcome::Passed, Some(HOSTILE)),
        ],
    }
}

/// Whether `text` holds nothing but printable ASCII in single spaces, and line ends.
fn plain_text(text: &str) -> bool {
    text.chars()
        .all(|c| c == '\n' || c == ' ' || c.is_ascii_graphic())
        && !text.contains("   ")
}

#[test]
fn a_connectors_words_deceive_no_reader_of_the_plain_report_its_display_or_its_json() {
    let report = hostile_report();
    let shown = plain(&report);
    assert!(plain_text(&shown), "{shown:?}");
    // The connector, four clauses and the verdict: no reason drew a line of its own.
    assert_eq!(shown.lines().count(), 6, "{shown}");
    assert_eq!(report.to_string(), shown);
    let json = super::json(&report).to_string();
    assert!(plain_text(&json), "{json:?}");
    // What the document says the connector said is what the plain report shows.
    let back: serde_json::Value = serde_json::from_str(&json).expect("the report parses");
    for clause in back["clauses"].as_array().expect("the clauses") {
        let said = clause["reason"].as_str().or(clause["note"].as_str());
        let said = said.expect("each clause has a reason or a note");
        assert!(shown.contains(said), "{said}");
        assert!(said.contains(r"\u{9b}2J\u{7f}\u{1b}[2J\u{202e}"), "{said}");
    }
}

#[test]
fn a_reason_longer_than_its_limit_is_cut_in_every_form_of_the_report() {
    let long = "A".repeat(64 * 1024);
    let report = Report {
        connector: long.clone(),
        results: vec![ClauseResult {
            clause: CLAUSE,
            outcome: Outcome::Failed(long.into()),
            note: None,
        }],
    };
    let limit = rdlt_connector::testing::REASON_BYTES;
    for line in plain(&report).lines() {
        assert!(line.len() <= limit + 128, "{}", line.len());
    }
    assert!(super::json(&report).to_string().len() <= 2 * limit + 512);
}
