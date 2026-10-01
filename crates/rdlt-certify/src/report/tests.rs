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
        shown.contains(r"\u{1b}[2J\n  pass S-CHECK\r\u{202e}"),
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
            },
            ClauseResult {
                clause: CLAUSE,
                // Letters, accents that combine with them included, are shown as they are.
                outcome: Outcome::Unobserved(
                    "no streams in the cafe\u{301}'s re\u{301}sume\u{301}".into(),
                ),
            },
            ClauseResult {
                clause: CLAUSE,
                outcome: Outcome::Inapplicable("the source serves one role".into()),
            },
            ClauseResult {
                clause: CLAUSE,
                outcome: Outcome::Failed("check failed".into()),
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
