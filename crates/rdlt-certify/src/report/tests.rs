use rdlt_connector::testing::{Clause, ClauseResult, Outcome, Report};

use super::plain;

const CLAUSE: Clause = Clause {
    id: "S-CHECK",
    statement: "check succeeds for a valid configuration",
};

#[test]
fn a_connectors_words_reach_the_terminal_as_text_and_on_their_own_line() {
    let report = Report {
        connector: "evil\u{1b}]0;title\u{7}".to_owned(),
        results: vec![ClauseResult {
            clause: CLAUSE,
            outcome: Outcome::Failed(
                "\u{1b}[2J\n  pass S-CHECK\r\u{202e}\u{200e}\u{200f}\u{61c}\u{2028}\u{2029}"
                    .to_owned(),
            ),
        }],
    };
    let shown = plain(&report);
    assert_eq!(shown.lines().count(), 2, "{shown}");
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
                outcome: Outcome::Skipped(
                    "no streams in the cafe\u{301}'s re\u{301}sume\u{301}".to_owned(),
                ),
            },
        ],
    };
    assert_eq!(plain(&report), report.to_string());
}
