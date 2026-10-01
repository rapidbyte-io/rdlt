//! Reports as text, for a terminal, and as JSON, for machines.

#[cfg(test)]
mod tests;

use rdlt_connector::testing::{Outcome, Reason, Report};
use serde_json::{Value, json};

/// `report` as text for a terminal: what the connector said is shown, not obeyed, and on its own
/// line; a clause that does not apply reads `n/a`, one not observed `skip`, and the last line
/// gives the verdict and counts.
pub fn plain(report: &Report) -> String {
    let lines = report.results.iter().map(|result| {
        let id = result.clause.id;
        match &result.outcome {
            Outcome::Passed => match &result.note {
                Some(note) => format!("  pass {id} ({})\n", shown(note)),
                None => format!("  pass {id}\n"),
            },
            Outcome::Failed(reason) => {
                let statement = result.clause.statement;
                format!("  FAIL {id}: {statement} ({})\n", shown(reason))
            }
            Outcome::Inapplicable(reason) => format!("  n/a  {id}: {}\n", shown(reason)),
            Outcome::Unobserved(reason) => format!("  skip {id}: {}\n", shown(reason)),
        }
    });
    std::iter::once(format!("certification of {}\n", shown(&report.connector)))
        .chain(lines)
        .chain(std::iter::once(format!("{}\n", report.summary())))
        .collect()
}

/// `text` as one line for a terminal: cut where a clause's reason is, then shown, not obeyed.
pub fn line(text: impl std::fmt::Display) -> String {
    shown(Reason::new(text).as_str())
}

/// `text` with each character a terminal would act on escaped: controls, line breaks among them,
/// and the marks that reorder text.
fn shown(text: &str) -> String {
    let mut shown = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_control() || reorders(c) {
            shown.extend(c.escape_debug());
        } else {
            shown.push(c);
        }
    }
    shown
}

/// Whether `c` reorders or breaks the text around it: the bidirectional marks, embeddings,
/// overrides and isolates, and the line and paragraph separators.
fn reorders(c: char) -> bool {
    matches!(
        c,
        '\u{61c}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{2028}'..='\u{202e}'
            | '\u{2066}'..='\u{2069}'
    )
}

/// `report` as JSON: the connector, its verdict, whether it passed, and each clause's outcome.
pub fn json(report: &Report) -> Value {
    let clauses: Vec<Value> = report
        .results
        .iter()
        .map(|result| {
            let (outcome, reason) = match &result.outcome {
                Outcome::Passed => ("passed", None),
                Outcome::Failed(reason) => ("failed", Some(reason.as_str())),
                Outcome::Inapplicable(reason) => ("inapplicable", Some(reason.as_str())),
                Outcome::Unobserved(reason) => ("unobserved", Some(reason.as_str())),
            };
            json!({
                "id": result.clause.id,
                "statement": result.clause.statement,
                "outcome": outcome,
                "reason": reason,
                "note": result.note.as_ref().map(Reason::as_str),
            })
        })
        .collect();
    json!({
        "connector": report.connector,
        "verdict": report.verdict().as_str(),
        "passed": report.passed(),
        "clauses": clauses,
    })
}
