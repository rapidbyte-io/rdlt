//! Reports as text, for a terminal, and as JSON, for machines.

#[cfg(test)]
mod tests;

use rdlt_connector::testing::{Outcome, Report};
use serde_json::{Value, json};

/// `report` as text for a terminal: what the connector said is shown, not obeyed, and on its own
/// line.
pub fn plain(report: &Report) -> String {
    let lines = report.results.iter().map(|result| {
        let id = result.clause.id;
        match &result.outcome {
            Outcome::Passed => format!("  pass {id}\n"),
            Outcome::Failed(reason) => {
                let statement = result.clause.statement;
                format!("  FAIL {id}: {statement} ({})\n", shown(reason))
            }
            Outcome::Skipped(reason) => format!("  skip {id}: {}\n", shown(reason)),
        }
    });
    std::iter::once(format!("certification of {}\n", shown(&report.connector)))
        .chain(lines)
        .collect()
}

/// `text` with each character a terminal would act on escaped: controls, line breaks among them,
/// and the marks that reorder text.
fn shown(text: &str) -> String {
    let mut shown = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_control() || matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}') {
            shown.extend(c.escape_debug());
        } else {
            shown.push(c);
        }
    }
    shown
}

/// `report` as JSON: the connector, whether it passed, and each clause's outcome.
pub fn json(report: &Report) -> Value {
    let clauses: Vec<Value> = report
        .results
        .iter()
        .map(|result| {
            let (outcome, reason) = match &result.outcome {
                Outcome::Passed => ("passed", None),
                Outcome::Failed(reason) => ("failed", Some(reason)),
                Outcome::Skipped(reason) => ("skipped", Some(reason)),
            };
            json!({
                "id": result.clause.id,
                "statement": result.clause.statement,
                "outcome": outcome,
                "reason": reason,
            })
        })
        .collect();
    json!({
        "connector": report.connector,
        "passed": report.passed(),
        "clauses": clauses,
    })
}
