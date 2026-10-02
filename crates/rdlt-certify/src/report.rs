//! Reports as text, for a terminal, and as JSON, for machines.

#[cfg(test)]
mod tests;

use rdlt_connector::testing::{Outcome, Reason, Report};
use serde_json::{Value, json};

/// `report` as text for a terminal, as its `Display` writes it: what the connector said is
/// shown, not obeyed, and on its clause's line; a clause that does not apply reads `n/a`, one
/// not observed `skip`, and the last line gives the verdict and counts.
pub fn plain(report: &Report) -> String {
    report.to_string()
}

/// `text` as one line for a terminal: cut where a clause's reason is, and shown, not obeyed.
pub fn line(text: impl std::fmt::Display) -> String {
    Reason::new(text).to_string()
}

/// `report` as JSON: the connector, its verdict, whether it passed, and each clause's outcome,
/// what the connector said shown as in the plain report, so that no reader of the document's
/// text is deceived either.
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
        "connector": Reason::new(&report.connector).as_str(),
        "verdict": report.verdict().as_str(),
        "passed": report.passed(),
        "clauses": clauses,
    })
}
