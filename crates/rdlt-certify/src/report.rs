//! Reports as text, for a terminal, and as JSON, for machines.

#[cfg(test)]
mod tests;

use rdlt_connector::testing::{Outcome, Reason, Report};
use serde_json::{Value, json};

use crate::target::Held;

/// `report` as text for a terminal, as its `Display` writes it, its first line naming too the
/// version and binary's digest its certification `held` the connector to, each where held.
///
/// What the connector said is shown, not obeyed, and on its clause's line; a clause that does
/// not apply reads `n/a`, one not observed `skip`, and the last line gives the verdict and
/// counts.
pub fn plain(report: &Report, held: &Held) -> String {
    let text = report.to_string();
    match text.split_once('\n') {
        Some((heading, rest)) => format!("{heading}{}\n{rest}", said(held)),
        None => text,
    }
}

/// What follows a report's connector on its first line: ` <version>` and
/// `, binary sha256:<digest>`, each where `held`.
fn said(held: &Held) -> String {
    let version = held
        .version
        .as_ref()
        .map(|version| format!(" {}", line(version)));
    let digest = held
        .digest
        .map(|digest| format!(", binary sha256:{digest}"));
    format!(
        "{}{}",
        version.unwrap_or_default(),
        digest.unwrap_or_default()
    )
}

/// `text` as one line for a terminal: cut where a clause's reason is, and shown, not obeyed.
pub fn line(text: impl std::fmt::Display) -> String {
    Reason::new(text).to_string()
}

/// `report` as JSON: the connector, the version and binary's digest its certification `held` it
/// to, each null where none was, its verdict, whether it passed, and each clause's outcome, what
/// the connector said shown as in the plain report, so that no reader of the document's text is
/// deceived either.
pub fn json(report: &Report, held: &Held) -> Value {
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
        "version": held.version.as_ref().map(line),
        "digest": held.digest.map(|digest| digest.to_string()),
        "verdict": report.verdict().as_str(),
        "passed": report.passed(),
        "clauses": clauses,
    })
}
