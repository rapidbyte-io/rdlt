//! How a certification reads on a terminal: a line per clause as it finishes, and a verdict.

#[cfg(test)]
mod tests;

use std::time::Duration;

use rdlt_certify::{ClauseResult, Outcome, Report, Verdict};

use super::{Tone, Ui, out_width, wrapped};
use crate::output::shown;

pub(crate) use rdlt_certify::Clause;

/// `✓ P-HELLO  2s`: one clause's line, scrubbed and wrapped, `took` shown only from a second on,
/// in whole seconds, as any finer figure would be the sampling's.
pub(crate) fn kept_line(result: &ClauseResult, took: Option<Duration>, ui: &Ui) -> String {
    let brush = ui.looks().out();
    let marks = ui.looks().marks();
    let id = result.clause.id;
    let timing = took
        .filter(|took| took.as_secs() >= 1)
        .map_or(String::new(), |took| {
            let seconds = format!("{}s", took.as_secs());
            format!("  {}", brush.paint(Tone::Dim, seconds))
        });
    let line = match &result.outcome {
        Outcome::Passed => {
            let note = result
                .note
                .as_ref()
                .map_or(String::new(), |note| format!("  {}", shown(note.as_str())));
            format!("  {} {id}{note}{timing}", brush.paint(Tone::Good, marks.ok))
        }
        Outcome::Failed(reason) => format!(
            "  {} {id}  {}{timing}",
            brush.paint(Tone::Bad, marks.fail),
            shown(reason.as_str())
        ),
        Outcome::Inapplicable(reason) | Outcome::Unobserved(reason) => format!(
            "  {} {id}  skipped: {}{timing}",
            brush.paint(Tone::Warn, marks.skip),
            shown(reason.as_str())
        ),
    };
    scrubbed_and_wrapped(line)
}

/// `line` scrubbed of the configuration's secrets, then wrapped to standard output's width, its
/// later lines indented: scrubbed first, as a break inside a secret would leave halves no scrub
/// finds.
fn scrubbed_and_wrapped(line: String) -> String {
    let line = crate::certify::redactions().scrubbed(line);
    wrapped(&line, out_width().saturating_sub(1), "    ").join("\n")
}

/// What the spinner names while `clause` of `role` is checked.
pub(crate) fn in_progress(role: rdlt_connector::Role, clause: Option<&Clause>) -> String {
    let role = role_word(role);
    match clause {
        Some(clause) => format!("Certifying the {role}: {} {}", clause.id, clause.statement),
        None => format!("Certifying the {role}"),
    }
}

/// `✓ Passed  source  connector  12 passed, ...`, of `report` as `role`, scrubbed and wrapped.
pub(crate) fn verdict_line(report: &Report, role: rdlt_connector::Role, ui: &Ui) -> String {
    let brush = ui.looks().out();
    let marks = ui.looks().marks();
    let (tone, mark, word) = match report.verdict() {
        Verdict::Passed => (Tone::Good, marks.ok, "Passed"),
        Verdict::Failed => (Tone::Bad, marks.fail, "Failed"),
        Verdict::Incomplete => (Tone::Warn, marks.stop, "Incomplete"),
    };
    let summary = report.summary();
    let counts = summary
        .split_once(": ")
        .map_or(summary.as_str(), |(_, counts)| counts);
    let line = format!(
        "{}  {}  {}  {}",
        brush.paint(tone, format!("{mark} {word}")),
        role_word(role),
        brush.paint(Tone::Name, shown(&report.connector)),
        brush.paint(Tone::Dim, counts)
    );
    scrubbed_and_wrapped(line)
}

fn role_word(role: rdlt_connector::Role) -> &'static str {
    match role {
        rdlt_connector::Role::Source => "source",
        rdlt_connector::Role::Destination => "destination",
    }
}
