//! The certification report vocabulary: per-clause verdicts, the pinned
//! render spellings (`PASS P1` / `FAIL S1: <why>` / `SKIP K-D4: <why>`
//! — the certifier bin's stdout contract), and the S/D-reuse fold that
//! maps the testkit's conformance failures into clause entries.

use std::fmt::Write as _;
use std::time::Duration;

use rdlt_testkit::conformance::ConformanceFailure;

/// The certification bar's no-hang rule: every clause is bounded by this
/// budget — a connector that stalls FAILS the clause, the certifier
/// never hangs. Generous on purpose: the S-suite replays every stream
/// once per checkpoint, so this is a wedge detector, not a performance
/// bar.
pub(crate) const CLAUSE_TIMEOUT: Duration = Duration::from_secs(30);

/// The one spelling every timed-out clause fails with.
pub(crate) fn timed_out() -> String {
    format!(
        "clause timed out after {}s — a connector that stalls fails the clause",
        CLAUSE_TIMEOUT.as_secs()
    )
}

/// One clause's verdict. `Skip` is an honest non-verdict — a clause the
/// session could not exercise, with the reason — and does NOT count
/// against [`Report::passed`]; only `Fail` does.
#[derive(Debug, Clone, serde::Serialize)]
pub enum Verdict {
    /// The clause held.
    Pass,
    /// The clause was violated — the payload says what the connector
    /// did, in terms an author can act on.
    Fail(String),
    /// The clause was not exercised — the payload says why.
    Skip(String),
}

/// One clause's line in the report.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Entry {
    /// Clause id from the contract vocabulary, e.g. `"S1"`, `"P1"`.
    pub clause: &'static str,
    /// What certification concluded about it.
    pub verdict: Verdict,
}

/// The certification report: clause entries in the order certification
/// produced them — that order is the stable order both renders emit.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Report {
    /// Every clause verdict, in certification order.
    pub entries: Vec<Entry>,
}

impl Report {
    /// Certified = no `Fail` entries. Skips do not refuse — an empty
    /// report passes vacuously, and the caller decides what an empty
    /// entry set means.
    pub fn passed(&self) -> bool {
        !self
            .entries
            .iter()
            .any(|entry| matches!(entry.verdict, Verdict::Fail(_)))
    }

    /// One line per entry, each newline-terminated, spelled exactly
    /// `PASS P1` / `FAIL S1: <why>` / `SKIP K-D4: <why>` — the bin's
    /// stdout contract, pinned full-string by the report tests.
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        for entry in &self.entries {
            match &entry.verdict {
                Verdict::Pass => writeln!(out, "PASS {}", entry.clause),
                Verdict::Fail(why) => writeln!(out, "FAIL {}: {why}", entry.clause),
                Verdict::Skip(why) => writeln!(out, "SKIP {}: {why}", entry.clause),
            }
            .expect("writing into a String cannot fail");
        }
        out
    }

    /// The report as one JSON document, entries in the same stable
    /// order as [`Self::render_text`].
    pub fn render_json(&self) -> String {
        serde_json::to_string(self).expect("a report of strings serializes infallibly")
    }

    /// Record that `clause` held.
    pub(crate) fn pass(&mut self, clause: &'static str) {
        self.entries.push(Entry {
            clause,
            verdict: Verdict::Pass,
        });
    }

    /// Record that `clause` was violated, and how.
    pub(crate) fn fail(&mut self, clause: &'static str, why: String) {
        self.entries.push(Entry {
            clause,
            verdict: Verdict::Fail(why),
        });
    }

    /// Record that `clause` was not exercised, and why.
    pub(crate) fn skip(&mut self, clause: &'static str, why: String) {
        self.entries.push(Entry {
            clause,
            verdict: Verdict::Skip(why),
        });
    }

    /// The S/D-reuse fold: map one conformance-suite run into clause
    /// entries. Every clause in `asserted` gets a verdict — each failure
    /// naming it becomes a `Fail` at that clause's position, and an
    /// asserted clause no failure mentions is a `Pass` (the suite ran
    /// and found nothing against it). A failure naming a clause OUTSIDE
    /// `asserted` is still folded in as a `Fail` — never dropped.
    pub(crate) fn absorb(&mut self, failures: Vec<ConformanceFailure>, asserted: &[&'static str]) {
        for clause in asserted {
            let mut violated = false;
            for failure in failures.iter().filter(|f| f.clause == *clause) {
                violated = true;
                self.fail(clause, failure.message.clone());
            }
            if !violated {
                self.pass(clause);
            }
        }
        for failure in failures.iter().filter(|f| !asserted.contains(&f.clause)) {
            self.fail(failure.clause, failure.message.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    //! `absorb` and the timeout spelling are `pub(crate)`, so their pins
    //! live beside them; the public report semantics (passed, renders)
    //! are pinned by the integration cases.

    use super::*;

    fn failure(clause: &'static str, message: &str) -> ConformanceFailure {
        ConformanceFailure {
            clause,
            message: message.to_string(),
        }
    }

    /// A failure maps to `Fail` at its asserted position; asserted
    /// clauses no failure mentions pass — here `entries[0]` is S1's
    /// `Fail` and S2/S4 follow as `Pass`.
    #[test]
    fn absorb_fails_the_named_clause_and_passes_the_unmentioned() {
        let mut report = Report::default();
        report.absorb(
            vec![failure("S1", "the resume law broke")],
            &["S1", "S2", "S4"],
        );
        assert_eq!(
            report.render_text(),
            "FAIL S1: the resume law broke\nPASS S2\nPASS S4\n"
        );
        assert!(!report.passed());
    }

    /// No failures at all: every asserted clause passes, in asserted
    /// order.
    #[test]
    fn absorb_of_no_failures_passes_every_asserted_clause() {
        let mut report = Report::default();
        report.absorb(vec![], &["S1", "S2", "S4"]);
        assert_eq!(report.render_text(), "PASS S1\nPASS S2\nPASS S4\n");
        assert!(report.passed());
    }

    /// Several failures on one clause all surface — one line each — and
    /// a failure naming a clause outside the asserted set is folded in,
    /// never dropped.
    #[test]
    fn absorb_keeps_every_failure_including_unasserted_clauses() {
        let mut report = Report::default();
        report.absorb(
            vec![
                failure("S1", "stream `a`: content differs"),
                failure("S1", "stream `b`: content differs"),
                failure("D3", "a clause this fold was not asked to assert"),
            ],
            &["S1", "S2"],
        );
        assert_eq!(
            report.render_text(),
            "FAIL S1: stream `a`: content differs\n\
             FAIL S1: stream `b`: content differs\n\
             PASS S2\n\
             FAIL D3: a clause this fold was not asked to assert\n"
        );
    }

    /// The one timeout spelling, full-string — the certification bar's
    /// no-hang rule renders through this everywhere.
    #[test]
    fn the_timeout_spelling_is_pinned() {
        assert_eq!(
            timed_out(),
            "clause timed out after 30s — a connector that stalls fails the clause"
        );
    }
}
