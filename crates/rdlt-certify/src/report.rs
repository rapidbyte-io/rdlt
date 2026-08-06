//! The certification report vocabulary: per-clause verdicts, the pinned
//! render spellings (`PASS P1 (<title>)` / `FAIL S1 (<title>): <why>` /
//! `SKIP K-D4 (<title>): <why>` — the certifier bin's stdout contract),
//! the one authoritative clause table (id, title, definition — what the
//! titles, the bin's `--explain` and the README all speak from), and
//! the S/D-reuse fold that maps the testkit's conformance failures into
//! clause entries.

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

/// One clause's user-facing vocabulary entry: the contract id, a fixed
/// short title (rendered beside the id on every report line), and a
/// self-contained definition (rendered by the bin's `--explain`). End
/// users never see this repository's specs, so a bare `FAIL P3` is
/// unactionable — the title makes the line readable and the definition
/// makes it fixable (D-040-5).
#[derive(Debug, Clone, Copy)]
pub struct Clause {
    /// The contract id, e.g. `"P3"`.
    pub id: &'static str,
    /// One short noun phrase naming what the clause holds.
    pub title: &'static str,
    /// What the clause asserts, in sentences a connector author can act
    /// on without reading this repository.
    pub definition: &'static str,
}

/// Every clause the certifier can emit, in explain order (S, D, P, K).
/// The ONE place titles and definitions live — [`clause_title`], the
/// bin's `--explain` and the crate README's clause table all read this
/// table, so they cannot drift apart. The id set is pinned against the
/// union of the crate's clause constants by the vocabulary test beside
/// this table.
pub const CLAUSES: &[Clause] = &[
    Clause {
        id: "S1",
        title: "checkpoint resume law",
        definition: "For every checkpoint the source emits, one full read equals the rows \
                     covered by that checkpoint followed by a resumed read since it — resuming \
                     from any checkpoint loses nothing and repeats nothing.",
    },
    Clause {
        id: "S2",
        title: "checkpoint coverage",
        definition: "A stream must checkpoint at least once during a read. A stream that never \
                     checkpoints cannot be certified for resume and fails by name.",
    },
    Clause {
        id: "S4",
        title: "prompt cancellation",
        definition: "When the record channel closes mid-read, the source stops promptly and \
                     returns Ok — never an error, never a hang.",
    },
    Clause {
        id: "D1",
        title: "staging invisibility",
        definition: "Rows written into a load session but not yet committed are invisible to \
                     readers of the table.",
    },
    Clause {
        id: "D2",
        title: "atomic state-with-data commit",
        definition: "A commit persists the pipeline's state document atomically with the data: \
                     reading state back afterward returns exactly the committed cursor.",
    },
    Clause {
        id: "D3",
        title: "idempotent commit receipts",
        definition: "Re-committing the same (load_id, commit_seq) returns the prior receipt and \
                     re-publishes nothing.",
    },
    Clause {
        id: "D4",
        title: "dead-predecessor staging teardown",
        definition: "A new session makes a dead predecessor's staged rows invisible — only the \
                     new session's rows ever publish.",
    },
    Clause {
        id: "D5",
        title: "idempotent ensure_table",
        definition: "Ensuring a table that already exists succeeds and disturbs nothing — \
                     ensure_table can be repeated freely.",
    },
    Clause {
        id: "D6",
        title: "no state for fresh pipelines",
        definition: "A pipeline that has never committed reads back no state.",
    },
    Clause {
        id: "D8",
        title: "merge upsert by _rdlt_id",
        definition: "Under the merge write mode, rows sharing an _rdlt_id upsert rather than \
                     duplicate. Asserted only for destinations that declare the merge \
                     capability; otherwise it is skipped with the reason, never vacuously \
                     passed.",
    },
    Clause {
        id: "P1",
        title: "one handshake line on stdout",
        definition: "The connector's first stdout line is the handshake line advertising its \
                     socket and protocol range, and stdout carries EXACTLY that one line — \
                     nothing may follow it. Stdout is the machine channel; logs belong on \
                     stderr.",
    },
    Clause {
        id: "P2",
        title: "typed unknown-config-field refusal",
        definition: "A config document containing an unknown field must be refused at the \
                     handshake with a typed refusal carrying its classification — never \
                     accepted, and never surfaced as a dial failure or a dead stream.",
    },
    Clause {
        id: "P3",
        title: "spec/handshake identity agreement",
        definition: "The identity the handshake reports (connector id and version) must agree \
                     with the connector's own spec document — and, when the operator named a \
                     connector id, with that id. Any skew between the two fails the clause.",
    },
    Clause {
        id: "P4",
        title: "complete pre-handshake Spec reply",
        definition: "The Spec call must answer with a non-empty name, a non-empty version and a \
                     JSON-object config schema — with no config supplied at all.",
    },
    Clause {
        id: "P5",
        title: "one Arrow batch per read frame",
        definition: "Every arrow_ipc read frame must decode as one Arrow IPC stream carrying \
                     exactly one record batch, judged on the wire bytes. Frames that are not \
                     arrow are exempt; a source that serves no arrow frames at all passes \
                     vacuously.",
    },
    Clause {
        id: "P6",
        title: "typed terminal error frame",
        definition: "A read refusal must arrive as a terminal error frame carrying a real \
                     classification value and bare cause text — never a clean end of stream, \
                     and never a client-side rendering baked into the message.",
    },
    Clause {
        id: "P7",
        title: "tolerated state-format version map",
        definition: "The handshake's state_format_versions field must decode as a map from \
                     state kind to format version — which protocol decoding itself enforces, so \
                     an undecodable map fails the whole handshake. An empty map is the v0 \
                     posture; a populated one is tolerated and threaded onward, never \
                     negotiated.",
    },
    Clause {
        id: "P8",
        title: "one-session-per-process ceiling",
        definition: "While one session is held, a second concurrent OpenSession on the live \
                     socket must be refused with the FailedPrecondition status — v0 allows \
                     exactly one session per connector process.",
    },
    Clause {
        id: "P9",
        title: "abandoned-session reclaim",
        definition: "A session abandoned without Close (its request stream just ends) must be \
                     reclaimed: within 10 seconds a fresh session on the same pipeline must \
                     open.",
    },
    Clause {
        id: "P10",
        title: "Backend-direct order book",
        definition: "The raw session choreography holds frame by frame, driven without any \
                     client-side manners in between: every request is answered with its own \
                     reply tag, a write to a never-ensured table is refused with a typed error \
                     frame, a publish for a load whose receipt already exists is refused or \
                     answers that same receipt (never a fresh mint), and no reply may arrive \
                     after close is answered.",
    },
    Clause {
        id: "K-S1",
        title: "SIGKILL before the first read, then typed error not hang",
        definition: "The connector is SIGKILLed after the handshake, before any read is opened. \
                     The next call on the dead wire must fail with a typed error within 10 \
                     seconds — a dead connector must fail the wire, never hang it.",
    },
    Clause {
        id: "K-S2",
        title: "SIGKILL mid-read, then typed error not hang",
        definition: "The connector is SIGKILLed mid-read, after the first frame. The stream \
                     must surface a typed error within 10 seconds; a stream that was already \
                     fully in flight ends cleanly and earns an honest Skip naming the fixture.",
    },
    Clause {
        id: "K-S3",
        title: "SIGKILL after the first checkpoint, then typed error not hang",
        definition: "The connector is SIGKILLed after the first checkpoint frame — the resume \
                     boundary. Same promise as K-S2: a typed error within 10 seconds, never a \
                     hang.",
    },
    Clause {
        id: "K-D1",
        title: "SIGKILL after open, then exactly-once on re-run",
        definition: "The connector is SIGKILLed right after the session opens. The dead wire \
                     must fail with a typed error within 10 seconds, and a fresh process \
                     re-driving the same load under a sibling pipeline scope must leave the \
                     table holding exactly the fixture rows — nothing lost, nothing doubled.",
    },
    Clause {
        id: "K-D2",
        title: "SIGKILL after ensure, then exactly-once on re-run",
        definition: "The connector is SIGKILLed right after the table is ensured. Same two \
                     promises as K-D1: a typed error on the dead wire, then exactly-once \
                     convergence on the re-run.",
    },
    Clause {
        id: "K-D3",
        title: "SIGKILL after an accepted write, then exactly-once on re-run",
        definition: "The connector is SIGKILLed right after a write is accepted, before any \
                     commit. Same two promises as K-D1: a typed error on the dead wire, then \
                     exactly-once convergence on the re-run.",
    },
    Clause {
        id: "K-D4",
        title: "SIGKILL between write and publish, then exactly-once on re-run",
        definition: "The connector is SIGKILLed after the receipt query is answered but before \
                     publish is sent — poised at the commit point. Same two promises as K-D1: \
                     a typed error on the dead wire, then exactly-once convergence on the \
                     re-run.",
    },
    Clause {
        id: "K-D5",
        title: "SIGKILL after publish, then exactly-once on re-run",
        definition: "The connector is SIGKILLed right after publish is answered. Same two \
                     promises as K-D1: a typed error on the dead wire, then a re-run that \
                     converges on exactly the fixture rows — a kill that let a committed \
                     row land twice breaks the count as surely as one that lost a row.",
    },
    Clause {
        id: "K-D6",
        title: "SIGKILL after close, then receipt-durable no-op re-run",
        definition: "The connector is SIGKILLed after its session closed cleanly. A re-run on \
                     the SAME pipeline must find the dead process's receipt still durable, \
                     replay instead of re-publishing, and leave the row count unmoved. This \
                     covers cleanly-closed receipt durability only — not crash-resume of a \
                     still-open session on the same pipeline.",
    },
];

/// The fixed short title for `clause` — `None` for an id outside the
/// vocabulary (the render shows such an id bare rather than inventing
/// a title for it).
pub fn clause_title(clause: &str) -> Option<&'static str> {
    CLAUSES
        .iter()
        .find(|entry| entry.id == clause)
        .map(|entry| entry.title)
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
    /// `PASS P1 (<title>)` / `FAIL S1 (<title>): <why>` /
    /// `SKIP K-D4 (<title>): <why>` — the bin's stdout contract, pinned
    /// full-string by the report tests. The title is the clause's fixed
    /// short name from [`CLAUSES`]; an id outside the vocabulary
    /// renders bare (foreign ids are kept, never dressed up).
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        for entry in &self.entries {
            let id = entry.clause;
            let heading = match clause_title(id) {
                Some(title) => format!("{id} ({title})"),
                None => id.to_string(),
            };
            match &entry.verdict {
                Verdict::Pass => writeln!(out, "PASS {heading}"),
                Verdict::Fail(why) => writeln!(out, "FAIL {heading}: {why}"),
                Verdict::Skip(why) => writeln!(out, "SKIP {heading}: {why}"),
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
            "FAIL S1 (checkpoint resume law): the resume law broke\n\
             PASS S2 (checkpoint coverage)\n\
             PASS S4 (prompt cancellation)\n"
        );
        assert!(!report.passed());
    }

    /// No failures at all: every asserted clause passes, in asserted
    /// order.
    #[test]
    fn absorb_of_no_failures_passes_every_asserted_clause() {
        let mut report = Report::default();
        report.absorb(vec![], &["S1", "S2", "S4"]);
        assert_eq!(
            report.render_text(),
            "PASS S1 (checkpoint resume law)\n\
             PASS S2 (checkpoint coverage)\n\
             PASS S4 (prompt cancellation)\n"
        );
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
            "FAIL S1 (checkpoint resume law): stream `a`: content differs\n\
             FAIL S1 (checkpoint resume law): stream `b`: content differs\n\
             PASS S2 (checkpoint coverage)\n\
             FAIL D3 (idempotent commit receipts): a clause this fold was not asked to \
             assert\n"
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

    /// The vocabulary covers EXACTLY the ids the certify paths can emit
    /// — both directions: a clause the code reports but the table does
    /// not know would render (and `--explain`) without a title, and a
    /// table row no path can emit would document a clause that never
    /// runs. The expected set is the union of the crate's own clause
    /// constants, never a hand list, so a clause added to any suite
    /// fails HERE until the vocabulary learns it.
    #[test]
    fn the_clause_vocabulary_matches_the_emittable_ids_exactly() {
        use std::collections::BTreeSet;

        let mut emittable: BTreeSet<&str> = BTreeSet::new();
        emittable.extend(crate::target::GENERIC_CLAUSES);
        emittable.extend(crate::wire::SOURCE_WIRE_CLAUSES);
        emittable.extend(crate::wire::DEST_WIRE_CLAUSES);
        emittable.extend(crate::source::SOURCE_CLAUSES);
        emittable.extend(crate::destination::DEST_CLAUSES);
        emittable.extend(crate::destination::SESSION_CLAUSES);
        emittable.extend(crate::kill::SOURCE_KILL_CLAUSES);
        emittable.extend(crate::kill::DEST_KILL_CLAUSES);

        let table: BTreeSet<&str> = CLAUSES.iter().map(|clause| clause.id).collect();
        assert_eq!(
            table, emittable,
            "the clause vocabulary and the emittable id set must agree"
        );
    }
}
