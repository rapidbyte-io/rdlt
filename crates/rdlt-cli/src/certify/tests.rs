use rdlt_certify::{Clause, ClauseResult, Outcome, Report, Verdict};

use super::session::within;
use super::{Require, Watch, bound, code, ran, verdict};
use crate::Exit;
use crate::output::Mode;
use crate::ui::Ui;

fn report(outcomes: &[Outcome]) -> Report {
    let clause = Clause {
        id: "S-CHECK",
        statement: "a statement",
        unless: "",
    };
    Report {
        connector: "test.cli".to_owned(),
        results: outcomes
            .iter()
            .map(|outcome| ClauseResult {
                clause,
                outcome: outcome.clone(),
                note: None,
            })
            .collect(),
    }
}

fn outcomes() -> [Outcome; 4] {
    [
        Outcome::Passed,
        Outcome::Failed("broken".into()),
        Outcome::Unobserved("unseen".into()),
        Outcome::Inapplicable("undeclared".into()),
    ]
}

#[test]
fn reports_amount_to_the_worst_of_their_verdicts() {
    let [passed, failed, unobserved, _] = outcomes();
    let passing = report(std::slice::from_ref(&passed));
    let incomplete = report(&[passed.clone(), unobserved]);
    let failing = report(&[passed, failed]);
    assert_eq!(verdict(&[]), Verdict::Passed);
    assert_eq!(
        verdict(&[passing.clone(), passing.clone()]),
        Verdict::Passed
    );
    assert_eq!(
        verdict(&[passing.clone(), incomplete.clone()]),
        Verdict::Incomplete
    );
    assert_eq!(
        verdict(&[incomplete.clone(), passing.clone()]),
        Verdict::Incomplete
    );
    assert_eq!(
        verdict(&[incomplete.clone(), failing.clone()]),
        Verdict::Failed
    );
    assert_eq!(verdict(&[failing, passing]), Verdict::Failed);
}

#[test]
fn an_incomplete_certification_exits_zero_only_when_part_is_required_and_each_report_passed_a_clause()
 {
    let [passed, failed, unobserved, inapplicable] = outcomes();
    let partly = report(&[passed.clone(), unobserved.clone()]);
    let unseen = report(&[unobserved, inapplicable]);
    let failing = report(&[passed.clone(), failed]);
    let passing = report(&[passed]);
    for require in [Require::Complete, Require::Partial] {
        assert_eq!(
            code(Verdict::Passed, require, std::slice::from_ref(&passing)),
            Exit::Success
        );
        assert_eq!(
            code(Verdict::Failed, require, std::slice::from_ref(&failing)),
            Exit::Findings
        );
        // A report none of whose clauses passed certified nothing, whatever is required.
        let reports = [partly.clone(), unseen.clone()];
        assert_eq!(code(Verdict::Incomplete, require, &reports), Exit::Findings);
        assert_eq!(
            code(Verdict::Incomplete, require, std::slice::from_ref(&unseen)),
            Exit::Findings
        );
    }
    let reports = [partly, passing];
    assert_eq!(
        code(Verdict::Incomplete, Require::Complete, &reports),
        Exit::Findings
    );
    assert_eq!(
        code(Verdict::Incomplete, Require::Partial, &reports),
        Exit::Success
    );
}

#[test]
fn a_role_ran_when_a_clause_of_it_applies() {
    let [passed, failed, unobserved, inapplicable] = outcomes();
    assert!(!ran(&report(&[])));
    assert!(!ran(&report(&[inapplicable.clone(), inapplicable.clone()])));
    for applies in [passed, failed, unobserved] {
        assert!(ran(&report(&[inapplicable.clone(), applies])));
    }
}

#[tokio::test(start_paused = true)]
async fn a_certification_is_cut_at_its_deadline_and_not_before() {
    let slow = |seconds: u64| async move {
        tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
        report(&[Outcome::Passed])
    };
    let until = |seconds: u64| {
        let now = tokio::time::Instant::now().into_std();
        Some(now + std::time::Duration::from_secs(seconds))
    };
    assert!(within(None, slow(1_000_000)).await.is_some());
    assert!(within(until(10), slow(9)).await.is_some());
    assert!(within(until(10), slow(11)).await.is_none());
    assert!(within(until(0), slow(1)).await.is_none());
}

#[test]
fn a_certification_is_bounded_unless_it_is_asked_not_to_be() {
    use std::time::Duration;
    // With no flag, a run ends within an hour, more than a connector that answers needs.
    assert_eq!(bound(None, false), Some(rdlt_certify::RUN_TIMEOUT));
    assert_eq!(rdlt_certify::RUN_TIMEOUT, Duration::from_secs(3600));
    assert_eq!(bound(Some(7200), false), Some(Duration::from_secs(7200)));
    assert_eq!(bound(Some(0), false), Some(Duration::ZERO));
    assert_eq!(
        bound(Some(u64::MAX), false),
        Some(Duration::from_secs(u64::MAX))
    );
    assert_eq!(bound(None, true), None);
}

#[test]
fn a_complete_certification_is_required_unless_part_is_asked_for() {
    use clap::Parser as _;
    let required = |asked: &[&str]| {
        let args = [&["rdlt", "certify", "connector"][..], asked].concat();
        let cli = crate::args::Cli::try_parse_from(args).ok()?;
        match cli.verb {
            crate::args::Verb::Certify(args) => Some(args.require),
            _ => None,
        }
    };
    assert_eq!(required(&[]), Some(Require::Complete));
    assert_eq!(
        required(&["--require", "complete"]),
        Some(Require::Complete)
    );
    assert_eq!(required(&["--require", "partial"]), Some(Require::Partial));
    assert_eq!(required(&["--require", "some"]), None);
}

/// A signal cuts a role short with results the last tick has not shown: every one is waited for,
/// with the painter behind, so a certification stopped from outside has said what it found.
#[test]
fn what_a_signal_cuts_short_has_said_everything_it_found_though_the_painter_is_behind() {
    let (ui, queue) = Ui::queued();
    let observed = rdlt_certify::Observed::new();
    let found = 20;
    for _ in 0..found {
        observed.tell(ClauseResult {
            clause: Clause {
                id: "S-CHECK",
                statement: "a statement",
                unless: "",
            },
            outcome: Outcome::Passed,
            note: None,
        });
    }
    let mut watch = Watch::new(&ui, Mode::Plain, rdlt_connector::Role::Source);
    let reading =
        std::thread::spawn(move || queue.lines(found, std::time::Duration::from_secs(60)));
    watch.drained(&observed);
    let lines = reading.join().expect("the reader ends");
    assert_eq!(lines.len(), found, "{lines:?}");
}

/// A role that applies to nothing is held back, and output that is not a terminal's says it as
/// a report, so neither prints a line for what a signal cut short.
#[test]
fn what_a_signal_cuts_short_says_nothing_of_a_held_role_or_off_the_live_view() {
    let told = |outcome: Outcome| {
        let observed = rdlt_certify::Observed::new();
        observed.tell(ClauseResult {
            clause: Clause {
                id: "S-CHECK",
                statement: "a statement",
                unless: "",
            },
            outcome,
            note: None,
        });
        observed
    };
    let short = std::time::Duration::from_millis(50);
    let (ui, queue) = Ui::queued();
    let held = told(Outcome::Inapplicable("undeclared".into()));
    Watch::new(&ui, Mode::Plain, rdlt_connector::Role::Source).drained(&held);
    assert_eq!(queue.lines(1, short), Vec::<String>::new(), "a held role");
    let found = told(Outcome::Passed);
    Watch::new(&ui, Mode::Json, rdlt_connector::Role::Source).drained(&found);
    assert_eq!(
        queue.lines(1, short),
        Vec::<String>::new(),
        "JSON says it once"
    );
}
