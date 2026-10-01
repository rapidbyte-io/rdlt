use rdlt_certify::{Clause, ClauseResult, Outcome, Report, Verdict};

use super::{FINDINGS, INCOMPLETE, PASSED, Require, bound, code, ran, verdict, within};

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
            PASSED
        );
        assert_eq!(
            code(Verdict::Failed, require, std::slice::from_ref(&failing)),
            FINDINGS
        );
        // A report none of whose clauses passed certified nothing, whatever is required.
        let reports = [partly.clone(), unseen.clone()];
        assert_eq!(code(Verdict::Incomplete, require, &reports), INCOMPLETE);
        assert_eq!(
            code(Verdict::Incomplete, require, std::slice::from_ref(&unseen)),
            INCOMPLETE
        );
    }
    let reports = [partly, passing];
    assert_eq!(
        code(Verdict::Incomplete, Require::Complete, &reports),
        INCOMPLETE
    );
    assert_eq!(
        code(Verdict::Incomplete, Require::Partial, &reports),
        PASSED
    );
    assert_eq!([PASSED, FINDINGS, INCOMPLETE], [0, 1, 2]);
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
