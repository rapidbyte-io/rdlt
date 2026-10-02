//! Certification over the wire: connectors served in this process, spawned, and listening.

#![forbid(unsafe_code)]

mod cli;
mod faults;
mod guarded;
mod killed;
mod listening;
mod probing;
mod read_back;
mod served;
mod spawned;
mod unmet;
mod unreached;

/// How long the tests that are not of `P-CREDIT` have a read whose credit is spent watched after
/// each grant: a source that waits for credit sends nothing however long it is watched.
const BRIEF: std::time::Duration = std::time::Duration::from_millis(50);

/// Has `child`, spawned to lead a process group, killed with its group when this test's
/// process ends, however it ends.
pub(crate) fn guarded(child: &tokio::process::Child) {
    let leader = child.id().expect("the child runs");
    rdlt_testkit::process::guard(leader).expect("the child is guarded");
}

/// The clauses of `report` that apply and were not observed, when no clause failed and one
/// passed.
fn unobserved(report: &rdlt_certify::Report) -> Vec<&'static str> {
    report.assert_none_failed();
    report.unobserved().map(|result| result.clause.id).collect()
}
