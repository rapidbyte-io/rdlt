//! Certification over the wire: connectors served in this process, spawned, and listening.

#![forbid(unsafe_code)]

mod cli;
mod faults;
mod killed;
mod listening;
mod probing;
mod read_back;
mod served;
mod spawned;
mod unmet;
mod unreached;

/// The clauses of `report` that apply and were not observed, when no clause failed and one
/// passed.
fn unobserved(report: &rdlt_certify::Report) -> Vec<&'static str> {
    report.assert_none_failed();
    report.unobserved().map(|result| result.clause.id).collect()
}
