//! Connectors that break one rule of the protocol each: its clause fails, and the others hold.

mod fake;

use rdlt_certify::{Outcome, PROTOCOL_CLAUSES, Target, certify_source};

use fake::{BROKEN, served};

#[tokio::test]
async fn each_protocol_clause_fails_a_connector_that_breaks_it_and_only_that() {
    for (fault, broken) in BROKEN {
        let target = Target::connected(move || Box::pin(async move { served(fault) }));
        let report = certify_source(&target, serde_json::json!({})).await;
        for clause in PROTOCOL_CLAUSES {
            let outcome = report.outcome(clause.id);
            if clause.id == broken {
                assert!(
                    matches!(outcome, Some(Outcome::Failed(_))),
                    "{fault:?} breaks {broken}: {report}"
                );
            } else {
                assert!(
                    matches!(outcome, Some(Outcome::Passed | Outcome::Skipped(_))),
                    "{fault:?} breaks {broken} alone, not {}: {report}",
                    clause.id
                );
            }
        }
    }
}
