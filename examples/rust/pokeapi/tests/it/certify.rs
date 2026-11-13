//! Certification in process: the source clauses that apply pass, and the rest do not apply.

use rdlt_connector::testing::{Outcome, certify_source};
use serde_json::json;

use crate::server::Api;

#[tokio::test]
async fn the_source_passes_every_clause_that_applies_to_it() {
    let api = Api::serve(60);
    let config = json!({ "base_url": api.url, "page_size": 7, "limit": 40 });
    let report = certify_source::<pokeapi::PokeApi>(config).await;
    report.assert_passed();
    let clauses = |kept: fn(&Outcome) -> bool| {
        let results = report.results.iter().filter(|result| kept(&result.outcome));
        results.map(|result| result.clause.id).collect::<Vec<_>>()
    };
    let passed = clauses(|outcome| matches!(outcome, Outcome::Passed));
    assert_eq!(
        passed,
        ["S-CHECK", "S-DISCOVER", "S-PLAN", "S-RESUME", "S-STOP"]
    );
    // Its one partition never ends, it checkpoints only after a page, and it keeps no position
    // outside the engine.
    let inapplicable = clauses(|outcome| matches!(outcome, Outcome::Inapplicable(_)));
    assert_eq!(inapplicable, ["S-PARTITION", "S-BARRIER", "S-ACK"]);
}
