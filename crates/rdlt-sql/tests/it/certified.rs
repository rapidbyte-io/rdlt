//! The `T` clauses over drawn queries.

use proptest::prelude::*;
use rdlt_certify::{TransformInputs, certify_transform};
use rdlt_sql::Sql;

use crate::queries::{EXPRESSIONS, PREDICATES, batches, large, query};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(32)))]

    #[test]
    fn every_drawn_query_meets_every_t_clause(
        batches in batches(),
        expression in proptest::sample::select(EXPRESSIONS),
        predicate in proptest::option::of(proptest::sample::select(PREDICATES)),
    ) {
        let text = query(expression, predicate);
        let sql = Sql::parse(&text).unwrap_or_else(|error| panic!("{text}: {error}"));
        let inputs = TransformInputs {
            batches,
            keys: vec!["n".to_owned()],
            change_time: None,
        };
        let report = certify_transform(&sql, &inputs);
        prop_assert!(report.passed(), "{text}: {report}");
    }
}

#[test]
fn every_expression_and_predicate_meets_every_t_clause() {
    let drawn = rdlt_testkit::draw::draw(&batches(), 7);
    let predicates = PREDICATES.iter().copied().map(Some).chain([None]);
    for expression in EXPRESSIONS {
        for predicate in predicates.clone() {
            let text = query(expression, predicate);
            let sql = Sql::parse(&text).unwrap_or_else(|error| panic!("{text}: {error}"));
            let inputs = TransformInputs {
                batches: drawn.clone(),
                keys: vec!["n".to_owned()],
                change_time: None,
            };
            certify_transform(&sql, &inputs).assert_passed();
        }
    }
}

#[test]
fn every_expression_meets_every_t_clause_on_a_megabyte_of_text() {
    for seed in 0..3 {
        let batch = rdlt_testkit::draw::draw(&large(), seed);
        for expression in EXPRESSIONS {
            let text = query(expression, None);
            let sql = Sql::parse(&text).unwrap_or_else(|error| panic!("{text}: {error}"));
            let inputs = TransformInputs {
                batches: vec![batch.clone()],
                keys: vec!["n".to_owned()],
                change_time: None,
            };
            let report = certify_transform(&sql, &inputs);
            assert!(report.passed(), "{text}: {report}");
        }
    }
}
