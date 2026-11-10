use pyo3::types::PyAnyMethods as _;

use crate::destination::NativeDestinationFactory;
use crate::embedded;
use crate::source::NativeSourceFactory;
use crate::transform::NativeTransform;

#[test]
fn a_python_source_s_certification_reports_every_clause_in_its_listed_order() {
    embedded::with(|py| {
        let made = py
            .eval(
                c"__import__('rdlt.connector._source', fromlist=['placed']).placed(__import__('tests.connectors.tickets', fromlist=['Tickets']).Tickets)",
                None,
                None,
            )
            .unwrap();
        let factory: pyo3::PyRef<'_, NativeSourceFactory> =
            made.get_item(0).unwrap().extract().unwrap();
        let config = serde_json::json!({ "pages": 6, "token": "t0ken" }).to_string();
        let report: serde_json::Value =
            serde_json::from_str(&super::certify_source(py, &factory, &config, None).unwrap())
                .unwrap();
        let ids: Vec<&str> = report["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|result| result["clause"].as_str().unwrap())
            .collect();
        let listed: Vec<String> = super::clauses("source")
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, listed);
        let failed: Vec<&serde_json::Value> = report["results"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|result| result["outcome"] == "failed")
            .collect();
        assert!(failed.is_empty(), "{failed:?}");
    });
}

#[test]
fn a_python_transform_s_certification_reports_every_t_clause_in_its_listed_order() {
    embedded::with(|py| {
        let function = py
            .eval(c"lambda batch: batch", None, None)
            .unwrap()
            .unbind();
        let transform =
            NativeTransform::new(py, "unchanged".to_owned(), function, false, None).unwrap();
        let batch = py
            .eval(
                c"__import__('pyarrow').record_batch({'n': [1, 2, 3]})",
                None,
                None,
            )
            .unwrap();
        let answered = super::certify_transform(py, &transform, vec![batch]).unwrap();
        let report: serde_json::Value = serde_json::from_str(&answered).unwrap();
        assert_eq!(report["connector"], "unchanged");
        let results = report["results"].as_array().unwrap();
        let ids: Vec<&str> = results
            .iter()
            .map(|result| result["clause"].as_str().unwrap())
            .collect();
        let listed: Vec<String> = super::clauses("transform")
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, listed);
        let rows = results
            .iter()
            .find(|result| result["clause"] == "T-ROWS")
            .unwrap();
        assert_eq!(rows["outcome"], "passed", "{rows}");
    });
}

#[test]
fn a_python_destination_s_certification_reports_every_clause_in_its_listed_order() {
    embedded::with(|py| {
        let made = py
            .eval(
                c"__import__('rdlt.connector._destination', fromlist=['placed']).placed(__import__('tests.connectors.memory', fromlist=['Memory']).Memory)",
                None,
                None,
            )
            .unwrap();
        let factory: pyo3::PyRef<'_, NativeDestinationFactory> =
            made.get_item(0).unwrap().extract().unwrap();
        let config = serde_json::json!({ "store": "certified-in-rust" }).to_string();
        let certified = super::certify_destination(py, &factory, &config, None).unwrap();
        let report: serde_json::Value = serde_json::from_str(&certified).unwrap();
        let results = report["results"].as_array().unwrap();
        let ids: Vec<&str> = results
            .iter()
            .map(|result| result["clause"].as_str().unwrap())
            .collect();
        let listed: Vec<String> = super::clauses("destination")
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, listed);
        let outcome = |id: &str| {
            results
                .iter()
                .find(|result| result["clause"] == id)
                .map(|result| result["outcome"].clone())
        };
        // Read back through the factory, the clauses that compare what was published run.
        for id in [
            "D-COMMIT",
            "D-LANDED",
            "D-STATE",
            "D-SCHEMA",
            "K-DESTINATION",
        ] {
            assert_eq!(
                outcome(id),
                Some(serde_json::json!("passed")),
                "{id}: {report}"
            );
        }
        let failed: Vec<&serde_json::Value> = results
            .iter()
            .filter(|result| result["outcome"] == "failed")
            .collect();
        assert!(failed.is_empty(), "{failed:?}");
    });
}

#[test]
fn a_launcher_s_kill_clause_says_its_rows_are_read_back_in_this_process() {
    use rdlt_certify::{ClauseResult, DESTINATION_CLAUSES, KILL_CLAUSES, Outcome, Reason, Report};
    let result = |clause, outcome| ClauseResult {
        clause,
        outcome,
        note: None,
    };
    let failed = || Outcome::Failed(Reason::new("row 0 was never published"));
    let report = Report {
        connector: "io.example.memory".to_owned(),
        results: vec![
            result(DESTINATION_CLAUSES[0], failed()),
            result(KILL_CLAUSES[1], failed()),
        ],
    };
    let said = super::read_back_here(report);
    assert_eq!(said.results[0].outcome, failed());
    assert_eq!(
        said.results[1].outcome,
        Outcome::Failed(Reason::new(
            "what the launcher published is read back in this process: row 0 was never published"
        ))
    );
    let unobserved = Report {
        connector: "io.example.memory".to_owned(),
        results: vec![result(
            KILL_CLAUSES[1],
            Outcome::Unobserved(Reason::new("no kill landed")),
        )],
    };
    let said = super::read_back_here(unobserved);
    assert_eq!(
        said.results[0].outcome,
        Outcome::Unobserved(Reason::new(
            "what the launcher published is read back in this process: no kill landed"
        ))
    );
    let passed = Report {
        connector: "io.example.memory".to_owned(),
        results: vec![result(KILL_CLAUSES[1], Outcome::Passed)],
    };
    assert_eq!(
        super::read_back_here(passed).results[0].outcome,
        Outcome::Passed
    );
}
