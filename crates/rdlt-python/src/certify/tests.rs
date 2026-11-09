use pyo3::types::PyAnyMethods as _;

use crate::embedded;
use crate::source::NativeSourceFactory;
use crate::transform::NativeTransform;

#[test]
fn a_python_source_s_certification_reports_every_clause_in_its_listed_order() {
    embedded::with(|py| {
        let made = py
            .eval(
                c"__import__('rdlt.connector._source', fromlist=['factory']).factory(__import__('tests.connectors.tickets', fromlist=['Tickets']).Tickets)",
                None,
                None,
            )
            .unwrap();
        let factory: pyo3::PyRef<'_, NativeSourceFactory> = made.extract().unwrap();
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
