use super::shortfalls;

fn export(lines: (u64, u64), branches: (u64, u64)) -> String {
    format!(
        r#"{{"type":"llvm.coverage.json.export","version":"2.0.1","data":[{{"totals":{{
            "lines":{{"count":{},"covered":{},"percent":0}},
            "branches":{{"count":{},"covered":{},"notcovered":0,"percent":0}}}}}}]}}"#,
        lines.0, lines.1, branches.0, branches.1
    )
}

#[test]
fn coverage_at_or_above_the_thresholds_passes() {
    assert!(
        shortfalls(&export((100, 90), (20, 17)), 90.0, 85.0)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn each_metric_below_its_threshold_is_reported() {
    let found = shortfalls(&export((100, 89), (20, 16)), 90.0, 85.0).unwrap();
    let metrics: Vec<_> = found.iter().map(|s| (s.metric, s.actual)).collect();
    assert_eq!(metrics, vec![("lines", 89.0), ("branches", 80.0)]);
}

#[test]
fn code_without_branches_counts_as_fully_covered() {
    assert!(
        shortfalls(&export((10, 10), (0, 0)), 90.0, 85.0)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn malformed_exports_are_errors() {
    assert!(shortfalls("{}", 90.0, 85.0).is_err());
    assert!(shortfalls(r#"{"data":[]}"#, 90.0, 85.0).is_err());
}
