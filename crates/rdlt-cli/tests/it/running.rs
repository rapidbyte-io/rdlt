//! `rdlt run`'s outputs: one line per event off a terminal, NDJSON events, the report file.

use crate::verbs::{memory_document, reference_connectors};

#[tokio::test]
async fn a_run_writes_its_events_as_ndjson_and_its_report_where_asked() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "told", "[{ id: 1 }, { id: 2 }]");
    let report = scratch.path().join("report.json");
    let output = scratch
        .rdlt(&[
            "run",
            document.to_str().unwrap(),
            "--events",
            "-",
            "--report",
            report.to_str().unwrap(),
        ])
        .await;
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines: Vec<serde_json::Value> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).expect("a JSON line"))
        .collect();
    let seqs: Vec<u64> = lines
        .iter()
        .filter_map(|line| line["seq"].as_u64())
        .collect();
    assert_eq!(
        seqs,
        (1..=u64::try_from(seqs.len()).unwrap()).collect::<Vec<_>>()
    );
    assert_eq!(
        lines.last().unwrap()["outcome"]["report"]["status"],
        "succeeded"
    );
    let written: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
    assert_eq!(written["rows"], 2);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("committed"),
        "one line per event off a terminal"
    );
}

/// A report path that cannot be written fails before anything is placed.
#[tokio::test]
async fn an_unwritable_report_path_fails_before_the_run_starts() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "unwritten", "[{ id: 1 }]");
    let output = scratch
        .rdlt(&[
            "run",
            document.to_str().unwrap(),
            "--report",
            "/nonexistent/dir/report.json",
        ])
        .await;
    assert_eq!(output.status.code(), Some(74));
    assert!(
        !scratch.path().join("unwritten.db").exists(),
        "nothing was loaded"
    );
}
