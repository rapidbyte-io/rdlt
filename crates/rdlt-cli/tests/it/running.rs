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
    assert!(seqs.len() >= 3, "an event per line: {lines:?}");
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

/// A file a run was to write keeps what it held until the run starts: a build that fails costs
/// nothing, and a run that starts writes the file anew.
#[tokio::test]
async fn a_failed_build_keeps_a_previous_report_and_a_run_replaces_it() {
    let scratch = reference_connectors();
    let report = scratch.path().join("report.json");
    let longer = "x".repeat(100_000);
    std::fs::write(&report, &longer).unwrap();
    let path = report.to_str().unwrap();
    let broken = scratch.document("broken.yaml", "version: 1\npipeline: broken\n");
    let refused = scratch
        .rdlt(&["run", broken.to_str().unwrap(), "--report", path])
        .await;
    assert_eq!(refused.status.code(), Some(2));
    assert_eq!(std::fs::read_to_string(&report).unwrap(), longer);
    let document = memory_document(&scratch, "replaced", "[{ id: 1 }]");
    let ran = scratch
        .rdlt(&["run", document.to_str().unwrap(), "--report", path])
        .await;
    assert_eq!(ran.status.code(), Some(0));
    let written: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
    assert_eq!(written["rows"], 1);
}

/// An events file gets each event as a line of JSON, in place of what it held.
#[tokio::test]
async fn a_run_writes_its_events_to_a_file_in_place_of_what_it_held() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "filed", "[{ id: 1 }]");
    let events = scratch.path().join("events.ndjson");
    std::fs::write(&events, "#".repeat(100_000)).unwrap();
    let output = scratch
        .rdlt(&[
            "run",
            document.to_str().unwrap(),
            "--events",
            events.to_str().unwrap(),
        ])
        .await;
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = std::fs::read_to_string(&events).unwrap();
    assert!(!text.contains('#'), "what it held is gone: {text}");
    let seqs: Vec<u64> = text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("a JSON line"))
        .filter_map(|line| line["seq"].as_u64())
        .collect();
    assert!(seqs.len() >= 3, "{text}");
    let counted: Vec<u64> = (1..=u64::try_from(seqs.len()).unwrap()).collect();
    assert_eq!(seqs, counted, "{text}");
}
