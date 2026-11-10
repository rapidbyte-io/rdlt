use std::collections::BTreeMap;
use std::time::Duration;

use indicatif::{HumanBytes, HumanDuration};
use rdlt::{Counters, PipelineId, Report, RunStatus, StreamReport};

use super::{Column, run_summary, table, took};
use crate::ui::Ui;

fn report(streams: &[(&str, u64)]) -> Report {
    Report {
        pipeline: PipelineId::parse("orders").expect("a pipeline's name"),
        status: RunStatus::Succeeded,
        attempts: Vec::new(),
        attempted: 1,
        elapsed: Duration::from_secs(2),
        rows: 7,
        bytes: 99_000_000,
        commits: 1,
        peak_memory: 0,
        counters: Counters::default(),
        streams: streams
            .iter()
            .map(|(name, bytes)| {
                let report = StreamReport {
                    rows: 1,
                    bytes: *bytes,
                    ..StreamReport::default()
                };
                ((*name).to_owned(), report)
            })
            .collect::<BTreeMap<_, _>>(),
    }
}

#[test]
fn a_summary_s_bytes_are_those_its_table_adds_up_to() {
    let text = run_summary(&report(&[("a", 1024), ("b", 2048)]), &Ui::for_tests());
    assert!(text.contains(&HumanBytes(3072).to_string()), "{text}");
    assert!(
        !text.contains(&HumanBytes(99_000_000).to_string()),
        "the receipts' measure is not the table's: {text}"
    );
}

#[test]
fn a_summary_s_time_is_to_the_tenth_under_a_minute() {
    assert_eq!(took(Duration::from_millis(59_900)), "59.9s");
    assert_eq!(
        took(Duration::from_secs(60)),
        HumanDuration(Duration::from_secs(60)).to_string()
    );
}

#[test]
fn a_table_names_its_rows_in_the_first_column_alone() {
    let columns = [
        Column {
            header: "A",
            cells: vec!["alpha".to_owned()],
        },
        Column {
            header: "B",
            cells: vec!["beta".to_owned()],
        },
    ];
    let text = table(
        &columns,
        &Ui::of_color_for_tests(crate::args::Color::Always),
    );
    assert!(text.contains("\u{1b}[36malpha"), "{text:?}");
    assert!(!text.contains("\u{1b}[36mbeta"), "{text:?}");
}
