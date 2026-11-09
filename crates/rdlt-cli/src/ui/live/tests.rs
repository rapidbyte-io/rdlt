use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use rdlt::{
    Counters, MemoryHeld, OpenWaits, Partitions, Phase, PipelineId, Report, RunStatus, Status,
    StreamReport,
};

use super::*;
use crate::args::Color;
use crate::ui::{ASCII, Asked, Found, UNICODE};

fn status(streams: &[(&str, u64)], phase: Phase, partitions: Partitions) -> Status {
    let report = Report {
        pipeline: PipelineId::parse("orders").expect("a pipeline's name"),
        status: RunStatus::Running,
        attempts: Vec::new(),
        attempted: 1,
        elapsed: Duration::from_secs(10),
        rows: 1_500,
        bytes: 3_000,
        commits: 2,
        peak_memory: 0,
        counters: Counters::default(),
        streams: streams
            .iter()
            .map(|(name, rows)| {
                let report = StreamReport {
                    rows: *rows,
                    ..StreamReport::default()
                };
                ((*name).to_owned(), report)
            })
            .collect(),
    };
    Status {
        phase,
        since: SystemTime::UNIX_EPOCH,
        partitions,
        quietest: None,
        waiting: OpenWaits::default(),
        memory: MemoryHeld::default(),
        writers: Vec::new(),
        streams: BTreeMap::new(),
        report,
    }
}

fn progress(status: Status, stop: Option<Stop>) -> View {
    let title = Arc::new(RunTitle {
        pipeline: "orders".to_owned(),
        source: "postgres".to_owned(),
        destination: "sqlite".to_owned(),
    });
    View::Run(Box::new(RunProgress::new(title, status, stop)))
}

fn looks() -> Looks {
    Looks::resolved(
        Asked {
            color: Color::Never,
            quiet: false,
            events_on_stdout: false,
            verbose_logs: false,
        },
        &Found {
            lc_all: Some("C.UTF-8".to_owned()),
            ..Found::default()
        },
    )
}

const ROOMY: Size = Size {
    rows: 40,
    columns: 200,
};

#[test]
fn a_run_s_first_lines_say_where_it_goes_and_how_it_stands() {
    let reading = Partitions {
        reading: 2,
        waiting: 1,
        ended: 5,
    };
    let view = progress(
        status(&[("a", 10), ("b", 20)], Phase::Reading, reading),
        None,
    );
    let lines = view.lines(0, looks(), ROOMY);
    assert_eq!(lines[0], "⠋ orders  postgres → sqlite  00:10");
    assert_eq!(
        lines[1],
        "  Reading    1,500 rows committed · 150 rows/s · 2 commits · 5/8 partitions"
    );
    assert_eq!(lines[2], "  a       10 rows committed");
    assert_eq!(lines[3], "  b       20 rows committed");
    assert_eq!(lines.len(), 4);
}

#[test]
fn a_stream_carries_no_mark_of_being_done() {
    let ended = Partitions {
        reading: 0,
        waiting: 0,
        ended: 4,
    };
    let lines = progress(status(&[("a", 1)], Phase::Reading, ended), None).lines(1, looks(), ROOMY);
    assert_eq!(lines[2], "  a       1 row committed");
}

#[test]
fn the_counts_stay_put_through_a_commit_and_its_step_comes_last() {
    let reading = progress(status(&[], Phase::Reading, Partitions::default()), None);
    let committing = progress(
        status(
            &[],
            Phase::Committing {
                seq: 1,
                step: Step::Flush,
                tries: 1,
            },
            Partitions::default(),
        ),
        None,
    );
    let read = reading.lines(0, looks(), ROOMY);
    let commit = committing.lines(0, looks(), ROOMY);
    assert_eq!(
        read[1],
        "  Reading    1,500 rows committed · 150 rows/s · 2 commits"
    );
    assert_eq!(
        commit[1],
        "  Committing 1,500 rows committed · 150 rows/s · 2 commits · flush"
    );
}

#[test]
fn a_press_replaces_the_phase_line() {
    let first = progress(
        status(&[], Phase::Reading, Partitions::default()),
        Some(Stop::AfterCommit),
    );
    assert_eq!(
        first.lines(0, looks(), ROOMY)[1],
        "  Stopping after the next commit (Ctrl-C again to stop now)"
    );
    assert!(first.stopping());
    let second = progress(
        status(&[], Phase::Reading, Partitions::default()),
        Some(Stop::Now),
    );
    assert_eq!(second.lines(0, looks(), ROOMY)[1], "  Stopping now");
    let running = progress(status(&[], Phase::Reading, Partitions::default()), None);
    assert!(!running.stopping());
}

#[test]
fn a_task_shows_beside_a_spinner_and_cannot_steer_the_terminal() {
    let task = View::Task("Placing\u{1b}[2J connectors".to_owned());
    let lines = task.lines(3, looks(), ROOMY);
    assert_eq!(lines, ["⠸ Placing\\u{1b}[2J connectors"]);
}

#[test]
fn streams_past_the_terminal_s_height_collapse_into_a_count() {
    let streams: Vec<(String, u64)> = (0..20).map(|index| (format!("s{index:02}"), 1)).collect();
    let refs: Vec<(&str, u64)> = streams
        .iter()
        .map(|(name, rows)| (name.as_str(), *rows))
        .collect();
    let view = progress(status(&refs, Phase::Reading, Partitions::default()), None);
    let short = Size {
        rows: 6,
        columns: 200,
    };
    let lines = view.lines(0, looks(), short);
    assert_eq!(lines.len(), 5, "one row stays free: {lines:?}");
    assert_eq!(lines[3], "  s01       1 row committed");
    assert_eq!(lines[4], "  +18 more");
    let tiny = Size {
        rows: 3,
        columns: 200,
    };
    let lines = view.lines(0, looks(), tiny);
    assert_eq!(lines.len(), 2, "only header and phase fit: {lines:?}");
    let fitting = Size {
        rows: 30,
        columns: 200,
    };
    let lines = view.lines(0, looks(), fitting);
    assert_eq!(lines.len(), 22, "all fit: {lines:?}");
    assert!(lines.iter().all(|line| !line.contains("more")), "{lines:?}");
    let exact = Size {
        rows: 23,
        columns: 200,
    };
    assert_eq!(
        view.lines(0, looks(), exact).len(),
        22,
        "exactly as many as fit"
    );
}

#[test]
fn every_line_is_cut_to_the_terminal_s_width() {
    let view = progress(
        status(
            &[("a_long_stream_name", 7)],
            Phase::Reading,
            Partitions::default(),
        ),
        None,
    );
    let narrow = Size {
        rows: 40,
        columns: 20,
    };
    for line in view.lines(0, looks(), narrow) {
        assert!(console::measure_text_width(&line) < 20, "{line}");
    }
}

#[test]
fn each_phase_has_its_words() {
    let backoff = Phase::BackingOff {
        until: SystemTime::now() + Duration::from_secs(90),
    };
    assert_eq!(phase_words(&Phase::Opening, 1), "Opening");
    assert_eq!(phase_words(&Phase::Reading, 1), "Reading");
    assert_eq!(phase_words(&Phase::Stopping, 1), "Stopping");
    // The seconds round up, and a loaded runner may stall a moment before asking.
    let seconds: u64 = phase_words(&backoff, 3)
        .strip_prefix("Backing off ")
        .and_then(|rest| rest.strip_suffix("s (attempt 3)"))
        .expect("the words of a back-off")
        .parse()
        .expect("whole seconds");
    assert!((80..=90).contains(&seconds), "{seconds}");
    let moment = Phase::BackingOff {
        until: SystemTime::now() + Duration::from_millis(1),
    };
    assert_eq!(
        phase_words(&moment, 1),
        "Backing off 1s (attempt 1)",
        "under a second rounds up"
    );
    let gone = Phase::BackingOff {
        until: SystemTime::UNIX_EPOCH,
    };
    assert_eq!(phase_words(&gone, 2), "Backing off 0s (attempt 2)");
    for (step, word) in [
        (Step::Barrier, "barrier"),
        (Step::Flush, "flush"),
        (Step::Commit, "commit"),
        (Step::Ack, "ack"),
    ] {
        assert_eq!(step_word(step), word);
    }
    let committing = Phase::Committing {
        seq: 1,
        step: Step::Ack,
        tries: 1,
    };
    assert_eq!(phase_words(&committing, 1), "Committing");
}

#[test]
fn a_back_off_names_the_attempt_that_comes_next() {
    let until = SystemTime::now() + Duration::from_secs(5);
    let backing = progress(
        status(&[], Phase::BackingOff { until }, Partitions::default()),
        None,
    );
    let lines = backing.lines(0, looks(), ROOMY);
    assert!(
        lines[1].contains("(attempt 2)"),
        "one attempt ran: {lines:?}"
    );
}

#[test]
fn counts_give_no_denominator_before_any_partition_is_known() {
    let none = status(&[], Phase::Reading, Partitions::default());
    assert_eq!(
        counts(&none, "·"),
        "1,500 rows committed · 150 rows/s · 2 commits"
    );
    let known = Partitions {
        reading: 1,
        waiting: 2,
        ended: 3,
    };
    let counted = status(&[], Phase::Reading, known);
    assert!(
        counts(&counted, "-").ends_with("- 3/6 partitions"),
        "{}",
        counts(&counted, "-")
    );
}

#[test]
fn a_rate_is_scaled_and_none_before_any_time_has_passed() {
    let second = Duration::from_secs(1);
    assert_eq!(rate(1_500, Duration::ZERO), None);
    assert_eq!(rate(0, second), None, "no rate before a row is committed");
    assert_eq!(rate(150, second).as_deref(), Some("150 rows/s"));
    assert_eq!(rate(999, second).as_deref(), Some("999 rows/s"));
    assert_eq!(rate(1_000, second).as_deref(), Some("1.0k rows/s"));
    assert_eq!(rate(999_900, second).as_deref(), Some("999.9k rows/s"));
    assert_eq!(rate(1_000_000, second).as_deref(), Some("1.0M rows/s"));
    assert_eq!(rate(1_500, second).as_deref(), Some("1.5k rows/s"));
    assert_eq!(rate(84_500, second).as_deref(), Some("84.5k rows/s"));
    assert_eq!(rate(2_500_000, second).as_deref(), Some("2.5M rows/s"));
}

#[test]
fn the_clock_reads_minutes_and_seconds_then_hours() {
    assert_eq!(clock(Duration::from_secs(12)), "00:12");
    assert_eq!(clock(Duration::from_secs(95)), "01:35");
    assert_eq!(clock(Duration::from_secs(60)), "01:00");
    assert_eq!(clock(Duration::from_secs(3_599)), "59:59");
    assert_eq!(clock(Duration::from_secs(3_600)), "1:00:00");
    assert_eq!(clock(Duration::from_secs(3_725)), "1:02:05");
}

#[test]
fn the_ascii_marks_differ_from_the_unicode_ones() {
    assert_ne!(ASCII.ok, UNICODE.ok);
    assert_eq!(ASCII.spinner.len(), 4);
    assert_eq!(UNICODE.spinner.len(), 10);
}
