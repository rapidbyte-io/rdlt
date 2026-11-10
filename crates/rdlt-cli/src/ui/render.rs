//! What a verb's result looks like where standard output is a terminal, and off one.

#[cfg(test)]
mod tests;

use std::path::Path;
use std::time::Duration;

use indicatif::{HumanBytes, HumanCount, HumanDuration};
use rdlt::RunStatus;

use super::live::rate;
use super::{Tone, Ui, counted, fit, short_id, wrapped};
use crate::output::shown;

/// A local connector the host finds: its binary's file name, and its path.
pub(crate) struct LocalConnector {
    pub(crate) name: String,
    pub(crate) path: String,
}

/// How the host runs local connectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Sandboxing {
    Sandboxed,
    Trusted,
}

/// Whether the host reaches remote connectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Remotes {
    Reached,
    NotReached,
}

/// One column of a table's rows, its cells already shown.
struct Column<'a> {
    header: &'a str,
    cells: Vec<String>,
}

/// A line of a result off a terminal: shown, and in `tone` where a colour asks for it.
pub(crate) fn plain_line(ui: &Ui, tone: Option<Tone>, line: &str) -> String {
    let line = shown(line);
    match tone {
        Some(tone) => ui.looks().out().paint(tone, line),
        None => line,
    }
}

/// `✓ orders  postgres → sqlite · 2 streams: a, b`.
pub(crate) fn check(checked: &rdlt::Checked, ui: &Ui) -> String {
    let brush = ui.looks().out();
    let marks = ui.looks().marks();
    let names: Vec<String> = checked.streams.iter().map(shown).collect();
    let line = format!(
        "{} {}  {} {} {} {} {}: {}",
        brush.paint(Tone::Good, marks.ok),
        brush.paint(Tone::Name, shown(&checked.pipeline)),
        short_id(&checked.source.id),
        marks.arrow,
        short_id(&checked.destination.id),
        marks.dot,
        counted(
            checked.streams.len().try_into().unwrap_or(u64::MAX),
            "stream",
            "streams"
        ),
        names.join(", ")
    );
    wrapped_block(&[line])
}

/// The end of a run, on standard output.
pub(crate) fn run_summary(report: &rdlt::Report, ui: &Ui) -> String {
    let brush = ui.looks().out();
    let marks = ui.looks().marks();
    let (tone, mark, word) = match report.status {
        RunStatus::Succeeded => (Tone::Good, marks.ok, "Succeeded"),
        RunStatus::Stopped => (Tone::Warn, marks.stop, "Stopped"),
        RunStatus::Cancelled => (Tone::Warn, marks.stop, "Cancelled"),
        RunStatus::Failed => (Tone::Bad, marks.fail, "Failed"),
        RunStatus::Running => (Tone::Warn, marks.skip, "Running"),
    };
    let mut lines = vec![format!(
        "{}  {}",
        brush.paint(tone, format!("{mark} {word}")),
        brush.paint(Tone::Name, shown(&report.pipeline)),
    )];
    // The streams' own bytes, the measure the table below shows, so the two add up.
    let bytes: u64 = report.streams.values().map(|stream| stream.bytes).sum();
    let mut totals = vec![
        counted(report.rows, "row", "rows"),
        HumanBytes(bytes).to_string(),
        counted(report.commits, "commit", "commits"),
        took(report.elapsed),
    ];
    totals.extend(rate(report.rows, report.elapsed));
    lines.push(format!("  {}", totals.join(&format!(" {} ", marks.dot))));
    if matches!(report.status, RunStatus::Stopped | RunStatus::Cancelled) && !ui.quiet() {
        lines.push("  Run it again to continue from the last commit.".to_owned());
    }
    let mut text = wrapped_block(&lines);
    if !report.streams.is_empty() {
        text.push('\n');
        text.push_str(&streams_table(report, ui));
    }
    text
}

/// The table of what each stream committed.
fn streams_table(report: &rdlt::Report, ui: &Ui) -> String {
    let streams = report.streams.values();
    let columns = [
        Column {
            header: "STREAM",
            cells: report.streams.keys().map(shown).collect(),
        },
        Column {
            header: "ROWS",
            cells: streams
                .clone()
                .map(|stream| HumanCount(stream.rows).to_string())
                .collect(),
        },
        Column {
            header: "BYTES",
            cells: streams
                .clone()
                .map(|stream| HumanBytes(stream.bytes).to_string())
                .collect(),
        },
        Column {
            header: "COMMITS",
            cells: streams
                .map(|stream| HumanCount(stream.commits).to_string())
                .collect(),
        },
    ];
    table(&columns, ui)
}

/// `discover`'s table of the catalog's streams.
pub(crate) fn catalog(catalog: &rdlt::Catalog, ui: &Ui) -> String {
    let streams: Vec<_> = catalog.iter().collect();
    let columns = [
        Column {
            header: "STREAM",
            cells: streams.iter().map(|stream| shown(stream.name())).collect(),
        },
        Column {
            header: "READS",
            cells: streams
                .iter()
                .map(|stream| {
                    let modes: Vec<String> = stream
                        .read_modes()
                        .iter()
                        .map(|mode| format!("{mode:?}").to_lowercase())
                        .collect();
                    modes.join(", ")
                })
                .collect(),
        },
        Column {
            header: "KEY",
            cells: streams
                .iter()
                .map(|stream| match stream.primary_key() {
                    Some(key) => {
                        let columns: Vec<String> = key.iter().map(shown).collect();
                        columns.join(", ")
                    }
                    None => "-".to_owned(),
                })
                .collect(),
        },
        Column {
            header: "COLUMNS",
            cells: streams
                .iter()
                .map(|stream| match stream.schema() {
                    Some(schema) => schema.fields().len().to_string(),
                    None => "-".to_owned(),
                })
                .collect(),
        },
    ];
    table(&columns, ui)
}

/// `connectors`' table, then the sandbox and the remote lines.
pub(crate) fn connectors(
    local: &[LocalConnector],
    sandboxing: Sandboxing,
    remotes: Remotes,
    ui: &Ui,
) -> String {
    let columns = [
        Column {
            header: "CONNECTOR",
            cells: local.iter().map(|found| shown(&found.name)).collect(),
        },
        Column {
            header: "PATH",
            cells: local.iter().map(|found| shown(&found.path)).collect(),
        },
    ];
    let mut text = if local.is_empty() {
        "  No local connectors found.\n".to_owned()
    } else {
        table(&columns, ui)
    };
    let local = match sandboxing {
        Sandboxing::Sandboxed => "in the sandbox",
        Sandboxing::Trusted => "trusted, in no sandbox",
    };
    let remote = match remotes {
        Remotes::Reached => "are reached over mutual TLS",
        Remotes::NotReached => "are not reached",
    };
    text.push('\n');
    text.push_str(&wrapped_block(&[
        format!("  Local connectors run {local}"),
        format!("  Remote connectors {remote}"),
    ]));
    text
}

/// What a reset reset, and the epoch of its session.
pub(crate) fn reset(streams: &[String], dropped: &[String], epoch: u64, ui: &Ui) -> String {
    let brush = ui.looks().out();
    let head = format!(
        "{}  at epoch {epoch}",
        brush.paint(Tone::Good, format!("{} Reset", ui.looks().marks().ok)),
    );
    let mut text = wrapped_block(&[head]);
    let reset_rows = streams.iter().map(|stream| ("stream", stream));
    let dropped_rows = dropped.iter().map(|table| ("table", table));
    let rows: Vec<(&str, &String)> = reset_rows.chain(dropped_rows).collect();
    let columns = [
        Column {
            header: "RESET",
            cells: rows.iter().map(|(kind, _)| (*kind).to_owned()).collect(),
        },
        Column {
            header: "NAME",
            cells: rows.iter().map(|(_, name)| shown(name)).collect(),
        },
    ];
    text.push('\n');
    text.push_str(&table(&columns, ui));
    text
}

/// `✓ Created <path>`, then the commands to run next, unwrapped so that they can be copied.
pub(crate) fn created(path: &Path, next: &[String], ui: &Ui) -> String {
    let brush = ui.looks().out();
    let head = format!(
        "{} Created {}",
        brush.paint(Tone::Good, ui.looks().marks().ok),
        shown(path.display())
    );
    let mut text = wrapped_block(&[head]);
    if !ui.quiet() {
        for command in next {
            text.push_str("  Next: ");
            text.push_str(&shown(command));
            text.push('\n');
        }
    }
    text
}

/// `elapsed` as a summary shows it: to the tenth of a second under a minute.
pub(super) fn took(elapsed: Duration) -> String {
    if elapsed < Duration::from_secs(60) {
        format!("{elapsed:.1?}")
    } else {
        HumanDuration(elapsed).to_string()
    }
}

/// `lines`, each wrapped to standard output's width, wrapped lines indented.
fn wrapped_block(lines: &[String]) -> String {
    let width = super::out_width().saturating_sub(1);
    let mut text = String::new();
    for line in lines {
        for wrapped in wrapped(line, width, "  ") {
            text.push_str(&wrapped);
            text.push('\n');
        }
    }
    text
}

/// A table of `columns`, its lines cut to standard output's width.
fn table(columns: &[Column<'_>], ui: &Ui) -> String {
    let brush = ui.looks().out();
    let marks = ui.looks().marks();
    let width = super::out_width();
    let widths: Vec<usize> = columns
        .iter()
        .map(|column| {
            let cells = column
                .cells
                .iter()
                .map(|cell| console::measure_text_width(cell));
            cells.fold(column.header.len(), usize::max) + 3
        })
        .collect();
    let rows = columns.first().map_or(0, |column| column.cells.len());
    let mut text = String::new();
    let line = |text: &mut String, cell: &dyn Fn(usize) -> String| {
        let mut line = String::from("  ");
        for (index, width) in widths.iter().enumerate() {
            let cell = cell(index);
            line.push_str(&console::pad_str(
                &cell,
                *width,
                console::Alignment::Left,
                None,
            ));
        }
        text.push_str(&fit(line.trim_end(), width, marks));
        text.push('\n');
    };
    line(&mut text, &|index| {
        brush.paint(Tone::Dim, columns[index].header)
    });
    for row in 0..rows {
        line(&mut text, &|index| {
            let cell = &columns[index].cells[row];
            if index == 0 {
                brush.paint(Tone::Name, cell)
            } else {
                cell.clone()
            }
        });
    }
    text
}
