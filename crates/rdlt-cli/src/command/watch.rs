//! The `watch` subcommand: a live monitor over an event log another
//! `rdlt run --events <file>` is writing — the operator's window into a
//! run on another terminal, another host, or another process entirely.
//!
//! The numbers are the CANONICAL fold (`rdlt::metrics::Metrics`) fed
//! the parsed event stream, so what watch shows is exactly what the
//! run's own display shows — never a second folding vocabulary. Final
//! honesty stays where it belongs: the report of the run being
//! watched; this is the live view only.
//!
//! Rendering is plain ANSI (cursor home + clear), deliberately: no TUI
//! dependency, no alternate-screen mode — scrollback survives, and the
//! recorded upgrade door to ratatui (036) needs no contract change.
//! Runs until interrupted (like `tail -f`); Ctrl-C is the exit.

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rdlt::event::PipelineEvent;
use rdlt::metrics::Metrics;

use crate::exit;
use crate::render;

const REDRAW_EVERY: Duration = Duration::from_millis(100);

pub(crate) fn watch(events_path: PathBuf) -> Result<(), exit::Error> {
    if !events_path.is_file() {
        return Err(exit::Error::Usage(format!(
            "{} does not exist yet — start the run with --events first",
            events_path.display()
        )));
    }
    let mut file = std::fs::File::open(&events_path)
        .map_err(|e| exit::Error::Io(format!("opening {}: {e}", events_path.display())))?;
    let mut metrics = Metrics::new();
    let mut partial = String::new();
    let mut events_seen = 0usize;

    loop {
        // Drain whatever arrived since the last tick: bytes appended,
        // split on newlines, each complete line one event. A torn tail
        // waits for its newline rather than parsing half an event.
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)
            .map_err(|e| exit::Error::Io(format!("reading {}: {e}", events_path.display())))?;
        if !buf.is_empty() {
            partial.push_str(&String::from_utf8_lossy(&buf));
            while let Some(newline) = partial.find('\n') {
                let line: String = partial.drain(..=newline).collect();
                let line = line.trim_end();
                if line.is_empty() {
                    continue;
                }
                match serde_json::from_str::<PipelineEvent>(line) {
                    Ok(event) => {
                        metrics.apply(&event);
                        events_seen += 1;
                    }
                    Err(_) => {
                        // One unparseable line is the writer's business
                        // (a future event kind this CLI predates) —
                        // skipped, never fatal, and COUNTED so silence
                        // cannot hide it.
                    }
                }
            }
        }

        render_snapshot(&mut metrics, &events_path, events_seen);
        std::thread::sleep(REDRAW_EVERY);
    }
}

/// One frame: cursor home, clear, then the snapshot's rows. Plain ANSI
/// over stderr so stdout stays untouched (a script piping watch's
/// stdout gets nothing).
fn render_snapshot(metrics: &mut Metrics, source: &Path, events_seen: usize) {
    const CURSOR_HOME_AND_CLEAR: &str = "\x1b[H\x1b[2J";
    let snap = metrics.snapshot();
    let mut frame = String::new();
    frame.push_str(&format!(
        "rdlt watch — {}   (events seen: {events_seen}; ctrl-c exits)\n\n",
        source.display()
    ));
    frame.push_str(&format!(
        "rows read {:>10}   written {:>10}   commits {:>6}{}   retries {}\n",
        snap.rows_read,
        snap.rows_written,
        snap.commits,
        if snap.committing { "+" } else { " " },
        snap.retries
    ));
    if let Some(rate) = snap.rows_per_sec {
        frame.push_str(&format!("rows/s {rate:.0}"));
        if let Some(avg) = snap.rows_per_sec_avg {
            frame.push_str(&format!("   (avg {avg:.0})"));
        }
        frame.push('\n');
    }
    if let Some(seq) = snap.last_commit_seq {
        let since = snap
            .since_last_commit
            .map(|d| format!("{}s ago", d.as_secs()))
            .unwrap_or_else(|| "?".to_string());
        frame.push_str(&format!("last commit #{seq} ({since})\n"));
    }
    if !snap.tables.is_empty() {
        frame.push_str("\ntable                 rows       bytes\n");
        for (table, totals) in &snap.tables {
            frame.push_str(&format!(
                "{:<20} {:>9} {:>12}\n",
                table.as_str(),
                totals.rows_written,
                totals.bytes_written
            ));
        }
    }
    if !snap.streams.is_empty() {
        frame.push_str("\nstream          state      read\n");
        for (stream, state) in &snap.stream_states {
            let totals = &snap.streams[stream];
            frame.push_str(&format!(
                "{:<14} {:<9} {:>9}\n",
                stream.as_str(),
                format!("{state:?}").to_lowercase(),
                totals.rows_read
            ));
        }
    }
    render::stderr::frame(CURSOR_HOME_AND_CLEAR, &frame);
}
