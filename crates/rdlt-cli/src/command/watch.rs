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

/// Bytes read from the log per tick. The file is another process's
/// and may be arbitrarily long on the first tick (or grow faster than
/// the display drains it); the rest is read on the next tick rather
/// than all of it held at once.
const READ_PER_TICK: u64 = 8 * 1024 * 1024;

/// The longest line the drain holds while waiting for its newline — the
/// same bound the engine puts on its own manifest lines. A newline-free
/// file otherwise grows the partial buffer without limit; past this a
/// line is dropped and counted, and the drain waits for the next
/// newline to resynchronize.
const MAX_LINE_BYTES: usize = 5 * 1024 * 1024;

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
    let mut drain = Drain::default();

    loop {
        // Drain whatever arrived since the last tick: bytes appended,
        // split on newlines, each complete line one event. A torn tail
        // waits for its newline rather than parsing half an event.
        let mut buf = Vec::new();
        (&mut file)
            .take(READ_PER_TICK)
            .read_to_end(&mut buf)
            .map_err(|e| exit::Error::Io(format!("reading {}: {e}", events_path.display())))?;
        drain.feed(&buf, &mut metrics);

        render_snapshot(&mut metrics, &events_path, &drain);
        std::thread::sleep(REDRAW_EVERY);
    }
}

/// The line splitter between the file and the fold: holds at most one
/// torn line, bounded, and counts what it could not apply.
#[derive(Default)]
struct Drain {
    partial: Vec<u8>,
    /// Bytes of a line past [`MAX_LINE_BYTES`] being discarded until
    /// its newline arrives.
    overlong: bool,
    events_seen: usize,
    /// Lines that were not events — an event kind this CLI predates,
    /// or a line past the length bound. Counted so silence cannot
    /// hide them; never fatal, because the writer's log is not ours.
    skipped: usize,
}

impl Drain {
    fn feed(&mut self, bytes: &[u8], metrics: &mut Metrics) {
        for byte in bytes {
            if *byte != b'\n' {
                if self.overlong {
                    continue;
                }
                if self.partial.len() >= MAX_LINE_BYTES {
                    self.partial.clear();
                    self.overlong = true;
                    self.skipped += 1;
                    continue;
                }
                self.partial.push(*byte);
                continue;
            }
            if std::mem::take(&mut self.overlong) {
                continue;
            }
            let line = String::from_utf8_lossy(&self.partial);
            let line = line.trim_end();
            if !line.is_empty() {
                match serde_json::from_str::<PipelineEvent>(line) {
                    Ok(event) => {
                        metrics.apply(&event);
                        self.events_seen += 1;
                    }
                    Err(_) => self.skipped += 1,
                }
            }
            self.partial.clear();
        }
    }
}

/// One frame: cursor home, clear, then the snapshot's rows. Plain ANSI
/// over stderr so stdout stays untouched (a script piping watch's
/// stdout gets nothing).
fn render_snapshot(metrics: &mut Metrics, source: &Path, drain: &Drain) {
    const CURSOR_HOME_AND_CLEAR: &str = "\x1b[H\x1b[2J";
    let snap = metrics.snapshot();
    let mut frame = String::new();
    frame.push_str(&format!(
        "rdlt watch — {}   (events seen: {}{}; ctrl-c exits)\n\n",
        source.display(),
        drain.events_seen,
        if drain.skipped > 0 {
            format!(", skipped: {}", drain.skipped)
        } else {
            String::new()
        }
    ));
    if snap.untracked_keys > 0 {
        frame.push_str(&format!(
            "({} events for tables or streams past the {}-key display ceiling are in the \
             totals only)\n",
            snap.untracked_keys,
            rdlt::metrics::MAX_TRACKED_KEYS
        ));
    }
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
                render::stderr::sanitize_identifier(table.as_str()),
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
                render::stderr::sanitize_identifier(stream.as_str()),
                format!("{state:?}").to_lowercase(),
                totals.rows_read
            ));
        }
    }
    render::stderr::frame(CURSOR_HOME_AND_CLEAR, &frame);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_line() -> String {
        let event = PipelineEvent::StreamStarted {
            stream: rdlt::id::StreamName::new("s"),
            table: rdlt::id::TableName::new("t"),
        };
        let mut line = serde_json::to_string(&event).expect("renders");
        line.push('\n');
        line
    }

    /// A line is applied once its newline arrives, however the bytes
    /// were split across ticks; a line that is not an event is counted
    /// as skipped, never fatal.
    #[test]
    fn the_drain_applies_complete_lines_and_counts_the_rest() {
        let mut metrics = Metrics::new();
        let mut drain = Drain::default();
        let line = event_line();
        let (head, tail) = line.as_bytes().split_at(7);
        drain.feed(head, &mut metrics);
        assert_eq!(drain.events_seen, 0, "a torn line waits");
        drain.feed(tail, &mut metrics);
        assert_eq!(drain.events_seen, 1);
        drain.feed(b"{\"event\":\"from_the_future\"}\n\n", &mut metrics);
        assert_eq!((drain.events_seen, drain.skipped), (1, 1));
    }

    /// A newline-free line past the bound is dropped and counted once,
    /// its bytes never retained, and the drain resynchronizes at the
    /// next newline.
    #[test]
    fn an_overlong_line_is_dropped_counted_and_never_held() {
        let mut metrics = Metrics::new();
        let mut drain = Drain::default();
        let flood = vec![b'x'; MAX_LINE_BYTES + 1];
        drain.feed(&flood, &mut metrics);
        drain.feed(&flood, &mut metrics);
        assert_eq!(drain.skipped, 1, "one line, however long, is one skip");
        assert!(drain.partial.is_empty(), "the flood's bytes are not held");
        drain.feed(b"\n", &mut metrics);
        drain.feed(event_line().as_bytes(), &mut metrics);
        assert_eq!((drain.events_seen, drain.skipped), (1, 1));
    }
}
