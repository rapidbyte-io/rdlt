//! What the live region shows of a verb at work: a task beside a spinner, or how a run stands.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rdlt::{Partitions, Phase, Status, Step};

use super::{Looks, Tone, counted, fit};
use crate::output::shown;

/// The columns the phase's word takes, so the counts beside it stay put as the phase changes.
const PHASE_COLUMNS: usize = 10;

/// The live region's room, in rows and columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Size {
    pub(super) rows: usize,
    pub(super) columns: usize,
}

/// How a run was asked to stop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stop {
    AfterCommit,
    Now,
}

/// What names a run in its first line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunTitle {
    pipeline: String,
    source: String,
    destination: String,
}

impl RunTitle {
    pub(crate) fn of(pipeline: &rdlt::Pipeline) -> Arc<Self> {
        Arc::new(Self {
            pipeline: shown(&pipeline.document().pipeline),
            source: super::short_id(&pipeline.source().id),
            destination: super::short_id(&pipeline.destination().id),
        })
    }
}

/// How a run stands, as it is shown.
pub(super) struct RunProgress {
    title: Arc<RunTitle>,
    status: Status,
    stop: Option<Stop>,
}

impl RunProgress {
    pub(super) fn new(title: Arc<RunTitle>, status: Status, stop: Option<Stop>) -> Self {
        Self {
            title,
            status,
            stop,
        }
    }
}

/// What the live region shows.
pub(super) enum View {
    /// A task, beside a spinner.
    Task(String),
    /// How a run stands.
    Run(Box<RunProgress>),
}

impl View {
    /// Whether this view shows a run asked to stop.
    pub(super) fn stopping(&self) -> bool {
        matches!(self, Self::Run(progress) if progress.stop.is_some())
    }

    /// The region's lines at spinner frame `frame`, each fitting one of `size`'s rows.
    pub(super) fn lines(&self, frame: usize, looks: Looks, size: Size) -> Vec<String> {
        let marks = looks.marks();
        let spinner = marks.spinner[frame % marks.spinner.len()];
        let lines = match self {
            Self::Task(words) => vec![format!("{spinner} {}", shown(words))],
            Self::Run(progress) => progress.lines(spinner, looks, size),
        };
        lines
            .iter()
            .map(|line| fit(line, size.columns, marks))
            .collect()
    }
}

impl RunProgress {
    fn lines(&self, spinner: &str, looks: Looks, size: Size) -> Vec<String> {
        let mut lines = vec![self.header(spinner, looks), self.phase_line(looks)];
        self.stream_lines(looks, size, &mut lines);
        lines
    }

    /// `⠹ orders  postgres → sqlite  00:12`.
    fn header(&self, spinner: &str, looks: Looks) -> String {
        let brush = looks.err();
        let title = &self.title;
        format!(
            "{spinner} {}  {} {} {}  {}",
            brush.paint(Tone::Name, &title.pipeline),
            title.source,
            looks.marks().arrow,
            title.destination,
            brush.paint(Tone::Dim, clock(self.status.report.elapsed)),
        )
    }

    /// What the run is in the middle of, what it has committed and how many partitions ended.
    fn phase_line(&self, looks: Looks) -> String {
        let brush = looks.err();
        match self.stop {
            Some(Stop::AfterCommit) => {
                let text = "Stopping after the next commit (Ctrl-C again to stop now)";
                format!("  {}", brush.paint(Tone::Warn, text))
            }
            Some(Stop::Now) => format!("  {}", brush.paint(Tone::Warn, "Stopping now")),
            None => {
                let phase = phase_words(&self.status.phase, self.status.report.attempted + 1);
                let dot = looks.marks().dot;
                // The step comes last, so the counts stay where they are through a commit.
                let step = match self.status.phase {
                    Phase::Committing { step, .. } => {
                        format!(" {dot} {}", step_word(step))
                    }
                    _ => String::new(),
                };
                format!(
                    "  {phase:<PHASE_COLUMNS$} {}{step}",
                    counts(&self.status, dot)
                )
            }
        }
    }

    /// One line per stream while they fit below the header and phase, in rows the terminal
    /// leaves; past that as many as fit and `+N more`.
    fn stream_lines(&self, looks: Looks, size: Size, lines: &mut Vec<String>) {
        let streams = &self.status.report.streams;
        // A region as tall as the screen scrolls on each redraw: one row stays free.
        let room = size.rows.saturating_sub(lines.len() + 1);
        if streams.is_empty() || room == 0 {
            return;
        }
        let showing = if streams.len() <= room {
            streams.len()
        } else {
            room.saturating_sub(1)
        };
        let names: Vec<String> = streams.keys().map(shown).collect();
        let widest = names
            .iter()
            .map(|name| console::measure_text_width(name))
            .max()
            .unwrap_or(0);
        for (name, stream) in names.iter().zip(streams.values()).take(showing) {
            let name = console::pad_str(name, widest + 7, console::Alignment::Left, None);
            let name = looks.err().paint(Tone::Name, name);
            lines.push(format!(
                "  {name}{}",
                counted(stream.rows, "row committed", "rows committed")
            ));
        }
        if showing < streams.len() {
            lines.push(format!("  +{} more", streams.len() - showing));
        }
    }
}

/// The phase's words, `attempt` being the number of the attempt a retry would start.
fn phase_words(phase: &Phase, attempt: u64) -> String {
    match phase {
        Phase::Opening => "Opening".to_owned(),
        Phase::Reading => "Reading".to_owned(),
        Phase::Committing { .. } => "Committing".to_owned(),
        Phase::BackingOff { until } => {
            let left = until
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO);
            // Rounded up: `0s` would claim the wait is over while it is not.
            let seconds = left.as_secs() + u64::from(left.subsec_nanos() > 0);
            format!("Backing off {seconds}s (attempt {attempt})")
        }
        Phase::Stopping => "Stopping".to_owned(),
        Phase::Ended { .. } => "Ended".to_owned(),
    }
}

fn step_word(step: Step) -> &'static str {
    match step {
        Step::Barrier => "barrier",
        Step::Flush => "flush",
        Step::Commit => "commit",
        Step::Ack => "ack",
    }
}

/// What the run has committed and how far its partitions are, as `status` counts them.
fn counts(status: &Status, dot: &str) -> String {
    let report = &status.report;
    let mut parts = vec![
        counted(report.rows, "row committed", "rows committed"),
        counted(report.commits, "commit", "commits"),
    ];
    if let Some(rate) = rate(report.rows, report.elapsed) {
        parts.insert(1, rate);
    }
    let Partitions {
        reading,
        waiting,
        ended,
    } = status.partitions;
    let known = reading + waiting + ended;
    if known > 0 {
        parts.push(format!("{ended}/{known} partitions"));
    }
    parts.join(&format!(" {dot} "))
}

/// `rows` over `elapsed`, in rows a second, none before a row is committed or any time passed.
pub(super) fn rate(rows: u64, elapsed: Duration) -> Option<String> {
    let seconds = elapsed.as_secs_f64();
    if rows == 0 || seconds <= 0.0 {
        return None;
    }
    // Rates are approximate: the integer conversion loses nothing that is shown.
    #[expect(clippy::cast_precision_loss, reason = "a rate shows three digits")]
    let per_second = rows as f64 / seconds;
    Some(if per_second >= 1_000_000.0 {
        format!("{:.1}M rows/s", per_second / 1_000_000.0)
    } else if per_second >= 1_000.0 {
        format!("{:.1}k rows/s", per_second / 1_000.0)
    } else {
        format!("{per_second:.0} rows/s")
    })
}

/// `elapsed` as a clock: `mm:ss`, or `h:mm:ss` from an hour on.
fn clock(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    let (hours, minutes, seconds) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes:02}:{seconds:02}")
    }
}
