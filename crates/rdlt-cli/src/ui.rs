//! How the command line looks: the colours and marks it may use, chosen once from its flags, the
//! environment and its standard streams, and what a terminal is shown while a verb works.
//!
//! Nothing else in the crate writes an escape sequence or names `indicatif`.

mod certify;
mod failure;
mod live;
mod painter;
mod render;
#[cfg(test)]
mod tests;

use std::fmt::Display;
use std::io::{IsTerminal as _, Write as _};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use console::Style;

use crate::args::Color;
use crate::output::{self, COLLECTOR, FINISHING, shown};

pub(crate) use certify::{Clause, in_progress, kept_line, verdict_line};
pub(crate) use failure::{certify_failure, failure};
pub(crate) use live::{RunTitle, Stop};
pub(crate) use painter::Std;
pub(crate) use render::{
    LocalConnector, Remotes, Sandboxing, catalog, check, connectors, created, plain_line, reset,
    run_summary,
};

use live::{RunProgress, View};
use painter::Painter;

/// The marks shown where the locale says UTF-8.
const UNICODE: Marks = Marks {
    ok: "✓",
    fail: "✗",
    stop: "■",
    skip: "–",
    arrow: "→",
    dot: "·",
    ellipsis: "…",
    spinner: &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
};

/// The marks shown in any other locale.
const ASCII: Marks = Marks {
    ok: "ok",
    fail: "FAIL",
    stop: "STOP",
    skip: "-",
    arrow: "->",
    dot: "-",
    ellipsis: "...",
    spinner: &["|", "/", "-", "\\"],
};

/// The look of this process's output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Looks {
    out: Channel,
    err: Channel,
    animates: bool,
    marks: &'static Marks,
    quiet: bool,
}

/// What a standard stream was found to be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Channel {
    terminal: bool,
    coloured: bool,
}

/// The marks of a locale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Marks {
    pub(crate) ok: &'static str,
    pub(crate) fail: &'static str,
    pub(crate) stop: &'static str,
    pub(crate) skip: &'static str,
    pub(crate) arrow: &'static str,
    pub(crate) dot: &'static str,
    pub(crate) ellipsis: &'static str,
    pub(crate) spinner: &'static [&'static str],
}

/// What the look of the output is decided by, as the process found them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Found {
    term: Option<String>,
    no_color: Option<String>,
    clicolor_force: Option<String>,
    lc_all: Option<String>,
    lc_ctype: Option<String>,
    lang: Option<String>,
    rust_log: Option<String>,
    collector: Option<String>,
    out_terminal: bool,
    err_terminal: bool,
}

impl Found {
    fn of_process() -> Self {
        let variable = |name: &str| std::env::var(name).ok();
        Self {
            term: variable("TERM"),
            no_color: variable("NO_COLOR"),
            clicolor_force: variable("CLICOLOR_FORCE"),
            lc_all: variable("LC_ALL"),
            lc_ctype: variable("LC_CTYPE"),
            lang: variable("LANG"),
            rust_log: variable("RUST_LOG"),
            collector: COLLECTOR.iter().find_map(|name| variable(name)),
            out_terminal: std::io::stdout().is_terminal(),
            err_terminal: std::io::stderr().is_terminal(),
        }
    }

    fn dumb(&self) -> bool {
        self.term.as_deref() == Some("dumb")
    }

    /// Whether the locale, the first of `LC_ALL`, `LC_CTYPE` and `LANG` set, names UTF-8.
    fn unicode(&self) -> bool {
        [&self.lc_all, &self.lc_ctype, &self.lang]
            .into_iter()
            .flatten()
            .find(|value| !value.is_empty())
            .is_some_and(|locale| {
                let locale = locale.to_ascii_lowercase();
                locale.contains("utf-8") || locale.contains("utf8")
            })
    }

    /// Whether a stream, a terminal or not, is coloured under `color`.
    fn coloured(&self, color: Color, terminal: bool) -> bool {
        match color {
            Color::Always => true,
            Color::Never => false,
            Color::Auto => {
                if self
                    .no_color
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
                {
                    false
                } else if self
                    .clicolor_force
                    .as_deref()
                    .is_some_and(|value| value != "0")
                {
                    true
                } else {
                    terminal && !self.dumb()
                }
            }
        }
    }
}

impl Looks {
    /// The look `asked` gives this process's standard streams.
    pub(crate) fn of_process(asked: Asked) -> Self {
        Self::resolved(asked, &Found::of_process())
    }

    /// The look `color` asks of a process that has not parsed its flags, or `Auto`.
    pub(crate) fn before_parsing(color: Option<Color>) -> Self {
        Self::of_process(Asked::plain(color.unwrap_or(Color::Auto)))
    }

    fn resolved(asked: Asked, found: &Found) -> Self {
        let channel = |terminal| Channel {
            terminal,
            coloured: found.coloured(asked.color, terminal),
        };
        // Anything else writing to the terminal would be written over by the next redraw.
        let another_writer = (asked.events_on_stdout && found.out_terminal)
            || asked.verbose_logs
            || found.rust_log.as_deref().is_some_and(|set| !set.is_empty())
            || found.collector.is_some();
        Self {
            out: channel(found.out_terminal),
            err: channel(found.err_terminal),
            animates: found.err_terminal && !found.dumb() && !asked.quiet && !another_writer,
            marks: if found.unicode() { &UNICODE } else { &ASCII },
            quiet: asked.quiet,
        }
    }

    pub(crate) fn marks(self) -> &'static Marks {
        self.marks
    }

    /// Standard output's brush.
    pub(crate) fn out(self) -> Brush {
        Brush(self.out.coloured)
    }

    /// Standard error's brush.
    pub(crate) fn err(self) -> Brush {
        Brush(self.err.coloured)
    }

    /// Whether standard error is written in colour.
    pub(crate) fn err_coloured(self) -> bool {
        self.err.coloured
    }
}

/// What the command line's flags ask of the look of its output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Asked {
    pub(crate) color: Color,
    pub(crate) quiet: bool,
    /// The run's events go to standard output.
    pub(crate) events_on_stdout: bool,
    /// Logs are asked for at a level that writes more than a live region can stay above.
    pub(crate) verbose_logs: bool,
}

impl Asked {
    /// Nothing but `color`.
    const fn plain(color: Color) -> Self {
        Self {
            color,
            quiet: false,
            events_on_stdout: false,
            verbose_logs: false,
        }
    }
}

/// A standard stream's colours: all of them, or none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Brush(bool);

impl Brush {
    /// `text` in `tone`, where this stream is coloured.
    pub(crate) fn paint(self, tone: Tone, text: impl Display) -> String {
        if self.0 {
            tone.style().force_styling(true).apply_to(text).to_string()
        } else {
            text.to_string()
        }
    }
}

/// The palette: green for success, red for failure, yellow for a warning or a stop, cyan for
/// names and dim for secondary text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Tone {
    Good,
    Bad,
    Warn,
    Name,
    Dim,
}

impl Tone {
    fn style(self) -> Style {
        let style = Style::new();
        match self {
            Self::Good => style.green(),
            Self::Bad => style.red(),
            Self::Warn => style.yellow(),
            Self::Name => style.cyan(),
            Self::Dim => style.dim(),
        }
    }
}

/// The look this process settled on, and the thread that draws on its terminal.
pub(crate) struct Ui {
    looks: Looks,
    painter: Option<Painter>,
    /// Whether something else came to write to the terminal, so nothing more is animated.
    stilled: AtomicBool,
}

impl Ui {
    /// The look `asked` gives this process, with what animates its terminal started where it
    /// animates.
    pub(crate) fn start(asked: Asked) -> Self {
        let looks = Looks::of_process(asked);
        let painter = looks.animates.then(|| Painter::start(looks)).flatten();
        Self {
            looks,
            painter,
            stilled: AtomicBool::new(false),
        }
    }

    pub(crate) fn looks(&self) -> Looks {
        self.looks
    }

    /// Whether notes and progress are dropped.
    pub(crate) fn quiet(&self) -> bool {
        self.looks.quiet
    }

    /// Whether standard error is being animated.
    pub(crate) fn animating(&self) -> bool {
        self.painter.is_some() && !self.stilled.load(Ordering::Relaxed)
    }

    /// Stops animating: something else, found only once it opened, writes to the terminal.
    pub(crate) fn stop_animating(&self) {
        self.stilled.store(true, Ordering::Relaxed);
    }

    /// The painter, where it is to be drawn with.
    fn drawing(&self) -> Option<&Painter> {
        self.painter.as_ref().filter(|_| self.animating())
    }

    /// Whether a verb's progress is written as lines of text, where it is not animated and not
    /// dropped.
    pub(crate) fn tells_lines(&self) -> bool {
        !self.animating() && !self.looks.quiet
    }

    /// Whether a verb's results take their full layout, standard output being a terminal.
    pub(crate) fn rich_results(&self) -> bool {
        self.looks.out.terminal
    }

    /// Whether a failure takes its full layout, standard error being a terminal.
    pub(crate) fn rich_failures(&self) -> bool {
        self.looks.err.terminal
    }

    /// Shows `words` beside the spinner, in place of anything else shown.
    pub(crate) fn task(&self, words: &str) {
        if let Some(painter) = self.drawing() {
            painter.show(View::Task(words.to_owned()));
        }
    }

    /// Shows how `run` stands; it does nothing where nothing animates.
    pub(crate) fn progress(&self, title: &Arc<RunTitle>, run: &rdlt::Run, stop: Option<Stop>) {
        if let Some(painter) = self.drawing() {
            painter.show(View::Run(Box::new(RunProgress::new(
                Arc::clone(title),
                run.status(),
                stop,
            ))));
        }
    }

    /// Prints `line` on `to` above what is animated, or nowhere when nothing animates, and
    /// answers whether it was accepted.
    pub(crate) fn above(&self, to: Std, line: &str) -> bool {
        self.painter
            .as_ref()
            .is_some_and(|painter| painter.above(to, line))
    }

    /// As [`above`](Self::above), waiting a bounded while for the painter to take `line` when it
    /// has fallen behind, on a thread that runs no task.
    pub(crate) fn above_waiting(&self, to: Std, line: &str) {
        if let Some(painter) = &self.painter {
            painter.above_waiting(to, line);
        }
    }

    /// What writes log lines above what is animated, where something animates.
    pub(crate) fn log_lines(&self) -> Option<painter::LogLines> {
        self.painter.as_ref().map(Painter::log_lines)
    }

    /// Clears what is animated, waiting a bounded while at most.
    pub(crate) async fn cleared(&self) {
        if let Some(painter) = &self.painter {
            painter.cleared().await;
        }
    }

    /// Writes a verb's last words to standard error within a bound; dropped at once where the
    /// painter is known to be stuck, the terminal that would show them having stopped reading.
    pub(crate) fn last_words(&self, text: String) {
        if !self.painter.as_ref().is_some_and(Painter::stuck) {
            output::written_within(Std::Err, text).ok();
        }
    }

    /// Ends the painter's thread within its bound and says how many log lines it could not
    /// show; one stuck in a write is left for the process's end to take.
    pub(crate) async fn finished(self) {
        let Some(painter) = self.painter else { return };
        if let Some(lost) = painter.finished().await.filter(|lost| *lost > 0) {
            let line = format!("{} not shown\n", counted(lost, "log line", "log lines"));
            tokio::time::timeout(FINISHING, output::stderr(&line))
                .await
                .ok();
        }
    }
}

/// Prints what parsing the command line answered: a usage error on standard error, help on
/// standard output, coloured as `color` asks of them.
pub(crate) fn print_error(error: &clap::Error, color: Option<Color>) {
    let looks = Looks::before_parsing(color);
    let rendered = error.render();
    let coloured = if error.use_stderr() {
        looks.err.coloured
    } else {
        looks.out.coloured
    };
    let text = if coloured {
        rendered.ansi().to_string()
    } else {
        rendered.to_string()
    };
    if error.use_stderr() {
        write!(std::io::stderr(), "{text}").ok();
    } else {
        write!(std::io::stdout(), "{text}").ok();
    }
}

/// Standard output's width, in columns.
fn out_width() -> usize {
    usize::from(console::Term::stdout().size().1)
}

/// Standard error's width, in columns.
fn err_width() -> usize {
    usize::from(console::Term::stderr().size().1)
}

/// `line` cut to fit `width` columns, one spare so that it can never wrap.
pub(crate) fn fit(line: &str, width: usize, marks: &Marks) -> String {
    console::truncate_str(line, width.saturating_sub(1), marks.ellipsis).into_owned()
}

/// `text` broken at its spaces into lines of at most `width` columns, its leading spaces kept on
/// the first and every later line indented by `indent`; a word that alone is wider is broken.
pub(crate) fn wrapped(text: &str, width: usize, indent: &str) -> Vec<String> {
    let body = text.trim_start_matches(' ');
    let lead = &text[..text.len() - body.len()];
    let room = width
        .saturating_sub(console::measure_text_width(indent).max(lead.len()))
        .max(1);
    let mut lines = Vec::new();
    let mut line = String::new();
    let mut taken = 0;
    for word in body.split(' ') {
        let left = if line.is_empty() {
            room
        } else {
            room.saturating_sub(taken + 1)
        };
        for piece in pieces(word, room, left) {
            let wide = console::measure_text_width(&piece);
            if !line.is_empty() && taken + 1 + wide > room {
                lines.push(mem::take(&mut line));
                taken = 0;
            }
            if !line.is_empty() {
                line.push(' ');
                taken += 1;
            }
            line.push_str(&piece);
            taken += wide;
        }
    }
    lines.push(line);
    for (index, wrapped) in lines.iter_mut().enumerate() {
        wrapped.insert_str(0, if index == 0 { lead } else { indent });
    }
    lines
}

/// `word` in pieces of at most `width` columns, the first of at most `first` where it fits any.
fn pieces(word: &str, width: usize, first: usize) -> Vec<String> {
    if console::measure_text_width(word) <= width || word.contains('\u{1b}') {
        return vec![word.to_owned()];
    }
    let mut pieces = Vec::new();
    let mut piece = String::new();
    let mut limit = if first == 0 { width } else { first };
    for character in word.chars() {
        let mut next = piece.clone();
        next.push(character);
        if console::measure_text_width(&next) > limit {
            pieces.push(mem::take(&mut piece));
            next = character.to_string();
            limit = width;
        }
        piece = next;
    }
    pieces.push(piece);
    pieces
}

/// `count` and its noun, `one` where it is one and `many` otherwise.
pub(crate) fn counted(count: u64, one: &str, many: &str) -> String {
    if count == 1 {
        format!("1 {one}")
    } else {
        format!("{} {many}", indicatif::HumanCount(count))
    }
}

/// The last segment of a connector's id: `postgres` of `io.rapidbyte.postgres`.
pub(crate) fn short_id(id: &str) -> String {
    let name = id.rsplit('.').next().unwrap_or(id);
    shown(name)
}

#[cfg(test)]
impl Ui {
    /// A look with no colour, Unicode marks and nothing animated.
    fn for_tests() -> Self {
        Self::of_color_for_tests(Color::Never)
    }

    /// As [`for_tests`](Self::for_tests), coloured as `color` says.
    fn of_color_for_tests(color: Color) -> Self {
        let found = Found {
            lc_all: Some("C.UTF-8".to_owned()),
            ..Found::default()
        };
        Self {
            looks: Looks::resolved(Asked::plain(color), &found),
            painter: None,
            stilled: AtomicBool::new(false),
        }
    }

    /// A look animating on a terminal, its painter's queue left for the caller to read.
    pub(crate) fn queued() -> (Self, painter::Queue) {
        Self::queued_having_lost(0)
    }

    /// As [`queued`](Self::queued), `lost` log lines already lost.
    fn queued_having_lost(lost: u64) -> (Self, painter::Queue) {
        let found = Found {
            lc_all: Some("C.UTF-8".to_owned()),
            out_terminal: true,
            err_terminal: true,
            ..Found::default()
        };
        let (painter, queue) = Painter::of_queue(lost);
        let ui = Self {
            looks: Looks::resolved(Asked::plain(Color::Never), &found),
            painter: Some(painter),
            stilled: AtomicBool::new(false),
        };
        (ui, queue)
    }
}
