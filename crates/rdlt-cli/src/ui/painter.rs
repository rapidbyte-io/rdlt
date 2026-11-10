//! The thread that draws what a terminal is shown while a verb works: the only code that touches
//! a progress bar.
//!
//! A terminal can stop reading, and indicatif holds its bar's lock across a write. Every call to
//! indicatif is made here and everything else only sends this thread updates that it may drop,
//! so a terminal that stops reading stops this thread and nothing else.

#[cfg(test)]
mod tests;

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use console::Term;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use tokio::sync::oneshot;

use super::Looks;
use super::live::{Size, View};
use crate::output::FINISHING;

/// The time between two drawings of the live region: at most 10 a second.
const FRAME: Duration = Duration::from_millis(100);

/// Updates that may wait for the painter; further ones are dropped, the newest saying all a
/// dropped one did.
const WAITING: usize = 16;

/// How long a request to clear waits between tries of a full queue.
const RETRY: Duration = Duration::from_millis(20);

/// Which of the standard streams a line is printed on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Std {
    Out,
    Err,
}

/// What the painter is told.
enum Paint {
    /// What the live region is to show from now on.
    View(View),
    /// A line to print above the live region.
    Above(Std, String),
    /// Clear the live region and say so.
    Clear(oneshot::Sender<()>),
    /// Clear the live region and end, the global subscriber's log writer keeping its sender.
    Finish,
}

/// The running painter.
pub(super) struct Painter {
    sender: SyncSender<Paint>,
    ended: oneshot::Receiver<()>,
    thread: JoinHandle<()>,
    /// Whether a wait for the painter already ran out: it is stuck on the terminal, and no later
    /// wait would end sooner.
    stuck: AtomicBool,
    /// Log lines the painter had no room for.
    lost_logs: Arc<AtomicU64>,
}

impl Painter {
    /// Starts the painter; none where the thread cannot start, and nothing is animated then.
    pub(super) fn start(looks: Looks) -> Option<Self> {
        let (sender, receiver) = mpsc::sync_channel(WAITING);
        let (done, ended) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("rdlt-painter".to_owned())
            .spawn(move || {
                painting(&receiver, looks);
                done.send(()).ok();
            })
            .ok()?;
        Some(Self {
            sender,
            ended,
            thread,
            stuck: AtomicBool::new(false),
            lost_logs: Arc::default(),
        })
    }

    /// Shows `view` from the next frame on; dropped where the painter has fallen behind.
    pub(super) fn show(&self, view: View) {
        self.sender.try_send(Paint::View(view)).ok();
    }

    /// Prints `line` on `to` above the live region, and answers whether the painter took it.
    pub(super) fn above(&self, to: Std, line: &str) -> bool {
        self.sender
            .try_send(Paint::Above(to, line.to_owned()))
            .is_ok()
    }

    /// As [`above`](Self::above), waiting within [`FINISHING`] on this thread, which runs no
    /// task, for the painter to take `line`.
    pub(super) fn above_waiting(&self, to: Std, line: &str) {
        if self.stuck() {
            return;
        }
        let waited = Instant::now();
        while !self.above(to, line) {
            if waited.elapsed() >= FINISHING {
                self.stuck.store(true, Ordering::Relaxed);
                return;
            }
            std::thread::sleep(RETRY);
        }
    }

    /// What logs lines above the live region, counting those the painter has no room for.
    pub(super) fn log_lines(&self) -> LogLines {
        LogLines {
            sender: self.sender.clone(),
            lost: Arc::clone(&self.lost_logs),
        }
    }

    /// Whether a wait for the painter already ran out.
    pub(super) fn stuck(&self) -> bool {
        self.stuck.load(Ordering::Relaxed)
    }

    /// Clears the live region, waiting for that within [`FINISHING`].
    pub(super) async fn cleared(&self) {
        if self.stuck() {
            return;
        }
        let (done, acknowledged) = oneshot::channel();
        let asking = async {
            delivered(&self.sender, Paint::Clear(done)).await;
            acknowledged.await.ok();
        };
        if tokio::time::timeout(FINISHING, asking).await.is_err() {
            self.stuck.store(true, Ordering::Relaxed);
        }
    }

    /// Ends the painter once all it was told is done, within [`FINISHING`] unless a wait already
    /// ran out, and answers how many log lines it could not show; a painter stuck in a write is
    /// left for the process's end to take, and answers nothing.
    pub(super) async fn finished(self) -> Option<u64> {
        if self.stuck() {
            return None;
        }
        let Self {
            sender,
            ended,
            thread,
            lost_logs,
            ..
        } = self;
        let ending = async {
            delivered(&sender, Paint::Finish).await;
            ended.await.ok();
        };
        tokio::time::timeout(FINISHING, ending).await.ok()?;
        thread.join().ok();
        Some(lost_logs.load(Ordering::Relaxed))
    }
}

/// Puts `paint` on the painter's queue, waiting its turn while the queue is full.
async fn delivered(sender: &SyncSender<Paint>, mut paint: Paint) {
    loop {
        match sender.try_send(paint) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => return,
            Err(TrySendError::Full(back)) => paint = back,
        }
        tokio::time::sleep(RETRY).await;
    }
}

/// Log lines, as they are told to the painter.
#[derive(Clone)]
pub(crate) struct LogLines {
    sender: SyncSender<Paint>,
    lost: Arc<AtomicU64>,
}

impl std::io::Write for LogLines {
    fn write(&mut self, written: &[u8]) -> std::io::Result<usize> {
        let line = String::from_utf8_lossy(written).trim_end().to_owned();
        if self.sender.try_send(Paint::Above(Std::Err, line)).is_err() {
            self.lost.fetch_add(1, Ordering::Relaxed);
        }
        Ok(written.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogLines {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Draws what the verb's updates say, redrawing at most every [`FRAME`], until told to finish.
fn painting(messages: &Receiver<Paint>, looks: Looks) {
    let terminal = Term::stderr();
    let started = Instant::now();
    let mut next_frame = started;
    let mut region = Live::default();
    loop {
        match messages.recv_timeout(next_frame.saturating_duration_since(Instant::now())) {
            Ok(Paint::View(view)) => region.update(view),
            Ok(Paint::Above(to, line)) => region.above(to, &line),
            Ok(Paint::Clear(done)) => {
                // A press of Ctrl-C is shown before the region goes, however soon the run ends.
                if region.shows_an_undrawn_stop() {
                    std::thread::sleep(next_frame.saturating_duration_since(Instant::now()));
                    region.draw(frame_of(started), looks, &terminal);
                }
                region.clear();
                done.send(()).ok();
            }
            Ok(Paint::Finish) | Err(RecvTimeoutError::Disconnected) => {
                region.clear();
                return;
            }
            Err(RecvTimeoutError::Timeout) => {}
        }
        if Instant::now() >= next_frame {
            region.draw(frame_of(started), looks, &terminal);
            next_frame = Instant::now() + FRAME;
        }
    }
}

/// Which spinner frame shows at this moment, for a painter that began at `started`.
fn frame_of(started: Instant) -> usize {
    usize::try_from(started.elapsed().as_millis() / FRAME.as_millis()).unwrap_or_default()
}

/// What is shown, and the progress bar it is drawn with, made once there is something to draw.
#[derive(Default)]
struct Live {
    view: Option<View>,
    /// Whether the view has changed since it was last drawn.
    undrawn: bool,
    bar: Option<ProgressBar>,
}

impl Live {
    fn update(&mut self, view: View) {
        self.view = Some(view);
        self.undrawn = true;
    }

    fn draw(&mut self, frame: usize, looks: Looks, terminal: &Term) {
        let Some(view) = &self.view else { return };
        let (rows, columns) = terminal.size();
        let size = Size {
            rows: usize::from(rows),
            columns: usize::from(columns),
        };
        let bar = self.bar.get_or_insert_with(new_bar);
        bar.set_message(view.lines(frame, looks, size).join("\n"));
        self.undrawn = false;
    }

    /// Whether the view holds a signal's stop that has not been drawn yet.
    fn shows_an_undrawn_stop(&self) -> bool {
        self.undrawn && self.view.as_ref().is_some_and(View::stopping)
    }

    /// Prints `line` on `to`, the live region out of its way.
    fn above(&self, to: Std, line: &str) {
        let print = || match to {
            Std::Out => {
                let mut stdout = std::io::stdout().lock();
                writeln!(stdout, "{line}").ok();
                stdout.flush().ok();
            }
            Std::Err => {
                writeln!(std::io::stderr().lock(), "{line}").ok();
            }
        };
        match &self.bar {
            Some(bar) => bar.suspend(print),
            None => print(),
        }
    }

    /// Clears the live region, the terminal then holding only what was printed above it.
    fn clear(&mut self) {
        self.view = None;
        if let Some(bar) = self.bar.take() {
            bar.finish_and_clear();
        }
    }
}

/// A progress bar that draws nothing but its message, on standard error.
fn new_bar() -> ProgressBar {
    let bar = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr());
    bar.set_style(ProgressStyle::with_template("{msg}").expect("a template that parses"));
    bar
}

/// The painter's queue, read by a test in the painter's place.
#[cfg(test)]
pub(crate) struct Queue(Receiver<Paint>);

#[cfg(test)]
impl Queue {
    /// The lines told to print above the live region, until `count` came or `within` passed.
    pub(crate) fn lines(&self, count: usize, within: Duration) -> Vec<String> {
        let until = Instant::now() + within;
        let mut lines = Vec::new();
        while lines.len() < count {
            let left = until.saturating_duration_since(Instant::now());
            match self.0.recv_timeout(left) {
                Ok(Paint::Above(_, line)) => lines.push(line),
                Ok(_) => {}
                Err(_) => break,
            }
        }
        lines
    }
}

#[cfg(test)]
impl Painter {
    /// A painter whose queue holds one update and has only the returned reader to empty it,
    /// `lost` log lines already lost.
    pub(super) fn of_queue(lost: u64) -> (Self, Queue) {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (_done, ended) = oneshot::channel();
        let painter = Self {
            sender,
            ended,
            thread: std::thread::spawn(|| {}),
            stuck: AtomicBool::new(false),
            lost_logs: Arc::new(AtomicU64::new(lost)),
        };
        (painter, Queue(receiver))
    }
}
