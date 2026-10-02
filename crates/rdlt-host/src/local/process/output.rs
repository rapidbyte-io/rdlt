//! A connector's standard output and error: read at a bounded rate, given to the log a bounded
//! number of lines at a time, each shown and none obeyed, and the end of its standard error
//! kept for the errors of its transport.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdlt_connector::ConnectorId;
use rdlt_connector::text::shown;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, BufReader};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::limits::{
    LAST_WORDS_BYTES, OUTPUT_BYTES_BURST, OUTPUT_BYTES_PER_SECOND, OUTPUT_LINE_BYTES,
    OUTPUT_LINES_BURST, OUTPUT_LINES_PER_SECOND, TAIL_BYTES,
};
use crate::secrets::{DROPPED, Redactions};

#[cfg(test)]
mod tests;

/// The last bytes a connector wrote to its standard error.
#[derive(Debug, Default)]
pub(crate) struct Tail {
    bytes: Mutex<VecDeque<u8>>,
}

impl Tail {
    fn push(&self, line: &[u8]) {
        let mut bytes = self
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        bytes.extend(line);
        let excess = bytes.len().saturating_sub(TAIL_BYTES);
        bytes.drain(..excess);
    }

    /// The end of the kept bytes as an error shows it: scrubbed of `redactions`, each character
    /// a reader could be deceived by escaped, in [`LAST_WORDS_BYTES`] at most.
    pub(crate) fn words(&self, redactions: &Redactions) -> String {
        let bytes = self
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (front, back) = bytes.as_slices();
        let text = String::from_utf8_lossy(&[front, back].concat()).into_owned();
        // A tail that is full may start within what the connector wrote, a secret included.
        let dropped = bytes.len() >= TAIL_BYTES;
        drop(bytes);
        let scrubbed = redactions.scrubbed_end(text, dropped);
        // Cut to its end after it was scrubbed, and scrubbed where it was cut.
        redactions.scrubbed(ending(shown(scrubbed, usize::MAX), LAST_WORDS_BYTES))
    }
}

/// The last `bytes` of `text`, which is shown text: from the start of a line where one starts
/// within them, and marked as an end.
fn ending(text: String, bytes: usize) -> String {
    if text.len() <= bytes {
        return text;
    }
    let from = text.ceil_char_boundary(text.len() - bytes.saturating_sub(DROPPED.len()));
    let end = &text[from..];
    let end = match end.split_once("\\n") {
        Some((_, lines)) if !lines.is_empty() => lines,
        _ => end,
    };
    format!("{DROPPED}{end}")
}

/// Which of a connector's streams an output is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stream {
    /// Its standard output, which a connector should not use.
    Stdout,
    /// Its standard error.
    Stderr,
}

/// What the log is given of a connector's output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Said {
    /// A line, shown, or the start of one longer than a line may be.
    Line(String),
    /// Lines come faster than the log is given them: those beyond are dropped from here on.
    Suppressing,
    /// The stream ended, having dropped this many lines.
    Suppressed(u64),
}

/// Whose output is drained, and where to.
pub(crate) struct Draining<F> {
    /// The end of the output, kept, and what tells it has closed.
    pub(crate) kept: Option<(Arc<Tail>, watch::Sender<bool>)>,
    /// What the connector was given that no log may hold.
    pub(crate) redactions: Redactions,
    /// What takes each thing the log is given.
    pub(crate) log: F,
}

/// Drains `output` to its end: read no faster than [`OUTPUT_BYTES_PER_SECOND`], so a connector
/// that floods waits, and given to the log no faster than [`OUTPUT_LINES_PER_SECOND`], so a
/// flood is a bounded number of events, said once to be suppressed.
pub(crate) async fn drain<F: FnMut(Said)>(
    output: impl AsyncRead + Unpin,
    mut draining: Draining<F>,
) {
    let mut reader = BufReader::new(output);
    let mut bytes = Allowance::new(OUTPUT_BYTES_BURST, OUTPUT_BYTES_PER_SECOND);
    let mut lines = Allowance::new(OUTPUT_LINES_BURST, OUTPUT_LINES_PER_SECOND);
    let (mut piece, mut within_line, mut suppressed) = (Vec::new(), false, 0_u64);
    loop {
        bytes.wait().await;
        piece.clear();
        let mut bounded = (&mut reader).take(TAIL_BYTES as u64);
        match bounded.read_until(b'\n', &mut piece).await {
            Ok(0) | Err(_) => break,
            Ok(read) => bytes.spend(u64::try_from(read).unwrap_or(u64::MAX)),
        }
        if let Some((tail, _)) = &draining.kept {
            tail.push(&piece);
        }
        // The rest of a line whose start was given, or dropped, is not a line of its own.
        let continued = within_line;
        within_line = !piece.ends_with(b"\n");
        let text = String::from_utf8_lossy(&piece);
        let text = text.trim_end();
        if continued || text.is_empty() {
            continue;
        }
        if lines.take() {
            let text = draining.redactions.scrubbed(text.to_owned());
            (draining.log)(Said::Line(shown(text, OUTPUT_LINE_BYTES)));
        } else {
            if suppressed == 0 {
                (draining.log)(Said::Suppressing);
            }
            suppressed = suppressed.saturating_add(1);
        }
    }
    if suppressed > 0 {
        (draining.log)(Said::Suppressed(suppressed));
    }
    if let Some((_, closed)) = draining.kept {
        closed.send_replace(true);
    }
}

/// What gives the log what a connector with `id`, of process `pid`, said on `stream`.
pub(crate) fn logging(id: ConnectorId, pid: u32, stream: Stream) -> impl FnMut(Said) + Send {
    let stream = match stream {
        Stream::Stdout => "stdout",
        Stream::Stderr => "stderr",
    };
    move |said| match said {
        // A connector serves on its socket: what it writes to its standard output is a fault.
        Said::Line(line) if stream == "stdout" => {
            tracing::warn!(connector = %id, pid, stream, line, "a connector wrote output");
        }
        Said::Line(line) => {
            tracing::info!(connector = %id, pid, stream, line, "a connector wrote output");
        }
        Said::Suppressing => {
            tracing::warn!(connector = %id, pid, stream, "a connector's output is suppressed");
        }
        Said::Suppressed(lines) => {
            tracing::warn!(connector = %id, pid, stream, lines, "a connector's output ended");
        }
    }
}

/// An allowance that refills with time: `burst` at once, then `rate` each second.
struct Allowance {
    /// What is left; below zero, what was spent beyond it.
    left: i128,
    burst: u64,
    rate: u64,
    refilled: Instant,
}

impl Allowance {
    fn new(burst: u64, rate: u64) -> Self {
        Self {
            left: i128::from(burst),
            burst,
            rate,
            refilled: Instant::now(),
        }
    }

    /// Adds what the time since the last refill allows, up to the burst.
    fn refill(&mut self) {
        let now = Instant::now();
        let nanos = now.duration_since(self.refilled).as_nanos();
        let earned = nanos.saturating_mul(u128::from(self.rate)) / 1_000_000_000;
        if earned == 0 {
            return;
        }
        let earned = i128::try_from(earned).unwrap_or(i128::MAX);
        self.left = self.left.saturating_add(earned).min(i128::from(self.burst));
        self.refilled = now;
    }

    /// Spends `amount`, which may be more than is left.
    fn spend(&mut self, amount: u64) {
        self.left = self.left.saturating_sub(i128::from(amount));
    }

    /// Spends one, when one is left.
    fn take(&mut self) -> bool {
        self.refill();
        if self.left < 1 {
            return false;
        }
        self.left -= 1;
        true
    }

    /// Waits until something is left, and lets other tasks run meanwhile.
    async fn wait(&mut self) {
        tokio::task::yield_now().await;
        loop {
            self.refill();
            if self.left >= 1 {
                return;
            }
            let owed = u128::try_from(1 - self.left).unwrap_or(u128::MAX);
            let nanos = owed.saturating_mul(1_000_000_000) / u128::from(self.rate.max(1));
            let wait = Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX));
            tokio::time::sleep(wait.max(Duration::from_millis(1))).await;
        }
    }
}
