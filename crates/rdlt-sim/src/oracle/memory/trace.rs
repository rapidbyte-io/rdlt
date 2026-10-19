//! A traced attempt: the most its thread held beyond its charge, read at every change of the
//! charge, and the changes after which it rose.

use std::backtrace::Backtrace;

use alloc_tracker::{Operation, Session, ThreadSpan};

/// The variable that, set, traces each measured attempt, so an attempt that held too much names
/// the change of the charge after which it did.
///
/// At every change of what is charged, the trace reads the most the thread held beyond its charge
/// so far. Its spans and backtraces make it slow: it is for replaying a seed.
pub const TRACE_VAR: &str = "RDLT_SIM_TRACE";

/// Changes of the charge a traced attempt reads the heap at; it reads none after them.
const TRACED: usize = 1 << 14;

/// Bytes: the steps of the most held beyond the charge whose crossing the trace keeps.
const RISE: u64 = 1 << 20;

/// Sessions to trace an attempt with, each for one span, made before the attempt's window opens
/// so that making them is not measured.
pub(in crate::oracle) struct Tracing {
    pub(super) sessions: Vec<(Session, Operation)>,
    /// Room for the sessions once read, kept until the attempt ends so that freeing them is
    /// not measured either.
    pub(super) read: Vec<Session>,
}

impl Tracing {
    /// Sessions for a trace where [`TRACE_VAR`] is set; none otherwise.
    pub(in crate::oracle) fn ready() -> Option<Self> {
        std::env::var_os(TRACE_VAR).filter(|set| !set.is_empty())?;
        // What a first backtrace keeps for every later one is kept before the window opens.
        drop(Backtrace::force_capture().to_string());
        let sessions = (0..TRACED)
            .map(|_| {
                let session = Session::new().no_stdout().no_file();
                let operation = session.operation("change");
                (session, operation)
            })
            .collect();
        Some(Self {
            sessions,
            read: Vec::with_capacity(TRACED),
        })
    }

    /// Starts tracing the calling thread from what it holds now, `offset` bytes more charged
    /// than as its seed began.
    pub(super) fn start(self, offset: i64) -> Trace {
        let spans = self
            .sessions
            .into_iter()
            .map(|(session, operation)| {
                let span = operation.measure_thread().iterations(1);
                (session, span)
            })
            .collect();
        Trace {
            spans,
            read: self.read,
            offset,
            changes: 0,
            most: 0,
            before: Backtrace::force_capture(),
            rises: Vec::new(),
        }
    }
}

/// An attempt traced: the spans it reads the heap with, and where the most held rose.
pub(super) struct Trace {
    /// Spans opened as the attempt began, the innermost last: closing one gives the most the
    /// thread held beyond what it held then.
    spans: Vec<(Session, ThreadSpan)>,
    /// The sessions of the spans closed.
    read: Vec<Session>,
    /// Bytes: what was charged as the spans opened beyond what was charged as the seed began.
    offset: i64,
    /// Changes of the charge read so far.
    changes: u64,
    /// Bytes: the most held beyond the charge as of the last change read.
    most: u64,
    /// Where the change before the last was made.
    before: Backtrace,
    rises: Vec<Rise>,
}

impl Trace {
    /// Reads the most held beyond the charge at a change of `change` bytes, or at the attempt's
    /// end where `change` is none, keeping the change where the most rose past another [`RISE`].
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a peak is a whole, non-negative number of bytes far below 2^53"
    )]
    pub(super) fn read(&mut self, change: Option<i64>) {
        let Some((session, span)) = self.spans.pop() else {
            return;
        };
        drop(span);
        let peak = session
            .to_report()
            .operations()
            .find_map(|(_, operation)| operation.peak_outstanding_bytes())
            .map_or(0, |peak| peak as u64);
        self.read.push(session);
        let most = u64::try_from(i128::from(peak) - i128::from(self.offset)).unwrap_or(0);
        self.changes += 1;
        let at = Backtrace::force_capture();
        if most / RISE > self.most / RISE {
            let before = std::mem::replace(&mut self.before, Backtrace::force_capture());
            self.rises.push(Rise {
                change: self.changes,
                by: change,
                most,
                before,
                at,
            });
        } else {
            self.before = at;
        }
        self.most = self.most.max(most);
    }

    /// Reads the most held as the attempt ends, closes the spans left, innermost first, and
    /// gives the rises.
    pub(super) fn end(mut self) -> Vec<Rise> {
        self.read(None);
        while let Some(span) = self.spans.pop() {
            drop(span);
        }
        self.rises
    }
}

/// A change of the charge after which the most a traced attempt's thread held beyond its charge
/// rose.
#[derive(Debug)]
pub(in crate::oracle) struct Rise {
    /// Which change of the attempt it was, from one.
    pub(super) change: u64,
    /// Bytes: what it changed the charge by, fewer where negative; none for the attempt's end.
    pub(super) by: Option<i64>,
    /// Bytes: the most held beyond the charge once it was made.
    pub(super) most: u64,
    /// Where the change before it was made.
    pub(super) before: Backtrace,
    /// Where it was made.
    pub(super) at: Backtrace,
}

impl Rise {
    /// The rise as a failure names it: the change, and the frames of the workspace's code that
    /// made it and the change before it.
    pub(super) fn named(&self) -> String {
        let Some(by) = self.by else {
            return format!(
                "it held {} bytes beyond its charge by the attempt's end, after the change of the \
                 charge made at\n{}",
                self.most,
                frames(&self.before)
            );
        };
        format!(
            "it held {} bytes beyond its charge once change {} of the charge, by {by} bytes, was \
             made at\n{}\nafter the change made at\n{}",
            self.most,
            self.change,
            frames(&self.at),
            frames(&self.before)
        )
    }
}

/// The frames of `backtrace` in the workspace's code, past the budget and this check, the
/// innermost first.
fn frames(backtrace: &Backtrace) -> String {
    let text = backtrace.to_string();
    let mut lines = text.lines().peekable();
    let mut frames = Vec::new();
    while let Some(line) = lines.next() {
        let Some((_, name)) = line.trim_start().split_once(": ") else {
            continue;
        };
        let place = lines
            .next_if(|next| next.trim_start().starts_with("at "))
            .map(|next| next.trim_start().trim_start_matches("at ").to_owned());
        let ours = ["rdlt_", "<rdlt_", "core::ptr::drop_glue::<rdlt_"]
            .iter()
            .any(|prefix| name.starts_with(prefix));
        let skipped = ["oracle::memory", "budget::", "SimEnv"]
            .iter()
            .any(|skip| name.contains(skip));
        if ours && !skipped {
            frames.push(match place {
                Some(place) => format!("  {name} at {place}"),
                None => format!("  {name}"),
            });
        }
    }
    frames.truncate(8);
    frames.join("\n")
}
