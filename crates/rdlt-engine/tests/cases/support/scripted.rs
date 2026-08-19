//! A scripted source: the testkit's memory stream (with its `fatal_after`
//! and `batch_delay` knobs) plus the two failures only the engine's retry
//! suites drive — a transient error at the start of a read, or transiently
//! mid-stream on the first attempt — and a log of the `since` cursor every
//! `read` received, which the resume suites assert against.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rdlt_connector::error::SourceError;
use rdlt_connector::source::{ReadRequest, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;
use rdlt_core::cursor::Cursor;
use rdlt_core::id::StreamName;
use rdlt_testkit::memory::{self, Batch};

/// One declared stream — the testkit's, with the engine-only failures
/// scripted on top.
#[derive(Debug, Clone)]
pub(crate) struct Stream {
    inner: memory::Stream,
    /// Fail with a Transient error at the start of the first N `read`
    /// attempts (the engine must retry; the source never does).
    transient_start_failures: u32,
    /// On the FIRST read attempt only, fail transiently after this many
    /// batches were pushed — a mid-stream transient with rows already
    /// staged past a checkpoint.
    transient_fail_after_once: Option<usize>,
}

impl Stream {
    /// A stream pushing `batches` with no injected failures.
    pub(crate) fn new(spec: StreamSpec, batches: Vec<Batch>) -> Self {
        Self {
            inner: memory::Stream::new(spec, batches),
            transient_start_failures: 0,
            transient_fail_after_once: None,
        }
    }

    /// Fail transiently after `count` batches, on the first attempt only.
    pub(crate) fn transient_fail_after_once(mut self, count: usize) -> Self {
        self.transient_fail_after_once = Some(count);
        self
    }

    /// Fail transiently at the start of the first `attempts` reads.
    pub(crate) fn transient_start_failures(mut self, attempts: u32) -> Self {
        self.transient_start_failures = attempts;
        self
    }

    /// Fail fatally after `count` batches (the run aborts, no retry).
    pub(crate) fn fatal_after(mut self, count: usize) -> Self {
        self.inner = self.inner.fatal_after(count);
        self
    }

    /// Sleep `delay` before each batch (cancellation pacing).
    pub(crate) fn batch_delay(mut self, delay: std::time::Duration) -> Self {
        self.inner = self.inner.batch_delay(delay);
        self
    }
}

/// Which `since` cursor each `read` call received, in call order.
pub(crate) type SinceLog = Arc<Mutex<Vec<(StreamName, Option<Cursor>)>>>;

/// The scripted source: streams plus the `since` log.
#[derive(Debug, Default)]
pub(crate) struct Source {
    streams: Vec<Stream>,
    since_log: SinceLog,
}

impl Source {
    /// A source serving `streams`.
    pub(crate) fn new(streams: Vec<Stream>) -> Self {
        Self {
            streams,
            since_log: SinceLog::default(),
        }
    }

    /// Handle for asserting what `since` values the engine passed.
    pub(crate) fn since_log(&self) -> SinceLog {
        Arc::clone(&self.since_log)
    }
}

#[async_trait]
impl rdlt_connector::source::Source for Source {
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("scripted-source", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(self.streams.iter().map(|s| s.inner.spec.clone()).collect())
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        // The attempt number is derived from the log itself: one entry
        // per read of this stream so far, this call included.
        let attempt = {
            let mut log = self.since_log.lock().expect("since log lock");
            log.push((req.stream.name.clone(), req.since.clone()));
            log.iter()
                .filter(|(name, _)| name == &req.stream.name)
                .count() as u32
        };

        let stream = self
            .streams
            .iter()
            .find(|s| s.inner.spec.name == req.stream.name)
            .ok_or_else(|| SourceError::fatal(format!("unknown stream {}", req.stream.name)))?;

        if attempt <= stream.transient_start_failures {
            return Err(SourceError::transient(format!(
                "injected transient failure (attempt {attempt})"
            )));
        }

        // Resume strictly after the batch that produced `since`; the
        // engine only passes cursors this source produced, so an unknown
        // one is a harness bug, not a resume-from-zero.
        let start = match &req.since {
            None => 0,
            Some(since) => {
                match stream
                    .inner
                    .batches
                    .iter()
                    .position(|b| b.checkpoint.as_ref() == Some(since))
                {
                    Some(idx) => idx + 1,
                    None => {
                        return Err(SourceError::fatal(format!(
                            "unknown resume cursor {since:?}"
                        )));
                    }
                }
            }
        };

        let mut pushed = 0usize;
        for batch in &stream.inner.batches[start..] {
            if let Some(delay) = stream.inner.batch_delay {
                tokio::time::sleep(delay).await;
            }
            // A closed channel is cancellation, not an error.
            if req.out.rows(batch.rows.iter().cloned()).await.is_err() {
                return Ok(());
            }
            if let Some(cursor) = &batch.checkpoint
                && req.out.checkpoint(cursor.clone()).await.is_err()
            {
                return Ok(());
            }
            pushed += 1;
            if stream.inner.fatal_after == Some(pushed) {
                return Err(SourceError::fatal("injected source crash"));
            }
            if attempt == 1 && stream.transient_fail_after_once == Some(pushed) {
                return Err(SourceError::transient(
                    "injected mid-stream transient failure",
                ));
            }
        }
        Ok(())
    }
}
