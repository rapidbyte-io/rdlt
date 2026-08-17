//! In-memory source. Honors `ReadRequest.since` for real — resume tests
//! depend on it (a prior incarnation ignored it and blocked every cursor
//! test downstream).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rdlt_connector::core::{Cursor, StreamName};
use rdlt_connector::error::SourceError;
use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;

/// One pushed unit: rows followed by an optional checkpoint.
#[derive(Debug, Clone)]
pub struct MemoryBatch {
    /// The rows this unit pushes.
    pub rows: Vec<serde_json::Value>,
    /// The checkpoint closing this unit, if any.
    pub checkpoint: Option<Cursor>,
}

impl MemoryBatch {
    /// A batch of `rows` with no checkpoint.
    pub fn new(rows: Vec<serde_json::Value>) -> Self {
        Self {
            rows,
            checkpoint: None,
        }
    }

    /// Close the batch with a checkpoint at `cursor`.
    pub fn with_checkpoint(mut self, cursor: impl Into<serde_json::Value>) -> Self {
        self.checkpoint = Some(Cursor::new(cursor));
        self
    }
}

/// One declared stream and its scripted behavior, including the failure
/// injections retry and crash tests drive.
#[derive(Debug, Clone)]
pub struct MemoryStream {
    /// The stream as declared to the host.
    pub spec: StreamSpec,
    /// The units pushed in order on a full read.
    pub batches: Vec<MemoryBatch>,
    /// Crash injection: return a FATAL error after this many batches were
    /// pushed (the run aborts and does not retry). Named to contrast with
    /// the `transient_*` siblings, which inject retryable failures.
    pub fatal_after: Option<usize>,
    /// Pacing for cancellation tests: sleep before each batch.
    pub batch_delay: Option<std::time::Duration>,
    /// Retry testing: fail with a Transient error at the start of the
    /// first N `read` attempts (clause E5 — the engine must retry, the
    /// source never does).
    pub transient_start_failures: u32,
    /// Retry testing: on the FIRST read attempt only, fail transiently
    /// after this many batches were pushed (reproduces mid-stream
    /// transient failures with rows already staged past a checkpoint).
    pub transient_fail_after_once: Option<usize>,
}

impl MemoryStream {
    /// A stream pushing `batches` with no injected failures.
    pub fn new(spec: StreamSpec, batches: Vec<MemoryBatch>) -> Self {
        Self {
            spec,
            batches,
            fatal_after: None,
            batch_delay: None,
            transient_start_failures: 0,
            transient_fail_after_once: None,
        }
    }

    /// Fail transiently after `count` batches, on the first attempt only.
    pub fn transient_fail_after_once(mut self, count: usize) -> Self {
        self.transient_fail_after_once = Some(count);
        self
    }

    /// Fail transiently at the start of the first `attempts` reads.
    pub fn transient_start_failures(mut self, attempts: u32) -> Self {
        self.transient_start_failures = attempts;
        self
    }

    /// Fail fatally after `count` batches (the run aborts, no retry).
    pub fn fatal_after(mut self, count: usize) -> Self {
        self.fatal_after = Some(count);
        self
    }

    /// Sleep `delay` before each batch (cancellation pacing).
    pub fn batch_delay(mut self, delay: std::time::Duration) -> Self {
        self.batch_delay = Some(delay);
        self
    }
}

/// Shared inspection log: which `since` cursor each `read` call received.
pub type SinceLog = Arc<Mutex<Vec<(StreamName, Option<Cursor>)>>>;

/// The in-memory source: scripted streams plus the `since` log tests
/// assert against.
#[derive(Debug, Default)]
pub struct MemorySource {
    streams: Vec<MemoryStream>,
    since_log: SinceLog,
}

impl MemorySource {
    /// A source serving `streams`.
    pub fn new(streams: Vec<MemoryStream>) -> Self {
        Self {
            streams,
            since_log: SinceLog::default(),
        }
    }

    /// The one-stream, one-batch, no-checkpoint convenience shape.
    pub fn single_stream(
        spec: StreamSpec,
        rows: impl IntoIterator<Item = serde_json::Value>,
    ) -> Self {
        Self::new(vec![MemoryStream::new(
            spec,
            vec![MemoryBatch::new(rows.into_iter().collect())],
        )])
    }

    /// Handle for asserting what `since` values the engine passed (clause
    /// S1 tests).
    pub fn since_log(&self) -> SinceLog {
        Arc::clone(&self.since_log)
    }
}

#[async_trait]
impl Source for MemorySource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("memory-source", env!("CARGO_PKG_VERSION"))
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(self.streams.iter().map(|s| s.spec.clone()).collect())
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
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
            .find(|s| s.spec.name == req.stream.name)
            .ok_or_else(|| SourceError::fatal(format!("unknown stream {}", req.stream.name)))?;

        if attempt <= stream.transient_start_failures {
            return Err(SourceError::transient(format!(
                "injected transient failure (attempt {attempt})"
            )));
        }

        // Clause S1: resume strictly after the batch that produced `since`.
        let start = match &req.since {
            None => 0,
            Some(since) => {
                match stream
                    .batches
                    .iter()
                    .position(|b| b.checkpoint.as_ref() == Some(since))
                {
                    Some(idx) => idx + 1,
                    // The engine only passes cursors we produced (clause
                    // E6); an unknown one is a harness bug, not a
                    // resume-from-zero.
                    None => {
                        return Err(SourceError::fatal(format!(
                            "unknown resume cursor {:?}",
                            since
                        )));
                    }
                }
            }
        };

        let mut pushed = 0usize;
        for batch in &stream.batches[start..] {
            if let Some(delay) = stream.batch_delay {
                tokio::time::sleep(delay).await;
            }
            if req.out.rows(batch.rows.iter().cloned()).await.is_err() {
                return Ok(()); // clause S4: closed channel = cancellation, not error
            }
            if let Some(cursor) = &batch.checkpoint
                && req.out.checkpoint(cursor.clone()).await.is_err()
            {
                return Ok(());
            }
            pushed += 1;
            if stream.fatal_after == Some(pushed) {
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
