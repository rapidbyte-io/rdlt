//! The in-memory source: scripted streams of JSON rows, checkpointed
//! after each batch, honoring `ReadRequest.since` for real — resume
//! tests depend on it.

use async_trait::async_trait;
use rdlt_connector::core::cursor::Cursor;
use rdlt_connector::error::SourceError;
use rdlt_connector::source::{ReadRequest, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;

/// One pushed unit: rows followed by an optional checkpoint.
#[derive(Debug, Clone)]
pub struct Batch {
    /// The rows this unit pushes.
    pub rows: Vec<serde_json::Value>,
    /// The checkpoint closing this unit, if any.
    pub checkpoint: Option<Cursor>,
}

impl Batch {
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

/// One declared stream and the units it pushes, in order, on a full read.
///
/// Two black-box knobs let a certification test script a source that dies
/// or one that is slow, without a second connector: `fatal_after` and
/// `batch_delay`.
#[derive(Debug, Clone)]
pub struct Stream {
    /// The stream as declared to the host.
    pub spec: StreamSpec,
    /// The units pushed in order on a full read.
    pub batches: Vec<Batch>,
    /// Return a fatal error once this many batches were pushed — the
    /// run aborts and does not retry.
    pub fatal_after: Option<usize>,
    /// Sleep this long before each batch, so a test can observe a read
    /// in flight or cancel one.
    pub batch_delay: Option<std::time::Duration>,
}

impl Stream {
    /// A stream pushing `batches`, with neither knob set.
    pub fn new(spec: StreamSpec, batches: Vec<Batch>) -> Self {
        Self {
            spec,
            batches,
            fatal_after: None,
            batch_delay: None,
        }
    }

    /// Fail fatally after `count` batches were pushed.
    pub fn fatal_after(mut self, count: usize) -> Self {
        self.fatal_after = Some(count);
        self
    }

    /// Sleep `delay` before each batch.
    pub fn batch_delay(mut self, delay: std::time::Duration) -> Self {
        self.batch_delay = Some(delay);
        self
    }
}

/// The in-memory source over scripted streams.
#[derive(Debug, Default)]
pub struct Source {
    streams: Vec<Stream>,
}

impl Source {
    /// A source serving `streams`.
    pub fn new(streams: Vec<Stream>) -> Self {
        Self { streams }
    }

    /// The one-stream, one-batch, no-checkpoint convenience shape.
    pub fn single_stream(
        spec: StreamSpec,
        rows: impl IntoIterator<Item = serde_json::Value>,
    ) -> Self {
        Self::new(vec![Stream::new(
            spec,
            vec![Batch::new(rows.into_iter().collect())],
        )])
    }
}

#[async_trait]
impl rdlt_connector::source::Source for Source {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("memory-source", env!("CARGO_PKG_VERSION"))
    }

    async fn check(&self) -> Result<(), SourceError> {
        // Honest, not trivial: an in-memory source has nothing external
        // to reach — holding its data IS its reachability.
        Ok(())
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(self.streams.iter().map(|s| s.spec.clone()).collect())
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let stream = self
            .streams
            .iter()
            .find(|s| s.spec.name == req.stream.name)
            .ok_or_else(|| SourceError::fatal(format!("unknown stream {}", req.stream.name)))?;

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
                    // The engine only passes cursors we produced; an
                    // unknown one is a harness bug, not a resume-from-zero.
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
        }
        Ok(())
    }
}

#[cfg(test)]
mod knob_tests {
    use rdlt_connector::channel::records;
    use rdlt_connector::source::Source as _;
    use serde_json::json;

    use super::*;

    fn three_batches() -> Vec<Batch> {
        (1..=3)
            .map(|i| Batch::new(vec![json!({"id": i})]))
            .collect()
    }

    #[tokio::test]
    async fn fatal_after_fails_fatally_once_that_many_batches_were_pushed() {
        let source = Source::new(vec![
            Stream::new(StreamSpec::new("events"), three_batches()).fatal_after(2),
        ]);
        let (out, mut input) = records(1 << 20);
        let read = source.read(ReadRequest::new(StreamSpec::new("events"), None, out));
        let drain = async {
            let mut pushes = 0;
            while input.recv().await.is_some() {
                pushes += 1;
            }
            pushes
        };
        let (result, pushes) = tokio::join!(read, drain);
        assert!(
            matches!(result, Err(SourceError::Fatal(_))),
            "the second push is followed by a fatal error, not {result:?}"
        );
        assert_eq!(
            pushes, 2,
            "the batches before the crash are pushed, no more"
        );
    }

    #[tokio::test]
    async fn batch_delay_waits_at_least_the_delay_before_each_batch() {
        let delay = std::time::Duration::from_millis(40);
        let source = Source::new(vec![
            Stream::new(StreamSpec::new("events"), three_batches()).batch_delay(delay),
        ]);
        let (out, mut input) = records(1 << 20);
        let started = std::time::Instant::now();
        let read = source.read(ReadRequest::new(StreamSpec::new("events"), None, out));
        let drain = async { while input.recv().await.is_some() {} };
        let (result, ()) = tokio::join!(read, drain);
        result.expect("a paced read completes");
        assert!(
            started.elapsed() >= delay * 3,
            "three batches each wait the delay: {:?}",
            started.elapsed()
        );
    }
}
