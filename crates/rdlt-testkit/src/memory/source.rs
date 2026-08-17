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
#[derive(Debug, Clone)]
pub struct Stream {
    /// The stream as declared to the host.
    pub spec: StreamSpec,
    /// The units pushed in order on a full read.
    pub batches: Vec<Batch>,
}

impl Stream {
    /// A stream pushing `batches`.
    pub fn new(spec: StreamSpec, batches: Vec<Batch>) -> Self {
        Self { spec, batches }
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

        for batch in &stream.batches[start..] {
            if req.out.rows(batch.rows.iter().cloned()).await.is_err() {
                return Ok(()); // clause S4: closed channel = cancellation, not error
            }
            if let Some(cursor) = &batch.checkpoint
                && req.out.checkpoint(cursor.clone()).await.is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}
