//! What a run loads: the pipeline, its streams and how each one is read and written.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use rdlt_connector::{PipelineId, ReadMode, StreamName};

use crate::error::Error;

/// How a stream's rows reach its table.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Insert every row.
    Append,
    /// Fill a hidden generation of the table and swap it in once the stream is fully read.
    Replace,
}

/// One stream of a [`PipelinePlan`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPlan {
    name: StreamName,
    read: ReadMode,
    write: WriteMode,
}

impl StreamPlan {
    /// A stream read in full and appended.
    pub fn new(name: StreamName) -> Self {
        Self {
            name,
            read: ReadMode::Full,
            write: WriteMode::Append,
        }
    }

    /// Sets how the stream is read.
    #[must_use]
    pub fn read(mut self, mode: ReadMode) -> Self {
        self.read = mode;
        self
    }

    /// Sets how the stream is written.
    #[must_use]
    pub fn write(mut self, mode: WriteMode) -> Self {
        self.write = mode;
        self
    }

    /// The stream.
    pub fn name(&self) -> &StreamName {
        &self.name
    }

    /// How the stream is read.
    pub fn read_mode(&self) -> ReadMode {
        self.read
    }

    /// How the stream is written.
    pub fn write_mode(&self) -> WriteMode {
        self.write
    }
}

/// A pipeline and the streams one run of it loads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelinePlan {
    pipeline: PipelineId,
    streams: Vec<StreamPlan>,
}

impl PipelinePlan {
    /// Validates `streams` of `pipeline`: at least one stream, distinct names and tables, and supported
    /// combinations of read and write modes (`full` with `append` or `replace`, `incremental`
    /// with `append`).
    pub fn new(
        pipeline: PipelineId,
        streams: impl IntoIterator<Item = StreamPlan>,
    ) -> Result<Self, Error> {
        let streams: Vec<StreamPlan> = streams.into_iter().collect();
        if streams.is_empty() {
            return Err(
                Error::config(format!("pipeline {pipeline} selects no streams"))
                    .with_code("plan_empty"),
            );
        }
        let mut names = BTreeSet::new();
        let mut tables = BTreeSet::new();
        for stream in &streams {
            if !names.insert(&stream.name) {
                return Err(
                    Error::config(format!("stream {} is selected twice", stream.name))
                        .with_code("plan_duplicate_stream")
                        .with_stream(&stream.name),
                );
            }
            // Each stream loads the table named after it, so two names that display alike
            // would write into one table.
            if !tables.insert(stream.name.to_string()) {
                return Err(Error::config(format!(
                    "stream {} would load the same table as another selected stream",
                    stream.name
                ))
                .with_code("plan_table_collision")
                .with_stream(&stream.name));
            }
            check_modes(stream)?;
        }
        Ok(Self { pipeline, streams })
    }

    /// The pipeline.
    pub fn pipeline(&self) -> &PipelineId {
        &self.pipeline
    }

    /// The selected streams.
    pub fn streams(&self) -> &[StreamPlan] {
        &self.streams
    }
}

fn check_modes(stream: &StreamPlan) -> Result<(), Error> {
    let refuse = |code: &str, reason: &str| {
        Err(Error::config(format!("stream {}: {reason}", stream.name))
            .with_code(code)
            .with_stream(&stream.name))
    };
    match (stream.read, stream.write) {
        (ReadMode::Full, WriteMode::Append | WriteMode::Replace)
        | (ReadMode::Incremental, WriteMode::Append) => Ok(()),
        (ReadMode::Incremental, WriteMode::Replace) => refuse(
            "plan_mode_invalid",
            "replace needs a full read, since the new generation replaces every row",
        ),
        _ => refuse(
            "plan_mode_unsupported",
            "change data capture is not supported yet",
        ),
    }
}
