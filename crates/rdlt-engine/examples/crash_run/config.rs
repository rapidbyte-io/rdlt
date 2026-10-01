//! What a crash run loads: one stream, its source and destination, where each runs, and the log.

use std::path::PathBuf;
use std::time::Duration;

use rdlt_connector::{PipelineId, ReadMode, StreamName};
use rdlt_engine::{
    CommitPolicy, DeleteMode, EngineConfig, PipelinePlan, RetryPolicy, StreamPlan, WriteMode,
};
use serde::Deserialize;
use serde_json::Value;

/// A crash run's configuration, as its file holds it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// The pipeline's id.
    pub(crate) pipeline: String,
    /// Where the engine keeps its write-ahead logs, where its loads keep one.
    #[serde(default)]
    pub(crate) wal: Option<PathBuf>,
    /// The stream loaded.
    pub(crate) stream: Stream,
    /// Where its rows come from.
    pub(crate) source: Place,
    /// Where they go.
    pub(crate) destination: Place,
    /// Rows a commit takes.
    pub(crate) commit_rows: u64,
    /// The most bytes of batches the run holds at once, where it holds fewer than the engine's
    /// default: a source reading more waits for commits, its read in flight across them.
    #[serde(default)]
    pub(crate) memory: Option<u64>,
    /// The events a partition holds before its source waits, where fewer than the engine's
    /// default.
    #[serde(default)]
    pub(crate) partition_buffer: Option<usize>,
    /// A connector killed before a commit, where one is.
    #[serde(default)]
    pub(crate) kill: Option<Kill>,
}

/// The stream a run loads.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Stream {
    pub(crate) name: String,
    /// `full`, `incremental` or `cdc`.
    pub(crate) read: String,
    /// `append`, `replace`, `merge` or `history`.
    pub(crate) write: String,
    /// `hard` or `soft`, for a change stream matched by key.
    #[serde(default)]
    pub(crate) deletes: Option<String>,
}

/// A connector: its kind, its configuration, and whether it runs in a process of its own.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Place {
    /// `generator`, `changes` or `log` for a source; `sqlite` or `files` for a destination.
    pub(crate) kind: String,
    pub(crate) config: Value,
    #[serde(default)]
    pub(crate) spawned: bool,
}

/// A spawned connector killed before a commit: a source only as it reads.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Kill {
    pub(crate) victim: Victim,
    /// The commit the kill falls before; a source's, the first from it on with a read in
    /// flight.
    pub(crate) before: Before,
}

/// The commit a kill falls before.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum Before {
    /// The commit of this number, counted across attempts.
    Commit(u64),
    /// The commit named so.
    Named(Named),
}

/// A commit named by what it does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Named {
    /// The commit swapping a replace's generation in.
    Publish,
}

/// Which spawned connector a kill takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Victim {
    Source,
    Destination,
}

impl Config {
    /// The pipeline the run loads, keeping a write-ahead log where the configuration names one.
    pub(crate) fn plan(&self) -> Result<PipelinePlan, String> {
        let stream = &self.stream;
        let name = StreamName::new(&stream.name).map_err(|error| error.to_string())?;
        let read = match stream.read.as_str() {
            "full" => ReadMode::Full,
            "incremental" => ReadMode::Incremental,
            "cdc" => ReadMode::Cdc,
            other => return Err(format!("no read mode {other}")),
        };
        let write = match stream.write.as_str() {
            "append" => WriteMode::Append,
            "replace" => WriteMode::Replace,
            "merge" => WriteMode::Merge,
            "history" => WriteMode::History,
            other => return Err(format!("no write mode {other}")),
        };
        let mut plan = StreamPlan::new(name).read(read).write(write);
        plan = match stream.deletes.as_deref() {
            None => plan,
            Some("hard") => plan.deletes(DeleteMode::Hard),
            Some("soft") => plan.deletes(DeleteMode::Soft),
            Some(other) => return Err(format!("no delete mode {other}")),
        };
        let pipeline = PipelineId::parse(&self.pipeline).map_err(|error| error.to_string())?;
        let plan = PipelinePlan::new(pipeline, [plan]).map_err(|error| error.to_string())?;
        Ok(plan.with_wal(self.wal.is_some()))
    }

    /// The engine's configuration: commits of `commit_rows` rows, the memory given, and retries
    /// enough to ride out a killed connector.
    pub(crate) fn engine(&self) -> Result<EngineConfig, String> {
        let commit = CommitPolicy::new(None, Some(self.commit_rows), None)
            .map_err(|error| error.to_string())?;
        let retry = RetryPolicy::default()
            .max_attempts(10)
            .initial(Duration::from_millis(10))
            .max_delay(Duration::from_millis(200));
        let mut builder = EngineConfig::builder().commit(commit).retry(retry).lanes(2);
        if let Some(memory) = self.memory {
            builder = builder.memory(memory);
        }
        if let Some(events) = self.partition_buffer {
            builder = builder.partition_buffer(events);
        }
        builder.build().map_err(|error| error.to_string())
    }
}
