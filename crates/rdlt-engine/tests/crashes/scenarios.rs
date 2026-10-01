//! The pipelines the sweep crashes, each with what its destination holds once it has run.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use rdlt_connector::PipelineId;
use rdlt_connector_reference::changes::{ChangedStream, Row, Version, expected, history};
use rdlt_connector_reference::{files, sqlite};
use rdlt_engine::{LocalWal, WalStore as _};
use serde_json::{Value, json};

/// A pipeline the sweep crashes: its harness configuration in a directory, and the check of
/// what the directory's destination holds.
pub(crate) struct Scenario {
    pub(crate) name: &'static str,
    /// Whether its runs keep a write-ahead log.
    pub(crate) logged: bool,
    /// Whether its stream completes: a full read, publishing what it read at its last commit.
    pub(crate) completes: bool,
    config: fn(&Path) -> Value,
    verify: fn(&Path, &str),
}

impl Scenario {
    /// This pipeline named `name`, run without a write-ahead log, as a replayable source runs by
    /// default.
    pub(crate) fn unlogged(self, name: &'static str) -> Self {
        Self {
            name,
            logged: false,
            ..self
        }
    }

    /// The harness configuration in `dir`.
    fn configured(&self, dir: &Path) -> Value {
        let mut config = (self.config)(dir);
        if !self.logged {
            config
                .as_object_mut()
                .expect("a configuration is an object")
                .remove("wal");
        }
        config
    }

    /// Writes the harness configuration into `dir`; its path.
    pub(crate) fn write(&self, dir: &Path) -> PathBuf {
        let path = dir.join("run.json");
        let config = serde_json::to_vec(&self.configured(dir)).expect("configurations serialize");
        std::fs::write(&path, config).expect("the configuration is written");
        path
    }

    /// Writes the harness configuration into `dir` with its source and destination spawned in
    /// processes of their own, and `kill` where given; its path.
    pub(crate) fn write_spawned(&self, dir: &Path, kill: Option<Value>) -> PathBuf {
        let mut config = self.configured(dir);
        config["source"]["spawned"] = json!(true);
        config["destination"]["spawned"] = json!(true);
        if let Some(kill) = kill {
            config["kill"] = kill;
        }
        let path = dir.join("run.json");
        let bytes = serde_json::to_vec(&config).expect("configurations serialize");
        std::fs::write(&path, bytes).expect("the configuration is written");
        path
    }

    /// Checks what `dir`'s destination holds, and that no log is left to replay, or none was
    /// written where the runs keep none.
    pub(crate) fn verify(&self, dir: &Path, case: &str) {
        (self.verify)(dir, &format!("{}: {case}", self.name));
        if !self.logged {
            assert!(
                !dir.join("wal").exists(),
                "{}: {case}: a log was written",
                self.name
            );
            return;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime starts");
        let pipeline = PipelineId::parse("crash").expect("a valid pipeline");
        let loads = runtime
            .block_on(LocalWal::new(dir.join("wal")).loads(&pipeline))
            .expect("the logs list");
        assert_eq!(loads, [], "{}: {case}: logs left", self.name);
    }
}

/// The harness configuration of a stream read as `read` and written as `write`, from `source`
/// into `destination`.
fn harness(dir: &Path, stream: &Value, source: &Value, destination: &Value) -> Value {
    json!({
        "pipeline": "crash", "wal": dir.join("wal"), "commit_rows": 16,
        "stream": stream, "source": source, "destination": destination,
    })
}

fn sqlite_at(dir: &Path) -> Value {
    json!({ "kind": "sqlite", "config": { "path": dir.join("out.db") } })
}

/// A log of two partitions of fifty messages that forgets what it committed, appended to SQLite.
pub(crate) fn forgetting_log() -> Scenario {
    Scenario {
        logged: true,
        completes: false,
        name: "a forgetting log",
        config: |dir| {
            let source = json!({ "kind": "log", "config": {
                "seed": 1, "group_path": dir.join("events.group"),
                "streams": [{ "name": "events", "partitions": 2, "messages": 50,
                              "batch_rows": 8, "replayable": false }],
            }});
            let stream = json!({ "name": "events", "read": "incremental", "write": "append" });
            harness(dir, &stream, &source, &sqlite_at(dir))
        },
        verify: |dir, case| {
            let batches = sqlite::published(dir.join("out.db"), "events").expect("the table reads");
            let mut messages = Vec::new();
            for batch in &batches {
                let partitions = text(batch, "partition");
                let offsets = ints(batch, "offset");
                for row in 0..batch.num_rows() {
                    messages.push((partitions[row].clone(), offsets[row]));
                }
            }
            messages.sort();
            let every: Vec<(Option<String>, Option<i64>)> = ["p0", "p1"]
                .into_iter()
                .flat_map(|partition| {
                    (0..50).map(move |offset| (Some(partition.to_owned()), Some(offset)))
                })
                .collect();
            assert_eq!(messages, every, "{case}");
        },
    }
}

/// The change stream the sweep reads: forgetting or not, timed and whole or not.
fn orders(replayable: bool, timed: bool) -> ChangedStream {
    ChangedStream {
        name: "orders".into(),
        keys: 20,
        snapshot_partitions: 2,
        changes: 120,
        batch_rows: 7,
        truncates: vec![70],
        captured: 10,
        replayable,
        changed_at: timed,
        partial: !timed,
    }
}

fn changes_of(dir: &Path, stream: &ChangedStream) -> Value {
    json!({ "kind": "changes", "config": {
        "seed": 4, "slot_path": dir.join("orders.slot"),
        "streams": [{
            "name": stream.name, "keys": stream.keys,
            "snapshot_partitions": stream.snapshot_partitions, "changes": stream.changes,
            "batch_rows": stream.batch_rows, "truncates": stream.truncates,
            "captured": stream.captured, "replayable": stream.replayable,
            "changed_at": stream.changed_at, "partial": stream.partial,
        }],
    }})
}

/// A change stream whose slot forgets what it acknowledged, merged into JSON-lines files.
pub(crate) fn forgetting_changes() -> Scenario {
    Scenario {
        logged: true,
        completes: false,
        name: "a forgetting change stream",
        config: |dir| {
            let stream = json!({ "name": "orders", "read": "cdc", "write": "merge" });
            let files = json!({ "kind": "files", "config": { "root": dir.join("out"), "format": "jsonl" } });
            harness(
                dir,
                &stream,
                &changes_of(dir, &orders(false, false)),
                &files,
            )
        },
        verify: |dir, case| {
            let batches = files::published(dir.join("out"), "orders").expect("the table reads");
            let mut table = BTreeMap::new();
            for batch in &batches {
                let (ids, values, n) = (ints(batch, "id"), text(batch, "value"), ints(batch, "n"));
                for row in 0..batch.num_rows() {
                    let id = ids[row].expect("every row has a key");
                    let merged = Row {
                        value: values[row].clone(),
                        n: n[row].unwrap_or_default(),
                    };
                    assert!(table.insert(id, merged).is_none(), "{case}: key {id} twice");
                }
            }
            assert_eq!(table, expected(4, &orders(false, false)), "{case}");
        },
    }
}

/// A full read of two thousand generated rows replacing a SQLite table, in commits enough for a
/// third.
pub(crate) fn replaced() -> Scenario {
    Scenario {
        logged: true,
        completes: true,
        name: "a full read replaced",
        config: |dir| {
            let source = json!({ "kind": "generator", "config": {
                "seed": 2, "streams": [{ "name": "orders", "rows": 2000, "partitions": 2,
                                          "batch_rows": 8 }],
            }});
            let stream = json!({ "name": "orders", "read": "full", "write": "replace" });
            harness(dir, &stream, &source, &sqlite_at(dir))
        },
        verify: |dir, case| {
            let batches = sqlite::published(dir.join("out.db"), "orders").expect("the table reads");
            let mut ids: Vec<Option<i64>> =
                batches.iter().flat_map(|batch| ints(batch, "id")).collect();
            ids.sort_unstable();
            let every: Vec<Option<i64>> = (0..2000).map(Some).collect();
            assert_eq!(ids, every, "{case}");
        },
    }
}

/// A timed change stream of whole rows kept as history in SQLite.
pub(crate) fn kept_history() -> Scenario {
    Scenario {
        logged: true,
        completes: false,
        name: "a history",
        config: |dir| {
            let stream = json!({ "name": "orders", "read": "cdc", "write": "history" });
            harness(
                dir,
                &stream,
                &changes_of(dir, &orders(true, true)),
                &sqlite_at(dir),
            )
        },
        verify: |dir, case| {
            let batches = sqlite::published(dir.join("out.db"), "orders").expect("the table reads");
            let mut versions = Vec::new();
            for batch in &batches {
                let (ids, values, n) = (ints(batch, "id"), text(batch, "value"), ints(batch, "n"));
                let (from, to) = (
                    micros(batch, "_rdlt_valid_from"),
                    micros(batch, "_rdlt_valid_to"),
                );
                let current =
                    arrow_cast::cast(column(batch, "_rdlt_is_current"), &DataType::Boolean)
                        .expect("flags");
                let current = current.as_boolean();
                for row in 0..batch.num_rows() {
                    versions.push(Version {
                        id: ids[row].expect("every version has a key"),
                        from: from[row]
                            .and_then(|at| u64::try_from(at).ok())
                            .unwrap_or(u64::MAX),
                        value: values[row].clone(),
                        n: n[row].unwrap_or_default(),
                        to: to[row].and_then(|at| u64::try_from(at).ok()),
                        current: current.value(row),
                        deleted: false,
                    });
                }
            }
            versions.sort();
            assert_eq!(versions, history(4, &orders(true, true), false), "{case}");
        },
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a dyn Array {
    batch
        .column_by_name(name)
        .expect("a published column")
        .as_ref()
}

fn ints(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let values = arrow_cast::cast(column(batch, name), &DataType::Int64).expect("integers");
    values.as_primitive::<Int64Type>().iter().collect()
}

fn text(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let values = arrow_cast::cast(column(batch, name), &DataType::Utf8).expect("text");
    values
        .as_string::<i32>()
        .iter()
        .map(|value| value.map(str::to_owned))
        .collect()
}

fn micros(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let at = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let values = arrow_cast::cast(column(batch, name), &at).expect("a timestamp");
    let values = arrow_cast::cast(&values, &DataType::Int64).expect("microseconds");
    values.as_primitive::<Int64Type>().iter().collect()
}
