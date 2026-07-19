//! Typed observability events (data-model.md §7, embedder-api.md O1–O2).
//!
//! Causal-order guarantees (clause R3): a table's `SchemaEvolved` precedes the first
//! `BatchLoaded` at the new version; `Committed` follows everything it covers.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::cursor::Cursor;
use crate::ids::{StreamName, TableName};
use crate::schema::SchemaDelta;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
#[non_exhaustive]
pub enum PipelineEvent {
    StreamStarted {
        stream: StreamName,
    },
    BatchLoaded {
        table: TableName,
        rows: u64,
        bytes: u64,
    },
    SchemaEvolved {
        delta: SchemaDelta,
    },
    Committed {
        commit_seq: u64,
        cursors: BTreeMap<StreamName, Cursor>,
    },
    /// A transient connector failure was retried by the engine.
    Retried {
        stream: Option<StreamName>,
        attempt: u32,
    },
    /// Rows or values were discarded under a Discard* policy — counted, never silent.
    Discarded {
        table: TableName,
        rows: u64,
        values: u64,
        reason: String,
    },
    StreamFinished {
        stream: StreamName,
    },
}
