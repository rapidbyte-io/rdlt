//! The manifest's on-disk vocabulary: one record shape per line, versioned by
//! the `Run` header.

use rdlt_core::{
    Cursor, LoadId, PipelineId, SchemaDelta, StreamName, TableName, TableSchema, WriteMode,
};
use serde::{Deserialize, Serialize};

/// WAL format version, covering the manifest record shapes AND the segment
/// container together — they are one format, because a manifest line is only
/// meaningful if the segment it names can be decoded.
///
/// v1: parquet segments. v2: Arrow IPC file segments (`.arrow`).
///
/// A manifest at any other version is REFUSED, in both directions. Refusing a
/// newer one is obvious; refusing an older one matters just as much here,
/// because a v1 manifest names parquet segments this build cannot read — and
/// discovering that at segment-open time would report "unreadable segment"
/// where the truth is "different format". Recovery degrades to source
/// re-extraction either way: slower, never wrong.
pub(crate) const WAL_FORMAT_VERSION: u32 = 2;

/// The serde fallback for a manifest whose `Run` header predates the versioned
/// header field. Pinned to `1` FOREVER — such a manifest is by definition a v1
/// one, so defaulting it to the current version would claim parquet segments
/// are Arrow IPC and hand corrupt input to the reader. Deliberately not
/// [`WAL_FORMAT_VERSION`]: that constant moves, this one describes history.
fn initial_wal_version() -> u32 {
    1
}

/// One manifest line. Order on disk IS the replay order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "rec", rename_all = "snake_case")]
pub(crate) enum WalRecord {
    /// First record of each run; identifies the load for recovery commits and
    /// carries the manifest format version.
    Run {
        #[serde(default = "initial_wal_version")]
        format_version: u32,
        load_id: LoadId,
        pipeline: PipelineId,
    },
    Delta {
        schema: TableSchema,
        delta: SchemaDelta,
        mode: WriteMode,
    },
    Segment {
        table: TableName,
        file: String,
        rows: u64,
    },
    Checkpoint {
        stream: StreamName,
        cursor: Cursor,
    },
    Committed {
        commit_seq: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `initial_wal_version` describes HISTORY: a manifest with no version field
    /// is by definition a v1 one. Defaulting it to the current version would
    /// claim its parquet segments are Arrow IPC and hand corrupt bytes to the
    /// reader, so this must stay 1 even as `WAL_FORMAT_VERSION` moves.
    #[test]
    fn a_headerless_manifest_version_defaults_to_one_forever() {
        assert_eq!(initial_wal_version(), 1);
        let decoded: WalRecord =
            serde_json::from_str(r#"{"rec":"run","load_id":"l","pipeline":"p"}"#)
                .expect("a pre-versioning header still decodes");
        match decoded {
            WalRecord::Run { format_version, .. } => assert_eq!(
                format_version, 1,
                "absent version means v1, never the current version"
            ),
            other => panic!("expected a Run header, got {other:?}"),
        }
    }
}
