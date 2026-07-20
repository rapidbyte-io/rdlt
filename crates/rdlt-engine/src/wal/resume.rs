//! WAL resume: single forward scan of the manifest, replay of the uncommitted span
//! (crash-matrix row 2), degradation to re-extraction on any damage (row 4 — slower,
//! never wrong).

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rdlt_connector::LoadSession;
use rdlt_core::{CommitMeta, LoadId, RdltError, StateDoc};

use super::WalRecord;
use crate::load::LoadItem;

/// The uncommitted tail of a previous run.
#[derive(Debug)]
pub(crate) struct RecoverySpan {
    pub(crate) load_id: LoadId,
    /// The seq the recovery commit must use — max committed seq of that load + 1.
    /// If the crash was mid-commit the destination already holds this seq and
    /// idempotence returns the prior receipt.
    pub(crate) next_commit_seq: u64,
    pub(crate) records: Vec<WalRecord>,
    /// Latest known schema + mode per table across the WHOLE manifest (committed
    /// spans included). A span whose schema delta committed earlier still needs
    /// `ensure_table` on the fresh recovery session (clause E1) — sessions register
    /// publishable tables per session.
    pub(crate) schemas: Vec<(rdlt_core::TableSchema, rdlt_core::WriteMode)>,
}

/// Scan outcome. `Damaged` means segments/manifest can't support replay — the caller
/// clears the WAL and falls back to cursor re-extraction.
#[derive(Debug)]
pub(crate) enum Scan {
    Nothing,
    Recover(RecoverySpan),
    Damaged(String),
}

/// Forward-scan the manifest. A torn FINAL line (crash mid-append) is truncated;
/// damage anywhere else degrades to re-extraction.
pub(crate) fn scan(dir: &Path) -> Scan {
    let path = dir.join("manifest.jsonl");
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return Scan::Nothing,
    };
    let mut records: Vec<WalRecord> = Vec::new();
    let mut damaged: Option<String> = None;
    let mut lines = BufReader::new(file).lines();
    while let Some(line) = lines.next() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                damaged = Some(format!("manifest read: {e}"));
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<WalRecord>(&line) {
            Ok(record) => records.push(record),
            Err(e) => {
                // Torn tail is fine only if nothing follows it.
                if lines.next().is_some() {
                    damaged = Some(format!("mid-manifest corruption: {e}"));
                }
                break;
            }
        }
    }
    if let Some(reason) = damaged {
        return Scan::Damaged(reason);
    }

    // Find the uncommitted tail: records after the last Committed, within the last Run.
    // Schemas accumulate across the WHOLE manifest: a replay span may contain batches
    // for tables whose delta committed in an earlier span.
    let mut load_id: Option<LoadId> = None;
    let mut max_committed_seq: u64 = 0;
    let mut span: Vec<WalRecord> = Vec::new();
    let mut schemas: std::collections::BTreeMap<
        rdlt_core::TableName,
        (rdlt_core::TableSchema, rdlt_core::WriteMode),
    > = std::collections::BTreeMap::new();
    for record in records {
        if let WalRecord::Delta { schema, mode, .. } = &record {
            schemas.insert(schema.table.clone(), (schema.clone(), mode.clone()));
        }
        match record {
            WalRecord::Run {
                format_version,
                load_id: id,
                ..
            } => {
                if format_version > super::WAL_FORMAT_VERSION {
                    // A future engine wrote this workdir: don't guess — degrade to
                    // cursor re-extraction (slower, never wrong).
                    return Scan::Damaged(format!(
                        "manifest format v{format_version} is newer than supported \
                         v{}",
                        super::WAL_FORMAT_VERSION
                    ));
                }
                // A run only ever starts after the previous span was resolved
                // (recovery runs before `Wal::open` appends the new header), so a Run
                // record always begins a fresh span.
                span.clear();
                load_id = Some(id);
                max_committed_seq = 0;
            }
            WalRecord::Committed { commit_seq } => {
                max_committed_seq = max_committed_seq.max(commit_seq);
                span.clear();
            }
            other => span.push(other),
        }
    }

    // CRITICAL: replay only up to the LAST checkpoint. Segments beyond it are not
    // covered by any cursor — committing them would double-apply once the source
    // re-extracts that range (recovery invariant 2). The uncovered tail is discarded;
    // re-extraction re-delivers it. A span with no checkpoint at all has nothing
    // safely replayable.
    let last_checkpoint = span
        .iter()
        .rposition(|r| matches!(r, WalRecord::Checkpoint { .. }));
    match (load_id, last_checkpoint) {
        (Some(load_id), Some(idx)) => {
            span.truncate(idx + 1);
            Scan::Recover(RecoverySpan {
                load_id,
                next_commit_seq: max_committed_seq + 1,
                records: span,
                schemas: schemas.into_values().collect(),
            })
        }
        _ => Scan::Nothing,
    }
}

/// Replay one span into an open session and commit it under the ORIGINAL run's
/// identity. Returns the number of replayed batches; `Err(Damaged…)`-style failures
/// come back as `Ok(None)` so the caller can degrade to re-extraction.
pub(crate) async fn replay(
    dir: &Path,
    span: RecoverySpan,
    session: &mut Box<dyn LoadSession>,
    state: &mut StateDoc,
    caps: rdlt_connector::DestCapabilities,
) -> Result<Option<u64>, RdltError> {
    // Damage checks first: all segment files must be readable before any write.
    let mut items: Vec<LoadItem> = Vec::new();
    let mut batches: u64 = 0;
    for record in span.records {
        match record {
            WalRecord::Delta {
                schema,
                delta,
                mode,
            } => {
                items.push(LoadItem::Delta {
                    schema,
                    delta,
                    mode,
                });
            }
            WalRecord::Checkpoint { stream, cursor } => {
                items.push(LoadItem::Checkpoint { stream, cursor });
            }
            WalRecord::Segment { table, file, .. } => {
                let path = dir.join(&file);
                let reader = match File::open(&path)
                    .map_err(|e| e.to_string())
                    .and_then(|f| {
                        ParquetRecordBatchReaderBuilder::try_new(f).map_err(|e| e.to_string())
                    })
                    .and_then(|b| b.build().map_err(|e| e.to_string()))
                {
                    Ok(reader) => reader,
                    Err(_) => return Ok(None), // quarantine → degrade to re-extract
                };
                for batch in reader {
                    match batch {
                        Ok(batch) => {
                            batches += 1;
                            items.push(LoadItem::Batch {
                                table: table.clone(),
                                batch,
                            });
                        }
                        Err(_) => return Ok(None),
                    }
                }
            }
            WalRecord::Run { .. } | WalRecord::Committed { .. } => {}
        }
    }

    // Every known table is ensured on THIS session before any write: destinations
    // register publishable tables per session, and a span's delta may have committed
    // in an earlier span (clause E1; review finding — silently lost spans otherwise).
    for (schema, mode) in &span.schemas {
        let lowered = crate::load::lowering::lower_schema(schema, &caps);
        session
            .ensure_table(&lowered, mode)
            .await
            .map_err(RdltError::destination)?;
        state
            .schema_hashes
            .insert(schema.table.clone(), schema.content_hash());
    }

    // Apply in WAL order: delta-before-batch survives crashes (invariant 3).
    for item in items {
        match item {
            LoadItem::Delta {
                schema,
                delta: _,
                mode,
            } => {
                // Same lowering seam as the live loader (design doc §5.3).
                let lowered = crate::load::lowering::lower_schema(&schema, &caps);
                session
                    .ensure_table(&lowered, &mode)
                    .await
                    .map_err(RdltError::destination)?;
                state
                    .schema_hashes
                    .insert(schema.table.clone(), schema.content_hash());
            }
            LoadItem::Batch { table, batch } => {
                let lowered = crate::load::lowering::lower_batch(&batch, &caps)?;
                session
                    .write(&table, lowered)
                    .await
                    .map_err(RdltError::destination)?;
            }
            LoadItem::Checkpoint { stream, cursor } => {
                state.cursors.insert(stream, cursor);
            }
            // Never persisted to the WAL; nothing to replay.
            LoadItem::Discarded { .. } => {}
        }
    }

    state.last_commit = Some(rdlt_core::LastCommit {
        load_id: span.load_id.clone(),
        commit_seq: span.next_commit_seq,
    });
    session
        .commit(CommitMeta {
            load_id: span.load_id,
            commit_seq: span.next_commit_seq,
            state: state.clone(),
            counters: Default::default(),
        })
        .await
        .map_err(RdltError::destination)?;
    Ok(Some(batches))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_core::{LoadId, PipelineId};

    fn write_manifest(dir: &std::path::Path, records: &[WalRecord]) {
        let mut out = String::new();
        for record in records {
            out.push_str(&serde_json::to_string(record).expect("record json"));
            out.push('\n');
        }
        std::fs::write(dir.join("manifest.jsonl"), out).expect("write manifest");
    }

    /// Mutation-report closure: the future-version guard is `>`, strictly — a
    /// NEWER manifest degrades to re-extraction (Damaged), while the current
    /// and any older version scan normally.
    #[test]
    fn future_manifest_version_degrades_older_scans_fine() {
        let run = |version: u32| {
            let dir = tempfile::tempdir().expect("tempdir");
            write_manifest(
                dir.path(),
                &[WalRecord::Run {
                    format_version: version,
                    load_id: LoadId::new("l"),
                    pipeline: PipelineId::new("p"),
                }],
            );
            scan(dir.path())
        };
        assert!(
            matches!(run(super::super::WAL_FORMAT_VERSION + 1), Scan::Damaged(_)),
            "future version must degrade"
        );
        // Current version: an empty span (no checkpoint) scans to Nothing, not Damaged.
        assert!(matches!(
            run(super::super::WAL_FORMAT_VERSION),
            Scan::Nothing
        ));
    }
}
