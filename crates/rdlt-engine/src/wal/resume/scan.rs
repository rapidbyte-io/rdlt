//! Forward scan of the manifest: classify what is on disk into a
//! [`ScanOutcome`] without touching a segment or a session.

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use rdlt_core::LoadId;

use crate::wal::WalRecord;

use super::blocking::off_runtime;

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
    /// `ensure_table` on the fresh recovery session — sessions register
    /// publishable tables per session.
    pub(crate) schemas: Vec<(rdlt_core::TableSchema, rdlt_core::WriteMode)>,
}

/// ScanOutcome outcome. `Damaged` means segments/manifest can't support replay — the caller
/// clears the WAL and falls back to cursor re-extraction.
#[derive(Debug)]
pub(crate) enum ScanOutcome {
    /// No manifest on disk: nothing was ever written here.
    Nothing,
    /// A manifest WAS read, but it holds nothing replayable — a span that never
    /// reached a checkpoint. Distinct from `Nothing` because the difference is
    /// what to do next: there is residue on disk, and leaving it means a
    /// pipeline that keeps dying before its first checkpoint accumulates
    /// manifest lines and orphaned segments without bound.
    Discard,
    Recover(RecoverySpan),
    Damaged(String),
    /// The manifest is intact and readable, but was written under a different
    /// format version, so its segments are in a container this build does not
    /// decode. Kept distinct from `Damaged` so the log — and any test — can
    /// tell "different version" from "corruption" by SHAPE rather than by
    /// matching words in a message.
    Unsupported {
        found: u32,
        supported: u32,
    },
}

/// Forward-scan the manifest. A torn FINAL line (crash mid-append) is truncated;
/// damage anywhere else degrades to re-extraction.
/// Async wrapper: the scan reads the manifest line by line, which is blocking
/// file I/O and belongs off an embedder's runtime for the same reason replay's
/// decoding does.
pub(crate) async fn scan_off_runtime(dir: &Path) -> ScanOutcome {
    let dir = dir.to_path_buf();
    off_runtime(move || scan(&dir)).await
}

pub(crate) fn scan(dir: &Path) -> ScanOutcome {
    let path = dir.join("manifest.jsonl");
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return ScanOutcome::Nothing,
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
        return ScanOutcome::Damaged(reason);
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
                if format_version != crate::wal::WAL_FORMAT_VERSION {
                    // EXACT match, in both directions. A newer manifest was
                    // written by an engine whose records this build cannot be
                    // trusted to read; an older one names segments in a
                    // container this build no longer decodes. Neither is
                    // guessable — degrade to cursor re-extraction.
                    return ScanOutcome::Unsupported {
                        found: format_version,
                        supported: crate::wal::WAL_FORMAT_VERSION,
                    };
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
    // re-extracts that range. The uncovered tail is discarded;
    // re-extraction re-delivers it. A span with no checkpoint at all has nothing
    // safely replayable.
    let last_checkpoint = span
        .iter()
        .rposition(|r| matches!(r, WalRecord::Checkpoint { .. }));
    match (load_id, last_checkpoint) {
        (Some(load_id), Some(idx)) => {
            span.truncate(idx + 1);
            ScanOutcome::Recover(RecoverySpan {
                load_id,
                next_commit_seq: max_committed_seq + 1,
                records: span,
                schemas: schemas.into_values().collect(),
            })
        }
        // A span with no checkpoint has nothing safely replayable — but the
        // manifest and its segments are on disk, so say so rather than reporting
        // an empty workdir.
        _ => ScanOutcome::Discard,
    }
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

    /// Mutation-report closure: the on-disk Run header must SERIALIZE the
    /// current format version — a defaulted or zero version would break forward
    /// detection.
    #[test]
    fn run_header_serializes_current_format_version() {
        let record = WalRecord::Run {
            format_version: crate::wal::WAL_FORMAT_VERSION,
            load_id: LoadId::new("l"),
            pipeline: PipelineId::new("p"),
        };
        let json = serde_json::to_string(&record).expect("serialize");
        assert!(
            json.contains(&format!(
                "\"format_version\":{}",
                crate::wal::WAL_FORMAT_VERSION
            )),
            "header must carry the version: {json}"
        );
        assert_eq!(
            crate::wal::WAL_FORMAT_VERSION,
            2,
            "bump deliberately, with a migration note"
        );
    }

    /// The version gate is EXACT, in both directions. A newer manifest carries
    /// records this build cannot be trusted to read; an older one names
    /// segments in a container it no longer decodes. Both degrade to
    /// re-extraction, and both report `Unsupported` rather than `Damaged` so
    /// the two causes stay distinguishable by shape, not by message text.
    #[test]
    fn any_other_manifest_version_is_unsupported_current_scans_fine() {
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
        let current = crate::wal::WAL_FORMAT_VERSION;
        assert!(
            matches!(run(current + 1), ScanOutcome::Unsupported { found, supported }
                     if found == current + 1 && supported == current),
            "a newer manifest must be refused by version"
        );
        assert!(
            matches!(run(current - 1), ScanOutcome::Unsupported { found, supported }
                     if found == current - 1 && supported == current),
            "an older manifest names segments in the previous container and must \
             be refused by version, not discovered unreadable at open time"
        );
        // Current version, no checkpoint: nothing is replayable, but a manifest
        // and its segments ARE on disk — `Discard` so the caller clears them.
        // `Nothing` would leave residue to accumulate across repeated crashes
        // before the first checkpoint.
        assert!(matches!(run(current), ScanOutcome::Discard));
    }

    /// A manifest predating the versioned header defaults to v1 — and must
    /// therefore be refused now, not treated as current. Defaulting it to the
    /// current version would claim its parquet segments are Arrow IPC.
    #[test]
    fn a_headerless_manifest_defaults_to_v1_and_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("manifest.jsonl"),
            "{\"rec\":\"run\",\"load_id\":\"l\",\"pipeline\":\"p\"}\n",
        )
        .expect("write manifest");
        assert!(
            matches!(scan(dir.path()), ScanOutcome::Unsupported { found: 1, .. }),
            "an unversioned header is a v1 manifest"
        );
    }
}

#[cfg(test)]
mod starvation_tests {
    //! Recovery must not monopolise the runtime it is polled on.
    //!
    //! These assert PROGRESS OF OTHER WORK, never a duration. rdlt is embedded
    //! in someone else's runtime, so the property that matters is "the host
    //! keeps running", and that is what is checked — a timing assertion here
    //! would be a throughput claim this change does not make, and would go
    //! flaky on a loaded machine besides.
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// One worker thread: the harshest honest setting. With the blocking work
    /// inline, that single worker is inside file I/O and the co-tenant task
    /// cannot be polled at all.
    fn single_worker_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime")
    }

    /// A manifest big enough that scanning it is real work rather than a
    /// syscall — the co-tenant needs a window in which to be starved.
    fn big_manifest(dir: &std::path::Path) {
        let mut records = vec![WalRecord::Run {
            format_version: crate::wal::WAL_FORMAT_VERSION,
            load_id: LoadId::from("starve"),
            pipeline: rdlt_core::PipelineId::from("p"),
        }];
        for seq in 0..20_000u64 {
            records.push(WalRecord::Checkpoint {
                stream: rdlt_core::StreamName::from("s"),
                cursor: rdlt_core::Cursor::new(format!("c{seq}")),
            });
        }
        let mut out = String::new();
        for record in &records {
            out.push_str(&serde_json::to_string(record).expect("record json"));
            out.push('\n');
        }
        std::fs::write(dir.join("manifest.jsonl"), out).expect("write manifest");
    }

    #[test]
    fn scanning_the_manifest_leaves_the_runtime_able_to_poll_other_work() {
        let dir = tempfile::tempdir().expect("tempdir");
        big_manifest(dir.path());

        let runtime = single_worker_runtime();
        let ticks = Arc::new(AtomicU64::new(0));
        let (during, scan) = runtime.block_on(async {
            let ticks_for_tenant = Arc::clone(&ticks);
            // A tight yield loop, NOT a sleep: it counts how many times the
            // worker was free to poll it, which is precisely the property at
            // issue. A sleeping tenant would measure elapsed time instead, and
            // that is the throughput claim this change does not make.
            let tenant = tokio::spawn(async move {
                loop {
                    ticks_for_tenant.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            });
            // The scan must be SPAWNED, not awaited here: `block_on` drives its
            // future on the calling thread, which would never contend with the
            // worker the tenant runs on, and the test would pass either way.
            //
            // It also has to SAY when it starts. Snapshotting at spawn time
            // measures the gap before the task is scheduled, during which the
            // tenant spins freely — enough to satisfy any `> 0` assertion no
            // matter how the scan behaves.
            let path = dir.path().to_path_buf();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let scanner = tokio::spawn(async move {
                let _ = started_tx.send(());
                scan_off_runtime(&path).await
            });
            started_rx.await.expect("scan started");
            let before = ticks.load(Ordering::Relaxed);
            let scan = scanner.await.expect("scan task");
            let during = ticks.load(Ordering::Relaxed) - before;
            tenant.abort();
            (during, scan)
        });

        // A starvation test that passes because the work never happened proves
        // nothing, so the scan's own result is asserted too.
        assert!(
            matches!(scan, ScanOutcome::Recover(_)),
            "the scan itself must still succeed"
        );
        assert!(
            during > 0,
            "the co-tenant was starved for the whole manifest scan: 0 polls"
        );
    }
}
