//! The write half of the WAL: segment files, the append-only manifest, and the
//! three-step commit protocol.
//!
//! Segments carry no dictionary, no statistics and no compression. A columnar
//! analytics container spends its encoding effort making data queryable, and
//! nothing ever queries a segment: it is written, replayed at most once, and
//! unlinked. The container is chosen for cheap round-tripping and for refusing
//! a truncated file, not for what a reader could do with it.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt as _,
    path::{Path, PathBuf},
};

use rdlt_connector::RecordBatch;
use rdlt_core::{LoadId, PipelineId, RdltError, crash_point};

use crate::load::LoadItem;

use super::record::{WAL_FORMAT_VERSION, WalRecord};

/// The WAL's directory inside a pipeline workdir — the one piece of the
/// on-disk layout, alongside the manifest and segment names below, that the
/// WAL owns rather than its callers.
pub(crate) fn dir_in(workdir: &Path) -> PathBuf {
    workdir.join("wal")
}

/// Create `dir` (and any missing parents) PRIVATE to the operating
/// user (0o700 on every component this call creates): the WAL holds
/// cleartext batch data and cursors, and a default-umask directory in
/// a shared location would let any local user read in-flight rows. An
/// already-existing directory keeps its mode — the operator's own
/// arrangement is not overridden. Shared with the workdir lock, so the
/// whole workdir tree is born private, not just the WAL under it.
pub(crate) fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

/// [`OpenOptions`] for a WAL-owned file, born owner-only (0o600) —
/// the file-level half of the directory hardening above: the mode
/// applies only at creation, so a file that already exists keeps
/// whatever the operator gave it. Every file the WAL creates —
/// manifest, rules sidecar, segments — goes through this, so none can
/// silently fall back to the umask default.
fn private_file() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.mode(0o600);
    options
}

/// The rules sidecar beside the manifest: the writer's `IdentRules`,
/// serialized verbatim. Recovery's stream↔segment join normalizes under
/// the resuming run's rules, and that join is only sound when they are
/// the WRITING run's rules — the sidecar is what proves it. A workdir
/// file beside the manifest, not a record in the manifest stream;
/// reclaimed with the directory by [`clear`].
pub(crate) const RULES_SIDECAR: &str = "rules.json";

/// Proof that the `wal` leaf was created or explicitly adopted by rdlt. The
/// marker is intentionally inside the leaf that cleanup removes; an explicit
/// workdir pointing at a non-empty foreign `wal` directory can no longer turn
/// that spelling into authority for `remove_dir_all`.
pub(crate) const OWNERSHIP_MARKER: &str = ".rdlt-wal";
const OWNERSHIP_MARKER_BYTES: &[u8] = b"rdlt-wal-v1\n";

fn ensure_owned_dir(dir: &Path) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(dir)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing WAL path `{}` because it is not a real directory",
                dir.display()
            ),
        ));
    }

    let marker = dir.join(OWNERSHIP_MARKER);
    match std::fs::symlink_metadata(&marker) {
        Ok(meta) if meta.file_type().is_file() && !meta.file_type().is_symlink() => {
            let bytes = std::fs::read(&marker)?;
            if bytes == OWNERSHIP_MARKER_BYTES {
                return Ok(());
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "WAL ownership marker `{}` has unrecognized contents",
                    marker.display()
                ),
            ));
        }
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "WAL ownership marker `{}` is not a regular file",
                    marker.display()
                ),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    if std::fs::read_dir(dir)?.next().is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing to adopt non-empty directory `{}` as a WAL without `{OWNERSHIP_MARKER}`",
                dir.display()
            ),
        ));
    }
    private_file()
        .create_new(true)
        .write(true)
        .open(marker)?
        .write_all(OWNERSHIP_MARKER_BYTES)
}

fn verify_owned_dir(dir: &Path) -> std::io::Result<()> {
    let dir_meta = std::fs::symlink_metadata(dir)?;
    if !dir_meta.file_type().is_dir() || dir_meta.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing to clear non-directory WAL path `{}`",
                dir.display()
            ),
        ));
    }
    let marker = dir.join(OWNERSHIP_MARKER);
    let meta = std::fs::symlink_metadata(&marker)?;
    if !meta.file_type().is_file()
        || meta.file_type().is_symlink()
        || std::fs::read(&marker)? != OWNERSHIP_MARKER_BYTES
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing to clear `{}` without a valid rdlt WAL ownership marker",
                dir.display()
            ),
        ));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct Wal {
    dir: PathBuf,
    manifest: File,
    load_id: LoadId,
    segment_seq: u64,
    /// Segment files written since the last fsync barrier.
    pending_sync: Vec<PathBuf>,
    /// Segment files of the current uncommitted span (GC'd after receipt).
    pending_gc: Vec<PathBuf>,
}

fn wal_err(context: &str, e: impl std::fmt::Display) -> RdltError {
    RdltError::wal(format!("{context}: {e}"))
}

impl Wal {
    /// Open (creating if needed) the WAL for a new run and append its
    /// `Run` header. `tolerate_resolved_residue` is recovery's voucher
    /// (round-13): true ONLY when the scan resolved the surviving
    /// manifest as holding nothing replayable and its clear failed —
    /// the new run's records then append after the resolved span, the
    /// manifest format's own multi-Run shape.
    pub(crate) fn open(
        dir: PathBuf,
        pipeline: &PipelineId,
        load_id: &LoadId,
        rules: rdlt_core::naming::IdentRules,
        tolerate_resolved_residue: bool,
    ) -> Result<Self, RdltError> {
        create_private_dir(&dir).map_err(|e| wal_err("creating wal dir", e))?;
        ensure_owned_dir(&dir).map_err(|e| wal_err("proving wal directory ownership", e))?;
        // A fresh open expects a CLEAN directory (round-12): recovery
        // resolves and clears any prior span before this runs, so a
        // surviving manifest here is unresolved residue — writing a
        // new Run header (and a fresh sidecar) over it would mask the
        // very drift gates the sidecar exists for. Refuse, naming the
        // residue — unless recovery vouched (see above).
        let manifest_path = dir.join("manifest.jsonl");
        if manifest_path.exists() && !tolerate_resolved_residue {
            return Err(RdltError::wal(format!(
                "a WAL manifest already exists at `{}` — a fresh run opens over a clean \
                 directory (recovery resolves and clears a prior span first), so \
                 surviving residue means the previous span was never resolved; refusing \
                 to write over it",
                manifest_path.display()
            )));
        }
        // The rules sidecar goes down BEFORE the manifest is created,
        // so a manifest can never exist without it: recovery refuses a
        // sidecar-less manifest as an unrecognized workdir state, the
        // same Damaged degradation a recorded mismatch gets. See
        // [`RULES_SIDECAR`] for why the rules must be recorded at all.
        let sidecar =
            serde_json::to_vec(&rules).map_err(|e| wal_err("encoding rules sidecar", e))?;
        // Unlink-then-create_new, NOT create+truncate (047 L4): a truncating
        // open FOLLOWS a pre-planted symlink and clobbers its target. The
        // unlink removes any residue (a symlink itself, never its target; a
        // real sidecar from the crash window between sidecar write and
        // manifest creation, or from vouched residue — both legitimately
        // re-written here), and `create_new` (O_EXCL, which refuses to
        // resolve through a symlink) makes the create fail LOUDLY if
        // anything reappears in between.
        let sidecar_path = dir.join(RULES_SIDECAR);
        match std::fs::remove_file(&sidecar_path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(wal_err("clearing prior rules sidecar", e));
            }
            _ => {}
        }
        private_file()
            .create_new(true)
            .write(true)
            .open(&sidecar_path)
            .and_then(|mut file| file.write_all(&sidecar))
            .map_err(|e| wal_err("writing rules sidecar", e))?;
        // The unvouched open is `create_new` for the same symlink reason —
        // the residue refusal above already promised the path is absent, and
        // O_EXCL keeps that promise atomic (a DANGLING symlink passes the
        // `exists()` check yet would be followed by a plain create). The
        // vouched open must append to the surviving resolved manifest, so it
        // alone keeps the plain create.
        let mut manifest_options = private_file();
        if tolerate_resolved_residue {
            manifest_options.create(true);
        } else {
            manifest_options.create_new(true);
        }
        // The vouched path opens an existing manifest, so `create_new` cannot
        // provide its usual symlink protection there. O_NOFOLLOW closes both
        // paths atomically at the final component.
        manifest_options.custom_flags(libc::O_NOFOLLOW);
        let manifest = manifest_options
            .append(true)
            .open(dir.join("manifest.jsonl"))
            .map_err(|e| wal_err("opening manifest", e))?;
        let mut wal = Self {
            dir,
            manifest,
            load_id: load_id.clone(),
            segment_seq: 0,
            pending_sync: Vec::new(),
            pending_gc: Vec::new(),
        };
        wal.append(&WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: load_id.clone(),
            pipeline: pipeline.clone(),
        })?;
        Ok(wal)
    }

    /// Record one load item ahead of applying it to the destination.
    ///
    /// The segment is written, then its manifest line appended. That order is
    /// the durability rule: replay follows manifest lines, so a segment must
    /// exist before anything names it. A crash between the two leaves an
    /// unreferenced file, which replay ignores and `clear` removes.
    ///
    /// The write happens ON THIS TASK, deliberately. Moving it to the blocking
    /// pool measured 6.7 ms per batch SLOWER on 8 MiB batches: the encode reads
    /// a batch that was just produced on this thread, and handing it to another
    /// core costs more in cache misses than the freed runtime thread is worth
    /// while the pipeline is serial and nothing else can use it.
    ///
    /// Deliberately NOT pipelined against the destination write either: the
    /// manifest's order on disk IS the replay order.
    pub(crate) async fn record(&mut self, item: &LoadItem) -> Result<(), RdltError> {
        match item {
            LoadItem::Delta {
                schema,
                delta,
                mode,
            } => self.append(&WalRecord::Delta {
                schema: schema.clone(),
                delta: delta.clone(),
                mode: mode.clone(),
            }),
            LoadItem::Checkpoint { stream, cursor } => self.append(&WalRecord::Checkpoint {
                stream: stream.clone(),
                cursor: cursor.clone(),
            }),
            LoadItem::Batch { table, batch, .. } => {
                crash_point!(
                    "wal.segment.write",
                    Err(wal_err(
                        "write segment",
                        std::io::Error::other("injected crash"),
                    ))
                );
                // The ONE name format, shared with recovery's read-side gate
                // (`record::verify_segment_file`) so writer and checker
                // cannot drift.
                let file = super::record::segment_file_name(&self.load_id, self.segment_seq);
                self.segment_seq += 1;
                let path = self.dir.join(&file);
                write_segment(&path, batch)?;
                self.pending_sync.push(path.clone());
                self.pending_gc.push(path);
                self.append(&WalRecord::Segment {
                    table: table.clone(),
                    file,
                    rows: batch.num_rows() as u64,
                })
            }
            // Report-only accounting; a replay regenerates nothing from it.
            LoadItem::Discarded { .. } => Ok(()),
        }
    }

    /// Step (1) of the commit protocol: make the whole span durable.
    ///
    /// The fsyncs DO go to the blocking pool: unlike the segment encode they are
    /// pure kernel wait with no working set to keep warm, so nothing is lost by
    /// moving them and a runtime thread is freed for the duration.
    ///
    /// TWO blocking hops, not one, and the split is forced rather than chosen:
    /// `crash_point!` expands to a fail point whose closure form RETURNS from
    /// the enclosing function, so moving one inside a `spawn_blocking` closure
    /// would change what it returns from — and under the panic action would
    /// move the panic onto a pool thread. `wal.manifest.fsync` sits between the
    /// segment fsyncs and the manifest fsync, so it stays on this side and the
    /// two fsync groups go over separately.
    /// Mutation note: replacing this body with `Ok(())` is UNKILLABLE by any
    /// test this suite can run, and that is a property of fsync rather than a
    /// gap in the pins.
    ///
    /// What this method buys is durability across POWER LOSS: without the
    /// fsyncs the data is still in the page cache, so every read — including a
    /// full crash-recovery replay after `kill -9` — returns exactly the same
    /// bytes. The difference appears only when the kernel dies with the cache
    /// unwritten, which no in-process test can produce. The crash sweep covers
    /// process death, which is a strictly weaker fault.
    ///
    /// Recorded rather than papered over: a test asserting "commit succeeded"
    /// here would pass with the fsyncs removed and would falsely claim the
    /// durability barrier is covered. Verifying it needs a different KIND of
    /// instrument (a fault-injecting filesystem, or hardware), not another
    /// assertion.
    pub(crate) async fn sync_for_commit(&mut self) -> Result<(), RdltError> {
        crash_point!(
            "wal.segment.fsync",
            Err(wal_err(
                "fsync segment",
                std::io::Error::other("injected crash"),
            ))
        );
        let pending = std::mem::take(&mut self.pending_sync);
        let dir = self.dir.clone();
        tokio::task::spawn_blocking(move || {
            for path in pending {
                File::open(&path)
                    .and_then(|f| f.sync_all())
                    .map_err(|e| wal_err("fsync segment", e))?;
            }
            // Persist directory entries for the sidecar, manifest and all
            // newly-created segments. File fsync alone does not make those
            // names survive power loss.
            File::open(&dir)
                .and_then(|f| f.sync_all())
                .map_err(|e| wal_err("fsync wal directory", e))?;
            Ok::<(), RdltError>(())
        })
        .await
        .map_err(|e| wal_err("segment fsync task", e))??;
        // A no-op on `File` (nothing is buffered in userspace), kept because it
        // states where the userspace boundary is.
        self.manifest
            .flush()
            .map_err(|e| wal_err("flush manifest", e))?;
        crash_point!(
            "wal.manifest.fsync",
            Err(wal_err(
                "fsync manifest",
                std::io::Error::other("injected crash"),
            ))
        );
        let handle = self
            .manifest
            .try_clone()
            .map_err(|e| wal_err("fsync manifest", e))?;
        tokio::task::spawn_blocking(move || {
            handle.sync_all().map_err(|e| wal_err("fsync manifest", e))
        })
        .await
        .map_err(|e| wal_err("manifest fsync task", e))??;
        Ok(())
    }

    /// Step (3): the destination acknowledged `commit_seq` — mark and reclaim.
    pub(crate) async fn mark_committed(&mut self, commit_seq: u64) -> Result<(), RdltError> {
        self.append(&WalRecord::Committed { commit_seq })?;
        self.manifest
            .flush()
            .map_err(|e| wal_err("flush committed marker", e))?;
        let manifest = self
            .manifest
            .try_clone()
            .map_err(|e| wal_err("fsync committed marker", e))?;
        tokio::task::spawn_blocking(move || {
            manifest
                .sync_all()
                .map_err(|e| wal_err("fsync committed marker", e))
        })
        .await
        .map_err(|e| wal_err("committed marker fsync task", e))??;

        // Only a durable Committed marker licenses segment reclamation. If the
        // fsync above fails, `pending_gc` remains intact for a later retry.
        let reclaim = std::mem::take(&mut self.pending_gc);
        // Best-effort: a survivor just gets replay-skipped via the Committed
        // record, so unlinking never blocks the commit's completion.
        let _ = tokio::task::spawn_blocking(move || {
            for path in reclaim {
                let _ = std::fs::remove_file(path);
            }
        })
        .await;
        Ok(())
    }

    fn append(&mut self, record: &WalRecord) -> Result<(), RdltError> {
        crash_point!(
            "wal.manifest.append",
            Err(wal_err(
                "append manifest",
                std::io::Error::other("injected crash"),
            ))
        );
        // Every line carries its blake3 trailer — see
        // [`super::record::encode_line`] for why the digest exists.
        let mut line =
            super::record::encode_line(record).map_err(|e| wal_err("encode record", e))?;
        line.push(b'\n');
        self.manifest
            .write_all(&line)
            .map_err(|e| wal_err("append manifest", e))
    }
}

/// Write one segment in the Arrow IPC **file** format.
///
/// The file format is chosen over the streaming one for a durability property,
/// not for speed: its footer is validated on open, so a segment truncated at a
/// block boundary by a power loss is REFUSED. A truncated IPC *stream*, by
/// contrast, decodes the messages it has and reports clean end-of-input — which
/// would replay a short span and silently drop the rest.
///
/// No dictionary construction, no statistics, no compression: a segment lives
/// only until its commit is acknowledged, so encoding effort spent making it
/// queryable is pure loss.
///
/// Buffered, because the writer does NOT emit only a few large writes: each
/// segment costs roughly sixteen `write_all` calls — continuation markers,
/// flatbuffer metadata, padding and the footer alongside the body buffers —
/// so several hundred per load, most of them tiny. Measured at 1.1% of wall on
/// the 1M-row relational cell: small, but free.
pub(crate) fn write_segment(path: &Path, batch: &RecordBatch) -> Result<(), RdltError> {
    // `create_new` (O_EXCL), not create+truncate (047 L4): a segment name is
    // never legitimately reused — the sequence is monotonic within a run and
    // the load id carries per-process OS entropy across runs — so an
    // existing file here is either a pre-planted symlink (which a
    // truncating open would FOLLOW, clobbering its target) or a writer
    // defect, and both must fail loudly rather than overwrite.
    let file = private_file()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|e| wal_err("create segment", e))?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new_buffered(file, batch.schema_ref())
        .map_err(|e| wal_err("open segment writer", e))?;
    writer
        .write(batch)
        .map_err(|e| wal_err("write segment", e))?;
    writer.finish().map_err(|e| wal_err("close segment", e))?;
    Ok(())
}

/// Remove the whole WAL directory (clean finish, or fresh start after
/// full recovery). An already-absent directory is a clean result; any
/// other failure surfaces (round-12 — the remove was a silent
/// best-effort, so recovery could "resolve" a span it never actually
/// cleared and proceed over the residue): RECOVERY propagates it and
/// refuses the run, while the clean-finish caller stays best-effort —
/// failing a fully committed run over cleanup would trade a real
/// success for an error, and the residue resolves as an ordinary
/// committed-manifest Discard on the next run's scan.
pub(crate) fn clear(dir: &Path) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    verify_owned_dir(dir)?;
    match std::fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            // A partial removal may have taken the ownership marker
            // while leaving residue it could not delete (a
            // write-protected subdirectory, say). Ownership was
            // verified at entry, so the surviving directory is still
            // ours — re-stamp it, or the tolerated-residue arm
            // (Discard's failed clear WARNS and the run proceeds)
            // would be refused adoption of its own WAL directory by
            // `ensure_owned_dir`'s non-empty-without-marker gate.
            if dir.exists() {
                let _ = private_file()
                    .create_new(true)
                    .write(true)
                    .open(dir.join(OWNERSHIP_MARKER))
                    .and_then(|mut f| {
                        use std::io::Write as _;
                        f.write_all(OWNERSHIP_MARKER_BYTES)
                    });
            }
            Err(e)
        }
        _ => Ok(()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_core::TableName;
    use std::sync::Arc;

    fn batch_of(rows: i64) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
        )
        .expect("batch")
    }

    fn manifest_records(dir: &Path) -> Vec<WalRecord> {
        let text = std::fs::read_to_string(dir.join("manifest.jsonl")).expect("read manifest");
        text.lines()
            .map(|line| match crate::wal::record::decode_line(line) {
                crate::wal::record::ManifestLine::Record(record) => record,
                _ => panic!("every written line must carry a verifying checksum: {line}"),
            })
            .collect()
    }

    fn segment_rows(path: &Path) -> usize {
        let file = std::fs::File::open(path).expect("open segment");
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).expect("arrow ipc");
        reader
            .map(|b| b.expect("decode batch").num_rows())
            .sum::<usize>()
    }

    /// Round-12: a fresh open expects a CLEAN directory — recovery
    /// resolves and clears any prior span first, so a manifest still
    /// present here is unresolved residue, and writing a new Run
    /// header (and fresh sidecar) over it would mask the sidecar's
    /// own drift gates. The refusal names the residue.
    #[test]
    fn open_refuses_a_directory_already_carrying_a_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        ensure_owned_dir(dir.path()).expect("adopt fixture dir");
        std::fs::write(dir.path().join("manifest.jsonl"), b"{}\n").expect("residue");
        let error = Wal::open(
            dir.path().to_path_buf(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::naming::IdentRules::default(),
            false,
        )
        .expect_err("a surviving manifest must refuse the open");
        let text = error.to_string();
        assert!(
            text.contains("manifest already exists") && text.contains("refusing to write over it"),
            "the refusal names the residue: {text}"
        );
    }

    /// Recovery's voucher (round-13): with `tolerate_resolved_residue`
    /// the open proceeds over a surviving manifest — the Discard-class
    /// shape whose clear failed — and the new Run header APPENDS after
    /// the resolved span, the manifest format's own multi-Run shape.
    #[test]
    fn open_with_the_residue_voucher_appends_after_the_resolved_span() {
        let dir = tempfile::tempdir().expect("tempdir");
        ensure_owned_dir(dir.path()).expect("adopt fixture dir");
        // A resolved current-version span whose clear failed — the only
        // shape recovery ever vouches for.
        let mut stale = crate::wal::record::encode_line(&WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: LoadId::new("old"),
            pipeline: PipelineId::new("p"),
        })
        .expect("stale line");
        stale.push(b'\n');
        std::fs::write(dir.path().join("manifest.jsonl"), stale).expect("residue");
        Wal::open(
            dir.path().to_path_buf(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::naming::IdentRules::default(),
            true,
        )
        .expect("the vouched open proceeds over resolved residue");
        let manifest =
            std::fs::read_to_string(dir.path().join("manifest.jsonl")).expect("manifest");
        let lines: Vec<&str> = manifest.lines().collect();
        assert_eq!(lines.len(), 2, "{manifest}");
        assert!(
            lines[0].contains("\"old\"") && lines[1].contains("\"l\""),
            "the new Run header appends AFTER the resolved span: {manifest}"
        );
    }

    /// 2M7: recovery's residue voucher never licenses following a symlink at
    /// the manifest path.
    #[test]
    fn residue_voucher_refuses_a_manifest_symlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_dir = dir.path().join("wal");
        create_private_dir(&wal_dir).expect("wal dir");
        ensure_owned_dir(&wal_dir).expect("adopt fixture dir");
        let target = dir.path().join("victim");
        std::fs::write(&target, b"precious").expect("victim");
        std::os::unix::fs::symlink(&target, wal_dir.join("manifest.jsonl")).expect("plant symlink");

        let error = Wal::open(
            wal_dir,
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::naming::IdentRules::default(),
            true,
        )
        .expect_err("the vouched open must still refuse a symlink");
        assert!(error.to_string().contains("opening manifest"), "{error}");
        assert_eq!(std::fs::read(target).expect("victim survives"), b"precious");
    }

    /// 047 L4: a pre-planted symlink where a segment will be written must
    /// fail LOUDLY, never be followed — a truncating open would clobber the
    /// symlink's target with segment bytes.
    #[tokio::test]
    async fn a_preplanted_symlink_at_a_segment_path_fails_instead_of_following() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("victim");
        std::fs::write(&target, b"precious").expect("victim file");
        let mut wal = Wal::open(
            dir.path().join("wal"),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::naming::IdentRules::default(),
            false,
        )
        .expect("open wal");
        // The name the next recorded batch will use, planted as a symlink.
        std::os::unix::fs::symlink(&target, dir.path().join("wal").join("l-000000.arrow"))
            .expect("plant symlink");
        let error = wal
            .record(&LoadItem::batch(TableName::new("t"), batch_of(3)))
            .await
            .expect_err("an occupied segment path must refuse");
        assert!(
            error.to_string().contains("create segment"),
            "the refusal names the create: {error}"
        );
        assert_eq!(
            std::fs::read(&target).expect("victim survives"),
            b"precious",
            "the symlink's target must never be written through"
        );
    }

    /// 047 L4's sidecar half: a pre-planted symlink at the rules sidecar
    /// path is UNLINKED (the symlink itself, never its target) and the
    /// sidecar written fresh — the target stays untouched.
    #[test]
    fn a_preplanted_symlink_at_the_sidecar_path_never_reaches_its_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("victim");
        std::fs::write(&target, b"precious").expect("victim file");
        let wal_dir = dir.path().join("wal");
        create_private_dir(&wal_dir).expect("wal dir");
        ensure_owned_dir(&wal_dir).expect("adopt fixture dir");
        std::os::unix::fs::symlink(&target, wal_dir.join(RULES_SIDECAR)).expect("plant symlink");
        Wal::open(
            wal_dir.clone(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::naming::IdentRules::default(),
            false,
        )
        .expect("open replaces the planted link");
        assert_eq!(
            std::fs::read(&target).expect("victim survives"),
            b"precious",
            "the symlink's target must never be written through"
        );
        let meta = std::fs::symlink_metadata(wal_dir.join(RULES_SIDECAR)).expect("sidecar");
        assert!(
            meta.file_type().is_file() && !meta.file_type().is_symlink(),
            "the sidecar is a fresh regular file, not the planted link"
        );
    }

    /// 2L16: naming an existing non-empty `wal` leaf is not ownership proof.
    /// Without the marker, neither opening nor later cleanup may adopt it.
    #[test]
    fn a_nonempty_foreign_wal_directory_is_not_adopted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir(&wal_dir).expect("foreign wal dir");
        let important = wal_dir.join("important");
        std::fs::write(&important, b"keep").expect("foreign data");

        let error = Wal::open(
            wal_dir.clone(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::naming::IdentRules::default(),
            false,
        )
        .expect_err("non-empty foreign directories must not be adopted");
        assert!(error.to_string().contains("ownership"), "{error}");
        assert_eq!(
            std::fs::read(important).expect("foreign data survives"),
            b"keep"
        );
        assert!(!wal_dir.join(OWNERSHIP_MARKER).exists());
    }

    /// 047 L4's manifest half: a DANGLING symlink passes the residue
    /// `exists()` check, and a plain create would follow it and mint the
    /// manifest at the link's target. `create_new` refuses it loudly.
    #[test]
    fn a_dangling_symlink_at_the_manifest_path_refuses_instead_of_following() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("minted-elsewhere");
        let wal_dir = dir.path().join("wal");
        create_private_dir(&wal_dir).expect("wal dir");
        ensure_owned_dir(&wal_dir).expect("adopt fixture dir");
        std::os::unix::fs::symlink(&target, wal_dir.join("manifest.jsonl"))
            .expect("plant dangling symlink");
        let error = Wal::open(
            wal_dir,
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::naming::IdentRules::default(),
            false,
        )
        .expect_err("an occupied manifest path must refuse");
        assert!(
            error.to_string().contains("opening manifest"),
            "the refusal names the open: {error}"
        );
        assert!(
            !target.exists(),
            "nothing may be minted at the symlink's target"
        );
    }

    /// Each recorded batch gets its OWN segment file, and the sequence advances
    /// by one so no name is ever reused. Under `segment_seq += 1` → `*=` the
    /// counter stays at zero, both batches write `l-000000.arrow`, and the
    /// second silently OVERWRITES the first — replay would then load the same
    /// rows twice and lose the others. Nothing about the counter is asserted
    /// directly: the pin is the two files and their distinct contents.
    #[tokio::test]
    async fn each_recorded_batch_gets_its_own_sequential_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = Wal::open(
            dir.path().to_path_buf(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::naming::IdentRules::default(),
            false,
        )
        .expect("open wal");

        // The rules sidecar is on disk before anything else — the
        // recovery scan refuses a manifest without it.
        let sidecar = std::fs::read_to_string(dir.path().join(RULES_SIDECAR))
            .expect("the sidecar exists beside the manifest");
        assert_eq!(
            serde_json::from_str::<rdlt_core::naming::IdentRules>(&sidecar).expect("parses"),
            rdlt_core::naming::IdentRules::default(),
            "the sidecar round-trips the writer's rules verbatim"
        );

        wal.record(&LoadItem::batch(TableName::new("t"), batch_of(3)))
            .await
            .expect("record first");
        wal.record(&LoadItem::batch(TableName::new("t"), batch_of(5)))
            .await
            .expect("record second");

        // Two distinct files, named in sequence.
        let first = dir.path().join("l-000000.arrow");
        let second = dir.path().join("l-000001.arrow");
        assert!(first.exists(), "first segment must exist: {first:?}");
        assert!(
            second.exists(),
            "the second segment must be a NEW file, not a reuse of the first"
        );

        // Each carries its own rows — proof the first was not overwritten.
        assert_eq!(segment_rows(&first), 3);
        assert_eq!(segment_rows(&second), 5);

        // And the manifest names them in write order, with matching row counts.
        let records = manifest_records(dir.path());
        let segments: Vec<(String, u64)> = records
            .iter()
            .filter_map(|r| match r {
                WalRecord::Segment { file, rows, .. } => Some((file.clone(), *rows)),
                _ => None,
            })
            .collect();
        assert_eq!(
            segments,
            vec![
                ("l-000000.arrow".to_owned(), 3),
                ("l-000001.arrow".to_owned(), 5)
            ],
            "manifest order IS replay order"
        );
        assert!(
            matches!(records.first(), Some(WalRecord::Run { .. })),
            "the Run header is always the first line"
        );
    }
}
