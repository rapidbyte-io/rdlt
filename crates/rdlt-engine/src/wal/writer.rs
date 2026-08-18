//! The live run's WAL: the append-only manifest, one segment per recorded
//! batch, and the three-step commit protocol (fsync the span, destination
//! commit, mark committed and reclaim).

use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt as _,
    path::PathBuf,
};

use rdlt_core::crash_point;
use rdlt_core::error::Error;
use rdlt_core::id::{LoadId, PipelineId};

use super::dir::{
    RULES_SIDECAR, create_private_dir, ensure_owned_dir, open_wal_read, private_file,
};
use super::format::{
    MAX_MANIFEST_LINE_BYTES, ManifestLine, WAL_FORMAT_VERSION, WalRecord, decode_line, encode_line,
    segment_file_name,
};
use super::segment::write_segment;
use crate::load::LoadItem;

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

pub(super) fn wal_err(context: &str, e: impl std::fmt::Display) -> Error {
    Error::wal(format!("{context}: {e}"))
}

/// A vouched manifest's last line may lack its terminator: a complete
/// record whose newline never landed, or a line torn mid-write (which is
/// how a resolved span ends up unreplayable). Terminate the former and
/// drop the latter before anything appends, because a Run header written
/// straight after either glues into ONE line the next scan reads as
/// corruption. Read through the already-open, symlink-refusing handle;
/// the residue's size passed the scan's total budget to earn the voucher.
fn reconcile_unterminated_tail(manifest: &mut File) -> Result<(), Error> {
    use std::io::Read as _;
    let mut bytes = Vec::new();
    manifest
        .read_to_end(&mut bytes)
        .map_err(|e| wal_err("reading vouched manifest", e))?;
    if bytes.is_empty() || bytes.ends_with(b"\n") {
        return Ok(());
    }
    let tail_start = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |at| at + 1);
    let complete = std::str::from_utf8(&bytes[tail_start..])
        .is_ok_and(|tail| matches!(decode_line(tail), ManifestLine::Record(_)));
    if complete {
        manifest
            .write_all(b"\n")
            .map_err(|e| wal_err("terminating vouched manifest tail", e))
    } else {
        manifest
            .set_len(tail_start as u64)
            .map_err(|e| wal_err("dropping torn vouched manifest tail", e))
    }
}

impl Wal {
    /// Open (creating if needed) the WAL for a new run and append its
    /// `Run` header. `tolerate_resolved_residue` is recovery's voucher:
    /// true ONLY when the scan resolved the surviving manifest as holding
    /// nothing replayable and its clear failed — the new run's records then
    /// append after the resolved span, the manifest format's own multi-Run
    /// shape.
    pub(crate) fn open(
        dir: PathBuf,
        pipeline: &PipelineId,
        load_id: &LoadId,
        rules: rdlt_core::schema::IdentRules,
        tolerate_resolved_residue: bool,
    ) -> Result<Self, Error> {
        create_private_dir(&dir).map_err(|e| wal_err("creating wal dir", e))?;
        ensure_owned_dir(&dir).map_err(|e| wal_err("proving wal directory ownership", e))?;
        // A fresh open expects a CLEAN directory: recovery resolves and
        // clears any prior span before this runs, so a
        // surviving manifest here is unresolved residue — writing a
        // new Run header (and a fresh sidecar) over it would mask the
        // very drift gates the sidecar exists for. Refuse, naming the
        // residue — unless recovery vouched (see above).
        let manifest_path = dir.join("manifest.jsonl");
        if manifest_path.exists() && !tolerate_resolved_residue {
            return Err(Error::wal(format!(
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
        // Unlink-then-create_new, NOT create+truncate: a truncating
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
            manifest_options.create(true).read(true);
        } else {
            manifest_options.create_new(true);
        }
        // The vouched path opens an existing manifest, so `create_new` cannot
        // provide its usual symlink protection there. O_NOFOLLOW closes both
        // paths atomically at the final component.
        manifest_options.custom_flags(libc::O_NOFOLLOW);
        let mut manifest = manifest_options
            .append(true)
            .open(dir.join("manifest.jsonl"))
            .map_err(|e| wal_err("opening manifest", e))?;
        if tolerate_resolved_residue {
            reconcile_unterminated_tail(&mut manifest)?;
        }
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
    pub(crate) async fn record(&mut self, item: &LoadItem) -> Result<(), Error> {
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
                // (`format::verify_segment_file`) so writer and checker
                // cannot drift.
                let file = segment_file_name(&self.load_id, self.segment_seq);
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
    ///
    /// What this method buys is durability across POWER LOSS: without the
    /// fsyncs the data is still in the page cache, so every read — including a
    /// full crash-recovery replay after `kill -9` — returns exactly the same
    /// bytes. No in-process test can observe the difference (the crash sweep
    /// covers process death, a strictly weaker fault), so no pin claims to:
    /// verifying it needs a fault-injecting filesystem or hardware.
    pub(crate) async fn sync_for_commit(&mut self) -> Result<(), Error> {
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
                // Re-opened through the read-side gate: the writer created
                // this segment as a regular file, but the name could have
                // been swapped for a FIFO since, and a plain open would
                // then block the commit forever.
                open_wal_read(&path)
                    .and_then(|f| f.sync_all())
                    .map_err(|e| wal_err("fsync segment", e))?;
            }
            // Persist directory entries for the sidecar, manifest and all
            // newly-created segments. File fsync alone does not make those
            // names survive power loss. `O_DIRECTORY` refuses anything that
            // is not a directory during the open itself — a FIFO swapped in
            // at this path fails with ENOTDIR instead of blocking.
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                    .open(&dir)
                    .and_then(|f| f.sync_all())
                    .map_err(|e| wal_err("fsync wal directory", e))?;
            }
            Ok::<(), Error>(())
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
    pub(crate) async fn mark_committed(&mut self, commit_seq: u64) -> Result<(), Error> {
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

    fn append(&mut self, record: &WalRecord) -> Result<(), Error> {
        crash_point!(
            "wal.manifest.append",
            Err(wal_err(
                "append manifest",
                std::io::Error::other("injected crash"),
            ))
        );
        // Every line carries its blake3 trailer — see
        // [`super::format::encode_line`] for why the digest exists.
        let line = encode_line(record).map_err(|e| wal_err("encode record", e))?;
        // The reader's line cap is an invariant the WRITER enforces too:
        // a record larger than the cap — reachable with a
        // wire-legal oversized cursor, or a destination declaring a huge
        // `IdentRules.max_len` — would be written here and then refused by
        // this engine's own recovery scan on every later run, degrading an
        // honest WAL to re-extraction forever. Refuse at write time, where
        // the error names the cause, instead of corrupting the WAL.
        if line.len() > MAX_MANIFEST_LINE_BYTES {
            return Err(Error::wal(format!(
                "a {}-byte manifest record exceeds the {}-byte line cap recovery enforces \
                 — refusing to write a WAL line this engine could never scan back (the \
                 record carries an oversized cursor or schema)",
                line.len(),
                MAX_MANIFEST_LINE_BYTES
            )));
        }
        let mut line = line;
        line.push(b'\n');
        self.manifest
            .write_all(&line)
            .map_err(|e| wal_err("append manifest", e))
    }
}
#[cfg(test)]
mod tests {
    use std::path::Path;

    use rdlt_core::id::TableName;

    use super::*;
    use crate::testing::int_batch as batch_of;
    use crate::wal::dir::OWNERSHIP_MARKER;

    fn manifest_records(dir: &Path) -> Vec<WalRecord> {
        let text = std::fs::read_to_string(dir.join("manifest.jsonl")).expect("read manifest");
        text.lines()
            .map(|line| match decode_line(line) {
                ManifestLine::Record(record) => record,
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

    /// A fresh open expects a CLEAN directory — recovery resolves and
    /// clears any prior span first, so a manifest still present here is
    /// unresolved residue, and writing a new Run header (and fresh sidecar)
    /// over it would mask the sidecar's own drift gates. The refusal names
    /// the residue.
    #[test]
    fn open_refuses_a_directory_already_carrying_a_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        ensure_owned_dir(dir.path()).expect("adopt fixture dir");
        std::fs::write(dir.path().join("manifest.jsonl"), b"{}\n").expect("residue");
        let error = Wal::open(
            dir.path().to_path_buf(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::schema::IdentRules::default(),
            false,
        )
        .expect_err("a surviving manifest must refuse the open");
        let text = error.to_string();
        assert!(
            text.contains("manifest already exists") && text.contains("refusing to write over it"),
            "the refusal names the residue: {text}"
        );
    }

    /// Recovery's voucher: with `tolerate_resolved_residue`
    /// the open proceeds over a surviving manifest — the Discard-class
    /// shape whose clear failed — and the new Run header APPENDS after
    /// the resolved span, the manifest format's own multi-Run shape.
    #[test]
    fn open_with_the_residue_voucher_appends_after_the_resolved_span() {
        let dir = tempfile::tempdir().expect("tempdir");
        ensure_owned_dir(dir.path()).expect("adopt fixture dir");
        // A resolved current-version span whose clear failed — the only
        // shape recovery ever vouches for.
        let mut stale = encode_line(&WalRecord::Run {
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
            rdlt_core::schema::IdentRules::default(),
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

    /// The residue voucher over a TORN tail: a Discard-class manifest
    /// whose final line tore mid-write (no terminator) and whose clear
    /// failed. The vouched Run header must not glue onto the torn
    /// bytes — the next scan must read a fresh span (here: nothing to
    /// replay, or the new span's records), never Damaged.
    #[test]
    fn open_with_the_residue_voucher_never_glues_onto_a_torn_tail() {
        use crate::wal::scan::{MAX_MANIFEST_TOTAL_BYTES, ScanOutcome, scan};
        let dir = tempfile::tempdir().expect("tempdir");
        ensure_owned_dir(dir.path()).expect("adopt fixture dir");
        let rules = rdlt_core::schema::IdentRules::default();
        std::fs::write(
            dir.path().join(RULES_SIDECAR),
            serde_json::to_vec(&rules).expect("rules"),
        )
        .expect("sidecar");
        // A resolved current-version span, then a line torn twenty
        // bytes short of its trailer: the Discard shape with a torn tail.
        let mut stale = encode_line(&WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: LoadId::new("old"),
            pipeline: PipelineId::new("p"),
        })
        .expect("stale line");
        stale.push(b'\n');
        let torn = encode_line(&WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: LoadId::new("older"),
            pipeline: PipelineId::new("p"),
        })
        .expect("torn line");
        stale.extend_from_slice(&torn[..torn.len() - 20]);
        std::fs::write(dir.path().join("manifest.jsonl"), stale).expect("residue");
        assert!(
            matches!(
                scan(
                    dir.path(),
                    rules,
                    &PipelineId::new("p"),
                    MAX_MANIFEST_TOTAL_BYTES
                ),
                ScanOutcome::Discard
            ),
            "the fixture is the Discard shape recovery vouches for"
        );

        // The vouched run opens (appending its Run header) and dies
        // before its first checkpoint.
        let wal = Wal::open(
            dir.path().to_path_buf(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rules,
            true,
        )
        .expect("the vouched open proceeds over resolved residue");
        drop(wal);

        // The next scan reads the new span, not corruption.
        let outcome = scan(
            dir.path(),
            rules,
            &PipelineId::new("p"),
            MAX_MANIFEST_TOTAL_BYTES,
        );
        assert!(
            !matches!(outcome, ScanOutcome::Damaged(_)),
            "a vouched append after a torn tail must not read back as damage: {outcome:?}"
        );
        // The torn bytes are gone and the new header starts its own
        // line: two verifying lines, the stale span's and the new one's.
        let manifest =
            std::fs::read_to_string(dir.path().join("manifest.jsonl")).expect("manifest");
        let lines: Vec<&str> = manifest.lines().collect();
        assert_eq!(lines.len(), 2, "{manifest:?}");
        assert!(
            lines[0].contains("\"old\"") && lines[1].contains("\"l\""),
            "the stale span's header, then the vouched run's: {manifest:?}"
        );
    }

    /// The other unterminated shape: a COMPLETE final record whose
    /// newline never landed. It is a record the scan keeps, so the
    /// vouched open terminates it rather than dropping it, and the new
    /// header follows on its own line.
    #[test]
    fn open_with_the_residue_voucher_terminates_a_complete_unterminated_tail() {
        use crate::wal::scan::{MAX_MANIFEST_TOTAL_BYTES, ScanOutcome, scan};
        let dir = tempfile::tempdir().expect("tempdir");
        ensure_owned_dir(dir.path()).expect("adopt fixture dir");
        let rules = rdlt_core::schema::IdentRules::default();
        std::fs::write(
            dir.path().join(RULES_SIDECAR),
            serde_json::to_vec(&rules).expect("rules"),
        )
        .expect("sidecar");
        let stale = encode_line(&WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: LoadId::new("old"),
            pipeline: PipelineId::new("p"),
        })
        .expect("stale line");
        std::fs::write(dir.path().join("manifest.jsonl"), stale).expect("residue");
        Wal::open(
            dir.path().to_path_buf(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rules,
            true,
        )
        .expect("the vouched open proceeds");
        let outcome = scan(
            dir.path(),
            rules,
            &PipelineId::new("p"),
            MAX_MANIFEST_TOTAL_BYTES,
        );
        assert!(!matches!(outcome, ScanOutcome::Damaged(_)), "{outcome:?}");
        let manifest =
            std::fs::read_to_string(dir.path().join("manifest.jsonl")).expect("manifest");
        let lines: Vec<&str> = manifest.lines().collect();
        assert_eq!(lines.len(), 2, "{manifest:?}");
        assert!(
            lines[0].contains("\"old\"") && lines[1].contains("\"l\""),
            "{manifest:?}"
        );
        assert!(manifest.ends_with('\n'));
    }

    /// Recovery's residue voucher never licenses following a symlink at
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
            rdlt_core::schema::IdentRules::default(),
            true,
        )
        .expect_err("the vouched open must still refuse a symlink");
        assert!(error.to_string().contains("opening manifest"), "{error}");
        assert_eq!(std::fs::read(target).expect("victim survives"), b"precious");
    }

    /// A pre-planted symlink where a segment will be written must
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
            rdlt_core::schema::IdentRules::default(),
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

    /// The sidecar half: a pre-planted symlink at the rules sidecar
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
            rdlt_core::schema::IdentRules::default(),
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

    /// Naming an existing non-empty `wal` leaf is not ownership proof.
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
            rdlt_core::schema::IdentRules::default(),
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

    /// The manifest half: a DANGLING symlink passes the residue
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
            rdlt_core::schema::IdentRules::default(),
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

    /// The reader's line cap is an invariant the writer enforces. A
    /// record whose line exceeds the recovery scan's cap — here, a
    /// checkpoint carrying an oversized cursor — is refused AT WRITE TIME
    /// instead of producing a WAL this engine's own recovery can never
    /// scan back.
    #[tokio::test]
    async fn an_over_cap_record_is_refused_at_write_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = Wal::open(
            dir.path().to_path_buf(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
            rdlt_core::schema::IdentRules::default(),
            false,
        )
        .expect("open wal");
        let oversized = rdlt_core::cursor::Cursor::new(serde_json::Value::String(
            "x".repeat(MAX_MANIFEST_LINE_BYTES),
        ));
        let error = wal
            .append(&WalRecord::Checkpoint {
                stream: rdlt_core::id::StreamName::new("s"),
                cursor: oversized,
            })
            .expect_err("an over-cap line must refuse at write time");
        assert!(
            error.to_string().contains("line cap"),
            "the refusal names the invariant: {error}"
        );
        // And the manifest holds only the Run header — the refused record
        // never reached the disk.
        let records = manifest_records(dir.path());
        assert_eq!(records.len(), 1, "only the Run header was written");
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
            rdlt_core::schema::IdentRules::default(),
            false,
        )
        .expect("open wal");

        // The rules sidecar is on disk before anything else — the
        // recovery scan refuses a manifest without it.
        let sidecar = std::fs::read_to_string(dir.path().join(RULES_SIDECAR))
            .expect("the sidecar exists beside the manifest");
        assert_eq!(
            serde_json::from_str::<rdlt_core::schema::IdentRules>(&sidecar).expect("parses"),
            rdlt_core::schema::IdentRules::default(),
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
