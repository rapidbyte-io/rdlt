//! Forward scan of the manifest: classify what is on disk into a
//! [`ScanOutcome`] without touching a segment or a session. Synchronous file
//! I/O — the caller runs it off the async runtime.

use std::{
    io::{BufRead, BufReader, Read as _},
    path::Path,
};

use rdlt_core::id::{LoadId, PipelineId};

use super::dir::{RULES_SIDECAR, open_wal_read};
use super::format::{
    MAX_MANIFEST_LINE_BYTES, ManifestLine, WAL_FORMAT_VERSION, WalRecord, decode_line,
    verify_segment_file,
};
use crate::lineage;

/// The scan's whole-file budget: the per-line cap bounds ONE line, but the
/// fold accumulates every line into memory, so a hostile multi-gigabyte
/// manifest of small legal lines is the same unbounded allocation one size
/// up. The honest arithmetic, stated as a RATE so the ceiling stays honest
/// at any policy: a manifest is cleared per successful run, so it holds ONE
/// run's span plus vouched residue — and at the stream cap a busy run writes
/// ~1024 checkpoint lines ≈ ~150 KB per checkpoint sweep, so the budget
/// divides by the sweep rate: ~2 hours at one sweep per second, ~12 minutes
/// at ten. A longer run's crash recovery then degrades to cursor
/// re-extraction — the safe direction, but a real availability cost for
/// exactly the big runs; the budget exists so a corrupted WAL cannot make
/// recovery materialize (and read) unboundedly, and 1 GiB is where "honest
/// span" and "bounded recovery work" meet.
/// What the budget buys in memory: scanning materializes records,
/// schema clones, and chain links at a measured ~3-4× constant over
/// the manifest's bytes — bounded and linear, so the budget bounds
/// peak recovery memory too.
pub(crate) const MAX_MANIFEST_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;

/// The rules sidecar is the writer's `IdentRules` verbatim — a ~100-byte
/// JSON document. It is read whole, so it gets a small cap of its own: a
/// sparse giant regular file planted at the sidecar path passes the
/// file-TYPE gate, and only this bound keeps recovery from reading it
/// unbounded.
const MAX_RULES_SIDECAR_BYTES: u64 = 8 * 1024;

/// One scanned manifest line: its content, and how many TERMINATOR bytes
/// preceded the next line on disk (`1` for `\n`, `2` for `\r\n`, `0`
/// for an unterminated final line). The budget counts both, so a
/// CRLF-hostile manifest cannot double the bytes recovery reads past
/// the bytes it accounts for.
#[derive(Debug)]
struct ReadLine {
    text: String,
    terminator: u64,
}

fn read_manifest_line(reader: &mut impl BufRead) -> Result<Option<ReadLine>, String> {
    let mut bytes = Vec::new();
    let read = reader
        .take((MAX_MANIFEST_LINE_BYTES + 2) as u64)
        .read_until(b'\n', &mut bytes)
        .map_err(|e| format!("manifest read: {e}"))?;
    if read == 0 {
        return Ok(None);
    }
    // Strip the terminator BEFORE measuring: the writer appends exactly
    // `\n` per line, and counting it against the cap would make the
    // effective bound MAX−1. A line of exactly MAX content bytes plus its
    // newline is legal and must scan.
    let mut terminator = 0u64;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        terminator += 1;
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
            terminator += 1;
        }
    }
    if bytes.len() > MAX_MANIFEST_LINE_BYTES {
        return Err(format!(
            "manifest line exceeds the {MAX_MANIFEST_LINE_BYTES}-byte metadata cap"
        ));
    }
    String::from_utf8(bytes)
        .map(|text| Some(ReadLine { text, terminator }))
        .map_err(|e| format!("manifest line is not UTF-8: {e}"))
}

/// The scan's accumulated per-table record: latest schema + write
/// mode, keyed by table — one alias for the map every scan consumer
/// reads (the fold builds it; the chain walk, `live_tables` and
/// `filter_covered` walk it).
type SchemaMap = std::collections::BTreeMap<
    rdlt_core::id::TableName,
    (rdlt_core::schema::TableSchema, rdlt_core::commit::WriteMode),
>;

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
    pub(crate) schemas: Vec<(rdlt_core::schema::TableSchema, rdlt_core::commit::WriteMode)>,
}

/// Scan outcome. `Damaged` means segments/manifest can't support replay — the caller
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
    /// A Run header names ANOTHER pipeline: this workdir is occupied.
    /// The ONE outcome recovery must never resolve by clearing — every
    /// other resolved arm clears the WAL, and clearing here would
    /// destroy the occupying pipeline's recovery material, while
    /// replaying would commit its rows and cursors under the wrong
    /// pipeline. The caller refuses the run and leaves the directory
    /// exactly as found.
    ForeignPipeline {
        occupant: PipelineId,
    },
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
/// Forward-scan the manifest under `total_budget` bytes. A torn FINAL line
/// (crash mid-append) is truncated; damage anywhere else degrades to
/// re-extraction. `rules` joins checkpoint streams to segment tables (see
/// `filter_covered`) and must be the destination's — the same rules the
/// writing run normalized its root tables with. The budget is a parameter so
/// its own pin can run against a small fixture; production passes
/// [`MAX_MANIFEST_TOTAL_BYTES`].
pub(crate) fn scan(
    dir: &Path,
    rules: rdlt_core::schema::IdentRules,
    pipeline: &PipelineId,
    total_budget: u64,
) -> ScanOutcome {
    // The read side matches the write side's directory gate:
    // `ensure_owned_dir` refuses a symlinked `wal` leaf, and following one
    // here would read a foreign target's manifest into every verdict below
    // (verdict-steering) and its segments into replay. A symlinked WAL
    // directory is damage, never followed. (Not `Nothing`: the directory
    // EXISTS, it just isn't ours to read. `Damaged` degrades to
    // re-extraction, and the caller's clear then refuses the same symlink
    // loudly.)
    if let Ok(meta) = std::fs::symlink_metadata(dir)
        && meta.file_type().is_symlink()
    {
        return ScanOutcome::Damaged(format!(
            "the WAL directory `{}` is a symlink — the write side refuses symlinked WAL \
             directories, so the read side refuses to follow one into a foreign target",
            dir.display()
        ));
    }
    let path = dir.join("manifest.jsonl");
    // The gated open ([`open_wal_read`]) refuses FIFOs, symlinks
    // and other non-regular plants at the manifest path — a plain open would
    // BLOCK forever on a writerless FIFO (nothing above this scan has a
    // timeout) or read a symlink's foreign target into the verdicts below.
    // Absence stays `Nothing`, and ENOTDIR is absence too (no `wal`
    // DIRECTORY exists — `Wal::open` refuses the occupied path loudly later,
    // after recovery has resolved, which is the pinned failure order); every
    // other failure is damage, named.
    let file = match open_wal_read(&path) {
        Ok(f) => f,
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                || e.raw_os_error() == Some(libc::ENOTDIR) =>
        {
            return ScanOutcome::Nothing;
        }
        Err(e) => return ScanOutcome::Damaged(format!("manifest is unreadable: {e}")),
    };
    let mut records: Vec<WalRecord> = Vec::new();
    let mut damaged: Option<String> = None;
    let mut total_bytes: u64 = 0;
    let mut reader = BufReader::new(file);
    loop {
        let line = match read_manifest_line(&mut reader) {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(reason) => {
                damaged = Some(reason);
                break;
            }
        };
        // The whole-file budget: the per-line cap bounds one line; this
        // bounds their SUM, which is what the fold below accumulates.
        // Content plus the real on-disk terminator: counting a flat +1
        // would let a CRLF-hostile manifest read twice the bytes it
        // accounted for.
        total_bytes += line.text.len() as u64 + line.terminator;
        if total_bytes > total_budget {
            damaged = Some(format!(
                "manifest exceeds the {total_budget}-byte total budget — the \
                 per-line cap bounds one line, but recovery accumulates every line"
            ));
            break;
        }
        if line.text.trim().is_empty() {
            continue;
        }
        match decode_line(&line.text) {
            ManifestLine::Record(record) => records.push(record),
            // Almost always corruption; the one content-dependent tear
            // shape that lands here too (see the Corrupt arm's doc)
            // misclassifies only in the safe direction — degrade, never
            // acceptance — so Damaged wherever it sits, the final line
            // included.
            ManifestLine::Corrupt(reason) => {
                damaged = Some(format!("manifest corruption: {reason}"));
                break;
            }
            ManifestLine::Untrailered(parsed) => {
                if let Some(WalRecord::Run { format_version, .. }) = &parsed
                    && *format_version != WAL_FORMAT_VERSION
                {
                    // A bare Run header claiming ANOTHER format version — the
                    // shape a pre-checksum dev-window manifest leads with.
                    // Hand exactly this one record to the fold, whose
                    // occupancy and version gates refuse every non-current
                    // header by SHAPE (`ForeignPipeline` / `Unsupported`,
                    // never acceptance), and stop reading: the rest of the
                    // file is in a format this build does not verify. A
                    // trailer-less line claiming the CURRENT version gets no
                    // such tolerance — this format's writers always write
                    // trailers, so accepting one would let a forger bypass
                    // the checksum by omission.
                    records.push(parsed.expect("matched Some above"));
                    break;
                }
                // Torn tail is fine only if nothing follows it.
                match read_manifest_line(&mut reader) {
                    Ok(Some(_)) => {
                        damaged = Some(
                            "mid-manifest corruption: a line carries no checksum trailer"
                                .to_owned(),
                        );
                    }
                    Ok(None) => {}
                    Err(reason) => damaged = Some(reason),
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
    let mut schemas = SchemaMap::new();
    // The segment names the CURRENT run's span already carries:
    // the writer mints one monotonic sequence per run, so a REPEATED
    // name is crafted — the zero-row amplification shape (millions of
    // `rows:0` lines all naming one file under the total budget)
    // repeats names because crafting millions of DISTINCT valid
    // footers costs real disk. Refusing repeats caps the amplification
    // at what distinct segments genuinely cost.
    let mut span_segment_names: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    for record in records {
        if let WalRecord::Delta { schema, mode, .. } = &record {
            schemas.insert(schema.table.clone(), (schema.clone(), mode.clone()));
        }
        match record {
            WalRecord::Run {
                format_version,
                load_id: id,
                pipeline: run_pipeline,
            } => {
                // The occupancy gate sits ABOVE the version gate: an
                // `Unsupported` outcome (like every resolved arm) is
                // CLEARED by the caller, and a foreign span must never
                // be cleared whatever its format version says. The
                // header check widens the last-Run rule to EVERY Run
                // header — an earlier foreign header can only be
                // pre-isolation residue, and it is not ours to resolve.
                if run_pipeline != *pipeline {
                    return ScanOutcome::ForeignPipeline {
                        occupant: run_pipeline,
                    };
                }
                if format_version != WAL_FORMAT_VERSION {
                    // EXACT match, in both directions. A newer manifest was
                    // written by an engine whose records this build cannot be
                    // trusted to read; an older one names segments in a
                    // container this build no longer decodes. Neither is
                    // guessable — degrade to cursor re-extraction.
                    return ScanOutcome::Unsupported {
                        found: format_version,
                        supported: WAL_FORMAT_VERSION,
                    };
                }
                // A run only ever starts after the previous span was resolved
                // (recovery runs before `Wal::open` appends the new header), so a Run
                // record always begins a fresh span.
                span.clear();
                span_segment_names.clear();
                load_id = Some(id);
                max_committed_seq = 0;
            }
            WalRecord::Committed { commit_seq } => {
                max_committed_seq = max_committed_seq.max(commit_seq);
                span.clear();
            }
            other => {
                // THE SEGMENT-NAME GATE: replay joins this name
                // onto the WAL directory, and `Path::join` hands an absolute
                // or `..`-carrying component the whole filesystem — so a
                // name the current run's writer could not have produced
                // refuses the manifest BEFORE anything is ever opened. The
                // check runs against the run's own load id from its header;
                // a segment with no header yet resolves as Discard below
                // and never joins either.
                if let WalRecord::Segment { file, .. } = &other
                    && let Some(load) = &load_id
                    && let Err(reason) = verify_segment_file(load, file)
                {
                    return ScanOutcome::Damaged(reason);
                }
                if let WalRecord::Segment { file, .. } = &other
                    && !span_segment_names.insert(file.clone())
                {
                    return ScanOutcome::Damaged(format!(
                        "the run's span names segment `{file}` more than once — the writer \
                         mints one monotonic sequence per run, so a repeat is crafted \
                         amplification, not a shape it could produce"
                    ));
                }
                span.push(other);
            }
        }
    }

    // THE RULES SIDECAR GATE sits below the record fold so a
    // different-version manifest still reports `Unsupported` by SHAPE, and
    // above everything that attributes segments to streams — the join is
    // not consulted until it is proven to run under the writer's rules.
    if let Some(reason) = sidecar_drift(dir, rules) {
        return ScanOutcome::Damaged(reason);
    }

    // CRITICAL: coverage is PER STREAM. A segment is replayable only if a
    // checkpoint of ITS OWN stream appears after it in the span — that
    // checkpoint's cursor is what makes re-extraction skip the segment's
    // rows. Anything less specific double-applies: an earlier rule truncated
    // positionally at the span's LAST checkpoint, which is equivalent only
    // while one stream exists, and an interleaved co-stream segment with no
    // checkpoint of its own was both replayed and then re-extracted (the
    // multi-table crash sweep shows it live). Uncovered segments are
    // dropped — re-extraction re-delivers them — and a span with no
    // checkpoint at all has nothing safely replayable.
    let mut chains = lineage::Chain::default();
    match (load_id, filter_covered(span, &schemas, rules, &mut chains)) {
        (Some(load_id), Ok(Some(records))) => {
            // No writer emits `u64::MAX` (the first commit is 1 and the
            // sequence only ever increments by one), so a committed sequence
            // with no successor is forgery or corruption — degrade rather
            // than overflow (a debug build would panic inside recovery; a
            // release build would wrap the recovery commit to sequence 0).
            let Some(next_commit_seq) = max_committed_seq.checked_add(1) else {
                return ScanOutcome::Damaged(format!(
                    "committed sequence {max_committed_seq} leaves no next commit \
                     sequence — no writer emits it, so the manifest was not written \
                     by one"
                ));
            };
            // REPLAY ENSURES ONLY WHAT IT WRITES: the
            // segment filter can drop every one of a table's segments
            // (uncovered co-stream rows re-extract instead), and an
            // ensure without rows is not harmless — a Replace stream's
            // once-per-load truncation fires at the replay commit, so a
            // zero-row replay would EMPTY the target and spend the
            // load's one truncation delivering nothing. Both ensure
            // feeds are pruned to the tables with surviving segments
            // (plus their recorded ancestor chains — a child batch
            // needs its parents ensured): the span's own Delta records
            // and the accumulated schema list below. Attribution is
            // unaffected — `filter_covered` already ran against the
            // full pre-filter `schemas` map. Re-extraction re-ensures
            // everything else live, delta-before-batch as always.
            let live = live_tables(&records, &schemas, &mut chains);
            let records = records
                .into_iter()
                .filter(|record| match record {
                    WalRecord::Delta { schema, .. } => live.contains(&schema.table),
                    _ => true,
                })
                .collect();
            ScanOutcome::Recover(RecoverySpan {
                load_id,
                next_commit_seq,
                records,
                schemas: schemas
                    .into_iter()
                    .filter_map(|(table, entry)| live.contains(&table).then_some(entry))
                    .collect(),
            })
        }
        (Some(_), Err(reason)) => ScanOutcome::Damaged(reason),
        // A span with no checkpoint has nothing safely replayable — but the
        // manifest and its segments are on disk, so say so rather than reporting
        // an empty workdir.
        _ => ScanOutcome::Discard,
    }
}

/// The rules-drift REFUSAL: the stream↔segment join normalizes under
/// `rules`, and it is sound only when they are THE WRITING RUN'S rules.
/// Under changed rules a checkpointed stream's normalized root can stop
/// matching its own recorded segments' root, so its COVERED segments read
/// as a co-stream's benign orphans — dropped from replay while the
/// checkpoint's cursor still commits: silent loss in the one-to-zero
/// direction the many-to-one tripwire in `filter_covered` cannot see. The
/// writer records its rules verbatim beside the manifest
/// ([`crate::wal::writer::Wal::open`], before the manifest is created, so
/// no manifest this engine writes ever exists without them); a RECORDED
/// mismatch, an unreadable or unparseable sidecar, or NO sidecar at all —
/// a manifest without its rules sidecar is not a recognized workdir state
/// (this engine is greenfield and carries no compat arm for writers that
/// never existed) — refuses the whole span. `Some(reason)` means Damaged:
/// the caller clears the WAL and re-extracts from last COMMITTED state,
/// so no cursor from the refused span ever commits.
fn sidecar_drift(dir: &Path, rules: rdlt_core::schema::IdentRules) -> Option<String> {
    let path = dir.join(RULES_SIDECAR);
    // Same gated open as the manifest's: the sidecar decides whether the
    // whole span is trusted, so a symlink here would let foreign content
    // vouch for the writer's rules, and a FIFO would hang the scan. The
    // read is BOUNDED: the writer's own sidecar is ~100 bytes, so a sparse
    // giant regular file planted here — which passes the type gate — must
    // refuse rather than be read whole.
    let text = match open_wal_read(&path).and_then(|file| {
        let mut bytes = Vec::new();
        file.take(MAX_RULES_SIDECAR_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_RULES_SIDECAR_BYTES {
            return Err(std::io::Error::other(format!(
                "exceeds the {MAX_RULES_SIDECAR_BYTES}-byte sidecar cap"
            )));
        }
        String::from_utf8(bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }) {
        Ok(text) => text,
        Err(e) => {
            return Some(format!(
                "the manifest has no readable `{}` sidecar ({e}) — a WAL without its \
                 recorded identifier-normalization rules is not a recognized workdir \
                 state, so segment attribution cannot be proven",
                RULES_SIDECAR
            ));
        }
    };
    match serde_json::from_str::<rdlt_core::schema::IdentRules>(&text) {
        // A recorded rules value must be SANE as well as matching —
        // an out-of-range `max_len` in the sidecar is not a state this
        // engine's writer produces (its rules were validated at plan
        // time), so refuse the span rather than feed it to the namer.
        Ok(recorded) if recorded.validate().is_err() => Some(format!(
            "the `{}` sidecar carries out-of-range identifier-normalization rules — no \
             validated writer produces them, so segment attribution cannot be proven",
            RULES_SIDECAR
        )),
        Ok(recorded) if recorded == rules => None,
        Ok(recorded) => Some(format!(
            "the WAL was written under identifier-normalization rules {recorded:?} but this \
             run's destination normalizes under {rules:?} — a rules change between the crash \
             and this resume could drop a checkpointed stream's own covered segments while \
             committing its cursor"
        )),
        Err(e) => Some(format!(
            "the `{}` sidecar does not parse as identifier-normalization rules ({e})",
            RULES_SIDECAR
        )),
    }
}

/// `table`'s recorded ancestor chain — the table itself first, its root
/// last — resolved through the shared memoized walker ONCE per scan and
/// read by both consumers: the covered-filter takes the root, the live-set
/// fold takes the whole chain. The scan's refusals ride the walk's error
/// channel: a table with no recorded schema breaks delta-before-first-batch,
/// and an unterminated chain is a cycle no writer produces. Attribution
/// stays on the RECORDED parent links — name prefixes would NOT do:
/// `child_table_name` re-normalizes, so a long child's name truncates to a
/// hash suffix that need not contain its root.
fn chain_of<'c>(
    chains: &'c mut lineage::Chain,
    table: &rdlt_core::id::TableName,
    schemas: &SchemaMap,
) -> Result<&'c lineage::Link, String> {
    chains
        .resolve(table, schemas.len(), |current| match schemas.get(current) {
            None => Err(format!(
                "segment table `{current}` has no schema delta anywhere in the manifest \
                 (the writer records delta-before-first-batch), so its covering stream \
                 is unknowable"
            )),
            Some((schema, _)) => Ok(schema.parent.as_ref().map(|link| link.parent.clone())),
        })?
        .ok_or_else(|| format!("table `{table}`'s recorded parent chain does not terminate"))
}

/// The chain's last hop — the root the covered-filter joins on.
fn root_of(
    chains: &mut lineage::Chain,
    table: &rdlt_core::id::TableName,
    schemas: &SchemaMap,
) -> Result<rdlt_core::id::TableName, String> {
    Ok(chain_of(chains, table, schemas)?.root().clone())
}

/// The tables replay will actually WRITE: every surviving segment's
/// table plus its recorded ancestors, read off the memo the
/// covered-filter already filled (survivors are a subset of what it
/// resolved, so these are cache reads). What this set gates: replay's
/// ensure calls — see the pruning at the scan's Recover arm for why an
/// ensure without rows is a hazard.
fn live_tables(
    records: &[WalRecord],
    schemas: &SchemaMap,
    chains: &mut lineage::Chain,
) -> std::collections::BTreeSet<rdlt_core::id::TableName> {
    let mut live = std::collections::BTreeSet::new();
    for record in records {
        if let WalRecord::Segment { table, .. } = record
            && let Ok(chain) = chain_of(chains, table, schemas)
        {
            // Stop at the first ancestor already folded in: any table in
            // `live` arrived with its whole tail (chains are
            // suffix-consistent), so the rest of this chain is already
            // there — the early stop is what keeps this fold linear over
            // a manifest of deep chains instead of re-walking each
            // segment's full ancestry.
            for table in chain.iter() {
                if !live.insert(table.clone()) {
                    break;
                }
            }
        }
    }
    live
}

/// Keep the covered part of one uncommitted span: every Delta and Checkpoint,
/// and exactly the Segments a checkpoint of their OWN stream follows. Returns
/// `Ok(None)` for a span with no checkpoint (nothing replayable), `Err` when
/// segment↔stream attribution cannot prove replay safe — the caller degrades
/// to cursor re-extraction, slower and never wrong.
///
/// The stream↔table join uses the mapping the writer itself used: a stream's
/// root table IS `normalize_ident(stream, rules)` ([`lineage::root_table`],
/// whose stream validation also proves the mapping injective across a run's
/// streams), and every child table's recorded Delta carries its parent link
/// — so a segment resolves to its root along recorded parents, and the root
/// to its stream by normalization. `rules` must be the same rules the
/// writing run normalized with — ENFORCED upstream by the rules sidecar gate
/// ([`sidecar_drift`]): a manifest whose recorded rules differ never reaches
/// this join; the residual shapes a matching-rules writer still cannot
/// produce are refused below rather than guessed at.
fn filter_covered(
    span: Vec<WalRecord>,
    schemas: &SchemaMap,
    rules: rdlt_core::schema::IdentRules,
    chains: &mut lineage::Chain,
) -> Result<Option<Vec<WalRecord>>, String> {
    use std::collections::BTreeMap;

    // A stream's last checkpoint position: every segment of that stream
    // before it is covered by its cursor.
    let mut last_checkpoint: BTreeMap<rdlt_core::id::StreamName, usize> = BTreeMap::new();
    for (index, record) in span.iter().enumerate() {
        if let WalRecord::Checkpoint { stream, .. } = record {
            last_checkpoint.insert(stream.clone(), index);
        }
    }
    if last_checkpoint.is_empty() {
        return Ok(None);
    }

    let mut root_to_stream: BTreeMap<rdlt_core::id::TableName, rdlt_core::id::StreamName> =
        BTreeMap::new();
    for stream in last_checkpoint.keys() {
        let root = lineage::root_table(stream, rules);
        if root_to_stream
            .insert(root.clone(), stream.clone())
            .is_some()
        {
            // The writer's stream validation refuses two streams on one root
            // table, so this join is ambiguous only when the manifest did not
            // come from a writer whose rules match ours.
            return Err(format!(
                "checkpointed streams normalize to one root table `{root}` — \
                 segment attribution is ambiguous"
            ));
        }
    }

    // THE RULES-DRIFT TRIPWIRE: the join trusts that this
    // run's normalization agrees with the writing run's, and the benign
    // orphan disposition above removed the last guard on the DROP
    // direction — but the wrong-KEEP direction is worse: under drifted
    // rules a checkpointed stream's normalized root can land on ANOTHER
    // trace's recorded table, covering rows its cursor never owned. The
    // cheap invariant that catches it: if MORE THAN ONE recorded root
    // table normalizes onto one checkpointed stream's root, the
    // stream↔segment join is many-to-one and nothing distinguishes the
    // stream's own trace from the stranger's — Damaged, re-extraction,
    // safe. Benign runs never trip it: the writer's own validation
    // keeps normalized roots injective, so at most the stream's own
    // table matches. Child tables never collide in (their names carry
    // the `__` separator and normalize to themselves).
    // The parentless recorded tables normalize ONCE into a count map,
    // then each checkpointed root reads its count.
    let mut normalized_roots: BTreeMap<rdlt_core::id::TableName, usize> = BTreeMap::new();
    for (table, (schema, _)) in schemas {
        if schema.parent.is_none() {
            let normalized =
                lineage::root_table(&rdlt_core::id::StreamName::new(table.as_str()), rules);
            *normalized_roots.entry(normalized).or_insert(0) += 1;
        }
    }
    for root in root_to_stream.keys() {
        let colliding = normalized_roots.get(root).copied().unwrap_or(0);
        if colliding > 1 {
            return Err(format!(
                "{colliding} recorded root tables normalize onto checkpointed root `{root}` — \
                 the stream-to-segment join is many-to-one (a normalization-rules drift \
                 shape) and cannot prove replay safe"
            ));
        }
    }

    // A segment's root table, along the parent links its Deltas
    // recorded — the same memoized walk the loader's commit gate
    // resolves roots with, resolved once per table.
    let mut keep = vec![true; span.len()];
    for (index, record) in span.iter().enumerate() {
        if let WalRecord::Segment { table, .. } = record {
            let root = root_of(chains, table, schemas)?;
            match root_to_stream.get(&root) {
                Some(stream) => {
                    // Covered iff this stream's LAST checkpoint follows the
                    // segment; on the covered side the checkpoint's cursor
                    // accounts for these rows, on the other side only
                    // re-extraction does.
                    if index >= last_checkpoint[stream] {
                        keep[index] = false;
                    }
                }
                None => {
                    keep[index] = false;
                }
            }
        }
    }

    // ORPHANS BESIDE SEGMENTLESS CHECKPOINTS ARE BENIGN: a checkpointed
    // stream whose root matches no segment AND no recorded schema simply
    // wrote zero rows for the whole run — checkpoint-only, so no delta was
    // ever recorded. Its checkpoint covers nothing and carries its cursor;
    // the orphans are a never-checkpointing co-stream's and re-extraction
    // re-delivers them; every OTHER productive stream's replay must
    // survive. The shapes that still degrade are the attributional ones
    // above: a segment whose table has no recorded schema anywhere
    // (root_of's refusal — a segment that exists but cannot be
    // attributed), and two checkpointed streams normalizing to one root.
    // A normalization-rules change between the writing run and this one
    // — which would make a stream's own segments look like a co-stream's
    // orphans — never reaches this join: the rules sidecar gate upstream
    // refuses any span whose recorded rules differ from this run's (or
    // are missing entirely).

    Ok(Some(
        span.into_iter()
            .zip(keep)
            .filter_map(|(record, keep)| keep.then_some(record))
            .collect(),
    ))
}

#[cfg(test)]
#[path = "scan_tests.rs"]
mod tests;
