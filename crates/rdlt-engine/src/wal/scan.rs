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
) -> Result<&'c [rdlt_core::id::TableName], String> {
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
    Ok(chain_of(chains, table, schemas)?
        .last()
        .expect("a chain holds at least its own table")
        .clone())
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
            live.extend(chain.iter().cloned());
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
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use rdlt_core::commit::WriteMode;
    use rdlt_core::cursor::Cursor;
    use rdlt_core::id::{LoadId, PipelineId, StreamName, TableName};
    use rdlt_core::schema::{self, IdentRules, ParentLink, TableSchema};

    use super::*;
    use crate::testing::run_header;
    use crate::wal::format::encode_line;

    /// `records` as the writer's own lines under the default rules.
    fn write_manifest(dir: &std::path::Path, records: &[WalRecord]) {
        crate::testing::write_manifest(dir, records, IdentRules::default());
    }

    /// A current-version Run header (load `l`, pipeline `p`) followed by
    /// `records`, with `writer_rules` in the sidecar.
    fn write_span(dir: &std::path::Path, records: Vec<WalRecord>, writer_rules: IdentRules) {
        let mut all = vec![run_header("l")];
        all.extend(records);
        crate::testing::write_manifest(dir, &all, writer_rules);
    }

    /// The production scan of `dir` under the default rules for pipeline `p`.
    fn scan_dir(dir: &std::path::Path) -> ScanOutcome {
        scan(
            dir,
            IdentRules::default(),
            &PipelineId::new("p"),
            MAX_MANIFEST_TOTAL_BYTES,
        )
    }

    /// Scan a manifest of `records` — writer and scanner under the SAME
    /// (default) rules, the benign shape the coverage pins hold on.
    fn scan_span(records: Vec<WalRecord>) -> ScanOutcome {
        let dir = tempfile::tempdir().expect("tempdir");
        write_span(dir.path(), records, IdentRules::default());
        scan_dir(dir.path())
    }

    fn delta(table: &str, parent: Option<&str>) -> WalRecord {
        let schema = TableSchema {
            table: TableName::new(table),
            parent: parent.map(|p| ParentLink {
                parent: TableName::new(p),
                depth: 1,
            }),
            columns: vec![],
        };
        WalRecord::Delta {
            delta: schema::Delta {
                table: schema.table.clone(),
                from: None,
                to: schema.content_hash(),
                changes: vec![],
            },
            schema,
            mode: WriteMode::Append,
        }
    }

    /// A delta whose stream writes in `mode` — the Replace-hazard pins
    /// need the mode replay would ensure the table under.
    fn delta_with_mode(table: &str, mode: WriteMode) -> WalRecord {
        let WalRecord::Delta { delta, schema, .. } = delta(table, None) else {
            unreachable!("delta() builds a Delta record");
        };
        WalRecord::Delta {
            delta,
            schema,
            mode,
        }
    }

    /// The tables the outcome's replay would ENSURE: the span's Delta
    /// records plus the RecoverySpan's accumulated schema list — both
    /// feed `apply_delta` in replay.rs, so both must agree.
    fn ensured_tables(outcome: &ScanOutcome) -> Vec<String> {
        let ScanOutcome::Recover(span) = outcome else {
            panic!("expected Recover, got {outcome:?}");
        };
        let mut tables: Vec<String> = span
            .schemas
            .iter()
            .map(|(schema, _)| schema.table.as_str().to_owned())
            .chain(span.records.iter().filter_map(|r| match r {
                WalRecord::Delta { schema, .. } => Some(schema.table.as_str().to_owned()),
                _ => None,
            }))
            .collect();
        tables.sort();
        tables.dedup();
        tables
    }

    fn segment(table: &str, file: &str) -> WalRecord {
        WalRecord::Segment {
            table: TableName::new(table),
            file: file.to_owned(),
            rows: 2,
        }
    }

    fn checkpoint(stream: &str) -> WalRecord {
        WalRecord::Checkpoint {
            stream: StreamName::new(stream),
            cursor: Cursor::new(serde_json::json!(2)),
        }
    }

    /// The segment files the outcome would replay, in span order.
    fn replayed_files(outcome: &ScanOutcome) -> Vec<String> {
        match outcome {
            ScanOutcome::Recover(span) => span
                .records
                .iter()
                .filter_map(|r| match r {
                    WalRecord::Segment { file, .. } => Some(file.clone()),
                    _ => None,
                })
                .collect(),
            other => panic!("expected Recover, got {other:?}"),
        }
    }

    /// A replayable one-stream span under a current-version header, written
    /// through the writer's own line encoding — the healthy baseline the
    /// tamper tests below corrupt.
    fn healthy_manifest(dir: &std::path::Path) {
        let schema = rdlt_core::schema::TableSchema {
            table: TableName::new("orders"),
            parent: None,
            columns: vec![],
        };
        let records = vec![
            run_header("l"),
            WalRecord::Delta {
                delta: rdlt_core::schema::Delta {
                    table: schema.table.clone(),
                    from: None,
                    to: schema.content_hash(),
                    changes: vec![],
                },
                schema,
                mode: rdlt_core::commit::WriteMode::Append,
            },
            WalRecord::Segment {
                table: TableName::new("orders"),
                file: "l-000000.arrow".to_owned(),
                rows: 2,
            },
            WalRecord::Checkpoint {
                stream: StreamName::new("orders"),
                cursor: Cursor::new(serde_json::json!(41)),
            },
        ];
        write_manifest(dir, &records);
    }

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
        let mut records = vec![run_header("starve")];
        for seq in 0..20_000u64 {
            records.push(WalRecord::Checkpoint {
                stream: StreamName::from("s"),
                cursor: Cursor::new(format!("c{seq}")),
            });
        }
        write_manifest(dir, &records);
    }

    /// `mkfifo` via the coreutils binary: the workspace denies `unsafe`, so
    /// `libc::mkfifo` is not callable, and no safe wrapper is in the tree.
    /// Returns false when the binary is unavailable so the test can skip
    /// rather than fail on an exotic host.
    fn mkfifo(path: &std::path::Path) -> bool {
        match std::process::Command::new("mkfifo").arg(path).status() {
            Ok(status) => status.success(),
            Err(_) => false,
        }
    }

    /// Run `scan` on its own thread under a deadline. The RED state of these
    /// pins is an eternal hang (a writerless FIFO blocks the open), and a
    /// test that hangs forever fails no gate — the deadline turns a
    /// regression into a loud failure. The scanning thread stays blocked
    /// after a timeout; the per-test process boundary reclaims it.
    fn scan_with_deadline(dir: std::path::PathBuf) -> ScanOutcome {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(scan_dir(&dir));
        });
        match rx.recv_timeout(std::time::Duration::from_secs(20)) {
            Ok(outcome) => outcome,
            Err(_) => {
                panic!("recovery hung: the scan blocked on a hostile file instead of refusing it")
            }
        }
    }

    /// The cursor half of the same face: a Checkpoint line carrying a
    /// maximal-contract cursor (4 MiB, `rdlt_connector::gate::MAX_CURSOR_BYTES`)
    /// plus its envelope and trailer must fit the cap — the cursor contract
    /// is only honest if the WAL can actually record one.
    #[test]
    fn the_line_cap_admits_a_maximal_cursor_line() {
        let cursor = rdlt_core::cursor::Cursor::new(serde_json::Value::String(
            "x".repeat(rdlt_connector::gate::MAX_CURSOR_BYTES as usize),
        ));
        let line = encode_line(&WalRecord::Checkpoint {
            stream: rdlt_core::id::StreamName::new("s"),
            cursor,
        })
        .expect("encode");
        assert!(
            line.len() <= MAX_MANIFEST_LINE_BYTES,
            "a maximal cursor line ({} bytes) must fit the {MAX_MANIFEST_LINE_BYTES}-byte cap",
            line.len()
        );
    }

    /// The line cap's other face: it must sit ABOVE anything this engine's
    /// own writer can append, or a run's own WAL becomes unscannable. This
    /// builds the largest Delta the shred-time bounds admit — 4,096 columns
    /// (`MAX_SOURCE_COLUMNS_PER_TABLE`), identifiers at the default rules'
    /// 63-byte bound, the schema serialized twice by a CreateTable change —
    /// and holds the cap over it. Growing the shred bounds or shrinking the
    /// cap fails HERE, before it fails as a `Damaged` scan in the field.
    #[test]
    fn the_line_cap_admits_the_writers_own_maximal_delta_line() {
        use rdlt_core::commit::WriteMode;
        use rdlt_core::schema::{self, Column, ColumnType, Provenance};
        use rdlt_core::types::LogicalType;
        let columns: Vec<Column> = (0..crate::shred::limits::MAX_SOURCE_COLUMNS_PER_TABLE)
            .map(|i| Column {
                name: format!("{:a>59}{i:04}", ""),
                column_type: ColumnType::scalar(LogicalType::Json),
                nullable: true,
                provenance: Provenance::Inferred,
            })
            .collect();
        let schema = rdlt_core::schema::TableSchema {
            table: rdlt_core::id::TableName::new(format!("{:t>63}", "")),
            parent: None,
            columns,
        };
        let delta = rdlt_core::schema::Delta {
            table: schema.table.clone(),
            from: None,
            to: schema.content_hash(),
            changes: vec![schema::Change::CreateTable {
                schema: schema.clone(),
            }],
        };
        let record = WalRecord::Delta {
            delta,
            schema,
            mode: WriteMode::Append,
        };
        let line = encode_line(&record).expect("encode");
        assert!(
            line.len() <= MAX_MANIFEST_LINE_BYTES,
            "the writer's own maximal delta line ({} bytes) must scan under the \
             {MAX_MANIFEST_LINE_BYTES}-byte cap — otherwise a legitimately huge \
             table makes its own run's recovery degrade",
            line.len()
        );
    }

    #[test]
    fn an_oversized_manifest_line_degrades_without_reading_it_unbounded() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("manifest.jsonl"),
            vec![b'x'; MAX_MANIFEST_LINE_BYTES + 1],
        )
        .expect("fixture manifest");
        let outcome = scan_dir(dir.path());
        assert!(matches!(outcome, ScanOutcome::Damaged(reason) if reason.contains("metadata cap")));
    }

    /// The line cap's boundary, both sides: the writer appends exactly `\n`
    /// per line, so a completed line of EXACTLY the cap in content bytes
    /// must scan (the terminator is not content), and one content byte over
    /// must refuse — counting the newline against the cap would make the
    /// real bound MAX−1.
    #[test]
    fn the_line_cap_counts_content_not_the_terminator() {
        let mut at_cap = vec![b'x'; MAX_MANIFEST_LINE_BYTES];
        at_cap.push(b'\n');
        let mut reader = std::io::BufReader::new(std::io::Cursor::new(at_cap));
        let line = read_manifest_line(&mut reader)
            .expect("a line at exactly the cap scans")
            .expect("a line, not EOF");
        assert_eq!(line.text.len(), MAX_MANIFEST_LINE_BYTES);
        assert_eq!(line.terminator, 1, "the writer's `\\n` terminator");
        // A CRLF terminator reports TWO on-disk bytes, so the
        // whole-file budget counts what recovery actually reads.
        let crlf = b"line\r\n".to_vec();
        let mut reader = std::io::BufReader::new(std::io::Cursor::new(crlf));
        let line = read_manifest_line(&mut reader)
            .expect("a CRLF line scans")
            .expect("a line, not EOF");
        assert_eq!(line.text, "line");
        assert_eq!(line.terminator, 2, "CRLF counts both bytes");

        let mut over_cap = vec![b'x'; MAX_MANIFEST_LINE_BYTES + 1];
        over_cap.push(b'\n');
        let mut reader = std::io::BufReader::new(std::io::Cursor::new(over_cap));
        let error = read_manifest_line(&mut reader).expect_err("one content byte over refuses");
        assert!(error.contains("metadata cap"), "{error}");
    }

    /// The whole-file half: the per-line cap bounds ONE line; this pins
    /// the SUM. Enough individually-legal lines to pass the budget must
    /// degrade, not accumulate. Driven through the budget SEAM with a
    /// small budget so the pin costs kilobytes rather than the production
    /// gibibyte — whose value is asserted alongside (the seam and the
    /// constant together are the whole defense).
    #[test]
    fn a_manifest_past_the_total_budget_degrades() {
        assert_eq!(
            MAX_MANIFEST_TOTAL_BYTES,
            1024 * 1024 * 1024,
            "the production whole-file budget (see its doc for the honest arithmetic)"
        );
        let dir = tempfile::tempdir().expect("tempdir");
        let cursor = rdlt_core::cursor::Cursor::new(serde_json::Value::String("x".repeat(1000)));
        let line = encode_line(&WalRecord::Checkpoint {
            stream: rdlt_core::id::StreamName::new("s"),
            cursor,
        })
        .expect("encode");
        let mut out = encode_line(&WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: LoadId::new("l"),
            pipeline: PipelineId::new("p"),
        })
        .expect("encode header");
        out.push(b'\n');
        // 1 KiB lines against a 16 KiB seam budget.
        let mut total = out.len() as u64;
        while total <= 16 * 1024 {
            out.extend_from_slice(&line);
            out.push(b'\n');
            total += line.len() as u64 + 1;
        }
        std::fs::write(dir.path().join("manifest.jsonl"), out).expect("write manifest");
        std::fs::write(
            dir.path().join(RULES_SIDECAR),
            serde_json::to_vec(&rdlt_core::schema::IdentRules::default()).expect("rules json"),
        )
        .expect("write sidecar");
        let outcome = scan(
            dir.path(),
            IdentRules::default(),
            &PipelineId::new("p"),
            16 * 1024,
        );
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("total budget")),
            "the total budget refuses, naming itself: {outcome:?}"
        );
        // And under the seam budget a small manifest still scans — the
        // budget, not the content, is what refused above.
        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(dir.path(), &[]);
        let outcome = scan(
            dir.path(),
            IdentRules::default(),
            &PipelineId::new("p"),
            16 * 1024,
        );
        assert!(
            !matches!(outcome, ScanOutcome::Damaged(_)),
            "a small manifest under the seam budget scans: {outcome:?}"
        );
    }

    /// A span naming one segment file TWICE is damage — the writer
    /// mints one monotonic sequence per run, so a repeat can only be a
    /// crafted manifest (the zero-row amplification shape: millions of
    /// `rows:0` lines all pointing at one cheap segment, minutes of
    /// recovery from kilobytes of disk). Distinct names still scan.
    #[test]
    fn a_span_naming_one_segment_twice_is_damage() {
        let load = LoadId::new("l");
        let header = WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: load.clone(),
            pipeline: PipelineId::new("p"),
        };
        let segment = |file: &str| WalRecord::Segment {
            table: rdlt_core::id::TableName::new("t"),
            file: file.to_owned(),
            rows: 0,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(
            dir.path(),
            &[
                header.clone(),
                segment("l-000000.arrow"),
                segment("l-000000.arrow"),
            ],
        );
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("more than once")),
            "a repeated segment name degrades the scan: {outcome:?}"
        );

        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(
            dir.path(),
            &[header, segment("l-000000.arrow"), segment("l-000001.arrow")],
        );
        let outcome = scan_dir(dir.path());
        assert!(
            !matches!(outcome, ScanOutcome::Damaged(_)),
            "distinct segment names still scan: {outcome:?}"
        );
    }

    /// The sidecar half: the writer's own sidecar is ~100 bytes, so a
    /// sparse giant regular file at the sidecar path — which passes the
    /// file-TYPE gate — refuses at the small content cap rather than being
    /// read whole.
    #[test]
    fn an_oversized_sidecar_is_damage_not_an_unbounded_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(dir.path(), &[]);
        std::fs::write(
            dir.path().join(RULES_SIDECAR),
            vec![b'x'; (MAX_RULES_SIDECAR_BYTES + 1) as usize],
        )
        .expect("plant oversized sidecar");
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("sidecar")),
            "an oversized sidecar degrades the scan: {outcome:?}"
        );
    }

    /// A recorded rules value that parses but is out
    /// of range is damage, not a mismatch — no validated writer produces
    /// it, so the span is refused rather than fed to the namer.
    #[test]
    fn a_sidecar_with_out_of_range_rules_is_damage() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(dir.path(), &[]);
        std::fs::write(
            dir.path().join(RULES_SIDECAR),
            serde_json::to_vec(&rdlt_core::schema::IdentRules { max_len: 2 }).expect("rules json"),
        )
        .expect("plant insane sidecar");
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("out-of-range")),
            "an out-of-range sidecar degrades the scan: {outcome:?}"
        );
    }

    /// A symlinked `wal` directory itself is refused, not followed —
    /// the write side's `ensure_owned_dir` gate's read-side twin.
    #[test]
    fn a_symlinked_wal_directory_is_damage_never_followed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let foreign = tempfile::tempdir().expect("foreign dir");
        write_manifest(foreign.path(), &[]);
        let link = dir.path().join("wal");
        std::os::unix::fs::symlink(foreign.path(), &link).expect("plant symlink");
        let outcome = scan_dir(&link);
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("symlink")),
            "a symlinked WAL directory refuses, never follows: {outcome:?}"
        );
    }

    /// A DIRECTORY planted at the manifest path opens fine on Unix
    /// (directories are readable), so only the handle-side regular-file
    /// check stands between the scan and decoding directory bytes. It
    /// refuses as damage.
    #[test]
    fn a_directory_planted_at_the_manifest_path_is_damage() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("manifest.jsonl")).expect("plant directory");
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(_)),
            "a directory at the manifest path is damage, not absence: {outcome:?}"
        );
    }

    /// Mutation-report closure: the on-disk Run header must SERIALIZE the
    /// current format version — a defaulted or zero version would break forward
    /// detection.
    #[test]
    fn run_header_serializes_current_format_version() {
        let record = WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: LoadId::new("l"),
            pipeline: PipelineId::new("p"),
        };
        let json = serde_json::to_string(&record).expect("serialize");
        assert!(
            json.contains(&format!("\"format_version\":{}", WAL_FORMAT_VERSION)),
            "header must carry the version: {json}"
        );
        assert_eq!(
            WAL_FORMAT_VERSION, 1,
            "the format version is 1 until the public release — in the unpublished \
             window format changes land IN PLACE (a skewed or unreadable WAL \
             degrades to re-extraction); version ceremony starts at the release"
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
            scan_dir(dir.path())
        };
        let current = WAL_FORMAT_VERSION;
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

    /// A manifest whose Run header carries NO version field is not a
    /// recognized manifest at all (every writer stamps the field), so a
    /// multi-line one lands on the corruption arm and degrades — never
    /// decodes under a defaulted version, never replays.
    #[test]
    fn a_versionless_manifest_is_unrecognized_and_degrades() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("manifest.jsonl"),
            concat!(
                "{\"rec\":\"run\",\"load_id\":\"l\",\"pipeline\":\"p\"}\n",
                "{\"rec\":\"committed\",\"commit_seq\":1}\n",
            ),
        )
        .expect("write manifest");
        assert!(
            matches!(scan_dir(dir.path()), ScanOutcome::Damaged(_)),
            "an unversioned header is unrecognized — corruption arm, re-extraction"
        );
    }

    /// A workdir whose Run header names ANOTHER pipeline refuses as
    /// `ForeignPipeline`, carrying the occupant — never `Recover` (which
    /// would replay the foreign span under the wrong pipeline) and never
    /// `Damaged`/`Discard`/`Unsupported` (every one of which the caller
    /// resolves by CLEARING the foreign pipeline's recovery material).
    #[test]
    fn a_manifest_written_by_another_pipeline_refuses_as_foreign() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(
            dir.path(),
            &[WalRecord::Run {
                format_version: WAL_FORMAT_VERSION,
                load_id: LoadId::new("l"),
                pipeline: PipelineId::new("orders"),
            }],
        );
        let outcome = scan(
            dir.path(),
            IdentRules::default(),
            &PipelineId::new("customers"),
            MAX_MANIFEST_TOTAL_BYTES,
        );
        assert!(
            matches!(outcome, ScanOutcome::ForeignPipeline { ref occupant }
                if occupant.as_str() == "orders"),
            "a foreign manifest must refuse naming the occupant: {outcome:?}"
        );
    }

    /// The occupancy gate outranks the version gate: a foreign manifest
    /// in a DIFFERENT format version must still refuse as foreign —
    /// `Unsupported` is a resolved outcome the caller clears, and the
    /// foreign span is not ours to clear.
    #[test]
    fn a_foreign_manifest_in_another_version_still_refuses_as_foreign() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(
            dir.path(),
            &[WalRecord::Run {
                format_version: WAL_FORMAT_VERSION + 1,
                load_id: LoadId::new("l"),
                pipeline: PipelineId::new("orders"),
            }],
        );
        let outcome = scan(
            dir.path(),
            IdentRules::default(),
            &PipelineId::new("customers"),
            MAX_MANIFEST_TOTAL_BYTES,
        );
        assert!(
            matches!(outcome, ScanOutcome::ForeignPipeline { .. }),
            "foreign-ness outranks version skew: {outcome:?}"
        );
    }

    /// The per-stream rule, deterministically: `events`' segment precedes
    /// `orders`' checkpoint POSITIONALLY, but no `events` checkpoint exists —
    /// so no cursor covers it and the run's own extraction will re-deliver
    /// its rows. Replaying it too is the double-apply. It must be dropped;
    /// `orders`' covered segment and checkpoint replay as before.
    #[test]
    fn an_uncovered_co_stream_segment_is_dropped_not_replayed() {
        let outcome = scan_span(vec![
            delta("events", None),
            delta("orders", None),
            segment("events", "l-000000.arrow"),
            segment("orders", "l-000001.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(
            replayed_files(&outcome),
            ["l-000001.arrow"],
            "a segment with no checkpoint of its OWN stream after it is not covered \
             by any cursor — replaying it double-applies once the source re-extracts"
        );
        let ScanOutcome::Recover(span) = &outcome else {
            unreachable!()
        };
        assert!(
            span.records.iter().any(
                |r| matches!(r, WalRecord::Checkpoint { stream, .. } if stream.as_str() == "orders")
            ),
            "the covering checkpoint itself must survive the filter"
        );
    }

    /// A manifest carrying TWO Run headers — the
    /// shape a hand-crafted (or pre-isolation-residue) directory can
    /// present even though the writer's invariant is one Run per
    /// resolved span. The fold must consider only the LAST run's span:
    /// the second `Run` resets the load id, the span, and the committed
    /// seq, exactly as the recovery clearing discipline assumes.
    #[test]
    fn a_two_run_manifest_replays_only_the_last_runs_span() {
        let dir = tempfile::tempdir().expect("tempdir");
        let run = |load: &str| WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: LoadId::new(load),
            pipeline: PipelineId::new("p"),
        };
        let records = vec![
            run("first"),
            delta("events", None),
            segment("events", "first-000000.arrow"),
            checkpoint("events"),
            WalRecord::Committed { commit_seq: 1 },
            // The second run: a different load id, its own span.
            run("second"),
            delta("events", None),
            segment("events", "second-000000.arrow"),
            checkpoint("events"),
        ];
        write_span(dir.path(), records, IdentRules::default());

        let outcome = scan_dir(dir.path());
        let ScanOutcome::Recover(span) = outcome else {
            unreachable!("a two-run manifest with a covered tail span recovers")
        };
        assert_eq!(
            span.load_id,
            LoadId::new("second"),
            "the LAST Run header owns the replay identity"
        );
        assert_eq!(
            span.records
                .iter()
                .filter_map(|r| match r {
                    WalRecord::Segment { file, .. } => Some(file.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            ["second-000000.arrow".to_owned()],
            "the first run's committed span is never replayed"
        );
        assert_eq!(span.next_commit_seq, 1, "the second run's seq resets");
    }

    /// The old positional rule got THIS single-stream shape right, and it must
    /// stay right: a segment after its own stream's last checkpoint is
    /// uncovered and dropped.
    #[test]
    fn a_segment_after_its_own_streams_last_checkpoint_stays_dropped() {
        let outcome = scan_span(vec![
            delta("events", None),
            segment("events", "l-000000.arrow"),
            checkpoint("events"),
            segment("events", "l-000001.arrow"),
        ]);
        assert_eq!(replayed_files(&outcome), ["l-000000.arrow"]);
    }

    /// Interleaved streams each replay exactly their covered prefix — no
    /// positional truncation in either direction.
    #[test]
    fn interleaved_streams_each_replay_exactly_their_covered_prefix() {
        let outcome = scan_span(vec![
            delta("events", None),
            delta("orders", None),
            segment("events", "l-000000.arrow"),
            segment("orders", "l-000001.arrow"),
            checkpoint("events"),
            segment("events", "l-000002.arrow"),
            checkpoint("orders"),
        ]);
        // f0 precedes events' checkpoint, f1 precedes orders' — both covered.
        // f2 follows events' LAST checkpoint: uncovered, even though orders'
        // later checkpoint follows it positionally.
        assert_eq!(
            replayed_files(&outcome),
            ["l-000000.arrow", "l-000001.arrow"]
        );
    }

    /// Attribution follows the RECORDED parent chain, not name prefixes: a
    /// child table's name can be truncated and hash-suffixed by
    /// `child_table_name`, so nothing about it need contain its root. The
    /// Delta's `parent` link is the join the format actually carries.
    #[test]
    fn attribution_follows_the_recorded_parent_link_not_name_prefixes() {
        let outcome = scan_span(vec![
            delta("orders", None),
            delta("itm_4f2a9c1b", Some("orders")),
            segment("itm_4f2a9c1b", "l-000000.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(
            replayed_files(&outcome),
            ["l-000000.arrow"],
            "a child segment is covered by its ROOT stream's checkpoint via the parent link"
        );
    }

    /// A checkpoint-only span still recovers: committing it advances cursors
    /// over a range that produced no rows, which is exactly what the source
    /// reported.
    #[test]
    fn a_checkpoint_only_span_still_recovers_the_cursor() {
        let outcome = scan_span(vec![delta("orders", None), checkpoint("orders")]);
        assert!(replayed_files(&outcome).is_empty());
    }

    /// A segment whose table has NO schema delta anywhere in the manifest
    /// breaks the writer's delta-before-first-batch invariant — attribution
    /// cannot say which stream covers it, so the span degrades to
    /// re-extraction rather than guessing.
    #[test]
    fn a_segment_with_no_recorded_schema_degrades_to_re_extraction() {
        let outcome = scan_span(vec![
            delta("orders", None),
            segment("orders", "l-000000.arrow"),
            segment("ghost", "l-000001.arrow"),
            checkpoint("orders"),
        ]);
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("ghost")),
            "unattributable segment must degrade, naming the table: {outcome:?}"
        );
    }

    /// THE ROUTINE POST-COMMIT SHAPE the loader's deferral makes: after a
    /// commit, an idle cursored stream checkpoints with
    /// zero new segments while a snapshot co-stream (which never
    /// checkpoints) keeps writing. The snapshot segments are orphans —
    /// dropped, re-extraction re-delivers them — and the idle checkpoint
    /// is benign; the join is PROVEN by the manifest itself: the
    /// checkpointed stream's root table was recorded by the writer (its
    /// earlier committed delta), so this run's normalization of the
    /// stream agrees with the writer's world and the span RECOVERS
    /// instead of degrading every such crash to full re-extraction.
    #[test]
    fn an_idle_checkpoint_beside_snapshot_orphans_recovers_when_its_root_is_recorded() {
        let outcome = scan_span(vec![
            delta("orders", None),
            segment("orders", "l-000000.arrow"),
            checkpoint("orders"),
            WalRecord::Committed { commit_seq: 1 },
            delta("events", None),
            segment("events", "l-000001.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(
            replayed_files(&outcome),
            Vec::<String>::new(),
            "the snapshot orphan drops (its rows re-extract); nothing else is staged"
        );
        let ScanOutcome::Recover(span) = &outcome else {
            unreachable!()
        };
        assert!(
            span.records.iter().any(
                |r| matches!(r, WalRecord::Checkpoint { stream, .. } if stream.as_str() == "orders")
            ),
            "the idle stream's checkpoint replays — a cursor advance over a rowless \
             range is exactly what the source reported"
        );
        assert_eq!(span.next_commit_seq, 2, "after the committed prefix");
    }

    /// THE REPLACE HAZARD: a table whose every span
    /// segment was dropped as uncovered must NOT be ensured by replay.
    /// Ensuring is not free — a Replace stream's once-per-load
    /// truncation fires at the replay commit, and a replay
    /// contributing ZERO rows for that table would empty the target
    /// and spend the load's one truncation on nothing (if the resumed
    /// source read then fails, the table stays empty). Re-extraction
    /// re-ensures live, delta-before-batch. The covered co-stream's
    /// checkpoint still replays.
    #[test]
    fn a_table_with_no_surviving_segments_is_not_ensured_by_replay() {
        let outcome = scan_span(vec![
            delta("orders", None),
            segment("orders", "l-000000.arrow"),
            checkpoint("orders"),
            WalRecord::Committed { commit_seq: 1 },
            delta_with_mode("events", WriteMode::Replace),
            segment("events", "l-000001.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(
            replayed_files(&outcome),
            Vec::<String>::new(),
            "the uncovered events segment drops — re-extraction re-delivers it"
        );
        assert_eq!(
            ensured_tables(&outcome),
            Vec::<String>::new(),
            "no table has surviving rows, so replay must ensure NOTHING — an ensured \
             Replace table would be truncated by a zero-row replay commit"
        );
        let ScanOutcome::Recover(span) = &outcome else {
            unreachable!()
        };
        assert!(
            span.records.iter().any(
                |r| matches!(r, WalRecord::Checkpoint { stream, .. } if stream.as_str() == "orders")
            ),
            "the covered checkpoint still replays"
        );
    }

    /// The positive control: a table WITH a surviving covered segment
    /// keeps both its span Delta and its schema entry — replay still
    /// ensures what it will actually write — while the uncovered
    /// co-stream's table is pruned from both.
    #[test]
    fn only_tables_with_surviving_segments_are_ensured() {
        let outcome = scan_span(vec![
            delta("events", None),
            delta("orders", None),
            segment("events", "l-000000.arrow"),
            segment("orders", "l-000001.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(replayed_files(&outcome), ["l-000001.arrow"]);
        assert_eq!(
            ensured_tables(&outcome),
            ["orders"],
            "the covered writer keeps its ensure; the dropped co-stream loses its"
        );
    }

    /// A surviving CHILD segment keeps its whole recorded ancestor
    /// chain ensured — a child batch cannot land in a session that
    /// never ensured its parent.
    #[test]
    fn a_surviving_child_segment_keeps_its_ancestor_chain_ensured() {
        let outcome = scan_span(vec![
            delta("orders", None),
            delta("itm_4f2a9c1b", Some("orders")),
            segment("itm_4f2a9c1b", "l-000000.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(replayed_files(&outcome), ["l-000000.arrow"]);
        assert_eq!(
            ensured_tables(&outcome),
            ["itm_4f2a9c1b", "orders"],
            "the child and its recorded root both stay ensured"
        );
    }

    /// The whole-run-idle stream is BENIGN, not damage: a stream that wrote
    /// zero rows all run is checkpoint-only, so no delta was ever
    /// recorded, and its root matches no segment and no schema. Its
    /// checkpoint covers nothing and replays its cursor; the snapshot
    /// co-stream's orphans drop for re-extraction.
    #[test]
    fn an_orphan_segment_beside_an_idle_streams_checkpoint_recovers() {
        let outcome = scan_span(vec![
            delta("events", None),
            segment("events", "l-000000.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(
            replayed_files(&outcome),
            Vec::<String>::new(),
            "the snapshot orphan drops — re-extraction re-delivers it"
        );
        let ScanOutcome::Recover(span) = &outcome else {
            unreachable!()
        };
        assert!(
            span.records.iter().any(
                |r| matches!(r, WalRecord::Checkpoint { stream, .. } if stream.as_str() == "orders")
            ),
            "the idle stream's checkpoint replays its cursor"
        );
    }

    /// THE ROUTINE THREE-STREAM SHAPE: A productive and covered, B idle
    /// all run (checkpoint-only, no delta anywhere), C a snapshot stream
    /// with orphaned segments. A's span must SURVIVE B's idleness.
    #[test]
    fn a_productive_streams_span_survives_an_idle_co_stream_and_snapshot_orphans() {
        let outcome = scan_span(vec![
            delta("a_events", None),
            segment("a_events", "l-000000.arrow"),
            delta("c_snap", None),
            segment("c_snap", "l-000001.arrow"),
            checkpoint("a_events"),
            checkpoint("b_idle"),
        ]);
        assert_eq!(
            replayed_files(&outcome),
            ["l-000000.arrow"],
            "A's covered segment replays; C's orphan drops; B's idleness is not damage"
        );
        assert_eq!(
            ensured_tables(&outcome),
            ["a_events"],
            "only the productive stream's table is ensured"
        );
    }

    /// THE RULES-DRIFT TRIPWIRE: recorded tables
    /// `EVENTS` and `events` both exist (a shape only a writer with
    /// DIFFERENT normalization rules produces — ours lowercases), and
    /// the checkpointed stream's normalized root lands on `events`.
    /// Keeping under that join would let EVENTS' checkpoint cover the
    /// stranger trace's rows — Damaged instead, naming the collision.
    #[test]
    fn two_recorded_roots_collapsing_onto_one_checkpointed_root_degrade() {
        let outcome = scan_span(vec![
            delta("EVENTS", None),
            delta("events", None),
            segment("EVENTS", "l-000000.arrow"),
            segment("events", "l-000001.arrow"),
            checkpoint("EVENTS"),
        ]);
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason)
                if reason.contains("many-to-one")),
            "a many-to-one normalized join must degrade: {outcome:?}"
        );
    }

    /// THE RULES-CHANGE REFUSAL (the loss it closes would run SILENT): a
    /// crashed span whose writer recorded
    /// different ident rules must refuse as Damaged NAMING the rules
    /// change, before any attribution. Without the gate, this exact
    /// shape (`events` covered by its own checkpoint, rules changed
    /// between crash and resume) could drop the stream's own covered
    /// segments as a co-stream's orphans while still committing its
    /// cursor — rows the cursor claims as delivered, never replayed
    /// and never re-extracted.
    #[test]
    fn a_crashed_span_resumed_under_changed_rules_degrades_naming_the_rules_change() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_span(
            dir.path(),
            vec![
                delta("events", None),
                segment("events", "l-000000.arrow"),
                checkpoint("events"),
            ],
            IdentRules::default(),
        );
        let outcome = scan(
            dir.path(),
            IdentRules { max_len: 30 },
            &PipelineId::new("p"),
            MAX_MANIFEST_TOTAL_BYTES,
        );
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason)
                if reason.contains("identifier-normalization rules")
                    && reason.contains("rules change")),
            "a rules change between crash and resume must refuse, naming itself: {outcome:?}"
        );
    }

    /// A manifest with NO rules sidecar refuses the same way (this engine
    /// is greenfield, no writer without the sidecar ever existed, so
    /// absence is not a compat case but an unrecognized workdir state):
    /// every manifest this engine
    /// writes gets its sidecar before the manifest is created, and
    /// attribution cannot be proven without the recorded rules.
    #[test]
    fn a_manifest_without_the_rules_sidecar_degrades() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_span(
            dir.path(),
            vec![
                delta("events", None),
                segment("events", "l-000000.arrow"),
                checkpoint("events"),
            ],
            IdentRules::default(),
        );
        std::fs::remove_file(dir.path().join(RULES_SIDECAR)).expect("drop sidecar");
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason)
                if reason.contains("no readable `rules.json` sidecar")
                    && reason.contains("not a recognized workdir state")),
            "a sidecar-less manifest must refuse, naming the missing file and the \
             unrecognized state: {outcome:?}"
        );
    }

    /// A sidecar that EXISTS but does not parse — something wrote it
    /// and it cannot be trusted, so the span refuses as Damaged like
    /// a recorded mismatch.
    #[test]
    fn an_unparseable_rules_sidecar_degrades() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_span(
            dir.path(),
            vec![
                delta("events", None),
                segment("events", "l-000000.arrow"),
                checkpoint("events"),
            ],
            IdentRules::default(),
        );
        std::fs::write(dir.path().join(RULES_SIDECAR), b"not json at all")
            .expect("corrupt sidecar");
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason)
                if reason.contains("does not parse")),
            "a present-but-unparseable sidecar must refuse: {outcome:?}"
        );
    }

    /// Two checkpointed streams normalizing to ONE root table cannot come
    /// from the writer (`validate_streams` refuses the config), so seeing it
    /// means the join cannot be trusted — degrade rather than attribute.
    #[test]
    fn two_streams_normalizing_to_one_root_degrade() {
        let outcome = scan_span(vec![
            delta("orders", None),
            segment("orders", "l-000000.arrow"),
            checkpoint("Orders"),
            checkpoint("orders"),
        ]);
        assert!(
            matches!(outcome, ScanOutcome::Damaged(_)),
            "an ambiguous stream→root join must not guess: {outcome:?}"
        );
    }

    /// Corruption that yields DIFFERENT VALID JSON.
    /// A flipped digit in a Checkpoint's cursor would commit a resume
    /// position the source never issued — the next extraction silently
    /// skips rows, permanently. The per-line checksum makes it loud.
    #[test]
    fn a_flipped_cursor_digit_degrades_instead_of_committing_a_forged_position() {
        let dir = tempfile::tempdir().expect("tempdir");
        healthy_manifest(dir.path());
        let path = dir.path().join("manifest.jsonl");
        let text = std::fs::read_to_string(&path).expect("read manifest");
        // Flip the cursor value 41 -> 47: still valid JSON, wrong content.
        let tampered = text.replace("41", "47");
        assert_ne!(tampered, text, "the tamper must hit the cursor digit");
        std::fs::write(&path, tampered).expect("write tampered manifest");
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("checksum")),
            "valid-JSON corruption must degrade naming the checksum, never \
             commit a cursor the source never issued: {outcome:?}"
        );
    }

    /// The baseline: the untampered manifest recovers — the checksum gate
    /// must not refuse the writer's own lines.
    #[test]
    fn the_writers_own_lines_scan_clean() {
        let dir = tempfile::tempdir().expect("tempdir");
        healthy_manifest(dir.path());
        assert!(
            matches!(scan_dir(dir.path()), ScanOutcome::Recover(_)),
            "the writer's own encoding must verify"
        );
    }

    /// A checksum-mismatched line is Damaged even though a torn FINAL
    /// line still tolerates: a tear only shortens, so outside the
    /// embedded-`|`+hex content shape (whose misclassification is
    /// safe-direction, pinned in record.rs) a complete trailer means
    /// corruption wherever it sits.
    #[test]
    fn a_torn_final_line_still_tolerates_while_a_mismatched_one_degrades() {
        // Torn tail: cut the last line mid-bytes — truncated, span survives.
        let dir = tempfile::tempdir().expect("tempdir");
        healthy_manifest(dir.path());
        let path = dir.path().join("manifest.jsonl");
        let text = std::fs::read_to_string(&path).expect("read manifest");
        let torn: String = text[..text.len() - 20].to_owned();
        std::fs::write(&path, torn).expect("write torn manifest");
        assert!(
            matches!(scan_dir(dir.path()), ScanOutcome::Discard),
            "a torn FINAL line truncates (the checkpoint is dropped, and \
             with it the span's only cover — Discard, not Damaged)"
        );

        // Mismatch on the final line: full trailer, wrong digest — Damaged.
        let dir = tempfile::tempdir().expect("tempdir");
        healthy_manifest(dir.path());
        let path = dir.path().join("manifest.jsonl");
        let text = std::fs::read_to_string(&path).expect("read manifest");
        let flipped = text.replace("41", "47");
        std::fs::write(&path, flipped).expect("write mismatched manifest");
        assert!(
            matches!(scan_dir(dir.path()), ScanOutcome::Damaged(_)),
            "a complete trailer that does not match its bytes is corruption \
             even on the final line"
        );
    }

    /// THE DEGRADE PIN for pre-checksum dev-window residue, synthesized
    /// INLINE (owner rule: no committed legacy fixtures, no legacy
    /// helpers): a manifest of bare JSON lines whose header claims a
    /// different version number refuses as `Unsupported` BY SHAPE — never
    /// `Damaged`, never `Recover` — and the caller clears it and
    /// re-extracts from cursors. Safe because the WAL is a replayable
    /// buffer, never the source of truth.
    #[test]
    fn a_bare_manifest_claiming_another_version_refuses_as_unsupported() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Bare lines, no trailers — the pre-checksum dev-window shape.
        std::fs::write(
            dir.path().join("manifest.jsonl"),
            concat!(
                "{\"rec\":\"run\",\"format_version\":2,\"load_id\":\"l\",\"pipeline\":\"p\"}\n",
                "{\"rec\":\"segment\",\"table\":\"orders\",\"file\":\"l-000000.arrow\",\"rows\":2}\n",
                "{\"rec\":\"checkpoint\",\"stream\":\"orders\",\"cursor\":41}\n",
            ),
        )
        .expect("write bare manifest");
        std::fs::write(
            dir.path().join(RULES_SIDECAR),
            serde_json::to_vec(&IdentRules::default()).expect("rules json"),
        )
        .expect("write sidecar");
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(
                outcome,
                ScanOutcome::Unsupported {
                    found: 2,
                    supported: 1
                }
            ),
            "a version-skewed bare manifest degrades to cursor re-extraction by \
             VERSION, distinguishable by shape from corruption: {outcome:?}"
        );
    }

    /// A trailer-less line claiming the CURRENT version gets no such
    /// tolerance: this format's writers always write trailers, so a stripped
    /// current-version Run header is a forgery or corruption — a torn
    /// FINAL header truncates, a mid-manifest one is Damaged. Accepting
    /// it would let an attacker bypass the checksum by omission.
    #[test]
    fn an_untrailered_current_version_header_is_never_accepted() {
        let dir = tempfile::tempdir().expect("tempdir");
        healthy_manifest(dir.path());
        let path = dir.path().join("manifest.jsonl");
        let text = std::fs::read_to_string(&path).expect("read manifest");
        let mut lines: Vec<&str> = text.lines().collect();
        // Strip the Run header's trailer, keep the rest verbatim.
        let header = lines[0];
        let stripped = &header[..header.rfind('|').expect("trailer separator")];
        lines[0] = stripped;
        std::fs::write(&path, lines.join("\n") + "\n").expect("write stripped manifest");
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(_)),
            "an unverified current-version line mid-manifest must refuse: {outcome:?}"
        );
    }

    /// A manifest naming a path-shaped segment refuses as Damaged —
    /// the name never reaches `dir.join`, so nothing outside the WAL
    /// directory is ever opened.
    #[test]
    fn a_segment_name_with_path_punctuation_degrades_before_any_open() {
        for evil in [
            "../../etc/hostname",
            "/etc/hostname",
            "..\\..\\x.arrow",
            "l-..0000.arrow",
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            healthy_manifest(dir.path());
            let path = dir.path().join("manifest.jsonl");
            let text = std::fs::read_to_string(&path).expect("read manifest");
            // Rebuild the segment line around the evil name, re-checksummed —
            // the forgery under test is the NAME, not the trailer.
            let rebuilt: Vec<Vec<u8>> = text
                .lines()
                .map(|line| {
                    if line.contains("l-000000.arrow") {
                        encode_line(&WalRecord::Segment {
                            table: TableName::new("orders"),
                            file: evil.to_owned(),
                            rows: 2,
                        })
                        .expect("encode line")
                    } else {
                        line.as_bytes().to_vec()
                    }
                })
                .collect();
            let mut out = Vec::new();
            for line in rebuilt {
                out.extend_from_slice(&line);
                out.push(b'\n');
            }
            std::fs::write(&path, out).expect("write forged manifest");
            let outcome = scan_dir(dir.path());
            assert!(
                matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("segment")),
                "segment name {evil:?} must refuse as Damaged: {outcome:?}"
            );
        }
    }

    /// The shape half: even without path punctuation, a name the writer
    /// could not have produced (wrong load prefix, short sequence) refuses.
    /// The writer names segments `{load_id}-{seq:06}.arrow` (writer.rs
    /// `record`), so anything else did not come from a writer.
    #[test]
    fn a_segment_name_off_the_writers_shape_degrades() {
        for forged in ["other-000000.arrow", "l-0000.arrow", "l-00000a.arrow"] {
            let dir = tempfile::tempdir().expect("tempdir");
            healthy_manifest(dir.path());
            let path = dir.path().join("manifest.jsonl");
            let text = std::fs::read_to_string(&path).expect("read manifest");
            let rebuilt: Vec<Vec<u8>> = text
                .lines()
                .map(|line| {
                    if line.contains("l-000000.arrow") {
                        encode_line(&WalRecord::Segment {
                            table: TableName::new("orders"),
                            file: forged.to_owned(),
                            rows: 2,
                        })
                        .expect("encode line")
                    } else {
                        line.as_bytes().to_vec()
                    }
                })
                .collect();
            let mut out = Vec::new();
            for line in rebuilt {
                out.extend_from_slice(&line);
                out.push(b'\n');
            }
            std::fs::write(&path, out).expect("write forged manifest");
            let outcome = scan_dir(dir.path());
            assert!(
                matches!(outcome, ScanOutcome::Damaged(_)),
                "segment name {forged:?} is not the writer's shape and must refuse: {outcome:?}"
            );
        }
    }

    /// A forged `Committed {{ u64::MAX }}` leaves no next commit
    /// sequence. No writer emits it (the first commit is 1), so the scan
    /// degrades instead of overflowing — a debug build would panic here, a
    /// release build wrap the recovery commit to sequence 0.
    #[test]
    fn a_forged_max_committed_seq_degrades_instead_of_overflowing() {
        let dir = tempfile::tempdir().expect("tempdir");
        healthy_manifest(dir.path());
        let path = dir.path().join("manifest.jsonl");
        let mut text = std::fs::read_to_string(&path).expect("read manifest");
        // Splice a forged Committed line (checksummed — the forgery under
        // test is the VALUE) between the header and the replay span.
        let forged = encode_line(&WalRecord::Committed {
            commit_seq: u64::MAX,
        })
        .expect("encode line");
        let header_end = text.find('\n').expect("header line") + 1;
        let mut spliced = text[..header_end].to_owned();
        spliced.push_str(&String::from_utf8(forged).expect("utf8 line"));
        spliced.push('\n');
        spliced.push_str(&text.split_off(header_end));
        std::fs::write(&path, spliced).expect("write forged manifest");
        let outcome = scan_dir(dir.path());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("sequence")),
            "a committed sequence with no successor must degrade, never wrap: {outcome:?}"
        );
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
                crate::runtime::recover::off_runtime(move || scan_dir(&path)).await
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

    #[test]
    fn a_fifo_planted_as_the_manifest_degrades_promptly_instead_of_hanging() {
        let dir = tempfile::tempdir().expect("tempdir");
        if !mkfifo(&dir.path().join("manifest.jsonl")) {
            eprintln!("skipping: no mkfifo binary on this host");
            return;
        }
        let outcome = scan_with_deadline(dir.path().to_path_buf());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason)
                if reason.contains("not a regular file")),
            "a FIFO at the manifest path must refuse as damage: {outcome:?}"
        );
    }

    #[test]
    fn a_fifo_planted_as_the_rules_sidecar_degrades_promptly_instead_of_hanging() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(dir.path(), &[run_header("l")]);
        std::fs::remove_file(dir.path().join(RULES_SIDECAR)).expect("drop sidecar");
        if !mkfifo(&dir.path().join(RULES_SIDECAR)) {
            eprintln!("skipping: no mkfifo binary on this host");
            return;
        }
        let outcome = scan_with_deadline(dir.path().to_path_buf());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason)
                if reason.contains("not a regular file")),
            "a FIFO at the sidecar path must refuse as damage: {outcome:?}"
        );
    }

    /// The symlink steer, demonstrated end to end: the planted link points
    /// at a fully valid manifest naming ANOTHER pipeline. Followed, the
    /// scan's verdict becomes `ForeignPipeline` — an outcome the caller
    /// must never resolve by clearing, so one symlink wedges the pipeline
    /// on foreign content. The link itself must refuse as damage instead.
    #[test]
    fn a_symlink_planted_as_the_manifest_refuses_instead_of_being_followed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let foreign = dir.path().join("foreign");
        std::fs::create_dir(&foreign).expect("foreign dir");
        let record = WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: LoadId::new("l"),
            pipeline: PipelineId::new("someone-elses-pipeline"),
        };
        let mut out = encode_line(&record).expect("record json");
        out.push(b'\n');
        std::fs::write(foreign.join("manifest.jsonl"), out).expect("foreign manifest");
        std::os::unix::fs::symlink(
            foreign.join("manifest.jsonl"),
            dir.path().join("manifest.jsonl"),
        )
        .expect("plant symlink");
        let outcome = scan_with_deadline(dir.path().to_path_buf());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("symlink")),
            "a manifest symlink must refuse as damage, never read through: {outcome:?}"
        );
    }

    /// The sidecar half of the same steer: a link pointing at a rules file
    /// OUTSIDE the WAL passes the drift gate on foreign content — the gate
    /// exists to prove the WRITER's rules, and a link proves nothing.
    #[test]
    fn a_symlink_planted_as_the_sidecar_refuses_instead_of_being_read_through() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_manifest(dir.path(), &[run_header("l")]);
        std::fs::remove_file(dir.path().join(RULES_SIDECAR)).expect("drop sidecar");
        let outside = dir.path().join("outside-rules.json");
        std::fs::write(
            &outside,
            serde_json::to_vec(&rdlt_core::schema::IdentRules::default()).expect("rules json"),
        )
        .expect("outside rules");
        std::os::unix::fs::symlink(&outside, dir.path().join(RULES_SIDECAR))
            .expect("plant symlink");
        let outcome = scan_with_deadline(dir.path().to_path_buf());
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("symlink")),
            "a sidecar symlink must refuse as damage, never read through: {outcome:?}"
        );
    }
}
