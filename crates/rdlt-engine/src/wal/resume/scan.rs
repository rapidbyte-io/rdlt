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
/// `rules` joins checkpoint streams to segment tables (see `filter_covered`)
/// and must be the destination's — the same rules the writing run normalized
/// its root tables with.
/// Async wrapper: the scan reads the manifest line by line, which is blocking
/// file I/O and belongs off an embedder's runtime for the same reason replay's
/// decoding does.
pub(crate) async fn scan_off_runtime(
    dir: &Path,
    rules: rdlt_core::naming::IdentRules,
) -> ScanOutcome {
    let dir = dir.to_path_buf();
    off_runtime(move || scan(&dir, rules)).await
}

fn scan(dir: &Path, rules: rdlt_core::naming::IdentRules) -> ScanOutcome {
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

    // THE RULES SIDECAR GATE (round-9 fix) sits below the record fold so a
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
    // checkpoint of its own was both replayed and then re-extracted (042
    // T7E, proven live on the multi-table crash sweep). Uncovered segments
    // are dropped — re-extraction re-delivers them — and a span with no
    // checkpoint at all has nothing safely replayable.
    match (load_id, filter_covered(span, &schemas, rules)) {
        (Some(load_id), Ok(Some(records))) => {
            // REPLAY ENSURES ONLY WHAT IT WRITES (round-3 fix): the
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
            let live = live_tables(&records, &schemas);
            let records = records
                .into_iter()
                .filter(|record| match record {
                    WalRecord::Delta { schema, .. } => live.contains(&schema.table),
                    _ => true,
                })
                .collect();
            ScanOutcome::Recover(RecoverySpan {
                load_id,
                next_commit_seq: max_committed_seq + 1,
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

/// The rules-drift REFUSAL (round-9 fix — this closed the residual the
/// round-6 comment in `filter_covered` recorded): the stream↔segment
/// join normalizes under `rules`, and it is sound only when they are
/// THE WRITING RUN'S rules. Under changed rules a checkpointed stream's
/// normalized root can stop matching its own recorded segments' root,
/// so its COVERED segments read as a co-stream's benign orphans —
/// dropped from replay while the checkpoint's cursor still commits:
/// silent loss in the one-to-zero direction the round-7 many-to-one
/// tripwire cannot see. The writer records its rules verbatim beside
/// the manifest ([`crate::wal::Wal::open`], before the manifest is
/// created, so no 042+ manifest exists without them); a RECORDED
/// mismatch, or a sidecar that cannot parse, refuses the whole span —
/// `Some(reason)` means Damaged: the caller clears the WAL and
/// re-extracts from last COMMITTED state, so no cursor from the
/// refused span ever commits.
///
/// ABSENCE warns and proceeds instead (round-10 fix — round 9 refused
/// it on the premise that nothing pre-042 exists, which is wrong for
/// LOCAL MAIN: pre-042 writers never wrote a sidecar, and discarding
/// their healthy WAL re-opens exactly the N2 duplication the WAL
/// mandate exists to prevent — a durable-identity destination's
/// partially-published snapshots key on the CRASHED load's id, so a
/// fresh-id re-extraction appends its rows a second time). The rules
/// are then UNVERIFIABLE, and the warning says so: the scan proceeds
/// under this run's rules, which is precisely the pre-sidecar
/// behavior, no worse and no better.
fn sidecar_drift(dir: &Path, rules: rdlt_core::naming::IdentRules) -> Option<String> {
    let path = dir.join(crate::wal::RULES_SIDECAR);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!(
                "the WAL manifest has no `{}` sidecar — a pre-042 writer left it, so the \
                 writing run's identifier-normalization rules are unverifiable; proceeding \
                 under this run's rules (the pre-sidecar behavior)",
                crate::wal::RULES_SIDECAR
            );
            return None;
        }
        Err(e) => {
            return Some(format!(
                "the `{}` sidecar exists but cannot be read ({e}) — the writing run's \
                 identifier-normalization rules are unknown, so segment attribution \
                 cannot be proven",
                crate::wal::RULES_SIDECAR
            ));
        }
    };
    match serde_json::from_str::<rdlt_core::naming::IdentRules>(&text) {
        Ok(recorded) if recorded == rules => None,
        Ok(recorded) => Some(format!(
            "the WAL was written under identifier-normalization rules {recorded:?} but this \
             run's destination normalizes under {rules:?} — a rules change between the crash \
             and this resume could drop a checkpointed stream's own covered segments while \
             committing its cursor"
        )),
        Err(e) => Some(format!(
            "the `{}` sidecar does not parse as identifier-normalization rules ({e})",
            crate::wal::RULES_SIDECAR
        )),
    }
}

/// The tables replay will actually WRITE: every surviving segment's
/// table plus its recorded ancestors (the bounded walk `filter_covered`
/// already proved terminates for every surviving segment). What this
/// set gates: replay's ensure calls — see the pruning at the scan's
/// Recover arm for why an ensure without rows is a hazard.
fn live_tables(
    records: &[WalRecord],
    schemas: &std::collections::BTreeMap<
        rdlt_core::TableName,
        (rdlt_core::TableSchema, rdlt_core::WriteMode),
    >,
) -> std::collections::BTreeSet<rdlt_core::TableName> {
    let mut live = std::collections::BTreeSet::new();
    for record in records {
        if let WalRecord::Segment { table, .. } = record {
            // The shared bounded walk, collecting every hop: the walk's
            // own root answer is not needed here — membership of the
            // whole chain is.
            let _ = crate::coverage::walk_to_root(table, schemas.len(), |current| {
                if !live.insert(current.clone()) {
                    return Ok::<_, std::convert::Infallible>(None); // chain already walked
                }
                Ok(schemas
                    .get(current)
                    .and_then(|(s, _)| s.parent.as_ref())
                    .map(|link| link.parent.clone()))
            });
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
/// root table IS `normalize_ident(stream, rules)` (`runtime::validate`'s
/// `root_table`, whose stream validation also proves the mapping injective
/// across a run's streams), and every child table's recorded Delta carries its
/// parent link — so a segment resolves to its root along recorded parents, and
/// the root to its stream by normalization. `rules` must be the same rules the
/// writing run normalized with — ENFORCED upstream by the rules sidecar gate
/// ([`sidecar_drift`], round-9): a manifest whose recorded rules differ never
/// reaches this join; the residual shapes a matching-rules writer still cannot
/// produce are refused below rather than guessed at.
fn filter_covered(
    span: Vec<WalRecord>,
    schemas: &std::collections::BTreeMap<
        rdlt_core::TableName,
        (rdlt_core::TableSchema, rdlt_core::WriteMode),
    >,
    rules: rdlt_core::naming::IdentRules,
) -> Result<Option<Vec<WalRecord>>, String> {
    use std::collections::BTreeMap;

    // A stream's last checkpoint position: every segment of that stream
    // before it is covered by its cursor.
    let mut last_checkpoint: BTreeMap<rdlt_core::StreamName, usize> = BTreeMap::new();
    for (index, record) in span.iter().enumerate() {
        if let WalRecord::Checkpoint { stream, .. } = record {
            last_checkpoint.insert(stream.clone(), index);
        }
    }
    if last_checkpoint.is_empty() {
        return Ok(None);
    }

    let mut root_to_stream: BTreeMap<rdlt_core::TableName, rdlt_core::StreamName> = BTreeMap::new();
    for stream in last_checkpoint.keys() {
        let root = crate::coverage::root_table(stream, rules);
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

    // THE RULES-DRIFT TRIPWIRE (round-7 fix): the join trusts that this
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
    for root in root_to_stream.keys() {
        let colliding = schemas
            .keys()
            .filter(|table| {
                schemas
                    .get(*table)
                    .is_some_and(|(schema, _)| schema.parent.is_none())
                    && crate::coverage::root_table(
                        &rdlt_core::StreamName::new(table.as_str()),
                        rules,
                    ) == *root
            })
            .count();
        if colliding > 1 {
            return Err(format!(
                "{colliding} recorded root tables normalize onto checkpointed root `{root}` — \
                 the stream-to-segment join is many-to-one (a normalization-rules drift \
                 shape) and cannot prove replay safe"
            ));
        }
    }

    // A segment's root table, along the parent links its Deltas recorded —
    // the shared bounded walk ([`crate::coverage::walk_to_root`], the same
    // implementation the loader's commit gate resolves roots with). Name
    // prefixes would NOT do: `child_table_name` re-normalizes, so a long
    // child's name truncates to a hash suffix that need not contain its
    // root. The scan's own refusals ride the walk's error channel: a table
    // with no recorded schema breaks delta-before-first-batch, and an
    // unterminated chain is a cycle no writer produces.
    let root_of = |table: &rdlt_core::TableName| -> Result<rdlt_core::TableName, String> {
        crate::coverage::walk_to_root(table, schemas.len(), |current| match schemas.get(current) {
            None => Err(format!(
                "segment table `{current}` has no schema delta anywhere in the manifest \
                 (the writer records delta-before-first-batch), so its covering stream \
                 is unknowable"
            )),
            Some((schema, _)) => Ok(schema.parent.as_ref().map(|link| link.parent.clone())),
        })?
        .ok_or_else(|| format!("table `{table}`'s recorded parent chain does not terminate"))
    };

    let mut keep = vec![true; span.len()];
    for (index, record) in span.iter().enumerate() {
        if let WalRecord::Segment { table, .. } = record {
            let root = root_of(table)?;
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

    // ORPHANS BESIDE SEGMENTLESS CHECKPOINTS ARE BENIGN (round-6 fix —
    // the guard here shrank twice and its last residue misdiagnosed a
    // ROUTINE shape as damage): a checkpointed stream whose root matches
    // no segment AND no recorded schema simply wrote zero rows for the
    // whole run — checkpoint-only, so no delta was ever recorded. Its
    // checkpoint covers nothing and carries its cursor; the orphans are
    // a never-checkpointing co-stream's and re-extraction re-delivers
    // them; every OTHER productive stream's replay must survive. The
    // shapes that still degrade are the attributional ones above: a
    // segment whose table has no recorded schema anywhere (root_of's
    // refusal — a segment that exists but cannot be attributed), and
    // two checkpointed streams normalizing to one root. The residual
    // this once accepted — a normalization-rules change between the
    // writing run and this one making a stream's own segments look
    // like a co-stream's orphans — is CLOSED (round-9): the rules
    // sidecar gate upstream refuses any span whose recorded rules
    // differ from this run's before the join is consulted at all. (A
    // pre-042 span with NO recorded rules proceeds with a warning —
    // the pre-sidecar posture, accepted so a healthy main-era WAL is
    // replayed rather than discarded into N2 duplication.)

    Ok(Some(
        span.into_iter()
            .zip(keep)
            .filter_map(|(record, keep)| keep.then_some(record))
            .collect(),
    ))
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
        // The sidecar every 042+ writer leaves beside its manifest —
        // the fixtures here model a matching-rules writer.
        std::fs::write(
            dir.join(crate::wal::RULES_SIDECAR),
            serde_json::to_vec(&rdlt_core::naming::IdentRules::default()).expect("rules json"),
        )
        .expect("write sidecar");
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
            scan(dir.path(), rdlt_core::naming::IdentRules::default())
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
            matches!(
                scan(dir.path(), rdlt_core::naming::IdentRules::default()),
                ScanOutcome::Unsupported { found: 1, .. }
            ),
            "an unversioned header is a v1 manifest"
        );
    }
}

#[cfg(test)]
mod per_stream_coverage_tests {
    //! The replay span is a PER-STREAM decision, not a positional one (042
    //! T7E). A checkpoint covers only the segments of ITS OWN stream that
    //! precede it: replaying anything else commits rows whose cursor never
    //! advanced, and the run's own extraction then delivers them AGAIN —
    //! the double-apply proven live on the multi-table crash sweep (3 of 4
    //! control runs on main @ 4e151e0e). These tests pin the scan directly
    //! on constructed manifests, so the race in the live trigger never
    //! decides whether the property holds.

    use super::*;
    use rdlt_core::{
        Cursor, LoadId, ParentLink, PipelineId, SchemaDelta, StreamName, TableName, TableSchema,
        WriteMode, naming::IdentRules,
    };

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
            delta: SchemaDelta {
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

    /// Write a manifest of `records` under a current-version Run
    /// header, with the writer's rules sidecar beside it.
    fn write_span(dir: &std::path::Path, records: Vec<WalRecord>, writer_rules: IdentRules) {
        let mut all = vec![WalRecord::Run {
            format_version: crate::wal::WAL_FORMAT_VERSION,
            load_id: LoadId::new("l"),
            pipeline: PipelineId::new("p"),
        }];
        all.extend(records);
        let mut out = String::new();
        for record in &all {
            out.push_str(&serde_json::to_string(record).expect("record json"));
            out.push('\n');
        }
        std::fs::write(dir.join("manifest.jsonl"), out).expect("write manifest");
        std::fs::write(
            dir.join(crate::wal::RULES_SIDECAR),
            serde_json::to_vec(&writer_rules).expect("rules json"),
        )
        .expect("write sidecar");
    }

    /// Scan a manifest of `records` — writer and scanner under the SAME
    /// (default) rules, the benign shape every pre-round-9 pin holds on.
    fn scan_span(records: Vec<WalRecord>) -> ScanOutcome {
        let dir = tempfile::tempdir().expect("tempdir");
        write_span(dir.path(), records, IdentRules::default());
        scan(dir.path(), IdentRules::default())
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

    /// THE T7E defect, deterministically: `events`' segment precedes
    /// `orders`' checkpoint POSITIONALLY, but no `events` checkpoint exists —
    /// so no cursor covers it and the run's own extraction will re-deliver
    /// its rows. Replaying it too is the double-apply. It must be dropped;
    /// `orders`' covered segment and checkpoint replay as before.
    #[test]
    fn an_uncovered_co_stream_segment_is_dropped_not_replayed() {
        let outcome = scan_span(vec![
            delta("events", None),
            delta("orders", None),
            segment("events", "f0.arrow"),
            segment("orders", "f1.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(
            replayed_files(&outcome),
            ["f1.arrow"],
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

    /// The old positional rule got THIS single-stream shape right, and it must
    /// stay right: a segment after its own stream's last checkpoint is
    /// uncovered and dropped.
    #[test]
    fn a_segment_after_its_own_streams_last_checkpoint_stays_dropped() {
        let outcome = scan_span(vec![
            delta("events", None),
            segment("events", "f0.arrow"),
            checkpoint("events"),
            segment("events", "f1.arrow"),
        ]);
        assert_eq!(replayed_files(&outcome), ["f0.arrow"]);
    }

    /// Interleaved streams each replay exactly their covered prefix — no
    /// positional truncation in either direction.
    #[test]
    fn interleaved_streams_each_replay_exactly_their_covered_prefix() {
        let outcome = scan_span(vec![
            delta("events", None),
            delta("orders", None),
            segment("events", "f0.arrow"),
            segment("orders", "f1.arrow"),
            checkpoint("events"),
            segment("events", "f2.arrow"),
            checkpoint("orders"),
        ]);
        // f0 precedes events' checkpoint, f1 precedes orders' — both covered.
        // f2 follows events' LAST checkpoint: uncovered, even though orders'
        // later checkpoint follows it positionally.
        assert_eq!(replayed_files(&outcome), ["f0.arrow", "f1.arrow"]);
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
            segment("itm_4f2a9c1b", "f0.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(
            replayed_files(&outcome),
            ["f0.arrow"],
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
            segment("orders", "f0.arrow"),
            segment("ghost", "f1.arrow"),
            checkpoint("orders"),
        ]);
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("ghost")),
            "unattributable segment must degrade, naming the table: {outcome:?}"
        );
    }

    /// THE ROUTINE POST-COMMIT SHAPE the loader's deferral makes (round-2
    /// fix wave): after a commit, an idle cursored stream checkpoints with
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
            segment("orders", "f0.arrow"),
            checkpoint("orders"),
            WalRecord::Committed { commit_seq: 1 },
            delta("events", None),
            segment("events", "f1.arrow"),
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

    /// THE REPLACE HAZARD (round-3 fix): a table whose every span
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
            segment("orders", "f0.arrow"),
            checkpoint("orders"),
            WalRecord::Committed { commit_seq: 1 },
            delta_with_mode("events", WriteMode::Replace),
            segment("events", "f1.arrow"),
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
            segment("events", "f0.arrow"),
            segment("orders", "f1.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(replayed_files(&outcome), ["f1.arrow"]);
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
            segment("itm_4f2a9c1b", "f0.arrow"),
            checkpoint("orders"),
        ]);
        assert_eq!(replayed_files(&outcome), ["f0.arrow"]);
        assert_eq!(
            ensured_tables(&outcome),
            ["itm_4f2a9c1b", "orders"],
            "the child and its recorded root both stay ensured"
        );
    }

    /// The whole-run-idle stream is BENIGN (round-6 fix — the guard's
    /// last residue misdiagnosed it as damage): a stream that wrote
    /// zero rows all run is checkpoint-only, so no delta was ever
    /// recorded, and its root matches no segment and no schema. Its
    /// checkpoint covers nothing and replays its cursor; the snapshot
    /// co-stream's orphans drop for re-extraction.
    #[test]
    fn an_orphan_segment_beside_an_idle_streams_checkpoint_recovers() {
        let outcome = scan_span(vec![
            delta("events", None),
            segment("events", "f0.arrow"),
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

    /// THE ROUTINE THREE-STREAM SHAPE (round-6 red pin): A productive
    /// and covered, B idle all run (checkpoint-only, no delta
    /// anywhere), C a snapshot stream with orphaned segments. A's span
    /// must SURVIVE — the old guard threw the whole replay away over
    /// B's idleness.
    #[test]
    fn a_productive_streams_span_survives_an_idle_co_stream_and_snapshot_orphans() {
        let outcome = scan_span(vec![
            delta("a_events", None),
            segment("a_events", "f0.arrow"),
            delta("c_snap", None),
            segment("c_snap", "f1.arrow"),
            checkpoint("a_events"),
            checkpoint("b_idle"),
        ]);
        assert_eq!(
            replayed_files(&outcome),
            ["f0.arrow"],
            "A's covered segment replays; C's orphan drops; B's idleness is not damage"
        );
        assert_eq!(
            ensured_tables(&outcome),
            ["a_events"],
            "only the productive stream's table is ensured"
        );
    }

    /// THE RULES-DRIFT TRIPWIRE (round-7 red pin): recorded tables
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
            segment("EVENTS", "f0.arrow"),
            segment("events", "f1.arrow"),
            checkpoint("EVENTS"),
        ]);
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason)
                if reason.contains("many-to-one")),
            "a many-to-one normalized join must degrade: {outcome:?}"
        );
    }

    /// THE RULES-CHANGE REFUSAL (round-9 red pin — the loss this
    /// closes ran SILENT): a crashed span whose writer recorded
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
                segment("events", "f0.arrow"),
                checkpoint("events"),
            ],
            IdentRules::default(),
        );
        let outcome = scan(dir.path(), IdentRules { max_len: 30 });
        assert!(
            matches!(outcome, ScanOutcome::Damaged(ref reason)
                if reason.contains("identifier-normalization rules")
                    && reason.contains("rules change")),
            "a rules change between crash and resume must refuse, naming itself: {outcome:?}"
        );
    }

    /// A manifest with NO rules sidecar is a PRE-042 writer's residue
    /// (round-10 correction — round 9 refused it as damage, which
    /// would DISCARD a healthy main-era WAL and re-open the N2
    /// duplication window for durable-identity destinations): the
    /// rules are unverifiable, so the scan warns and proceeds under
    /// this run's rules — the pre-sidecar behavior, and the span
    /// replays.
    #[test]
    fn a_manifest_without_the_rules_sidecar_warns_and_proceeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_span(
            dir.path(),
            vec![
                delta("events", None),
                segment("events", "f0.arrow"),
                checkpoint("events"),
            ],
            IdentRules::default(),
        );
        std::fs::remove_file(dir.path().join(crate::wal::RULES_SIDECAR)).expect("drop sidecar");
        let outcome = scan(dir.path(), IdentRules::default());
        assert_eq!(
            replayed_files(&outcome),
            ["f0.arrow"],
            "a pre-042 WAL (no sidecar) must replay, not discard: {outcome:?}"
        );
    }

    /// A sidecar that EXISTS but does not parse is not the pre-042
    /// shape — something wrote it and it cannot be trusted, so the
    /// span refuses as Damaged like a recorded mismatch.
    #[test]
    fn an_unparseable_rules_sidecar_degrades() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_span(
            dir.path(),
            vec![
                delta("events", None),
                segment("events", "f0.arrow"),
                checkpoint("events"),
            ],
            IdentRules::default(),
        );
        std::fs::write(
            dir.path().join(crate::wal::RULES_SIDECAR),
            b"not json at all",
        )
        .expect("corrupt sidecar");
        let outcome = scan(dir.path(), IdentRules::default());
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
            segment("orders", "f0.arrow"),
            checkpoint("Orders"),
            checkpoint("orders"),
        ]);
        assert!(
            matches!(outcome, ScanOutcome::Damaged(_)),
            "an ambiguous stream→root join must not guess: {outcome:?}"
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
        std::fs::write(
            dir.join(crate::wal::RULES_SIDECAR),
            serde_json::to_vec(&rdlt_core::naming::IdentRules::default()).expect("rules json"),
        )
        .expect("write sidecar");
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
                scan_off_runtime(&path, rdlt_core::naming::IdentRules::default()).await
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
