//! The WAL scan's pins, beside the production they judge (a child
//! module can read `scan`'s private helpers, which these pins need —
//! the same placement discipline `load::loader` documents).

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

/// THE SCAN-SCALE COMPLEXITY PIN, on the folds the scan actually
/// runs: resolving every table of a 100,000-deep linear chain
/// (shallowest-first, the punishing order) and then folding the
/// live set over one segment per table stays LINEAR — the hop
/// meter, not a flaky wall clock, is the bound. Single-table
/// memoization priced this same fixture at ~5×10⁹ hops (the
/// measured 1,000-table quadratic extrapolated), hours of
/// single-threaded recovery on a manifest the byte budget happily
/// admits; it now completes in well under a second of CI time.
#[test]
fn deep_chain_resolution_stays_linear_across_the_scan_folds() {
    const K: usize = 100_000;
    let mut schemas = SchemaMap::new();
    for i in 0..=K {
        let table = TableName::new(format!("t{i}"));
        let parent = (i > 0).then(|| ParentLink {
            parent: TableName::new(format!("t{}", i - 1)),
            depth: 1,
        });
        schemas.insert(
            table.clone(),
            (
                TableSchema {
                    table,
                    parent,
                    columns: vec![],
                },
                WriteMode::Append,
            ),
        );
    }
    let mut chains = lineage::Chain::default();
    for i in 0..=K {
        let root = root_of(&mut chains, &TableName::new(format!("t{i}")), &schemas)
            .expect("a recorded chain resolves");
        assert_eq!(root, TableName::new("t0"));
    }
    let records: Vec<WalRecord> = (0..=K)
        .map(|i| segment(&format!("t{i}"), &format!("l-{i:06}.arrow")))
        .collect();
    let live = live_tables(&records, &schemas, &mut chains);
    assert_eq!(live.len(), K + 1, "every chained table is live");
    assert!(
        chains.hops() <= 2 * (K as u64 + 1),
        "resolution across the whole manifest stays linear: {} hops for {K} tables",
        chains.hops()
    );
}

/// The same shape end to end through the PRODUCTION scan: a
/// manifest of thousands of chained deltas and segments under one
/// covering checkpoint recovers with every segment covered, in CI
/// time.
#[test]
fn a_deep_chained_manifest_recovers_with_full_coverage() {
    const K: usize = 2_000;
    let mut records = Vec::new();
    for i in 0..=K {
        let parent = (i > 0).then(|| format!("t{}", i - 1));
        records.push(delta(&format!("t{i}"), parent.as_deref()));
        records.push(segment(&format!("t{i}"), &format!("l-{i:06}.arrow")));
    }
    records.push(checkpoint("t0"));
    let outcome = scan_span(records);
    let ScanOutcome::Recover(span) = outcome else {
        panic!("a checkpointed chained span recovers: {outcome:?}");
    };
    assert_eq!(
        replayed_files(&ScanOutcome::Recover(span)).len(),
        K + 1,
        "every chained segment is covered by the root stream's checkpoint"
    );
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
    std::fs::write(dir.path().join(RULES_SIDECAR), b"not json at all").expect("corrupt sidecar");
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
/// safe-direction, pinned in format.rs) a complete trailer means
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
            crate::blocking::off_runtime(move || scan_dir(&path)).await
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
    std::os::unix::fs::symlink(&outside, dir.path().join(RULES_SIDECAR)).expect("plant symlink");
    let outcome = scan_with_deadline(dir.path().to_path_buf());
    assert!(
        matches!(outcome, ScanOutcome::Damaged(ref reason) if reason.contains("symlink")),
        "a sidecar symlink must refuse as damage, never read through: {outcome:?}"
    );
}
