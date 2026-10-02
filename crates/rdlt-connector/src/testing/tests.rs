mod acks;
mod changes;
mod floods;
mod history;
mod read_back;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{
    Clause, ClauseResult, DESTINATION_CLAUSES, Observed, Outcome, Probe, Report, SOURCE_CLAUSES,
    Unprobed, Verdict, certify_destination, certify_destination_factory_observed, certify_source,
    certify_source_factory_observed,
};
use crate::capabilities::{Capabilities, SchemaChanges};
use crate::catalog::{Catalog, Checkpointing, StreamSpec};
use crate::commit::{CommitMeta, Receipt};
use crate::cursor::Cursor;
use crate::destination::{
    DestinationConnector, MergeKey, OpenContext, Opened, RootKey, Session, TableChange, TableRef,
    TableWriter, WriteStats,
};
use crate::emitter::Emitter;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{
    CommitSeq, Epoch, GenerationId, LoadId, PartitionId, PipelineId, SegmentId, StreamName,
    TablePath,
};
use crate::sink::Push;
use crate::source::{Partition, PartitionPlan, ReadStream, SourceConnector, Streams};
use crate::spec::{BoxFuture, ConnectContext};
use crate::state::{StateChange, StateRecord, StreamState};
use crate::types::LogicalType;

fn failed(report: &Report) -> Vec<&'static str> {
    report.failures().map(|result| result.clause.id).collect()
}

/// A source that is correct unless a flag breaks one behavior.
#[derive(Deserialize, JsonSchema)]
#[serde(default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag breaks one behavior"
)]
struct PagesConfig {
    pages: u32,
    ignore_cursor: bool,
    /// A read from a cursor starts a page before it and ends a page short: as many rows as it
    /// owes, not the ones.
    shifted: bool,
    ignore_barriers: bool,
    /// Its reads send no checkpoint at all.
    uncheckpointed: bool,
    /// A read from this page's cursor or a later one is shifted as `shifted` shifts every read;
    /// 0 shifts none.
    shifted_from: u32,
    natural: bool,
    unstable_discover: bool,
    empty_catalog: bool,
    repeat_partitions: bool,
    fail_on_stop: bool,
    refuse_connect: bool,
    /// Fails its check, though it reads.
    refuse_check: bool,
    /// Fails every read, though it checks.
    refuse_reads: bool,
    /// Its reads wait for data that never comes, as a quiet stream's do.
    idle: bool,
    /// The call that never returns: `connect`, `check`, `discover` or `plan`.
    hang: String,
    /// How a plan from a state naming a partition breaks: `renamed` names the partition anew, so
    /// it is read again from its start; `forgotten` names none, so the rest is never read.
    replanned: String,
    /// The phase its plans name, as a stream read in phases does; 0 names none.
    phase: u16,
    /// The page its phase starts at, which its plans name; a read from before it is refused, as
    /// a change stream's is from before the position its snapshot captured.
    start: u32,
}

impl Default for PagesConfig {
    fn default() -> Self {
        Self {
            pages: 4,
            ignore_cursor: false,
            shifted: false,
            ignore_barriers: false,
            uncheckpointed: false,
            shifted_from: 0,
            natural: false,
            unstable_discover: false,
            empty_catalog: false,
            repeat_partitions: false,
            fail_on_stop: false,
            refuse_connect: false,
            refuse_check: false,
            refuse_reads: false,
            idle: false,
            hang: String::new(),
            replanned: String::new(),
            phase: 0,
            start: 0,
        }
    }
}

struct Pages {
    config: PagesConfig,
    discovered: AtomicBool,
}

impl PagesConfig {
    /// Never returns when this configuration hangs in `call`.
    async fn hang_in(&self, call: &str) {
        if self.hang == call {
            std::future::pending::<()>().await;
        }
    }
}

impl SourceConnector for Pages {
    const ID: &'static str = "io.test.pages";
    const VERSION: &'static str = "0.0.1";
    type Config = PagesConfig;

    async fn connect(config: PagesConfig, _context: &ConnectContext) -> Result<Self> {
        config.hang_in("connect").await;
        if config.refuse_connect {
            return Err(ConnectorError::config("refused")
                .with_source(std::io::Error::other("the vault is sealed")));
        }
        Ok(Self {
            config,
            discovered: AtomicBool::new(false),
        })
    }

    async fn check(&self) -> Result<()> {
        self.config.hang_in("check").await;
        if self.config.refuse_check {
            return Err(ConnectorError::config("refused"));
        }
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        if self.config.empty_catalog {
            Streams::new()
        } else {
            Streams::new().with(Page {
                natural: self.config.natural,
            })
        }
    }

    async fn discover(&self) -> Result<Catalog> {
        self.config.hang_in("discover").await;
        let mut streams: Vec<StreamSpec> =
            self.streams().catalog().map(Vec::from).unwrap_or_default();
        if self.config.unstable_discover && self.discovered.swap(true, Ordering::SeqCst) {
            streams.push(StreamSpec::new(StreamName::new("late").unwrap()));
        }
        Ok(Catalog::new(streams).unwrap())
    }
}

struct Page {
    natural: bool,
}

impl ReadStream<Pages> for Page {
    type Cursor = u32;

    fn spec(&self) -> StreamSpec {
        let checkpointing = if self.natural {
            Checkpointing::Natural
        } else {
            Checkpointing::OnDemand
        };
        StreamSpec::new(StreamName::new("pages").unwrap()).with_checkpointing(checkpointing)
    }

    async fn partitions(&self, source: &Pages, state: &StreamState) -> Result<Vec<Partition>> {
        source.config.hang_in("plan").await;
        if !state.partitions.is_empty() {
            match source.config.replanned.as_str() {
                "renamed" => return Ok(vec![Partition::new(PartitionId::parse("again").unwrap())]),
                "forgotten" => return Ok(Vec::new()),
                _ => {}
            }
        }
        let copies = if source.config.repeat_partitions {
            2
        } else {
            1
        };
        Ok(vec![Partition::single(); copies])
    }

    async fn plan(&self, source: &Pages, state: &StreamState) -> Result<PartitionPlan> {
        let mut plan = PartitionPlan::from(self.partitions(source, state).await?);
        if source.config.start > 0 {
            let start = Cursor::encode(Self::CURSOR_VERSION, &source.config.start)?;
            for partition in &plan.partitions {
                plan.starts.insert(partition.id().clone(), start.clone());
            }
        }
        Ok(match source.config.phase {
            0 => plan,
            phase => plan.phase(phase),
        })
    }

    async fn read(
        &self,
        source: &Pages,
        _partition: &Partition,
        cursor: u32,
        out: &mut Emitter<u32>,
    ) -> Result<()> {
        if source.config.refuse_reads {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                "the pages are gone",
            ));
        }
        if source.config.idle {
            std::future::pending::<()>().await;
        }
        if cursor < source.config.start {
            return Err(ConnectorError::data("the phase starts later"));
        }
        let late = source.config.shifted_from > 0 && cursor >= source.config.shifted_from;
        let shifted = source.config.shifted || late;
        let (start, end) = match (source.config.ignore_cursor, shifted) {
            (true, _) => (0, source.config.pages),
            (false, true) if cursor > 0 => (cursor - 1, source.config.pages - 1),
            (false, _) => (cursor, source.config.pages),
        };
        for page in start..end {
            let pushed = out.rows(&[json!({ "page": page })]).await;
            if source.config.fail_on_stop && pushed.is_err() {
                return Err(ConnectorError::data("gave up"));
            }
            pushed?;
            let answers = !source.config.ignore_barriers || !out.checkpoint_due();
            if answers && !source.config.uncheckpointed {
                out.checkpoint(&(page + 1)).await?;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_correct_source_passes_every_clause() {
    certify_source::<Pages>(json!({})).await.assert_passed();
}

#[tokio::test]
async fn a_source_with_no_data_is_incomplete_where_nothing_could_be_seen() {
    let empty = certify_source::<Pages>(json!({ "pages": 0 })).await;
    assert_eq!(empty.verdict(), Verdict::Incomplete, "{empty}");
    assert_eq!(
        empty.outcome("S-BARRIER"),
        Some(&Outcome::Passed),
        "a source with no data owes no answer"
    );
    // A stream that never checkpoints leaves nothing to resume, or plan again, from.
    for clause in ["S-RESUME", "S-PARTITION"] {
        assert!(
            matches!(empty.outcome(clause), Some(Outcome::Unobserved(_))),
            "{clause}: {empty}"
        );
    }
    let unobserved: Vec<_> = empty.unobserved().map(|result| result.clause.id).collect();
    assert_eq!(unobserved, ["S-RESUME", "S-PARTITION"]);
}

#[tokio::test]
async fn a_source_whose_plans_name_their_phase_is_planned_again_within_it() {
    let report = certify_source::<Pages>(json!({ "phase": 2, "start": 1 })).await;
    report.assert_passed();
    assert_eq!(report.outcome("S-PARTITION"), Some(&Outcome::Passed));
}

#[test]
fn a_report_passes_only_when_every_clause_that_applies_was_seen_to_be_met() {
    let result = |outcome: &Outcome| ClauseResult {
        clause: Clause {
            id: "S-CHECK",
            statement: "a statement",
            unless: "",
        },
        outcome: outcome.clone(),
        note: None,
    };
    let (passed, failed) = (Outcome::Passed, Outcome::Failed("broken".into()));
    let inapplicable = Outcome::Inapplicable("not served".into());
    let unobserved = Outcome::Unobserved("not seen".into());
    let cases: [(&[&Outcome], Verdict); 9] = [
        (&[], Verdict::Incomplete),
        (&[&passed], Verdict::Passed),
        (&[&passed, &inapplicable], Verdict::Passed),
        // A report none of whose clauses applies certified nothing.
        (&[&inapplicable, &inapplicable], Verdict::Incomplete),
        (&[&passed, &unobserved], Verdict::Incomplete),
        (&[&unobserved], Verdict::Incomplete),
        (&[&passed, &failed], Verdict::Failed),
        (&[&unobserved, &failed, &inapplicable], Verdict::Failed),
        (&[&failed], Verdict::Failed),
    ];
    for (outcomes, verdict) in cases {
        let report = Report {
            connector: "test.verdict".to_owned(),
            results: outcomes.iter().copied().map(result).collect(),
        };
        assert_eq!(report.verdict(), verdict, "{report}");
        assert_eq!(report.passed(), verdict == Verdict::Passed, "{report}");
        let asserted = std::panic::catch_unwind(|| report.assert_passed());
        assert_eq!(asserted.is_ok(), verdict == Verdict::Passed, "{report}");
        // Whatever was not observed, nothing failed and something passed.
        let kept = verdict != Verdict::Failed && outcomes.contains(&&passed);
        let asserted = std::panic::catch_unwind(|| report.assert_none_failed());
        assert_eq!(asserted.is_ok(), kept, "{report}");
        assert_eq!(
            report.failures().count(),
            usize::from(outcomes.contains(&&failed))
        );
        let unseen = outcomes.iter().filter(|outcome| ***outcome == unobserved);
        assert_eq!(report.unobserved().count(), unseen.count());
    }
}

#[test]
fn a_report_ends_with_its_verdict_and_how_many_clauses_fared_each_way() {
    let result = |outcome: Outcome| ClauseResult {
        clause: Clause {
            id: "S-CHECK",
            statement: "a statement",
            unless: "",
        },
        outcome,
        note: None,
    };
    let mut results = vec![result(Outcome::Passed), result(Outcome::Passed)];
    results.extend((0..3).map(|_| result(Outcome::Unobserved("unseen".into()))));
    results.extend((0..4).map(|_| result(Outcome::Inapplicable("undeclared".into()))));
    let mut report = Report {
        connector: "test.summary".to_owned(),
        results,
    };
    assert_eq!(
        report.summary(),
        "incomplete: 2 passed, 0 failed, 3 not observed, 4 not applicable"
    );
    report
        .results
        .push(result(Outcome::Failed("broken".into())));
    assert_eq!(
        report.summary(),
        "failed: 2 passed, 1 failed, 3 not observed, 4 not applicable"
    );
    assert_eq!(
        report.to_string().lines().last(),
        Some(report.summary().as_str())
    );
    assert_eq!(
        [Verdict::Passed, Verdict::Incomplete, Verdict::Failed].map(Verdict::as_str),
        ["passed", "incomplete", "failed"]
    );
    assert_eq!(Verdict::Incomplete.to_string(), "incomplete");
}

#[tokio::test]
async fn the_barrier_clause_does_not_apply_to_natural_checkpointing() {
    let report = certify_source::<Pages>(json!({ "natural": true })).await;
    report.assert_passed();
    assert!(
        matches!(report.outcome("S-BARRIER"), Some(Outcome::Inapplicable(_))),
        "{report}"
    );
    assert_eq!(report.outcome("S-CHECK"), Some(&Outcome::Passed));
    assert_eq!(report.outcome("S-NONE"), None);
}

#[tokio::test]
async fn each_broken_source_behavior_fails_exactly_its_clause() {
    let cases: [(&str, &[&str]); 8] = [
        // A read from a cursor that starts over, or elsewhere, reads the wrong rows however it
        // is planned.
        ("ignore_cursor", &["S-RESUME", "S-PARTITION"]),
        ("shifted", &["S-RESUME", "S-PARTITION"]),
        ("ignore_barriers", &["S-BARRIER"]),
        ("unstable_discover", &["S-DISCOVER"]),
        ("empty_catalog", &["S-DISCOVER"]),
        ("repeat_partitions", &["S-PLAN"]),
        ("fail_on_stop", &["S-STOP"]),
        ("refuse_check", &["S-CHECK"]),
    ];
    for (flag, clauses) in cases {
        let report = certify_source::<Pages>(json!({ flag: true })).await;
        assert_eq!(failed(&report), clauses, "{flag}: {report}");
    }
}

#[tokio::test]
async fn a_source_that_never_checkpoints_leaves_its_resumes_unobserved() {
    let config = json!({ "uncheckpointed": true, "natural": true });
    let report = certify_source::<Pages>(config).await;
    let outcome = report.outcome("S-RESUME");
    assert!(matches!(outcome, Some(Outcome::Unobserved(_))), "{report}");
    assert!(failed(&report).is_empty(), "{report}");
    assert_eq!(report.verdict(), Verdict::Incomplete, "{report}");
}

#[tokio::test]
async fn a_source_whose_late_cursors_resume_wrongly_fails_its_resume_clause() {
    for shifted_from in [6, 7, 9] {
        let config = json!({ "pages": 12, "shifted_from": shifted_from });
        let report = certify_source::<Pages>(config).await;
        assert_eq!(failed(&report), ["S-RESUME"], "{shifted_from}: {report}");
    }
}

#[test]
fn resumes_are_sampled_from_the_first_checkpoint_to_the_last() {
    assert!(super::source::resume::sampled(0).is_empty());
    for sent in 1..=5 {
        let every: Vec<usize> = (0..sent).collect();
        assert_eq!(super::source::resume::sampled(sent), every);
    }
    assert_eq!(super::source::resume::sampled(6), [0, 1, 2, 3, 5]);
    assert_eq!(super::source::resume::sampled(12), [0, 2, 5, 8, 11]);
    assert_eq!(
        super::source::resume::sampled(1_000_001),
        [0, 250_000, 500_000, 750_000, 1_000_000]
    );
}

#[tokio::test]
async fn a_source_planned_again_with_a_gap_or_an_overlap_fails_its_partition_clause() {
    for replanned in ["renamed", "forgotten"] {
        let report = certify_source::<Pages>(json!({ "replanned": replanned })).await;
        assert_eq!(failed(&report), ["S-PARTITION"], "{replanned}: {report}");
    }
}

#[tokio::test(start_paused = true)]
async fn the_clauses_a_certification_finished_before_it_was_cut_are_kept() {
    // Its reads wait for data: the first clause that records one takes its whole bound.
    let observed = Observed::new();
    let factory = crate::source::source_factory::<Pages>();
    let certifying =
        certify_source_factory_observed(factory.as_ref(), json!({ "idle": true }), &observed);
    let cut = tokio::time::timeout(std::time::Duration::from_secs(20), certifying).await;
    assert!(cut.is_err(), "the certification ended before it was cut");
    let finished: Vec<&str> = observed
        .results()
        .iter()
        .map(|result| result.clause.id)
        .collect();
    assert_eq!(finished, ["S-CHECK", "S-DISCOVER", "S-PLAN"]);
    assert_eq!(observed.connector().as_deref(), Some("io.test.pages"));
    // Left to end, it tells every clause, as its report does.
    let observed = Observed::new();
    let report = certify_source_factory_observed(factory.as_ref(), json!({}), &observed).await;
    assert_eq!(observed.results(), report.results);
    let observed = Observed::new();
    let refused = json!({ "refuse_connect": true });
    let report = certify_source_factory_observed(factory.as_ref(), refused, &observed).await;
    assert_eq!(observed.results(), report.results);
    assert_eq!(report.results.len(), SOURCE_CLAUSES.len());
}

#[tokio::test]
async fn a_destination_tells_each_clause_as_it_ends_as_its_report_does() {
    for flag in [None, Some("refuse_connect")] {
        let observed = Observed::new();
        let name = format!("observed_{}", flag.is_some());
        let mut config = json!({ "store": name });
        if let Some(flag) = flag {
            config[flag] = json!(true);
        }
        let factory = crate::destination::destination_factory::<Vault>();
        let probe = VaultProbe(vault(&name));
        let report =
            certify_destination_factory_observed(factory.as_ref(), config, &probe, &observed).await;
        assert_eq!(observed.results(), report.results);
        assert_eq!(observed.connector().as_deref(), Some("io.test.vault"));
        assert_eq!(report.results.len(), DESTINATION_CLAUSES.len());
    }
}

/// Certification must end even when a connector never answers; a day of paused time passes
/// instantly, so a call without a timeout fails this bound instead of hanging the test.
async fn within_a_day<T>(certification: impl Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_hours(24), certification)
        .await
        .expect("certification ended")
}

#[tokio::test(start_paused = true)]
async fn a_source_whose_read_waits_for_data_passes_its_check() {
    let report = within_a_day(certify_source::<Pages>(json!({ "idle": true }))).await;
    assert_eq!(
        report.outcome("S-CHECK"),
        Some(&Outcome::Passed),
        "{report}"
    );
}

#[tokio::test]
async fn a_source_that_checks_but_cannot_read_fails_its_check() {
    let report = certify_source::<Pages>(json!({ "refuse_reads": true })).await;
    assert!(failed(&report).contains(&"S-CHECK"), "{report}");
}

#[tokio::test(start_paused = true)]
async fn a_source_call_that_never_returns_fails_instead_of_hanging() {
    let cases = [
        ("connect", SOURCE_CLAUSES.len()),
        ("check", 1),
        ("discover", SOURCE_CLAUSES.len()),
        ("plan", 6),
    ];
    for (call, failures) in cases {
        let report = within_a_day(certify_source::<Pages>(json!({ "hang": call }))).await;
        assert_eq!(failed(&report).len(), failures, "{call}: {report}");
        assert!(
            report.to_string().contains("took longer"),
            "{call}: {report}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_destination_whose_flushes_never_end_fails_instead_of_hanging() {
    let report = within_a_day(certify_vault("hang_flush", Some("hang_flush"))).await;
    assert!(
        matches!(report.outcome("D-LANES"), Some(Outcome::Failed(_))),
        "{report}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_destination_connect_that_never_returns_fails_instead_of_hanging() {
    let report = within_a_day(certify_vault("hang_connect", Some("hang_connect"))).await;
    assert_eq!(failed(&report).len(), DESTINATION_CLAUSES.len(), "{report}");
    assert!(report.to_string().contains("took longer"), "{report}");
}

#[tokio::test]
async fn a_source_that_cannot_connect_fails_every_clause() {
    let report = certify_source::<Pages>(json!({ "refuse_connect": true })).await;
    assert!(report.results.iter().all(
        |result| matches!(&result.outcome, Outcome::Failed(reason) if reason.contains("connect"))
    ));
    assert!(
        report.results.iter().all(|result| matches!(
            &result.outcome,
            Outcome::Failed(reason) if reason.contains("the vault is sealed")
        )),
        "each failure says what caused it: {report}"
    );
    let rendered = report.to_string();
    assert!(rendered.contains("FAIL S-CHECK"), "{rendered}");
    assert!(std::panic::catch_unwind(|| report.assert_passed()).is_err());
}

#[tokio::test]
async fn json_pushes_compare_by_rows_not_formatting() {
    let normalize = super::source::recording::normalize;
    let array = normalize(Push::Json(Bytes::from_static(
        b"[ {\"a\": 1}, {\"a\": 2} ]",
    )))
    .await;
    let lines = normalize(Push::Json(Bytes::from_static(b"{\"a\":1}\n{\"a\":2}\n"))).await;
    assert_eq!(array, lines);
    assert_eq!(
        array,
        Push::Json(Bytes::from_static(b"[{\"a\":1},{\"a\":2}]"))
    );
}

/// A destination that is correct unless a flag breaks one behavior.
#[derive(Default, Deserialize, JsonSchema)]
#[serde(default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag breaks one behavior"
)]
struct VaultConfig {
    store: String,
    static_epoch: bool,
    miscount: bool,
    republish: bool,
    forget_state: bool,
    ignore_deletes: bool,
    publish_on_write: bool,
    keep_staging: bool,
    no_fence: bool,
    wrong_fence_kind: bool,
    refuse_connect: bool,
    forget_receipts: bool,
    publish_all: bool,
    /// Publishes, for each segment a commit lists, one it staged and the commit does not list.
    publish_other: bool,
    /// Stores every `name` value as null.
    blank_names: bool,
    local_epoch: bool,
    local_state: bool,
    stale_writes: bool,
    refuse_unstaged: bool,
    hang_connect: bool,
    replace_early: bool,
    merge_appends: bool,
    /// Keeps the first row of each key a merge meets, whatever its sequence.
    merge_keeps_first: bool,
    /// Keeps the last row of each key a merge meets, whatever its sequence.
    merge_keeps_last: bool,
    refuse_repeated_changes: bool,
    ignore_added_columns: bool,
    refuse_widening: bool,
    /// Declares only the minimal capabilities.
    minimal: bool,
    /// Declares no schema change at all.
    fixed_schema: bool,
    /// Applies a change that conflicts with a column's type instead of refusing it.
    accept_conflicts: bool,
    /// Refuses a batch whose column is narrower than the table's.
    refuse_narrower: bool,
    /// Reports a conflict as a plain data error, without the `schema_conflict` code.
    uncoded_conflicts: bool,
    /// Refuses a batch holding a dictionary-encoded column.
    refuse_dictionaries: bool,
    /// Refuses a batch holding a dictionary whose values are not strings.
    decode_only_strings: bool,
    /// Keeps only the last table's rows staged in a segment.
    one_table_per_segment: bool,
    /// Merges a child table by its own key, as if it had no root.
    children_merge_by_key: bool,
    /// Replaces child rows only in the child tables a commit stages rows for, not in every one
    /// it lists.
    ignore_child_tables: bool,
    /// Keeps no history.
    no_history: bool,
    /// Replaces a history key's current version instead of closing it.
    history_overwrites: bool,
    /// Opens a version for a change equal to its key's current one.
    history_duplicates: bool,
    /// Closes versions without clearing their current flag.
    history_stays_current: bool,
    /// Versions a change whatever its key's newest version's sequence.
    history_ignores_seq: bool,
    /// Leaves the version a hard delete should close current.
    history_keeps_deleted: bool,
    /// Closes nothing on a truncate.
    history_ignores_truncates: bool,
    /// Keeps the closed version's sequence in the version a soft delete opens.
    history_soft_keeps_seq: bool,
    /// Spares, on a truncate, the versions its own commit opened.
    history_truncate_spares_commit: bool,
    /// Stores versions without their hash.
    history_drops_hash: bool,
    /// Swaps in only the first generation a commit finishes.
    finish_one_generation: bool,
    /// Publishes its columns under lower-case names, though it declares it keeps case.
    fold_names: bool,
    /// Keeps only the staging of the table's writer that wrote last, of all its writers.
    lose_lanes: bool,
    /// Fails its check, though sessions open.
    refuse_check: bool,
    /// The writers it declares it runs at once, when not the default.
    writers: u16,
    /// The longest identifier it declares, when not the default.
    identifier_len: u16,
    /// Reserves `id`, and every lengthening of it its identifiers are long enough for.
    reserve_ids: bool,
    /// Never finishes a flush.
    hang_flush: bool,
    /// Lets any pipeline write into any table, whichever pipeline created it.
    share_tables: bool,
    /// Refuses every writer of a table once another pipeline's writer of it was refused.
    lock_on_intrusion: bool,
    /// Refuses another pipeline's table as a data error, though coded `table_owned`.
    owned_as_data: bool,
    /// Refuses another pipeline's table as a configuration error with no code.
    owned_uncoded: bool,
    /// Applies a change whatever the sequence of the row its key holds.
    ignore_seq_guard: bool,
    /// Keeps no tombstones, so a change sent again brings a removed row back.
    forget_tombstones: bool,
    /// Truncates every row, those sequenced after the truncate too.
    truncate_everything: bool,
    /// Removes the rows soft deletes and truncates should mark.
    hard_on_soft: bool,
    /// Stores null in the columns an update flags unchanged.
    drop_unchanged: bool,
    /// Keeps a change stream's tombstones only as long as the session that made them.
    session_tombstones: bool,
    /// Keeps a table's tombstones when a generation replaces its rows.
    keep_replaced_tombstones: bool,
    /// Declares it merges no change stream.
    no_change_merges: bool,
    /// Declares it cannot replace a table's rows with a generation.
    no_replace: bool,
    /// Declares it removes rows but never marks them deleted.
    hard_deletes_only: bool,
    /// Declares it marks rows deleted but never removes them.
    soft_deletes_only: bool,
    /// Declares it keeps no column an update leaves unchanged.
    no_partial_updates: bool,
    /// Declares it drops no tables.
    no_drops: bool,
    /// Keeps the rows of the tables a commit drops.
    keep_dropped: bool,
    /// Drops a table's rows but keeps its owner, so no other pipeline may create it.
    keep_owner: bool,
    /// Drops another pipeline's table.
    drop_others: bool,
    /// Swaps a generation into another pipeline's table.
    swap_others: bool,
    /// Lets a session a newer one fenced claim a table no pipeline owns.
    stale_claims: bool,
}

#[derive(Default)]
struct VaultStore {
    epochs: BTreeMap<PipelineId, u64>,
    state: BTreeMap<PipelineId, BTreeMap<String, StateRecord>>,
    receipts: BTreeMap<(LoadId, CommitSeq), Receipt>,
    staged: BTreeMap<(PipelineId, SegmentId), Vec<Staged>>,
    published: BTreeMap<String, Vec<RecordBatch>>,
    /// Committed rows of replace generations, by table and generation.
    generations: BTreeMap<(String, GenerationId), Vec<RecordBatch>>,
    /// Each table's name, by path, as writers named it.
    tables: BTreeMap<TablePath, String>,
    /// Every schema change applied, rendered.
    changes: BTreeSet<String>,
    /// Columns added while `ignore_added_columns` was set, by table; writes drop them.
    ignored: BTreeSet<(String, String)>,
    /// Each table's columns and their types, as schema changes left them.
    columns: BTreeMap<String, BTreeMap<String, LogicalType>>,
    /// The pipeline each table belongs to: the first to refer to it.
    owners: BTreeMap<String, PipelineId>,
    /// Tables no writer may write, under `lock_on_intrusion`.
    locked: BTreeSet<String>,
    /// Each change stream's table's tombstones.
    tombstones: BTreeMap<String, changes::Tombstones>,
}

impl VaultStore {
    /// Claims `table` for `pipeline` where no pipeline owns it yet; another pipeline's table is
    /// refused as `table_owned`, unless `shared`.
    fn claim(&mut self, pipeline: &PipelineId, table: &str, shared: bool) -> Result<()> {
        let owner = self
            .owners
            .entry(table.to_owned())
            .or_insert_with(|| pipeline.clone());
        if owner == pipeline || shared {
            Ok(())
        } else {
            Err(ConnectorError::table_owned(table, owner.as_str()))
        }
    }
}

/// Every vault writer made, so each is told from the others.
static WRITERS: AtomicU64 = AtomicU64::new(0);

/// A batch staged for a table, a replace generation of it, or a merge into it.
#[derive(Clone)]
struct Staged {
    table: String,
    writer: u64,
    generation: Option<GenerationId>,
    merge: Option<MergeKey>,
    batch: RecordBatch,
}

type SharedVault = Arc<Mutex<VaultStore>>;

static VAULTS: LazyLock<Mutex<BTreeMap<String, SharedVault>>> = LazyLock::new(Mutex::default);

fn vault(name: &str) -> SharedVault {
    Arc::clone(VAULTS.lock().unwrap().entry(name.to_owned()).or_default())
}

/// The shared store, and this connection's private one for the `local_*` flags.
#[derive(Clone)]
struct VaultStores {
    shared: SharedVault,
    local: SharedVault,
}

impl VaultConfig {
    /// `refusal`, of another pipeline's table, as `owned_as_data` or `owned_uncoded` shape it.
    fn refused_as(&self, refusal: ConnectorError) -> ConnectorError {
        if self.owned_as_data {
            ConnectorError::data(refusal.to_string()).with_code("table_owned")
        } else if self.owned_uncoded {
            ConnectorError::config(refusal.to_string())
        } else {
            refusal
        }
    }

    /// The pipeline's current epoch, from the store this configuration keeps epochs in.
    fn epoch(&self, shared: &VaultStore, stores: &VaultStores, pipeline: &PipelineId) -> u64 {
        let epochs = |store: &VaultStore| store.epochs.get(pipeline).copied().unwrap_or_default();
        if self.local_epoch {
            epochs(&stores.local.lock().unwrap())
        } else {
            epochs(shared)
        }
    }

    fn state_store<'a>(&self, stores: &'a VaultStores) -> &'a SharedVault {
        if self.local_state {
            &stores.local
        } else {
            &stores.shared
        }
    }
}

struct Vault {
    config: Arc<VaultConfig>,
    stores: VaultStores,
}

struct VaultSession {
    config: Arc<VaultConfig>,
    stores: VaultStores,
    pipeline: PipelineId,
    epoch: u64,
    /// The tombstones this session made, under `session_tombstones`.
    tombstones: Mutex<BTreeMap<String, changes::Tombstones>>,
}

struct VaultWriter {
    /// Tells this writer's staging from its table's other writers'.
    id: u64,
    config: Arc<VaultConfig>,
    stores: VaultStores,
    pipeline: PipelineId,
    epoch: u64,
    table: String,
    generation: Option<GenerationId>,
    merge: Option<MergeKey>,
}

impl DestinationConnector for Vault {
    const ID: &'static str = "io.test.vault";
    const VERSION: &'static str = "0.0.1";
    type Config = VaultConfig;
    type Session = VaultSession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        if !self.config.minimal {
            capabilities.write_modes.replace = true;
            capabilities.write_modes.merge = true;
            capabilities.write_modes.history = !self.config.no_history;
            capabilities.schema_changes = SchemaChanges::all();
            capabilities.max_parallel_writers = std::num::NonZeroU16::new(4).expect("not zero");
            capabilities.merge_changes = true;
            capabilities.delete_modes.hard = true;
            capabilities.delete_modes.soft = true;
            capabilities.partial_updates = true;
        }
        if let Some(writers) = std::num::NonZeroU16::new(self.config.writers) {
            capabilities.max_parallel_writers = writers;
        }
        if let Some(longest) = std::num::NonZeroU16::new(self.config.identifier_len) {
            capabilities.identifiers.max_len = longest;
        }
        if self.config.reserve_ids {
            let longest = usize::from(capabilities.identifiers.max_len.get());
            capabilities.identifiers.reserved = (0..=longest - 2)
                .map(|more| format!("id{}", "_".repeat(more)))
                .collect();
        }
        if self.config.fixed_schema {
            capabilities.schema_changes = SchemaChanges::default();
        }
        capabilities.merge_changes &= !self.config.no_change_merges;
        capabilities.write_modes.replace &= !self.config.no_replace;
        capabilities.delete_modes.hard &= !self.config.soft_deletes_only;
        capabilities.delete_modes.soft &= !self.config.hard_deletes_only;
        capabilities.partial_updates &= !self.config.no_partial_updates;
        capabilities.drop_tables = !self.config.minimal && !self.config.no_drops;
        capabilities
    }

    async fn connect(config: VaultConfig, _context: &ConnectContext) -> Result<Self> {
        if config.hang_connect {
            std::future::pending::<()>().await;
        }
        if config.refuse_connect {
            return Err(ConnectorError::config("refused")
                .with_source(std::io::Error::other("the vault is sealed")));
        }
        let stores = VaultStores {
            shared: vault(&config.store),
            local: SharedVault::default(),
        };
        Ok(Self {
            config: Arc::new(config),
            stores,
        })
    }

    async fn check(&self) -> Result<()> {
        if self.config.refuse_check {
            return Err(ConnectorError::config("refused"));
        }
        Ok(())
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<VaultSession>> {
        let epochs = if self.config.local_epoch {
            &self.stores.local
        } else {
            &self.stores.shared
        };
        let epoch = {
            let mut store = epochs.lock().unwrap();
            let epoch = store.epochs.entry(context.pipeline.clone()).or_default();
            *epoch += 1;
            *epoch
        };
        let state = self
            .config
            .state_store(&self.stores)
            .lock()
            .unwrap()
            .state
            .get(&context.pipeline)
            .map(|state| state.values().cloned().collect())
            .unwrap_or_default();
        let reported = if self.config.static_epoch {
            Epoch(1)
        } else {
            Epoch(epoch)
        };
        let session = VaultSession {
            config: Arc::clone(&self.config),
            stores: self.stores.clone(),
            pipeline: context.pipeline.clone(),
            epoch,
            tombstones: Mutex::default(),
        };
        Ok(Opened {
            session,
            epoch: reported,
            state,
        })
    }
}

impl VaultSession {
    /// Claims `table` for the session's pipeline, as the vault's flags allow: a claim of a table
    /// no pipeline owns by a session a newer one fenced is refused as fenced, unless
    /// `stale_claims`.
    fn claimed(&self, store: &mut VaultStore, table: &str) -> Result<()> {
        let owned = store.owners.contains_key(table);
        let current = self.config.epoch(store, &self.stores, &self.pipeline);
        if !owned && current != self.epoch && !self.config.stale_claims {
            return Err(ConnectorError::fenced("a newer session opened"));
        }
        store
            .claim(&self.pipeline, table, self.config.share_tables)
            .map_err(|error| self.config.refused_as(error))
    }

    /// Applies the state changes of `meta`, to this connection's store with `local_state`.
    fn apply_state(&self, shared: &mut VaultStore, meta: &CommitMeta) {
        let mut local = self.stores.local.lock().unwrap();
        let states = if self.config.local_state {
            &mut local.state
        } else {
            &mut shared.state
        };
        let state = states.entry(self.pipeline.clone()).or_default();
        for change in &meta.state_delta {
            match change {
                StateChange::Put(record) => state.insert(record.key.clone(), record.clone()),
                StateChange::Delete(_) if self.config.ignore_deletes => None,
                StateChange::Delete(key) => state.remove(key),
            };
        }
    }

    /// The refusal of a commit that swaps a generation into, or drops, another pipeline's table,
    /// unless the vault is set to let it.
    fn intruding(&self, store: &VaultStore, meta: &CommitMeta) -> Option<ConnectorError> {
        let swapped = meta
            .finish_generations
            .iter()
            .filter(|_| !self.config.swap_others)
            .filter_map(|(path, _)| store.tables.get(path).cloned());
        let dropped = meta
            .drop_tables
            .iter()
            .filter(|_| !self.config.drop_others)
            .map(|dropped| dropped.name.to_string());
        swapped.chain(dropped).find_map(|name| {
            let owner = store.owners.get(&name)?;
            (*owner != self.pipeline).then(|| {
                self.config
                    .refused_as(ConnectorError::table_owned(&name, owner.as_str()))
            })
        })
    }

    /// Drops the tables `meta` names: their rows, generations, columns and tombstones, and their
    /// owners.
    fn drop_tables(&self, store: &mut VaultStore, meta: &CommitMeta) {
        if self.config.keep_dropped {
            return;
        }
        for dropped in &meta.drop_tables {
            let name = dropped.name.to_string();
            store.published.remove(&name);
            store.generations.retain(|(table, _), _| *table != name);
            store.columns.remove(&name);
            store.tombstones.remove(&name);
            store.tables.retain(|_, table| *table != name);
            if !self.config.keep_owner {
                store.owners.remove(&name);
            }
        }
    }

    /// Publishes the segments of `meta` into tables, generations and merges, and swaps in the
    /// generations it finishes; returns the rows published.
    fn publish(&self, store: &mut VaultStore, meta: &CommitMeta) -> u64 {
        let mut rows = 0;
        let mut merging: BTreeMap<String, Vec<(MergeKey, RecordBatch)>> = BTreeMap::new();
        // Every batch the commit publishes, by table: a child table follows its root's.
        let mut committed: BTreeMap<String, Vec<RecordBatch>> = BTreeMap::new();
        for segment in self.segments_to_publish(store, meta) {
            let key = (self.pipeline.clone(), segment);
            let batches = if self.config.republish {
                store.staged.get(&key).cloned().unwrap_or_default()
            } else {
                store.staged.remove(&key).unwrap_or_default()
            };
            for staged in batches {
                rows += staged.batch.num_rows() as u64;
                committed
                    .entry(staged.table.clone())
                    .or_default()
                    .push(staged.batch.clone());
                match (staged.generation, staged.merge) {
                    (Some(generation), _) if !self.config.replace_early => store
                        .generations
                        .entry((staged.table, generation))
                        .or_default()
                        .push(staged.batch),
                    (_, Some(key)) if !(self.config.merge_appends && key.root.is_none()) => merging
                        .entry(staged.table)
                        .or_default()
                        .push((key, staged.batch)),
                    _ => store
                        .published
                        .entry(staged.table)
                        .or_default()
                        .push(staged.batch),
                }
            }
        }
        self.merge_all(store, merging, &committed, meta);
        let finishing = if self.config.finish_one_generation {
            1
        } else {
            usize::MAX
        };
        for (path, generation) in meta.finish_generations.iter().take(finishing) {
            let Some(table) = store.tables.get(path).cloned() else {
                continue;
            };
            let rows = store
                .generations
                .remove(&(table.clone(), *generation))
                .unwrap_or_default();
            store.generations.retain(|(name, _), _| *name != table);
            if !self.config.keep_replaced_tombstones {
                store.tombstones.remove(&table);
            }
            store.published.insert(table, rows);
        }
        rows
    }

    /// Merges `merging`, the rows the commit publishes into merge tables, into `store`: a child
    /// table follows its root's rows in `committed`, even where the commit staged it nothing.
    fn merge_all(
        &self,
        store: &mut VaultStore,
        mut merging: BTreeMap<String, Vec<(MergeKey, RecordBatch)>>,
        committed: &BTreeMap<String, Vec<RecordBatch>>,
        meta: &CommitMeta,
    ) {
        // A listed child table the commit staged nothing for follows its root all the same.
        for child in &meta.child_tables {
            if !self.config.children_merge_by_key && !self.config.ignore_child_tables {
                merging.entry(child.table.to_string()).or_default();
            }
        }
        for (table, incoming) in merging {
            let listed = meta
                .child_tables
                .iter()
                .find(|child| *child.table == *table);
            let key = incoming
                .first()
                .map(|(key, _)| key.clone())
                .or_else(|| listed.map(|child| child.merge.clone()));
            let published = store.published.entry(table.clone()).or_default();
            match key {
                Some(key) if key.root.is_none() && key.history.is_some() => {
                    let history = key.history.as_ref().expect("a history table's key");
                    let batches: Vec<RecordBatch> =
                        incoming.into_iter().map(|(_, batch)| batch).collect();
                    let tombstones = store.tombstones.entry(table).or_default();
                    history::merge_history(
                        published,
                        tombstones,
                        &batches,
                        (&key, history),
                        self.config.history_flaws(),
                    );
                }
                Some(key) if key.root.is_none() && key.changes.is_some() => {
                    let changes = key.changes.as_ref().expect("a change stream's key");
                    let batches: Vec<RecordBatch> =
                        incoming.into_iter().map(|(_, batch)| batch).collect();
                    let mut own = self.tombstones.lock().unwrap();
                    let tombstones = if self.config.session_tombstones {
                        own.entry(table).or_default()
                    } else {
                        store.tombstones.entry(table).or_default()
                    };
                    changes::merge_changes(
                        published,
                        tombstones,
                        &batches,
                        (&key, changes),
                        self.config.flaws(),
                    );
                }
                Some(key) if key.root.is_some() && !self.config.children_merge_by_key => {
                    let root = key.root.clone().expect("a child table names its root");
                    let root_rows = committed.get(root.table.as_ref()).cloned();
                    let root_rows = root_rows.unwrap_or_default();
                    replace_children(published, &incoming, &root, &root_rows, &key);
                }
                _ => merge(published, incoming, self.config.winner()),
            }
        }
    }

    /// With `refuse_unstaged`, the first segment of `meta` this pipeline never staged.
    fn refused_segment(&self, store: &VaultStore, meta: &CommitMeta) -> Option<SegmentId> {
        let staged = |segment: &SegmentId| {
            store
                .staged
                .contains_key(&(self.pipeline.clone(), *segment))
        };
        self.config
            .refuse_unstaged
            .then(|| meta.segments.iter().find(|segment| !staged(segment)))
            .flatten()
    }

    /// The segments `meta` commits, or with `publish_all` every segment the pipeline staged.
    fn segments_to_publish(&self, store: &VaultStore, meta: &CommitMeta) -> Vec<SegmentId> {
        let staged = store
            .staged
            .keys()
            .filter(|(staged, _)| *staged == self.pipeline)
            .map(|(_, segment)| *segment);
        if self.config.publish_all {
            staged.collect()
        } else if self.config.publish_other {
            let others: Vec<SegmentId> = staged
                .filter(|segment| !meta.segments.contains(*segment))
                .collect();
            if others.is_empty() {
                meta.segments.iter().collect()
            } else {
                let listed = usize::try_from(meta.segments.len()).unwrap_or(usize::MAX);
                others.into_iter().take(listed).collect()
            }
        } else {
            meta.segments.iter().collect()
        }
    }
}

impl Session for VaultSession {
    type Writer = VaultWriter;

    async fn apply_schema(&mut self, change: &TableChange) -> Result<()> {
        // It keeps to the identifiers it declares: ASCII word characters.
        if let TableChange::Create { schema, .. } = change
            && let Some(field) = schema.fields().iter().find(|field| {
                !field
                    .name()
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
            })
        {
            let message = format!("column {} breaks the identifier rules", field.name());
            return Err(ConnectorError::data(message));
        }
        let mut store = self.stores.shared.lock().unwrap();
        self.claimed(&mut store, &change.table().name)?;
        let first = store.changes.insert(format!("{change:?}"));
        let alters = !matches!(change, TableChange::Create { .. });
        if self.config.refuse_repeated_changes && alters && !first {
            return Err(ConnectorError::data("the change was already applied"));
        }
        if !self.config.accept_conflicts
            && let Some(conflict) = conflict(&store, change)
        {
            let error = ConnectorError::data(conflict);
            if self.config.uncoded_conflicts {
                return Err(error);
            }
            return Err(error.with_code("schema_conflict"));
        }
        record(&mut store, change);
        match change {
            TableChange::AddColumn { table, field } if self.config.ignore_added_columns => {
                store
                    .ignored
                    .insert((table.name.to_string(), field.name().to_owned()));
            }
            TableChange::Widen { .. } if self.config.refuse_widening => {
                return Err(ConnectorError::data("columns never widen here"));
            }
            _ => {}
        }
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<VaultWriter> {
        {
            let mut store = self.stores.shared.lock().unwrap();
            let claimed = self.claimed(&mut store, &table.name);
            let intruded = claimed
                .as_ref()
                .is_err_and(|error| error.kind() != ConnectorErrorKind::Fenced);
            if intruded && self.config.lock_on_intrusion {
                store.locked.insert(table.name.to_string());
            }
            claimed?;
            if store.locked.contains(&*table.name) {
                return Err(ConnectorError::data("the table is locked"));
            }
            store
                .tables
                .insert(table.path.clone(), table.name.to_string());
        }
        Ok(VaultWriter {
            id: WRITERS.fetch_add(1, Ordering::SeqCst),
            config: Arc::clone(&self.config),
            stores: self.stores.clone(),
            pipeline: self.pipeline.clone(),
            epoch: self.epoch,
            table: table.name.to_string(),
            generation: table.generation,
            merge: table.merge.clone(),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        if !self.config.keep_staging {
            self.stores
                .shared
                .lock()
                .unwrap()
                .staged
                .retain(|(pipeline, _), _| *pipeline != self.pipeline);
        }
        Ok(())
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        let mut store = self.stores.shared.lock().unwrap();
        let current = self.config.epoch(&store, &self.stores, &self.pipeline);
        if !self.config.no_fence && current != self.epoch {
            let error = if self.config.wrong_fence_kind {
                ConnectorError::data("stale")
            } else {
                ConnectorError::fenced("stale")
            };
            return Err(error);
        }
        let key = (meta.load_id, meta.commit_seq);
        let recall = !self.config.republish && !self.config.forget_receipts;
        if let Some(receipt) = store.receipts.get(&key).filter(|_| recall) {
            return Ok(receipt.clone());
        }
        if let Some(segment) = self.refused_segment(&store, meta) {
            return Err(ConnectorError::data(format!(
                "segment {segment} is not staged"
            )));
        }
        if let Some(error) = self.intruding(&store, meta) {
            return Err(error);
        }
        let rows = self.publish(&mut store, meta);
        self.drop_tables(&mut store, meta);
        if !self.config.forget_state {
            self.apply_state(&mut store, meta);
        }
        let rows = if self.config.miscount { rows + 1 } else { rows };
        let receipt = Receipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
            committed_at: UNIX_EPOCH,
            rows,
            bytes: 0,
        };
        store.receipts.insert(key, receipt.clone());
        Ok(receipt)
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}

impl VaultConfig {
    /// Why a vault with this configuration refuses `batch` for a table of `columns`, if it does.
    fn refusal(
        &self,
        columns: Option<&BTreeMap<String, LogicalType>>,
        batch: &RecordBatch,
    ) -> Option<&'static str> {
        let schema = batch.schema();
        if self.refuse_dictionaries
            && schema
                .fields()
                .iter()
                .any(|field| matches!(field.data_type(), DataType::Dictionary(..)))
        {
            return Some("the batch holds a dictionary");
        }
        if self.decode_only_strings
            && schema.fields().iter().any(|field| {
                matches!(field.data_type(), DataType::Dictionary(_, value) if **value != DataType::Utf8)
            })
        {
            return Some("the batch holds a dictionary of other values than strings");
        }
        let narrower = columns.is_some_and(|columns| {
            schema.fields().iter().any(|field| {
                let stored = match field.data_type() {
                    DataType::Dictionary(_, value) => value.as_ref(),
                    other => other,
                };
                columns
                    .get(field.name())
                    .is_some_and(|column| column.to_arrow() != *stored)
            })
        });
        (self.refuse_narrower && narrower).then_some("the batch does not match the table")
    }
}

impl VaultWriter {
    /// `batch`, as the flags that break names and lanes stage it in `store`.
    fn faulted(&self, store: &mut VaultStore, batch: RecordBatch) -> RecordBatch {
        let batch = if self.config.blank_names {
            blanked_names(&batch)
        } else {
            batch
        };
        if self.config.lose_lanes {
            for ((pipeline, _), staged) in &mut store.staged {
                if *pipeline == self.pipeline {
                    staged.retain(|staged| {
                        staged.table != self.table
                            || staged.generation != self.generation
                            || staged.writer == self.id
                    });
                }
            }
        }
        if !self.config.fold_names {
            return batch;
        }
        let fields: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_name(field.name().to_lowercase())
            })
            .collect();
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        RecordBatch::try_new(schema, batch.columns().to_vec()).expect("same columns")
    }
}

impl TableWriter for VaultWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        let mut store = self.stores.shared.lock().unwrap();
        let current = self.config.epoch(&store, &self.stores, &self.pipeline);
        if !self.config.stale_writes && current != self.epoch {
            return Err(ConnectorError::fenced("stale"));
        }
        let Some(columns) = store.columns.get(&self.table) else {
            return Err(ConnectorError::data("no table of that name was created"));
        };
        if let Some(refusal) = self.config.refusal(Some(columns), &batch) {
            return Err(ConnectorError::data(refusal));
        }
        let dropped: Vec<usize> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                store
                    .ignored
                    .contains(&(self.table.clone(), field.name().clone()))
            })
            .map(|(index, _)| index)
            .collect();
        let kept: Vec<usize> = (0..batch.num_columns())
            .filter(|index| !dropped.contains(index))
            .collect();
        let batch = self.faulted(
            &mut store,
            batch.project(&kept).expect("kept columns exist"),
        );
        if self.config.publish_on_write {
            store
                .published
                .entry(self.table.clone())
                .or_default()
                .push(batch.clone());
        }
        let staged = store
            .staged
            .entry((self.pipeline.clone(), segment))
            .or_default();
        if self.config.one_table_per_segment {
            staged.retain(|staged| staged.table == self.table);
        }
        staged.push(Staged {
            table: self.table.clone(),
            writer: self.id,
            generation: self.generation,
            merge: self.merge.clone(),
            batch,
        });
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        if self.config.hang_flush {
            std::future::pending::<()>().await;
        }
        Ok(WriteStats::default())
    }
}

/// Replaces the children of the roots `root_rows` publish: published rows of those roots go, and
/// of `incoming`, the rows of each root's winning row stay.
fn replace_children(
    published: &mut Vec<RecordBatch>,
    incoming: &[(MergeKey, RecordBatch)],
    root: &RootKey,
    root_rows: &[RecordBatch],
    key: &MergeKey,
) {
    let bytes = |batch: &RecordBatch, column: &str, row: usize| -> Vec<u8> {
        let values = batch
            .column_by_name(column)
            .expect("lineage columns are there");
        values.as_binary::<i32>().value(row).to_vec()
    };
    let mut winners: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for batch in root_rows {
        for row in 0..batch.num_rows() {
            let (id, seq) = (bytes(batch, &root.id, row), bytes(batch, &root.seq, row));
            if winners.get(&id).is_none_or(|best| *best < seq) {
                winners.insert(id, seq);
            }
        }
    }
    let of = |batch: &RecordBatch, row: usize| {
        (
            bytes(batch, &key.columns[0], row),
            bytes(batch, &key.seq, row),
        )
    };
    let mut kept = Vec::new();
    for batch in published.iter() {
        for row in 0..batch.num_rows() {
            if !winners.contains_key(&of(batch, row).0) {
                kept.push(batch.slice(row, 1));
            }
        }
    }
    for (_, batch) in incoming {
        for row in 0..batch.num_rows() {
            let (id, seq) = of(batch, row);
            if winners.get(&id) == Some(&seq) {
                kept.push(batch.slice(row, 1));
            }
        }
    }
    *published = kept;
}

/// Which of a key's incoming rows a merge keeps.
#[derive(Clone, Copy)]
enum Winner {
    /// The greatest sequence's, as a merge must.
    Newest,
    /// The first met.
    First,
    /// The last met.
    Last,
}

impl VaultConfig {
    fn flaws(&self) -> changes::Flaws {
        changes::Flaws {
            ignore_seq_guard: self.ignore_seq_guard,
            forget_tombstones: self.forget_tombstones,
            truncate_everything: self.truncate_everything,
            hard_on_soft: self.hard_on_soft,
            drop_unchanged: self.drop_unchanged,
        }
    }

    fn history_flaws(&self) -> history::Flaws {
        history::Flaws {
            overwrite: self.history_overwrites,
            duplicate: self.history_duplicates,
            stay_current: self.history_stays_current,
            ignore_seq: self.history_ignores_seq,
            keep_deleted: self.history_keeps_deleted,
            ignore_truncates: self.history_ignores_truncates,
            soft_keeps_seq: self.history_soft_keeps_seq,
            spare_commit: self.history_truncate_spares_commit,
            drop_hash: self.history_drops_hash,
        }
    }

    fn winner(&self) -> Winner {
        if self.merge_keeps_first {
            Winner::First
        } else if self.merge_keeps_last {
            Winner::Last
        } else {
            Winner::Newest
        }
    }
}

/// Merges `incoming` into `published` a row at a time: an incoming row replaces the published
/// row with its key, and among incoming rows of one key `winner` says which wins.
fn merge(published: &mut Vec<RecordBatch>, incoming: Vec<(MergeKey, RecordBatch)>, winner: Winner) {
    let Some(key) = incoming.first().map(|(key, _)| key.clone()) else {
        return;
    };
    let key_of = |row: &RecordBatch| -> Vec<String> {
        key.columns
            .iter()
            .map(|column| {
                let values = row
                    .column_by_name(column)
                    .expect("merge rows carry their key");
                arrow_cast::display::array_value_to_string(values, 0).unwrap()
            })
            .collect()
    };
    let seq_of = |row: &RecordBatch| -> Vec<u8> {
        let seq = row
            .column_by_name(&key.seq)
            .expect("merge rows carry a sequence");
        seq.as_binary::<i32>().value(0).to_vec()
    };
    let rows = |batches: &[RecordBatch]| -> Vec<RecordBatch> {
        batches
            .iter()
            .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
            .collect()
    };
    let mut winners: BTreeMap<Vec<String>, RecordBatch> = BTreeMap::new();
    let batches: Vec<RecordBatch> = incoming.into_iter().map(|(_, batch)| batch).collect();
    for row in rows(&batches) {
        let kept = winners.get(&key_of(&row)).is_some_and(|best| match winner {
            Winner::Newest => seq_of(best) >= seq_of(&row),
            Winner::First => true,
            Winner::Last => false,
        });
        if !kept {
            winners.insert(key_of(&row), row);
        }
    }
    let mut kept: Vec<RecordBatch> = rows(published)
        .into_iter()
        .filter(|row| !winners.contains_key(&key_of(row)))
        .collect();
    kept.extend(winners.into_values());
    *published = kept;
}

/// Why `change` conflicts with the table's columns, if it does: a column it names already has
/// another type.
fn conflict(store: &VaultStore, change: &TableChange) -> Option<String> {
    let columns = store.columns.get(change.table().name.as_ref())?;
    let clash = |name: &str, declared: &LogicalType| {
        columns
            .get(name)
            .filter(|existing| existing.join(declared) != **existing)
            .map(|existing| format!("column {name} is {existing}"))
    };
    match change {
        TableChange::Create { schema, .. } => schema
            .fields()
            .iter()
            .find_map(|field| clash(field.name(), field.logical_type())),
        TableChange::AddColumn { field, .. } => clash(field.name(), field.logical_type()),
        TableChange::Widen { .. } => None,
    }
}

/// Records what `change` leaves the table's columns as.
fn record(store: &mut VaultStore, change: &TableChange) {
    let columns = store
        .columns
        .entry(change.table().name.to_string())
        .or_default();
    match change {
        TableChange::Create { schema, .. } => {
            for field in schema.fields().iter() {
                columns
                    .entry(field.name().to_owned())
                    .or_insert_with(|| field.logical_type().clone());
            }
        }
        TableChange::AddColumn { field, .. } => {
            columns
                .entry(field.name().to_owned())
                .or_insert_with(|| field.logical_type().clone());
        }
        TableChange::Widen { column, to, .. } => {
            let held = columns
                .entry(column.to_string())
                .or_insert_with(|| to.clone());
            *held = held.join(to);
        }
    }
}

/// `batch` with its `name` column, if it has one, all nulls.
fn blanked_names(batch: &RecordBatch) -> RecordBatch {
    let (fields, columns): (Vec<_>, Vec<_>) = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            if field.name() == "name" {
                let nulls = arrow_array::new_null_array(field.data_type(), column.len());
                (field.as_ref().clone().with_nullable(true), nulls)
            } else {
                (field.as_ref().clone(), Arc::clone(column))
            }
        })
        .unzip();
    let schema = Arc::new(arrow_schema::Schema::new(fields));
    RecordBatch::try_new(schema, columns).expect("nulls fit a nullable column")
}

struct VaultProbe(SharedVault);

impl Probe for VaultProbe {
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        let batches = self
            .0
            .lock()
            .unwrap()
            .published
            .get(table.name.as_ref())
            .cloned()
            .unwrap_or_default();
        Box::pin(async move { Ok(batches) })
    }
}

async fn certify_vault(name: &str, flag: Option<&str>) -> Report {
    let mut config = json!({ "store": name });
    if let Some(flag) = flag {
        config[flag] = json!(true);
    }
    certify_destination::<Vault>(config, &VaultProbe(vault(name))).await
}

#[tokio::test]
async fn the_lanes_clause_does_not_apply_to_one_writer_and_short_names_leave_names_unobserved() {
    let cases = [
        (json!({ "writers": 1 }), "D-LANES", false),
        (json!({ "writers": 2 }), "D-LANES", true),
        (json!({ "identifier_len": 31 }), "D-NAMES", false),
        (json!({ "identifier_len": 32 }), "D-NAMES", true),
    ];
    for (mut config, clause, runs) in cases {
        config["store"] = json!(format!("{clause}_{runs}"));
        let store = config["store"].as_str().expect("a store name").to_owned();
        let report = certify_destination::<Vault>(config, &VaultProbe(vault(&store))).await;
        // The engine never runs two writers where one is declared; it names tables and
        // columns whatever their length, which the clause's own names do not fit.
        let expected = match (runs, report.outcome(clause)) {
            (true, outcome) => outcome == Some(&Outcome::Passed),
            (false, Some(Outcome::Inapplicable(_))) => clause == "D-LANES",
            (false, Some(Outcome::Unobserved(_))) => clause == "D-NAMES",
            (false, _) => false,
        };
        assert!(expected, "{clause} runs: {runs}: {report}");
    }
}

#[tokio::test]
async fn a_destination_reserving_every_form_of_a_name_fails_the_names_clause_alone() {
    let config = json!({ "store": "reserved", "identifier_len": 40, "reserve_ids": true });
    let report = certify_destination::<Vault>(config, &VaultProbe(vault("reserved"))).await;
    assert_eq!(failed(&report), ["D-NAMES"], "{report}");
}

#[tokio::test]
async fn a_correct_destination_passes_every_clause() {
    let report = certify_vault("correct", None).await;
    report.assert_passed();
    for id in ["D-NAMES", "D-LANES", "D-HIST"] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
}

/// Each flag that breaks one behavior, with the clauses it fails.
const BROKEN: &[(&str, &[&str])] = &[
    ("history_overwrites", &["D-HIST"]),
    ("history_duplicates", &["D-HIST"]),
    ("history_stays_current", &["D-HIST"]),
    ("history_ignores_seq", &["D-HIST"]),
    ("history_keeps_deleted", &["D-HIST"]),
    ("history_ignores_truncates", &["D-HIST"]),
    ("history_soft_keeps_seq", &["D-HIST"]),
    ("history_truncate_spares_commit", &["D-HIST"]),
    ("history_drops_hash", &["D-HIST"]),
    ("static_epoch", &["D-EPOCH"]),
    ("fold_names", &["D-NAMES"]),
    ("refuse_check", &["D-CHECK"]),
    ("lose_lanes", &["D-LANES"]),
    ("keep_dropped", &["D-DROP"]),
    ("keep_owner", &["D-DROP"]),
    ("drop_others", &["D-DROP"]),
    ("swap_others", &["D-OWNED"]),
    ("stale_claims", &["D-DROP"]),
    ("miscount", &["D-COMMIT"]),
    ("republish", &["D-IDEMPOTENT"]),
    ("forget_state", &["D-STATE"]),
    ("ignore_deletes", &["D-STATE"]),
    ("keep_staging", &["D-DISCARD"]),
    ("no_fence", &["D-FENCE"]),
    ("wrong_fence_kind", &["D-FENCE"]),
    ("forget_receipts", &["D-IDEMPOTENT"]),
    ("publish_all", &["D-COMMIT"]),
    (
        "blank_names",
        &[
            "D-COMMIT",
            "D-IDEMPOTENT",
            "D-DISCARD",
            "D-REPLACE",
            "D-MERGE",
            "D-DELETE",
            "D-PARTIAL",
            "D-TRUNCATE",
            "D-HIST",
            "D-ENCODING",
            "D-TABLES",
            "D-LANES",
            "D-OWNED",
            "D-DROP",
        ],
    ),
    (
        "publish_other",
        &["D-COMMIT", "D-REPLACE", "D-MERGE", "D-CHILDREN"],
    ),
    ("local_state", &["D-STATE"]),
    ("stale_writes", &["D-DISCARD"]),
    ("replace_early", &["D-REPLACE", "D-DELETE"]),
    (
        "merge_appends",
        &[
            "D-MERGE",
            "D-DELETE",
            "D-PARTIAL",
            "D-TRUNCATE",
            "D-HIST",
            "D-CHILDREN",
        ],
    ),
    ("merge_keeps_first", &["D-MERGE"]),
    ("merge_keeps_last", &["D-MERGE"]),
    ("refuse_repeated_changes", &["D-SCHEMA"]),
    ("ignore_added_columns", &["D-SCHEMA"]),
    ("accept_conflicts", &["D-SCHEMA"]),
    ("refuse_narrower", &["D-SCHEMA"]),
    ("uncoded_conflicts", &["D-SCHEMA"]),
    ("refuse_widening", &["D-SCHEMA"]),
    ("refuse_dictionaries", &["D-ENCODING"]),
    ("decode_only_strings", &["D-ENCODING"]),
    (
        "one_table_per_segment",
        &["D-REPLACE", "D-CHILDREN", "D-TABLES"],
    ),
    ("children_merge_by_key", &["D-CHILDREN"]),
    ("ignore_child_tables", &["D-CHILDREN"]),
    ("finish_one_generation", &["D-REPLACE"]),
    ("ignore_seq_guard", &["D-MERGE", "D-DELETE", "D-TRUNCATE"]),
    ("forget_tombstones", &["D-DELETE", "D-TRUNCATE"]),
    ("truncate_everything", &["D-TRUNCATE"]),
    ("hard_on_soft", &["D-DELETE", "D-TRUNCATE"]),
    ("drop_unchanged", &["D-PARTIAL"]),
    ("session_tombstones", &["D-DELETE", "D-TRUNCATE"]),
    ("keep_replaced_tombstones", &["D-DELETE"]),
];

#[tokio::test]
async fn each_broken_destination_behavior_fails_exactly_its_clauses() {
    for (flag, clauses) in BROKEN {
        let report = certify_vault(flag, Some(flag)).await;
        assert_eq!(failed(&report), *clauses, "{flag}: {report}");
    }
}

#[tokio::test]
async fn change_clauses_check_only_what_a_destination_declares_it_does() {
    // Each broken behavior goes unchecked where the destination declares it does not do it.
    let cases: [(&[&str], &[&str]); 7] = [
        (&["no_replace", "keep_replaced_tombstones"], &["D-REPLACE"]),
        (&["no_drops", "keep_dropped"], &["D-DROP"]),
        (
            &["no_change_merges", "ignore_seq_guard"],
            &["D-DELETE", "D-PARTIAL", "D-TRUNCATE"],
        ),
        (&["hard_deletes_only", "hard_on_soft"], &[]),
        (&["soft_deletes_only", "forget_tombstones"], &[]),
        (&["no_partial_updates", "drop_unchanged"], &["D-PARTIAL"]),
        (
            &[
                "hard_deletes_only",
                "soft_deletes_only",
                "truncate_everything",
            ],
            &["D-DELETE", "D-TRUNCATE"],
        ),
    ];
    for (flags, inapplicable) in cases {
        let name = flags.join("+");
        let mut config = json!({ "store": name });
        for flag in flags {
            config[flag] = json!(true);
        }
        let report = certify_destination::<Vault>(config, &VaultProbe(vault(&name))).await;
        report.assert_passed();
        let actual: Vec<&str> = report
            .results
            .iter()
            .filter(|result| matches!(result.outcome, Outcome::Inapplicable(_)))
            .map(|result| result.clause.id)
            .collect();
        assert_eq!(actual, inapplicable, "{name}: {report}");
    }
}

#[tokio::test]
async fn each_way_of_letting_pipelines_meet_at_a_table_fails_d_owned() {
    // A refusal of the wrong kind or without its code fails the drop of another pipeline's table
    // too.
    let cases: [(&str, &[&str]); 4] = [
        ("share_tables", &["D-OWNED"]),
        ("lock_on_intrusion", &["D-OWNED"]),
        ("owned_as_data", &["D-OWNED", "D-DROP"]),
        ("owned_uncoded", &["D-OWNED", "D-DROP"]),
    ];
    for (flag, clauses) in cases {
        let report = certify_vault(flag, Some(flag)).await;
        assert_eq!(failed(&report), clauses, "{flag}: {report}");
    }
}

#[tokio::test]
async fn a_destination_nothing_reads_back_is_still_certified_to_refuse_other_pipelines() {
    for (flag, outcome) in [(None, true), (Some("share_tables"), false)] {
        let mut config = json!({ "store": format!("unprobed_{flag:?}") });
        if let Some(flag) = flag {
            config[flag] = json!(true);
        }
        let report = certify_destination::<Vault>(config, &Unprobed).await;
        let passed = matches!(report.outcome("D-OWNED"), Some(Outcome::Passed));
        assert_eq!(passed, outcome, "{flag:?}: {report}");
    }
}

#[tokio::test]
async fn epochs_kept_by_one_connection_fail_the_clauses_that_span_two() {
    let report = certify_vault("local_epoch", Some("local_epoch")).await;
    assert_eq!(
        failed(&report),
        ["D-EPOCH", "D-DISCARD", "D-FENCE"],
        "{report}"
    );
}

#[tokio::test]
async fn a_destination_may_refuse_to_commit_segments_it_never_staged() {
    certify_vault("refuse_unstaged", Some("refuse_unstaged"))
        .await
        .assert_passed();
}

#[tokio::test]
async fn certification_passes_again_against_the_same_store() {
    certify_vault("rerun", None).await.assert_passed();
    certify_vault("rerun", None).await.assert_passed();
}

#[tokio::test]
async fn visible_staging_fails_every_clause_that_reads_published_data() {
    let report = certify_vault("publish_on_write", Some("publish_on_write")).await;
    assert_eq!(
        failed(&report),
        [
            "D-STAGING",
            "D-COMMIT",
            "D-IDEMPOTENT",
            "D-DISCARD",
            "D-REPLACE",
            "D-SCHEMA",
            "D-MERGE",
            "D-DELETE",
            "D-PARTIAL",
            "D-TRUNCATE",
            "D-HIST",
            "D-ENCODING",
            "D-TABLES",
            "D-NAMES",
            "D-LANES",
            "D-OWNED",
            "D-DROP",
            "D-FENCE"
        ],
        "{report}"
    );
}

#[tokio::test]
async fn a_destination_that_cannot_connect_fails_every_clause() {
    let report = certify_vault("refused", Some("refuse_connect")).await;
    assert_eq!(failed(&report).len(), DESTINATION_CLAUSES.len());
}

#[tokio::test]
async fn clauses_for_capabilities_a_destination_lacks_do_not_apply() {
    let report = certify_vault("minimal", Some("minimal")).await;
    report.assert_passed();
    for clause in ["D-REPLACE", "D-MERGE", "D-HIST"] {
        assert!(
            matches!(report.outcome(clause), Some(Outcome::Inapplicable(_))),
            "{clause}: {report}"
        );
    }
    assert_eq!(
        report.outcome("D-SCHEMA"),
        Some(&Outcome::Passed),
        "minimal destinations add columns"
    );
}

#[tokio::test]
async fn the_schema_clause_does_not_apply_to_a_destination_that_changes_no_schema() {
    let report = certify_vault("fixed_schema", Some("fixed_schema")).await;
    report.assert_passed();
    assert!(
        matches!(report.outcome("D-SCHEMA"), Some(Outcome::Inapplicable(_))),
        "{report}"
    );
}

#[tokio::test]
async fn s_ack_does_not_apply_to_a_source_that_tells_nothing_of_where_it_stands() {
    let report = certify_source::<Pages>(json!({})).await;
    let outcome = report.outcome("S-ACK");
    assert!(
        matches!(outcome, Some(Outcome::Inapplicable(_))),
        "{report}"
    );
}

#[test]
fn a_report_that_did_not_pass_panics_with_text_a_terminal_does_not_obey() {
    let hostile = "db said: \u{1b}[2J\u{1b}[H\n  pass S-CHECK\n  pass S-RESUME\u{1b}]52;c;ZXZpbA==\u{7}\u{202e}";
    let report = Report {
        connector: hostile.to_owned(),
        results: vec![ClauseResult {
            clause: SOURCE_CLAUSES[0],
            outcome: Outcome::Failed(hostile.into()),
            note: Some(hostile.into()),
        }],
    };
    let shown = report.to_string();
    let obeyed = |text: &str| {
        text.chars()
            .any(|c| c != '\n' && (c.is_control() || !c.is_ascii()))
    };
    assert!(!obeyed(&shown), "{shown:?}");
    // The connector, the clause and the verdict: the reason drew no line that reads as a pass.
    assert_eq!(shown.lines().count(), 3, "{shown}");
    for assert in [Report::assert_passed, Report::assert_none_failed] {
        let panic = std::panic::catch_unwind(|| assert(&report)).expect_err("it did not pass");
        let message = panic.downcast_ref::<String>().expect("a message");
        assert_eq!(*message, shown);
    }
}
