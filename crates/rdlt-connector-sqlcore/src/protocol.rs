//! The load-commit protocol planner: the correctness-critical half of the
//! commit unit. [`commit_script`] is a PURE function — no driver types cross
//! into it (Principle III) — that decides, from the session tables, the
//! destination options, and the transaction facts a destination has already
//! gathered, the exact ordered [`Step`] program a publish executes. The
//! destinations own only EXECUTION: they run each step's SQL through their own
//! connection + [`crate::dialect::MergeDialect`] seam.
//!
//! The single-unit discipline and scope-replacement ordering are planner
//! decisions here; an executor may not reorder or re-decide them. Golden pins
//! (the tests below) freeze the emitted script for the representative plan
//! matrix.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use rdlt_connector::WriteMode;
use rdlt_connector::core::{TableName, TableSchema, schema::system_columns};

use crate::dialect::MergeDialect;
use crate::options::{DestOptions, MergeStrategy, Scd2Options};
use crate::plan::{
    HardDelete, MergeCtx, MergePlan, identity_delete_insert_sql, keyed_delete_insert_sql,
    keyed_upsert_sql, scd2_merge_sql, single_unit_violation,
};

/// One executable step of a commit publish, in emitted order. Every step names
/// the table it acts on (except the whole-unit state/receipt steps); the
/// executor renders each step's SQL through its dialect and runs it in the
/// publish transaction. The strategy arm carries its RESOLVED shape, never raw
/// SQL — the text is still produced through the dialect seam at execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Clear a Replace target before its insert — emitted only for the FIRST
    /// commit unit of a load (durable once-per-load guard).
    ClearTarget { table: TableName },
    /// Append/Replace publish: by-name `INSERT … SELECT` of the staged rows.
    InsertSelect { table: TableName },
    /// Scope replacement before a merge arm (non-scd2 scoped merge): delete the
    /// delivered scopes from the target.
    ScopeReplace {
        table: TableName,
        scope: Vec<String>,
    },
    /// A resolved merge strategy arm.
    MergeArm { table: TableName, arm: MergeArm },
    /// Truncate a stage table — after ALL tables publish (child merges read the
    /// root's stage), and on the replay path.
    TruncateStage { table: TableName },
    /// Upsert the state document (fresh unit only — same transaction as data).
    UpsertState,
    /// Insert the commit receipt (fresh unit only) — the idempotence key.
    InsertReceipt,
}

/// A resolved merge strategy arm — the planner's arm selection, with the scd2
/// settings resolved BY CONSTRUCTION so the executor never re-derives them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeArm {
    /// Keyed structured delete-insert.
    KeyedDeleteInsert,
    /// Keyed structured upsert (conflict-update on the merge key).
    KeyedUpsert,
    /// Shredded identity delete-insert (subtree replacement by root id).
    IdentityDeleteInsert,
    /// SCD2 retire-then-insert with the resolved settings.
    Scd2(Scd2Options),
}

/// The transaction facts a destination gathers before planning: whether this
/// exact `(load_id, commit_seq)` already committed, whether any unit of this
/// load committed (the Replace once-per-load guard), which tables the
/// single-unit discipline has already counted this load, and which full-feed
/// tables staged a non-empty unit (see [`staged_probe_targets`]).
#[derive(Debug, Clone, Copy)]
pub struct CommitCtx<'a> {
    pub replayed: bool,
    pub load_committed_before: bool,
    pub single_unit_done: &'a BTreeSet<TableName>,
    pub staged_nonempty: &'a BTreeSet<TableName>,
}

/// The planner's output: the ordered in-transaction step program, plus the
/// tables whose single-unit discipline the executor marks AFTER the transaction
/// commits (a rolled-back unit never counts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitScript {
    pub steps: Vec<Step>,
    pub marks: Vec<TableName>,
}

/// A planner decision that fails the unit — surfaced typed so the destination
/// maps it to a fatal `DestError` at its SPI boundary with the exact current
/// text (the SPI conversion stays at the destination).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    /// A full-feed table (merge_key scope replacement or scd2 `absent: retire`)
    /// published a second non-empty unit within one load.
    SingleUnit { table: String, scoped: bool },
    /// A merge arm impossible for the stream shape (shredded upsert / scd2).
    /// `ensure_table` rejects these, so reaching it means a driver bypassed
    /// validation — kept as a construction guard.
    ArmUnsupported { table: String, what: &'static str },
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitError::SingleUnit { table, scoped } => {
                write!(f, "{}", single_unit_violation(table, *scoped))
            }
            CommitError::ArmUnsupported { table, what } => {
                write!(f, "table `{table}`: {what}")
            }
        }
    }
}

impl std::error::Error for CommitError {}

/// The single-unit discipline decision for one full-feed table this unit.
enum SingleUnit {
    /// Nothing staged this unit — skip the table's publish (an empty stage
    /// must not read as "every key absent" = mass retirement).
    SkipEmpty,
    /// The table already published a non-empty unit this load — rule violated.
    Violation,
    /// First non-empty unit — mark the table done after the tx commits.
    Mark,
}

fn check_single_unit(already_done: bool, staged: bool) -> SingleUnit {
    if !staged {
        SingleUnit::SkipEmpty
    } else if already_done {
        SingleUnit::Violation
    } else {
        SingleUnit::Mark
    }
}

/// The full-feed merge tables whose stage the executor must probe for emptiness
/// before planning — scope replacement and absent-retire read the stage as the
/// complete truth, so an empty stage means "skip this table this unit", not
/// "mass retirement". The executor probes exactly these and passes the
/// non-empty ones in [`CommitCtx::staged_nonempty`]; every other table's stage
/// row count never affects the plan.
pub fn staged_probe_targets<'a>(
    tables: &'a BTreeMap<TableName, (TableSchema, WriteMode)>,
    options: &DestOptions,
) -> Vec<&'a TableName> {
    tables
        .iter()
        .filter(|(table, (_, mode))| {
            matches!(mode, WriteMode::Merge { .. })
                && MergeCtx::resolve(options, table.as_str()).full_feed()
        })
        .map(|(table, _)| table)
        .collect()
}

fn select_arm(
    table: &str,
    has_identity: bool,
    strategy: MergeStrategy,
    options: &DestOptions,
) -> Result<MergeArm, CommitError> {
    Ok(match (has_identity, strategy) {
        (false, MergeStrategy::DeleteInsert) => MergeArm::KeyedDeleteInsert,
        (false, MergeStrategy::Upsert) => MergeArm::KeyedUpsert,
        (true, MergeStrategy::DeleteInsert) => MergeArm::IdentityDeleteInsert,
        // `scd2_for` is TOTAL (it defaults when unset), so the scd2 arm's
        // settings are resolved by construction — no Option to unwrap.
        (false, MergeStrategy::Scd2) => MergeArm::Scd2(options.scd2_for(table)),
        (true, MergeStrategy::Upsert) => {
            return Err(CommitError::ArmUnsupported {
                table: table.to_owned(),
                what: "upsert on a shredded stream",
            });
        }
        (true, MergeStrategy::Scd2) => {
            return Err(CommitError::ArmUnsupported {
                table: table.to_owned(),
                what: "scd2 on a shredded stream",
            });
        }
    })
}

/// Plan the ordered step program for a commit unit — the ONE place the
/// commit-unit decisions and ordering live. Pure: given the same tables,
/// options, and facts, it emits the same script byte-for-byte.
pub fn commit_script(
    tables: &BTreeMap<TableName, (TableSchema, WriteMode)>,
    options: &DestOptions,
    ctx: &CommitCtx<'_>,
) -> Result<CommitScript, CommitError> {
    let mut steps = Vec::new();
    let mut marks = Vec::new();

    if ctx.replayed {
        // Replay of a unit that DID commit server-side: the merge SQL never
        // re-runs, but the single-unit discipline still counts each redelivered
        // full-feed unit, and the stages get truncated.
        for (table, (_, mode)) in tables {
            if !matches!(mode, WriteMode::Merge { .. }) {
                continue;
            }
            let mctx = MergeCtx::resolve(options, table.as_str());
            if mctx.full_feed()
                && !ctx.single_unit_done.contains(table)
                && ctx.staged_nonempty.contains(table)
            {
                marks.push(table.clone());
            }
        }
        for table in tables.keys() {
            steps.push(Step::TruncateStage {
                table: table.clone(),
            });
        }
        return Ok(CommitScript { steps, marks });
    }

    // Fresh unit: publish every table, then truncate stages, then persist state
    // + receipt in the SAME transaction (so state and data become visible
    // atomically).
    for (table, (schema, mode)) in tables {
        // A schema without the per-row identity column is a STRUCTURED stream's
        // table — merge (if requested) goes by key.
        let has_identity = schema.columns.iter().any(|c| c.name == system_columns::ID);
        match mode {
            WriteMode::Append => steps.push(Step::InsertSelect {
                table: table.clone(),
            }),
            WriteMode::Replace => {
                // Truncate at most once per LOAD, guarded durably from the
                // receipt log — a crash-recovery session must never re-truncate
                // rows an earlier commit already published.
                if !ctx.load_committed_before {
                    steps.push(Step::ClearTarget {
                        table: table.clone(),
                    });
                }
                steps.push(Step::InsertSelect {
                    table: table.clone(),
                });
            }
            WriteMode::Merge { .. } => {
                let mctx = MergeCtx::resolve(options, table.as_str());
                // Single-unit discipline, PER TABLE: a unit where this table
                // stages NOTHING is skipped outright; a second non-empty unit
                // violates.
                if mctx.full_feed() {
                    match check_single_unit(
                        ctx.single_unit_done.contains(table),
                        ctx.staged_nonempty.contains(table),
                    ) {
                        SingleUnit::SkipEmpty => continue,
                        SingleUnit::Violation => {
                            return Err(CommitError::SingleUnit {
                                table: table.as_str().to_owned(),
                                // Name the rule that FIRED — under scd2 the
                                // absent-retire rule governs even when a
                                // merge_key scopes it.
                                scoped: mctx.scoped.is_some() && !mctx.retire,
                            });
                        }
                        SingleUnit::Mark => marks.push(table.clone()),
                    }
                }
                // Scope replacement runs BEFORE the strategy arm. NOT for scd2:
                // there the merge_key scopes RETIREMENT inside the arm —
                // deleting scope rows would destroy history.
                if let Some(scope) = mctx.scoped
                    && mctx.strategy != MergeStrategy::Scd2
                {
                    steps.push(Step::ScopeReplace {
                        table: table.clone(),
                        scope: scope.to_vec(),
                    });
                }
                let arm = select_arm(table.as_str(), has_identity, mctx.strategy, options)?;
                steps.push(Step::MergeArm {
                    table: table.clone(),
                    arm,
                });
            }
        }
    }
    // Truncate stages only after ALL tables published: child-table merges read
    // the ROOT's stage for their delete-by-root-id subquery.
    for table in tables.keys() {
        steps.push(Step::TruncateStage {
            table: table.clone(),
        });
    }
    steps.push(Step::UpsertState);
    steps.push(Step::InsertReceipt);
    Ok(CommitScript { steps, marks })
}

// ---- Execution helpers: the destinations render step SQL through these, so
// the by-name publish text and the arm→SQL dispatch each exist once. ----

/// Append/Replace publish text — by-name `INSERT … SELECT`, identical across
/// destinations (no dialect divergence). Operands are already quoted.
pub fn insert_select_sql(target: &str, cols: &str, stage: &str) -> String {
    format!("INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}")
}

/// Assemble the [`MergePlan`] for a merge arm — the shared 10-field shape +
/// hard-delete resolution. The destination supplies its dialect and the
/// (differently-scoped) stage identifiers; every merge decision the plan needs
/// is resolved here from the options.
#[allow(clippy::too_many_arguments)]
pub fn build_merge_plan<'a>(
    dialect: &'a dyn MergeDialect,
    options: &'a DestOptions,
    table: &'a TableName,
    schema: &'a TableSchema,
    key: &'a [String],
    target: &'a str,
    stage: &'a str,
    cols: &'a str,
    root: &TableName,
    root_stage: String,
    root_schema: Option<&TableSchema>,
) -> MergePlan<'a> {
    MergePlan {
        dialect,
        target,
        stage,
        cols,
        schema,
        key,
        root_stage,
        is_child: table != root,
        hard_delete: options
            .hard_delete_for(root.as_str())
            .and_then(|col| Some(HardDelete::new(col, root_schema?, dialect))),
        dedup_sort: options.dedup_sort_for(table.as_str()),
        merge_scope: options.merge_key_for(table.as_str()),
    }
}

/// Render a resolved arm to its statements through the plan's dialect seam.
pub fn render_arm(plan: &MergePlan<'_>, arm: &MergeArm) -> Vec<String> {
    match arm {
        MergeArm::KeyedDeleteInsert => keyed_delete_insert_sql(plan),
        MergeArm::KeyedUpsert => keyed_upsert_sql(plan),
        MergeArm::IdentityDeleteInsert => identity_delete_insert_sql(plan),
        MergeArm::Scd2(scd2) => scd2_merge_sql(plan, scd2),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::{MergeStrategy, TableOptions};
    use rdlt_connector::core::schema::ColumnDef;
    use rdlt_connector::core::{ColumnType, LogicalType, Provenance};

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            ty: ColumnType::scalar(LogicalType::Int64),
            nullable: true,
            provenance: Provenance::Inferred,
        }
    }

    /// A keyed structured schema (no `_rdlt_id`): merge goes by declared key.
    fn keyed_schema(table: &str) -> TableSchema {
        TableSchema {
            table: TableName::new(table),
            parent: None,
            columns: vec![col("id"), col("day"), col("name")],
        }
    }

    fn tables(
        entries: Vec<(&str, TableSchema, WriteMode)>,
    ) -> BTreeMap<TableName, (TableSchema, WriteMode)> {
        entries
            .into_iter()
            .map(|(t, s, m)| (TableName::new(t), (s, m)))
            .collect()
    }

    fn merge(keys: &[&str]) -> WriteMode {
        WriteMode::Merge {
            key: keys.iter().map(|k| k.to_string()).collect(),
        }
    }

    fn ctx<'a>(
        replayed: bool,
        load_committed_before: bool,
        done: &'a BTreeSet<TableName>,
        staged: &'a BTreeSet<TableName>,
    ) -> CommitCtx<'a> {
        CommitCtx {
            replayed,
            load_committed_before,
            single_unit_done: done,
            staged_nonempty: staged,
        }
    }

    fn t(name: &str) -> TableName {
        TableName::new(name)
    }

    fn empty() -> BTreeSet<TableName> {
        BTreeSet::new()
    }

    #[test]
    fn pin_append_only() {
        let tbls = tables(vec![("events", keyed_schema("events"), WriteMode::Append)]);
        let opts = DestOptions::default();
        let (done, staged) = (empty(), empty());
        let script = commit_script(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(
            script.steps,
            vec![
                Step::InsertSelect { table: t("events") },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
        assert!(script.marks.is_empty());
    }

    #[test]
    fn pin_replace_first_and_later_unit() {
        let tbls = tables(vec![("events", keyed_schema("events"), WriteMode::Replace)]);
        let opts = DestOptions::default();
        let (done, staged) = (empty(), empty());
        // First unit of the load: clear the target once.
        let first = commit_script(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(
            first.steps,
            vec![
                Step::ClearTarget { table: t("events") },
                Step::InsertSelect { table: t("events") },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
        // A later unit (load already committed once): NO clear.
        let later = commit_script(&tbls, &opts, &ctx(false, true, &done, &staged)).unwrap();
        assert_eq!(
            later.steps,
            vec![
                Step::InsertSelect { table: t("events") },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
    }

    #[test]
    fn pin_merge_upsert_and_scd2_arms() {
        let opts = DestOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            tables: [(
                "dims".to_string(),
                TableOptions {
                    merge_strategy: Some(MergeStrategy::Scd2),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
        };
        let tbls = tables(vec![
            ("events", keyed_schema("events"), merge(&["id"])),
            ("dims", keyed_schema("dims"), merge(&["id"])),
        ]);
        let (done, staged) = (empty(), empty());
        let script = commit_script(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(
            script.steps,
            vec![
                // BTreeMap order: dims before events.
                Step::MergeArm {
                    table: t("dims"),
                    arm: MergeArm::Scd2(Scd2Options::default()),
                },
                Step::MergeArm {
                    table: t("events"),
                    arm: MergeArm::KeyedUpsert,
                },
                Step::TruncateStage { table: t("dims") },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
    }

    #[test]
    fn pin_scoped_merge_publishes_and_marks_when_staged() {
        let opts = DestOptions {
            tables: [(
                "events".to_string(),
                TableOptions {
                    merge_key: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![("events", keyed_schema("events"), merge(&["id"]))]);
        let done = empty();
        let staged: BTreeSet<TableName> = [t("events")].into_iter().collect();
        let script = commit_script(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(
            script.steps,
            vec![
                Step::ScopeReplace {
                    table: t("events"),
                    scope: vec!["day".to_string()],
                },
                Step::MergeArm {
                    table: t("events"),
                    arm: MergeArm::KeyedDeleteInsert,
                },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
        assert_eq!(script.marks, vec![t("events")]);
    }

    #[test]
    fn pin_scoped_merge_skips_when_stage_empty() {
        let opts = DestOptions {
            tables: [(
                "events".to_string(),
                TableOptions {
                    merge_key: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![("events", keyed_schema("events"), merge(&["id"]))]);
        let (done, staged) = (empty(), empty()); // stage empty this unit
        let script = commit_script(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        // No publish for the skipped table — just the stage truncate + persist.
        assert_eq!(
            script.steps,
            vec![
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
        assert!(script.marks.is_empty());
    }

    #[test]
    fn pin_single_unit_violation() {
        let opts = DestOptions {
            tables: [(
                "events".to_string(),
                TableOptions {
                    merge_key: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![("events", keyed_schema("events"), merge(&["id"]))]);
        let done: BTreeSet<TableName> = [t("events")].into_iter().collect(); // already published
        let staged: BTreeSet<TableName> = [t("events")].into_iter().collect();
        let err = commit_script(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap_err();
        assert_eq!(
            err,
            CommitError::SingleUnit {
                table: "events".to_string(),
                scoped: true,
            }
        );
        // The Display text matches the shared violation message verbatim.
        assert_eq!(err.to_string(), single_unit_violation("events", true));
    }

    #[test]
    fn pin_replayed_unit_truncates_and_recounts() {
        let opts = DestOptions {
            tables: [(
                "events".to_string(),
                TableOptions {
                    merge_key: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![("events", keyed_schema("events"), merge(&["id"]))]);
        let done = empty();
        let staged: BTreeSet<TableName> = [t("events")].into_iter().collect();
        let script = commit_script(&tbls, &opts, &ctx(true, true, &done, &staged)).unwrap();
        // Replay: only stage truncation, no publish/state/receipt; the
        // redelivered full-feed unit is still counted.
        assert_eq!(
            script.steps,
            vec![Step::TruncateStage { table: t("events") }]
        );
        assert_eq!(script.marks, vec![t("events")]);
    }

    #[test]
    fn pin_shredded_upsert_is_unsupported() {
        let mut schema = keyed_schema("events");
        schema.columns.insert(0, col(system_columns::ID));
        let opts = DestOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            ..Default::default()
        };
        let tbls = tables(vec![("events", schema, merge(&[]))]);
        let (done, staged) = (empty(), empty());
        let err = commit_script(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap_err();
        assert_eq!(
            err,
            CommitError::ArmUnsupported {
                table: "events".to_string(),
                what: "upsert on a shredded stream",
            }
        );
    }
}
