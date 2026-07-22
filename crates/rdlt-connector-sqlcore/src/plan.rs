//! The shared merge shapes (contract SM1): survivor selection, scope
//! replacement, strategy arms, hard-delete decisions, open-time validation,
//! index plans, and the single-commit-unit rule. MOVED VERBATIM from the
//! postgres destination's `dest/commit.rs` (features 006/008/010); every
//! statement's text is produced through the [`MergeDialect`] seam and pinned
//! byte-for-byte by the postgres golden-SQL suite (SM4).

use rdlt_connector::core::{TableSchema, schema::system_columns};

use crate::dialect::MergeDialect;
use crate::options::{DedupSort, DestOptions, MergeStrategy, Scd2Options, SortOrder};

/// Hard-delete flag semantics (M4): boolean columns compare `IS TRUE`,
/// other types `IS NOT NULL` — both NULL-safe on the KEEP side.
pub struct HardDelete {
    pub(crate) flagged: String,
    pub(crate) keep: String,
}

impl HardDelete {
    pub fn new(column: &str, root_schema: &TableSchema, dialect: &dyn MergeDialect) -> Self {
        use rdlt_connector::core::{ColumnType, LogicalType};
        let is_bool = matches!(
            root_schema.column(column).map(|c| &c.ty),
            Some(ColumnType::Scalar {
                scalar: LogicalType::Bool
            })
        );
        let col = dialect.quote(column);
        if is_bool {
            Self {
                flagged: format!("{col} IS TRUE"),
                keep: format!("{col} IS NOT TRUE"),
            }
        } else {
            Self {
                flagged: format!("{col} IS NOT NULL"),
                keep: format!("{col} IS NULL"),
            }
        }
    }
}

/// Everything a strategy arm needs about one table's publish.
pub struct MergePlan<'a> {
    pub dialect: &'a dyn MergeDialect,
    pub target: &'a str,
    pub stage: &'a str,
    pub cols: &'a str,
    pub schema: &'a TableSchema,
    pub key: &'a [String],
    pub root_stage: String,
    pub is_child: bool,
    pub hard_delete: Option<HardDelete>,
    /// Feature 010 (MR1): ordered in-load survivor selection; None keeps
    /// arrival-order last-wins.
    pub dedup_sort: Option<&'a DedupSort>,
}

impl MergePlan<'_> {
    fn quote(&self, ident: &str) -> String {
        self.dialect.quote(ident)
    }

    fn key_list(&self) -> String {
        self.key
            .iter()
            .map(|k| self.quote(k))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// In-batch dedup over the stage: arrival-order last-wins (finding #7),
    /// or — feature 010 MR1 — ordered survivor selection when `dedup_sort`
    /// is declared. Values beat NULL (`NULLS LAST` both directions); the
    /// arrival order stays as the trailing tie-breaker, so ties and
    /// all-NULL groups keep the deterministic last-wins. EVERY strategy's
    /// survivor decision flows through here (MR2).
    fn deduped(&self, identity: &str) -> String {
        let sort = match self.dedup_sort {
            Some(d) => {
                let dir = match d.order {
                    SortOrder::Asc => "ASC",
                    SortOrder::Desc => "DESC",
                };
                format!("{} {dir} NULLS LAST, ", self.quote(&d.column))
            }
            None => String::new(),
        };
        self.dialect.dedup_subquery(identity, &sort, self.stage)
    }

    /// `SET c = EXCLUDED.c, …` over the non-identity columns.
    fn update_set(&self, identity: &[String]) -> String {
        self.schema
            .columns
            .iter()
            .filter(|c| !identity.contains(&c.name))
            .map(|c| format!("{q} = EXCLUDED.{q}", q = self.quote(&c.name)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Roots flagged for hard deletion — decided from the DEDUPED last-wins
    /// root row (review F3: reading the RAW stage disagreed with survival
    /// when a root was flagged then re-created in the same load).
    fn flagged_roots(&self) -> Option<String> {
        self.hard_delete.as_ref().map(|hd| {
            let id = self.quote(system_columns::ID);
            format!(
                "(SELECT {id} FROM (SELECT DISTINCT ON ({id}) * FROM {} \
                 ORDER BY {id}, {} DESC) d WHERE {})",
                self.root_stage,
                self.dialect.arrival_order(),
                hd.flagged
            )
        })
    }
}

/// Scope replacement (feature 010, contract merge-refinements.md MR3–MR5):
/// delete every target row whose scope matches a DELIVERED stage scope,
/// inside the publish transaction, in the load's FIRST commit unit only
/// (the caller enforces the single-unit rule). NULL is not a scope: stage
/// rows with any NULL scope column are excluded explicitly, and target-side
/// row comparison is never TRUE against NULL (MR4). Committed-unit
/// redelivery exits before merge SQL (D3), so the delete can never
/// double-fire.
pub fn scope_replace_sql(
    dialect: &dyn MergeDialect,
    target: &str,
    stage: &str,
    scope: &[String],
) -> String {
    let cols = scope
        .iter()
        .map(|c| dialect.quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    let not_null = scope
        .iter()
        .map(|c| format!("{} IS NOT NULL", dialect.quote(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "DELETE FROM {target} WHERE ({cols}) IN (
             SELECT {cols} FROM {stage} WHERE {not_null})"
    )
}

/// Keyed structured delete-insert (the 006 arm + M4 hard delete).
///
/// NULL keys: the `(key) IN (...)` predicate is NULL-blind by SQL semantics,
/// but NULL merge-key VALUES cannot reach this code — the engine rejects
/// them typed at write time (feature 006, `rdlt-engine/src/load/mod.rs`
/// structured_merge_keys guard, conformance-pinned). Direct SPI drivers
/// bypassing the engine inherit that contract obligation.
pub fn keyed_delete_insert_sql(plan: &MergePlan<'_>) -> Vec<String> {
    let (target, stage, cols) = (plan.target, plan.stage, plan.cols);
    let key_list = plan.key_list();
    let keep = plan
        .hard_delete
        .as_ref()
        .map(|hd| format!(" WHERE {}", hd.keep))
        .unwrap_or_default();
    vec![
        format!("DELETE FROM {target} WHERE ({key_list}) IN (SELECT {key_list} FROM {stage})"),
        format!(
            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {} deduped{keep}",
            plan.deduped(&key_list),
        ),
    ]
}

/// Keyed structured upsert (M2): conflict-update on the merge key.
pub fn keyed_upsert_sql(plan: &MergePlan<'_>) -> Vec<String> {
    let (target, cols) = (plan.target, plan.cols);
    let key_list = plan.key_list();
    let mut out = Vec::new();
    if let Some(hd) = &plan.hard_delete {
        out.push(format!(
            "DELETE FROM {target} WHERE ({key_list}) IN \
             (SELECT {key_list} FROM {} d WHERE {})",
            plan.deduped(&key_list),
            hd.flagged
        ));
    }
    let keep = plan
        .hard_delete
        .as_ref()
        .map(|hd| format!(" WHERE {}", hd.keep))
        .unwrap_or_default();
    let set = plan.update_set(plan.key);
    let action = if set.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {set}")
    };
    out.push(plan.dialect.upsert_stmt(
        target,
        cols,
        &plan.deduped(&key_list),
        &keep,
        &key_list,
        &action,
    ));
    out
}

/// Shredded identity delete-insert (the original arm + M4 hard delete).
pub fn identity_delete_insert_sql(plan: &MergePlan<'_>) -> Vec<String> {
    let (target, cols) = (plan.target, plan.cols);
    let id = plan.quote(system_columns::ID);
    let id_col = if plan.is_child {
        plan.quote(system_columns::ROOT_ID)
    } else {
        id.clone()
    };
    // Hard delete (M4): flagged ROOTS drop from the root insert; their
    // children drop by root-id membership.
    let keep = match (&plan.hard_delete, plan.is_child) {
        (Some(hd), false) => format!(" WHERE {}", hd.keep),
        (Some(_), true) => format!(
            " WHERE {} NOT IN {}",
            plan.quote(system_columns::ROOT_ID),
            plan.flagged_roots().expect("hard_delete present")
        ),
        (None, _) => String::new(),
    };
    vec![
        // Subtree replacement by root id + DETERMINISTIC in-batch dedup:
        // arrival order breaks ties, so "last wins" is real (finding #7).
        format!(
            "DELETE FROM {target} WHERE {id_col} IN (SELECT {id} FROM {})",
            plan.root_stage
        ),
        format!(
            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {} deduped{keep}",
            plan.deduped(&id),
        ),
    ]
}

/// SCD2 (contract scd2.md): retire-changed-then-insert with NULL-safe
/// column-wise change detection. One boundary per commit unit: the dialect's
/// timestamp expression is TRANSACTION-stable, so every statement in this
/// publish sees the same instant (S5); redelivery re-executes nothing (D3).
pub fn scd2_merge_sql(plan: &MergePlan<'_>, scd2: &Scd2Options) -> Vec<String> {
    let (target, cols) = (plan.target, plan.cols);
    let now = plan.dialect.tx_timestamp();
    let key_list = plan.key_list();
    let deduped = plan.deduped(&key_list);
    let vf = plan.quote(&scd2.valid_from);
    let vt = plan.quote(&scd2.valid_to);
    let key_match = plan
        .key
        .iter()
        .map(|k| format!("t.{q} = d.{q}", q = plan.quote(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    // Change detection (S3): NULL-safe, over the DATA columns — the key is
    // the identity and the load-id changes every load by construction.
    let changed = plan
        .schema
        .columns
        .iter()
        .filter(|c| !plan.key.contains(&c.name) && c.name != system_columns::LOAD_ID)
        .map(|c| format!("t.{q} IS DISTINCT FROM d.{q}", q = plan.quote(&c.name)))
        .collect::<Vec<_>>()
        .join(" OR ");

    let mut out = Vec::new();
    // S3 retire: active versions whose key arrives with DIFFERENT values.
    if !changed.is_empty() {
        out.push(format!(
            "UPDATE {target} t SET {vt} = {now} \
             FROM {deduped} d WHERE t.{vt} IS NULL AND {key_match} AND ({changed})"
        ));
    }
    // S2/S3 insert: staged rows with NO remaining active version (changed
    // keys were just retired; unchanged keys still hold their identical
    // active version and are SKIPPED — no churn).
    out.push(format!(
        "INSERT INTO {target} ({cols}, {vf}, {vt}) \
         SELECT {cols}, {now}, NULL FROM {deduped} d \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM {target} t WHERE t.{vt} IS NULL AND {key_match})"
    ));
    // S6: full-feed absence semantics on request.
    if scd2.absent == crate::options::AbsentPolicy::Retire {
        out.push(format!(
            "UPDATE {target} t SET {vt} = {now} \
             WHERE t.{vt} IS NULL AND ({key_list}) NOT IN \
                   (SELECT {key_list} FROM {} d)",
            plan.deduped(&key_list),
        ));
    }
    out
}

// ---- Open-time validation (SM1/SM5: one rule set, identical typed errors) ----

/// Table facts a destination resolves before validation.
pub struct TableFacts<'a> {
    pub schema: &'a TableSchema,
    pub has_identity: bool,
    pub is_child: bool,
}

/// Feature 010 review (the 008 F6 lesson applied to the options): options
/// under Append or Replace would be silently inert — reject typed instead
/// (incl. the 011 R5 explicit-vs-default merge_strategy distinction).
pub fn validate_non_merge(options: &DestOptions, table: &str) -> Result<(), String> {
    for (declared, name) in [
        (
            options.explicit_strategy_for(table).is_some(),
            "merge_strategy",
        ),
        (options.dedup_sort_for(table).is_some(), "dedup_sort"),
        (options.merge_key_for(table).is_some(), "merge_key"),
    ] {
        if declared {
            return Err(format!(
                "table `{table}`: {name} requires the merge write mode \
                 (contract merge-refinements.md MR6) — under \
                 append/replace it would be silently inert"
            ));
        }
    }
    Ok(())
}

/// The 008/010 merge-mode option checks (existence, collisions, keyed-only
/// rules) — every error names the offender, text frozen from the postgres
/// cells that pin it.
pub fn validate_merge(
    options: &DestOptions,
    table: &str,
    key: &[String],
    facts: &TableFacts<'_>,
) -> Result<(), String> {
    let strategy = options.strategy_for(table);
    let has_identity = facts.has_identity;
    if let Some(col) = options.hard_delete_for(table) {
        // Review F6: configuring hard_delete on a CHILD table was silently
        // inert — reject typed instead (M4: flags live on the ROOT row of a
        // shredded stream).
        if facts.is_child {
            return Err(format!(
                "table `{table}`: hard_delete applies to the ROOT table of a \
                 shredded stream — configure it on the root, not the child"
            ));
        }
        // M4: the flag column must exist on THIS table's schema.
        if facts.schema.column(col).is_none() {
            return Err(format!(
                "hard_delete column `{col}` is not a column of table `{table}`"
            ));
        }
    }
    // Feature 010 (MR6): both refinement options are keyed-structured only,
    // their columns must exist, and they may not repurpose the hard_delete
    // flag. (A collision with scd2 validity columns is unreachable: validity
    // names may not be stream columns [S1] while these options' columns MUST
    // be — S1 fires first.)
    if let Some(dedup) = options.dedup_sort_for(table) {
        if has_identity {
            return Err(format!(
                "table `{table}`: dedup_sort requires a KEYED structured \
                 stream (contract merge-refinements.md MR6) — a shredded \
                 stream's identity is a content hash, ordered survivors \
                 are meaningless there"
            ));
        }
        if facts.schema.column(&dedup.column).is_none() {
            return Err(format!(
                "dedup_sort column `{}` is not a column of table `{table}`",
                dedup.column
            ));
        }
        if options.hard_delete_for(table) == Some(dedup.column.as_str()) {
            return Err(format!(
                "table `{table}`: dedup_sort column `{}` is the hard_delete \
                 flag — use a distinct ordering column (contract \
                 merge-refinements.md MR6)",
                dedup.column
            ));
        }
        // Review: a merge-key column is CONSTANT within each identity
        // group — the ordering could never choose a survivor, silently
        // reverting to arrival order.
        if key.contains(&dedup.column) {
            return Err(format!(
                "table `{table}`: dedup_sort column `{}` is part of the \
                 merge key — constant within each identity group, it can \
                 never order survivors (contract merge-refinements.md MR6)",
                dedup.column
            ));
        }
    }
    if let Some(scope) = options.merge_key_for(table) {
        if has_identity {
            return Err(format!(
                "table `{table}`: merge_key requires a KEYED structured \
                 stream (contract merge-refinements.md MR6) — shredded \
                 streams replace by root subtree"
            ));
        }
        if strategy == MergeStrategy::Scd2 {
            // Belt: parse-time validation already rejects this; direct
            // struct construction must not slip past it.
            return Err(format!(
                "table `{table}`: merge_key is not valid with scd2 \
                 (contract merge-refinements.md MR6)"
            ));
        }
        for col in scope {
            if facts.schema.column(col).is_none() {
                return Err(format!(
                    "merge_key column `{col}` is not a column of table `{table}`"
                ));
            }
            if options.hard_delete_for(table) == Some(col.as_str()) {
                return Err(format!(
                    "table `{table}`: merge_key column `{col}` is the \
                     hard_delete flag — a deletion flag is not a scope \
                     (contract merge-refinements.md MR6)"
                ));
            }
        }
    }
    if strategy == MergeStrategy::Upsert && has_identity {
        // Review F4 / contract M7 (amended): a shredded stream's _rdlt_id is
        // a CONTENT hash for keyless streams — updates mint new ids and
        // ON CONFLICT never fires, silently duplicating. The destination
        // cannot distinguish keyed from keyless shredded streams, so upsert
        // is keyed-structured only; shredded streams keep delete_insert
        // (subtree replacement).
        return Err(format!(
            "table `{table}`: the upsert strategy requires a KEYED \
             structured stream (contract merge-strategies.md M2/M7) — \
             shredded streams use delete_insert"
        ));
    }
    if strategy == MergeStrategy::Scd2 {
        if has_identity {
            return Err(format!(
                "table `{table}`: scd2 requires a KEYED structured stream \
                 (contract scd2.md S1) — shredded streams have no declared key"
            ));
        }
        let scd2 = options.scd2_for(table);
        for name in [&scd2.valid_from, &scd2.valid_to] {
            if facts.schema.column(name).is_some() {
                return Err(format!(
                    "table `{table}`: scd2 validity column `{name}` collides \
                     with a stream column (contract scd2.md S1) — configure \
                     different names"
                ));
            }
        }
    }
    let _ = key;
    Ok(())
}

/// Index plan (008 data-model): identity per table kind, plus the 010 scope
/// index. `(unique, columns)` pairs; SQL text is the destination's.
pub fn index_plan(
    options: &DestOptions,
    table: &str,
    key: &[String],
    has_identity: bool,
    is_child: bool,
) -> Vec<(bool, Vec<String>)> {
    let strategy = options.strategy_for(table);
    let mut indexes: Vec<(bool, Vec<String>)> = Vec::new();
    if has_identity {
        indexes.push((false, vec![system_columns::ID.to_string()]));
        if is_child {
            indexes.push((false, vec![system_columns::ROOT_ID.to_string()]));
        }
    } else {
        match strategy {
            MergeStrategy::Upsert => indexes.push((true, key.to_vec())),
            MergeStrategy::DeleteInsert => indexes.push((false, key.to_vec())),
            MergeStrategy::Scd2 => {
                // (key…, valid_to): active-version lookups + retire.
                let scd2 = options.scd2_for(table);
                let mut cols = key.to_vec();
                cols.push(scd2.valid_to.clone());
                indexes.push((false, cols));
            }
        }
        // Feature 010 review: the scope delete probes by the scope columns —
        // without this index it seq-scans the whole target every load (the
        // scoreboard number was measured WITH it).
        if let Some(scope) = options.merge_key_for(table) {
            indexes.push((false, scope.to_vec()));
        }
    }
    indexes
}

/// The per-table single-commit-unit violation (MR5 / scd2 S6, one shared
/// rule + one shared message).
pub fn single_unit_violation(table: &str, scoped: bool) -> String {
    let (what, contract) = if scoped {
        ("merge_key scope replacement", "merge-refinements.md MR5")
    } else {
        ("scd2 `absent: retire`", "scd2.md S6")
    };
    format!(
        "table `{table}`: {what} requires the table's full \
         feed in a SINGLE commit unit (contract {contract}) — \
         raise the engine commit thresholds and re-run; the \
         fixed configuration re-delivers the full feed and \
         converges"
    )
}
