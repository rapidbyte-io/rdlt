//! The parts of a commit unit that are the same wherever it runs.
//!
//! Not a shared execute skeleton: the destinations drive their transactions in
//! structurally different ways — one `async` over an owned client, one inside
//! a synchronous closure holding a connection guard — and forcing those into
//! one shape needs a redesign, not a refactor. What IS shared is the small set
//! of decisions and probe texts every SQL destination must get right, and
//! getting one of them wrong duplicates rows.

use std::collections::BTreeMap;

use rdlt_connector::core::{LoadId, TableName, TableSchema};

use super::FullLoadPublish;

/// What a REDELIVERED unit must do — the outcome is INVERTED between publish
/// paths, and getting it backwards duplicates rows or loses them.
///
/// `replayed` means a receipt for this exact (load, commit-seq) already
/// exists: the original unit committed and its rows are durable. This unit
/// then re-delivered the same rows, and where those rows are sitting right now
/// is what decides the correct response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplayDisposition {
    /// Direct-to-target: the redelivered rows went STRAIGHT INTO the target
    /// inside the transaction still open. Committing would land them a second
    /// time, so the unit is abandoned — a rollback discards exactly what this
    /// attempt wrote and leaves the earlier commit standing. The receipt is
    /// still returned as success: from the caller's side the unit did commit,
    /// it just committed the first time.
    DiscardUnit,
    /// Staged: the redelivered rows sit in stage tables and have reached no
    /// reader. The planner's program for a replayed unit truncates those
    /// stages and nothing else, so running and committing it is what reclaims
    /// them — the guarantee is structural rather than earned by rolling back.
    RunScript,
}

/// Which response a redelivered unit owes, given how this destination
/// publishes.
///
/// One rule instead of two prose comments: the direct and staged paths were
/// each documented at length in their own executor, in opposite terms, with
/// nothing checking that a third destination read either of them.
pub fn replay_disposition(full_load_publish: FullLoadPublish) -> ReplayDisposition {
    match full_load_publish {
        FullLoadPublish::DirectToTarget => ReplayDisposition::DiscardUnit,
        FullLoadPublish::Staged => ReplayDisposition::RunScript,
    }
}

/// Has this exact (load, commit-seq) already been receipted?
///
/// `ph` renders the n-th positional placeholder in this destination's dialect
/// (`$1` on postgres, `?` on duckdb) — the only thing that differed between
/// the two copies of this query.
pub fn receipt_exists_sql(ph: impl Fn(usize) -> String) -> String {
    format!(
        "SELECT count(*) FROM {} WHERE load_id = {} AND commit_seq = {}",
        crate::names::COMMITS_TABLE,
        ph(1),
        ph(2)
    )
}

/// Has ANY unit of this load committed before?
///
/// The durable half of the once-per-load Replace guard: a crash-recovery
/// session has fresh memory but the same load, and must not re-clear a target
/// an earlier unit already published into.
pub fn load_committed_sql(ph: impl Fn(usize) -> String) -> String {
    format!(
        "SELECT count(*) FROM {} WHERE load_id = {}",
        crate::names::COMMITS_TABLE,
        ph(1)
    )
}

/// Does this stage hold any rows?
///
/// Takes an ALREADY-QUOTED name because quoting is the destination's, and
/// carries no parameters — so this text is identical everywhere today.
pub fn stage_nonempty_sql(quoted_stage: &str) -> String {
    format!("SELECT EXISTS (SELECT 1 FROM {quoted_stage})")
}

/// The child-to-root map every executor builds before running steps.
///
/// Deliberately built ON TOP of [`crate::plan::root_of`] rather than walking
/// the parent links again: a second walk of the same links is the drift this
/// module exists to prevent, and it would be free to disagree about depth
/// bounds or cycle handling.
pub fn roots_of<V>(
    tables: &BTreeMap<TableName, (TableSchema, V)>,
) -> BTreeMap<TableName, TableName> {
    tables
        .keys()
        .map(|table| (table.clone(), crate::plan::root_of(tables, table)))
        .collect()
}

/// A session opened for one load must not commit another.
///
/// Returns the message for a typed error, or `None` when the identities agree.
/// The check is cheap and the failure it catches is not: committing a unit
/// under the wrong load id writes a receipt no recovery will ever match.
pub fn load_mismatch(session: &LoadId, meta: &LoadId) -> Option<String> {
    (session != meta).then(|| {
        format!(
            "session was opened for load `{}` but is committing load `{}`",
            session.as_str(),
            meta.as_str()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_connector::core::TableSchema;

    fn pg(n: usize) -> String {
        format!("${n}")
    }
    fn duck(_: usize) -> String {
        "?".to_owned()
    }

    #[test]
    fn the_replay_outcome_is_inverted_between_publish_paths() {
        // The whole reason this type exists. Reading these two lines together
        // is the point — they were previously two long comments in two files,
        // saying opposite things, with nothing binding them.
        assert_eq!(
            replay_disposition(FullLoadPublish::DirectToTarget),
            ReplayDisposition::DiscardUnit
        );
        assert_eq!(
            replay_disposition(FullLoadPublish::Staged),
            ReplayDisposition::RunScript
        );
    }

    #[test]
    fn the_probes_differ_only_in_their_placeholders() {
        assert_eq!(
            receipt_exists_sql(pg),
            "SELECT count(*) FROM _rdlt_commits WHERE load_id = $1 AND commit_seq = $2"
        );
        assert_eq!(
            receipt_exists_sql(duck),
            "SELECT count(*) FROM _rdlt_commits WHERE load_id = ? AND commit_seq = ?"
        );
        assert_eq!(
            load_committed_sql(pg),
            "SELECT count(*) FROM _rdlt_commits WHERE load_id = $1"
        );
        assert_eq!(
            stage_nonempty_sql("\"_rdlt_stage_x\""),
            "SELECT EXISTS (SELECT 1 FROM \"_rdlt_stage_x\")"
        );
    }

    #[test]
    fn a_child_resolves_to_its_topmost_ancestor() {
        let mut tables: BTreeMap<TableName, (TableSchema, ())> = BTreeMap::new();
        for (name, parent) in [
            ("root", None),
            ("child", Some("root")),
            ("grandchild", Some("child")),
            ("lonely", None),
        ] {
            tables.insert(
                TableName::from(name),
                (
                    TableSchema {
                        table: TableName::from(name),
                        parent: parent.map(|p: &str| rdlt_connector::core::ParentLink {
                            parent: TableName::from(p),
                            depth: 1,
                        }),
                        columns: Vec::new(),
                    },
                    (),
                ),
            );
        }
        let roots = roots_of(&tables);
        assert_eq!(
            roots[&TableName::from("grandchild")],
            TableName::from("root")
        );
        assert_eq!(roots[&TableName::from("child")], TableName::from("root"));
        assert_eq!(roots[&TableName::from("root")], TableName::from("root"));
        assert_eq!(roots[&TableName::from("lonely")], TableName::from("lonely"));
    }

    #[test]
    fn a_cyclic_map_terminates_instead_of_hanging() {
        // Not reachable through a shredded schema, but a commit that hangs is
        // worse than one that resolves oddly.
        let mut tables: BTreeMap<TableName, (TableSchema, ())> = BTreeMap::new();
        for (name, parent) in [("a", "b"), ("b", "a")] {
            tables.insert(
                TableName::from(name),
                (
                    TableSchema {
                        table: TableName::from(name),
                        parent: Some(rdlt_connector::core::ParentLink {
                            parent: TableName::from(parent),
                            depth: 1,
                        }),
                        columns: Vec::new(),
                    },
                    (),
                ),
            );
        }
        let roots = roots_of(&tables);
        assert_eq!(roots.len(), 2);
    }

    #[test]
    fn a_session_may_only_commit_its_own_load() {
        let a = LoadId::from("load-a");
        let b = LoadId::from("load-b");
        assert!(load_mismatch(&a, &a).is_none());
        let message = load_mismatch(&a, &b).expect("a mismatch is refused");
        assert!(message.contains("load-a") && message.contains("load-b"));
    }
}
