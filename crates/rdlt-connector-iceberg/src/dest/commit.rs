//! The commit loop: the snapshot-summary commit identity, the ONE bounded
//! optimistic-concurrency retry every commit rides (data, state, and schema
//! alike), and the fast-append that publishes a load's staged data files.
//! Library types stay BELOW this module and `errors.rs`.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, TableIdent};
use rdlt_connector::DestError;
use rdlt_connector::core::crash_point;

use super::errors::{classify, is_commit_conflict};

/// Snapshot-summary receipt keys — the persisted commit identity.
pub(crate) const PROP_PIPELINE: &str = "rdlt.pipeline";
pub(crate) const PROP_LOAD_ID: &str = "rdlt.load-id";
pub(crate) const PROP_COMMIT_SEQ: &str = "rdlt.commit-seq";

/// Bounded conflict-retry attempt count. Shared by every commit path so the
/// retry budget is one number.
pub(crate) const COMMIT_ATTEMPTS: u32 = 4;

/// The commit identity a snapshot carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitIdentity {
    pub scope: String,
    pub load_id: String,
    pub commit_seq: u64,
}

impl CommitIdentity {
    pub(crate) fn summary_props(&self) -> HashMap<String, String> {
        HashMap::from([
            (PROP_PIPELINE.to_string(), self.scope.clone()),
            (PROP_LOAD_ID.to_string(), self.load_id.clone()),
            (PROP_COMMIT_SEQ.to_string(), self.commit_seq.to_string()),
        ])
    }

    /// Is this identity already present in the table's snapshot history?
    pub(crate) fn already_committed(&self, table: &iceberg::table::Table) -> bool {
        table.metadata().snapshots().any(|snapshot| {
            let summary = &snapshot.summary().additional_properties;
            summary.get(PROP_PIPELINE) == Some(&self.scope)
                && summary.get(PROP_LOAD_ID) == Some(&self.load_id)
                && summary.get(PROP_COMMIT_SEQ) == Some(&self.commit_seq.to_string())
        })
    }
}

/// What one attempt of [`commit_with_retry`] found to do.
pub(crate) enum CommitPlan {
    /// The desired state already holds (an idempotent re-run or a converged
    /// no-op) — return the loaded table unchanged, committing nothing.
    Settled,
    /// Commit this prepared transaction; a conflict rides the bounded retry.
    /// Boxed: a `Transaction` dwarfs the unit `Settled` variant.
    Commit(Box<Transaction>),
}

/// The ONE bounded optimistic-concurrency retry: build → commit → on a CAS
/// conflict refresh the table and let `plan` rebuild against the competitor's
/// snapshot (never dropping their history), up to [`COMMIT_ATTEMPTS`]. Data
/// appends, state-property writes, and schema evolution all ride this one
/// loop. `subject` names the commit kind in the exhaustion diagnostic;
/// `entropy` distinguishes writers so two of them do not back off in lockstep
/// (see [`backoff`]). The loop diverges by construction — every path returns
/// or retries, so exhaustion needs no `unreachable!` tail.
pub(crate) async fn commit_with_retry<F>(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    context: &str,
    subject: &str,
    entropy: &str,
    initial: iceberg::table::Table,
    mut plan: F,
) -> Result<iceberg::table::Table, DestError>
where
    F: FnMut(&iceberg::table::Table) -> Result<CommitPlan, DestError>,
{
    let mut current = initial;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let tx = match plan(&current)? {
            CommitPlan::Settled => return Ok(current),
            CommitPlan::Commit(tx) => *tx,
        };
        match tx.commit(catalog.as_ref()).await {
            Ok(table) => return Ok(table),
            Err(e) if is_commit_conflict(&e) && attempt < COMMIT_ATTEMPTS => {
                backoff(entropy, attempt).await;
                current = catalog
                    .load_table(ident)
                    .await
                    .map_err(|e| classify(context, e))?;
            }
            // The final attempt's conflict is the typed exhaustion; `classify`
            // renders "commit conflicts exhausted the bounded retry", so the
            // subject/attempt prefix must NOT repeat the word.
            Err(e) if is_commit_conflict(&e) => {
                return Err(classify(
                    &format!("{context} ({subject} attempt {attempt}/{COMMIT_ATTEMPTS})"),
                    e,
                ));
            }
            Err(e) => return Err(classify(context, e)),
        }
    }
}

/// Jittered exponential backoff between optimistic-commit attempts. The base
/// doubles per attempt; the jitter is a full `[0, base)` window keyed on the
/// writer's `entropy` plus a per-process seed. Two DIFFERENT writers (distinct
/// entropy) therefore diverge instead of colliding on an identical schedule,
/// while one writer's schedule stays reproducible within a process run — no
/// wall-clock or global RNG in the commit path. `std::hash::RandomState`
/// gives per-process entropy with no new dependency.
async fn backoff(entropy: &str, attempt: u32) {
    static SEED: OnceLock<std::collections::hash_map::RandomState> = OnceLock::new();
    let base = 50u64 * (1u64 << attempt.min(4));
    let jitter = SEED
        .get_or_init(std::collections::hash_map::RandomState::new)
        .hash_one((entropy, attempt))
        % base;
    tokio::time::sleep(Duration::from_millis(base + jitter)).await;
}

/// Commit staged data files as ONE fast-append snapshot carrying the
/// identity, riding the bounded conflict retry. Returns the refreshed table.
/// The caller has already checked replay (a replayed identity never reaches
/// here); the retry re-checks it because the competitor it lost to may have
/// been our OWN replay landing the same identity — a second append would
/// double that identity's snapshot.
pub(crate) async fn append_commit(
    catalog: &Arc<dyn Catalog>,
    table: iceberg::table::Table,
    files: Vec<iceberg::spec::DataFile>,
    identity: &CommitIdentity,
) -> Result<iceberg::table::Table, DestError> {
    let ident = table.identifier().clone();
    let context = format!("table `{ident}`");
    let entropy = format!("{}:{}", identity.scope, identity.load_id);
    commit_with_retry(
        catalog,
        &ident,
        &context,
        "commit",
        &entropy,
        table,
        |current| {
            if identity.already_committed(current) {
                return Ok(CommitPlan::Settled);
            }
            let tx = Transaction::new(current);
            let action = tx
                .fast_append()
                .add_data_files(files.clone())
                .set_snapshot_properties(identity.summary_props());
            let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
            crash_point!(
                "ice.commit",
                Err(DestError::fatal("injected crash at ice.commit"))
            );
            Ok(CommitPlan::Commit(Box::new(tx)))
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use iceberg::{NamespaceIdent, TableIdent};

    use super::*;
    use crate::dest::test_support::{ConflictCatalog, data_file};

    fn identity() -> CommitIdentity {
        CommitIdentity {
            scope: "abc123".into(),
            load_id: "load-a".into(),
            commit_seq: 1,
        }
    }

    /// Conflicts within the bound are retried (refresh → rebuild → commit)
    /// and the commit lands.
    #[tokio::test]
    async fn commit_retries_through_transient_conflicts() {
        let catalog = ConflictCatalog::failing(COMMIT_ATTEMPTS - 1);
        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns".into()),
                "events".into(),
            ))
            .await
            .expect("load");
        let arc: Arc<dyn Catalog> = catalog.clone();
        append_commit(&arc, table, vec![data_file()], &identity())
            .await
            .expect("lands within the bound");
        assert_eq!(
            catalog.commits.load(Ordering::SeqCst),
            COMMIT_ATTEMPTS,
            "3 conflicts + the landing attempt"
        );
    }

    /// Exhaustion is a TYPED error naming the table and the attempt bound —
    /// never an unbounded loop.
    #[tokio::test]
    async fn commit_exhaustion_is_typed() {
        let catalog = ConflictCatalog::failing(u32::MAX);
        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns".into()),
                "events".into(),
            ))
            .await
            .expect("load");
        let arc: Arc<dyn Catalog> = catalog.clone();
        let err = append_commit(&arc, table, vec![data_file()], &identity())
            .await
            .expect_err("must exhaust");
        let text = format!("{err}");
        assert!(text.contains("events"), "names the table: {text}");
        assert!(
            text.contains(&format!("attempt {COMMIT_ATTEMPTS}/{COMMIT_ATTEMPTS}")),
            "names the exhausted bound: {text}"
        );
        assert_eq!(catalog.commits.load(Ordering::SeqCst), COMMIT_ATTEMPTS);
    }
}
