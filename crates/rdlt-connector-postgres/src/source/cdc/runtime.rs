//! Run-scoped CDC state: the replica-identity preflight, the shared control
//! and snapshot connections, and per-stream ack/cursor bookkeeping — one
//! instance per engine run.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use rdlt_connector::{Cursor, SourceError};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use crate::source::config::{CdcConfig, PostgresConfig};
use crate::source::connect;
use crate::source::errors::{self, Phase};
use crate::source::reflect::ReflectedTable;

use super::slot;

/// The engine cursor for a CDC stream: one LSN, distinct JSON shape from
/// the cursor-column state so misrouted state fails typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CdcCursor {
    pub(super) cdc_lsn: u64,
}

impl CdcCursor {
    pub(super) fn encode(self) -> Cursor {
        Cursor::new(serde_json::to_value(self).expect("cdc cursor serializes"))
    }

    pub(super) fn decode(cursor: &Cursor, table: &str) -> Result<Self, SourceError> {
        serde_json::from_value(cursor.as_value().clone()).map_err(|e| {
            errors::fatal(
                Phase::Slot,
                Some(table),
                format!("stored CDC cursor does not decode (state corruption?): {e}"),
            )
        })
    }
}

/// Per-table replica-identity preflight result.
#[derive(Debug, Clone)]
pub(crate) struct TableIdentity {
    /// The merge key: the table's replica identity columns.
    pub key: Vec<String>,
    /// REPLICA IDENTITY FULL — old tuples carry every column (TOAST
    /// substitution is possible).
    pub full: bool,
}

/// Run-scoped CDC state on the source (one instance per engine run).
pub(crate) struct Runtime {
    pub(super) state: tokio::sync::Mutex<RunState>,
    identities: tokio::sync::OnceCell<BTreeMap<String, TableIdentity>>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // RunState holds live connections (no Debug); the identities map is
        // the useful part.
        f.debug_struct("cdc::Runtime")
            .field("identities", &self.identities.get())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
pub(super) struct RunState {
    /// Lifecycle client, lazily opened; also carries peeks and the ack.
    /// Arc so passes run WITHOUT the state lock held (a stream blocked on
    /// destination backpressure must never stall the other streams), and
    /// dropped on any error (the engine's transient in-run retries re-enter
    /// with this same state — a dead connection must not be reused).
    pub(super) control: Option<std::sync::Arc<Client>>,
    pub(super) ensured: Option<slot::EnsureOutcome>,
    /// Open REPEATABLE READ snapshot transaction (first run) — ONE view
    /// for every CDC table. Arc + drop-on-error like `control`.
    pub(super) snapshot: Option<std::sync::Arc<Client>>,
    /// The shared snapshot's cursor start point for a PRE-EXISTING slot:
    /// the WAL position read BEFORE the transaction began (its visibility
    /// horizon) — every commit ≤ it is IN the snapshot; commits after it
    /// replay and converge. (Starting at confirmed_flush instead would
    /// replay a window that can contain unappliable records — TRUNCATE,
    /// TOAST without an old image — and permanently wedge recovery.)
    pub(super) snapshot_start: Option<u64>,
    /// `target_lsn`, pinned once per run at the first CDC read.
    pub(super) target: Option<u64>,
    /// Per-stream ack floors: destination-committed `since`, or the fresh
    /// snapshot start point (see module docs).
    pub(super) ack_floor: BTreeMap<String, u64>,
    /// CDC streams that have not completed their pass this run; `None`
    /// until the first CDC read initializes it.
    pub(super) pending: Option<BTreeSet<String>>,
    /// Each stream's final cursor this run — the lag report's baseline.
    pub(super) final_cursor: BTreeMap<String, u64>,
}

impl RunState {
    /// Open the lifecycle client if this run has not yet, pinning the session
    /// GUCs the pgoutput TEXT forms depend on (a role whose DateStyle or
    /// bytea_output differs must not change what the feed looks like).
    /// Idempotent within a run.
    pub(super) async fn ensure_control(
        &mut self,
        config: &PostgresConfig,
        name: &str,
    ) -> Result<(), SourceError> {
        if self.control.is_none() {
            let client = connect(config).await?;
            client
                .batch_execute("SET datestyle = 'ISO'; SET bytea_output = 'hex'")
                .await
                .map_err(|e| errors::classify(Phase::Slot, Some(name), &e))?;
            self.control = Some(std::sync::Arc::new(client));
        }
        Ok(())
    }

    /// Open the shared REPEATABLE READ snapshot transaction if this run has
    /// not yet, recording its visibility horizon `start`. Idempotent — the
    /// first CDC stream opens it; every snapshot pass this run shares it.
    pub(super) async fn ensure_snapshot(
        &mut self,
        config: &PostgresConfig,
        name: &str,
        start: u64,
    ) -> Result<(), SourceError> {
        if self.snapshot.is_none() {
            let snap = connect(config).await?;
            snap.batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await
                .map_err(|e| errors::classify(Phase::Copy, Some(name), &e))?;
            self.snapshot = Some(std::sync::Arc::new(snap));
            self.snapshot_start = Some(start);
        }
        Ok(())
    }

    /// The lifecycle client, guaranteed present once `ensure_control` has run
    /// at the top of `read_stream_inner`. The one audited panic site: the run
    /// clears `control` only on error, which returns before any getter runs.
    pub(super) fn control(&self) -> &std::sync::Arc<Client> {
        self.control
            .as_ref()
            .expect("control client opened at run start")
    }

    /// The shared snapshot client, guaranteed present once `ensure_snapshot`
    /// has run for the snapshot pass. The one audited panic site.
    pub(super) fn snapshot(&self) -> &std::sync::Arc<Client> {
        self.snapshot
            .as_ref()
            .expect("snapshot client opened for the snapshot pass")
    }
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            state: tokio::sync::Mutex::new(RunState::default()),
            identities: tokio::sync::OnceCell::new(),
        }
    }

    /// The replica-identity preflight, once per run: key + FULL flag
    /// per CDC table, flag-column collisions, unusable identities — all
    /// typed BEFORE any stream declares itself.
    pub async fn identities(
        &self,
        config: &PostgresConfig,
        cdc: &CdcConfig,
        reflected: &BTreeMap<String, ReflectedTable>,
        cdc_tables: &[String],
    ) -> Result<&BTreeMap<String, TableIdentity>, SourceError> {
        self.identities
            .get_or_try_init(|| async {
                let client = connect(config).await?;
                preflight(&client, config, cdc, reflected, cdc_tables).await
            })
            .await
    }
}

async fn preflight(
    client: &Client,
    config: &PostgresConfig,
    cdc: &CdcConfig,
    reflected: &BTreeMap<String, ReflectedTable>,
    cdc_tables: &[String],
) -> Result<BTreeMap<String, TableIdentity>, SourceError> {
    let rows = client
        .query(
            "SELECT c.relname, c.relreplident::text,
                    coalesce((SELECT array_agg(a.attname ORDER BY x.ord)
                              FROM pg_index i
                              CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
                              JOIN pg_attribute a
                                ON a.attrelid = i.indrelid AND a.attnum = x.attnum
                              WHERE i.indrelid = c.oid AND i.indisreplident),
                             '{}') AS ident_cols
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = ANY($2)",
            &[&config.schema, &cdc_tables],
        )
        .await
        .map_err(|e| errors::classify(Phase::Slot, None, &e))?;
    let facts: HashMap<String, (String, Vec<String>)> = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                (row.get::<_, String>(1), row.get::<_, Vec<String>>(2)),
            )
        })
        .collect();

    let mut identities = BTreeMap::new();
    for table in cdc_tables {
        let reflected_table = reflected.get(table).ok_or_else(|| {
            errors::fatal(Phase::Slot, Some(table), "CDC table has no reflected shape")
        })?;
        if reflected_table.column(&cdc.flag_column).is_some() {
            return Err(errors::fatal(
                Phase::Slot,
                Some(table),
                format!(
                    "flag column `{}` collides with an existing column — set \
                     `cdc.flag_column` to an unused name",
                    cdc.flag_column
                ),
            ));
        }
        let (replident, ident_cols) = facts.get(table).ok_or_else(|| {
            errors::fatal(
                Phase::Slot,
                Some(table),
                "CDC table not found in the catalog",
            )
        })?;
        let pk: Vec<String> = reflected_table
            .primary_key()
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let declared = config
            .table_config(table)
            .and_then(|t| t.primary_key.clone());
        let identity = match replident.as_str() {
            // Default: delete/update records carry the PRIMARY KEY columns.
            "d" if !pk.is_empty() => TableIdentity {
                key: pk,
                full: false,
            },
            "d" => {
                return Err(errors::fatal(
                    Phase::Slot,
                    Some(table),
                    "CDC needs a usable replica identity and the table has no \
                     primary key — add one, or `ALTER TABLE … REPLICA IDENTITY \
                     FULL` / `USING INDEX …`",
                ));
            }
            // Explicit index: the identity columns come from that index. A
            // dropped identity index leaves relreplident = 'i' with NO
            // indisreplident row — an empty key would merge on nothing and
            // corrupt deletes, so it is a typed error, never accepted.
            "i" if !ident_cols.is_empty() => TableIdentity {
                key: ident_cols.clone(),
                full: false,
            },
            "i" => {
                return Err(errors::fatal(
                    Phase::Slot,
                    Some(table),
                    "REPLICA IDENTITY USING INDEX but no replica-identity \
                     index exists (was it dropped?) — recreate the index or \
                     `ALTER TABLE … REPLICA IDENTITY DEFAULT`/`FULL`",
                ));
            }
            // FULL: old tuples carry everything, so ANY declared key has its
            // values — a declared primary_key override wins; else the PK.
            "f" => match declared
                .clone()
                .or_else(|| (!pk.is_empty()).then(|| pk.clone()))
            {
                Some(key) => TableIdentity { key, full: true },
                None => {
                    return Err(errors::fatal(
                        Phase::Slot,
                        Some(table),
                        "REPLICA IDENTITY FULL but no key to merge by — add a \
                         primary key or declare `primary_key` on the table",
                    ));
                }
            },
            // NOTHING (or anything else): updates/deletes carry no key data.
            other => {
                return Err(errors::fatal(
                    Phase::Slot,
                    Some(table),
                    format!(
                        "replica identity `{other}` cannot replicate \
                         updates/deletes — `ALTER TABLE … REPLICA IDENTITY \
                         DEFAULT` (with a primary key) or `FULL`"
                    ),
                ));
            }
        };
        // A declared primary_key that disagrees with the identity columns
        // would leave delete records without values for the merge key —
        // typed, not silently ignored. (Under FULL any declared key works.)
        if let Some(declared) = declared
            && !identity.full
            && declared != identity.key
        {
            return Err(errors::fatal(
                Phase::Slot,
                Some(table),
                format!(
                    "declared primary_key {declared:?} differs from the replica \
                     identity columns {:?} — delete records only carry the \
                     identity columns; align them or use REPLICA IDENTITY FULL",
                    identity.key
                ),
            ));
        }
        // Defense in depth: every arm above guarantees a non-empty key; an
        // empty one would pass every downstream guard vacuously.
        assert!(!identity.key.is_empty(), "preflight resolved an empty key");
        identities.insert(table.clone(), identity);
    }
    Ok(identities)
}
