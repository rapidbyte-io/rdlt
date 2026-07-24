//! Slot + publication lifecycle over the SQL logical-decoding interface:
//! create-if-missing — idempotent, and rdlt NEVER drops either resource —
//! range peek via
//! `pg_logical_slot_peek_binary_changes` (reads WITHOUT consuming), and
//! explicit acknowledgement via `pg_replication_slot_advance`. Every ugly
//! lifecycle state read from `pg_replication_slots` is its own
//! DISTINGUISHED typed error (no silent failures).

use std::collections::BTreeSet;

use rdlt_connector::SourceError;
use tokio_postgres::Client;

use crate::source::config::CdcConfig;
use crate::source::errors::{Phase, classify, fatal};
use crate::source::sqlgen::quote_ident;

/// Parse postgres's `pg_lsn` text form `X/Y` (two hex halves) into the
/// 64-bit position.
pub fn parse_lsn(text: &str) -> Option<u64> {
    let (hi, lo) = text.split_once('/')?;
    if hi.is_empty() || lo.is_empty() || hi.len() > 8 || lo.len() > 8 {
        return None;
    }
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some((hi << 32) | lo)
}

/// Render an LSN the way postgres and its operators do (`X/Y`).
pub fn fmt_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

/// Outcome of [`ensure`]: whether this run created the slot, and the
/// slot's consistent point when it did (the snapshot handoff cursor).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnsureOutcome {
    pub created_slot: bool,
    /// Present exactly when `created_slot` — a pre-existing slot's start
    /// position is whatever cursor state the engine already holds.
    pub consistent_point: Option<u64>,
}

/// One peeked change: the WAL position and the raw pgoutput message.
#[derive(Debug)]
pub struct Change {
    pub lsn: u64,
    pub data: Vec<u8>,
}

/// Preflight + lifecycle: publication exists and covers every CDC
/// table; slot exists (created here only under `create_if_missing`), runs
/// the `pgoutput` plugin, is not held by a concurrent consumer, and has
/// not been invalidated by WAL retention. Each violation is its own typed
/// fatal error naming the resource and the fix.
pub async fn ensure(
    client: &Client,
    cdc: &CdcConfig,
    schema: &str,
    tables: &[String],
) -> Result<EnsureOutcome, SourceError> {
    let publication_exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&cdc.publication],
        )
        .await
        .map_err(|e| classify(Phase::Slot, None, &e))?
        .is_some();
    if !publication_exists {
        if !cdc.create_if_missing {
            return Err(fatal(
                Phase::Slot,
                None,
                format!(
                    "publication `{}` does not exist (set `cdc.create_if_missing: true` \
                     to have rdlt create it for the configured tables)",
                    cdc.publication
                ),
            ));
        }
        let table_list = tables
            .iter()
            .map(|t| format!("{}.{}", quote_ident(schema), quote_ident(t)))
            .collect::<Vec<_>>()
            .join(", ");
        client
            .batch_execute(&format!(
                "CREATE PUBLICATION {} FOR TABLE {table_list}",
                quote_ident(&cdc.publication)
            ))
            .await
            .map_err(|e| classify(Phase::Slot, None, &e))?;
    }
    // Coverage check runs even on the freshly created publication — the
    // check is the contract, creation is just one way to satisfy it. An
    // EXISTING publication with gaps is an error, never an ALTER: the
    // publication is a user-owned resource and rdlt only ever creates.
    let covered: BTreeSet<String> = client
        .query(
            "SELECT tablename FROM pg_publication_tables \
             WHERE pubname = $1 AND schemaname = $2",
            &[&cdc.publication, &schema],
        )
        .await
        .map_err(|e| classify(Phase::Slot, None, &e))?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let missing: Vec<&str> = tables
        .iter()
        .map(String::as_str)
        .filter(|t| !covered.contains(*t))
        .collect();
    if !missing.is_empty() {
        return Err(fatal(
            Phase::Slot,
            None,
            format!(
                "publication `{}` does not cover configured table(s) {} — \
                 `ALTER PUBLICATION {} ADD TABLE …` (rdlt never alters the \
                 publication itself)",
                cdc.publication,
                missing
                    .iter()
                    .map(|t| format!("`{t}`"))
                    .collect::<Vec<_>>()
                    .join(", "),
                cdc.publication
            ),
        ));
    }

    let slot = client
        .query_opt(
            "SELECT plugin, active_pid, wal_status FROM pg_replication_slots \
             WHERE slot_name = $1",
            &[&cdc.slot],
        )
        .await
        .map_err(|e| classify(Phase::Slot, None, &e))?;
    let Some(row) = slot else {
        if !cdc.create_if_missing {
            return Err(fatal(
                Phase::Slot,
                None,
                format!(
                    "replication slot `{}` does not exist (set \
                     `cdc.create_if_missing: true` to have rdlt create it)",
                    cdc.slot
                ),
            ));
        }
        let created = client
            .query_one(
                "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&cdc.slot],
            )
            .await
            .map_err(|e| classify(Phase::Slot, None, &e))?;
        let point: String = created.get(0);
        let consistent_point = parse_lsn(&point).ok_or_else(|| {
            fatal(
                Phase::Slot,
                None,
                format!("server returned unparseable consistent point `{point}`"),
            )
        })?;
        return Ok(EnsureOutcome {
            created_slot: true,
            consistent_point: Some(consistent_point),
        });
    };

    let plugin: String = row.get(0);
    if plugin != "pgoutput" {
        return Err(fatal(
            Phase::Slot,
            None,
            format!(
                "replication slot `{}` runs plugin `{plugin}`, not `pgoutput` — \
                 it belongs to another tool; configure a different `cdc.slot`",
                cdc.slot
            ),
        ));
    }
    if let Some(pid) = row.get::<_, Option<i32>>(1) {
        return Err(fatal(
            Phase::Slot,
            None,
            format!(
                "replication slot `{}` is actively held by pid {pid} — a slot \
                 has exactly one consumer; stop the other consumer or configure \
                 a different `cdc.slot`",
                cdc.slot
            ),
        ));
    }
    if let Some(status) = row.get::<_, Option<String>>(2)
        && matches!(status.as_str(), "lost" | "unreserved")
    {
        return Err(fatal(
            Phase::Slot,
            None,
            format!(
                "replication slot `{}` has wal_status `{status}`: the server \
                 discarded WAL the slot still needed (retention overrun). The \
                 feed cannot resume — drop the slot, then re-run to take a \
                 fresh snapshot (rdlt never drops slots itself)",
                cdc.slot
            ),
        ));
    }
    Ok(EnsureOutcome {
        created_slot: false,
        consistent_point: None,
    })
}

/// Peek the change feed up to `upto` WITHOUT consuming it: every
/// CDC stream re-peeks the same range and filters its own table; nothing
/// moves until [`advance`].
pub async fn peek(client: &Client, cdc: &CdcConfig, upto: u64) -> Result<Vec<Change>, SourceError> {
    let rows = client
        .query(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes(\
             $1, $2::text::pg_lsn, NULL::int4, \
             'proto_version', '1', 'publication_names', $3)",
            &[&cdc.slot, &fmt_lsn(upto), &cdc.publication],
        )
        .await
        .map_err(|e| classify(Phase::Slot, None, &e))?;
    rows.into_iter()
        .map(|row| {
            let lsn: String = row.get(0);
            let lsn = parse_lsn(&lsn).ok_or_else(|| {
                fatal(
                    Phase::Slot,
                    None,
                    format!("server returned unparseable change lsn `{lsn}`"),
                )
            })?;
            Ok(Change {
                lsn,
                data: row.get(1),
            })
        })
        .collect()
}

/// Acknowledge: advance the slot's confirmed position to `upto` — once per
/// run, to the min committed cursor across streams, only after every
/// stream's destination commit; never a correctness dependency.
pub async fn advance(client: &Client, slot: &str, upto: u64) -> Result<(), SourceError> {
    client
        .query_one(
            "SELECT end_lsn::text FROM pg_replication_slot_advance($1, $2::text::pg_lsn)",
            &[&slot, &fmt_lsn(upto)],
        )
        .await
        .map_err(|e| classify(Phase::Slot, None, &e))?;
    Ok(())
}

/// The feed's current position — the `target_lsn` pin for a bounded
/// catch-up pass.
pub async fn current_wal_lsn(client: &Client) -> Result<u64, SourceError> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .map_err(|e| classify(Phase::Slot, None, &e))?;
    let lsn: String = row.get(0);
    parse_lsn(&lsn).ok_or_else(|| {
        fatal(
            Phase::Slot,
            None,
            format!("server returned unparseable wal position `{lsn}`"),
        )
    })
}

/// The slot's acknowledged position (`confirmed_flush_lsn`) — lag
/// reporting and the ack-after-commit conformance pin read this.
pub async fn confirmed_flush_lsn(client: &Client, slot: &str) -> Result<u64, SourceError> {
    let row = client
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|e| classify(Phase::Slot, None, &e))?;
    let lsn: String = row.get(0);
    parse_lsn(&lsn).ok_or_else(|| {
        fatal(
            Phase::Slot,
            None,
            format!("server returned unparseable confirmed position `{lsn}`"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_text_round_trips() {
        for (text, value) in [
            ("0/0", 0u64),
            ("0/1", 1),
            ("1/0", 1 << 32),
            ("16/B374D848", (0x16 << 32) | 0xB374_D848),
            ("FFFFFFFF/FFFFFFFF", u64::MAX),
        ] {
            assert_eq!(parse_lsn(text), Some(value), "{text}");
            assert_eq!(parse_lsn(&fmt_lsn(value)).unwrap(), value);
        }
        for bad in ["", "0", "0/", "/0", "x/0", "0/x", "123456789/0", "0/0/0"] {
            assert_eq!(parse_lsn(bad), None, "{bad}");
        }
    }
}
