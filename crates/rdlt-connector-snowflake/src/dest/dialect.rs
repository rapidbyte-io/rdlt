//! Snowflake's answers to the shared merge seam.
//!
//! This is the first dialect that cannot ride the trait's defaults. Three
//! things the other two SQL destinations rely on are simply absent here:
//!
//! - no `DISTINCT ON`, so the survivor subquery is `QUALIFY ROW_NUMBER()`;
//! - no `ON CONFLICT`, and no ENFORCED unique constraint for one to key on
//!   even if there were — primary keys are informational — so the upsert is
//!   `MERGE INTO`;
//! - no transaction-stable clock, so the scd2 boundary reads a value the unit
//!   captured rather than calling the clock again.
//!
//! The strategies themselves are the shared planner's; only their spelling
//! lives here.

use rdlt_connector_sqlcore::{MergeDialect, Upsert, UpsertAction, names};

use super::ddl::quote;

/// The session variable holding the unit's instant.
///
/// The scd2 boundary must be ONE instant across a unit: the version a merge
/// retires and the version it inserts have to meet exactly, or the entity has
/// a gap in which nothing is current. `CURRENT_TIMESTAMP()` is fixed per
/// statement here, not per transaction (pinned by the live semantics suite),
/// so the unit captures it once under this name and every statement reads it.
pub(super) const TX_TIMESTAMP_VAR: &str = "rdlt_tx_ts";

/// The statement that captures the unit's instant.
///
/// Safe inside the unit because setting a session variable does not commit the
/// transaction — which is a fact about the service and pinned as one, since
/// DDL here DOES commit and the difference is invisible in the syntax.
pub(super) fn capture_tx_timestamp() -> String {
    format!("SET {TX_TIMESTAMP_VAR} = CURRENT_TIMESTAMP()")
}

/// The alias the upsert gives its incoming rows.
///
/// `EXCLUDED` is `ON CONFLICT`'s pseudo-table and does not exist here; a
/// `MERGE` names its source, and the shared assignment list is built from
/// whatever this dialect calls it.
const INCOMING: &str = "SRC";

pub(super) struct SnowflakeDialect;

impl MergeDialect for SnowflakeDialect {
    fn quote(&self, ident: &str) -> String {
        quote(ident)
    }

    fn arrival_order(&self) -> String {
        quote(names::ARRIVAL_COL)
    }

    fn tx_timestamp(&self) -> &'static str {
        // Deliberately not `CURRENT_TIMESTAMP()`: see TX_TIMESTAMP_VAR.
        concat!("$", "rdlt_tx_ts")
    }

    fn clear_table(&self, table: &str) -> String {
        // Never TRUNCATE: it is DDL here and would commit the unit, publishing
        // a cleared table before its replacement rows landed.
        format!("DELETE FROM {table}")
    }

    fn dedup_subquery(&self, identity: &str, sort_prefix: &str, stage: &str) -> String {
        // `QUALIFY` filters after the window function, which is what makes
        // this one pass over the stage rather than a self-join against a
        // grouped subquery.
        format!(
            "(SELECT * FROM {stage} QUALIFY ROW_NUMBER() OVER \
             (PARTITION BY {identity} ORDER BY {sort_prefix}{} DESC) = 1)",
            self.arrival_order()
        )
    }

    fn flag_set(&self, column: &str) -> String {
        // `IS TRUE` is REJECTED here — a compilation error, not a subtlety —
        // so the hard-delete predicate cannot ride the shared default.
        format!("{column} = TRUE")
    }

    fn flag_unset(&self, column: &str) -> String {
        // NULL-safe, which `<> TRUE` would not be: a NULL flag means "not
        // deleted", and a predicate evaluating to NULL would drop the row from
        // the keep set and delete it.
        format!("{column} IS DISTINCT FROM TRUE")
    }

    fn incoming_ref(&self) -> &'static str {
        INCOMING
    }

    fn upsert_stmt(&self, upsert: &Upsert<'_>) -> String {
        let Upsert {
            target,
            columns,
            cols_sql,
            source,
            keep,
            keys,
            ..
        } = upsert;

        // The merge key match. Compared with `=`, not a NULL-safe operator,
        // deliberately: a NULL merge key is refused before a row is ever
        // written, so a NULL here would mean the guard failed — and silently
        // matching NULL to NULL would hide that rather than surface it.
        let on = keys
            .iter()
            .map(|key| format!("TGT.{key} = {INCOMING}.{key}"))
            .collect::<Vec<_>>()
            .join(" AND ");

        let values = columns
            .iter()
            .map(|column| format!("{INCOMING}.{column}"))
            .collect::<Vec<_>>()
            .join(", ");

        // `WHEN MATCHED` is emitted only when there is something to assign.
        // Every column being part of the key is the case that makes an empty
        // assignment list possible, and `UPDATE SET` with nothing after it is
        // a syntax error rather than a no-op.
        let matched = match &upsert.action {
            UpsertAction::Update { set } => format!(" WHEN MATCHED THEN UPDATE SET {set}"),
            UpsertAction::Nothing => String::new(),
        };

        format!(
            "MERGE INTO {target} TGT USING \
             (SELECT {cols_sql} FROM {source} deduped{keep}) {INCOMING} ON {on}{matched} \
             WHEN NOT MATCHED THEN INSERT ({cols_sql}) VALUES ({values})"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<String> {
        vec![quote("id"), quote("region"), quote("note")]
    }

    fn upsert<'a>(action: UpsertAction<'a>, keys: &'a [String], columns: &'a [String]) -> String {
        SnowflakeDialect.upsert_stmt(&Upsert {
            target: "\"DB\".\"S\".\"EVENTS\"",
            columns,
            cols_sql: "\"ID\", \"REGION\", \"NOTE\"",
            source: "(SELECT * FROM STG)",
            keep: "",
            keys,
            key_list: "\"ID\"",
            action,
        })
    }

    #[test]
    fn the_upsert_is_a_merge_because_there_is_no_on_conflict() {
        let keys = vec![quote("id")];
        let columns = cols();
        let sql = upsert(
            UpsertAction::Update {
                set: "\"NOTE\" = SRC.\"NOTE\"",
            },
            &keys,
            &columns,
        );
        assert_eq!(
            sql,
            "MERGE INTO \"DB\".\"S\".\"EVENTS\" TGT USING \
             (SELECT \"ID\", \"REGION\", \"NOTE\" FROM (SELECT * FROM STG) deduped) SRC \
             ON TGT.\"ID\" = SRC.\"ID\" \
             WHEN MATCHED THEN UPDATE SET \"NOTE\" = SRC.\"NOTE\" \
             WHEN NOT MATCHED THEN INSERT (\"ID\", \"REGION\", \"NOTE\") \
             VALUES (SRC.\"ID\", SRC.\"REGION\", SRC.\"NOTE\")"
        );
    }

    #[test]
    fn a_composite_key_matches_on_every_column() {
        let keys = vec![quote("id"), quote("region")];
        let columns = cols();
        let sql = upsert(UpsertAction::Nothing, &keys, &columns);
        assert!(
            sql.contains("ON TGT.\"ID\" = SRC.\"ID\" AND TGT.\"REGION\" = SRC.\"REGION\""),
            "{sql}"
        );
    }

    #[test]
    fn nothing_to_update_emits_no_matched_arm_at_all() {
        // `UPDATE SET` with an empty assignment list is a syntax error, not a
        // no-op, so the arm has to disappear rather than render empty.
        let keys = cols();
        let columns = cols();
        let sql = upsert(UpsertAction::Nothing, &keys, &columns);
        assert!(!sql.contains("WHEN MATCHED"), "{sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "{sql}");
    }

    #[test]
    fn the_keep_predicate_rides_the_source_subquery() {
        let keys = vec![quote("id")];
        let columns = cols();
        let sql = SnowflakeDialect.upsert_stmt(&Upsert {
            target: "\"T\"",
            columns: &columns,
            cols_sql: "\"ID\"",
            source: "(SELECT * FROM STG)",
            keep: " WHERE \"DELETED\" IS NOT TRUE",
            keys: &keys,
            key_list: "\"ID\"",
            action: UpsertAction::Nothing,
        });
        assert!(
            sql.contains("deduped WHERE \"DELETED\" IS NOT TRUE) SRC"),
            "the hard-delete keep must filter the SOURCE, not the target: {sql}"
        );
    }

    #[test]
    fn the_survivor_subquery_uses_qualify_not_distinct_on() {
        let sql = SnowflakeDialect.dedup_subquery("\"ID\"", "", "\"STG\"");
        assert!(!sql.contains("DISTINCT ON"), "{sql}");
        assert_eq!(
            sql,
            "(SELECT * FROM \"STG\" QUALIFY ROW_NUMBER() OVER \
             (PARTITION BY \"ID\" ORDER BY \"__RDLT_ARRIVAL\" DESC) = 1)"
        );
    }

    #[test]
    fn a_declared_sort_precedes_arrival_which_stays_the_tie_breaker() {
        let sql = SnowflakeDialect.dedup_subquery("\"ID\"", "\"V\" DESC NULLS LAST, ", "\"STG\"");
        assert!(
            sql.contains("ORDER BY \"V\" DESC NULLS LAST, \"__RDLT_ARRIVAL\" DESC"),
            "{sql}"
        );
    }

    #[test]
    fn clearing_a_table_never_truncates() {
        // TRUNCATE is DDL here and would commit the unit mid-flight.
        assert_eq!(SnowflakeDialect.clear_table("\"T\""), "DELETE FROM \"T\"");
    }

    #[test]
    fn the_boundary_instant_is_read_from_the_unit_not_from_the_clock() {
        assert_eq!(SnowflakeDialect.tx_timestamp(), "$rdlt_tx_ts");
        assert_eq!(capture_tx_timestamp(), "SET rdlt_tx_ts = CURRENT_TIMESTAMP()");
        // The capture must not be classified as DDL, or the unit executor
        // would refuse the statement the unit needs to run first.
        assert!(!super::super::client::is_ddl(&capture_tx_timestamp()));
    }

    #[test]
    fn identifiers_and_arrival_follow_the_quoted_upper_policy() {
        assert_eq!(SnowflakeDialect.quote("events"), "\"EVENTS\"");
        assert_eq!(SnowflakeDialect.arrival_order(), "\"__RDLT_ARRIVAL\"");
    }
}
