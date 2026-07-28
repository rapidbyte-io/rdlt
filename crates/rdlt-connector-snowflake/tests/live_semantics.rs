//! Service behaviours the design depends on, pinned against the real account.
//!
//! Each of these is load-bearing: a decision elsewhere in the crate is only
//! correct because the service behaves this way, and none of them is
//! guessable from the SQL standard or from how the other two destinations
//! work. Pinning them here means a service release that changes one fails
//! HERE, naming the assumption, rather than silently corrupting a load.

use rdlt_connector_snowflake::dest::SnowflakeConfig;
use rdlt_connector_snowflake::dest::testhook::connect_and_run_script;
use rdlt_testkit::snowflake::{credentials, scratch_schema};

fn config_in(schema: &str) -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    let doc = serde_json::json!({
        "account": creds.account,
        "user": creds.user,
        "database": creds.database,
        "schema": schema,
        "warehouse": creds.warehouse,
        "role": creds.role,
        "auth": {"key_pair": {
            "private_key": creds.private_key_path,
            "passphrase": creds.passphrase,
        }},
    });
    Some(SnowflakeConfig::from_value(doc).expect("valid config"))
}

/// The clock moves DURING a transaction, so it cannot mark a boundary.
///
/// Postgres's `now()` is fixed at transaction start, which is what makes it a
/// usable scd2 boundary there: the row a merge retires and the row it inserts
/// share an instant exactly. Here the same expression is fixed per STATEMENT,
/// so a retire at one instant and an insert at a later one would leave the
/// entity with a gap in which no version is current — or, read the other way,
/// two versions whose ranges do not meet.
///
/// This is why the unit captures one instant into a session variable and every
/// statement in it reads that instead.
#[tokio::test]
async fn the_clock_is_not_stable_across_a_transaction() {
    let Some(config) = config_in("PUBLIC") else {
        return;
    };
    let drift = connect_and_run_script(
        &config,
        &[
            "BEGIN",
            "SET rdlt_probe_ts = CURRENT_TIMESTAMP()",
            "CALL SYSTEM$WAIT(2)",
            "SELECT DATEDIFF('millisecond', $rdlt_probe_ts, CURRENT_TIMESTAMP())",
            "ROLLBACK",
        ],
    )
    .await
    .expect("the probe runs");
    let drift: i64 = drift.parse().expect("a millisecond count");
    assert!(
        drift >= 1_000,
        "the clock advanced only {drift}ms across a two-second wait inside one \
         transaction — if it has become transaction-stable, the session variable \
         the scd2 boundary relies on is no longer needed, but the change must be \
         a decision rather than a surprise"
    );

    // And the captured instant does NOT move, which is what makes it usable.
    let stable = connect_and_run_script(
        &config,
        &[
            "BEGIN",
            "SET rdlt_probe_ts = CURRENT_TIMESTAMP()",
            "CALL SYSTEM$WAIT(2)",
            "SELECT DATEDIFF('millisecond', $rdlt_probe_ts, $rdlt_probe_ts)",
            "ROLLBACK",
        ],
    )
    .await
    .expect("the probe runs");
    assert_eq!(stable, "0", "the captured instant must not move");
}

/// `IS TRUE` is not accepted, and the NULL-safe alternative is.
///
/// The shared hard-delete predicate spells a boolean flag `IS TRUE` / `IS NOT
/// TRUE`, which reads as standard SQL and compiles nowhere here. The failure
/// is a syntax error reported at the enclosing subquery rather than at the
/// predicate, so it arrives pointing away from its cause — worth pinning the
/// fact rather than rediscovering it.
#[tokio::test]
async fn the_standard_boolean_predicate_is_rejected_and_the_null_safe_one_is_not() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema = scratch_schema("boolprobe");
    let database = admin.database.to_uppercase();
    connect_and_run_script(
        &admin,
        &[&format!("CREATE SCHEMA \"{database}\".\"{schema}\"")],
    )
    .await
    .expect("scratch schema");

    let config = config_in(&schema).expect("credentials");
    let outcome = async {
        connect_and_run_script(
            &config,
            &[
                "CREATE TABLE T (DELETED BOOLEAN)",
                "INSERT INTO T VALUES (TRUE), (FALSE), (NULL)",
            ],
        )
        .await?;
        let rejected = connect_and_run_script(&config, &["SELECT count(*) FROM T WHERE DELETED IS TRUE"])
            .await
            .is_err();
        // The flag set, and the NULL-safe complement: a NULL flag means "not
        // deleted", so it must fall on the KEEP side.
        let set = connect_and_run_script(&config, &["SELECT count(*) FROM T WHERE DELETED = TRUE"])
            .await?;
        let unset = connect_and_run_script(
            &config,
            &["SELECT count(*) FROM T WHERE DELETED IS DISTINCT FROM TRUE"],
        )
        .await?;
        Ok::<_, rdlt_connector::DestinationError>((rejected, set, unset))
    }
    .await;

    let _ = connect_and_run_script(
        &admin,
        &[&format!(
            "DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\""
        )],
    )
    .await;

    let (rejected, set, unset) = outcome.expect("the probe runs");
    assert!(
        rejected,
        "`IS TRUE` compiled — if it is now accepted, the dialect's override is \
         no longer needed, but that must be a decision rather than a surprise"
    );
    assert_eq!(set, "1", "one row carries the flag");
    assert_eq!(
        unset, "2",
        "the FALSE and the NULL rows are both kept — a NULL flag is not a deletion"
    );
}

/// Setting a session variable does NOT commit the open transaction.
///
/// DDL here does — proven, and the whole commit protocol is shaped around it.
/// The scd2 boundary needs a value captured at unit start, and capturing it
/// with `SET` is only safe because `SET` is not DDL. That is a fact about the
/// service, not an inference from the statement's shape, so it is measured:
/// were it wrong, every scd2 load would publish half a unit with no error
/// anywhere.
#[tokio::test]
async fn setting_a_session_variable_does_not_commit_the_unit() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema = scratch_schema("setprobe");
    let database = admin.database.to_uppercase();
    connect_and_run_script(
        &admin,
        &[&format!("CREATE SCHEMA \"{database}\".\"{schema}\"")],
    )
    .await
    .expect("scratch schema");

    let config = config_in(&schema).expect("credentials");
    let outcome = async {
        connect_and_run_script(&config, &["CREATE TABLE T (ID NUMBER)"]).await?;
        // Write, set the variable, then abandon the transaction. A `SET` that
        // committed would leave the row behind.
        connect_and_run_script(
            &config,
            &[
                "BEGIN",
                "INSERT INTO T VALUES (1)",
                "SET rdlt_probe_ts = CURRENT_TIMESTAMP()",
                "ROLLBACK",
                "SELECT count(*) FROM T",
            ],
        )
        .await
    }
    .await;

    let _ = connect_and_run_script(
        &admin,
        &[&format!(
            "DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\""
        )],
    )
    .await;

    assert_eq!(
        outcome.expect("the probe runs"),
        "0",
        "the rolled-back row survived, so `SET` committed the unit"
    );
}
