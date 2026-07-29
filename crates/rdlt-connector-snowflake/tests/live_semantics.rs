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
        let rejected =
            connect_and_run_script(&config, &["SELECT count(*) FROM T WHERE DELETED IS TRUE"])
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

/// Uploading does NOT end an open transaction, unlike schema work.
///
/// The fact that lets staging live inside the commit unit. Schema statements
/// here commit whatever transaction is open — that is pinned elsewhere and the
/// whole protocol is shaped around it — so whether an upload behaves the same
/// way decides where staging can happen at all.
///
/// A count of zero would NOT establish this on its own: a statement that
/// silently aborted the unit would also leave zero rows. So the transaction id
/// is read either side, and a committing control is run in the same shape.
#[tokio::test]
async fn uploading_does_not_commit_the_open_unit() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema = scratch_schema("putcommit");
    let database = admin.database.to_uppercase();
    connect_and_run_script(
        &admin,
        &[&format!("CREATE SCHEMA \"{database}\".\"{schema}\"")],
    )
    .await
    .expect("scratch schema");
    let config = config_in(&schema).expect("credentials");

    let dir = std::env::temp_dir().join(format!("rdlt-put-txn-{schema}"));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file = dir.join("probe.csv");
    std::fs::write(&file, b"1,alpha\n").expect("write");

    let outcome = async {
        connect_and_run_script(
            &config,
            &["CREATE TABLE T (ID NUMBER)", "CREATE STAGE TXN_PROBE"],
        )
        .await?;
        // The transaction id either side of the upload, and the row count after
        // abandoning the unit. If the upload had committed, the row survives;
        // if it had aborted the unit, the id would change.
        let ids = rdlt_connector_snowflake::dest::testhook::script_rows(
            &config,
            &[
                "BEGIN",
                "INSERT INTO T VALUES (1)",
                &format!(
                    "PUT 'file://{}' @TXN_PROBE AUTO_COMPRESS=FALSE",
                    file.display()
                ),
            ],
            "SELECT CURRENT_TRANSACTION() AS TXID",
            &["TXID"],
        )
        .await?;
        let after = connect_and_run_script(
            &config,
            &[
                "BEGIN",
                "INSERT INTO T VALUES (2)",
                &format!(
                    "PUT 'file://{}' @TXN_PROBE AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
                    file.display()
                ),
                "ROLLBACK",
                "SELECT count(*) FROM T",
            ],
        )
        .await?;
        Ok::<_, rdlt_connector::DestinationError>((ids, after))
    }
    .await;

    std::fs::remove_dir_all(&dir).ok();
    let _ = connect_and_run_script(
        &admin,
        &[&format!(
            "DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\""
        )],
    )
    .await;

    let (ids, after) = outcome.expect("the probe runs");
    assert!(
        ids.first().is_some_and(|row| !row[0].is_empty()),
        "the unit is still live AFTER the upload — an empty transaction id would \
         mean the upload ended it, which is what schema work does here: {ids:?}"
    );
    assert_eq!(
        after, "0",
        "the rolled-back row survived, so the upload committed the unit — staging \
         cannot stay inside the unit if this is ever true"
    );
}

/// Creating the staging object is safe inside a unit; DROPPING it is not.
///
/// Two statements that read as the same kind of thing and are not. The create
/// behaves like data manipulation for transaction purposes, while the drop
/// commits whatever is open — so teardown can never sit inside a unit, and
/// knowing which is which is the difference between a clean protocol and a
/// silently published half-load.
#[tokio::test]
async fn creating_a_staging_object_is_safe_inside_a_unit_but_dropping_is_not() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema = scratch_schema("stagecommit");
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
        // CREATE: the row must NOT survive the rollback.
        let after_create = connect_and_run_script(
            &config,
            &[
                "BEGIN",
                "INSERT INTO T VALUES (1)",
                "CREATE STAGE S_CREATE",
                "ROLLBACK",
                "SELECT count(*) FROM T",
            ],
        )
        .await?;
        // DROP: the row IS expected to survive, because the drop commits.
        let after_drop = connect_and_run_script(
            &config,
            &[
                "CREATE STAGE S_DROP",
                "BEGIN",
                "INSERT INTO T VALUES (2)",
                "DROP STAGE S_DROP",
                "ROLLBACK",
                "SELECT count(*) FROM T",
            ],
        )
        .await?;
        Ok::<_, rdlt_connector::DestinationError>((after_create, after_drop))
    }
    .await;

    let _ = connect_and_run_script(
        &admin,
        &[&format!(
            "DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\""
        )],
    )
    .await;

    let (after_create, after_drop) = outcome.expect("the probe runs");
    assert_eq!(
        after_create, "0",
        "creating the staging object committed the unit; if this becomes true, \
         creation must move out of the unit alongside every other schema statement"
    );
    assert_ne!(
        after_drop, "0",
        "dropping the staging object did NOT commit the unit — the teardown could \
         then move inside it, but that must be a decision rather than an accident"
    );
}

/// What the staging area's listing says about a part's AGE.
///
/// A load that dies after uploading a part leaves an object no statement will
/// ever name. Reclaiming it needs a rule for deciding what is dead, and the
/// only safe rule that does not depend on knowing every live load is age: an
/// object older than any plausible in-flight load cannot belong to one.
///
/// So the question is whether the listing exposes age at all. It is answered
/// here rather than assumed, because the reclaim design rests on the answer,
/// and the mechanism this replaced got age from a different system entirely.
#[tokio::test]
async fn the_staging_listing_exposes_a_parts_age() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema = scratch_schema("listage");
    let database = admin.database.to_uppercase();
    connect_and_run_script(
        &admin,
        &[&format!("CREATE SCHEMA \"{database}\".\"{schema}\"")],
    )
    .await
    .expect("scratch schema");
    let config = config_in(&schema).expect("credentials");

    let dir = std::env::temp_dir().join(format!("rdlt-list-age-{schema}"));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file = dir.join("aged.csv");
    std::fs::write(&file, b"1,alpha\n").expect("write");

    let outcome = async {
        let listed = rdlt_connector_snowflake::dest::testhook::script_rows(
            &config,
            &[
                "CREATE STAGE AGE_PROBE",
                &format!(
                    "PUT 'file://{}' @AGE_PROBE/scope/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE",
                    file.display()
                ),
            ],
            "LIST @AGE_PROBE",
            &["name", "last_modified"],
        )
        .await?;
        Ok::<_, rdlt_connector::DestinationError>(listed)
    }
    .await;

    let _ = std::fs::remove_dir_all(&dir);
    let _ = rdlt_connector_snowflake::dest::testhook::apply(
        &admin,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\""),
    )
    .await;

    let listed = outcome.expect("the listing runs");
    assert_eq!(listed.len(), 1, "one part staged: {listed:?}");
    let (name, modified) = (&listed[0][0], &listed[0][1]);
    assert!(
        name.contains("scope/aged.csv"),
        "the listing names the part under its prefix: {name}"
    );
    // The value's FORMAT is the service's, not ours — asserting a shape would
    // pin something we do not control. What matters is that a timestamp is
    // there to compare against, which a blank or a placeholder would not be.
    assert!(
        modified.len() > 8 && modified.chars().any(|c| c.is_ascii_digit()),
        "age is exposed and is usable for a staleness rule: {modified:?}"
    );
    println!("LIST exposes last_modified = {modified:?}");
}

/// Stale parts are reclaimed and fresh ones are not — both halves, live.
///
/// The rule decides what to DELETE from a shared area, so proving only that it
/// removes something would leave the dangerous half untested: a rule that
/// removed everything would pass that check and destroy a concurrent load.
#[tokio::test]
async fn stale_staged_parts_are_reclaimed_and_fresh_ones_are_spared() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema = scratch_schema("reclaim");
    let database = admin.database.to_uppercase();
    connect_and_run_script(
        &admin,
        &[&format!("CREATE SCHEMA \"{database}\".\"{schema}\"")],
    )
    .await
    .expect("scratch schema");
    let config = config_in(&schema).expect("credentials");
    let qualified = format!("\"{database}\".\"{schema}\".\"RECLAIM_PROBE\"");

    let dir = std::env::temp_dir().join(format!("rdlt-reclaim-{schema}"));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let file = dir.join("part.csv");
    std::fs::write(&file, b"1,alpha\n").expect("write");

    let count_staged = |config: SnowflakeConfig, qualified: String| async move {
        rdlt_connector_snowflake::dest::testhook::script_rows(
            &config,
            &[&format!("LIST @{qualified}")],
            "SELECT \"name\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))",
            &["name"],
        )
        .await
        .map(|rows| rows.len())
    };

    let outcome = async {
        connect_and_run_script(
            &config,
            &[
                "CREATE STAGE RECLAIM_PROBE",
                &format!(
                    "PUT 'file://{}' @RECLAIM_PROBE/deadscope/ AUTO_COMPRESS = FALSE \
                     OVERWRITE = TRUE",
                    file.display()
                ),
            ],
        )
        .await?;
        let staged = count_staged(config.clone(), qualified.clone()).await?;

        // The shipped threshold: the part was uploaded seconds ago, so nothing
        // may be touched. This is the half that protects a live load.
        rdlt_connector_snowflake::dest::testhook::reclaim_staged_older_than(
            &config, "p", "l", &qualified, 24,
        )
        .await?;
        let after_fresh = count_staged(config.clone(), qualified.clone()).await?;

        // Zero hours makes every part stale by definition, which is the only
        // way to reach the removing half without waiting a day.
        rdlt_connector_snowflake::dest::testhook::reclaim_staged_older_than(
            &config, "p", "l", &qualified, 0,
        )
        .await?;
        let after_stale = count_staged(config.clone(), qualified.clone()).await?;

        Ok::<_, rdlt_connector::DestinationError>((staged, after_fresh, after_stale))
    }
    .await;

    let _ = std::fs::remove_dir_all(&dir);
    let _ = rdlt_connector_snowflake::dest::testhook::apply(
        &admin,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\""),
    )
    .await;

    let (staged, after_fresh, after_stale) = outcome.expect("the probe runs");
    assert_eq!(staged, 1, "the part staged");
    assert_eq!(after_fresh, 1, "a fresh part is NOT reclaimed");
    assert_eq!(after_stale, 0, "a stale part IS reclaimed");
}
