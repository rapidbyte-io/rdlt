//! Connecting, running statements, and classifying failures — against the
//! real service.
//!
//! Credential-gated: without the documented convention every cell here
//! returns early, which counts as a pass. That is the hazard the container
//! legs carry too — a skipping test proves nothing — so these confirm against
//! reality what the container-free cells already demonstrate.
//!
//! What only a live service can establish is exactly what is checked here:
//! that error CLASSIFICATION is right. A mock can assert we map `ErrorKind`
//! faithfully; only Snowflake can say which kind it actually returns for a
//! wrong password, a missing table, or a duplicate merge key.

use rdlt_connector::DestinationError;
use rdlt_connector::PemSource;
use rdlt_connector_snowflake::dest::testhook::{
    DUPLICATE_ROW_IN_DML, classify_live_error, connect_and_run,
};
use rdlt_connector_snowflake::dest::{Auth, KeyPair, Password, SnowflakeConfig};
use rdlt_testkit::snowflake::{TokenKind, credentials, scratch_schema, token};

/// A config for the qual account, authenticating by key pair.
fn key_pair_config() -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    let doc = serde_json::json!({
        "account": creds.account,
        "user": creds.user,
        "database": creds.database,
        "schema": "PUBLIC",
        "warehouse": creds.warehouse,
        "role": creds.role,
        "auth": {"key_pair": {
            "private_key": creds.private_key_path,
            "passphrase": creds.passphrase,
        }},
    });
    Some(SnowflakeConfig::from_value(doc).expect("the convention yields a valid config"))
}

#[tokio::test]
async fn key_pair_auth_reaches_the_account() {
    let Some(config) = key_pair_config() else {
        return;
    };
    // A countable query: the protocol's probes are all counts, so this
    // exercises the same read path they use. Executing at all is the proof —
    // an unauthenticated session cannot run a statement.
    let answer = connect_and_run(&config, "SELECT 1")
        .await
        .expect("key-pair auth connects");
    assert_eq!(answer, "1");
}

#[tokio::test]
async fn a_wrong_secret_is_fatal_and_never_echoes_the_secret() {
    let Some(mut config) = key_pair_config() else {
        return;
    };
    // Password auth with a value that is definitely not one. The point is the
    // SHAPE: authentication failure must be Fatal (retrying cannot fix a bad
    // credential) and must not put the credential in the message.
    const WRONG: &str = "not-the-password-9c1f";
    config.auth = Auth::password(Password::new(WRONG));
    let err = connect_and_run(&config, "SELECT 1")
        .await
        .expect_err("a wrong credential is refused");
    assert!(
        matches!(err, DestinationError::Fatal(_)),
        "authentication failure is not retryable: {err:?}"
    );
    let rendered = format!("{err}");
    assert!(
        !rendered.contains(WRONG),
        "the credential must not reach the error text: {rendered}"
    );
}

#[tokio::test]
async fn a_rotated_key_is_fatal_naming_the_identity_and_never_the_key() {
    let Some(mut config) = key_pair_config() else {
        return;
    };
    // A well-formed key the account has never seen — a rotated key, from the
    // pipeline's point of view. Generated here rather than committed: a real
    // key in the repository is exactly what the credential convention exists
    // to prevent, even a discarded one.
    const STRANGER: &str = "-----BEGIN PRIVATE KEY-----\n\
        MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg8vMlKRUAAAAAAAAAA\n\
        -----END PRIVATE KEY-----\n";
    config.auth = Auth::key_pair(KeyPair::new(PemSource::from(STRANGER)));

    let err = connect_and_run(&config, "SELECT 1")
        .await
        .expect_err("a key the account does not know is refused");
    assert!(
        matches!(err, DestinationError::Fatal(_)),
        "a rotated key cannot heal on retry: {err:?}"
    );
    let rendered = format!("{err}");
    // WHICH credential to fix is the operator's next question, and the answer
    // is in the configuration rather than in the service's reply.
    assert!(
        rendered.contains(&config.user) && rendered.contains(&config.account),
        "the failure must name the identity it was attempted under: {rendered}"
    );
    // And the key itself must be nowhere near it.
    for fragment in ["BEGIN PRIVATE KEY", "MIGHAgEAMBMGByqGSM49"] {
        assert!(
            !rendered.contains(fragment),
            "key material reached the error text: {rendered}"
        );
    }
}

#[tokio::test]
async fn a_suspended_warehouse_resumes_within_the_statement() {
    let Some(config) = key_pair_config() else {
        return;
    };
    let Some(warehouse) = config.warehouse.clone() else {
        // No warehouse configured means the account default applies and this
        // cell has nothing to suspend; skipping is honest, unlike suspending
        // something the pipeline does not use.
        return;
    };
    // Suspension is the state a cost-managed account spends most of its time
    // in. Auto-resume makes the next statement slow, not failed — and "slow"
    // must stay inside the connector's own timeouts rather than surfacing as
    // a transient error the engine then retries against a warehouse that is
    // already resuming.
    let suspend =
        connect_and_run(&config, &format!("ALTER WAREHOUSE \"{warehouse}\" SUSPEND")).await;
    // Suspending an already-suspended warehouse is an error, and an
    // unprivileged role cannot suspend at all; neither says anything about
    // the property under test, so the resume is what is asserted.
    drop(suspend);

    let answer = connect_and_run(&config, "SELECT 1")
        .await
        .expect("a suspended warehouse resumes rather than failing the statement");
    assert_eq!(answer, "1");
}

#[tokio::test]
async fn a_missing_object_is_fatal_rather_than_retried() {
    let Some(config) = key_pair_config() else {
        return;
    };
    let err = connect_and_run(&config, "SELECT * FROM RDLT_NO_SUCH_TABLE_5f2a")
        .await
        .expect_err("a missing table is an error");
    assert!(
        matches!(err, DestinationError::Fatal(_)),
        "a SQL error is not worth retrying: {err:?}"
    );
}

#[tokio::test]
async fn a_pat_authenticates_on_the_password_channel() {
    // Gated on its OWN credential: no PAT skips this leg alone.
    let (Some(mut config), Some(pat)) = (key_pair_config(), token(TokenKind::Pat)) else {
        return;
    };
    config.auth = Auth::pat(pat);
    let answer = connect_and_run(&config, "SELECT 1")
        .await
        .expect("a PAT authenticates on the password channel");
    assert_eq!(answer, "1");
}

#[tokio::test]
async fn a_duplicate_merge_key_carries_the_structured_code() {
    // This service enforces no unique constraints, so a duplicate key surfaces
    // when the MERGE runs. The code is what the shared diagnosis keys on —
    // matching the message text would break on any service release.
    let Some(mut config) = key_pair_config() else {
        return;
    };
    let schema = scratch_schema("dupe");
    config.schema = schema.clone();
    let base = key_pair_config().expect("credentials");

    connect_and_run(&base, &format!("CREATE SCHEMA {schema}"))
        .await
        .expect("scratch schema");
    let run = async {
        for sql in [
            format!("CREATE TABLE {schema}.T (ID NUMBER, V STRING)"),
            format!("INSERT INTO {schema}.T VALUES (1, 'a')"),
            format!("CREATE TABLE {schema}.S (ID NUMBER, V STRING)"),
            format!("INSERT INTO {schema}.S VALUES (1, 'x'), (1, 'y')"),
        ] {
            connect_and_run(&base, &sql).await?;
        }
        // An UNDEDUPED source against one target row: exactly the situation
        // the merge dialect's QUALIFY exists to prevent.
        connect_and_run(
            &base,
            &format!(
                "MERGE INTO {schema}.T USING (SELECT ID, V FROM {schema}.S) s \
                 ON T.ID = s.ID WHEN MATCHED THEN UPDATE SET V = s.V"
            ),
        )
        .await
    }
    .await;

    let err = run.expect_err("an undeduped merge source is refused");
    assert_eq!(
        classify_live_error(&err).as_deref(),
        Some(DUPLICATE_ROW_IN_DML),
        "the duplicate-row code is what the diagnosis keys on: {err}"
    );

    connect_and_run(&base, &format!("DROP SCHEMA IF EXISTS {schema}"))
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn ddl_is_refused_inside_a_unit_before_it_reaches_the_service() {
    // The guard is code, not convention, because the failure it prevents is
    // silent: DDL auto-commits, publishing rows the unit had not finished.
    let Some(config) = key_pair_config() else {
        return;
    };
    let err = rdlt_connector_snowflake::dest::testhook::run_in_unit(
        &config,
        "CREATE TABLE RDLT_SHOULD_NEVER_EXIST (X NUMBER)",
    )
    .await
    .expect_err("DDL inside a unit is refused");
    let rendered = format!("{err}");
    assert!(rendered.contains("CREATE"), "{rendered}");
    assert!(rendered.contains("commit the transaction"), "{rendered}");

    // And it never ran: the table must not exist.
    let count = connect_and_run(
        &config,
        "SELECT count(*) FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_NAME = 'RDLT_SHOULD_NEVER_EXIST'",
    )
    .await
    .expect("catalog query");
    assert_eq!(count.trim(), "0", "the refused DDL must not have executed");
}

#[tokio::test]
async fn ensure_reads_the_catalog_once_and_then_emits_nothing() {
    // The requirement the whole ddl module exists for, checked against the
    // real catalog rather than a hand-built image: a steady-state load must
    // issue ZERO schema statements.
    use rdlt_connector::core::{
        ColumnDef, ColumnType, LogicalType, Provenance, TableName, TableSchema, WriteMode,
    };
    use rdlt_connector_snowflake::dest::TableType;
    use rdlt_connector_snowflake::dest::testhook::{
        Catalog, apply, ensure_table_sql, read_catalog,
    };

    let Some(base) = key_pair_config() else {
        return;
    };
    let schema_name = scratch_schema("ensure");
    connect_and_run(&base, &format!("CREATE SCHEMA {schema_name}"))
        .await
        .expect("scratch schema");

    let mut config = key_pair_config().expect("credentials");
    config.schema = schema_name.clone();

    let table = TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: vec![
            ColumnDef {
                name: "id".to_owned(),
                column_type: ColumnType::scalar(LogicalType::Int64),
                nullable: false,
                provenance: Provenance::Inferred,
            },
            ColumnDef {
                name: "note".to_owned(),
                column_type: ColumnType::scalar(LogicalType::Utf8),
                nullable: true,
                provenance: Provenance::Inferred,
            },
        ],
    };

    let result = async {
        // First ensure: the catalog knows nothing, so the table is created.
        let mut catalog = Catalog::default();
        catalog.observe("events", read_catalog(&config, "events").await?);
        let first = ensure_table_sql(
            "p",
            &table,
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &catalog,
        );
        assert_eq!(first.len(), 1, "one CREATE: {first:?}");
        for sql in &first {
            apply(&config, sql).await?;
        }

        // Second ensure, reading the catalog fresh: the table now matches, so
        // there is nothing to emit at all.
        let mut catalog = Catalog::default();
        let columns = read_catalog(&config, "events").await?;
        assert!(
            columns.contains("ID") && columns.contains("NOTE"),
            "the catalog reports the columns upper case: {columns:?}"
        );
        catalog.observe("events", columns);
        let second = ensure_table_sql(
            "p",
            &table,
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &catalog,
        );
        assert!(
            second.is_empty(),
            "a steady-state ensure issues no statements: {second:?}"
        );

        // A grown schema emits exactly the one missing column.
        let mut grown = table.clone();
        grown.columns.push(ColumnDef {
            name: "added".to_owned(),
            column_type: ColumnType::scalar(LogicalType::Bool),
            nullable: true,
            provenance: Provenance::Inferred,
        });
        let third = ensure_table_sql(
            "p",
            &grown,
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &catalog,
        );
        assert_eq!(third.len(), 1, "exactly the missing column: {third:?}");
        assert!(third[0].contains("\"ADDED\""), "{}", third[0]);
        apply(&config, &third[0]).await?;

        // And the unquoted name a user would type resolves to what was
        // written — the reason identifiers are emitted quoted-UPPER.
        let count = connect_and_run(
            &config,
            &format!("SELECT count(*) FROM {schema_name}.events"),
        )
        .await?;
        assert_eq!(count, "0");
        Ok::<(), rdlt_connector::DestinationError>(())
    }
    .await;

    connect_and_run(&base, &format!("DROP SCHEMA IF EXISTS {schema_name}"))
        .await
        .expect("cleanup");
    result.expect("the ensure sequence succeeds");
}

/// An upload that partly fails reports SUCCESS overall.
///
/// The hazard the per-part verification exists for, and the one the fork has no
/// test covering. A transfer matching several files returns an error only when
/// EVERY file failed; when some succeed and some do not, it returns success
/// with the failure visible only in an individual row. A caller that runs the
/// statement and discards its rows — which is the shape of every other
/// statement this connector issues — would lose data and report none.
#[tokio::test]
async fn a_partly_failed_upload_reports_success_and_hides_the_failure_in_a_row() {
    let Some(config) = key_pair_config() else {
        return;
    };
    let schema = scratch_schema("putpartial");
    let database = config.database.to_uppercase();
    let mut scoped = config.clone();
    scoped.schema = schema.clone();

    // Two local files, one of them unreadable. The glob matches both, so the
    // transfer has something to succeed at and something to fail at.
    let dir = std::env::temp_dir().join(format!("rdlt-put-partial-{schema}"));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let good = dir.join("a_readable.csv");
    let bad = dir.join("b_unreadable.csv");
    std::fs::write(&good, b"1,alpha\n").expect("write");
    std::fs::write(&bad, b"2,beta\n").expect("write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    }

    let outcome = async {
        connect_and_run(
            &config,
            &format!("CREATE SCHEMA \"{database}\".\"{schema}\""),
        )
        .await?;
        connect_and_run(&scoped, "CREATE STAGE PARTIAL_PROBE").await?;
        rdlt_connector_snowflake::dest::testhook::rows(
            &scoped,
            &format!(
                "PUT 'file://{}/*.csv' @PARTIAL_PROBE AUTO_COMPRESS=FALSE",
                dir.display()
            ),
            &["source", "status", "message"],
        )
        .await
    }
    .await;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&bad, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::remove_dir_all(&dir).ok();
    let _ = connect_and_run(
        &config,
        &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{schema}\""),
    )
    .await;

    let rows = outcome.expect(
        "a partly failed upload must return SUCCESS — if this now returns an error, the \
         per-part verification can be simplified, but that must be a decision rather than \
         a surprise",
    );
    assert_eq!(rows.len(), 2, "both files are reported: {rows:?}");
    let statuses: Vec<&str> = rows.iter().map(|r| r[1].as_str()).collect();
    assert!(
        statuses.contains(&"UPLOADED"),
        "one file uploaded: {rows:?}"
    );
    assert!(
        statuses.iter().any(|s| *s != "UPLOADED"),
        "one file failed, and ONLY the row says so: {rows:?}"
    );
}
