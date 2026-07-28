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
use rdlt_connector_snowflake::dest::testhook::{
    DUPLICATE_ROW_IN_DML, classify_live_error, connect_and_run,
};
use rdlt_connector_snowflake::dest::{Auth, Password, SnowflakeConfig};
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
