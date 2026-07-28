//! Connecting to Snowflake and running statements against it.
//!
//! This module is also the crate's ONE boundary with the Snowflake library:
//! every library type stops here, so replacing the library is a change to this
//! file rather than to the destination. What crosses out is a plain
//! [`Executor`], SPI errors, and strings.
//!
//! It holds three things a caller should not have to think about separately —
//! how a session is opened from configuration (every authentication method
//! mapped in one place), how a statement is run, and how a library error
//! becomes one of the SPI's three classifications.
//!
//! [`DmlOnly`] lives here rather than with the commit protocol because it is a
//! property of the executor, not of the unit: it wraps ANY executor and
//! refuses the statements that would end a transaction early.

use async_trait::async_trait;
use rdlt_connector::DestinationError;
use snowflake_connector_rs::{
    AuthConfig, Client, ClientConfig, DynamicRow, EndpointConfig, Error as SfError, ErrorKind,
    KeyPairConfig, PasswordConfig, Session, SessionConfig,
};

use super::config::{Auth, SnowflakeConfig};

/// Snowflake's code for a MERGE whose source held duplicate rows for one
/// target key.
///
/// The analogue of a unique-violation elsewhere: this service enforces no
/// unique constraints, so a duplicate key surfaces when the MERGE runs rather
/// than when an index is built. Callers turn it into the shared
/// duplicate-merge-key diagnosis.
pub(crate) const DUPLICATE_ROW_IN_DML: &str = "100090";

/// Server codes worth retrying.
///
/// Deliberately a SHORT allowlist rather than a guess at the whole space: an
/// unrecognised server error is a SQL or schema problem far more often than a
/// blip, and retrying those wastes the budget without changing the outcome.
/// Codes join this list when they are observed to be recoverable, not when
/// they look like they might be.
const TRANSIENT_SERVER_CODES: &[&str] = &[
    // The statement was aborted because its warehouse was resized, suspended
    // or resumed underneath it — the same statement on the resumed warehouse
    // succeeds.
    "000629", "000630",
    // Concurrent DDL on the same object; the retry serialises behind it.
    "000625",
];

/// Translate a library error into the SPI taxonomy.
///
/// Classification reads `ErrorKind` and, for server errors, the structured
/// code — never the rendered message, which is free to change with a service
/// release and would take the retry policy with it.
pub(crate) fn classify(err: SfError) -> DestinationError {
    let transient = match err.kind() {
        // A dropped connection, a timeout, or a session the service retired:
        // the work is unchanged and the next attempt is a fresh one.
        ErrorKind::Network | ErrorKind::Timeout | ErrorKind::SessionExpired => true,
        ErrorKind::Server => err
            .snowflake_code()
            .is_some_and(|code| TRANSIENT_SERVER_CODES.contains(&code)),
        // Config, Auth, Cancelled, Protocol, BindEncode, Decode, Internal,
        // Other: retrying cannot change any of these. Auth in particular is
        // pre-SQL and carries no server code, which is why kind — not code —
        // has to be the discriminator.
        _ => false,
    };
    if transient {
        DestinationError::transient(err)
    } else {
        DestinationError::fatal(err)
    }
}

/// The structured code carried by an already-classified error.
///
/// The classification keeps the library error as the SPI error's source, so
/// the code stays reachable without the library type escaping this module —
/// which is what lets the merge path recognise a duplicate key by code rather
/// than by matching text.
pub(crate) fn code_in(err: &DestinationError) -> Option<String> {
    let source: &(dyn std::error::Error + 'static) = match err {
        DestinationError::Fatal(e) | DestinationError::Transient(e) => e.as_ref(),
        DestinationError::RateLimited { source, .. } => source.as_ref(),
        _ => return None,
    };
    source
        .downcast_ref::<SfError>()
        .and_then(|e| e.snowflake_code())
        .map(str::to_owned)
}

/// Does this statement commit the open transaction as a side effect?
///
/// Snowflake auto-commits on DDL, so a schema statement issued inside a unit
/// silently publishes everything written before it — turning a partial unit
/// into a durable one and breaking exactly-once without any error. The unit
/// executor refuses these rather than trusting call sites to remember.
///
/// `TRUNCATE` is the trap worth naming: it reads as data manipulation and is
/// classified as DDL, so the Replace clear must use `DELETE FROM` instead.
pub(crate) fn is_ddl(sql: &str) -> bool {
    const DDL_VERBS: &[&str] = &[
        "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "COMMENT", "USE", "UNDROP",
        "RENAME",
    ];
    // `split_whitespace` already skips leading whitespace, so a YAML-indented
    // or newline-prefixed statement is classified on its real first word.
    let first = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    DDL_VERBS.contains(&first.as_str())
}

/// What a caller may run against the service.
///
/// A trait rather than the session directly, so the statement-economy tests
/// can count and script statements without a warehouse — and so the
/// unit-scoped wrapper below can refuse DDL by wrapping any executor.
#[async_trait]
pub(crate) trait Executor: Send + Sync {
    /// Run a statement, discarding any result.
    async fn execute(&self, sql: &str) -> Result<(), DestinationError>;

    /// Run a statement and read one integer from the first column of the
    /// first row — the shape every probe in the commit protocol needs.
    async fn scalar_u64(&self, sql: &str, binds: &[&str]) -> Result<u64, DestinationError>;

    /// Run a statement and read its first column as a set of names — the
    /// shape the catalog read needs.
    async fn column_names(
        &self,
        sql: &str,
        binds: &[&str],
    ) -> Result<std::collections::BTreeSet<String>, DestinationError>;
}

/// A live session.
pub(crate) struct SessionExecutor {
    session: Session,
}

#[async_trait]
impl Executor for SessionExecutor {
    async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
        self.session
            .query(sql.to_owned())
            .await
            .map(|_| ())
            .map_err(classify)
    }

    async fn scalar_u64(&self, sql: &str, binds: &[&str]) -> Result<u64, DestinationError> {
        let table = self.query(sql, binds).await?;
        let value = table
            .rows::<DynamicRow>()
            .map_err(classify)?
            .next()
            .transpose()
            .map_err(classify)?
            .and_then(|row| row.value_at(0).ok().and_then(cell_as_u64));
        value.ok_or_else(|| {
            DestinationError::fatal(format!(
                "snowflake: expected one integer from `{sql}`, got nothing usable"
            ))
        })
    }

    async fn column_names(
        &self,
        sql: &str,
        binds: &[&str],
    ) -> Result<std::collections::BTreeSet<String>, DestinationError> {
        let table = self.query(sql, binds).await?;
        let mut out = std::collections::BTreeSet::new();
        for row in table.rows::<DynamicRow>().map_err(classify)? {
            let row = row.map_err(classify)?;
            if let Ok(cell) = row.value_at(0)
                && let Some(name) = cell_as_text(cell)
            {
                out.insert(name);
            }
        }
        Ok(out)
    }
}

impl SessionExecutor {
    /// Run a query and collect its result.
    ///
    /// Binds are positional. The library's builder is a typestate — the first
    /// bind changes the statement's type — so the first is applied before the
    /// rest, and the values are owned before the await because a borrowed bind
    /// cannot outlive the frame the future is polled from.
    async fn query(
        &self,
        sql: &str,
        binds: &[&str],
    ) -> Result<snowflake_connector_rs::ResultTable, DestinationError> {
        let owned: Vec<String> = binds.iter().map(|b| (*b).to_owned()).collect();
        let statement = snowflake_connector_rs::Statement::from(sql.to_owned());
        let cursor = match owned.split_first() {
            None => self.session.query(statement).await,
            Some((first, rest)) => {
                let mut bound = statement.bind(first.clone());
                for bind in rest {
                    bound = bound.bind(bind.clone());
                }
                self.session.query(bound).await
            }
        };
        cursor
            .map_err(classify)?
            .collect_table()
            .await
            .map_err(classify)
    }
}

/// Read a cell as an unsigned count.
///
/// Counts arrive as Snowflake NUMBERs, which the library may hand back as an
/// integer or as a fixed-point decimal depending on the query; both are read
/// here so a probe never fails on the representation.
fn cell_as_text(cell: &snowflake_connector_rs::CellValue) -> Option<String> {
    use snowflake_connector_rs::CellValue;
    match cell {
        CellValue::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn cell_as_u64(cell: &snowflake_connector_rs::CellValue) -> Option<u64> {
    use snowflake_connector_rs::CellValue;
    match cell {
        CellValue::Integer(v) => u64::try_from(*v).ok(),
        CellValue::Decimal(d) => d.to_string().parse().ok(),
        CellValue::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// An executor that refuses DDL.
///
/// Wraps the unit's executor for the duration of a unit transaction. The
/// refusal is code rather than convention because the failure it prevents is
/// silent: a `CREATE` inside the unit would commit the rows written before it
/// and leave exactly-once broken with nothing to see.
pub(crate) struct DmlOnly<'a>(pub(crate) &'a dyn Executor);

#[async_trait]
impl Executor for DmlOnly<'_> {
    async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
        if is_ddl(sql) {
            return Err(ddl_in_unit(sql));
        }
        self.0.execute(sql).await
    }

    async fn scalar_u64(&self, sql: &str, binds: &[&str]) -> Result<u64, DestinationError> {
        if is_ddl(sql) {
            return Err(ddl_in_unit(sql));
        }
        self.0.scalar_u64(sql, binds).await
    }

    async fn column_names(
        &self,
        sql: &str,
        binds: &[&str],
    ) -> Result<std::collections::BTreeSet<String>, DestinationError> {
        if is_ddl(sql) {
            return Err(ddl_in_unit(sql));
        }
        self.0.column_names(sql, binds).await
    }
}

fn ddl_in_unit(sql: &str) -> DestinationError {
    let verb = sql.split_whitespace().next().unwrap_or("");
    DestinationError::fatal(format!(
        "snowflake: `{verb}` cannot run inside a commit unit — it would commit the \
         transaction and publish rows the unit had not finished writing; schema \
         work belongs before the unit opens"
    ))
}

/// Open a session from configuration.
///
/// Every authentication method the config vocabulary accepts is mapped here,
/// and nowhere else.
pub(crate) async fn connect(
    config: &SnowflakeConfig,
) -> Result<Box<dyn Executor>, DestinationError> {
    let auth = auth_config(&config.auth)?;
    let mut client = ClientConfig::new(config.user.clone(), config.account.clone(), auth);

    // The derived host is replaced only when the user asked for it; a
    // PrivateLink deployment fronts the account under its own name.
    if config.host.is_some() {
        let url = format!("https://{}", config.host())
            .parse()
            .map_err(|e| DestinationError::fatal(format!("snowflake: `host` is not a URL: {e}")))?;
        client = client.with_endpoint(EndpointConfig::custom_base_url(url));
    }

    let mut session = SessionConfig::new()
        .with_database(config.database.clone())
        .with_schema(config.schema.clone());
    if let Some(warehouse) = &config.warehouse {
        session = session.with_warehouse(warehouse.clone());
    }
    if let Some(role) = &config.role {
        session = session.with_role(role.clone());
    }
    for (key, value) in &config.session_parameters {
        session = session.with_session_parameter(key.clone(), value.clone().into());
    }
    if let Some(tag) = &config.query_tag {
        session = session.with_session_parameter("QUERY_TAG", tag.clone().into());
    }

    let client = Client::new(client.with_session(session)).map_err(classify)?;
    Ok(Box::new(SessionExecutor {
        session: client.create_session().await.map_err(classify)?,
    }))
}

/// Map the config's auth block onto the library's.
///
/// A programmatic access token rides the PASSWORD channel — verified against
/// a live account rather than assumed, and it is NOT accepted on the OAuth
/// channel, which rejects it.
fn auth_config(auth: &Auth) -> Result<AuthConfig, DestinationError> {
    if let Some(key_pair) = &auth.key_pair {
        let pem = key_pair.private_key.read_to_string().map_err(|e| {
            DestinationError::fatal(format!(
                "snowflake: `auth.key_pair.private_key` unreadable: {e}"
            ))
        })?;
        return Ok(match &key_pair.passphrase {
            Some(passphrase) => AuthConfig::key_pair(KeyPairConfig::from_encrypted_pem(
                pem,
                passphrase.reveal().as_bytes().to_vec(),
            )),
            None => AuthConfig::key_pair(KeyPairConfig::from_pem(pem)),
        });
    }
    if let Some(password) = &auth.password {
        let mut config = PasswordConfig::from(password.password.reveal().to_owned());
        if let Some(passcode) = &password.passcode {
            config = config.with_passcode(passcode.reveal().to_owned());
        }
        return Ok(AuthConfig::password(config));
    }
    if let Some(token) = &auth.oauth_token {
        return Ok(AuthConfig::oauth(token.reveal().to_owned()));
    }
    if let Some(pat) = &auth.pat {
        return Ok(AuthConfig::password(pat.reveal().to_owned()));
    }
    // Unreachable through a validated config; the config's own validation
    // rejects an empty auth block with a message naming the alternatives.
    Err(DestinationError::fatal(
        "snowflake: `auth` names no authentication method",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_verbs_are_recognised_whatever_the_spacing_or_case() {
        for sql in [
            "CREATE TABLE t (id NUMBER)",
            "  create table t (id NUMBER)",
            "\n\tALTER TABLE t ADD COLUMN c NUMBER",
            "drop table t",
            "GRANT SELECT ON t TO ROLE r",
            "USE SCHEMA s",
        ] {
            assert!(is_ddl(sql), "must be DDL: {sql}");
        }
    }

    #[test]
    fn truncate_is_ddl_here_which_is_why_replace_deletes_instead() {
        // The trap this guard exists for: TRUNCATE reads as data manipulation
        // but commits the open transaction, and the Replace clear runs INSIDE
        // the unit.
        assert!(is_ddl("TRUNCATE TABLE \"EVENTS\""));
        assert!(!is_ddl("DELETE FROM \"EVENTS\""));
    }

    #[test]
    fn the_statements_a_unit_actually_runs_are_not_ddl() {
        for sql in [
            "INSERT INTO \"T\" (\"ID\") VALUES (1)",
            "MERGE INTO \"T\" USING (SELECT 1) s ON s.x = \"T\".x WHEN MATCHED THEN UPDATE SET a = 1",
            "DELETE FROM \"T\" WHERE id = 1",
            "SELECT count(*) FROM \"T\"",
            "COPY INTO \"T\" FROM @stage",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
        ] {
            assert!(!is_ddl(sql), "must not be DDL: {sql}");
        }
    }

    #[test]
    fn an_empty_statement_is_not_mistaken_for_ddl() {
        assert!(!is_ddl(""));
        assert!(!is_ddl("   \n  "));
    }

    /// An executor that records what it was asked to run.
    struct Recorder {
        seen: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Executor for Recorder {
        async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
            self.seen.lock().expect("lock").push(sql.to_owned());
            Ok(())
        }
        async fn scalar_u64(&self, sql: &str, _: &[&str]) -> Result<u64, DestinationError> {
            self.seen.lock().expect("lock").push(sql.to_owned());
            Ok(0)
        }
        async fn column_names(
            &self,
            sql: &str,
            _: &[&str],
        ) -> Result<std::collections::BTreeSet<String>, DestinationError> {
            self.seen.lock().expect("lock").push(sql.to_owned());
            Ok(std::collections::BTreeSet::new())
        }
    }

    #[tokio::test]
    async fn a_unit_executor_refuses_ddl_and_never_forwards_it() {
        let inner = Recorder {
            seen: std::sync::Mutex::new(Vec::new()),
        };
        let unit = DmlOnly(&inner);

        let err = unit
            .execute("CREATE TABLE \"T\" (\"ID\" NUMBER)")
            .await
            .expect_err("DDL inside a unit is refused");
        let rendered = format!("{err}");
        assert!(rendered.contains("CREATE"), "names the verb: {rendered}");
        assert!(
            rendered.contains("commit the transaction"),
            "explains the consequence: {rendered}"
        );

        // The refusal must happen BEFORE the statement reaches the service —
        // a statement that ran and was then complained about has already done
        // the damage.
        assert!(
            inner.seen.lock().expect("lock").is_empty(),
            "the refused statement must not be forwarded"
        );

        unit.execute("INSERT INTO \"T\" VALUES (1)")
            .await
            .expect("DML passes through");
        assert_eq!(inner.seen.lock().expect("lock").len(), 1);
    }

    #[tokio::test]
    async fn the_refusal_covers_the_query_path_too() {
        let inner = Recorder {
            seen: std::sync::Mutex::new(Vec::new()),
        };
        let unit = DmlOnly(&inner);
        unit.scalar_u64("TRUNCATE TABLE \"T\"", &[])
            .await
            .expect_err("a probe may not smuggle DDL past the guard");
        assert!(inner.seen.lock().expect("lock").is_empty());
    }
}
