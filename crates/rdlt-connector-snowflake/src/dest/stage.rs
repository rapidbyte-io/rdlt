//! The bulk path: parquet parts in the user's bucket, loaded by `COPY INTO`.
//!
//! Two halves that must agree on one thing — a key. This process writes parts
//! to an S3 prefix; the account reads that same prefix back through an external
//! stage. Everything here exists to keep those two views identical and to keep
//! rdlt's keys inside a scope it owns, so a bucket shared with other data is
//! safe and cleanup can never reach beyond this pipeline.
//!
//! The credentials appear in exactly one statement — the stage definition —
//! and that statement is run through [`execute_redacted`], because a service
//! error commonly quotes the statement it failed on and this is the one
//! statement whose text is a secret.

use futures::StreamExt as _;
use object_store::{ObjectStore, aws::AmazonS3, aws::AmazonS3Builder, path::Path as ObjectPath};
use rdlt_connector::DestinationError;
use rdlt_connector::core::{crash_point, naming::ident_hash};

use super::config::S3Stage;

/// The external stage's name: rdlt's shared prefix, a marker distinguishing it
/// from the merge stage TABLES, and the pipeline it belongs to.
///
/// Pipeline-scoped so two pipelines sharing a schema never redefine each
/// other's stage, and deterministic so a later run finds the same object.
pub(super) fn stage_object_name(pipeline: &str) -> String {
    format!(
        "{}ext_{}",
        rdlt_connector_sqlcore::names::STAGE_PREFIX,
        ident_hash(pipeline, 8)
    )
}

/// One written part: the key it lives at, relative to the stage root, and how
/// many rows it holds.
///
/// The rowcount is recorded at WRITE time because that is the only moment it
/// is known for free — reading it back from the file to check the load would
/// be checking the service against itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Part {
    /// Key relative to the stage root — what `FILES = (…)` names.
    pub(super) tail: String,
    /// Rows written into it.
    pub(super) rows: u64,
}

/// A bucket prefix this pipeline owns, and the account-side stage over it.
pub(super) struct Stage {
    store: AmazonS3,
    /// Absolute key prefix of this pipeline's scope, inside the bucket.
    root: String,
    /// `s3://bucket/root/` — the URL the stage is defined over.
    url: String,
    /// Endpoint override, for S3-compatible storage.
    endpoint: Option<String>,
    /// Credentials for the stage definition, unless an integration supersedes.
    access_key: String,
    secret_key: String,
    storage_integration: Option<String>,
    /// The stage object's unqualified name.
    name: String,
    /// Never reset within a session, so a part written after a commit cannot
    /// take the name of one whose cleanup failed.
    next_part: u64,
}

impl Stage {
    /// Connect to the bucket and derive this pipeline's scope within it.
    pub(super) fn connect(options: &S3Stage, pipeline: &str) -> Result<Self, DestinationError> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&options.bucket)
            .with_region(options.region.as_deref().unwrap_or("us-east-1"))
            .with_access_key_id(options.access_key.reveal())
            .with_secret_access_key(options.secret_key.reveal())
            .with_virtual_hosted_style_request(!options.path_style);
        if let Some(endpoint) = &options.endpoint {
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }
        let store = builder.build().map_err(|e| {
            DestinationError::fatal(format!(
                "snowflake: stage bucket `{}` is unusable: {e}",
                options.bucket
            ))
        })?;

        // The scope is hashed rather than spelled: a pipeline name is free
        // text, and a key derived from it would have to be sanitised — at
        // which point two different pipelines can sanitise to the same scope
        // and start deleting each other's parts.
        let scope = ident_hash(pipeline, 12);
        let root = match options.prefix.as_deref().map(str::trim).map(|p| p.trim_matches('/')) {
            Some(prefix) if !prefix.is_empty() => format!("{prefix}/{scope}"),
            _ => scope,
        };
        let scheme = if options.endpoint.is_some() {
            "s3compat"
        } else {
            "s3"
        };
        Ok(Self {
            store,
            url: format!("{scheme}://{}/{root}/", options.bucket),
            root,
            endpoint: options.endpoint.clone(),
            access_key: options.access_key.reveal().to_owned(),
            secret_key: options.secret_key.reveal().to_owned(),
            storage_integration: options.storage_integration.clone(),
            name: stage_object_name(pipeline),
            next_part: 0,
        })
    }

    /// The stage object's unqualified name, for the caller to qualify.
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    /// The statement defining the stage, and the secrets it carries.
    ///
    /// `CREATE OR REPLACE` rather than `IF NOT EXISTS`: the definition is
    /// derived entirely from configuration, so an existing stage built from
    /// SUPERSEDED configuration must not survive. Rotating the bucket keys is
    /// the case that makes this matter — `IF NOT EXISTS` would leave the old
    /// pair in place and fail every load afterwards with an access error that
    /// points at the bucket rather than at the stale definition. A stage holds
    /// no data, so replacing it costs nothing.
    pub(super) fn create_sql(&self, qualified_name: &str) -> (String, Vec<String>) {
        let mut sql = format!("CREATE OR REPLACE STAGE {qualified_name} URL = '{}'", self.url);
        let mut secrets = Vec::new();
        match &self.storage_integration {
            Some(integration) => {
                sql.push_str(&format!(" STORAGE_INTEGRATION = {integration}"));
            }
            None => {
                sql.push_str(&format!(
                    " CREDENTIALS = ( AWS_KEY_ID = '{}' AWS_SECRET_KEY = '{}' )",
                    self.access_key, self.secret_key
                ));
                secrets.push(self.secret_key.clone());
                secrets.push(self.access_key.clone());
            }
        }
        if let Some(endpoint) = &self.endpoint {
            // The scheme-less host is what the parameter takes.
            let host = endpoint
                .split_once("://")
                .map_or(endpoint.as_str(), |(_, rest)| rest);
            sql.push_str(&format!(" ENDPOINT = '{host}'"));
        }
        (sql, secrets)
    }

    /// Write one part and record what it holds.
    pub(super) async fn put_part(
        &mut self,
        table: &str,
        bytes: Vec<u8>,
        rows: u64,
    ) -> Result<Part, DestinationError> {
        // Hashed for the same reason the scope is: a table name is free text,
        // and two tables must never resolve to one key.
        let tail = format!(
            "{}/{:08}.parquet",
            ident_hash(table, 16),
            {
                let n = self.next_part;
                self.next_part += 1;
                n
            }
        );
        crash_point!(
            "sf.stage.write",
            Err(DestinationError::fatal("injected crash at sf.stage.write"))
        );
        self.store
            .put(&self.key(&tail), bytes::Bytes::from(bytes).into())
            .await
            .map_err(|e| store_err(e, &tail))?;
        Ok(Part { tail, rows })
    }

    /// Remove parts this pipeline wrote. Best effort: the objects are dead
    /// either way, and a cleanup failure must not fail a committed load. The
    /// scope wipe at the next open collects whatever is left.
    pub(super) async fn remove(&self, parts: &[Part]) {
        for part in parts {
            let _ = self.store.delete(&self.key(&part.tail)).await;
        }
    }

    /// Delete everything under this pipeline's scope.
    ///
    /// Run at open, against exactly one pipeline's prefix: parts staged by a
    /// session that died are unreachable — no receipt names them — and a
    /// sibling pipeline's live parts are outside the prefix entirely.
    pub(super) async fn clear_scope(&self) -> Result<(), DestinationError> {
        let prefix = ObjectPath::from(self.root.clone());
        let mut listing = self.store.list(Some(&prefix));
        while let Some(entry) = listing.next().await {
            let meta = entry.map_err(|e| store_err(e, &self.root))?;
            self.store
                .delete(&meta.location)
                .await
                .map_err(|e| store_err(e, meta.location.as_ref()))?;
        }
        Ok(())
    }

    /// An absolute key from a stage-relative tail.
    fn key(&self, tail: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/{tail}", self.root))
    }
}

/// The `COPY INTO` for a set of parts.
///
/// `MATCH_BY_COLUMN_NAME` because the parts carry the arrow schema's names in
/// their own case while the catalog holds them upper — and because it makes
/// the load by NAME, so a target whose column order differs from the batch's
/// still lands correctly.
///
/// `FORCE = TRUE` deliberately: Snowflake otherwise skips files its load
/// history has seen, which is a file-level dedup with a 64-day window and no
/// knowledge of transactions. Exactly-once here comes from the unit
/// transaction and the receipt — a re-run whose unit rolled back MUST load
/// its parts again, and a `COPY` that silently skipped them would report zero
/// rows loaded and leave the target short.
pub(super) fn copy_sql(qualified_target: &str, qualified_stage: &str, parts: &[Part]) -> String {
    let files = parts
        .iter()
        .map(|p| format!("'{}'", p.tail))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "COPY INTO {qualified_target} FROM @{qualified_stage} FILES = ({files}) \
         FILE_FORMAT = ( TYPE = PARQUET ) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE \
         FORCE = TRUE PURGE = FALSE ON_ERROR = ABORT_STATEMENT"
    )
}

/// Run a statement whose text contains credentials.
///
/// The service quotes the failing statement in many of its errors, so an
/// ordinary failure here would print the key pair. On failure the rendered
/// error is rebuilt with each secret replaced; when no secret appears the
/// original error is returned untouched, keeping its structured code reachable.
pub(super) async fn execute_redacted(
    executor: &dyn super::client::Executor,
    sql: &str,
    secrets: &[String],
) -> Result<(), DestinationError> {
    executor.execute(sql).await.map_err(|err| {
        let rendered = format!("{err}");
        let redacted = redact(&rendered, secrets);
        if redacted == rendered {
            return err;
        }
        match err {
            DestinationError::Transient(_) => DestinationError::transient(redacted),
            _ => DestinationError::fatal(redacted),
        }
    })
}

/// Replace every secret with a marker.
fn redact(text: &str, secrets: &[String]) -> String {
    let mut out = text.to_owned();
    for secret in secrets {
        if !secret.is_empty() {
            out = out.replace(secret.as_str(), "****");
        }
    }
    out
}

/// Classify a store failure, naming the key it happened on.
///
/// WHETHER to retry is the SPI's one rule, not this crate's: the file
/// connector asks the same question of the same error type, and two answers
/// would drift. What belongs here is only the subject — an operator reading
/// this needs the key, which the shared rule cannot know.
fn store_err(error: object_store::Error, key: &str) -> DestinationError {
    let recoverable = rdlt_connector::objects::is_recoverable(&error);
    let message = format!("snowflake: stage object `{key}`: {error}");
    if recoverable {
        DestinationError::transient(message)
    } else {
        DestinationError::fatal(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_connector::Secret;

    fn options() -> S3Stage {
        S3Stage::new("parts", Secret::from("AK-SECRET"), Secret::from("SK-SECRET"))
    }

    fn stage() -> Stage {
        Stage::connect(&options(), "sales").expect("connect")
    }

    #[test]
    fn the_scope_is_hashed_so_two_pipelines_cannot_share_a_prefix() {
        // Sanitising free text into a key is what makes this dangerous:
        // `a/b` and `a_b` would sanitise to one prefix, and the scope wipe at
        // open would then delete the other pipeline's parts.
        let a = Stage::connect(&options(), "a/b").expect("connect");
        let b = Stage::connect(&options(), "a_b").expect("connect");
        assert_ne!(a.root, b.root);
        assert!(!a.root.contains('/'), "the hash needs no sanitising");
    }

    #[test]
    fn a_configured_prefix_contains_the_scope() {
        let stage = Stage::connect(&options().with_prefix("/warehouse/rdlt/"), "sales")
            .expect("connect");
        assert!(stage.root.starts_with("warehouse/rdlt/"), "{}", stage.root);
        assert!(stage.url.starts_with("s3://parts/warehouse/rdlt/"), "{}", stage.url);
        assert!(stage.url.ends_with('/'), "a stage URL names a prefix");
    }

    #[test]
    fn the_stage_definition_carries_credentials_and_reports_them_as_secrets() {
        let (sql, secrets) = stage().create_sql("\"DB\".\"S\".\"ST\"");
        assert!(sql.starts_with("CREATE OR REPLACE STAGE \"DB\".\"S\".\"ST\""), "{sql}");
        assert!(sql.contains("AWS_KEY_ID = 'AK-SECRET'"), "{sql}");
        // Every secret in the text must be declared, or redaction misses it.
        for secret in ["AK-SECRET", "SK-SECRET"] {
            assert!(
                secrets.iter().any(|s| s == secret),
                "`{secret}` is in the statement but not declared: {secrets:?}"
            );
        }
    }

    #[test]
    fn a_storage_integration_keeps_the_keys_out_of_the_definition() {
        let stage = Stage::connect(&options().with_storage_integration("RDLT_INT"), "sales")
            .expect("connect");
        let (sql, secrets) = stage.create_sql("\"ST\"");
        assert!(sql.contains("STORAGE_INTEGRATION = RDLT_INT"), "{sql}");
        assert!(!sql.contains("SK-SECRET"), "no key material: {sql}");
        assert!(secrets.is_empty(), "nothing to redact: {secrets:?}");
    }

    #[test]
    fn an_endpoint_switches_the_url_scheme_and_names_the_host() {
        let mut options = options();
        options.endpoint = Some("https://storage.example.com".to_owned());
        let stage = Stage::connect(&options, "sales").expect("connect");
        assert!(stage.url.starts_with("s3compat://parts/"), "{}", stage.url);
        let (sql, _) = stage.create_sql("\"ST\"");
        assert!(
            sql.contains("ENDPOINT = 'storage.example.com'"),
            "the parameter takes a host, not a URL: {sql}"
        );
    }

    #[test]
    fn redaction_replaces_every_secret_wherever_it_appears() {
        let rendered = "failed on CREATE STAGE ... AWS_SECRET_KEY = 'SK-SECRET' ... SK-SECRET";
        let out = redact(rendered, &["SK-SECRET".to_owned()]);
        assert!(!out.contains("SK-SECRET"), "{out}");
        assert_eq!(out.matches("****").count(), 2);
    }

    #[tokio::test]
    async fn a_failing_credentialed_statement_never_echoes_the_key() {
        // The failure mode this guards: the service quotes the statement it
        // could not run, and this is the one statement whose text is a secret.
        struct Echo;
        #[async_trait::async_trait]
        impl super::super::client::Executor for Echo {
            async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
                Err(DestinationError::fatal(format!("SQL compilation error: {sql}")))
            }
            async fn scalar_u64(&self, _: &str, _: &[&str]) -> Result<u64, DestinationError> {
                Ok(0)
            }
            async fn sum_column(&self, _: &str, _: &str) -> Result<u64, DestinationError> {
                Ok(0)
            }
            async fn column_names(
                &self,
                _: &str,
                _: &[&str],
            ) -> Result<std::collections::BTreeSet<String>, DestinationError> {
                Ok(std::collections::BTreeSet::new())
            }
        }
        let (sql, secrets) = stage().create_sql("\"ST\"");
        let err = execute_redacted(&Echo, &sql, &secrets)
            .await
            .expect_err("the echo fails");
        let rendered = format!("{err}");
        assert!(!rendered.contains("SK-SECRET"), "{rendered}");
        assert!(!rendered.contains("AK-SECRET"), "{rendered}");
        assert!(rendered.contains("****"), "{rendered}");
    }

    #[test]
    fn part_keys_are_unique_per_table_and_never_reused_within_a_session() {
        let mut stage = stage();
        let mut seen = std::collections::BTreeSet::new();
        for table in ["events", "orders"] {
            for _ in 0..3 {
                let tail = format!(
                    "{}/{:08}.parquet",
                    ident_hash(table, 16),
                    stage.next_part
                );
                stage.next_part += 1;
                assert!(seen.insert(tail.clone()), "reused key {tail}");
            }
        }
        // Two tables never share a directory, so a COPY naming one table's
        // parts cannot reach the other's.
        assert_eq!(seen.len(), 6);
    }

    #[test]
    fn the_copy_names_its_files_and_forces_past_the_load_history() {
        let parts = vec![
            Part { tail: "aa/00000000.parquet".into(), rows: 3 },
            Part { tail: "aa/00000001.parquet".into(), rows: 2 },
        ];
        let sql = copy_sql("\"DB\".\"S\".\"EVENTS\"", "\"DB\".\"S\".\"ST\"", &parts);
        assert!(
            sql.contains("FILES = ('aa/00000000.parquet', 'aa/00000001.parquet')"),
            "the load is limited to this unit's parts: {sql}"
        );
        assert!(sql.contains("MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"), "{sql}");
        assert!(
            sql.contains("FORCE = TRUE"),
            "a rolled-back unit must be able to load the same parts again: {sql}"
        );
        assert!(sql.contains("ON_ERROR = ABORT_STATEMENT"), "{sql}");
        // A COPY is DML: it must be runnable inside the unit transaction.
        assert!(!super::super::client::is_ddl(&sql), "{sql}");
    }

    #[test]
    fn the_stage_object_is_named_apart_from_the_merge_stage_tables() {
        // Both live under the same `_rdlt_stage_` prefix but in different
        // namespaces; an operator reading SHOW STAGES next to SHOW TABLES
        // should not have to work out which is which.
        let stage = stage_object_name("sales");
        assert!(stage.starts_with(rdlt_connector_sqlcore::names::STAGE_PREFIX));
        assert_ne!(stage, rdlt_connector_sqlcore::names::stage_table("sales", "events"));
        assert!(stage.contains("ext_"), "{stage}");
        assert_eq!(stage, stage_object_name("sales"), "deterministic across runs");
    }
}
