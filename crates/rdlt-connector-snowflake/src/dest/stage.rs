//! Staging: parts built locally, uploaded to storage the service provides.
//!
//! The upload reads a FILE, so a part is written to local disk before it moves.
//! One part exists at a time — built, uploaded, deleted — so peak local usage
//! is one part rather than proportional to the unit or the dataset.
//!
//! Two rules here are not obvious and both were measured against the service:
//!
//! **Uploading does not end an open transaction**, unlike schema work, which
//! is why staging happens inside the commit unit. **Dropping the staging
//! object DOES**, which is why teardown never does.
//!
//! And the one that would otherwise corrupt a load silently: an upload of
//! several files reports SUCCESS overall while an individual file failed. Every
//! row's status is therefore inspected, and any failure abandons the unit.

use std::path::{Path, PathBuf};

use rdlt_connector::DestinationError;
use rdlt_connector::core::{crash_point, naming::ident_hash};

use super::client::Executor;

/// The staging object's name: rdlt's shared prefix, a marker distinguishing it
/// from the merge stage TABLES, and the pipeline it belongs to.
///
/// Pipeline-scoped so two pipelines sharing a schema never redefine each
/// other's, and deterministic so a later run finds the same object.
pub(super) fn stage_object_name(pipeline: &str) -> String {
    format!(
        "{}int_{}",
        rdlt_connector_sqlcore::names::STAGE_PREFIX,
        ident_hash(pipeline, 8)
    )
}

/// One staged part: what the load statement names it, and how many rows it
/// holds.
///
/// The rowcount is recorded at BUILD time because that is the only moment it
/// is known for free — reading it back from the file to check the load would
/// be checking the service against itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Part {
    /// Name relative to the prefix the load statement names.
    pub(super) tail: String,
    /// Rows written into it.
    pub(super) rows: u64,
}

/// What the upload reports per file.
const UPLOAD_STATUS: &str = "status";
const UPLOAD_TARGET: &str = "target";
const UPLOAD_MESSAGE: &str = "message";

/// How long a staged part must sit untouched before a later load may remove it.
///
/// A day, deliberately far beyond any load this connector is built for: the
/// window only needs to exceed the longest run that could still be in flight,
/// and being wrong in the tight direction deletes parts out from under a live
/// load.
const STALE_AFTER_HOURS: u32 = 24;

/// The one status that means a file arrived.
///
/// Anything else is treated as failure, because a part the load statement names
/// and the stage does not hold is the same problem whatever the service calls
/// it.
const UPLOADED: &str = "UPLOADED";

/// Where the service's own storage is reached, for one pipeline and one load.
pub(super) struct Stage {
    /// The staging object's unqualified name.
    name: String,
    /// This load's segment beneath it — what makes a part name unique to the
    /// session that wrote it.
    load: String,
    /// Local directory holding at most one part at a time.
    local: PathBuf,
    /// Never reset within a session, so a part written after a commit cannot
    /// take the name of one whose cleanup failed.
    next_part: u64,
}

impl Stage {
    /// Derive this load's staging identity. Touches nothing yet.
    pub(super) fn new(pipeline: &str, load: &str) -> Self {
        // Hashed rather than spelled: a pipeline or load name is free text, and
        // a key derived from it would have to be sanitised — at which point two
        // different names can sanitise to one scope and start deleting each
        // other's parts.
        let scope = ident_hash(pipeline, 12);
        let load = ident_hash(load, 12);
        let local = std::env::temp_dir().join(format!("rdlt-sf-{scope}-{load}"));
        Self {
            name: stage_object_name(pipeline),
            load,
            local,
            next_part: 0,
        }
    }

    /// The staging object's unqualified name, for the caller to qualify.
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    /// The statement that ensures the staging object exists.
    ///
    /// `IF NOT EXISTS` rather than `OR REPLACE`: the object is derived entirely
    /// from the pipeline's name and carries no configuration that could go
    /// stale, so replacing it every load would discard another session's staged
    /// parts for nothing. It is schema work and runs before any unit opens.
    pub(super) fn create_sql(qualified_name: &str) -> String {
        format!("CREATE STAGE IF NOT EXISTS {qualified_name}")
    }

    /// The prefix beneath the staging object that one table's parts live under.
    fn prefix(&self, table: &str) -> String {
        format!("{}/{}", self.load, ident_hash(table, 16))
    }

    /// Build a part locally, upload it, and delete the local copy.
    ///
    /// The local file is removed on EVERY path out, including failure: it has
    /// no readers once this returns, and a part left behind after an error is
    /// exactly the residue a crash would otherwise leave.
    pub(super) async fn put_part(
        &mut self,
        executor: &dyn Executor,
        qualified_stage: &str,
        table: &str,
        bytes: Vec<u8>,
        rows: u64,
    ) -> Result<Part, DestinationError> {
        let index = self.next_part;
        self.next_part += 1;
        // `.parquet` is load-bearing, not decoration: the upload recognises an
        // already-compressed payload by extension as well as by content, and a
        // part it failed to recognise would be compressed — leaving the staged
        // name with a suffix the load statement does not name.
        let file_name = format!("{index:08}.parquet");
        let prefix = self.prefix(table);

        std::fs::create_dir_all(&self.local).map_err(|e| local_err(&self.local, "creating", e))?;
        let path = self.local.join(&file_name);

        std::fs::write(&path, &bytes).map_err(|e| local_err(&path, "writing", e))?;
        // The part is on the local disk and nowhere else. Crashing here leaves
        // a file the next run of THIS load must clear; another load's file
        // must survive, because from here the two are indistinguishable.
        crash_point!(
            "sf.stage.write",
            Err(DestinationError::fatal("injected crash at sf.stage.write"))
        );

        let outcome = self
            .upload(executor, &path, qualified_stage, &prefix, &file_name)
            .await;
        // Removed whether the upload worked or not.
        let _ = std::fs::remove_file(&path);
        outcome?;
        // Uploaded, and the session has not yet recorded it. Crashing here
        // leaves an object no load statement will name — the remote debris the
        // scope wipe collects, as distinct from the local file above.
        crash_point!(
            "sf.stage.upload",
            Err(DestinationError::fatal("injected crash at sf.stage.upload"))
        );

        Ok(Part {
            tail: format!("{prefix}/{file_name}"),
            rows,
        })
    }

    /// Issue the upload and verify EVERY row it reports.
    async fn upload(
        &self,
        executor: &dyn Executor,
        path: &Path,
        qualified_stage: &str,
        prefix: &str,
        file_name: &str,
    ) -> Result<(), DestinationError> {
        // `AUTO_COMPRESS = FALSE` keeps the staged name equal to the local one.
        // Left on, the service appends a compression suffix to anything it
        // decides to compress, and the load statement would then name a file
        // that does not exist. `OVERWRITE = TRUE` because a retried unit must
        // be able to re-upload the same part.
        //
        // The statement begins with the verb and nothing else: the library
        // switches its result format on the FIRST token, and a leading comment
        // would leave it asking for a format the service refuses for uploads.
        let sql = format!(
            "PUT 'file://{}' @{qualified_stage}/{prefix}/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE",
            path.display()
        );
        let rows = executor
            .rows(&sql, &[UPLOAD_TARGET, UPLOAD_STATUS, UPLOAD_MESSAGE])
            .await?;

        if rows.is_empty() {
            return Err(DestinationError::fatal(format!(
                "snowflake: uploading `{file_name}` reported no result at all; the part \
                 cannot be assumed to have arrived"
            )));
        }
        for row in &rows {
            let (target, status, message) = (&row[0], &row[1], &row[2]);
            if !status.eq_ignore_ascii_case(UPLOADED) {
                return Err(DestinationError::fatal(format!(
                    "snowflake: uploading `{file_name}` reported `{status}` for `{target}`{}; \
                     the unit is abandoned rather than loading a part that is not there",
                    if message.is_empty() {
                        String::new()
                    } else {
                        format!(" ({message})")
                    }
                )));
            }
            // The staged name must be the one the load statement will name.
            // With compression disabled the service returns the basename
            // unchanged; anything else means it renamed the file underneath us.
            if !target.eq_ignore_ascii_case(file_name) {
                return Err(DestinationError::fatal(format!(
                    "snowflake: uploading `{file_name}` staged it as `{target}` instead; the \
                     load statement names files by their local name and would not find this one"
                )));
            }
        }
        Ok(())
    }

    /// Remove staged parts. Best effort by design: they are dead either way —
    /// a part is named by exactly one load statement, and a unit that did not
    /// commit will never name them again — and a cleanup failure must not fail
    /// a load that committed.
    pub(super) async fn remove(&self, executor: &dyn Executor, qualified_stage: &str) {
        let _ = executor
            .execute(&format!("REMOVE @{qualified_stage}/{}/", self.load))
            .await;
    }

    /// Drop parts left behind by loads that died before they could clean up.
    ///
    /// A load removes its OWN parts when it settles, so anything still present
    /// belongs to a load that crashed — or to one running right now, and from
    /// here those two are indistinguishable by name. Age is what separates
    /// them, which is why [`STALE_AFTER_HOURS`] is generous rather than tight:
    /// deleting a live load's parts would make it commit short, while leaving a
    /// dead load's parts one more day costs a few objects of storage.
    ///
    /// The comparison runs on the SERVICE's clock, in SQL, rather than by
    /// parsing the timestamp here. Two reasons, both load-bearing: this host's
    /// clock has no defined relationship to the one that stamped the object,
    /// and hand-parsing a date format nobody controls to decide what to DELETE
    /// is a bug with an expensive blast radius.
    ///
    /// Best effort throughout. Failing to reclaim is a storage cost; failing a
    /// load that would have worked is a correctness cost, and the two are not
    /// worth trading.
    pub(super) async fn reclaim_remote(&self, executor: &dyn Executor, qualified_stage: &str) {
        self.reclaim_remote_older_than(executor, qualified_stage, STALE_AFTER_HOURS)
            .await
    }

    /// [`reclaim_remote`](Self::reclaim_remote) against a chosen age.
    ///
    /// The age is a parameter only so a test can prove the rule BOTH ways —
    /// that a stale part goes and a fresh one stays. A shipped threshold no
    /// test can move is a threshold whose two outcomes are never both checked.
    pub(super) async fn reclaim_remote_older_than(
        &self,
        executor: &dyn Executor,
        qualified_stage: &str,
        stale_after_hours: u32,
    ) {
        // The listing has to run first: the filter reads its result set, which
        // only exists as the PREVIOUS query of this same session.
        if executor
            .execute(&format!("LIST @{qualified_stage}"))
            .await
            .is_err()
        {
            return;
        }
        let Ok(rows) = executor
            .rows(
                &format!(
                    "SELECT \"name\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) \
                     WHERE TO_TIMESTAMP_TZ(\"last_modified\", \
                     \'DY, DD MON YYYY HH24:MI:SS TZD\') \
                     < DATEADD(hour, -{stale_after_hours}, CURRENT_TIMESTAMP())",
                ),
                &["name"],
            )
            .await
        else {
            return;
        };
        // The listing names objects with the stage's own name prefixed; the
        // removal wants the path RELATIVE to the stage, so the leading segment
        // comes off. A name that does not look like that is left alone rather
        // than guessed at.
        let mut scopes: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for row in &rows {
            let Some((_, tail)) = row[0].split_once('/') else {
                continue;
            };
            if let Some((scope, _)) = tail.split_once('/') {
                scopes.insert(scope);
            }
        }
        for scope in scopes {
            let _ = executor
                .execute(&format!("REMOVE @{qualified_stage}/{scope}/"))
                .await;
        }
    }

    /// Reclaim local residue this load left behind.
    ///
    /// Only THIS load's directory, and unconditionally: a previous attempt of
    /// the same load staged those files, no receipt names them, and the attempt
    /// about to run writes its own. Another load's directory is left alone,
    /// because from here a concurrent load and an abandoned one look identical.
    pub(super) fn reclaim_local(&self) {
        let _ = std::fs::remove_dir_all(&self.local);
    }

    /// Where this load's parts are written while they wait to be uploaded.
    ///
    /// Exposed so a caller can assert the directory is EMPTY once a load has
    /// settled: a crash between writing a part and uploading it leaves a file
    /// here, and "the next run cleans it up" is a claim worth checking rather
    /// than assuming.
    pub(super) fn local_dir(&self) -> &Path {
        &self.local
    }
}

/// A local filesystem failure, classified by what actually went wrong.
///
/// Never a bare I/O error: an operator seeing a load fail needs to know whether
/// to free disk, fix a mount, or look elsewhere entirely.
fn local_err(path: &Path, doing: &str, error: std::io::Error) -> DestinationError {
    use std::io::ErrorKind;
    let where_ = path.display();
    match error.kind() {
        // Out of space is TRANSIENT: the usual cause is another process's
        // temporary files, and the next attempt often finds room. Retrying
        // costs one attempt; treating it as fatal fails a load that would have
        // succeeded a minute later.
        ErrorKind::StorageFull => DestinationError::transient(format!(
            "snowflake: no space left while {doing} a staged part at `{where_}`; parts are \
             built in the system temporary directory, which TMPDIR selects"
        )),
        ErrorKind::PermissionDenied | ErrorKind::ReadOnlyFilesystem => {
            DestinationError::fatal(format!(
                "snowflake: cannot write a staged part at `{where_}` ({doing}): the temporary \
                 directory is not writable; TMPDIR selects it"
            ))
        }
        _ => DestinationError::fatal(format!(
            "snowflake: {doing} a staged part at `{where_}`: {error}"
        )),
    }
}

/// The load statement for a set of staged parts.
///
/// Columns are mapped EXPLICITLY, by projecting each one out of the file,
/// rather than by asking the service to match them by name. The difference is
/// not stylistic: name-matching sets a target column absent from the file to
/// NULL rather than to its default, which silently destroys the arrival order
/// a merge's last-wins tie-break reads — every staged row ties, and the
/// survivor becomes arbitrary. Projecting the schema's columns and leaving the
/// arrival column out of the list lets it take its assigned default, in file
/// order.
///
/// It also makes the case difference explicit: the parts carry the encoding's
/// own column names, the catalog holds them upper case, and the projection
/// states the correspondence instead of relying on a matching mode.
///
/// `FORCE = TRUE` deliberately: the service otherwise skips files its load
/// history has seen, which is file-level dedup with a long window and no
/// knowledge of transactions. Exactly-once here comes from the unit and the
/// receipt — a re-run whose unit rolled back MUST load its parts again, and a
/// load that silently skipped them would report zero rows and leave the target
/// short.
pub(super) fn copy_sql(
    qualified_target: &str,
    qualified_stage: &str,
    columns: &[String],
    parts: &[Part],
) -> String {
    let files = parts
        .iter()
        .map(|p| format!("'{}'", p.tail))
        .collect::<Vec<_>>()
        .join(", ");
    let target_columns = columns
        .iter()
        .map(|c| super::ddl::quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    // The projection reads the file's OWN column names, which the encoder wrote
    // in the case the schema uses; the target list is the catalog's, which is
    // upper. Quoting the projection with the target's case would look
    // symmetrical and find nothing — the accessor is case-sensitive, so every
    // column would arrive NULL and a non-nullable one would fail the load with
    // an error naming a column the file plainly contains.
    let projection = columns
        .iter()
        .map(|c| format!("$1:\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "COPY INTO {qualified_target} ({target_columns}) \
         FROM (SELECT {projection} FROM @{qualified_stage}/) FILES = ({files}) \
         FILE_FORMAT = ( TYPE = PARQUET ) FORCE = TRUE PURGE = FALSE \
         ON_ERROR = ABORT_STATEMENT"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_staging_object_is_named_apart_from_the_merge_stage_tables() {
        // Both live under the same prefix but in different namespaces; an
        // operator reading one listing beside the other should not have to work
        // out which is which.
        let stage = stage_object_name("sales");
        assert!(stage.starts_with(rdlt_connector_sqlcore::names::STAGE_PREFIX));
        assert!(stage.contains("int_"), "{stage}");
        assert_ne!(
            stage,
            rdlt_connector_sqlcore::names::stage_table("sales", "events")
        );
        assert_eq!(stage, stage_object_name("sales"), "deterministic");
    }

    #[test]
    fn two_loads_of_one_pipeline_cannot_collide_on_a_part_name() {
        // The defect this pins, learned the hard way on the path this replaces:
        // part numbering restarts per session, so without the load segment
        // every session's FIRST part has the same name under one shared prefix
        // — and reclamation then deletes a part another session is about to
        // load.
        let first = Stage::new("sales", "load-1");
        let second = Stage::new("sales", "load-2");
        assert_eq!(first.name(), second.name(), "same pipeline, same object");
        assert_ne!(
            first.load, second.load,
            "different loads, different segment"
        );
        assert_ne!(
            first.prefix("events"),
            second.prefix("events"),
            "the same table in two loads must not share a prefix"
        );
        assert_ne!(
            first.local, second.local,
            "and their local directories must not collide either"
        );
    }

    #[test]
    fn two_tables_never_share_a_prefix() {
        let stage = Stage::new("sales", "load-1");
        assert_ne!(stage.prefix("events"), stage.prefix("orders"));
    }

    #[test]
    fn the_upload_statement_starts_with_its_verb_and_disables_renaming() {
        // Two properties that would each break a load silently. The library
        // switches result format on the FIRST token, so a leading comment makes
        // it ask for a format uploads are refused in. And compression left on
        // renames the staged file, so the load statement would name something
        // that is not there.
        let stage = Stage::new("sales", "load-1");
        let sql = format!(
            "PUT 'file:///tmp/x/00000000.parquet' @\"S\"/{}/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE",
            stage.prefix("events")
        );
        assert!(sql.starts_with("PUT "), "{sql}");
        assert!(sql.contains("AUTO_COMPRESS = FALSE"), "{sql}");
        assert!(!super::super::client::is_ddl(&sql), "an upload is not DDL");
    }

    #[test]
    fn the_load_statement_names_each_part_beneath_the_object_root() {
        let parts = vec![
            Part {
                tail: "aa/bb/00000000.parquet".into(),
                rows: 3,
            },
            Part {
                tail: "aa/bb/00000001.parquet".into(),
                rows: 2,
            },
        ];
        let columns = vec!["id".to_string(), "note".to_string()];
        let sql = copy_sql(
            "\"DB\".\"S\".\"EVENTS\"",
            "\"DB\".\"S\".\"ST\"",
            &columns,
            &parts,
        );
        assert!(
            sql.contains("FILES = ('aa/bb/00000000.parquet', 'aa/bb/00000001.parquet')"),
            "the load is limited to this unit's parts: {sql}"
        );
        assert!(
            sql.contains("(\"ID\", \"NOTE\")") && sql.contains("$1:\"id\", $1:\"note\""),
            "the target list is the catalog's upper case and the projection is the \
             file's own — the accessor is case-sensitive, and matching by name \
             would NULL the arrival column and make last-wins arbitrary: {sql}"
        );
        assert!(
            !sql.contains("MATCH_BY_COLUMN_NAME"),
            "name-matching is exactly what this avoids: {sql}"
        );
        assert!(
            sql.contains("FORCE = TRUE"),
            "a rolled-back unit must be able to load the same parts again: {sql}"
        );
        assert!(!super::super::client::is_ddl(&sql), "{sql}");
    }

    #[test]
    fn creating_the_staging_object_is_idempotent() {
        let sql = Stage::create_sql("\"DB\".\"S\".\"ST\"");
        assert_eq!(sql, "CREATE STAGE IF NOT EXISTS \"DB\".\"S\".\"ST\"");
        // It IS schema work, and the unit executor must keep refusing it — the
        // protocol runs it before any unit opens.
        assert!(super::super::client::is_ddl(&sql));
    }

    #[test]
    fn local_failures_name_the_condition_rather_than_the_errno() {
        use std::io::{Error, ErrorKind};
        let path = Path::new("/tmp/rdlt-sf-probe/00000000.parquet");

        let full = local_err(path, "writing", Error::from(ErrorKind::StorageFull));
        assert!(
            matches!(full, DestinationError::Transient(_)),
            "another process's temporary files are the usual cause, and the next \
             attempt often finds room: {full:?}"
        );
        assert!(format!("{full}").contains("no space left"), "{full}");
        assert!(
            format!("{full}").contains("TMPDIR"),
            "names the lever: {full}"
        );

        let readonly = local_err(path, "writing", Error::from(ErrorKind::ReadOnlyFilesystem));
        assert!(
            matches!(readonly, DestinationError::Fatal(_)),
            "a read-only filesystem will not become writable on retry: {readonly:?}"
        );
        assert!(format!("{readonly}").contains("not writable"), "{readonly}");
    }
}
