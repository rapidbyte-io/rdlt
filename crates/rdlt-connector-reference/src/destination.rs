//! The reference destination: jsonl parts plus commit receipts, in ONE
//! output directory.
//!
//! Staging is in-memory (a crashed session's staging simply vanishes —
//! the open contract by construction); publish stream-encodes each
//! table's staged rows to `<table>-<load_id>-<part>-<digest>.jsonl`
//! (the digest an injective hash of the whole tuple — 8L10), persists
//! the state document, and appends a receipt line LAST — each step
//! fsynced, so the receipt can only be durable after the parts and
//! state it acknowledges are. The part number IS the commit sequence,
//! deliberately: a crash after the parts but before the receipt leaves
//! no receipt, so the retried commit re-publishes — and deterministic
//! names make that re-publish overwrite its own files instead of
//! duplicating them. Staging clears only after the receipt append:
//! a mid-publish failure (transient by classification) leaves it
//! intact, so a client retrying the SAME commit without re-writing
//! re-persists every row over those same deterministic names instead
//! of minting a receipt for an empty publish. One session at a time:
//! connect takes an OS advisory lease beside the state slot, released
//! on drop and on process death.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::PathBuf;

use async_trait::async_trait;
use fs4::fs_std::FileExt as _;
use rdlt_connector_sdk::config::{self, Document};
use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
use rdlt_connector_sdk::spi::arrow::RecordBatch;
use rdlt_connector_sdk::spi::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector_sdk::spi::destination::{Capabilities, OpenContext};
use rdlt_connector_sdk::spi::error::DestinationError;

/// The append-only receipt log: one json line per published commit,
/// `{"load_id":<string>,"commit_seq":<u64>}` — what `existing_receipt`
/// answers the sdk's replay choreography from. A line's terminating
/// newline is its durability marker: a newline-less tail is a torn
/// append, read as absent and truncated before the next append.
const RECEIPTS_FILE: &str = "_reference_receipts.json";

/// The latest committed state document, written atomically (write to a
/// temporary, fsync, then rename) by every publish, BEFORE its receipt.
const STATE_FILE: &str = "_reference_state.json";

/// The session lease: an OS advisory lock held from connect to drop.
/// Two concurrent sessions over one directory would each read the same
/// persisted cursor and publish the same rows under their own load ids
/// — deterministic part names dedupe only WITHIN a load — so the
/// second open is refused typed instead. Advisory locks release on
/// process death, so a crashed run never blocks its own recovery.
const LEASE_FILE: &str = "_reference_lease.lock";

/// A reference connector must model a bounded staging posture. Four
/// maximum-size wire frames leave ample room for ordinary commits while
/// preventing a client from retaining an unbounded session in memory.
const STAGING_CEILING_BYTES: usize = 256 << 20;

/// The reference destination document: ONE output directory.
/// `{ "path": "out/dir" }`
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The directory the parts, receipts, and state land in; created at
    /// the first connect.
    pub path: String,
}

/// The destination's configuration error — parser framings plus the
/// config gate's own refusals, every spelling owned here.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// YAML did not parse as the config document.
    #[error("invalid reference destination YAML: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    /// JSON did not parse as the config document.
    #[error("invalid reference destination JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The document parsed but violates an invariant.
    #[error("invalid reference destination config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = Error;

    fn validate(&self) -> Result<(), Error> {
        if self.path.is_empty() {
            return Err(Error::Invalid(
                "`path` is empty — one output directory is required".into(),
            ));
        }
        Ok(())
    }
}

/// The connector: one directory. `Clone` is part of the in-process
/// face: the sdk Shell forwards it, and the engine's crash sweep
/// clones its destination handle once per recovery attempt.
#[derive(Debug, Clone)]
pub struct Reference {
    dir: PathBuf,
}

#[async_trait]
impl DestinationConnector for Reference {
    const NAME: &'static str = "io.rapidbyte.reference";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = Config;
    type Backend = Writer;

    fn assemble(config: Config) -> Result<Self, Error> {
        Ok(Self {
            dir: PathBuf::from(config.path),
        })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::schema_of::<Config>())
    }

    fn capabilities(&self) -> Capabilities {
        // Truthful: arrow's json writer renders structs, lists, json
        // and decimals into the parts. Merge stays undeclared — jsonl
        // parts are append-only files with no upsert machinery.
        Capabilities::default()
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(true)
            .with_decimal(true)
    }

    async fn connect(&self, context: &OpenContext) -> Result<Writer, DestinationError> {
        // The open contract — a crashed predecessor's staging invisible
        // and reclaimable — holds by construction: staging lives in the
        // dead session's memory, and nothing staged ever touches disk.
        std::fs::create_dir_all(&self.dir).map_err(|error| {
            DestinationError::transient(format!(
                "reference destination: create {}: {error}",
                self.dir.display()
            ))
        })?;
        let lease = self.acquire_lease()?;
        Ok(Writer {
            dir: self.dir.clone(),
            load_id: context.load_id.clone(),
            staged: Vec::new(),
            staged_bytes: 0,
            lease: Some(lease),
        })
    }
}

impl Reference {
    /// Take the session lease, refusing typed when another session
    /// holds it. Fatal, not transient: the holder may be a hung run,
    /// and retrying against it forever is exactly the double-fired-cron
    /// scenario the lease exists to surface.
    fn acquire_lease(&self) -> Result<std::fs::File, DestinationError> {
        let path = self.dir.join(LEASE_FILE);
        let framed = |verb: &str, error: std::io::Error| {
            DestinationError::transient(format!(
                "reference destination: {verb} {}: {error}",
                path.display()
            ))
        };
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|error| framed("open", error))?;
        if !file
            .try_lock_exclusive()
            .map_err(|error| framed("lock", error))?
        {
            return Err(DestinationError::fatal(format!(
                "reference destination: another session holds the lease at {} — one session \
                 per output directory",
                path.display()
            )));
        }
        Ok(file)
    }
}

/// One session's system IO: staged batches in memory, published files,
/// receipts and state on disk. Holds the session lease; `close` and
/// drop both release it.
#[derive(Debug)]
pub struct Writer {
    dir: PathBuf,
    load_id: LoadId,
    staged: Vec<(TableName, RecordBatch)>,
    staged_bytes: usize,
    lease: Option<std::fs::File>,
}

#[async_trait]
impl Backend for Writer {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        match mode {
            // A jsonl part carries its own column names on every row —
            // there is no DDL to run, and re-ensuring is trivially
            // idempotent. Append is the ONE disposition this
            // destination performs.
            WriteMode::Append => Ok(()),
            // Everything else — Replace, Merge, and any future
            // disposition — is typed-unsupported, never silent.
            // Accepting Replace would append where the pipeline asked
            // for a table's contents to be replaced; accepting Merge
            // would append where the pipeline asked for upsert-by-key,
            // duplicating every redelivery — each quietly, forever. The
            // engine's validate gate refuses Merge against the declared
            // `merge = false` capability, but a host driving this
            // backend directly never passes that gate, so the refusal
            // lives here too.
            other => {
                let mode_name = match other {
                    WriteMode::Replace => "replace".to_owned(),
                    WriteMode::Merge { .. } => "merge".to_owned(),
                    other => format!("{other:?}"),
                };
                Err(DestinationError::fatal(format!(
                    "reference destination: table `{}`: write mode `{mode_name}` is not \
                     supported — jsonl parts are append-only",
                    schema.table
                )))
            }
        }
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let batch_bytes = rdlt_connector_sdk::spi::channel::arrow_batch_footprint(&batch);
        let next = next_staging_bytes(self.staged_bytes, batch_bytes)?;
        self.staged_bytes = next;
        self.staged.push((table.clone(), batch));
        Ok(())
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        let path = self.dir.join(RECEIPTS_FILE);
        // Read BYTES, never `read_to_string` (7L6): a torn append can
        // split a multi-byte UTF-8 character (a non-ASCII load id rides
        // the receipt line verbatim), and `read_to_string` would fail
        // the WHOLE read as `InvalidData` — a transient the choreography
        // retries forever, when the torn tail is contractually ABSENT
        // and only `publish`'s truncation ever repairs it. Decoding per
        // COMPLETE line keeps the tear where it belongs: in the tail.
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(DestinationError::transient(format!(
                    "reference destination: read {}: {error}",
                    path.display()
                )));
            }
        };
        // The terminating newline is an append's durability marker:
        // bytes after the LAST newline are a receipt whose append tore
        // mid-write (disk full, a crash inside the write). That receipt
        // never became durable, so it reads as ABSENT — the retried
        // commit republishes convergently over its deterministic part
        // names, and publish truncates the torn bytes before appending.
        // An unparseable line that IS newline-terminated sits in the
        // log's interior: that is corruption, not a torn append, and
        // stays a typed refusal below.
        let durable_end = bytes
            .iter()
            .rposition(|&byte| byte == b'\n')
            .map(|last_newline| last_newline + 1)
            .unwrap_or(0);
        let durable = &bytes[..durable_end];
        let durable = std::str::from_utf8(durable).map_err(|error| {
            // FATAL, not transient (8L9): the writer only ever appends
            // valid UTF-8, and a torn append is newline-less (bytes
            // AFTER the last newline, already excluded above) — so
            // invalid UTF-8 before the last complete line is permanent
            // corruption no retry repairs, the same taxonomy the
            // unparseable-interior-line arm below applies.
            DestinationError::fatal(format!(
                "reference destination: {} carries a corrupt receipt log (invalid UTF-8 \
                 before the last complete line): {error}",
                path.display()
            ))
        })?;
        for line in durable.lines().filter(|line| !line.trim().is_empty()) {
            let receipt: CommitReceipt = serde_json::from_str(line).map_err(|error| {
                DestinationError::fatal(format!(
                    "reference destination: {} carries a corrupt receipt line `{line}`: {error}",
                    path.display()
                ))
            })?;
            if receipt.load_id == *load_id && receipt.commit_seq == commit_seq {
                return Ok(Some(receipt));
            }
        }
        Ok(None)
    }

    async fn replay(
        &mut self,
        _meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        // The redelivered unit was already published under this receipt;
        // dropping its staging is what keeps a LATER commit from
        // publishing it a second time.
        self.staged.clear();
        self.staged_bytes = 0;
        Ok(())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        // THE DURABILITY ORDER IS THE EXACTLY-ONCE PROOF and is pinned
        // by the suite: every part, then the state document, each
        // persisted through fsync — and only THEN the receipt append.
        // A receipt that reached the journal while a part still sat in
        // page cache would, after power loss, answer `existing_receipt`
        // for a commit whose rows are gone: replay would drop the
        // redelivered staging and the loss would be silent.
        //
        // Staging is read BY REFERENCE and cleared only after the
        // receipt append — never drained up front. Mid-publish failures
        // classify transient, and a client retrying the SAME commit
        // without re-writing is exactly what that classification
        // invites: a drain would hand that retry EMPTY staging, and the
        // zero-part publish would still write state and append a
        // receipt `existing_receipt` then vouches for — rows silently
        // gone. With staging intact, the retry re-persists everything
        // convergently over the same deterministic part names.
        let mut tables: BTreeMap<&TableName, Vec<&RecordBatch>> = BTreeMap::new();
        for (table, batch) in &self.staged {
            tables.entry(table).or_default().push(batch);
        }
        for (table, batches) in &tables {
            part_component(table)?;
            // The tuple-encoding is INJECTIVE (8L10): the plain
            // `{table}-{load}-{seq}` spelling collides across dash-rich
            // ids (`(a, b-c)` vs `(a-b, c)` map to one part file, the
            // later publish silently overwriting the earlier tuple's
            // rows — reachable only by a direct-`Backend` host with
            // custom ids, precisely the lane this template tutors).
            // A short digest of the WHOLE tuple separates them without
            // changing the name's shape for the engine's dash-fixed
            // ids (the digest is constant per tuple, so re-publish
            // overwrite determinism is preserved).
            let part = format!(
                "{table}-{}-{}-{}.jsonl",
                self.load_id,
                meta.commit_seq,
                part_tuple_digest(table.as_str(), self.load_id.as_str(), meta.commit_seq)
            );
            part_filename(&part)?;
            self.persist_part(&part, table, batches)?;
        }

        // State BEFORE receipt: a crash between the two re-publishes the
        // whole commit (no receipt yet), overwriting both parts and
        // state with identical content — convergent, never duplicated.
        let state = serde_json::to_vec(&meta.state).map_err(|error| {
            DestinationError::fatal(format!(
                "reference destination: encode the state document: {error}"
            ))
        })?;
        self.persist(STATE_FILE, &state)?;

        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        self.append_receipt(&receipt)?;

        // The commit is fully durable — only now does its staging
        // retire, so no later commit can publish it a second time.
        self.staged.clear();
        self.staged_bytes = 0;
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let path = self.dir.join(STATE_FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(DestinationError::transient(format!(
                    "reference destination: read {}: {error}",
                    path.display()
                )));
            }
        };
        let state: StateDoc = serde_json::from_str(&text).map_err(|error| {
            DestinationError::fatal(format!(
                "reference destination: {} carries a corrupt state document: {error}",
                path.display()
            ))
        })?;
        // ONE state slot, ONE pipeline per directory: another
        // pipeline's read REFUSES rather than answering None — `None`
        // is the SPI's "never committed", so the engine would
        // re-extract from scratch, append every already-loaded row a
        // second time (receipts are per load id and cannot help across
        // pipelines), and the next publish would overwrite the
        // occupant's cursors in the slot.
        if state.pipeline != *pipeline {
            return Err(DestinationError::fatal(format!(
                "reference destination: {} carries the state of pipeline `{}` — this \
                 session is pipeline `{pipeline}`, and one directory holds ONE pipeline's \
                 state: reading it as fresh would append every already-loaded row again, \
                 and the next publish would destroy `{}`' cursors; give each pipeline its \
                 own output directory",
                path.display(),
                state.pipeline,
                state.pipeline
            )));
        }
        Ok(Some(state))
    }

    async fn close(&mut self) -> Result<(), DestinationError> {
        // The lease ends with the SESSION, not with the object: a
        // well-behaved host closes a session before opening the next,
        // and the successor must not be refused by a closed
        // predecessor whose handle is still in scope. Dropping the
        // file releases the advisory lock; an unclosed drop (crash,
        // error path) releases it the same way.
        self.staged.clear();
        self.staged_bytes = 0;
        self.lease = None;
        Ok(())
    }
}

fn next_staging_bytes(current: usize, batch: usize) -> Result<usize, DestinationError> {
    let next = current.checked_add(batch).ok_or_else(|| {
        DestinationError::fatal(
            "reference destination: staged Arrow footprint overflowed usize".to_string(),
        )
    })?;
    if next > STAGING_CEILING_BYTES {
        return Err(DestinationError::fatal(format!(
            "reference destination: staged Arrow data would exceed the \
             {STAGING_CEILING_BYTES}-byte session ceiling"
        )));
    }
    Ok(next)
}

/// The gate on the table component of a part filename. A table name is
/// the SOURCE's declaration — third-party input by the time it reaches
/// a destination — and `TableName` is deliberately unvalidated, so the
/// seat that turns one into a filename must judge it. Refused typed and
/// FATAL (no retry changes a declared name): a name carrying a path
/// separator, a `..` sequence, or a control character could steer the
/// part write outside the configured output directory. Engine hosts
/// normalize names before they get here, but a direct `Backend` driver
/// never passes that gate — and this connector is the worked example
/// third parties copy, so the safe pattern is modeled where the
/// filename is built.
/// A short hex digest of the whole `(table, load_id, commit_seq)` tuple
/// (8L10). Inputs are length-prefixed and domain-separated so
/// `("a","b-c")` and `("a-b","c")` cannot collide — the same discipline
/// the engine's row-identity hashing applies — and the digest is a pure
/// function of the tuple, so a retried publish still overwrites its own
/// part deterministically.
fn part_tuple_digest(table: &str, load_id: &str, commit_seq: u64) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"rdlt-reference:part:v1\0");
    for field in [table.as_bytes(), load_id.as_bytes()] {
        hasher.update(&(field.len() as u64).to_le_bytes());
        hasher.update(field);
    }
    hasher.update(&commit_seq.to_le_bytes());
    hasher.finalize().to_hex()[..8].to_owned()
}

fn part_component(table: &TableName) -> Result<(), DestinationError> {
    let name = table.as_str();
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || name.chars().any(char::is_control)
    {
        return Err(DestinationError::fatal(format!(
            "reference destination: table name {name:?} cannot become a part filename — \
             names carrying path separators, `..`, or control characters are refused, \
             because a filename built from them could land outside the output directory"
        )));
    }
    Ok(())
}

/// Gate the complete generated filename as well as its table component:
/// `load_id` is supplied by the host and must not become an accidental
/// path capability if the filename layout is ever refactored.
fn part_filename(name: &str) -> Result<(), DestinationError> {
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || name.chars().any(char::is_control)
    {
        return Err(DestinationError::fatal(format!(
            "reference destination: generated part filename {name:?} is unsafe — path \
             separators, `..`, and control characters are refused"
        )));
    }
    Ok(())
}

impl Writer {
    /// Cut a torn (newline-less) tail off the receipt log before
    /// appending to it. Those bytes were never a durable receipt —
    /// `existing_receipt` already reads them as absent — and appending
    /// after them would glue this commit's receipt into a corrupt,
    /// newline-terminated INTERIOR line, which is exactly the shape
    /// every later read refuses.
    fn truncate_torn_tail(&self, path: &std::path::Path) -> Result<(), DestinationError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(DestinationError::transient(format!(
                    "reference destination: read {}: {error}",
                    path.display()
                )));
            }
        };
        let durable = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |newline| newline + 1);
        if durable == bytes.len() {
            return Ok(());
        }
        let log = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|error| {
                DestinationError::transient(format!(
                    "reference destination: open {}: {error}",
                    path.display()
                ))
            })?;
        log.set_len(durable as u64).map_err(|error| {
            DestinationError::transient(format!(
                "reference destination: truncate the torn tail of {}: {error}",
                path.display()
            ))
        })?;
        Ok(())
    }

    /// Persist one table's part with [`persist`]'s exact atomic-durable
    /// shape — temporary, fsync, rename, directory fsync — but
    /// STREAM-ENCODED: the jsonl encoder writes straight into the
    /// buffered temporary file instead of one in-memory `Vec`. The
    /// staging ceiling meters the Arrow footprint of what a session
    /// retains; a whole-part jsonl buffer at publish time would be a
    /// second, unmetered copy at several times that footprint for
    /// struct- and json-heavy schemas.
    ///
    /// Failure classification splits by CAUSE: an IO failure (disk
    /// full, permissions — arrow surfaces it as its `IoError` arm)
    /// stays a transient `write` refusal like every other IO failure
    /// here; a genuine encode failure is fatal — no retry re-encodes
    /// the same batch differently.
    fn persist_part(
        &self,
        name: &str,
        table: &TableName,
        batches: &[&RecordBatch],
    ) -> Result<(), DestinationError> {
        let temp = self.dir.join(format!("_staged-{name}"));
        let target = self.dir.join(name);
        let framed = |verb: &str, path: &PathBuf, error: &std::io::Error| {
            DestinationError::transient(format!(
                "reference destination: {verb} {}: {error}",
                path.display()
            ))
        };
        let arrow_framed = |error: arrow::error::ArrowError| match error {
            arrow::error::ArrowError::IoError(_, io_error) => framed("write", &temp, &io_error),
            error => DestinationError::fatal(format!(
                "reference destination: encode `{table}` as jsonl: {error}"
            )),
        };
        let file = std::fs::File::create(&temp).map_err(|error| framed("write", &temp, &error))?;
        let mut writer = arrow::json::LineDelimitedWriter::new(std::io::BufWriter::new(file));
        for batch in batches {
            writer.write(batch).map_err(arrow_framed)?;
        }
        writer.finish().map_err(arrow_framed)?;
        let file = writer
            .into_inner()
            .into_inner()
            .map_err(|error| framed("write", &temp, error.error()))?;
        file.sync_all()
            .map_err(|error| framed("sync", &temp, &error))?;
        drop(file);
        std::fs::rename(&temp, &target).map_err(|error| framed("publish", &target, &error))?;
        self.sync_dir()?;
        Ok(())
    }

    /// Write `bytes` to `name` atomically AND durably: to an
    /// underscore-prefixed temporary first (invisible to any
    /// table-prefix reader), fsynced BEFORE the same-directory rename
    /// so the rename can never land pointing at unwritten cache, then
    /// the directory fsynced so the rename itself survives power loss.
    fn persist(&self, name: &str, bytes: &[u8]) -> Result<(), DestinationError> {
        let temp = self.dir.join(format!("_staged-{name}"));
        let target = self.dir.join(name);
        let framed = |verb: &str, path: &PathBuf, error: std::io::Error| {
            DestinationError::transient(format!(
                "reference destination: {verb} {}: {error}",
                path.display()
            ))
        };
        let mut file =
            std::fs::File::create(&temp).map_err(|error| framed("write", &temp, error))?;
        file.write_all(bytes)
            .map_err(|error| framed("write", &temp, error))?;
        file.sync_all()
            .map_err(|error| framed("sync", &temp, error))?;
        drop(file);
        std::fs::rename(&temp, &target).map_err(|error| framed("publish", &target, error))?;
        self.sync_dir()?;
        Ok(())
    }

    /// Append `receipt` to the log durably: torn tail cut, the line
    /// written and the log fsynced — and, when this append CREATED the
    /// log, the directory fsynced too, so the new file's very existence
    /// survives power loss. Called only after every part and the state
    /// document have been persisted; the ordering is the barrier.
    fn append_receipt(&self, receipt: &CommitReceipt) -> Result<(), DestinationError> {
        let line = serde_json::to_string(receipt).map_err(|error| {
            DestinationError::fatal(format!(
                "reference destination: encode the receipt: {error}"
            ))
        })?;
        let path = self.dir.join(RECEIPTS_FILE);
        self.truncate_torn_tail(&path)?;
        let framed = |verb: &str, error: std::io::Error| {
            DestinationError::transient(format!(
                "reference destination: {verb} {}: {error}",
                path.display()
            ))
        };
        let created = !path.exists();
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| framed("open", error))?;
        writeln!(log, "{line}").map_err(|error| framed("append to", error))?;
        log.sync_all().map_err(|error| framed("sync", error))?;
        if created {
            self.sync_dir()?;
        }
        Ok(())
    }

    /// Fsync the output directory itself — what makes a rename or a
    /// file creation durable, not just the bytes behind it.
    fn sync_dir(&self) -> Result<(), DestinationError> {
        std::fs::File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|error| {
                DestinationError::transient(format!(
                    "reference destination: sync {}: {error}",
                    self.dir.display()
                ))
            })
    }
}

/// The canonical face: `Shell::from_yaml(text)?` / `Shell::new(config)?`
/// is a running SPI destination in one call.
pub type Shell = rdlt_connector_sdk::destination::Shell<Reference>;

#[cfg(test)]
mod hardening_tests {
    use super::*;

    #[test]
    fn generated_parts_gate_the_load_id_and_every_filename_component() {
        assert!(part_filename("orders-load-1.jsonl").is_ok());
        assert!(part_filename("orders-load/escape-1.jsonl").is_err());
        assert!(part_filename("orders-load..escape-1.jsonl").is_err());
    }

    #[test]
    fn staging_refuses_before_crossing_its_memory_ceiling() {
        assert_eq!(
            next_staging_bytes(STAGING_CEILING_BYTES - 1, 1).expect("the boundary passes"),
            STAGING_CEILING_BYTES
        );
        assert!(next_staging_bytes(STAGING_CEILING_BYTES, 1).is_err());
        assert!(next_staging_bytes(usize::MAX, 1).is_err());
    }
}
