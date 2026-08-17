//! The on-disk formats and the fsync discipline, as free functions over
//! the output directory.
//!
//! THE DURABILITY ORDER IS THE EXACTLY-ONCE PROOF: every part, then the
//! state document, each written to an underscore-prefixed temporary,
//! fsynced, renamed into place and the directory fsynced — and only
//! THEN the receipt appended and its log fsynced. A receipt that
//! reached the journal while a part still sat in page cache would,
//! after power loss, vouch for a commit whose rows are gone: replay
//! would drop the redelivered staging and the loss would be silent.
//! The receipt's terminating newline is its durability marker: a
//! newline-less tail is a torn append, read as absent and cut before
//! the next append, so the retried commit republishes convergently over
//! its deterministic part names.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use rdlt_connector_sdk::spi::arrow::RecordBatch;
use rdlt_connector_sdk::spi::core::commit::CommitReceipt;
use rdlt_connector_sdk::spi::core::id::{LoadId, TableName};
use rdlt_connector_sdk::spi::core::state::StateDoc;
use rdlt_connector_sdk::spi::error::DestinationError;

/// The append-only receipt log: one json line per published commit,
/// `{"load_id":<string>,"commit_seq":<u64>}` — what the sdk's replay
/// choreography is answered from.
pub(crate) const RECEIPTS_FILE: &str = "_reference_receipts.json";

/// The latest committed state document, written atomically (write to a
/// temporary, fsync, then rename) by every publish, BEFORE its receipt.
pub(crate) const STATE_FILE: &str = "_reference_state.json";

/// The session lease: an OS advisory lock held from connect to drop.
/// Two concurrent sessions over one directory would each read the same
/// persisted cursor and publish the same rows under their own load ids
/// — deterministic part names dedupe only WITHIN a load — so the
/// second open is refused typed instead. Advisory locks release on
/// process death, so a crashed run never blocks its own recovery.
pub(crate) const LEASE_FILE: &str = "_reference_lease.lock";

/// Write `bytes` to `name` atomically AND durably: to an
/// underscore-prefixed temporary first (invisible to any table-prefix
/// reader), fsynced BEFORE the same-directory rename so the rename can
/// never land pointing at unwritten cache, then the directory fsynced
/// so the rename itself survives power loss.
pub(crate) fn persist(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), DestinationError> {
    let temp = dir.join(format!("_staged-{name}"));
    let target = dir.join(name);
    let framed = |verb: &str, path: &PathBuf, error: std::io::Error| {
        DestinationError::transient(format!(
            "reference destination: {verb} {}: {error}",
            path.display()
        ))
    };
    let mut file = std::fs::File::create(&temp).map_err(|error| framed("write", &temp, error))?;
    file.write_all(bytes)
        .map_err(|error| framed("write", &temp, error))?;
    file.sync_all()
        .map_err(|error| framed("sync", &temp, error))?;
    drop(file);
    std::fs::rename(&temp, &target).map_err(|error| framed("publish", &target, error))?;
    sync_dir(dir)?;
    Ok(())
}

/// Persist one table's part with [`persist`]'s exact atomic-durable
/// shape — temporary, fsync, rename, directory fsync — but
/// STREAM-ENCODED: the jsonl encoder writes straight into the buffered
/// temporary file instead of one in-memory `Vec`. The staging ceiling
/// meters the Arrow footprint of what a session retains; a whole-part
/// jsonl buffer at publish time would be a second, unmetered copy at
/// several times that footprint for struct- and json-heavy schemas.
///
/// Failure classification splits by CAUSE: an IO failure (disk full,
/// permissions — arrow surfaces it as its `IoError` arm) stays a
/// transient `write` refusal like every other IO failure here; a
/// genuine encode failure is fatal — no retry re-encodes the same batch
/// differently.
pub(crate) fn persist_part(
    dir: &Path,
    name: &str,
    table: &TableName,
    batches: &[&RecordBatch],
) -> Result<(), DestinationError> {
    let temp = dir.join(format!("_staged-{name}"));
    let target = dir.join(name);
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
    sync_dir(dir)?;
    Ok(())
}

/// Append `receipt` to the log durably: torn tail cut, the line written
/// and the log fsynced — and, when this append CREATED the log, the
/// directory fsynced too, so the new file's very existence survives
/// power loss.
pub(crate) fn append_receipt(dir: &Path, receipt: &CommitReceipt) -> Result<(), DestinationError> {
    let line = serde_json::to_string(receipt).map_err(|error| {
        DestinationError::fatal(format!(
            "reference destination: encode the receipt: {error}"
        ))
    })?;
    let path = dir.join(RECEIPTS_FILE);
    truncate_torn_tail(&path)?;
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
        sync_dir(dir)?;
    }
    Ok(())
}

/// The receipt the log holds for `(load_id, commit_seq)`, if any. Only
/// newline-terminated lines count: bytes after the LAST newline are an
/// append that tore mid-write and never became durable, so they read as
/// absent (the next append cuts them); an unparseable or non-UTF-8 line
/// that IS newline-terminated sits in the log's interior, which the
/// writer never produces — corruption, refused fatal.
pub(crate) fn find_receipt(
    dir: &Path,
    load_id: &LoadId,
    commit_seq: u64,
) -> Result<Option<CommitReceipt>, DestinationError> {
    let path = dir.join(RECEIPTS_FILE);
    // Read BYTES, never `read_to_string`: a torn append can split a
    // multi-byte UTF-8 character (a non-ASCII load id rides the receipt
    // line verbatim), and `read_to_string` would fail the WHOLE read as
    // `InvalidData` — a transient the choreography retries forever, when
    // the torn tail is contractually ABSENT. Decoding the complete lines
    // keeps the tear where it belongs: in the tail.
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
    let durable = &bytes[..durable_len(&bytes)];
    let durable = std::str::from_utf8(durable).map_err(|error| {
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

/// The persisted state document, if any publish ever wrote one.
pub(crate) fn read_state(dir: &Path) -> Result<Option<StateDoc>, DestinationError> {
    let path = dir.join(STATE_FILE);
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
    Ok(Some(state))
}

/// The length of the log's durable prefix: everything up to and
/// including the last newline.
fn durable_len(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |newline| newline + 1)
}

/// Cut a torn (newline-less) tail off the receipt log before appending
/// to it. Those bytes were never a durable receipt — `find_receipt`
/// already reads them as absent — and appending after them would glue
/// this commit's receipt into a corrupt, newline-terminated INTERIOR
/// line, which is exactly the shape every later read refuses.
fn truncate_torn_tail(path: &Path) -> Result<(), DestinationError> {
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
    let durable = durable_len(&bytes);
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

/// Fsync the output directory itself — what makes a rename or a file
/// creation durable, not just the bytes behind it.
fn sync_dir(dir: &Path) -> Result<(), DestinationError> {
    std::fs::File::open(dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| {
            DestinationError::transient(format!(
                "reference destination: sync {}: {error}",
                dir.display()
            ))
        })
}
