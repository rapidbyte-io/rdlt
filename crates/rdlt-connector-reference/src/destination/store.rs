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
use std::path::Path;

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

/// Write `bytes` to `name` atomically AND durably (see [`durable_write`]).
pub(crate) fn persist(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), DestinationError> {
    durable_write(dir, name, |file, temp| {
        file.write_all(bytes)
            .map_err(|error| io_refusal("write", temp, &error))
    })
}

/// Persist one table's part with the same atomic-durable shape, but
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
    durable_write(dir, name, |file, temp| {
        let arrow_refusal = |error: arrow::error::ArrowError| match error {
            arrow::error::ArrowError::IoError(_, io_error) => io_refusal("write", temp, &io_error),
            // The table name is wire-authored for a served backend and
            // unbounded for a direct driver — quoted bounded, like
            // every refusal seat around it.
            error => DestinationError::fatal(format!(
                "reference destination: encode `{}` as jsonl: {error}",
                rdlt_connector_sdk::spi::gate::render_diagnostic(table.as_str(), 256)
            )),
        };
        let mut writer = arrow::json::LineDelimitedWriter::new(file);
        for batch in batches {
            writer.write(batch).map_err(arrow_refusal)?;
        }
        writer.finish().map_err(arrow_refusal)
    })
}

/// THE atomic-durable write every published file goes through: `fill`
/// writes into an underscore-prefixed temporary (invisible to any
/// table-prefix reader), the temporary is fsynced BEFORE the
/// same-directory rename so the rename can never land pointing at
/// unwritten cache, then the directory is fsynced so the rename itself
/// survives power loss.
fn durable_write(
    dir: &Path,
    name: &str,
    fill: impl FnOnce(&mut std::io::BufWriter<std::fs::File>, &Path) -> Result<(), DestinationError>,
) -> Result<(), DestinationError> {
    let temp = dir.join(format!("_staged-{name}"));
    let target = dir.join(name);
    let file = std::fs::File::create(&temp).map_err(|error| io_refusal("write", &temp, &error))?;
    let mut writer = std::io::BufWriter::new(file);
    fill(&mut writer, &temp)?;
    let file = writer
        .into_inner()
        .map_err(|error| io_refusal("write", &temp, error.error()))?;
    file.sync_all()
        .map_err(|error| io_refusal("sync", &temp, &error))?;
    drop(file);
    std::fs::rename(&temp, &target).map_err(|error| io_refusal("publish", &target, &error))?;
    sync_dir(dir)
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
    let created = !path.exists();
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| io_refusal("open", &path, &error))?;
    writeln!(log, "{line}").map_err(|error| io_refusal("append to", &path, &error))?;
    log.sync_all()
        .map_err(|error| io_refusal("sync", &path, &error))?;
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
    if !gate_store_read(&path, MAX_RECEIPT_LOG_BYTES, "receipt log")? {
        return Ok(None);
    }
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_refusal("read", &path, &error)),
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
            // The quoted line is DISK content and the serde error can
            // embed fragments of it — both through the bounded
            // diagnostic render, so a corrupt log cannot hand a
            // terminal-injection payload to whoever reads the error.
            DestinationError::fatal(format!(
                "reference destination: {} carries a corrupt receipt line `{}`: {}",
                path.display(),
                rdlt_connector_sdk::spi::gate::render_diagnostic(line, 256),
                rdlt_connector_sdk::spi::gate::render_diagnostic(&error.to_string(), 256)
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
    // The state document rides the shared 8 MiB document ceiling —
    // the same bound every untyped-document seat enforces.
    if !gate_store_read(
        &path,
        rdlt_connector_sdk::spi::gate::MAX_DOCUMENT_BYTES,
        "state document",
    )? {
        return Ok(None);
    }
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_refusal("read", &path, &error)),
    };
    let state: StateDoc = serde_json::from_str(&text).map_err(|error| {
        // The serde error can embed fragments of the corrupt DISK
        // content — bounded like the receipt-line refusal.
        DestinationError::fatal(format!(
            "reference destination: {} carries a corrupt state document: {}",
            path.display(),
            rdlt_connector_sdk::spi::gate::render_diagnostic(&error.to_string(), 256)
        ))
    })?;
    Ok(Some(state))
}

/// The receipt log's read ceiling. One line is at most ~1.1 KiB (a
/// load id at the 1024-byte wire identifier ceiling, a u64 sequence,
/// and JSON punctuation), so 8 MiB holds ~7,600 maximal-id receipts —
/// or ~250,000 short-id ones — far past this exemplar store's honest
/// life, and consistent with the document family's 8 MiB ceilings.
const MAX_RECEIPT_LOG_BYTES: u64 = 8 * 1024 * 1024;

/// Refuse a store file that cannot be an honest artifact BEFORE any
/// byte of it is read: a non-regular occupant refuses typed (a FIFO
/// would block the read forever), and a size past the seat's ceiling
/// refuses typed (a sparse or hostile multi-GiB occupant would
/// materialize whole before any content check could run). `Ok(false)`
/// is the absent arm — the caller's `NotFound` disposition. The
/// metadata-then-read window is the at-rest directory writer's
/// existing power (directory ownership is the trust boundary), not a
/// new one.
fn gate_store_read(path: &Path, ceiling: u64, what: &str) -> Result<bool, DestinationError> {
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(io_refusal("probe", path, &error)),
    };
    if !meta.is_file() {
        return Err(DestinationError::fatal(format!(
            "reference destination: {} is not a regular file — refusing to read the {what} \
             from it",
            rdlt_connector_sdk::spi::gate::render_diagnostic(&path.display().to_string(), 256)
        )));
    }
    if meta.len() > ceiling {
        return Err(DestinationError::fatal(format!(
            "reference destination: {} weighs {} bytes — over the {ceiling}-byte {what} read \
             ceiling; the store never writes it that large",
            rdlt_connector_sdk::spi::gate::render_diagnostic(&path.display().to_string(), 256),
            meta.len()
        )));
    }
    Ok(true)
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
    if !gate_store_read(path, MAX_RECEIPT_LOG_BYTES, "receipt log")? {
        return Ok(());
    }
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_refusal("read", path, &error)),
    };
    let durable = durable_len(&bytes);
    if durable == bytes.len() {
        return Ok(());
    }
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|error| io_refusal("open", path, &error))?
        .set_len(durable as u64)
        .map_err(|error| io_refusal("truncate the torn tail of", path, &error))
}

/// Fsync the output directory itself — what makes a rename or a file
/// creation durable, not just the bytes behind it.
fn sync_dir(dir: &Path) -> Result<(), DestinationError> {
    std::fs::File::open(dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| io_refusal("sync", dir, &error))
}

/// The one transient IO refusal shape: `<verb> <path>: <os error>`.
/// The path renders BOUNDED: staged paths embed the caller-supplied
/// part name, unbounded for a direct driver — identity for every
/// honest path.
fn io_refusal(verb: &str, path: &Path, error: &std::io::Error) -> DestinationError {
    DestinationError::transient(format!(
        "reference destination: {verb} {}: {error}",
        rdlt_connector_sdk::spi::gate::render_diagnostic(&path.display().to_string(), 256)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The staged-path io refusal renders its path BOUNDED: the path
    /// embeds the caller-supplied part name, unbounded for a direct
    /// driver, so a hostile multi-KB name arrives escaped and
    /// truncated in the refusal, never whole.
    #[test]
    fn a_staged_path_io_refusal_renders_bounded() {
        let missing = Path::new("/nonexistent-rdlt-test-dir");
        let hostile = format!("evil\u{1b}]52;c;A\u{7}{}", "x".repeat(2000));
        let refused = persist(missing, &hostile, b"x")
            .expect_err("writing under a missing directory refuses");
        let rendered = refused.to_string();
        assert!(
            !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
            "no raw control byte survives: {rendered:?}"
        );
        assert!(
            rendered.len() < 700,
            "the path render is bounded, not name-scale: {} bytes",
            rendered.len()
        );
    }
}
