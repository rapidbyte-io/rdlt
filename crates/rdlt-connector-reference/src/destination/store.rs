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
    // The write side gates every line at the same bound the readers
    // enforce, so the log can never hold a line its own scans refuse —
    // an id long enough to cross it is a wrong configuration no retry
    // shortens, refused HERE instead of poisoning every later publish.
    if line.len() > MAX_RECEIPT_LINE_BYTES {
        return Err(DestinationError::fatal(format!(
            "reference destination: the receipt line for this commit is {} bytes — over the \
             {MAX_RECEIPT_LINE_BYTES}-byte line bound the log's readers enforce; a load id \
             this long cannot be receipted",
            line.len()
        )));
    }
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
/// absent (the next append cuts them); an unparseable, non-UTF-8, or
/// over-long line that IS newline-terminated sits in the log's
/// interior, which the writer never produces — corruption, refused
/// fatal. The scan STREAMS: the log is an append-only journal that
/// grows for the store's whole life, so its reader holds one line at a
/// time — a total ceiling here would be a wedge every honest store
/// eventually reaches, not a bound.
pub(crate) fn find_receipt(
    dir: &Path,
    load_id: &LoadId,
    commit_seq: u64,
) -> Result<Option<CommitReceipt>, DestinationError> {
    use std::io::BufRead as _;
    let path = dir.join(RECEIPTS_FILE);
    if gate_regular(&path, "receipt log")?.is_none() {
        return Ok(None);
    }
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_refusal("open", &path, &error)),
    };
    let mut reader = std::io::BufReader::new(file);
    let mut line: Vec<u8> = Vec::new();
    loop {
        let chunk = match reader.fill_buf() {
            Ok(chunk) => chunk,
            Err(error) => return Err(io_refusal("read", &path, &error)),
        };
        if chunk.is_empty() {
            // EOF with accumulated bytes: a torn append that never
            // became durable — contractually ABSENT (the next append
            // cuts it).
            return Ok(None);
        }
        let (taken, complete) = match chunk.iter().position(|byte| *byte == b'\n') {
            Some(newline) => {
                line.extend_from_slice(&chunk[..newline]);
                (newline + 1, true)
            }
            None => {
                line.extend_from_slice(chunk);
                (chunk.len(), false)
            }
        };
        reader.consume(taken);
        if line.len() > MAX_RECEIPT_LINE_BYTES {
            return Err(receipt_line_bound_refusal(&path));
        }
        if !complete {
            continue;
        }
        if let Some(receipt) = decode_receipt_line(&path, &line)?
            && receipt.load_id == *load_id
            && receipt.commit_seq == commit_seq
        {
            return Ok(Some(receipt));
        }
        line.clear();
    }
}

/// One complete (newline-terminated) receipt line, decoded — or `None`
/// for a blank line. Decoded per line and BYTES-first, never
/// `read_to_string` over the log: a torn append can split a multi-byte
/// UTF-8 character (a non-ASCII load id rides the receipt line
/// verbatim), and a whole-file decode would fail the durable prefix
/// for the tail's tear.
fn decode_receipt_line(
    path: &Path,
    line: &[u8],
) -> Result<Option<CommitReceipt>, DestinationError> {
    let line = std::str::from_utf8(line).map_err(|error| {
        DestinationError::fatal(format!(
            "reference destination: {} carries a corrupt receipt log (invalid UTF-8 \
             before the last complete line): {error}",
            path.display()
        ))
    })?;
    if line.trim().is_empty() {
        return Ok(None);
    }
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
    Ok(Some(receipt))
}

/// The refusal for a receipt line no writer produces: the append side
/// gates every line at the same bound, so an over-long line — complete
/// or torn — is disk corruption, not growth.
fn receipt_line_bound_refusal(path: &Path) -> DestinationError {
    DestinationError::fatal(format!(
        "reference destination: {} carries a receipt line over the \
         {MAX_RECEIPT_LINE_BYTES}-byte line bound — the store never writes one that long; \
         refusing the log as corrupt",
        path.display()
    ))
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

/// The receipt log's per-LINE bound — the streaming scan's whole
/// memory footprint, and the append side's own gate, so the reader
/// never refuses a line the writer can produce. A maximal honest line
/// is a load id at the 1024-byte wire identifier ceiling JSON-escaped
/// at worst six bytes per input byte (`\uXXXX`), plus keys, a u64
/// sequence and punctuation: under 6.2 KiB. 8 KiB refuses nothing an
/// honest append writes. The log's TOTAL size is deliberately
/// unbounded: it is an append-only journal that grows for the store's
/// whole life, and a total ceiling was a wedge every honest store
/// eventually reached — once crossed, every later publish refused
/// FATAL with no compaction story.
const MAX_RECEIPT_LINE_BYTES: usize = 8 * 1024;

/// Refuse a store file that cannot be an honest artifact BEFORE any
/// byte of it is read: a non-regular occupant refuses typed (a FIFO
/// would block the read forever). `Ok(None)` is the absent arm — the
/// caller's `NotFound` disposition; `Ok(Some(len))` carries the
/// occupant's size for seats that bound their reads. The
/// metadata-then-read window is the at-rest directory writer's
/// existing power (directory ownership is the trust boundary), not a
/// new one.
fn gate_regular(path: &Path, what: &str) -> Result<Option<u64>, DestinationError> {
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_refusal("probe", path, &error)),
    };
    if !meta.is_file() {
        return Err(DestinationError::fatal(format!(
            "reference destination: {} is not a regular file — refusing to read the {what} \
             from it",
            rdlt_connector_sdk::spi::gate::render_diagnostic(&path.display().to_string(), 256)
        )));
    }
    Ok(Some(meta.len()))
}

/// The shape-and-size gate for SINGLE-DOCUMENT store files (the state
/// document): a size past the seat's ceiling refuses typed — a sparse
/// or hostile multi-GiB occupant would materialize whole before any
/// content check could run. `Ok(false)` is the absent arm.
fn gate_store_read(path: &Path, ceiling: u64, what: &str) -> Result<bool, DestinationError> {
    let Some(len) = gate_regular(path, what)? else {
        return Ok(false);
    };
    if len > ceiling {
        return Err(DestinationError::fatal(format!(
            "reference destination: {} weighs {len} bytes — over the {ceiling}-byte {what} read \
             ceiling; the store never writes it that large",
            rdlt_connector_sdk::spi::gate::render_diagnostic(&path.display().to_string(), 256)
        )));
    }
    Ok(true)
}

/// Cut a torn (newline-less) tail off the receipt log before appending
/// to it. Those bytes were never a durable receipt — `find_receipt`
/// already reads them as absent — and appending after them would glue
/// this commit's receipt into a corrupt, newline-terminated INTERIOR
/// line, which is exactly the shape every later read refuses. Only the
/// tail WINDOW is read (one maximal line and its would-be newline):
/// a tear is at most one gated append, so a longer newline-less tail
/// is not a tear at all — it is corruption, refused like the over-long
/// interior line it would otherwise become.
fn truncate_torn_tail(path: &Path) -> Result<(), DestinationError> {
    use std::io::{Read as _, Seek as _};
    let Some(len) = gate_regular(path, "receipt log")? else {
        return Ok(());
    };
    if len == 0 {
        return Ok(());
    }
    let window = len.min(MAX_RECEIPT_LINE_BYTES as u64 + 1);
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_refusal("open", path, &error)),
    };
    file.seek(std::io::SeekFrom::Start(len - window))
        .map_err(|error| io_refusal("seek", path, &error))?;
    let mut tail = vec![0u8; window as usize];
    file.read_exact(&mut tail)
        .map_err(|error| io_refusal("read", path, &error))?;
    drop(file);
    if tail.ends_with(b"\n") {
        return Ok(());
    }
    let durable = match tail.iter().rposition(|byte| *byte == b'\n') {
        Some(newline) => len - window + newline as u64 + 1,
        // No newline anywhere in the window: either the whole log is
        // one short torn write (cut to empty), or the newline-less
        // tail outweighs any line the gated append writes — corruption.
        None if window == len => 0,
        None => return Err(receipt_line_bound_refusal(path)),
    };
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|error| io_refusal("open", path, &error))?
        .set_len(durable)
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
