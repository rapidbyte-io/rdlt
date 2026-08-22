//! The SPI's trust-boundary toolbox: the size ceilings and refusal
//! helpers every seat that handles connector-authored bytes installs
//! before acting on them.
//!
//! One implementation of each rule, shared by both sides of the wire:
//! the sdk's serve seats (whose adversary is a rogue client) and the
//! client's decode seats (whose adversary is a rogue connector) import
//! the same functions, so the two sides cannot drift. The engine's
//! in-process validation imports the same ceilings, so a declaration
//! that would be refused on the wire is refused in-process too. The
//! Arrow IPC seats live here too — the one decode/encode discipline
//! every wire frame's batch payload passes, in whichever direction it
//! travels.

use std::borrow::Cow;

use arrow_array::RecordBatch;
// ArrowError is defined in arrow-schema and re-exported at its root
// (`pub use error::*`) — the fine-grained crates carry no other public
// path to it.
use arrow_schema::ArrowError;
use rdlt_core::inventory;

/// The most a JSON configuration/state DOCUMENT may weigh, in bytes.
///
/// Connector config documents and persisted cursors are hand-written
/// or summarized state measured in kilobytes, and an untyped
/// `serde_json::Value` materializes at many times its wire size — so
/// every seat that parses or forwards one enforces this ONE ceiling
/// before parsing, bounding the parse's own materialization rather
/// than cleaning up after it. The cursor contract for connector
/// authors: persisted state MUST serialize under this ceiling — a
/// connector whose state can outgrow it summarizes (a high-water mark,
/// an offset, a resume token) rather than embedding the data.
pub const MAX_DOCUMENT_BYTES: u64 = 8 * 1024 * 1024;

/// The most a persisted CURSOR may weigh, in bytes — the effective
/// cursor contract, TIGHTER than [`MAX_DOCUMENT_BYTES`].
///
/// A cursor is not only parsed and sent but recorded in the engine's
/// WAL, whose per-line metadata cap is sized to carry one maximal
/// cursor line. A cursor over this bound is refused typed at every
/// seat, so an over-scale cursor fails LOUDLY at first contact (the
/// connector must summarize) rather than crash-looping a resume
/// against a line its own recovery could never scan back.
pub const MAX_CURSOR_BYTES: u64 = 4 * 1024 * 1024;

/// The most BYTES a wire-declared identifier may carry.
///
/// Content gates refuse hostile CHARACTERS; this prices the LENGTH,
/// which nothing else bounds — a 60 MiB control-free stream name would
/// pass every content gate within the frame cap and swell logs and
/// plans. Real identifiers are bounded far lower by the destinations'
/// own limits (postgres 63 bytes, snowflake 255); a KiB is already
/// absurd for a name.
pub const MAX_WIRE_IDENTIFIER_BYTES: usize = 1024;

/// The most fields one primary key may name.
///
/// An honest primary key names one to a few columns; a hostile spec
/// names hundreds of thousands of tiny gate-legal keys and passes every
/// per-value identifier gate within one frame otherwise. Every seat
/// that admits a declared spec's key — the client's decode seat and the
/// serve side's read seat — holds the collection to this count, so the
/// two sides cannot drift.
pub const MAX_PRIMARY_KEY_FIELDS: usize = 64;

/// The most columns one table or stream may carry, wherever that width
/// is declared or enforced.
///
/// One number, one meaning, every seat: the engine's shred assembly
/// refuses a table past this width, the wire's batch decode refuses a
/// frame declaring more `Field`s (a schema message spends forty-odd
/// flatbuffer bytes per field — the cheapest per-byte expansion an
/// Arrow frame offers), the ensure seat holds an ensured schema's
/// column count (nested fields included) to it, and a spec's type
/// hints — which cover at most the stream's columns — are capped at
/// the same number. Sharing the constant is the point: a width that is
/// legal in-process is legal on the wire, and raising one seat raises
/// every seat or fails to compile.
pub const MAX_SOURCE_COLUMNS_PER_TABLE: usize = 4096;

/// The most stream specs one `Streams` reply may declare.
///
/// Both ends of the wire hold the reply to this: the serving side so a
/// connector learns from its own refusal rather than by building a
/// reply no frame can carry, and the dialing side because it serves
/// every host — an embedder driving the client directly has no engine
/// in front of it, and the engine's own per-source cap refuses only
/// after the reply has fully materialized.
///
/// It is the engine's default `max_streams_per_source`. A host that
/// raises that knob past this number over a SERVED source will have its
/// streams reply refused at discovery; the ceiling is not configurable
/// today.
pub const MAX_DECLARED_STREAM_SPECS: usize = 1024;

/// Render a JSON parse failure as kind and position only: which class
/// of failure, at which line and column.
///
/// serde's data errors embed the offending TOKEN (`invalid type:
/// string "…"`), and the token is wire text — a malformed document
/// would otherwise ride a fragment into a host log or a certification
/// report through the refusal that rejected it. Nothing derived from
/// the document's content appears in the output.
pub fn describe_parse_error(error: &serde_json::Error) -> String {
    let kind = match error.classify() {
        serde_json::error::Category::Syntax => "syntax error",
        serde_json::error::Category::Eof => "unexpected end of input",
        serde_json::error::Category::Data => "document shape mismatch",
        serde_json::error::Category::Io => "read failure",
    };
    format!("{kind} at line {} column {}", error.line(), error.column())
}

/// Refuse typed when a raw inbound document exceeds
/// [`MAX_DOCUMENT_BYTES`] — the gate every untyped `Value` (or typed
/// shell around one) runs BEFORE parsing. The message carries byte
/// counts and the ceiling only; nothing derived from the document's
/// content.
pub fn refuse_oversized_document(field: &str, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
        return Err(format!(
            "an inbound {field} of {} bytes exceeds the {}-byte document ceiling — a \
             hand-authored or summarized document measures in kilobytes",
            bytes.len(),
            MAX_DOCUMENT_BYTES
        ));
    }
    Ok(())
}

/// The greatest number of NODES a cursor may carry once parsed.
/// [`MAX_CURSOR_BYTES`] bounds what arrives; it does not bound what the
/// arrival becomes, and structure is where an untyped document expands
/// worst: `[0,0,0,…]` spends two wire bytes per element and buys a
/// `Value` node — tens of bytes plus vector slack — so a legal cursor
/// reaches a couple of million nodes, and every seat that takes one
/// RETAINS it (a read for its lifetime, a checkpoint into queued and
/// persisted state, a state document into the destination's store).
/// Counting the nodes bounds that dimension without inspecting a
/// single value: the cursor stays the opaque document its contract
/// says it is, and only its size is judged. An honest cursor — a
/// watermark, a page token, a handful of per-partition offsets — is
/// orders of magnitude below it.
pub const MAX_CURSOR_NODES: usize = 64 * 1024;

/// A parsed cursor's node count against [`MAX_CURSOR_NODES`] — the ONE
/// walk every seat that parses a cursor runs immediately after the
/// parse: a checkpoint from a source, a resume cursor into a source, a
/// committed or read-back state document's cursors. Iterative with its
/// own stack: a cursor's nesting is bounded by the parser that
/// produced it, but a recursive walk would make this gate the thing
/// that overflows. What it bounds is RETENTION; the byte ceiling bounds
/// the parse that precedes it.
pub fn refuse_dense_cursor(field: &str, value: &serde_json::Value) -> Result<(), String> {
    let mut pending = vec![value];
    let mut nodes = 0usize;
    while let Some(node) = pending.pop() {
        nodes += 1;
        if nodes > MAX_CURSOR_NODES {
            return Err(format!(
                "{field} carries more than {MAX_CURSOR_NODES} document nodes — a cursor is a \
                 position, not a payload"
            ));
        }
        match node {
            serde_json::Value::Array(items) => pending.extend(items.iter()),
            serde_json::Value::Object(fields) => pending.extend(fields.values()),
            _ => {}
        }
    }
    Ok(())
}

/// Render adversarial text for a DIAGNOSTIC, bounded: control
/// characters (the terminal-injection class — ESC/BEL-driven OSC
/// sequences, forged newlines) render as their spelled-out escapes,
/// and the whole render truncates at `cap` raw bytes with a marker
/// naming the true length — a forged identity as large as a frame
/// allows must not turn a refusal into a firehose. Non-control
/// characters pass through — Cf format characters included (bidi
/// overrides, joiners), deliberately: this is a diagnostic quoting
/// seat, not a display seat, and the full invisible-character
/// inventory belongs to the wire gates and the display renders (the
/// CLI's display boundary spells them out).
pub fn render_diagnostic(text: &str, cap: usize) -> String {
    let mut out = String::with_capacity(text.len().min(cap + 32));
    for character in text.chars() {
        if character.is_control() {
            for escaped in character.escape_debug() {
                out.push(escaped);
            }
        } else {
            out.push(character);
        }
        if out.len() >= cap {
            use std::fmt::Write as _;
            let _ = write!(out, "…[truncated from {} bytes]", text.len());
            return out;
        }
    }
    out
}

/// Walk one Arrow IPC stream's encapsulated-message framing — optional
/// continuation marker, an `i32` metadata length, the metadata bytes,
/// then the metadata's own declared `bodyLength` of body bytes — and
/// refuse any message whose DECLARED lengths exceed what the frame
/// actually carries. Install it before every `StreamReader`/
/// `FileReader` construction over connector-supplied bytes, and keep a
/// `catch_unwind` belt beside it for arrow's panic arms — the two
/// failure modes are disjoint.
///
/// The threat: arrow-ipc trusts both declarations before verifying
/// them against the input. Its stream reader `resize`s the metadata
/// buffer to the declared length (a commit and memset of the full
/// size) and allocates `bodyLength` zeroed bytes for the body, in each
/// case BEFORE `read_exact` discovers the bytes are missing — so a
/// ~30-byte frame declaring ~2 GiB forces a 2 GiB allocate-and-memset
/// in the host per frame, which is not a panic (no `catch_unwind` is a
/// defense) and under a memory limit is an OOM kill. A negative
/// `bodyLength` is the sibling: cast to `usize` it wraps huge, and the
/// failing allocation ABORTS the process outright. Checking every
/// declaration against `bytes.len()` first kills both vectors; wire
/// frames arrive whole (one gRPC field each), so a valid frame can
/// never declare past its own end. Should IPC compression ever be
/// enabled (today no feature and no lz4/zstd is in the lockfile), this
/// walk must ALSO bound the decompressed length — arrow's
/// decompression does an unbounded `Vec::with_capacity` from a
/// body-declared length, the same class one layer down.
///
/// The reason strings name the offending declaration verbatim; seats
/// wrap them in their own refusal vocabulary. All arithmetic is
/// checked: a walk this refuses is malformed by construction, never a
/// panic. Truncation SHORT of a declaration (too few bytes for a
/// length word) is left for the reader's own EOF handling — nothing
/// oversized gets allocated on that path.
pub fn refuse_overdeclared_framing(bytes: &[u8]) -> Result<(), String> {
    const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
    let refuse = |what: &str, declared: u64| {
        format!(
            "a declared {what} length of {declared} bytes exceeds the {}-byte frame",
            bytes.len()
        )
    };
    let mut pos = 0usize;
    loop {
        let Some(word) = bytes.get(pos..pos + 4) else {
            return Ok(());
        };
        let word: [u8; 4] = word.try_into().expect("a 4-byte slice");
        let length_word = if word == CONTINUATION_MARKER {
            pos += 4;
            match bytes.get(pos..pos + 4) {
                Some(next) => next.try_into().expect("a 4-byte slice"),
                None => return Ok(()),
            }
        } else {
            word
        };
        pos += 4;
        let declared_meta = i32::from_le_bytes(length_word);
        if declared_meta == 0 {
            // The stream's end-of-stream marker.
            return Ok(());
        }
        let meta_len = usize::try_from(declared_meta)
            .map_err(|_| format!("a negative declared metadata length ({declared_meta})"))?;
        let meta_end = pos
            .checked_add(meta_len)
            .filter(|&end| end <= bytes.len())
            .ok_or_else(|| refuse("metadata", meta_len as u64))?;
        // The metadata really is present — now hold its own body
        // declaration to the same standard. The flatbuffer runs the
        // same verifier the reader itself would, so an unverifiable
        // message refuses here with the verifier's diagnostic.
        let message = arrow_ipc::root_as_message(&bytes[pos..meta_end])
            .map_err(|error| format!("unverifiable message metadata: {error}"))?;
        // A negative declaration renders SIGNED — casting to u64 first
        // would print the wrapped value, a diagnostic that lies about
        // what the frame actually declared.
        let body_len = u64::try_from(message.bodyLength())
            .map_err(|_| format!("a negative declared body length ({})", message.bodyLength()))?;
        pos = usize::try_from(body_len)
            .ok()
            .and_then(|body| meta_end.checked_add(body))
            .filter(|&end| end <= bytes.len())
            .ok_or_else(|| refuse("body", body_len))?;
    }
}

// ---- the bounded render -----------------------------------------------------

/// Render text inert for display: every control or invisible character
/// — the full inventory, joiners included — becomes its spelled-out
/// escape, while everything else (quotes, non-ASCII text, which are
/// data) passes byte-identical. Borrowed unchanged when there is
/// nothing to escape, which is every honest message.
pub fn escape(text: &str) -> Cow<'_, str> {
    if !text.chars().any(inventory::is_control_or_invisible) {
        return Cow::Borrowed(text);
    }
    let mut escaped = String::with_capacity(text.len() + 8);
    for character in text.chars() {
        push_escaped(&mut escaped, character);
    }
    Cow::Owned(escaped)
}

/// One character, spelled inert if the inventory names it, verbatim
/// otherwise — the escape rule both renders share.
fn push_escaped(out: &mut String, character: char) {
    if inventory::is_control_or_invisible(character) {
        // `escape_debug` passes "printable" characters through, and
        // the inventory's two Hangul fillers are category Lo —
        // printable to it while rendering as blank glyphs — so
        // anything it hands back unchanged is spelled out by hand.
        let mut debug = character.escape_debug();
        if debug.len() == 1 && debug.next() == Some(character) {
            use std::fmt::Write as _;
            let _ = write!(out, "\\u{{{:x}}}", character as u32);
        } else {
            out.extend(character.escape_debug());
        }
    } else {
        out.push(character);
    }
}

/// The bounded render as an `fmt::Write` sink: text written to it is
/// escaped (via the shared `push_escaped` rule) and KEPT only until
/// the output reaches `cap`, with everything after counted and
/// discarded — up to a HARD SOURCE CEILING of twice the cap. A write
/// arriving past the ceiling is refused with `fmt::Error`, which the
/// std formatting machinery propagates, so a hostile `Debug` that
/// would stream tens of MB of chunks (seconds of synchronous CPU
/// inside error construction) stops within one chunk of the ceiling
/// instead. The one write that CROSSES the ceiling is still counted
/// whole and accepted — so a source that arrives as a single `&str`
/// keeps its exact count whatever its size, and only a source with
/// more to say AFTER the ceiling is cut. [`BoundedWriter::finish`]
/// appends the truncation marker: the exact source length when
/// everything was counted, `≥N` when the refusal left bytes uncounted.
#[derive(Debug)]
pub struct BoundedWriter {
    out: String,
    cap: usize,
    /// Every byte the source offered before the ceiling refusal, kept
    /// or discarded — what the truncation marker reports.
    source_bytes: usize,
    /// Set the moment the output reaches the cap: later writes only
    /// count, and `finish` appends the marker.
    saturated: bool,
    /// Set when the counted source crosses twice the cap: the next
    /// write refuses and nothing more is counted.
    refused: bool,
    /// Set when a write WAS refused — bytes exist beyond the count, so
    /// the marker spells `≥`.
    undercounted: bool,
}

impl BoundedWriter {
    /// A sink keeping at most `cap` escaped bytes.
    pub fn new(cap: usize) -> Self {
        BoundedWriter {
            out: String::with_capacity(cap.min(4096) / 8),
            cap,
            source_bytes: 0,
            saturated: false,
            refused: false,
            undercounted: false,
        }
    }

    /// The rendered text, with the truncation marker appended when the
    /// cap was reached.
    pub fn finish(mut self) -> String {
        if self.saturated {
            use std::fmt::Write as _;
            let floor = if self.undercounted { "≥" } else { "" };
            let _ = write!(
                self.out,
                "…[truncated; {floor}{} source bytes]",
                self.source_bytes
            );
        }
        self.out
    }
}

impl std::fmt::Write for BoundedWriter {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        if self.refused {
            self.undercounted = true;
            return Err(std::fmt::Error);
        }
        self.source_bytes += text.len();
        if !self.saturated {
            for character in text.chars() {
                push_escaped(&mut self.out, character);
                if self.out.len() >= self.cap {
                    self.saturated = true;
                    break;
                }
            }
        }
        if self.source_bytes > self.cap * 2 {
            self.refused = true;
        }
        Ok(())
    }
}

/// Render a value for display THROUGH the bounded sink rather than
/// materialized first: a cause whose rendering amplifies (an arrow
/// error carrying a whole `Field`, metadata map included) never exists
/// as a full string — only its capped, escaped prefix does.
pub fn render_bounded(cap: usize, value: &dyn std::fmt::Display) -> String {
    use std::fmt::Write as _;
    let mut sink = BoundedWriter::new(cap);
    let _ = write!(sink, "{value}");
    sink.finish()
}

/// The `Debug` twin of [`render_bounded`]: a wrong-variant wire reply
/// can carry a workload-sized bytes field whose derived Debug is a
/// per-byte decimal list ~4-5× the payload — rendered through the
/// sink, that rendering is counted but never materialized past the cap.
pub fn render_bounded_debug(cap: usize, value: &dyn std::fmt::Debug) -> String {
    use std::fmt::Write as _;
    let mut sink = BoundedWriter::new(cap);
    let _ = write!(sink, "{value:?}");
    sink.finish()
}

// ---- the Arrow IPC seats ----------------------------------------------------

/// One Arrow batch as an IPC *stream* (not the `File` container — no
/// footer, a schema message followed by one record-batch message,
/// exactly what a single-batch frame needs). THE encoder seat for both
/// directions of the wire: the serve side encoding a source's push,
/// the client encoding a `Write` frame's batch. Writing into an
/// in-memory `Vec` fails only on a schema/batch mismatch the caller
/// itself produced; even that cause is rendered through the bounded
/// sink, because a batch forwarded from a connector carries that
/// connector's schema metadata and arrow's error text can render a
/// whole `Field`.
pub fn encode_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let render = |error: ArrowError| render_bounded(PANIC_TEXT_CAP, &error);
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), batch.schema_ref())
        .map_err(|error| format!("opening an arrow ipc stream writer: {}", render(error)))?;
    writer
        .write(batch)
        .map_err(|error| format!("writing an arrow ipc record batch: {}", render(error)))?;
    writer
        .into_inner()
        .map_err(|error| format!("closing an arrow ipc stream writer: {}", render(error)))
}

/// One `arrow_ipc` stream's exactly-one record batch — THE decode seat
/// for both directions of the wire: the client decoding a read frame's
/// payload (adversary: the connector), the serve side decoding a
/// `Write` frame's (adversary: the client). Every defense runs in one
/// fixed order, whichever direction the bytes travel:
///
/// 1. the panic belt — arrow's IPC reader PANICS on some crafted
///    frames instead of returning `Err` (the schema converter aborts
///    on e.g. an Int field declaring a negative bit width; found by
///    the `arrow_ipc_decode` fuzz target), so the whole decode runs
///    under `catch_unwind` and this seat owns its failure as the typed
///    refusal. What the belt cannot suppress: the process panic HOOK
///    still writes its line to stderr before the unwind is caught — a
///    library must not replace the global hook;
/// 2. the framing pre-pass — declared metadata/body lengths are held
///    against the frame's real bytes ([`refuse_overdeclared_framing`])
///    BEFORE arrow's reader sees a byte, closing the commit-and-memset
///    and aborting-allocation arms no belt contains;
/// 3. the schema WIDTH cap, judged on the schema message before any
///    batch is pulled — by the time a `RecordBatch` exists arrow has
///    built every `Field` AND every array, which is the allocation the
///    cap exists to prevent;
/// 4. the ROW cap on the first batch — the memory dimension the
///    framing pre-pass cannot see, since Null and run-end-encoded
///    columns carry millions of rows in almost no body bytes;
/// 5. the ONE-BATCH rule — a second decodable batch refuses with
///    `multi_batch_refusal` rather than silently taking the first,
///    because on the write direction of this wire that same silence
///    was measured as row loss;
/// 6. every field name the decoded batch carries — nested containers
///    and dictionary value types included ([`refuse_record_batch_field_names`])
///    — through `refuse_field_name`, each side's own identifier
///    discipline (the client's content gate; the serve side's
///    length-only rule).
///
/// `refusal` is the seat's frozen prefix, prepended to every
/// Err-shaped arm's cause; `multi_batch_refusal` is the frozen
/// second-batch spelling, rendered bare (no cause exists for it).
/// Causes render through the bounded sink — never raw, never
/// materialized whole. `fatal` lifts the rendered refusal into the
/// caller's error type.
pub fn decode_one_batch_ipc<E>(
    bytes: &[u8],
    refusal: &str,
    multi_batch_refusal: &str,
    fatal: impl Fn(String) -> E,
    refuse_field_name: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<RecordBatch, E> {
    // The belt wraps the whole erring half in `AssertUnwindSafe`: the
    // closure captures only `bytes` (a shared slice) and the shared
    // refusal strings by reference, plus the field-name predicate —
    // none of which a mid-decode unwind can leave broken behind, and
    // no mutable state escapes the closure.
    match contain_panics(
        || decode_one_batch_ipc_erring(bytes, refusal, multi_batch_refusal, refuse_field_name),
        refusal,
    ) {
        Ok(decoded) => Ok(decoded),
        Err(refusal_message) => Err(fatal(refusal_message)),
    }
}

/// The seat's panic belt: `catch_unwind` contains PANICS only — the
/// declared-length ABORT class is closed by the framing pre-pass, the
/// two defenses are disjoint. A payload is attacker-adjacent text; the
/// shared rendering bounds it. What the belt cannot suppress: the
/// process panic HOOK still writes its line to stderr before the
/// unwind is caught — a library must not replace the global hook.
fn contain_panics<T>(work: impl FnOnce() -> Result<T, String>, refusal: &str) -> Result<T, String> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
        Ok(decoded) => decoded,
        Err(payload) => Err(format!(
            "{refusal}: the Arrow decoder panicked: {}",
            panic_text(payload.as_ref())
        )),
    }
}

/// The `Err`-shaped half of the decode: every failure arrow REPORTS
/// (as opposed to panics on) maps behind the frozen prefix here.
fn decode_one_batch_ipc_erring(
    bytes: &[u8],
    refusal: &str,
    multi_batch_refusal: &str,
    refuse_field_name: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<RecordBatch, String> {
    refuse_overdeclared_framing(bytes).map_err(|reason| format!("{refusal}: {reason}"))?;
    let mut reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| format!("{refusal}: {}", render_bounded(PANIC_TEXT_CAP, &error)))?;
    // The schema is judged BEFORE the first batch is decoded — its
    // width at every depth, its names' uniqueness at every level, and
    // every name through the caller's gate — so a schema the batch
    // could never honestly fill is refused before arrow allocates an
    // array per field for it.
    gate_schema_fields(reader.schema().fields(), refuse_field_name)
        .map_err(|reason| format!("{refusal}: {reason}"))?;
    let first = match reader.next() {
        Some(Ok(batch)) => batch,
        Some(Err(error)) => {
            return Err(format!(
                "{refusal}: {}",
                render_bounded(PANIC_TEXT_CAP, &error)
            ));
        }
        None => return Err(refusal.to_string()),
    };
    if first.num_rows() > crate::channel::MAX_RECORD_BATCH_ROWS {
        return Err(format!(
            "{refusal}: the batch carries {} rows, over the {}-row wire cap — \
             row count is bounded separately from encoded bytes",
            first.num_rows(),
            crate::channel::MAX_RECORD_BATCH_ROWS
        ));
    }
    match reader.next() {
        None => Ok(first),
        Some(Ok(_)) => Err(multi_batch_refusal.to_string()),
        Some(Err(error)) => Err(format!(
            "{refusal}: {}",
            render_bounded(PANIC_TEXT_CAP, &error)
        )),
    }
}

/// Validate every field name a decoded record batch carries, nested
/// container fields included, through `refuse_name` — the batch's
/// schema through [`gate_schema_fields`].
pub fn refuse_record_batch_field_names(
    batch: &RecordBatch,
    refuse_name: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<(), String> {
    gate_schema_fields(batch.schema().fields(), refuse_name)
}

/// Judge an Arrow schema as ONE tree: every named field at every depth
/// spends the same [`MAX_SOURCE_COLUMNS_PER_TABLE`] budget (a struct of
/// four thousand children is as wide as four thousand columns — each
/// is an array the decoder allocates and every downstream seat
/// null-fills, joins and casts), names are unique among their
/// siblings at every level (the engine joins and projects struct
/// fields BY NAME, and a duplicate would silently route one child's
/// values into another's column or drop them), and each name passes
/// `refuse_name`. The walk is iterative so an attacker-controlled
/// schema cannot add a second recursive traversal after Arrow's own
/// verified decoder; a dictionary encodes another type without a field
/// of its own, but its VALUE type can carry named fields (a struct, a
/// list, another dictionary) — unwrapped before matching, or those
/// inner names bypass the gate. The unwrap loop is bounded by the
/// schema's finite type depth, and a dictionary's key type is always a
/// bare integer carrying no fields.
pub fn gate_schema_fields(
    fields: &arrow_schema::Fields,
    refuse_name: &mut dyn FnMut(&str) -> Result<(), String>,
) -> Result<(), String> {
    use arrow_schema::DataType;

    let mut counted = 0usize;
    // Each entry is one set of SIBLINGS — the fields whose names must
    // not collide with each other.
    let mut pending: Vec<Vec<&arrow_schema::FieldRef>> = vec![fields.iter().collect()];
    while let Some(siblings) = pending.pop() {
        let mut seen = std::collections::BTreeSet::new();
        for field in siblings {
            counted += 1;
            if counted > MAX_SOURCE_COLUMNS_PER_TABLE {
                return Err(format!(
                    "the batch declares more than {MAX_SOURCE_COLUMNS_PER_TABLE} fields once \
                     nested fields are counted, over the column wire cap — width is \
                     bounded separately from encoded bytes, at every depth"
                ));
            }
            refuse_name(field.name())?;
            if !seen.insert(field.name().as_str()) {
                return Err(format!(
                    "the batch declares the field name `{}` twice among one set of \
                     siblings — fields are joined and projected by name, so a duplicate \
                     would route one child's values into another's column",
                    escape(field.name())
                ));
            }
            let mut data_type = field.data_type();
            while let DataType::Dictionary(_, value) = data_type {
                data_type = value;
            }
            match data_type {
                DataType::Struct(children) => pending.push(children.iter().collect()),
                DataType::List(child)
                | DataType::LargeList(child)
                | DataType::ListView(child)
                | DataType::LargeListView(child)
                | DataType::FixedSizeList(child, _)
                | DataType::Map(child, _) => pending.push(vec![child]),
                DataType::Union(union, _) => {
                    pending.push(union.iter().map(|(_, child)| child).collect());
                }
                DataType::RunEndEncoded(run_ends, values) => {
                    pending.push(vec![run_ends, values]);
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// The most bytes of a panic payload any decode belt will render:
/// arrow's own panics are static strings, but a payload is arbitrary
/// attacker-adjacent text, and an evidence line is not a firehose —
/// the certifier's violations, the client's refusal messages, and the
/// serve side's typed decode refusals all end up in reports and logs.
pub const PANIC_TEXT_CAP: usize = 4096;

/// A panic payload's message, bounded for rendering beside a decode
/// refusal: `&str` (the `panic!` literal form) and `String` (the
/// formatted form) render truncated to [`PANIC_TEXT_CAP`] bytes at a
/// char boundary; anything else renders as the honest placeholder.
///
/// One implementation for every `catch_unwind` belt around Arrow
/// decode — the module that owns the decode-seat discipline owns the
/// panic-rendering discipline, so the belts cannot drift.
pub fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    let text: &str = if let Some(text) = payload.downcast_ref::<&str>() {
        text
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text
    } else {
        return "<non-text panic payload>".to_string();
    };
    if text.len() <= PANIC_TEXT_CAP {
        return text.to_string();
    }
    let mut cut = PANIC_TEXT_CAP;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    format!(
        "{cut}-byte prefix of a {}-byte payload: {}",
        text.len(),
        &text[..cut]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cursor node ceiling is inclusive at its boundary, a broad
    /// array spends it like a deep object, and the walk is the one
    /// every cursor-parsing seat shares.
    #[test]
    fn the_cursor_node_ceiling_is_inclusive_at_the_boundary() {
        let at = serde_json::Value::Array(vec![serde_json::Value::Null; MAX_CURSOR_NODES - 1]);
        refuse_dense_cursor("a cursor", &at).expect("the array and its items are the ceiling");
        let over = serde_json::Value::Array(vec![serde_json::Value::Null; MAX_CURSOR_NODES]);
        let refusal = refuse_dense_cursor("a cursor", &over).expect_err("one node over");
        assert!(refusal.contains(&MAX_CURSOR_NODES.to_string()), "{refusal}");
    }

    /// The document ceiling, inclusive at its boundary — the client's
    /// boundary pin rides the same function through delegation.
    #[test]
    fn the_document_ceiling_is_inclusive_at_the_boundary() {
        refuse_oversized_document("state_doc_json", &[b'x'; MAX_DOCUMENT_BYTES as usize])
            .expect("a document at the cap passes");
        let error =
            refuse_oversized_document("state_doc_json", &[b'x'; MAX_DOCUMENT_BYTES as usize + 1])
                .expect_err("one byte over refuses");
        assert!(
            error.contains("document ceiling"),
            "the refusal names the ceiling: {error}"
        );
    }

    /// Control bytes render as their spelled-out escapes (never raw),
    /// the render truncates at the cap with a marker naming the true
    /// length, and ordinary text passes through untouched.
    #[test]
    fn the_diagnostic_render_escapes_and_caps() {
        assert_eq!(render_diagnostic("load-7", 256), "load-7");
        let hostile = render_diagnostic("evil\u{1b}]52;c;A\u{7}id", 256);
        assert!(
            !hostile.contains('\u{1b}') && !hostile.contains('\u{7}'),
            "no raw control bytes: {hostile:?}"
        );
        assert!(
            hostile.contains("u{1b}]52;c;A"),
            "the escape spells the byte out: {hostile}"
        );
        let long = render_diagnostic(&"x".repeat(10_000), 64);
        assert!(
            long.len() < 200 && long.contains("truncated from 10000 bytes"),
            "the cap bounds the render and names the true length: {long}"
        );
    }

    /// The rendering carries kind and position, and NOT the token: the
    /// document's own bytes (here, a would-be secret value) must not
    /// appear anywhere in the refusal.
    #[test]
    fn the_rendering_names_kind_and_location_but_never_the_value() {
        let error = serde_json::from_str::<std::collections::BTreeMap<String, String>>(
            r#"{"password": "hunter2",}"#,
        )
        .expect_err("trailing comma");
        let rendered = describe_parse_error(&error);
        assert_eq!(rendered, "syntax error at line 1 column 24");
        assert!(!rendered.contains("hunter2"), "no token echo: {rendered}");
    }

    /// The two declaration arms at their exact spellings — seats append
    /// these reasons to their own frozen prefixes, so the wording is
    /// the contract.
    #[test]
    fn overdeclared_lengths_refuse_with_the_shared_spellings() {
        let mut frame = vec![0xff, 0xff, 0xff, 0xff];
        frame.extend_from_slice(&0x7fff_fff0_i32.to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);
        assert_eq!(
            refuse_overdeclared_framing(&frame).expect_err("meta overdeclare refuses"),
            "a declared metadata length of 2147483632 bytes exceeds the 24-byte frame"
        );

        // A negative metadata length word.
        let mut frame = vec![0xff, 0xff, 0xff, 0xff];
        frame.extend_from_slice(&(-1_i32).to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);
        assert_eq!(
            refuse_overdeclared_framing(&frame).expect_err("negative meta refuses"),
            "a negative declared metadata length (-1)"
        );
    }

    /// The belt's rendering is bounded — a text payload renders in
    /// full under the cap, truncates at a char boundary over it, and a
    /// non-text payload renders the honest placeholder.
    #[test]
    fn panic_text_is_bounded_and_char_safe() {
        assert_eq!(panic_text(&"crafted metadata"), "crafted metadata");
        assert_eq!(
            panic_text(&String::from("formatted panic")),
            "formatted panic"
        );
        assert_eq!(panic_text(&7usize), "<non-text panic payload>");
        // A multi-byte char straddling the cut must not split: an ASCII
        // lead byte shifts the cap into the middle of an `é`. (The owned
        // `String` payload exercises the same truncation path as `&str`.)
        let long = format!("x{}", "é".repeat(PANIC_TEXT_CAP));
        let rendered = panic_text(&long);
        assert!(rendered.starts_with("4095-byte prefix"), "{rendered}");
        assert!(
            rendered.ends_with('é'),
            "the cut is char-safe: …{rendered:?}"
        );
        assert!(rendered.len() < long.len() + 64, "bounded, not doubled");
    }

    /// The walk's honest-pass property: a real one-batch stream, an
    /// end-of-stream marker, and a schema-only stream all walk clean.
    #[test]
    fn honest_streams_walk_clean() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .expect("batch");
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), batch.schema_ref())
            .expect("writer");
        writer.write(&batch).expect("write");
        let bytes = writer.into_inner().expect("finish");
        refuse_overdeclared_framing(&bytes).expect("an honest one-batch stream walks clean");
    }

    // ---- the bounded render -------------------------------------------------

    /// Escaping spells control bytes out and touches nothing else —
    /// quotes and backslashes included, which `escape_debug` over the
    /// WHOLE string would mangle.
    #[test]
    fn escaping_spells_control_bytes_and_leaves_data_alone() {
        assert_eq!(
            escape("\u{1b}]52;\u{7}\nx \"quoted\" \\slash é"),
            "\\u{1b}]52;\\u{7}\\nx \"quoted\" \\slash é"
        );
        assert!(matches!(
            escape("clean \"text\""),
            Cow::Borrowed("clean \"text\"")
        ));
    }

    /// The inventory's two Hangul fillers are category `Lo` — printable
    /// to `escape_debug`, which hands them back unchanged while they
    /// render as blank glyphs. The display seat's invariant is that
    /// rendered text cannot hide an inventory character, so the
    /// fallback spells them out.
    #[test]
    fn the_hangul_fillers_escaped_raw_get_spelled_out() {
        assert_eq!(escape("a\u{3164}b"), "a\\u{3164}b");
        assert_eq!(escape("a\u{ffa0}b"), "a\\u{ffa0}b");
    }

    /// The invariant at the refusal seats too: every inventory
    /// character renders spelled-out through the shared escape, so a
    /// refusal quoting a hostile value cannot carry the bytes it
    /// refuses. (Mechanical sweep across the whole table, not a
    /// sample.)
    #[test]
    fn no_inventory_character_survives_the_escape_raw() {
        let table: Vec<char> = ('\u{0}'..='\u{10FFFF}')
            .filter(|c| inventory::is_control_or_invisible(*c))
            .collect();
        assert!(table.len() > 100, "the sweep covers the inventory");
        for character in table {
            let input = character.to_string();
            let escaped = escape(&input);
            assert_ne!(
                escaped, input,
                "U+{:04X} must not survive the escape raw",
                character as u32
            );
        }
    }

    const RENDER_CAP: usize = 4096;

    /// The sink neither materializes NOR STREAMS what it discards: a
    /// chunked `Display` source that would write ten times the source
    /// ceiling is refused within one chunk of it — the write budget
    /// spent is ceiling/chunk + 2 attempts, not the source's length —
    /// and the marker spells the count as a floor (`≥`), because bytes
    /// were left uncounted. The kept text still stops at the cap.
    #[test]
    fn a_64mib_display_source_never_grows_the_sink_past_the_cap() {
        use std::cell::Cell;
        struct Firehose {
            attempts: Cell<usize>,
        }
        impl std::fmt::Display for Firehose {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let chunk = "x".repeat(1024);
                // Ten times the source ceiling, in 1 KiB chunks; `?`
                // is how every real Display reacts to a refused write.
                for _ in 0..(10 * 2 * RENDER_CAP / 1024) {
                    self.attempts.set(self.attempts.get() + 1);
                    f.write_str(&chunk)?;
                }
                Ok(())
            }
        }
        use std::fmt::Write as _;
        let source = Firehose {
            attempts: Cell::new(0),
        };
        let mut sink = BoundedWriter::new(RENDER_CAP);
        write!(sink, "{source}").expect_err("the sink refuses the runaway source");
        let ceiling = 2 * RENDER_CAP;
        assert!(
            source.attempts.get() <= ceiling / 1024 + 2,
            "the source is stopped within the ceiling's write budget, \
             not streamed to completion: {} attempts",
            source.attempts.get()
        );
        assert!(
            sink.source_bytes <= ceiling + 1024,
            "the count stops within one chunk of the ceiling: {} bytes",
            sink.source_bytes
        );
        assert!(
            sink.out.len() <= RENDER_CAP + 4,
            "the kept text stops at the cap: {} bytes",
            sink.out.len()
        );
        let counted = sink.source_bytes;
        let rendered = sink.finish();
        assert!(
            rendered.len() <= RENDER_CAP + 64,
            "the finished render is cap plus envelope: {} bytes",
            rendered.len()
        );
        assert!(
            rendered.ends_with(&format!("…[truncated; ≥{counted} source bytes]")),
            "the marker spells the count as a floor: …{}",
            &rendered[rendered.len() - 60..]
        );
    }

    /// The ceiling cuts only sources with more to say AFTER it: a
    /// source arriving as ONE write keeps its exact count whatever its
    /// size, so the single-write marker contract is unchanged at any
    /// scale.
    #[test]
    fn a_single_write_source_keeps_its_exact_count_at_any_size() {
        use std::fmt::Write as _;
        let mut sink = BoundedWriter::new(RENDER_CAP);
        sink.write_str(&"x".repeat(1 << 20))
            .expect("the crossing write itself is accepted");
        let rendered = sink.finish();
        assert!(
            rendered.ends_with(&format!("…[truncated; {} source bytes]", 1 << 20)),
            "one write, exact count, no floor marker: …{}",
            &rendered[rendered.len() - 60..]
        );
    }

    /// The kept prefix is escaped AS IT ARRIVES — a control byte in the
    /// window is spelled out, and the cap bounds the ESCAPED output, so
    /// no post-hoc escape pass can amplify past it.
    #[test]
    fn the_sink_escapes_the_kept_prefix_and_caps_the_escaped_form() {
        use std::fmt::Write as _;
        let mut sink = BoundedWriter::new(64);
        write!(sink, "a\u{1b}b{}", "\u{7}".repeat(1000)).expect("the sink never errors");
        let rendered = sink.finish();
        assert!(
            rendered.starts_with("a\\u{1b}b"),
            "the kept prefix is escaped: {rendered:?}"
        );
        assert!(
            !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
            "no raw control byte survives: {rendered:?}"
        );
        assert!(
            rendered.len() <= 64 + 10 + 40,
            "the cap bounds the escaped output plus one char of overshoot \
             and the marker: {} bytes",
            rendered.len()
        );
    }

    /// An honest source renders whole through both value renderers.
    #[test]
    fn an_honest_source_renders_whole_through_the_value_renderers() {
        assert_eq!(
            render_bounded(RENDER_CAP, &"an honest cause"),
            "an honest cause"
        );
        assert_eq!(render_bounded_debug(2048, &"quoted"), "\"quoted\"");
    }

    // ---- the shared Arrow seats ---------------------------------------------

    /// A batch through the encode seat decodes back through the decode
    /// seat — the two seats agree on the stream shape they produce and
    /// consume.
    #[test]
    fn the_encode_seat_feeds_the_decode_seat() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let bytes = encode_batch_ipc(&batch).expect("an honest batch encodes");
        let decoded = decode_one_batch_ipc(
            &bytes,
            "test refusal",
            "test multi",
            |message| message,
            &mut |_| Ok(()),
        )
        .expect("the encoded stream decodes");
        assert_eq!(decoded.num_rows(), 3);
    }

    /// Zero batches (a schema-only stream) refuse with the bare
    /// refusal; a second decodable batch refuses with the bare
    /// multi-batch spelling — no cause exists for either, and the seat
    /// invents none.
    #[test]
    fn zero_and_second_batch_refuse_with_their_bare_spellings() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = |values: Vec<i64>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(values))],
            )
            .expect("batch")
        };
        let schema_only = {
            let mut writer =
                arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema).expect("writer");
            writer.finish().expect("finish");
            writer.into_inner().expect("inner")
        };
        let error = decode_one_batch_ipc(
            &schema_only,
            "REFUSAL",
            "MULTI",
            |message| message,
            &mut |_| Ok(()),
        )
        .expect_err("zero batches must refuse");
        assert_eq!(error, "REFUSAL");

        // One stream carrying TWO record-batch messages: the reader
        // sees a second decodable batch behind the first.
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema).expect("writer");
        writer.write(&batch(vec![1])).expect("first");
        writer.write(&batch(vec![2])).expect("second");
        let bytes = writer.into_inner().expect("finish");
        let error =
            decode_one_batch_ipc(&bytes, "REFUSAL", "MULTI", |message| message, &mut |_| {
                Ok(())
            })
            .expect_err("a second batch must refuse");
        assert_eq!(error, "MULTI");
    }

    /// The row cap is the memory dimension the framing pre-pass cannot
    /// see: one row over refuses behind the refusal prefix, a batch at
    /// exactly the cap decodes.
    #[test]
    fn the_row_cap_refuses_one_row_over_and_admits_the_cap() {
        use arrow_array::BooleanArray;
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
        let batch = |rows: usize| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(BooleanArray::from(vec![None::<bool>; rows]))],
            )
            .expect("batch")
        };
        let over = encode_batch_ipc(&batch(crate::channel::MAX_RECORD_BATCH_ROWS + 1))
            .expect("an over-row batch still ENCODES");
        let error =
            decode_one_batch_ipc(
                &over,
                "REFUSAL",
                "MULTI",
                |message| message,
                &mut |_| Ok(()),
            )
            .expect_err("one row over the cap refuses");
        assert!(
            error.contains("row wire cap"),
            "the refusal names the row cap: {error}"
        );
        let at_cap = encode_batch_ipc(&batch(crate::channel::MAX_RECORD_BATCH_ROWS))
            .expect("a batch at the cap encodes");
        decode_one_batch_ipc(&at_cap, "REFUSAL", "MULTI", |message| message, &mut |_| {
            Ok(())
        })
        .expect("a batch at exactly the row cap decodes");
    }

    /// The width cap is judged on the SCHEMA, before any batch is
    /// pulled: one column over the shared per-table width refuses
    /// behind the refusal prefix.
    #[test]
    fn the_width_cap_refuses_a_schema_over_the_per_table_width() {
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let fields: Vec<_> = (0..MAX_SOURCE_COLUMNS_PER_TABLE + 1)
            .map(|i| Field::new(format!("f{i}"), DataType::Int64, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema)
            .expect("a wide schema still encodes");
        writer.finish().expect("finish");
        let bytes = writer.into_inner().expect("inner");
        let error =
            decode_one_batch_ipc(&bytes, "REFUSAL", "MULTI", |message| message, &mut |_| {
                Ok(())
            })
            .expect_err("a schema over the width cap refuses");
        assert!(
            error.contains("column wire cap"),
            "the refusal names the width cap: {error}"
        );
    }

    /// The width cap is spent by the WHOLE schema: one top-level struct
    /// of 4,097 children refuses, and one of exactly 4,095 (the struct
    /// itself making 4,096) is admitted. The same walk judges a decode
    /// at the source-read and destination-write seats, since both are
    /// this function.
    #[test]
    fn the_width_cap_counts_nested_fields() {
        use arrow_schema::{DataType, Field, Fields, Schema};
        use std::sync::Arc;

        let nested = |children: usize| {
            let fields: Vec<Field> = (0..children)
                .map(|i| Field::new(format!("c{i}"), DataType::Int64, true))
                .collect();
            let schema = Arc::new(Schema::new(vec![Field::new(
                "s",
                DataType::Struct(Fields::from(fields)),
                true,
            )]));
            let mut writer =
                arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema).expect("encodes");
            writer.finish().expect("finish");
            writer.into_inner().expect("inner")
        };
        let error = decode_one_batch_ipc(
            &nested(MAX_SOURCE_COLUMNS_PER_TABLE),
            "REFUSAL",
            "MULTI",
            |message| message,
            &mut |_| Ok(()),
        )
        .expect_err("4,096 children under one struct is 4,097 fields");
        assert!(error.contains("once nested fields are counted"), "{error}");
        // At the cap (a schema-only stream refuses for having no batch,
        // which is the NEXT check — so the width passed).
        let error = decode_one_batch_ipc(
            &nested(MAX_SOURCE_COLUMNS_PER_TABLE - 1),
            "REFUSAL",
            "MULTI",
            |message| message,
            &mut |_| Ok(()),
        )
        .expect_err("no batch");
        assert_eq!(
            error, "REFUSAL",
            "the width passed; only the batch is missing"
        );
    }

    /// Duplicate names among siblings refuse at any depth — two
    /// top-level fields, or two children of one struct — while the
    /// same name under DIFFERENT parents is honest.
    #[test]
    fn duplicate_sibling_names_refuse_at_every_depth() {
        use arrow_schema::{DataType, Field, Fields};
        let gate = |fields: Vec<Field>| gate_schema_fields(&Fields::from(fields), &mut |_| Ok(()));
        let error = gate(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("a", DataType::Utf8, true),
        ])
        .expect_err("top-level twins");
        assert!(
            error.contains("`a` twice among one set of siblings"),
            "{error}"
        );
        let error = gate(vec![Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int64, true),
                Field::new("x", DataType::Int64, true),
            ])),
            true,
        )])
        .expect_err("nested twins");
        assert!(error.contains("`x` twice"), "{error}");
        gate(vec![
            Field::new(
                "s",
                DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
                true,
            ),
            Field::new(
                "t",
                DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
                true,
            ),
            Field::new("x", DataType::Int64, true),
        ])
        .expect("the same name under different parents is distinct");
    }

    /// The field-name walk reaches nested containers and dictionary
    /// value types: `Dictionary(Int32, Struct([...]))` is encodable by
    /// arrow's own writer, and without a Dictionary arm the walk never
    /// reaches the inner struct's names.
    #[test]
    fn the_field_walk_reaches_nested_and_dictionary_names() {
        use arrow_schema::DataType;
        use std::sync::Arc;

        let nested = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                "outer",
                DataType::Struct(
                    vec![arrow_schema::Field::new(
                        "inner\u{202e}",
                        DataType::Int64,
                        true,
                    )]
                    .into(),
                ),
                true,
            ),
        ])));
        let refused = refuse_record_batch_field_names(&nested, &mut |name| {
            if name.contains('\u{202e}') {
                Err(format!("hostile: {name}"))
            } else {
                Ok(())
            }
        })
        .expect_err("a nested hostile name is reached");
        assert!(refused.contains("hostile"), "{refused}");

        let dictionary = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                "outer",
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Struct(
                        vec![arrow_schema::Field::new(
                            "inner\u{202e}",
                            DataType::Int64,
                            true,
                        )]
                        .into(),
                    )),
                ),
                true,
            ),
        ])));
        let refused = refuse_record_batch_field_names(&dictionary, &mut |name| {
            if name.contains('\u{202e}') {
                Err(format!("hostile: {name}"))
            } else {
                Ok(())
            }
        })
        .expect_err("a dictionary's inner hostile name is reached");
        assert!(refused.contains("hostile"), "{refused}");
    }

    /// The fuzzer-found frame (the arrow_ipc_decode target's first
    /// catch): a 160-byte crafted stream declaring an Int field whose
    /// bit width is negative — arrow-ipc's schema converter PANICS on
    /// it inside `StreamReader::try_new` instead of returning `Err`.
    /// Today the framing pre-pass refuses these bytes FIRST (their
    /// double continuation marker reads as an overdeclared length),
    /// which is the defense working as layered; the pin is the TYPED
    /// refusal behind the frozen prefix — whichever layer answers,
    /// never an unwind escaping to the caller. Bytes embedded verbatim
    /// so the pin is hermetic.
    #[test]
    fn a_crafted_frame_that_panics_arrow_refuses_typed() {
        const REPRO: [u8; 160] = [
            0xff, 0xff, 0xff, 0xff, 0x78, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x0a, 0x00, 0x0c, 0x00, 0x06, 0x00, 0x05, 0x00, 0x08, 0x00, 0x0a, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x04, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x08, 0x00, 0x08, 0x00, 0x00, 0x00,
            0x04, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x14, 0x00, 0x00, 0x00, 0x10, 0x00, 0x14, 0x00, 0x08, 0x00, 0x06, 0x00, 0x07, 0x00,
            0x0c, 0x00, 0x00, 0x00, 0x10, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
            0x10, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x69, 0x64, 0x00, 0x00, 0x08, 0x00, 0x0c, 0x00,
            0x08, 0x00, 0x07, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x40, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x29, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
            0x88, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00,
            0x16, 0x00, 0x06, 0x00, 0x05, 0x00,
        ];

        let error =
            decode_one_batch_ipc(&REPRO, "REFUSAL", "MULTI", |message| message, &mut |_| {
                Ok(())
            })
            .expect_err("crafted bytes must refuse typed");
        assert!(
            error.starts_with("REFUSAL: ") && error.len() > "REFUSAL: ".len() + 2,
            "the refusal rides behind the frozen prefix with a cause: {error}"
        );
    }

    /// The belt pinned DIRECTLY (synthetic, because every live input
    /// that once reached it now refuses earlier at the pre-pass): an
    /// unwind inside the decode is classified as a typed refusal, never
    /// an escape.
    #[test]
    fn a_decoder_panic_is_contained_as_a_typed_refusal() {
        let error = contain_panics::<RecordBatch>(|| panic!("crafted metadata"), "REFUSAL")
            .expect_err("the unwind is contained");
        assert!(error.contains("Arrow decoder panicked"), "{error}");
        assert!(error.contains("crafted metadata"), "{error}");
    }

    /// A ~24-byte frame whose 4-byte length field declares ~2 GiB of
    /// metadata. arrow-ipc's stream reader `resize`s its metadata
    /// buffer to the DECLARED size — committing and zero-filling the
    /// full 2 GiB — before `read_exact` discovers the frame holds no
    /// such bytes. The framing pre-pass must refuse first: the refusal
    /// spelling is the pre-pass's own, which is the structural proof
    /// arrow's reader (and its allocation) was never entered.
    #[test]
    fn a_frame_declaring_a_huge_metadata_length_refuses_before_arrow_allocates() {
        let mut frame = vec![0xff, 0xff, 0xff, 0xff];
        frame.extend_from_slice(&0x7fff_fff0_i32.to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);

        let error =
            decode_one_batch_ipc(&frame, "REFUSAL", "MULTI", |message| message, &mut |_| {
                Ok(())
            })
            .expect_err("a declared length past the frame's end must refuse");
        assert_eq!(
            error,
            "REFUSAL: a declared metadata length of 2147483632 bytes exceeds \
             the 24-byte frame"
        );
    }

    /// The body-length sibling: a real one-batch stream whose
    /// record-batch message is patched to declare a ~2 GiB body — the
    /// allocation whose failure ABORTS, which no belt contains. The
    /// patch locates `bodyLength` structurally (root table offset →
    /// vtable → the field's slot), self-verified against the decoded
    /// value before patching.
    #[test]
    fn a_frame_declaring_a_huge_body_length_refuses_before_arrow_allocates() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .expect("batch");
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema).expect("writer");
        writer.write(&batch).expect("write");
        let mut bytes = writer.into_inner().expect("finish");
        decode_one_batch_ipc(&bytes, "REFUSAL", "MULTI", |message| message, &mut |_| {
            Ok(())
        })
        .expect("the unpatched stream decodes");

        // Walk to the second message (the record batch); patch its
        // declared bodyLength.
        let (meta_start, meta_end) = {
            let mut pos = 0usize;
            let mut spans = Vec::new();
            while spans.len() < 2 {
                assert_eq!(&bytes[pos..pos + 4], [0xff; 4], "continuation marker");
                let meta_len =
                    i32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().expect("4 bytes"))
                        as usize;
                let start = pos + 8;
                spans.push((start, start + meta_len));
                let body = usize::try_from(
                    arrow_ipc::root_as_message(&bytes[start..start + meta_len])
                        .expect("valid metadata")
                        .bodyLength(),
                )
                .expect("non-negative body");
                pos = start + meta_len + body;
            }
            spans[1]
        };
        let meta = &bytes[meta_start..meta_end];
        let root = u32::from_le_bytes(meta[0..4].try_into().expect("4 bytes")) as i64;
        let soffset = i32::from_le_bytes(
            meta[root as usize..root as usize + 4]
                .try_into()
                .expect("4 bytes"),
        ) as i64;
        let vtable = usize::try_from(root - soffset).expect("in-bounds vtable");
        let slot =
            u16::from_le_bytes(meta[vtable + 10..vtable + 12].try_into().expect("2 bytes")) as i64;
        assert_ne!(slot, 0, "bodyLength is present in the message");
        let field_pos = meta_start + usize::try_from(root + slot).expect("in-bounds field");
        bytes[field_pos..field_pos + 8].copy_from_slice(&0x7fff_fff0_i64.to_le_bytes());

        let frame_len = bytes.len();
        let error =
            decode_one_batch_ipc(&bytes, "REFUSAL", "MULTI", |message| message, &mut |_| {
                Ok(())
            })
            .expect_err("an overdeclared body refuses typed");
        assert_eq!(
            error,
            format!(
                "REFUSAL: a declared body length of 2147483632 bytes exceeds \
                 the {frame_len}-byte frame"
            )
        );
    }
}
