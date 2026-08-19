//! The SPI's trust-boundary toolbox: the size ceilings and refusal
//! helpers every seat that handles connector-authored bytes installs
//! before acting on them.
//!
//! One implementation of each rule, shared by both sides of the wire:
//! the sdk's serve seats (whose adversary is a rogue client) and the
//! client's decode seats (whose adversary is a rogue connector) import
//! the same functions, so the two sides cannot drift. The engine's
//! in-process validation imports the same ceilings, so a declaration
//! that would be refused on the wire is refused in-process too.

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
}
