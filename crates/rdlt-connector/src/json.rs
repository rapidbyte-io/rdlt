//! Rendering serde_json parse failures without echoing the parsed bytes,
//! and the document ceiling every untyped parse runs before it parses.
//!
//! serde's data errors embed the offending TOKEN (`invalid type: string
//! "…"`), and the token is wire text: a malformed multi-megabyte state
//! document would otherwise ride a fragment into a host log or a
//! certification report through the refusal that rejected it. The rule
//! this module owns is the one the sdk's handshake arm established
//! (GLM round-2, L8) and round-6 generalized to EVERY decode seat
//! (6L7): a parse refusal renders KIND and LOCATION — never the value.
//!
//! The ceiling is the same discipline one layer up (GLM round-7, 7M2):
//! an untyped `serde_json::Value` materializes at many times its wire
//! size, so every seat that parses one — client or serve, whichever
//! side of the wire the adversary sits on — measures the RAW bytes
//! against [`crate::MAX_DOCUMENT_BYTES`] first, bounding the parse's
//! own materialization rather than cleaning up after it.
//!
//! One implementation of each, shared by both sides of the wire: the
//! sdk's serve seats (whose adversary is a rogue client) and the
//! client's decode seats (whose adversary is a rogue connector) import
//! the same functions so the two sides cannot drift.

/// Render a JSON parse failure as kind and position only: which class
/// of failure, at which line and column. Nothing derived from the
/// document's content appears in the output.
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
/// [`crate::MAX_DOCUMENT_BYTES`] — the gate every untyped `Value` (or
/// typed shell around one) runs BEFORE parsing. The message carries
/// byte counts and the ceiling only; nothing derived from the
/// document's content.
pub fn refuse_oversized_document(field: &str, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() as u64 > crate::MAX_DOCUMENT_BYTES {
        return Err(format!(
            "an inbound {field} of {} bytes exceeds the {}-byte document ceiling — a \
             hand-authored or summarized document measures in kilobytes",
            bytes.len(),
            crate::MAX_DOCUMENT_BYTES
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The document ceiling, inclusive at its boundary (7M2's helper
    /// pinned at its own seat — the client's boundary pin rides the
    /// same function through delegation).
    #[test]
    fn the_document_ceiling_is_inclusive_at_the_boundary() {
        refuse_oversized_document(
            "state_doc_json",
            &[b'x'; crate::MAX_DOCUMENT_BYTES as usize],
        )
        .expect("a document at the cap passes");
        let error = refuse_oversized_document(
            "state_doc_json",
            &[b'x'; crate::MAX_DOCUMENT_BYTES as usize + 1],
        )
        .expect_err("one byte over refuses");
        assert!(
            error.contains("document ceiling"),
            "the refusal names the ceiling: {error}"
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
}
