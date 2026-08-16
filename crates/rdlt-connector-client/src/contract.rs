//! The document and cursor ceilings, enforced at the trust boundary
//! (GLM round 6, 6M1/6M2/6L1): every wire seat that decodes connector
//! authored JSON into an untyped `serde_json::Value` — or into a typed
//! shell that CONTAINS one — measures the bytes it is about to parse
//! against the SPI's constants first, and every cursor the host comes
//! to hold is measured on its RE-SERIALIZED form, because that is the
//! form the WAL records and the pre-send gate forwards.
//!
//! The raw-bytes gates bound the PARSE (a compact 64 MiB frame would
//! otherwise materialize several hundred MB of `Value` before any
//! semantic check could refuse); the serialized-cursor gate bounds the
//! PERSISTED form, where serde_json's own number rendering can inflate
//! a wire-legal cursor past its contract (`1e15` re-serializes as
//! `1000000000000000.0` — ryu's pretty notation, ~3.3× with
//! separators), which would otherwise crash-loop a resume against the
//! WAL line cap that receives the inflated spelling.

/// Serialize `cursor` and refuse typed when its SERIALIZED form exceeds
/// [`rdlt_connector::MAX_CURSOR_BYTES`] — the bound every persisted
/// cursor must honor (the WAL line cap is sized to carry one maximal
/// cursor line plus its envelope). Returns the serialized bytes so the
/// pre-send seat forwards exactly what was measured.
pub(crate) fn cursor_within_contract(cursor: &serde_json::Value) -> Result<Vec<u8>, String> {
    let bytes =
        serde_json::to_vec(cursor).expect("a serde_json::Value serializes to JSON infallibly");
    if bytes.len() as u64 > rdlt_connector::MAX_CURSOR_BYTES {
        return Err(format!(
            "a cursor serializes to {} bytes, over the {}-byte cursor contract — the connector \
             must summarize its state (a high-water mark, an offset, a resume token) rather \
             than embed the data",
            bytes.len(),
            rdlt_connector::MAX_CURSOR_BYTES
        ));
    }
    Ok(bytes)
}

/// The document ceiling, delegated to the SPI's ONE implementation
/// (7M2's hoist): every crate that parses an untyped wire document
/// imports the same gate, so the client's seats and the serve's cannot
/// drift.
pub(crate) fn refuse_oversized_document(field: &str, bytes: &[u8]) -> Result<(), String> {
    rdlt_connector::json::refuse_oversized_document(field, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 6.5: the cursor contract is inclusive at its boundary — a
    /// cursor serializing to EXACTLY the ceiling passes; one byte over
    /// refuses naming the contract.
    #[test]
    fn the_cursor_ceiling_is_inclusive_at_the_boundary() {
        let at_cap = serde_json::Value::String("c".repeat(
            rdlt_connector::MAX_CURSOR_BYTES as usize - 2, // quotes round it to exactly the cap
        ));
        let bytes = cursor_within_contract(&at_cap).expect("a cursor at the cap passes");
        assert_eq!(bytes.len() as u64, rdlt_connector::MAX_CURSOR_BYTES);
        let over =
            serde_json::Value::String("c".repeat(rdlt_connector::MAX_CURSOR_BYTES as usize - 1));
        let error = cursor_within_contract(&over).expect_err("one byte over refuses");
        assert!(
            error.contains("cursor contract"),
            "the refusal names the contract: {error}"
        );
    }

    /// 6L1: the contract measures the RE-SERIALIZED form, because that
    /// is the form the WAL records — serde_json's own number rendering
    /// inflates a compact wire cursor (`1e15` re-serializes as
    /// `1000000000000000.0`), so a wire-legal document can still
    /// violate the contract its persistence must honor.
    #[test]
    fn the_cursor_gate_measures_the_re_serialized_form() {
        // A compact wire spelling whose serialized form is ~4.5× larger.
        let floats = format!("[{}]", vec!["1e15"; 300_000].join(","));
        let parsed: serde_json::Value =
            serde_json::from_str(&floats).expect("compact exponent notation parses");
        assert!(
            floats.len() < rdlt_connector::MAX_CURSOR_BYTES as usize,
            "the wire form is well under the ceiling: {}",
            floats.len()
        );
        let error =
            cursor_within_contract(&parsed).expect_err("the inflated serialization must refuse");
        assert!(
            error.contains("cursor contract"),
            "the refusal names the contract: {error}"
        );
        // The same numbers spelled the way serde re-serializes them sit
        // over the ceiling — the measurement is honest, not synthetic.
        assert!(format!("{parsed}").len() > rdlt_connector::MAX_CURSOR_BYTES as usize);
    }
}
