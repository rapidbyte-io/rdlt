//! Rendering serde_json parse failures without echoing the parsed bytes.
//!
//! serde's data errors embed the offending TOKEN (`invalid type: string
//! "…"`), and the token is wire text: a malformed multi-megabyte state
//! document would otherwise ride a fragment into a host log or a
//! certification report through the refusal that rejected it. The rule
//! this module owns is the one the sdk's handshake arm established
//! (GLM round-2, L8) and round-6 generalized to EVERY decode seat
//! (6L7): a parse refusal renders KIND and LOCATION — never the value.
//!
//! One implementation, shared by both sides of the wire: the sdk's
//! serve seats (whose adversary is a rogue client) and the client's
//! decode seats (whose adversary is a rogue connector) import the same
//! function so the two sides cannot drift.

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

#[cfg(test)]
mod tests {
    use super::*;

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
