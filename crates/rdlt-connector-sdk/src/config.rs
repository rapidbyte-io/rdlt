//! The validated config-document seam, and the raw-text YAML gate that
//! guards its text entry.
//!
//! Every rdlt connector accepts its configuration as a document — YAML
//! text, JSON text, or an already-parsed `serde_json::Value` from a
//! platform holding configs as data — and every entry point must run
//! the SAME validation, or an invariant the runtime relies on holds for
//! one entry and silently not for another. [`Document`] makes that a
//! property of the trait: the provided methods parse, then validate,
//! and there is no other path through them.
//!
//! THE SEAM RENDERS NO TEXT. Error types, message prefixes, and every
//! refusal spelling stay in the connector — the associated
//! [`Document::Error`] only has to absorb the two parser errors, which
//! each connector's error type already does with its own wording.
//!
//! The YAML entry is also a security seat. `serde_yaml_ng` expands
//! anchors and aliases while constructing the target value, so an
//! input-byte cap cannot bound the resulting allocation: a few
//! megabytes of anchored content plus a stream of three-byte aliases
//! materializes quadratically, and the expansion happens even when the
//! document ultimately fails to deserialize. Configuration documents
//! are trees; [`reject_graph_syntax`] refuses graph syntax at the text
//! boundary, before the parser allocates anything. It is a SCANNER
//! because the pinned `serde_yaml_ng` keeps its event stream private,
//! so the gate must decide from raw text — and a naive character scan
//! is unsound (one apostrophe inside `pipeline: john's orders`, misread
//! as a quote-open, blinds every later check). The scanner instead
//! tracks the one thing that matters, WHERE A TOKEN CAN START: `&` and
//! `*` begin an anchor or alias only at a token-start position (after
//! `-`/`?`/`:` separators, flow indicators `, [ {`, a tag, a document
//! marker, or a line start) and are scalar data everywhere else; quote
//! and block-scalar indicators obey the same rule, which is what keeps
//! `john's` from opening a quote. It OVER-approximates — every position
//! the parser could start a token is a token-start here — and the few
//! spellings whose reading cannot be decided line-locally are refused
//! rather than guessed: quoted scalars spanning a line break; a quote,
//! tag, or block-scalar indicator at a token-start while a plain scalar
//! may still be continuing; verbatim `!<…>` tags. Every such refusal is
//! availability-only over legal-but-exotic YAML, the everyday
//! configuration vocabulary passes untouched, and anchors and aliases
//! can NEVER pass, because the token-start set here is a superset of
//! the parser's by construction.

use serde::de::DeserializeOwned;

/// A connector configuration document: parse from any accepted form,
/// then validate — by construction, at every entry point.
///
/// Implementers supply [`Document::validate`] (the ONE gate, holding the
/// crate's own invariants with the crate's own error type and message
/// spellings) and inherit the three entry points. Overriding one to skip
/// validation would recreate the defect class this trait exists to
/// close.
///
/// # Example
///
/// ```
/// use rdlt_connector_sdk::config::Document;
///
/// #[derive(Debug, serde::Deserialize)]
/// struct Config {
///     url: String,
/// }
///
/// #[derive(Debug, thiserror::Error)]
/// enum ConfigError {
///     #[error("invalid example YAML: {0}")]
///     Yaml(#[from] serde_yaml_ng::Error),
///     #[error("invalid example JSON: {0}")]
///     Json(#[from] serde_json::Error),
///     #[error("invalid example config: {0}")]
///     Invalid(String),
/// }
///
/// impl Document for Config {
///     type Error = ConfigError;
///     fn validate(&self) -> Result<(), Self::Error> {
///         if self.url.is_empty() {
///             return Err(ConfigError::Invalid("`url` must not be empty".into()));
///         }
///         Ok(())
///     }
/// }
///
/// // Every entry point runs the same gate:
/// assert!(Config::from_yaml("url: https://x").is_ok());
/// let refused = Config::from_yaml("url: \"\"").unwrap_err();
/// assert_eq!(
///     refused.to_string(),
///     "invalid example config: `url` must not be empty"
/// );
/// let refused = Config::from_json("{\"url\": \"\"}").unwrap_err();
/// assert!(refused.to_string().contains("must not be empty"));
/// ```
pub trait Document: DeserializeOwned + Sized {
    /// The connector's own configuration error. It absorbs the two
    /// parser errors (keeping the connector's wording via its own
    /// `From` impls) and carries the connector's refusals. `Display` is
    /// required because the serve layer renders a handshake's config
    /// refusal from a shell generic over the connector alone, with no
    /// connector-specific error type to match on.
    type Error: std::fmt::Display + From<serde_yaml_ng::Error> + From<serde_json::Error>;

    /// The ONE validation gate. Every invariant the connector's runtime
    /// relies on is checked here, once — the provided entry points all
    /// route through it, so nothing downstream re-checks or papers over.
    fn validate(&self) -> Result<(), Self::Error>;

    /// Parse from YAML text and validate.
    ///
    /// Two raw-text security gates run BEFORE the parser sees the text,
    /// because YAML deserialization allocates while it parses: documents
    /// over [`rdlt_connector::gate::MAX_DOCUMENT_BYTES`] are refused by
    /// size, and YAML anchors/aliases are refused outright by
    /// [`reject_graph_syntax`] (alias expansion materializes
    /// quadratically, so a byte cap alone cannot bound it). Both
    /// refusals arrive through the YAML arm of [`Document::Error`],
    /// rendered in the connector's own wording around the gate's
    /// one-line reason.
    fn from_yaml(yaml: &str) -> Result<Self, Self::Error> {
        if yaml.len() as u64 > rdlt_connector::gate::MAX_DOCUMENT_BYTES {
            let refusal = <serde_yaml_ng::Error as serde::de::Error>::custom(format!(
                "the document is {} bytes, over the {}-byte cap — connector configuration \
                 is hand-written, so a document this size is almost certainly not the \
                 configuration it was passed as",
                yaml.len(),
                rdlt_connector::gate::MAX_DOCUMENT_BYTES
            ));
            return Err(refusal.into());
        }
        if let Err(reason) = reject_graph_syntax(yaml) {
            let refusal = <serde_yaml_ng::Error as serde::de::Error>::custom(reason);
            return Err(refusal.into());
        }
        let document: Self = serde_yaml_ng::from_str(yaml)?;
        document.validate()?;
        Ok(document)
    }

    /// Parse from JSON text and validate — the same document shape and
    /// the same gate as YAML: the byte ceiling before the parser sees
    /// the text (JSON has no aliases to expand, so the ceiling is the
    /// whole text gate), refused through the JSON arm of
    /// [`Document::Error`].
    fn from_json(json: &str) -> Result<Self, Self::Error> {
        if json.len() as u64 > rdlt_connector::gate::MAX_DOCUMENT_BYTES {
            let refusal = <serde_json::Error as serde::de::Error>::custom(format!(
                "the document is {} bytes, over the {}-byte cap — connector configuration \
                 is hand-written, so a document this size is almost certainly not the \
                 configuration it was passed as",
                json.len(),
                rdlt_connector::gate::MAX_DOCUMENT_BYTES
            ));
            return Err(refusal.into());
        }
        let document: Self = serde_json::from_str(json)?;
        document.validate()?;
        Ok(document)
    }

    /// The embedder entry point: a platform holding connector configs as
    /// JSON documents passes the `serde_json::Value` directly — no
    /// string round-trip, same gate as every other entry.
    fn from_value(value: serde_json::Value) -> Result<Self, Self::Error> {
        let document: Self = serde_json::from_value(value)?;
        document.validate()?;
        Ok(document)
    }
}

/// The JSON Schema of a config document, GENERATED from its type — the
/// declared schema and the parser cannot drift, because they are the
/// same structs. Each connector keeps its own one-line public
/// `config_schema()` delegating here.
#[cfg(feature = "schema")]
pub fn schema_of<T: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("a generated schema serializes")
}

/// Refuse YAML graph syntax — and the few spellings whose reading
/// cannot be decided without a full parser — before deserialization.
///
/// `Ok(())` means the document contains no anchors and no aliases, so
/// `serde_yaml_ng` materialization is tree-bounded: the value it builds
/// can only be as large as the text that spells it. The error is one
/// rendered line naming the refused indicator and its byte offset;
/// callers absorb it into their own error vocabulary. Public because a
/// connector's own raw-YAML seat (a pipeline document, a sub-document
/// parsed by hand) answers to the same scanner as `from_yaml`.
pub fn reject_graph_syntax(text: &str) -> Result<(), String> {
    let mut mode = Mode::Scan;
    // A token may start at the current position. Superset of the
    // parser's token-start positions; `&`/`*` here are refused.
    let mut node_start = true;
    // A plain scalar begun earlier may still be open. `:` followed by
    // a blank always ends one; a close quote, a comment, or a document
    // marker does too; flow indicators do not (in block context they
    // are scalar data), so the flag survives them AND line breaks —
    // multi-line plain continuation is exactly the ambiguity it exists
    // to track.
    let mut run_open = false;
    // How many flow collections are open. Flow is where a `,` or an
    // opener starts a fresh node — in block context both are ordinary
    // scalar characters, and treating them alike is what made a
    // wildcard selector look like an alias.
    let mut flow_depth: usize = 0;
    // The immediately-previous character was a blank or a line start —
    // the precondition for `#` opening a comment.
    let mut prev_blank = true;
    // Only indent and `- `/`? ` prefixes seen on this line so far.
    let mut line_prefix_only = true;
    // Column of this line's first token past those prefixes: the
    // block-scalar parent column (content must be deeper; anything at
    // or left of it is structure and gets rescanned).
    let mut anchor_col: Option<usize> = None;
    let mut col: usize = 0;
    let mut idx: usize = 0;

    while idx < text.len() {
        let c = char_after(text, idx).expect("idx sits on a char boundary");
        let c_len = c.len_utf8();
        match mode {
            Mode::Scan => {
                if is_break(c) {
                    node_start = true;
                    prev_blank = true;
                    line_prefix_only = true;
                    anchor_col = None;
                    col = 0;
                    idx += c_len;
                    continue;
                }
                if is_blank(c) {
                    prev_blank = true;
                    col += 1;
                    idx += c_len;
                    continue;
                }
                // Document markers hold token-start so `--- &a` is
                // still seen as an anchor; they also end any plain
                // scalar continuing from the lines above.
                if col == 0
                    && (text[idx..].starts_with("---") || text[idx..].starts_with("..."))
                    && blankz_after(text, idx + 3)
                {
                    node_start = true;
                    run_open = false;
                    prev_blank = false;
                    col += 3;
                    idx += 3;
                    continue;
                }
                // A `%` directive line is inert — unless a plain scalar
                // may be continuing from above, in which case this
                // character is (or errors as) scalar data.
                if col == 0 && c == '%' && !run_open {
                    mode = Mode::Comment;
                    idx += c_len;
                    continue;
                }
                match c {
                    '&' | '*' if node_start => {
                        return Err(format!(
                            "YAML anchors and aliases are refused (graph indicator `{c}` at byte \
                             {idx}) — configuration documents must be trees"
                        ));
                    }
                    '\'' | '"' | '!' | '|' | '>' if node_start && run_open => {
                        // Data if the scalar above really continues, a
                        // fresh quoted scalar / tag / block-scalar
                        // header if it does not — deciding needs the
                        // parser's indent stack, and guessing either
                        // way can blind the anchor refusal. Refused,
                        // not guessed.
                        return Err(format!(
                            "a `{c}` at a position where a multi-line plain scalar may continue \
                             is refused (byte {idx}) — the graph gate cannot tell scalar data \
                             from a new token here; quote the scalar or reindent it"
                        ));
                    }
                    '\'' if node_start => {
                        if line_prefix_only {
                            anchor_col = Some(col);
                            line_prefix_only = false;
                        }
                        mode = Mode::SingleQuoted { opened_at: idx };
                        prev_blank = false;
                    }
                    '"' if node_start => {
                        if line_prefix_only {
                            anchor_col = Some(col);
                            line_prefix_only = false;
                        }
                        mode = Mode::DoubleQuoted { opened_at: idx };
                        prev_blank = false;
                    }
                    '!' if node_start => {
                        if char_after(text, idx + 1) == Some('<') {
                            return Err(format!(
                                "verbatim YAML tags are refused (`!<` at byte {idx}) — \
                                 configuration documents use shorthand tags only, and the \
                                 verbatim URI form can hide graph indicators"
                            ));
                        }
                        if line_prefix_only {
                            anchor_col = Some(col);
                            line_prefix_only = false;
                        }
                        mode = Mode::Tag;
                        prev_blank = false;
                    }
                    '|' | '>' if node_start => {
                        let parent_col = if line_prefix_only {
                            col
                        } else {
                            anchor_col.unwrap_or(col)
                        };
                        line_prefix_only = false;
                        mode = Mode::BlockHeader { parent_col };
                        prev_blank = false;
                    }
                    '#' if prev_blank => {
                        mode = Mode::Comment;
                        run_open = false;
                    }
                    '-' | '?' if node_start && blankz_after(text, idx + 1) => {
                        // A block entry / explicit-key indicator:
                        // token-start survives it. When a scalar from
                        // above is continuing this is its data, and
                        // holding token-start merely over-refuses the
                        // graph indicators behind it.
                        prev_blank = false;
                    }
                    ':' => {
                        // `:` yields a token-start in every context
                        // the parser has one (block `: `, flow `:`,
                        // and the adjacent form after a quoted key);
                        // over-approximating the rest costs data
                        // spellings like `a:&b`, never soundness. It
                        // definitely ends a plain scalar only when a
                        // blank follows.
                        if line_prefix_only {
                            anchor_col = Some(col);
                            line_prefix_only = false;
                        }
                        node_start = true;
                        if blankz_after(text, idx + 1) {
                            run_open = false;
                        }
                        prev_blank = false;
                    }
                    ',' | '[' | '{' => {
                        // A flow indicator opens a token in FLOW
                        // context and is scalar data in BLOCK context,
                        // and the two must be told apart: `key:
                        // data.items[*]` is one plain scalar whose `[`
                        // is data, so treating it as a token-start made
                        // the `*` behind it read as an alias — refusing
                        // a document YAML itself accepts, and the shape
                        // every path-like selector writes. Inside flow
                        // the indicator always starts a token (a plain
                        // scalar cannot contain one there), and a `[`
                        // or `{` with no scalar running opens one.
                        if line_prefix_only {
                            anchor_col = Some(col);
                            line_prefix_only = false;
                        }
                        let opens_node = flow_depth > 0 || (c != ',' && !run_open);
                        if opens_node {
                            if c != ',' {
                                flow_depth += 1;
                            }
                            node_start = true;
                            run_open = false;
                        }
                        prev_blank = false;
                    }
                    ']' | '}' => {
                        if line_prefix_only {
                            anchor_col = Some(col);
                            line_prefix_only = false;
                        }
                        // Closing what an opener counted; in block
                        // context (depth zero) it is scalar data and
                        // the depth stays put.
                        if flow_depth > 0 {
                            flow_depth -= 1;
                            run_open = false;
                        } else {
                            run_open = true;
                        }
                        node_start = false;
                        prev_blank = false;
                    }
                    _ => {
                        // Scalar data — including `&`, `*`, quotes and
                        // `#` once a token has begun.
                        if line_prefix_only {
                            anchor_col = Some(col);
                            line_prefix_only = false;
                        }
                        node_start = false;
                        run_open = true;
                        prev_blank = false;
                    }
                }
                col += 1;
                idx += c_len;
            }
            Mode::SingleQuoted { opened_at } => {
                if is_break(c) {
                    return Err(multiline_quote_refusal('\'', opened_at));
                }
                if c == '\'' {
                    if char_after(text, idx + 1) == Some('\'') {
                        // An escaped quote (`''`) stays inside.
                        col += 2;
                        idx += 2;
                        continue;
                    }
                    mode = Mode::Scan;
                    node_start = false;
                    run_open = false;
                    prev_blank = false;
                }
                col += 1;
                idx += c_len;
            }
            Mode::DoubleQuoted { opened_at } => {
                if is_break(c) {
                    return Err(multiline_quote_refusal('"', opened_at));
                }
                if c == '\\' {
                    // The escaped character is content — unless it is
                    // a line break, which is the fold-escape spelling
                    // of a multi-line quoted scalar.
                    match char_after(text, idx + 1) {
                        Some(next) if !is_break(next) => {
                            col += 2;
                            idx += 1 + next.len_utf8();
                            continue;
                        }
                        _ => return Err(multiline_quote_refusal('"', opened_at)),
                    }
                }
                if c == '"' {
                    mode = Mode::Scan;
                    node_start = false;
                    run_open = false;
                    prev_blank = false;
                }
                col += 1;
                idx += c_len;
            }
            Mode::Comment => {
                if is_break(c) {
                    mode = Mode::Scan;
                    // Reprocess the break in Scan for the line reset.
                    continue;
                }
                col += 1;
                idx += c_len;
            }
            Mode::Tag => {
                if is_break(c) {
                    mode = Mode::Scan;
                    continue;
                }
                if is_blank(c) {
                    mode = Mode::Scan;
                    // The tag decorates the node that follows:
                    // token-start survives, so `!!str &a` refuses.
                    prev_blank = true;
                    col += 1;
                    idx += c_len;
                    continue;
                }
                if matches!(c, ',' | '[' | ']' | '{' | '}') {
                    // A shorthand tag cannot contain flow indicators;
                    // hand the character back to the structural scan.
                    mode = Mode::Scan;
                    continue;
                }
                col += 1;
                idx += c_len;
            }
            Mode::BlockHeader { parent_col } => {
                if is_break(c) {
                    mode = Mode::BlockMeasure { parent_col };
                    col = 0;
                    idx += c_len;
                    continue;
                }
                col += 1;
                idx += c_len;
            }
            Mode::BlockMeasure { parent_col } => {
                if is_break(c) {
                    // A blank line is content wherever it sits.
                    col = 0;
                    idx += c_len;
                    continue;
                }
                if c == ' ' {
                    col += 1;
                    idx += c_len;
                    continue;
                }
                if col > parent_col {
                    mode = Mode::BlockContent { parent_col };
                    continue;
                }
                // At or left of the parent column: the scalar is over
                // and this line is structure — rescan it from here.
                mode = Mode::Scan;
                node_start = true;
                run_open = false;
                prev_blank = true;
                line_prefix_only = true;
                anchor_col = None;
                continue;
            }
            Mode::BlockContent { parent_col } => {
                if is_break(c) {
                    mode = Mode::BlockMeasure { parent_col };
                    col = 0;
                }
                idx += c_len;
            }
        }
    }
    match mode {
        Mode::SingleQuoted { opened_at } => Err(multiline_quote_refusal('\'', opened_at)),
        Mode::DoubleQuoted { opened_at } => Err(multiline_quote_refusal('"', opened_at)),
        _ => Ok(()),
    }
}

/// Which sink the scanner is inside, when it is inside one.
enum Mode {
    /// Structural scanning: token-start tracking is live.
    Scan,
    /// Inside a single-quoted scalar (must close on its own line).
    SingleQuoted { opened_at: usize },
    /// Inside a double-quoted scalar (must close on its own line).
    DoubleQuoted { opened_at: usize },
    /// A `#` comment or a `%` directive line: inert to end of line.
    Comment,
    /// A shorthand tag token: runs to the next blank, flow indicator,
    /// or end of line; token-start survives it (a tag decorates the
    /// node that follows, so `!!str &a` is still an anchor).
    Tag,
    /// The rest of a `|`/`>` header line: indicators or a comment for
    /// the parser, junk otherwise — either way inert to end of line.
    BlockHeader { parent_col: usize },
    /// At the start of a line inside block-scalar content, counting
    /// indent to decide content (skip) versus structure (rescan).
    BlockMeasure { parent_col: usize },
    /// The rest of a block-scalar content line: inert to end of line.
    BlockContent { parent_col: usize },
}

fn is_break(c: char) -> bool {
    matches!(c, '\n' | '\r' | '\u{0085}' | '\u{2028}' | '\u{2029}')
}

fn is_blank(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\u{FEFF}')
}

/// The character after byte `idx`, if any.
fn char_after(text: &str, idx: usize) -> Option<char> {
    text[idx..].chars().next()
}

/// True when the character after byte `idx` is a blank, a line break,
/// or the end of input — the parser's "blank or zero" class that turns
/// `-`, `?` and `:` into separators and ends document markers.
fn blankz_after(text: &str, idx: usize) -> bool {
    match char_after(text, idx) {
        None => true,
        Some(c) => is_blank(c) || is_break(c),
    }
}

fn multiline_quote_refusal(quote: char, opened_at: usize) -> String {
    format!(
        "a quoted scalar spanning lines is refused (`{quote}` opened at byte {opened_at} is \
         still open at the end of its line) — an open quote would blind the anchor-and-alias \
         refusal, so the graph gate reads quotes one line at a time; use a block scalar \
         (`|`) for multi-line text"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    struct Probe {
        name: String,
        #[serde(default)]
        limit: u32,
    }

    #[derive(Debug, thiserror::Error)]
    enum ProbeError {
        #[error("probe yaml: {0}")]
        Yaml(#[from] serde_yaml_ng::Error),
        #[error("probe json: {0}")]
        Json(#[from] serde_json::Error),
        #[error("probe config: {0}")]
        Invalid(String),
    }

    impl Document for Probe {
        type Error = ProbeError;
        fn validate(&self) -> Result<(), Self::Error> {
            if self.name.is_empty() {
                return Err(ProbeError::Invalid("`name` must not be empty".into()));
            }
            if self.limit > 100 {
                return Err(ProbeError::Invalid(format!(
                    "`limit` is {} — the maximum is 100",
                    self.limit
                )));
            }
            Ok(())
        }
    }

    /// The trait's whole point: the gate runs in EVERY entry path, so an
    /// invalid document is refused identically from all three.
    #[test]
    fn every_entry_point_runs_the_same_gate() {
        let refused_yaml = Probe::from_yaml("name: \"\"").unwrap_err();
        let refused_json = Probe::from_json("{\"name\": \"\"}").unwrap_err();
        let refused_value = Probe::from_value(serde_json::json!({"name": ""})).unwrap_err();
        for refused in [refused_yaml, refused_json, refused_value] {
            assert_eq!(
                refused.to_string(),
                "probe config: `name` must not be empty"
            );
        }
    }

    /// Parse errors surface through the connector's OWN From impls — the
    /// seam adds no framing of its own. The value path's shape mismatch
    /// is a parse error too, absorbed the same way.
    #[test]
    fn parse_errors_keep_the_connector_wording() {
        let yaml = Probe::from_yaml(": not yaml").unwrap_err();
        assert!(yaml.to_string().starts_with("probe yaml: "), "{yaml}");
        let json = Probe::from_json("not json").unwrap_err();
        assert!(json.to_string().starts_with("probe json: "), "{json}");
        assert!(matches!(yaml, ProbeError::Yaml(_)));
        assert!(matches!(json, ProbeError::Json(_)));
        let value = Probe::from_value(serde_json::json!({"name": 7})).unwrap_err();
        assert!(value.to_string().starts_with("probe json: "), "{value}");
        assert!(matches!(value, ProbeError::Json(_)));
    }

    /// A valid document comes back parsed, from any form, including the
    /// embedder's no-round-trip value path.
    #[test]
    fn valid_documents_parse_from_all_three_forms() {
        let from_yaml = Probe::from_yaml("name: a\nlimit: 7").expect("yaml");
        assert_eq!((from_yaml.name.as_str(), from_yaml.limit), ("a", 7));
        let from_json = Probe::from_json("{\"name\": \"a\"}").expect("json");
        assert_eq!(from_json.limit, 0, "serde defaults apply");
        let from_value =
            Probe::from_value(serde_json::json!({"name": "a", "limit": 100})).expect("value");
        assert_eq!(from_value.limit, 100, "the boundary is inside the gate");
    }

    /// Validation sees the PARSED document — refusals can quote resolved
    /// values, not just raw text.
    #[test]
    fn refusals_quote_the_parsed_value() {
        let refused = Probe::from_value(serde_json::json!({"name": "a", "limit": 101}))
            .unwrap_err()
            .to_string();
        assert_eq!(refused, "probe config: `limit` is 101 — the maximum is 100");
    }

    /// The YAML entry point is a security seat, not just a parser: the
    /// graph gate runs before deserialization, so anchors and aliases
    /// refuse instead of expanding — through the connector's own YAML
    /// error wording, since the seam still renders no text of its own.
    #[test]
    fn from_yaml_refuses_graph_syntax_before_parsing() {
        let refused = Probe::from_yaml("name: &n probe\nalias: *n\n").unwrap_err();
        assert!(matches!(refused, ProbeError::Yaml(_)));
        let rendered = refused.to_string();
        assert!(rendered.starts_with("probe yaml: "), "{rendered}");
        assert!(rendered.contains("anchors and aliases"), "{rendered}");

        // The blinding shape: an apostrophe inside a plain scalar must
        // not disarm the gate for the anchors after it.
        let refused = Probe::from_yaml("name: it's\nbig: &b [x]\nboom: *b\n").unwrap_err();
        assert!(
            refused.to_string().contains("anchors and aliases"),
            "{refused}"
        );

        // And the gate stays a gate, not a filter: ordinary documents
        // with indicator characters as data still parse.
        let parsed = Probe::from_yaml("name: don't#stop&go\n").expect("data indicators parse");
        assert_eq!(parsed.name, "don't#stop&go");
    }

    /// The YAML entry point refuses over-sized documents before any
    /// parsing — configuration is hand-written and measures in
    /// kilobytes, so a multi-megabyte document is a wrong input, not a
    /// big config.
    #[test]
    fn from_yaml_refuses_documents_over_the_size_cap() {
        let mut oversized = String::from("name: ");
        oversized.push_str(&"a".repeat(rdlt_connector::gate::MAX_DOCUMENT_BYTES as usize));
        let refused = Probe::from_yaml(&oversized).unwrap_err();
        assert!(matches!(refused, ProbeError::Yaml(_)));
        let rendered = refused.to_string();
        assert!(rendered.contains("over the 8388608-byte cap"), "{rendered}");
    }

    /// The boundary half: the refusal fires at cap+1, so a document of
    /// EXACTLY the cap must be ACCEPTED — the check is `>`, and an
    /// off-by-one here rejects the largest legitimate document.
    #[test]
    fn from_yaml_accepts_a_document_at_exactly_the_size_cap() {
        let cap = rdlt_connector::gate::MAX_DOCUMENT_BYTES as usize;
        let prefix = "name: '";
        let suffix = "'\n";
        let document = format!(
            "{prefix}{}{suffix}",
            "a".repeat(cap - prefix.len() - suffix.len())
        );
        assert_eq!(document.len(), cap, "the fixture IS the cap");
        let parsed = Probe::from_yaml(&document).expect("a document at exactly the cap parses");
        assert_eq!(parsed.name.len(), cap - prefix.len() - suffix.len());
    }

    /// The JSON entry rides the same ceiling as the YAML one, on the
    /// same boundary: exactly the cap parses, one byte over refuses
    /// through the JSON arm, before the parser runs.
    #[test]
    fn from_json_holds_the_same_size_cap_as_from_yaml() {
        let cap = rdlt_connector::gate::MAX_DOCUMENT_BYTES as usize;
        let prefix = "{\"name\":\"";
        let suffix = "\"}";
        let at_cap = format!(
            "{prefix}{}{suffix}",
            "a".repeat(cap - prefix.len() - suffix.len())
        );
        assert_eq!(at_cap.len(), cap);
        Probe::from_json(&at_cap).expect("a document at exactly the cap parses");
        let over = format!(
            "{prefix}{}{suffix}",
            "a".repeat(cap + 1 - prefix.len() - suffix.len())
        );
        let refused = Probe::from_json(&over).unwrap_err();
        assert!(matches!(refused, ProbeError::Json(_)), "{refused}");
        assert!(
            refused.to_string().contains("over the 8388608-byte cap"),
            "{refused}"
        );
    }

    /// The generated schema is the parser's own shape.
    #[cfg(feature = "schema")]
    #[test]
    fn schema_of_reflects_the_document_type() {
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        struct Documented {
            #[allow(dead_code)]
            url: String,
        }
        let schema = schema_of::<Documented>();
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["url"].is_object(), "{schema}");
    }

    /// The document must refuse, and the refusal must name graph
    /// syntax — a hostile spelling failing for some OTHER reason would
    /// leave the anchor claim untested.
    fn assert_graph_refused(doc: &str) {
        let error = reject_graph_syntax(doc).expect_err(doc);
        assert!(error.contains("anchors and aliases"), "{doc:?}: {error}");
    }

    fn assert_accepted(doc: &str) {
        if let Err(error) = reject_graph_syntax(doc) {
            panic!("{doc:?} must pass the gate, refused with: {error}");
        }
        // An accepted document must actually be the YAML it looks
        // like — acceptance by a gate that desynchronized from the
        // parser would be vacuous.
        serde_yaml_ng::from_str::<serde_yaml_ng::Value>(doc).expect(doc);
    }

    #[test]
    fn anchors_and_aliases_refuse_in_every_position() {
        assert_graph_refused("a: &x v");
        assert_graph_refused("a: *x");
        assert_graph_refused("&x v");
        assert_graph_refused("*x");
        assert_graph_refused("- &x v");
        assert_graph_refused("- *x");
        assert_graph_refused("a: [&x v]");
        assert_graph_refused("a: [b, *x]");
        assert_graph_refused("a: {k: &x v}");
        assert_graph_refused("a: {k: *x}");
        assert_graph_refused("--- &x v");
        assert_graph_refused("a:\n  <<: *base\n");
        // An anchor rides a tag: the tag decorates the node, so the
        // anchor after it is live.
        assert_graph_refused("a: !!str &x v");
        // The flow adjacent-value forms: a value indicator with no
        // trailing space after a quoted key, and a space-separated
        // bare `:` in flow.
        assert_graph_refused("{\"a\":&x v}");
        assert_graph_refused("{a :&x v}");
        // An explicit complex key.
        assert_graph_refused("? &x\n: v\n");
        // A flow collection continuing on the next line.
        assert_graph_refused("a: [b,\n  *x]\n");
    }

    #[test]
    fn mid_scalar_quotes_do_not_blind_the_gate() {
        // The plain scalar `john's orders` contains an apostrophe that
        // is data, not a quote-open; the anchor and alias after it
        // must still refuse — the exact shape that blinds a naive
        // character scan.
        assert_graph_refused("p: john's orders\nbig: &big [a]\nboom: [*big, *big]\n");
        assert_graph_refused("p: say \"hi there\nbig: &big [a]\nboom: *big\n");
        // Both quote kinds, several scalars deep.
        assert_graph_refused("a: it's\nb: rock \"n\" roll\nc: &x v\nd: *x\n");
    }

    #[test]
    fn smuggling_spellings_refuse_rather_than_slip() {
        // A quoted key whose content looks like a comment: the ` # `
        // is quote content, and the anchor after the real `:` is live.
        assert_graph_refused("\"a # b\": &x v");
        // A flow entry opening a quote right after a line-spanning
        // plain scalar: data if the scalar continues, a live quote if
        // it does not — the gate refuses the ambiguity instead of
        // guessing, so the anchors behind it cannot ride through.
        let error = reject_graph_syntax("a: [b\n'q, &x [c], *x]\n").expect_err("ambiguous opener");
        assert!(error.contains("multi-line plain scalar"), "{error}");
        // A quote left open across a line break is refused at the
        // break — everything behind it stays unreadable to the gate,
        // so it never vouches for it.
        let error = reject_graph_syntax("a: 'text\nb: &x v\nc: *x\n").expect_err("open quote");
        assert!(
            error.contains("still open at the end of its line"),
            "{error}"
        );
        let error = reject_graph_syntax("a: \"text\nb: &x v\n").expect_err("open quote");
        assert!(
            error.contains("still open at the end of its line"),
            "{error}"
        );
        // The double-quoted fold escape (backslash before the break)
        // is the same multi-line spelling.
        let error = reject_graph_syntax("a: \"text\\\nmore\"\n").expect_err("fold escape");
        assert!(
            error.contains("still open at the end of its line"),
            "{error}"
        );
        // A block-scalar header where a plain scalar may continue.
        let error = reject_graph_syntax("a: text\n| header\n").expect_err("ambiguous header");
        assert!(error.contains("multi-line plain scalar"), "{error}");
        // A verbatim tag could carry flow indicators in its URI.
        let error = reject_graph_syntax("a: !<tag:x,y> v").expect_err("verbatim tag");
        assert!(error.contains("verbatim YAML tags"), "{error}");
    }

    #[test]
    fn indicator_characters_as_data_are_accepted() {
        // The over-rejections a naive character scan carries: `#`
        // mid-token, `&`/`*` mid-scalar and after spaces.
        assert_accepted("key: val#ue\n");
        assert_accepted("key: a&b\n");
        assert_accepted("key: a*b\n");
        assert_accepted("key: see *chapter and & sign\n");
        assert_accepted("key: don't\nit's: fine\n");
        assert_accepted("cmd: a | b > c\n");
        assert_accepted("expr: items[0] and {braces} in block scalars\n");
        assert_accepted("url: http://example.com/x?y=1&z=2\n");
        assert_accepted("dsn: postgres://user@host:5432/db\n");
        assert_accepted("path: C:\\dir\\file\n");
    }

    #[test]
    fn block_scalars_carry_any_content() {
        assert_accepted("notes: |\n  this & that\n  * bullet\n  'quote\n  \"quote\nnext: 1\n");
        assert_accepted("notes: >\n  folded & text\n  * still data\n");
        assert_accepted("notes: |-\n  chomped & content\n");
        assert_accepted("notes: |2\n   explicit & indent\n");
        // Nested under a sequence entry, with a sibling after it.
        assert_accepted("- text: |\n    a & b\n- other\n");
        // Deeper structure resumes after the scalar.
        assert_accepted("a:\n  b: |\n    x & y\n  c: 1\n");
    }

    #[test]
    fn quotes_and_comments_stay_inert() {
        assert_accepted("a: 'it''s quoted, & safe'\n");
        assert_accepted("b: \"say \\\"hi\\\" & bye\"\n");
        assert_accepted("c: '*not-an-alias'\nd: \"&not-an-anchor\"\n");
        assert_accepted("cron: '*/5 * * * *'\n");
        assert_accepted("# top comment with & and *\nkey: v # trailing & *\n");
    }

    #[test]
    fn everyday_structure_is_accepted() {
        assert_accepted("tables: [a, b, c]\nmap: {k: v, n: 2}\n");
        assert_accepted("tables: [\n  'a',\n  'b'\n]\n");
        assert_accepted("desc: a long\n  sentence that continues\n");
        assert_accepted("---\nkey: v\n");
        assert_accepted("");
        assert_accepted("key:\n  nested:\n    - 1\n    - 2\n");
        assert_accepted("write_mode: {merge: {key: [a, b]}}\n");
        assert_accepted("empty: {}\nlist: []\nnothing:\n");
    }

    #[test]
    fn shorthand_tags_pass_and_still_guard_what_follows() {
        // The tagged-enum vocabulary some connectors accept is
        // shorthand tags — those pass.
        let doc = "auth: !bearer {token: x}\n";
        assert!(reject_graph_syntax(doc).is_ok());
        let doc = "value: !!str 42\n";
        assert_accepted(doc);
    }

    #[test]
    fn directives_and_markers_are_inert() {
        assert_accepted("%YAML 1.1\n---\nkey: v\n");
        assert_accepted("key: v\n...\n");
    }

    /// A path-shaped selector is scalar data, not a graph reference.
    /// `data.items[*]` is one plain scalar: its `[` cannot open a flow
    /// collection mid-scalar, so the `*` behind it is a wildcard the
    /// document is entitled to write. Refusing it would reject a
    /// document YAML itself accepts — and every JSONPath-shaped config
    /// key writes this shape.
    #[test]
    fn a_wildcard_inside_a_plain_scalar_is_data_not_an_alias() {
        for document in [
            "records_path: data.items[*].payload\n",
            "select: a[*]\nother: b\n",
            "nested:\n  path: root.items[*]\n",
        ] {
            reject_graph_syntax(document)
                .unwrap_or_else(|e| panic!("{document:?} is a tree document, refused: {e}"));
        }
    }

    /// The refusal still fires where a flow collection genuinely opens
    /// a node — the indicator is a token-start when no plain scalar is
    /// running, which is where an alias can actually appear.
    #[test]
    fn an_alias_opening_a_flow_node_is_still_refused() {
        for document in [
            "a: &x 1\nb: [*x]\n",
            "a: &x 1\nb: {k: *x}\n",
            "a: &x 1\nb: [1, *x]\n",
        ] {
            reject_graph_syntax(document).expect_err(&format!("{document:?} carries an alias"));
        }
    }
}
