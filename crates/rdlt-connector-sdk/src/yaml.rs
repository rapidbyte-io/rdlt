//! The raw-text YAML security gate, run BEFORE any serde
//! materialization of an untrusted document.
//!
//! `serde_yaml` expands anchors and aliases while constructing the
//! target value, so an input-byte cap cannot bound the resulting
//! allocation: a few megabytes of anchored content plus a stream of
//! three-byte aliases materializes quadratically. Worse, the expansion
//! of everything parsed so far happens even when the document
//! ultimately fails to deserialize — the parse error is only surfaced
//! afterwards. Configuration documents are trees; graph syntax is
//! refused here, at the text boundary, before the parser allocates
//! anything.
//!
//! # Why this is a scanner, and how it stays sound
//!
//! The pinned `serde_yaml` 0.9.34 keeps its event stream private
//! (`mod loader` / `mod libyaml`), so the gate cannot refuse on parser
//! events; it must decide from the raw text. A naive character scan is
//! unsound — one apostrophe inside a plain scalar (`pipeline: john's
//! orders`) misread as quote-open blinds every later check. This
//! scanner instead tracks the one thing that matters: WHERE A TOKEN
//! CAN START. In YAML, `&` and `*` begin an anchor or alias only at a
//! token-start position (after `-`/`?`/`:` separators, flow indicators
//! `, [ {`, a tag, a document marker, or a line start); everywhere
//! else they are scalar data. Quote and block-scalar indicators obey
//! the same rule, which is what keeps `john's` from opening a quote.
//!
//! The scanner OVER-approximates: every position where the underlying
//! parser could start a token is a token-start here, and the handful of
//! spellings whose reading cannot be decided line-locally are refused
//! outright rather than guessed:
//!
//! - quoted scalars spanning a line break (an open quote at end of
//!   line would blind every later check, so it refuses instead);
//! - a quote, tag, or block-scalar indicator at a token-start position
//!   while a plain scalar may still be continuing (indistinguishable
//!   from scalar data without the parser's indent stack);
//! - verbatim tags `!<…>` (their URI form may contain flow indicators
//!   that would desynchronize the scan).
//!
//! Every refusal is availability-only over legal-but-exotic YAML; the
//! common configuration vocabulary — plain scalars with apostrophes,
//! `&`/`*`/`#` as data inside scalars, single-line quoted strings,
//! block scalars with any content, shorthand tags, flow collections —
//! passes untouched. Anchors and aliases can NEVER pass: a miss would
//! need a token-start position this scan does not model, and the
//! token-start set here is a superset of the parser's by construction.

/// The most a YAML/JSON configuration document may weigh before parsing
/// is refused: hand-written configuration measures in kilobytes, so a
/// multi-megabyte "document" is a wrong path (a data file, a dump) —
/// better refused by size, typed, than slurped whole into memory and
/// fed to a recursive parser.
pub const MAX_DOCUMENT_BYTES: u64 = 8 * 1024 * 1024;

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

/// Refuse YAML graph syntax — and the few spellings whose reading
/// cannot be decided without a full parser — before deserialization.
///
/// `Ok(())` means the document contains no anchors and no aliases, so
/// `serde_yaml` materialization is tree-bounded: the value it builds
/// can only be as large as the text that spells it. The error is one
/// rendered line naming the refused indicator and its byte offset;
/// callers absorb it into their own error vocabulary.
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
                        // Flow separators are token-starts in flow and
                        // scalar data in block context; holding
                        // token-start in both is the refusing
                        // direction.
                        if line_prefix_only {
                            anchor_col = Some(col);
                            line_prefix_only = false;
                        }
                        node_start = true;
                        prev_blank = false;
                    }
                    ']' | '}' => {
                        if line_prefix_only {
                            anchor_col = Some(col);
                            line_prefix_only = false;
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
    use super::reject_graph_syntax;

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
        serde_yaml::from_str::<serde_yaml::Value>(doc).expect(doc);
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
        // must still refuse. This is the exact shape that blinded the
        // character-scanner generation of this gate.
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
        // The over-rejections the character-scanner generation carried:
        // `#` mid-token, `&`/`*` mid-scalar and after spaces.
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
        // The tagged-enum vocabulary (the REST source's legacy auth
        // form) is shorthand tags — those pass.
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
}
