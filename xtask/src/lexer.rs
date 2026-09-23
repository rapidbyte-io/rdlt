//! A minimal Rust lexer that separates comments from code.

#[cfg(test)]
mod tests;

/// The syntactic form of a comment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommentKind {
    /// `// ...`
    Line,
    /// `/// ...`
    OuterDoc,
    /// `//! ...`
    InnerDoc,
    /// `/* ... */`
    Block,
}

/// One comment and the line it starts on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Comment {
    pub(crate) line: usize,
    pub(crate) kind: CommentKind,
    pub(crate) text: String,
}

/// A source file split into comments and comment-free code.
#[derive(Debug, Default)]
pub(crate) struct Scan {
    pub(crate) comments: Vec<Comment>,
    /// The source with comments removed and literal contents blanked; line breaks are kept.
    pub(crate) code: String,
}

impl Scan {
    /// Lines that contain code.
    pub(crate) fn code_lines(&self) -> usize {
        self.code
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count()
    }

    /// Distinct lines holding `//` or `/* */` comments; rustdoc does not count.
    pub(crate) fn comment_lines(&self) -> usize {
        let mut lines: Vec<usize> = Vec::new();
        let prose =
            |comment: &&Comment| matches!(comment.kind, CommentKind::Line | CommentKind::Block);
        for comment in self.comments.iter().filter(prose) {
            let extra = comment.text.matches('\n').count();
            lines.extend(comment.line..=comment.line + extra);
        }
        lines.sort_unstable();
        lines.dedup();
        lines.len()
    }
}

/// Splits `source` into comments and code.
pub(crate) fn scan(source: &str) -> Scan {
    let chars: Vec<char> = source.chars().collect();
    let mut out = Scan::default();
    let mut line = 1;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c == '/' && next == Some('/') {
            let start = i;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            let text: String = chars[start..i].iter().collect();
            out.comments.push(Comment {
                line,
                kind: line_kind(&text),
                text,
            });
        } else if c == '/' && next == Some('*') {
            let start_line = line;
            let (end, text) = block_comment(&chars, i, &mut out.code, &mut line);
            out.comments.push(Comment {
                line: start_line,
                kind: CommentKind::Block,
                text,
            });
            i = end;
        } else if c == '"' {
            i = skip_quoted(&chars, i, &mut out.code, &mut line);
        } else if let Some(hashes) = raw_string_start(&chars, i) {
            i = skip_raw(&chars, i, hashes, &mut out.code, &mut line);
        } else if c == '\'' {
            i = skip_char_literal(&chars, i, &mut out.code);
        } else {
            if c == '\n' {
                line += 1;
            }
            out.code.push(c);
            i += 1;
        }
    }
    out
}

fn line_kind(text: &str) -> CommentKind {
    if text.starts_with("///") && !text.starts_with("////") {
        CommentKind::OuterDoc
    } else if text.starts_with("//!") {
        CommentKind::InnerDoc
    } else {
        CommentKind::Line
    }
}

/// Consumes a possibly nested block comment starting at `start`; returns the end index and text.
fn block_comment(
    chars: &[char],
    start: usize,
    code: &mut String,
    line: &mut usize,
) -> (usize, String) {
    let mut depth = 0usize;
    let mut i = start;
    while i < chars.len() {
        if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
            depth += 1;
            i += 2;
        } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
            depth -= 1;
            i += 2;
            if depth == 0 {
                break;
            }
        } else {
            if chars[i] == '\n' {
                code.push('\n');
                *line += 1;
            }
            i += 1;
        }
    }
    let end = i.min(chars.len());
    (end, chars[start..end].iter().collect())
}

fn is_ident(c: Option<&char>) -> bool {
    c.is_some_and(|c| c.is_alphanumeric() || *c == '_')
}

/// The number of `#`s when a raw string literal (`r"`, `r#"`, `br#"`) starts at `i`.
fn raw_string_start(chars: &[char], i: usize) -> Option<usize> {
    if chars[i] != 'r' {
        return None;
    }
    let before = |back: usize| i.checked_sub(back).and_then(|p| chars.get(p));
    let prefix_ok = !is_ident(before(1)) || (before(1) == Some(&'b') && !is_ident(before(2)));
    if !prefix_ok {
        return None;
    }
    let hashes = chars[i + 1..].iter().take_while(|c| **c == '#').count();
    (chars.get(i + 1 + hashes) == Some(&'"')).then_some(hashes)
}

fn skip_quoted(chars: &[char], start: usize, code: &mut String, line: &mut usize) -> usize {
    code.push('"');
    let mut i = start + 1;
    while i < chars.len() && chars[i] != '"' {
        if chars[i] == '\\' {
            i += 1;
        }
        blank(chars.get(i).copied(), code, line);
        i += 1;
    }
    code.push('"');
    i + 1
}

fn skip_raw(
    chars: &[char],
    start: usize,
    hashes: usize,
    code: &mut String,
    line: &mut usize,
) -> usize {
    code.push('"');
    let mut i = start + hashes + 2;
    while i < chars.len() {
        let closes = chars[i] == '"'
            && chars[i + 1..]
                .iter()
                .take(hashes)
                .filter(|c| **c == '#')
                .count()
                == hashes;
        if closes {
            code.push('"');
            return i + 1 + hashes;
        }
        blank(Some(chars[i]), code, line);
        i += 1;
    }
    i
}

/// Consumes a char literal at `start`, or only the `'` of a lifetime or label.
fn skip_char_literal(chars: &[char], start: usize, code: &mut String) -> usize {
    let first = chars.get(start + 1);
    if first != Some(&'\\') && first != Some(&'\n') && chars.get(start + 2) == Some(&'\'') {
        code.push_str("' '");
        return start + 3;
    }
    if first == Some(&'\\') && chars.get(start + 2).is_some_and(|c| *c != '\n') {
        let close = chars
            .iter()
            .enumerate()
            .skip(start + 3)
            .take(10)
            .find(|(_, c)| **c == '\'' || **c == '\n');
        if let Some((end, '\'')) = close.map(|(end, c)| (end, *c)) {
            code.push_str("' '");
            return end + 1;
        }
    }
    code.push('\'');
    start + 1
}

fn blank(c: Option<char>, code: &mut String, line: &mut usize) {
    if c == Some('\n') {
        code.push('\n');
        *line += 1;
    } else {
        code.push(' ');
    }
}
