//! The comment, structure and error-style rules `cargo xtask lint` enforces.

#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;

use crate::lexer::{Comment, CommentKind, Scan};

/// Whether a finding fails the check or is only reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Severity {
    Warning,
    Error,
}

/// The rule a finding violates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Rule {
    ModRs,
    TrackerId,
    Jargon,
    Shouting,
    TodoWithoutIssue,
    LongLineComment,
    LongDocSummary,
    CommentRatio,
    FileLength,
    StringlyError,
    UnbiasedSelect,
}

impl Rule {
    pub(crate) fn severity(self) -> Severity {
        match self {
            Self::CommentRatio | Self::FileLength => Severity::Warning,
            _ => Severity::Error,
        }
    }
}

/// One rule violation at a line of a file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Finding {
    pub(crate) line: usize,
    pub(crate) rule: Rule,
    pub(crate) message: String,
}

/// How a file is checked, derived from its path.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FileRole {
    /// A `tests.rs` file, or a file under `tests/`, `benches/` or `examples/`.
    pub(crate) test: bool,
    /// A file named `mod.rs`.
    pub(crate) mod_rs: bool,
}

impl FileRole {
    pub(crate) fn of(path: &Path) -> Self {
        let named = |name: &str| path.file_name().is_some_and(|file| file == name);
        let under = |dirs: &[&str]| {
            path.components()
                .any(|c| c.as_os_str().to_str().is_some_and(|c| dirs.contains(&c)))
        };
        Self {
            test: named("tests.rs") || under(&["tests", "benches", "examples"]),
            mod_rs: named("mod.rs"),
        }
    }
}

const MAX_LINE_COMMENT_RUN: usize = 3;
const MAX_PRODUCTION_LINES: usize = 400;
const MAX_COMMENT_RATIO: f64 = 0.25;
const MIN_LINES_FOR_RATIO: usize = 20;

/// Upper-case words of four or more letters that comments may use.
const SHOUTING_ALLOWLIST: &[&str] = &[
    "ALTER", "ASCII", "BEGIN", "COMMIT", "CONFLICT", "COPY", "CREATE", "DELETE", "DISTINCT",
    "DROP", "EINTR", "EPIPE", "EXISTS", "FIFO", "FIXME", "FROM", "GROUP", "HOME", "HTTP", "HTTPS",
    "INSERT", "INTO", "JSON", "JSONL", "LIMIT", "MERGE", "MIME", "NDJSON", "NULL", "ORDER", "OVER",
    "PATH", "README", "RENAME", "ROLLBACK", "SAFETY", "SELECT", "SIGHUP", "SIGINT", "SIGKILL",
    "SIGTERM", "SIMD", "TABLE", "TODO", "TOML", "TRUNCATE", "UNION", "UNIX", "UPDATE", "UUID",
    "VALUES", "WHERE", "YAML",
];

fn pattern(source: &str) -> Regex {
    Regex::new(source).expect("lint patterns are valid regular expressions")
}

static CODE_SPAN: LazyLock<Regex> = LazyLock::new(|| pattern(r"`[^`]*`"));
static TRACKER: LazyLock<Regex> =
    LazyLock::new(|| pattern(r"\b(US\d+|D-\d+|GLM|Round-\d+|0\d{2})\b|\bspecs/"));
static JARGON: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r"(?i)\b(seats?|doors?|belts?|honest|honestly|deliberately|laws?|spelling|house|the one)\b|\b(pins?|pinned|pinning)\b",
    )
});
static SHOUT: LazyLock<Regex> = LazyLock::new(|| pattern(r"\b[A-Z]{4,}\b"));
static TODO: LazyLock<Regex> = LazyLock::new(|| pattern(r"\b(TODO|FIXME|XXX)\b(\(#\d+\))?"));
static SENTENCE_END: LazyLock<Regex> = LazyLock::new(|| pattern(r"[.!?](\s|$)"));
static ABBREVIATION: LazyLock<Regex> = LazyLock::new(|| pattern(r"\b(e\.g|i\.e|etc|vs)\."));
static RESULT_OPEN: LazyLock<Regex> = LazyLock::new(|| pattern(r"\bResult\s*<"));

/// Error types a `Result` may not carry outside tests.
const STRING_ERRORS: &[&str] = &[
    "String",
    "std::string::String",
    "alloc::string::String",
    "&'static str",
    "&str",
];
static SELECT: LazyLock<Regex> = LazyLock::new(|| pattern(r"select!\s*\{"));

/// Checks one scanned file against every rule.
pub(crate) fn check(role: FileRole, scan: &Scan) -> Vec<Finding> {
    let mut findings = Vec::new();
    if role.mod_rs {
        findings.push(finding(
            1,
            Rule::ModRs,
            "use `foo.rs` with a `foo/` directory, not `mod.rs`",
        ));
    }
    let mut in_code_block = false;
    for comment in &scan.comments {
        let is_doc = matches!(comment.kind, CommentKind::OuterDoc | CommentKind::InnerDoc);
        if is_doc && comment.text[3..].trim_start().starts_with("```") {
            in_code_block = !in_code_block;
            continue;
        }
        if !(is_doc && in_code_block) {
            check_comment_words(comment, &mut findings);
        }
    }
    check_line_comment_runs(&scan.comments, &mut findings);
    check_doc_summaries(&scan.comments, &mut findings);
    check_code(role, scan, &mut findings);
    findings.sort_by_key(|f| (f.line, f.rule));
    findings
}

fn finding(line: usize, rule: Rule, message: impl Into<String>) -> Finding {
    Finding {
        line,
        rule,
        message: message.into(),
    }
}

fn check_comment_words(comment: &Comment, findings: &mut Vec<Finding>) {
    let prose = CODE_SPAN.replace_all(&comment.text, "");
    if let Some(m) = TRACKER.find(&prose) {
        let message = format!("tracker or spec reference `{}`", m.as_str());
        findings.push(finding(comment.line, Rule::TrackerId, message));
    }
    if let Some(m) = JARGON.find(&prose) {
        findings.push(finding(
            comment.line,
            Rule::Jargon,
            format!("banned word `{}`", m.as_str()),
        ));
    }
    for m in SHOUT.find_iter(&prose) {
        if !SHOUTING_ALLOWLIST.contains(&m.as_str()) {
            let message = format!("upper-case emphasis `{}`", m.as_str());
            findings.push(finding(comment.line, Rule::Shouting, message));
        }
    }
    for caps in TODO.captures_iter(&prose) {
        if &caps[1] != "TODO" || caps.get(2).is_none() {
            let message = "use `TODO(#123)` with an issue number";
            findings.push(finding(comment.line, Rule::TodoWithoutIssue, message));
        }
    }
}

fn check_line_comment_runs(comments: &[Comment], findings: &mut Vec<Finding>) {
    let mut run_start = 0;
    let mut run_len = 0;
    let mut last_line = 0;
    for comment in comments.iter().filter(|c| c.kind == CommentKind::Line) {
        if run_len > 0 && comment.line == last_line + 1 {
            run_len += 1;
        } else {
            run_start = comment.line;
            run_len = 1;
        }
        last_line = comment.line;
        if run_len == MAX_LINE_COMMENT_RUN + 1 {
            let message = "`//` comment longer than 3 lines; move the reasoning to an ADR";
            findings.push(finding(run_start, Rule::LongLineComment, message));
        }
    }
}

fn check_doc_summaries(comments: &[Comment], findings: &mut Vec<Finding>) {
    let mut i = 0;
    while i < comments.len() {
        let kind = comments[i].kind;
        if !matches!(kind, CommentKind::OuterDoc | CommentKind::InnerDoc) {
            i += 1;
            continue;
        }
        let start_line = comments[i].line;
        let mut summary = String::new();
        let mut in_summary = true;
        let mut previous_line = start_line - 1;
        while i < comments.len()
            && comments[i].kind == kind
            && comments[i].line == previous_line + 1
        {
            let body = comments[i].text[3..].trim();
            if body.is_empty() || body.starts_with("```") || body.starts_with('#') {
                in_summary = false;
            }
            if in_summary {
                summary.push_str(body);
                summary.push(' ');
            }
            previous_line = comments[i].line;
            i += 1;
        }
        let without_spans = CODE_SPAN.replace_all(&summary, "x");
        let plain = ABBREVIATION.replace_all(&without_spans, "x");
        if SENTENCE_END.find_iter(&plain).count() > 1 {
            let message = "a doc comment's first paragraph is one sentence";
            findings.push(finding(start_line, Rule::LongDocSummary, message));
        }
    }
}

fn check_code(role: FileRole, scan: &Scan, findings: &mut Vec<Finding>) {
    let code_lines = scan.code_lines();
    if !role.test && code_lines > MAX_PRODUCTION_LINES {
        let message = format!("{code_lines} code lines; split files over {MAX_PRODUCTION_LINES}");
        findings.push(finding(1, Rule::FileLength, message));
    }
    #[expect(clippy::cast_precision_loss, reason = "line counts are far below 2^52")]
    let ratio = scan.comment_lines() as f64 / code_lines.max(1) as f64;
    if code_lines >= MIN_LINES_FOR_RATIO && ratio > MAX_COMMENT_RATIO {
        let message = format!("comment:code ratio {ratio:.2} exceeds {MAX_COMMENT_RATIO}");
        findings.push(finding(1, Rule::CommentRatio, message));
    }
    if !role.test {
        for m in RESULT_OPEN.find_iter(&scan.code) {
            if !error_is_string(&scan.code[m.end()..]) {
                continue;
            }
            let line = line_of(&scan.code, m.start());
            findings.push(finding(
                line,
                Rule::StringlyError,
                "use a typed error, not a string",
            ));
        }
    }
    for m in SELECT.find_iter(&scan.code) {
        if !scan.code[m.end()..].trim_start().starts_with("biased;") {
            let line = line_of(&scan.code, m.start());
            findings.push(finding(
                line,
                Rule::UnbiasedSelect,
                "start every `select!` with `biased;`",
            ));
        }
    }
}

/// Whether the last top-level argument of the generic list starting at `args` is a string type.
fn error_is_string(args: &str) -> bool {
    let mut depth = 0usize;
    let mut last_start = 0;
    let mut previous = ' ';
    for (offset, c) in args.char_indices() {
        match c {
            '<' | '(' | '[' => depth += 1,
            '>' if previous == '-' => {}
            ',' if depth == 0 => last_start = offset + 1,
            '>' | ')' | ']' if depth == 0 => {
                let last = args[last_start..offset].trim();
                let last = if last.is_empty() {
                    args[..last_start]
                        .trim_end_matches([',', ' ', '\n'])
                        .rsplit(',')
                        .next()
                } else {
                    Some(last)
                };
                let normalized =
                    last.map(|arg| arg.split_whitespace().collect::<Vec<_>>().join(" "));
                return normalized.is_some_and(|arg| STRING_ERRORS.contains(&arg.as_str()));
            }
            '>' | ')' | ']' => depth -= 1,
            _ => {}
        }
        previous = c;
    }
    false
}

fn line_of(text: &str, offset: usize) -> usize {
    text[..offset].matches('\n').count() + 1
}
