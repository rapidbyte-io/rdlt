//! Holds a crate that catches a panic to a build that unwinds: its root must refuse to compile
//! with `panic = "abort"`, under which a caught panic ends the process instead.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

use crate::lexer::Scan;
use crate::rules::{FileRole, Finding, Rule};

static CATCHES_PANIC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\bcatch_unwind\b").expect("a valid pattern"));
static GUARDED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"#\[\s*cfg\s*\([^\]]*\]\s*compile_error\s*!").expect("a valid pattern")
});
static CONDITION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)#\[\s*cfg\s*\((.*?)\)\s*\]").expect("a valid pattern"));
static TOKEN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"^\s*(?:(\w+)|"([^"]*)"|([(),=]))"#).expect("a valid pattern"));

/// What the files read so far say about unwinding, by the crate under `crates` they belong to.
#[derive(Debug, Default)]
pub(crate) struct Unwinding {
    /// Each crate's files outside tests that catch a panic, with the line of the first catch.
    catching: BTreeMap<PathBuf, Vec<(PathBuf, usize)>>,
    /// The crates whose roots require unwinding.
    guarded: BTreeSet<PathBuf>,
}

impl Unwinding {
    /// Reads the file at `path`, relative to the repository root, whose text is `source`.
    pub(crate) fn read(&mut self, path: &Path, role: FileRole, scan: &Scan, source: &str) {
        let Some(member) = member(path) else {
            return;
        };
        if let Some(caught) = CATCHES_PANIC.find(&scan.code).filter(|_| !role.test) {
            let line = scan.code[..caught.start()].matches('\n').count() + 1;
            let catching = self.catching.entry(member.clone()).or_default();
            catching.push((path.to_path_buf(), line));
        }
        if path == member.join("src/lib.rs") && requires_unwinding(scan, source) {
            self.guarded.insert(member);
        }
    }

    /// A finding for each file that catches a panic in a crate whose root does not require
    /// unwinding.
    pub(crate) fn findings(self) -> Vec<(PathBuf, Finding)> {
        let unguarded = self
            .catching
            .into_iter()
            .filter(|(member, _)| !self.guarded.contains(member))
            .flat_map(|(_, caught)| caught);
        let finding = |line| Finding {
            line,
            rule: Rule::UnguardedUnwind,
            message: "a crate that catches a panic refuses to build with `panic = \"abort\"`: add \
                      `#[cfg(panic = \"abort\")] compile_error!(..)` to its root"
                .to_owned(),
        };
        unguarded
            .map(|(path, line)| (path, finding(line)))
            .collect()
    }
}

/// Whether a crate's root refuses to build where panics do not unwind: a `compile_error!`,
/// outside every module and item, under a `cfg` whose condition holds in a build that aborts.
fn requires_unwinding(scan: &Scan, source: &str) -> bool {
    GUARDED.find_iter(&scan.code).any(|guard| {
        let before = &scan.code[..guard.start()];
        if before.matches('{').count() != before.matches('}').count() {
            return false;
        }
        // The scan blanks what a condition's strings say; its lines are the source's.
        let line = before.matches('\n').count();
        let from: usize = source.split_inclusive('\n').take(line).map(str::len).sum();
        let condition = source.get(from..).and_then(|rest| CONDITION.captures(rest));
        condition.is_some_and(|condition| {
            let holds = |aborts| holds(&condition[1], aborts);
            // In a build that aborts, and in no build that unwinds.
            holds(true) == Some(true) && holds(false) == Some(false)
        })
    })
}

/// Whether the `cfg` condition `text` holds in a build whose panics abort, or unwind, as
/// `aborts` says, where every condition that is not of the panic strategy holds; `None` where
/// it is no condition.
fn holds(text: &str, aborts: bool) -> Option<bool> {
    let mut tokens = Vec::new();
    let mut rest = text;
    while !rest.trim().is_empty() {
        let token = TOKEN.captures(rest)?;
        rest = &rest[token.get(0)?.end()..];
        tokens.push(token);
    }
    let tokens: Vec<Token<'_>> = tokens.iter().filter_map(Token::of).collect();
    let (holds, rest) = predicate(&tokens, aborts)?;
    rest.is_empty().then_some(holds)
}

/// One token of a `cfg` condition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token<'a> {
    Name(&'a str),
    Text(&'a str),
    Mark(&'a str),
}

impl<'a> Token<'a> {
    fn of(token: &regex::Captures<'a>) -> Option<Self> {
        let name = token.get(1).map(|name| Self::Name(name.as_str()));
        let text = token.get(2).map(|text| Self::Text(text.as_str()));
        let mark = token.get(3).map(|mark| Self::Mark(mark.as_str()));
        name.or(text).or(mark)
    }
}

/// Whether the condition at the start of `tokens` holds in a build that aborts or unwinds as
/// `aborts` says, and the tokens after it.
fn predicate<'a, 'b>(tokens: &'b [Token<'a>], aborts: bool) -> Option<(bool, &'b [Token<'a>])> {
    let (Token::Name(name), rest) = tokens.split_first().map(|(first, rest)| (*first, rest))?
    else {
        return None;
    };
    match rest {
        [Token::Mark("="), Token::Text(value), rest @ ..] => {
            let strategy = if aborts { "abort" } else { "unwind" };
            Some((name != "panic" || *value == strategy, rest))
        }
        [Token::Mark("("), inner @ ..] => {
            let (each, rest) = listed(inner, aborts)?;
            let holds = match name {
                "all" => each.iter().all(|holds| *holds),
                "any" => each.iter().any(|holds| *holds),
                "not" => matches!(each[..], [false]),
                _ => return None,
            };
            Some((holds, rest))
        }
        rest => Some((true, rest)),
    }
}

/// Whether each condition of a list holds, up to the mark that closes it, and the tokens after
/// that mark.
fn listed<'a, 'b>(
    mut tokens: &'b [Token<'a>],
    aborts: bool,
) -> Option<(Vec<bool>, &'b [Token<'a>])> {
    let mut each = Vec::new();
    loop {
        if let [Token::Mark(")"), rest @ ..] = tokens {
            return Some((each, rest));
        }
        let (holds, rest) = predicate(tokens, aborts)?;
        each.push(holds);
        tokens = rest.strip_prefix(&[Token::Mark(",")]).unwrap_or(rest);
    }
}

/// The directory of the workspace member under `crates` that `path` belongs to.
fn member(path: &Path) -> Option<PathBuf> {
    let mut parts = path.components();
    let crates = parts.next().filter(|part| part.as_os_str() == "crates")?;
    Some(Path::new(&crates).join(parts.next()?))
}
