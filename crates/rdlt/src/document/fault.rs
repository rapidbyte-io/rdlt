//! What is wrong with a document: its code, where, the key path, and the nearest known key for an
//! unknown one; never a value, which may be a secret.

#[cfg(test)]
pub(crate) mod tests;

use std::fmt::Write as _;

use rdlt_connector::text;
use rdlt_engine::Code;
use serde_saphyr::budget::BudgetBreach;

use super::read::{ONLY_VERSION, nearest};
use crate::codes;
use crate::limits::{DOCUMENT_BYTES, KEY_BYTES, ORIGIN_BYTES, PATH_BYTES};

/// A line and a column of a document, each counted from 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Place {
    pub(crate) line: u64,
    pub(crate) column: u64,
}

impl Place {
    pub(crate) fn new(line: usize, column: usize) -> Self {
        let wide = |count: usize| u64::try_from(count).unwrap_or(u64::MAX);
        Self {
            line: wide(line),
            column: wide(column),
        }
    }
}

/// What the screen refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refused {
    Anchor,
    Alias,
    Tag,
    Document,
}

/// What is wrong with a document, in no words of its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Fault {
    code: Code,
    at: Option<Place>,
    path: Option<String>,
    said: String,
    nearest: Option<&'static str>,
}

impl Fault {
    fn new(code: Code, said: impl Into<String>) -> Self {
        Self {
            code,
            at: None,
            path: None,
            said: said.into(),
            nearest: None,
        }
    }

    pub(crate) fn too_large(noun: &str) -> Self {
        let said = format!("{noun} is larger than {DOCUMENT_BYTES} bytes");
        Self::new(codes::DOCUMENT_TOO_LARGE, said)
    }

    pub(crate) fn repeated(at: Place) -> Self {
        Self {
            at: Some(at),
            ..Self::new(codes::DOCUMENT_DUPLICATE_KEY, "a key is given twice")
        }
    }

    pub(crate) fn too_deep(noun: &str, at: Place) -> Self {
        Self {
            at: Some(at),
            ..Self::new(codes::DOCUMENT_TOO_DEEP, too_deep(noun))
        }
    }

    pub(crate) fn refused(refused: Refused, at: Place) -> Self {
        let said = match refused {
            Refused::Anchor => "an anchor is refused: nothing is repeated by reference",
            Refused::Alias => "an alias is refused: nothing is repeated by reference",
            Refused::Tag => "a tag is refused: write the value without it",
            Refused::Document => "a second document is refused: a file holds one",
        };
        Self {
            at: Some(at),
            ..Self::new(codes::DOCUMENT_REFUSED, said)
        }
    }

    /// The fault the reader's `error` says, at the key `path`, of what `noun` names: classed by
    /// the error's kind, its words of the document's own kept and the refused value never read.
    pub(crate) fn of(error: &serde_saphyr::Error, path: Option<String>, noun: &str) -> Self {
        let error = error.without_snippet();
        let (code, said, nearest) = classified(error, noun);
        // An unknown key's path ends with the key, which its fault names itself.
        let path = match error {
            serde_saphyr::Error::SerdeUnknownField { field, .. } => {
                path.map(|path| parent(&path, field).to_owned())
            }
            _ => path,
        };
        Self {
            code,
            at: error.location().map(|at| Place {
                line: at.line(),
                column: at.column(),
            }),
            path: path.filter(|path| !path.is_empty() && path != "."),
            said,
            nearest,
        }
    }

    pub(crate) fn code(&self) -> Code {
        self.code
    }

    /// `origin:line:col: key.path: what is wrong; did you mean `key`?`, the parts known; the
    /// origin is where the text came from, a file's path or what its caller names it.
    pub(crate) fn render(&self, origin: Option<&str>) -> String {
        let mut rendered = String::new();
        if let Some(origin) = origin {
            write!(rendered, "{}:", text::shown(origin, ORIGIN_BYTES)).ok();
        }
        if let Some(at) = self.at {
            write!(rendered, "{}:{}:", at.line, at.column).ok();
        }
        if !rendered.is_empty() {
            rendered.push(' ');
        }
        if let Some(path) = &self.path {
            write!(rendered, "{}: ", text::shown(path, PATH_BYTES)).ok();
        }
        rendered.push_str(&self.said);
        if let Some(nearest) = self.nearest {
            write!(rendered, "; did you mean `{nearest}`?").ok();
        }
        rendered
    }
}

/// What `noun`, past the limits of depth or nodes, is told.
fn too_deep(noun: &str) -> String {
    format!("{noun} nests deeper, or holds more nodes, than it may")
}

/// The code of the reader's `error`, what to say of it and the nearest known key for an unknown
/// one, of what `noun` names: its kind and the words of the document's own, never its text,
/// which may quote a value.
fn classified(error: &serde_saphyr::Error, noun: &str) -> (Code, String, Option<&'static str>) {
    use serde_saphyr::Error as Read;
    match error {
        Read::SerdeUnknownField {
            field, expected, ..
        } => (
            codes::DOCUMENT_UNKNOWN_KEY,
            format!("unknown key `{}`", text::shown(field, KEY_BYTES)),
            nearest(field, expected.iter().copied()),
        ),
        Read::SerdeMissingField { field, .. } => (
            codes::DOCUMENT_MISSING_KEY,
            format!("the key `{field}` is missing"),
            None,
        ),
        Read::SerdeInvalidType { expected, .. } => (
            codes::DOCUMENT_INVALID,
            format!("its value is of the wrong kind: expected {expected}"),
            None,
        ),
        Read::SerdeInvalidValue { expected, .. } => (
            if expected == ONLY_VERSION {
                codes::DOCUMENT_VERSION
            } else {
                codes::DOCUMENT_INVALID
            },
            format!("its value is not one this place takes: expected {expected}"),
            None,
        ),
        Read::SerdeInvalidLength { expected, .. } => (
            codes::DOCUMENT_INVALID,
            format!("it holds the wrong number of items: expected {expected}"),
            None,
        ),
        Read::DuplicateMappingKey { .. } => (
            codes::DOCUMENT_DUPLICATE_KEY,
            "a key is given twice".to_owned(),
            None,
        ),
        Read::MultipleDocuments { .. } => (
            codes::DOCUMENT_REFUSED,
            "a second document is refused: a file holds one".to_owned(),
            None,
        ),
        Read::MergeKeyNotAllowed { .. } => (
            codes::DOCUMENT_REFUSED,
            "a merge key is refused: nothing is repeated by reference".to_owned(),
            None,
        ),
        Read::Budget { breach, .. } => {
            let (code, said) = breached(breach, noun);
            (code, said, None)
        }
        _ => (
            codes::DOCUMENT_SYNTAX,
            format!("{noun} is not YAML or JSON this reader takes"),
            None,
        ),
    }
}

/// The code of the reader's limit `breach`, and what to say of it, of what `noun` names.
fn breached(breach: &BudgetBreach, noun: &str) -> (Code, String) {
    match breach {
        BudgetBreach::Depth { .. } | BudgetBreach::Nodes { .. } => {
            (codes::DOCUMENT_TOO_DEEP, too_deep(noun))
        }
        BudgetBreach::ScalarBytes { .. } => (
            codes::DOCUMENT_TOO_LARGE,
            format!("{noun}'s text, unescaped, is larger than {DOCUMENT_BYTES} bytes"),
        ),
        _ => (
            codes::DOCUMENT_REFUSED,
            format!("{noun} repeats something by reference, which is refused"),
        ),
    }
}

/// `path` without its last key, `field`.
fn parent<'a>(path: &'a str, field: &str) -> &'a str {
    let parent = path.strip_suffix(field).unwrap_or(path);
    parent.strip_suffix('.').unwrap_or(parent)
}
