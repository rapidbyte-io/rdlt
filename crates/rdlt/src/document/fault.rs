//! What is wrong with a document: its code, where, the key path, and the nearest known key for an
//! unknown one; never a value, which may be a secret.

use std::fmt::Write as _;

use rdlt_connector::text;
use rdlt_engine::Code;

use crate::codes;
use crate::limits::{DOCUMENT_BYTES, ORIGIN_BYTES, PATH_BYTES};

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

/// What is wrong with a document.
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

    pub(crate) fn too_large() -> Self {
        let said = format!("the document is larger than {DOCUMENT_BYTES} bytes");
        Self::new(codes::DOCUMENT_TOO_LARGE, said)
    }

    pub(crate) fn too_deep(at: Place) -> Self {
        Self {
            at: Some(at),
            ..Self::new(codes::DOCUMENT_TOO_DEEP, TOO_DEEP)
        }
    }

    pub(crate) fn refused(refused: Refused, at: Place) -> Self {
        let said = match refused {
            Refused::Anchor => "an anchor is refused: a document repeats nothing by reference",
            Refused::Alias => "an alias is refused: a document repeats nothing by reference",
            Refused::Tag => "a tag is refused: write the value without it",
            Refused::Document => "a second document is refused: a file holds one",
        };
        Self {
            at: Some(at),
            ..Self::new(codes::DOCUMENT_REFUSED, said)
        }
    }

    /// The fault the reader's `error` says, at the key `path`.
    pub(crate) fn of(error: &serde_saphyr::Error, path: Option<String>) -> Self {
        let error = error.without_snippet();
        let mut fault = Self::new(codes::DOCUMENT_SYNTAX, SYNTAX);
        fault.at = error.location().map(|at| Place {
            line: at.line(),
            column: at.column(),
        });
        fault.path = path.filter(|path| !path.is_empty() && path != ".");
        fault
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

const SYNTAX: &str = "the document is not YAML or JSON this reader takes";

const TOO_DEEP: &str = "the document nests deeper, or holds more nodes, than a document may";
