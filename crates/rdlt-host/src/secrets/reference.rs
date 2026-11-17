//! Secret references in a configuration's text values: found, checked and replaced.

use std::fmt::Write as _;

use rdlt_connector::text::shown;
use zeroize::Zeroizing;

use super::redact::maskable;
use super::{Redactions, SecretError, SecretFault, SecretKind, SecretReference, SecretResolver};
use crate::limits::SECRET_NAME_BYTES;

/// Bytes: bounds the field path an error names.
const FIELD_BYTES: usize = 256;

/// What is wrong with a secret reference.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReferenceFault {
    /// It has no closing brace.
    #[error("is not closed: a literal `${{` is written `$${{`")]
    Unclosed,
    /// It names no kind this host knows: `env`, `file` or `secret`.
    #[error("is of no known kind: `env`, `file` or `secret`")]
    Kind,
    /// Its name is empty, too long, or holds a control character.
    #[error("has no usable name")]
    Name,
    /// The text is not one reference and nothing else, where one is asked for.
    #[error("is not one reference and nothing else")]
    NotOne,
}

impl SecretReference {
    /// The reference `text` is, where it is one reference and nothing else: a value only a
    /// resolver may give, such as a credential, never written in a configuration itself.
    ///
    /// # Errors
    ///
    /// [`ReferenceFault::NotOne`] for text that holds no reference, more than one, or anything
    /// beside it; the fault of a malformed reference otherwise.
    pub fn parse(text: &str) -> Result<Self, ReferenceFault> {
        match pieces(text)?.as_slice() {
            [Piece::Reference(reference)] => Ok(reference.clone()),
            _ => Err(ReferenceFault::NotOne),
        }
    }
}

/// A piece of a text value: text as it is, or a reference to resolve.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Piece<'a> {
    /// Text of the value, borrowed: a copy would be left unwiped.
    Text(&'a str),
    Reference(SecretReference),
}

/// The pieces of `text`: each `${kind:name}` a reference, each `$${` a literal `${`.
pub(super) fn pieces(text: &str) -> Result<Vec<Piece<'_>>, ReferenceFault> {
    let (mut pieces, mut rest) = (Vec::new(), text);
    while let Some(at) = rest.find("${") {
        let after = &rest[at + 2..];
        if rest[..at].ends_with('$') {
            // `$${` is `${`: the text to its first `$`, and on from its brace.
            literal(&mut pieces, &rest[..at]);
            rest = &rest[at + 1..];
            continue;
        }
        literal(&mut pieces, &rest[..at]);
        let end = after.find('}').ok_or(ReferenceFault::Unclosed)?;
        let (kind, name) = after[..end].split_once(':').ok_or(ReferenceFault::Kind)?;
        let kind = match kind {
            "env" => SecretKind::Env,
            "file" => SecretKind::File,
            "secret" => SecretKind::Named,
            _ => return Err(ReferenceFault::Kind),
        };
        let unusable =
            name.is_empty() || name.len() > SECRET_NAME_BYTES || name.chars().any(char::is_control);
        if unusable {
            return Err(ReferenceFault::Name);
        }
        let name = name.to_owned();
        pieces.push(Piece::Reference(SecretReference { kind, name }));
        rest = &after[end + 1..];
    }
    literal(&mut pieces, rest);
    Ok(pieces)
}

/// Adds `text` to `pieces`, unless it is empty.
fn literal<'a>(pieces: &mut Vec<Piece<'a>>, text: &'a str) {
    if !text.is_empty() {
        pieces.push(Piece::Text(text));
    }
}

/// How many of `pieces` are references.
pub(super) fn count(pieces: &[Piece<'_>]) -> usize {
    let reference = |piece: &&Piece<'_>| matches!(piece, Piece::Reference(_));
    pieces.iter().filter(reference).count()
}

/// The text `pieces`, in `field`, make once `secrets` has resolved each reference, every
/// resolved value added to `redactions`; `None` for a text that stays as it is.
pub(super) async fn resolved(
    pieces: &[Piece<'_>],
    field: &str,
    secrets: &dyn SecretResolver,
    redactions: &Redactions,
) -> Option<Result<Zeroizing<String>, SecretError>> {
    // A text with no reference is one piece as it is, or none; a literal `${` makes more.
    if count(pieces) == 0 && pieces.len() < 2 {
        return None;
    }
    let mut secrets_sent = Vec::with_capacity(count(pieces));
    for piece in pieces {
        let Piece::Reference(reference) = piece else {
            continue;
        };
        let secret = match secrets.resolve(reference).await {
            Ok(secret) => secret,
            Err(fault) => return Some(Err(unresolved(field, reference.kind, fault))),
        };
        let value = secret.expose();
        if !value.is_empty() && !maskable(value) {
            let (field, kind) = (field.to_owned(), reference.kind);
            return Some(Err(SecretError::Unredactable { field, kind }));
        }
        redactions.add(value);
        secrets_sent.push(secret);
    }
    let length = |piece: &Piece<'_>| match piece {
        Piece::Text(text) => text.len(),
        Piece::Reference(_) => 0,
    };
    let resolved = secrets_sent.iter().map(|secret| secret.expose().len());
    let length = pieces.iter().map(length).sum::<usize>() + resolved.sum::<usize>();
    // Room for the whole text at once: a buffer that grows leaves copies behind.
    let mut text = Zeroizing::new(String::with_capacity(length));
    let mut values = secrets_sent.iter().map(|secret| secret.expose().as_str());
    for piece in pieces {
        match piece {
            Piece::Text(literal) => text.push_str(literal),
            Piece::Reference(_) => text.push_str(values.next().unwrap_or_default()),
        }
    }
    Some(Ok(text))
}

/// The error of a reference of `kind`, in `field`, that did not resolve for `fault`.
fn unresolved(field: &str, kind: SecretKind, fault: SecretFault) -> SecretError {
    let field = field.to_owned();
    match fault {
        SecretFault::Refused => SecretError::Refused { field, kind },
        source => SecretError::Unresolved {
            field,
            kind,
            source,
        },
    }
}

/// Every text value of `document`, with the path of keys and indexes that leads to it, shown
/// and bounded as an error names it; `path` is the path so far.
pub(super) fn texts<'a>(
    document: &'a mut serde_json::Value,
    path: &mut String,
    texts: &mut Vec<(String, &'a mut String)>,
) {
    let length = path.len();
    match document {
        serde_json::Value::String(text) => {
            let field = if path.is_empty() { "." } else { path.as_str() };
            texts.push((shown(field, FIELD_BYTES), text));
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                // Writing to a `String` cannot fail.
                write!(path, "[{index}]").ok();
                self::texts(item, path, texts);
                path.truncate(length);
            }
        }
        serde_json::Value::Object(fields) => {
            for (key, value) in fields.iter_mut() {
                if !path.is_empty() {
                    path.push('.');
                }
                path.push_str(key);
                self::texts(value, path, texts);
                path.truncate(length);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}
