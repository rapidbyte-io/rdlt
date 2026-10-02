//! Secret references in a configuration's text values: found, checked and replaced.

use std::fmt::Write as _;

use rdlt_connector::text::shown;
use zeroize::Zeroizing;

use super::{Redactions, SecretFault, SecretKind, SecretReference, SecretResolver};
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
}

/// A piece of a text value: text as it is, or a reference to resolve.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Piece {
    Text(String),
    Reference(SecretReference),
}

/// The pieces of `text`: each `${kind:name}` a reference, each `$${` a literal `${`.
pub(super) fn pieces(text: &str) -> Result<Vec<Piece>, ReferenceFault> {
    let (mut pieces, mut literal, mut rest) = (Vec::new(), String::new(), text);
    while let Some(at) = rest.find("${") {
        let after = &rest[at + 2..];
        if rest[..at].ends_with('$') {
            literal.push_str(&rest[..at - 1]);
            literal.push_str("${");
            rest = after;
            continue;
        }
        literal.push_str(&rest[..at]);
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
        if !literal.is_empty() {
            pieces.push(Piece::Text(std::mem::take(&mut literal)));
        }
        let name = name.to_owned();
        pieces.push(Piece::Reference(SecretReference { kind, name }));
        rest = &after[end + 1..];
    }
    literal.push_str(rest);
    if !literal.is_empty() {
        pieces.push(Piece::Text(literal));
    }
    Ok(pieces)
}

/// How many of `pieces` are references.
pub(super) fn count(pieces: &[Piece]) -> usize {
    let reference = |piece: &&Piece| matches!(piece, Piece::Reference(_));
    pieces.iter().filter(reference).count()
}

/// The text `pieces` of `original` make once `secrets` has resolved each reference, every
/// resolved value added to `redactions`; `None` for a text that stays as it is.
pub(super) async fn resolved(
    pieces: &[Piece],
    original: &str,
    secrets: &dyn SecretResolver,
    redactions: &Redactions,
) -> Option<Result<Zeroizing<String>, (SecretKind, SecretFault)>> {
    if count(pieces) == 0 {
        return match pieces {
            [Piece::Text(text)] if text != original => Some(Ok(Zeroizing::new(text.clone()))),
            _ => None,
        };
    }
    let mut values = Vec::with_capacity(pieces.len());
    for piece in pieces {
        match piece {
            Piece::Text(text) => values.push(Zeroizing::new(text.clone())),
            Piece::Reference(reference) => match secrets.resolve(reference).await {
                Ok(secret) => {
                    redactions.add(secret.expose());
                    values.push(Zeroizing::new(secret.expose().clone()));
                }
                Err(fault) => return Some(Err((reference.kind, fault))),
            },
        }
    }
    let length = values.iter().map(|value| value.len()).sum();
    let mut text = Zeroizing::new(String::with_capacity(length));
    for value in &values {
        text.push_str(value);
    }
    Some(Ok(text))
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
