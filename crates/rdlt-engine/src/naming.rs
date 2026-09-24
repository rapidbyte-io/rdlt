//! Destination identifiers for tables and columns (spec §8.6).
//!
//! An identifier is the source name folded and cleaned under the destination's rules. When that is
//! taken — by another column, a metadata column or a reserved word — a hash of the exact source
//! path is appended, so two source paths never share an identifier and a path keeps its identifier
//! whatever else arrives. Assigned identifiers are recorded in an append-only name map and never
//! move.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use rdlt_connector::{
    ColumnKey, IdentifierCase, IdentifierChars, IdentifierRules, NameMap, TablePath,
};

use crate::error::{Error, ErrorKind};

/// Base32 digits of the hash that tells colliding identifiers apart: 13 hold its 64 bits.
const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// Hash digits appended at first; more are added one at a time while the result is taken.
const HASH_DIGITS: usize = 6;

/// Assigns identifiers under one destination's rules.
#[derive(Clone, Debug)]
pub(crate) struct Naming {
    rules: IdentifierRules,
    /// Whether every identifier carries its hash, even one that is free without it.
    hashed: bool,
}

impl Naming {
    pub(crate) fn new(rules: IdentifierRules) -> Self {
        Self {
            rules,
            hashed: false,
        }
    }

    /// The same rules, appending the hash to every identifier: what a table falls back to when
    /// the destination already holds a column an attempt that never committed left behind.
    pub(crate) fn hashing(&self) -> Self {
        Self {
            rules: self.rules.clone(),
            hashed: true,
        }
    }

    /// Maps every key of `keys` that `names` lacks to a free identifier, never one in
    /// `reserved` (the metadata columns).
    ///
    /// Keys are assigned in their sorted order, so the result does not depend on the order they
    /// arrived in.
    pub(crate) fn assign_columns(
        &self,
        names: &mut NameMap,
        keys: &BTreeSet<ColumnKey>,
        reserved: &[&str],
    ) -> Result<(), Error> {
        for key in keys {
            if names.get(key).is_some() {
                continue;
            }
            let taken = |name: &str| names.owner(name).is_some() || reserved.contains(&name);
            let name = self.identifier(&candidate(key), &path_bytes(key), taken)?;
            names.insert(key.clone(), name).map_err(|conflict| {
                Error::internal(format!("assigning a column identifier: {conflict}"))
            })?;
        }
        Ok(())
    }

    /// A free identifier for the table at `path`, never one of `taken`.
    pub(crate) fn table(
        &self,
        path: &TablePath,
        taken: &BTreeSet<String>,
    ) -> Result<String, Error> {
        let segments: Vec<&str> = path.segments().collect();
        let mut bytes = b"table".to_vec();
        for segment in &segments {
            push_segment(&mut bytes, segment);
        }
        self.identifier(&join(&segments), &bytes, |name| taken.contains(name))
    }

    /// The identifier of the metadata column `name`, never one of `taken`.
    pub(crate) fn metadata(&self, name: &str, taken: &BTreeSet<String>) -> Result<String, Error> {
        let mut bytes = b"metadata".to_vec();
        push_segment(&mut bytes, name);
        self.identifier(name, &bytes, |candidate| taken.contains(candidate))
    }

    /// `candidate` cleaned under the rules if that is free, and otherwise with a hash of
    /// `source` appended.
    fn identifier(
        &self,
        candidate: &str,
        source: &[u8],
        taken: impl Fn(&str) -> bool,
    ) -> Result<String, Error> {
        let base = self.clean(candidate);
        let max = usize::from(self.rules.max_len.get());
        let first = truncate(&base, max);
        if !self.hashed && !self.unusable(&first, &taken) {
            return Ok(first);
        }
        let hash = self.fold(&base32(xxhash_rust::xxh3::xxh3_64(source)));
        for digits in HASH_DIGITS..=hash.len() {
            let suffix = format!("_{}", &hash[..digits]);
            let room = max.saturating_sub(suffix.len());
            let name = truncate(&format!("{}{suffix}", truncate(&base, room)), max);
            if !self.unusable(&name, &taken) {
                return Ok(name);
            }
        }
        Err(Error::new(
            ErrorKind::Destination,
            format!("no free identifier for {candidate:?} under the destination's rules"),
        )
        .with_code("identifier_exhausted"))
    }

    /// Whether `name` may not be used: taken, or a reserved word.
    fn unusable(&self, name: &str, taken: &impl Fn(&str) -> bool) -> bool {
        taken(name)
            || self
                .rules
                .reserved
                .iter()
                .any(|word| self.fold(word) == name)
    }

    /// `name` folded and cleaned: disallowed characters become `_`, and an identifier that would
    /// be empty or, for ASCII identifiers, start with a digit gets a leading `_`.
    fn clean(&self, name: &str) -> String {
        let folded = self.fold(name);
        let mut cleaned: String = folded
            .chars()
            .map(|c| match self.rules.chars {
                IdentifierChars::AsciiWord if c.is_ascii_alphanumeric() || c == '_' => c,
                IdentifierChars::Any if !c.is_control() => c,
                _ => '_',
            })
            .collect();
        let leading_digit = self.rules.chars == IdentifierChars::AsciiWord
            && cleaned.starts_with(|c: char| c.is_ascii_digit());
        if cleaned.is_empty() || leading_digit {
            cleaned.insert(0, '_');
        }
        cleaned
    }

    fn fold(&self, name: &str) -> String {
        match self.rules.case {
            IdentifierCase::Preserve => name.to_owned(),
            IdentifierCase::Lower => name.to_lowercase(),
            IdentifierCase::Upper => name.to_uppercase(),
        }
    }
}

/// The name a column asks for: its path's segments joined by `__`, and for a variant the kind's
/// name after another `__`.
fn candidate(key: &ColumnKey) -> String {
    let segments: Vec<&str> = key.column().segments().collect();
    let joined = join(&segments);
    match key {
        ColumnKey::Source(_) => joined,
        ColumnKey::Variant { kind, .. } => format!("{joined}__{}", kind_name(*kind)),
    }
}

/// Segments joined by `__`; within a nested path, a literal `__` in a segment becomes `_x5f_`, so
/// `a__b` as one name and `a.b` as nesting never ask for the same identifier.
fn join(segments: &[&str]) -> String {
    if let [only] = segments {
        return (*only).to_owned();
    }
    segments
        .iter()
        .map(|segment| segment.replace("__", "_x5f_"))
        .collect::<Vec<_>>()
        .join("__")
}

/// The exact source identity of a column, hashed when its identifier collides.
fn path_bytes(key: &ColumnKey) -> Vec<u8> {
    let mut bytes = match key {
        ColumnKey::Source(_) => b"source".to_vec(),
        ColumnKey::Variant { kind, .. } => format!("variant {}", kind_name(*kind)).into_bytes(),
    };
    for segment in key.column().segments() {
        push_segment(&mut bytes, segment);
    }
    bytes
}

/// Appends `segment` length-prefixed, so no two paths encode alike.
fn push_segment(bytes: &mut Vec<u8>, segment: &str) {
    let length = u64::try_from(segment.len()).unwrap_or(u64::MAX);
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(segment.as_bytes());
}

/// The lower-case name of a kind, as variant identifiers spell it.
pub(crate) fn kind_name(kind: rdlt_connector::TypeKind) -> String {
    format!("{kind:?}").to_lowercase()
}

/// `value` in base32, most significant digit first.
fn base32(value: u64) -> String {
    (0..13)
        .rev()
        .map(|digit| {
            let index = (value >> (digit * 5)) & 31;
            char::from(ALPHABET[usize::try_from(index).unwrap_or(0)])
        })
        .collect()
}

/// The longest prefix of `name` of at most `max` bytes that ends on a character boundary.
fn truncate(name: &str, max: usize) -> String {
    let mut end = max.min(name.len());
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    name[..end].to_owned()
}
