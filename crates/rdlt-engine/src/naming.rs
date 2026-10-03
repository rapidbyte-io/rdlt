//! Destination identifiers for tables and columns (spec §8.6).
//!
//! An identifier is the source name folded and cleaned under the destination's rules. When that is
//! taken — by another column, a metadata column or a reserved word — a hash of the exact source
//! path is appended, so two source paths never share an identifier and a path keeps its identifier
//! whatever else arrives. Assigned identifiers are recorded in an append-only name map and never
//! move.
//!
//! Every metadata column the engine writes has one identifier under a destination's rules, whether
//! or not a table has the column yet, and no source column takes one: a table keeps its names when
//! its stream starts to merge, normalize or keep history. A source column asking for a metadata
//! column's name is refused.

pub(crate) mod recorded;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rdlt_connector::{
    ColumnKey, DELETED_AT_COLUMN, ID_COLUMN, IDX_COLUMN, IS_CURRENT_COLUMN, IdentifierCase,
    IdentifierChars, IdentifierRules, LOAD_ID_COLUMN, LOADED_AT_COLUMN, NameMap, OP_COLUMN,
    PARENT_ID_COLUMN, ROOT_ID_COLUMN, ROW_HASH_COLUMN, SEQ_COLUMN, TablePath, UNCHANGED_COLUMN,
    VALID_FROM_COLUMN, VALID_TO_COLUMN,
};

use crate::error::{Error, ErrorKind};

/// Base32 digits of the hash that tells colliding identifiers apart: 13 hold its 64 bits.
const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// Hash digits appended at first; more are added one at a time while the result is taken.
const HASH_DIGITS: usize = 6;

/// Every metadata column the engine writes, in the order their identifiers are assigned: each
/// takes the first identifier the ones before it leave free.
const METADATA: [&str; 14] = [
    LOAD_ID_COLUMN,
    LOADED_AT_COLUMN,
    SEQ_COLUMN,
    OP_COLUMN,
    UNCHANGED_COLUMN,
    DELETED_AT_COLUMN,
    VALID_FROM_COLUMN,
    VALID_TO_COLUMN,
    IS_CURRENT_COLUMN,
    ROW_HASH_COLUMN,
    ID_COLUMN,
    PARENT_ID_COLUMN,
    ROOT_ID_COLUMN,
    IDX_COLUMN,
];

/// The code of the error refusing a source column that asks for a metadata column's name.
pub(crate) const COLUMN_NAME_RESERVED: &str = "column_name_reserved";

/// Assigns identifiers under one destination's rules.
#[derive(Clone, Debug)]
pub(crate) struct Naming {
    rules: Arc<Folded>,
    /// When set, every identifier carries its hash, seeded with this salt, even one that is free
    /// without it.
    salt: Option<u64>,
}

/// A destination's rules, its reserved words and table prefixes folded as identifiers are, and
/// the identifiers of the engine's metadata columns under them.
#[derive(Debug)]
struct Folded {
    rules: IdentifierRules,
    reserved: BTreeSet<String>,
    prefixes: Vec<String>,
    /// Each metadata column's identifier, by the column's name.
    metadata: BTreeMap<&'static str, String>,
    /// Each metadata column's name folded and cleaned, which no source column may ask for.
    claimed: BTreeSet<String>,
}

impl Naming {
    /// Naming under `rules` as a destination declares them.
    ///
    /// # Errors
    ///
    /// Rules beyond their limits are `capabilities_invalid`, and rules that leave a metadata
    /// column no identifier `identifier_exhausted`, Destination errors.
    pub(crate) fn checked(rules: &IdentifierRules) -> Result<Self, Error> {
        rules.validate().map_err(|invalid| {
            Error::new(
                ErrorKind::Destination,
                format!("the destination's identifier rules are refused: {invalid}"),
            )
            .with_code("capabilities_invalid")
        })?;
        Self::build(rules.clone())
    }

    /// Naming under `rules`, which tests hold within their limits.
    #[cfg(test)]
    pub(crate) fn new(rules: IdentifierRules) -> Self {
        Self::build(rules).expect("the rules leave every metadata column an identifier")
    }

    /// Naming under `rules`, with the identifier of every metadata column assigned.
    fn build(rules: IdentifierRules) -> Result<Self, Error> {
        let fold = |name: &str| fold(rules.case, name);
        let reserved = rules.reserved.iter().map(|word| fold(word)).collect();
        let prefixes = rules
            .reserved_table_prefixes
            .iter()
            .map(|prefix| fold(prefix))
            .collect();
        let mut naming = Self {
            rules: Arc::new(Folded {
                rules,
                reserved,
                prefixes,
                metadata: BTreeMap::new(),
                claimed: BTreeSet::new(),
            }),
            salt: None,
        };
        let mut metadata = BTreeMap::new();
        let mut taken = BTreeSet::new();
        for column in METADATA {
            let mut bytes = b"metadata".to_vec();
            push_segment(&mut bytes, column);
            let name = naming.identifier(column, &bytes, |candidate| taken.contains(candidate))?;
            taken.insert(name.clone());
            metadata.insert(column, name);
        }
        let claimed = METADATA.iter().map(|column| naming.clean(column)).collect();
        let folded = Arc::get_mut(&mut naming.rules).expect("the rules are not shared yet");
        folded.metadata = metadata;
        folded.claimed = claimed;
        Ok(naming)
    }

    /// Whether these rules could have given a table `name`: cleaned, within the length limit,
    /// and under no reserved prefix.
    pub(crate) fn admits_table(&self, name: &str) -> bool {
        self.admits(name) && !self.reserved_prefix(name)
    }

    /// Whether `name` starts with a prefix the destination reserves for its own tables.
    pub(crate) fn reserved_prefix(&self, name: &str) -> bool {
        let prefixes = &self.rules.prefixes;
        prefixes
            .iter()
            .any(|prefix| name.starts_with(prefix.as_str()))
    }

    /// Whether these rules could have given a column `name`: cleaned and within the length
    /// limit.
    pub(crate) fn admits(&self, name: &str) -> bool {
        name.len() <= usize::from(self.rules.rules.max_len.get()) && self.clean(name) == name
    }

    /// The same rules, appending a hash seeded with `salt` to every identifier.
    ///
    /// A table falls back to it when the destination already holds a column that an attempt which
    /// never committed left behind. Each salt names around the columns the ones before it left.
    pub(crate) fn hashing(&self, salt: u64) -> Self {
        Self {
            rules: Arc::clone(&self.rules),
            salt: Some(salt),
        }
    }

    /// Maps every key of `keys` that `names` lacks to a free identifier, never a metadata
    /// column's.
    ///
    /// Keys are assigned in their sorted order, so the result does not depend on the order they
    /// arrived in.
    ///
    /// # Errors
    ///
    /// A key asking for the name of a metadata column, folded and cleaned as the rules make it,
    /// is `column_name_reserved`, a Schema error.
    pub(crate) fn assign_columns(
        &self,
        names: &mut NameMap,
        keys: &BTreeSet<ColumnKey>,
    ) -> Result<(), Error> {
        for key in keys {
            if names.get(key).is_some() {
                continue;
            }
            let candidate = candidate(key);
            if self.rules.claimed.contains(&self.clean(&candidate)) {
                return Err(Error::schema(format!(
                    "column {} asks for the name of a metadata column the engine writes",
                    key.column()
                ))
                .with_code(COLUMN_NAME_RESERVED));
            }
            let taken = |name: &str| names.owner(name).is_some() || self.is_metadata(name);
            let name = self.identifier(&candidate, &path_bytes(key), taken)?;
            names.insert(key.clone(), name).map_err(|conflict| {
                Error::internal(format!("assigning a column identifier: {conflict}"))
            })?;
        }
        Ok(())
    }

    /// A free identifier for the table at `path`, never one of `taken`, and one the rules admit
    /// as a table's (see [`Naming::admits_table`]).
    ///
    /// A name that starts with a prefix the destination reserves for its own tables is prefixed
    /// with `_` until it no longer does, and a name with its hash appended is held to the
    /// prefixes as well: one that falls under a prefix is passed over, then the name escaped once
    /// more, so the result is the same for the same name and rules.
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
        // A prefix traps one count of leading `_`, or every count from one on: a free count, where
        // there is one, is at most the number of prefixes, which is bounded.
        let cleaned = self.clean(&join(&segments));
        let prefixes = &self.rules.prefixes;
        for escapes in 0..=prefixes.len() {
            // The candidate starts with a prefix whose first bytes, as many as there are
            // escapes, are each `_`, and whose rest the cleaned name starts with.
            let starts = |prefix: &String| {
                let escaped = prefix.len().min(escapes);
                prefix.as_bytes()[..escaped]
                    .iter()
                    .all(|byte| *byte == b'_')
                    && cleaned.starts_with(&prefix[escaped..])
            };
            if prefixes.iter().any(starts) {
                continue;
            }
            let candidate = format!("{}{cleaned}", "_".repeat(escapes));
            let unusable = |name: &str| taken.contains(name) || self.reserved_prefix(name);
            if let Ok(name) = self.identifier(&candidate, &bytes, unusable) {
                return Ok(name);
            }
        }
        Err(Error::new(
            ErrorKind::Destination,
            format!("no identifier for table {path} avoids the destination's reserved prefixes"),
        )
        .with_code("identifier_exhausted"))
    }

    /// The identifier of the metadata column `column`, one of the engine's.
    pub(crate) fn metadata(&self, column: &str) -> Arc<str> {
        let name = self.rules.metadata.get(column);
        Arc::from(
            name.expect("every metadata column has an identifier")
                .as_str(),
        )
    }

    /// Whether `name` is the identifier of a metadata column.
    pub(crate) fn is_metadata(&self, name: &str) -> bool {
        self.rules
            .metadata
            .values()
            .any(|metadata| metadata == name)
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
        let max = usize::from(self.rules.rules.max_len.get());
        let first = truncate(&base, max);
        if self.salt.is_none() && !self.unusable(&first, &taken) {
            return Ok(first);
        }
        let seed = self.salt.unwrap_or(0);
        let hash = self.fold(&base32(xxhash_rust::xxh3::xxh3_64_with_seed(source, seed)));
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
        taken(name) || self.rules.reserved.contains(name)
    }

    /// `name` folded and cleaned: disallowed characters become `_`, and an identifier that would
    /// be empty or, for ASCII identifiers, start with a digit gets a leading `_`.
    fn clean(&self, name: &str) -> String {
        let folded = self.fold(name);
        let mut cleaned: String = folded
            .chars()
            .map(|c| match self.rules.rules.chars {
                IdentifierChars::AsciiWord if c.is_ascii_alphanumeric() || c == '_' => c,
                IdentifierChars::Any if !rdlt_connector::text::deceives(c) => c,
                _ => '_',
            })
            .collect();
        let leading_digit = self.rules.rules.chars == IdentifierChars::AsciiWord
            && cleaned.starts_with(|c: char| c.is_ascii_digit());
        if cleaned.is_empty() || leading_digit {
            cleaned.insert(0, '_');
        }
        cleaned
    }

    fn fold(&self, name: &str) -> String {
        fold(self.rules.rules.case, name)
    }
}

/// `name` folded to `case`.
fn fold(case: IdentifierCase, name: &str) -> String {
    match case {
        IdentifierCase::Preserve => name.to_owned(),
        IdentifierCase::Lower => name.to_lowercase(),
        IdentifierCase::Upper => name.to_uppercase(),
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
