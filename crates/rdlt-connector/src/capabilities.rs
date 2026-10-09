//! What a destination can store.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

use crate::types::TypeKind;

/// The longest identifier [`Capabilities::minimal`] allows, in bytes.
const MINIMAL_IDENTIFIER_LEN: NonZeroU16 = NonZeroU16::new(63).expect("63 is non-zero");

/// The write modes a destination supports.
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag is an independent capability"
)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WriteModes {
    /// Insert every row.
    pub append: bool,
    /// Atomically swap in a new generation of the table.
    pub replace: bool,
    /// Upsert by key.
    pub merge: bool,
    /// Keep every version of each key (SCD2).
    pub history: bool,
}

/// The delete modes a destination supports for change streams.
///
/// A destination declaring none merges no change stream, since merging one needs the seq guard and
/// the change columns (`MergeKey::changes`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteModes {
    /// Remove the row.
    pub hard: bool,
    /// Mark the row deleted and keep it.
    pub soft: bool,
}

/// Which nested values a destination stores natively.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NestedSupport {
    /// Struct columns.
    pub structs: bool,
    /// List columns.
    pub lists: bool,
    /// A JSON column type.
    pub json: bool,
}

/// The schema changes a destination applies in place.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaChanges {
    /// Adding a nullable column.
    pub add_column: bool,
    /// Widening a column from the first kind to the second.
    pub widenings: BTreeSet<(TypeKind, TypeKind)>,
}

impl SchemaChanges {
    /// Adding columns and every widening the type lattice makes, for a destination that stores
    /// any column type and can change it in place.
    pub fn all() -> Self {
        use TypeKind as K;
        let integers = [K::Int8, K::Int16, K::Int32, K::Int64];
        let mut widenings = BTreeSet::new();
        for (index, from) in integers.iter().enumerate() {
            for to in &integers[index + 1..] {
                widenings.insert((*from, *to));
            }
            widenings.insert((*from, K::Decimal));
            if *from != K::Int64 {
                widenings.insert((*from, K::Float64));
            }
        }
        widenings.extend([
            (K::Float32, K::Float64),
            (K::Decimal, K::Decimal),
            (K::Date, K::Timestamp),
            (K::Timestamp, K::Timestamp),
            (K::Time, K::Time),
            (K::Duration, K::Duration),
            (K::Struct, K::Struct),
            (K::List, K::List),
        ]);
        Self {
            add_column: true,
            widenings,
        }
    }

    /// Whether a column of kind `from` can become kind `to` in place.
    pub fn widens(&self, from: TypeKind, to: TypeKind) -> bool {
        self.widenings.contains(&(from, to))
    }
}

/// How a destination folds the case of identifiers.
///
/// The engine gives two columns or tables one identifier where their names fold alike, and keeps
/// apart names that fold apart: a destination whose identifiers compare alike more widely, by
/// Unicode's case folding (`straße` and `strasse`), by normalization (`é` composed and
/// decomposed) or by ASCII case under `Preserve`, declares narrower characters
/// ([`IdentifierChars::AsciiWord`]) or fails `D-NAMES`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierCase {
    /// Keeps case: names compare as written.
    Preserve,
    /// Folds to lower case as Unicode's lower-case mapping does (`str::to_lowercase`), and no
    /// further: `Kept` is `kept`, `ẞ` is `ß`, `ß` stays `ß`.
    Lower,
    /// Folds to upper case as Unicode's upper-case mapping does (`str::to_uppercase`): `kept` is
    /// `KEPT`, `ß` is `SS`.
    Upper,
}

/// Which characters a destination allows in an identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierChars {
    /// Any character except controls.
    Any,
    /// ASCII letters, digits and `_`.
    AsciiWord,
}

/// The identifier rules the engine's naming follows for a destination.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdentifierRules {
    /// Case folding.
    pub case: IdentifierCase,
    /// Longest identifier, in bytes.
    pub max_len: NonZeroU16,
    /// Allowed characters.
    pub chars: IdentifierChars,
    /// Words that may not be identifiers, compared after case folding.
    pub reserved: BTreeSet<String>,
    /// Prefixes a table identifier may not start with, compared after case folding: names the
    /// destination keeps for its own tables.
    #[serde(default)]
    pub reserved_table_prefixes: BTreeSet<String>,
}

/// Why identifier rules are refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidRules {
    /// The longest identifier is shorter than [`MIN_IDENTIFIER_LEN`](crate::limits::MIN_IDENTIFIER_LEN).
    #[error("identifiers are shorter than the least length an identifier holds")]
    TooShort,
    /// More words are reserved than [`MAX_RESERVED_WORDS`](crate::limits::MAX_RESERVED_WORDS).
    #[error("more words are reserved than the limit")]
    TooManyWords,
    /// More table prefixes are reserved than
    /// [`MAX_RESERVED_PREFIXES`](crate::limits::MAX_RESERVED_PREFIXES).
    #[error("more table prefixes are reserved than the limit")]
    TooManyPrefixes,
    /// A reserved word is empty or longer than
    /// [`MAX_RESERVED_BYTES`](crate::limits::MAX_RESERVED_BYTES).
    #[error("a reserved word is empty or beyond the limit of its length")]
    Word,
    /// A reserved table prefix is empty, which no table name avoids, or longer than
    /// [`MAX_RESERVED_BYTES`](crate::limits::MAX_RESERVED_BYTES).
    #[error("a reserved table prefix is empty or beyond the limit of its length")]
    Prefix,
}

impl IdentifierRules {
    /// Checks the rules against the limits a destination's rules have.
    ///
    /// # Errors
    ///
    /// The first [`InvalidRules`] the rules are.
    pub fn validate(&self) -> Result<(), InvalidRules> {
        use crate::limits::{
            MAX_RESERVED_BYTES, MAX_RESERVED_PREFIXES, MAX_RESERVED_WORDS, MIN_IDENTIFIER_LEN,
        };
        let sized = |text: &String| (1..=MAX_RESERVED_BYTES).contains(&text.len());
        if self.max_len.get() < MIN_IDENTIFIER_LEN {
            Err(InvalidRules::TooShort)
        } else if self.reserved.len() > MAX_RESERVED_WORDS {
            Err(InvalidRules::TooManyWords)
        } else if self.reserved_table_prefixes.len() > MAX_RESERVED_PREFIXES {
            Err(InvalidRules::TooManyPrefixes)
        } else if !self.reserved.iter().all(sized) {
            Err(InvalidRules::Word)
        } else if !self.reserved_table_prefixes.iter().all(sized) {
            Err(InvalidRules::Prefix)
        } else {
            Ok(())
        }
    }
}

/// Everything the engine needs to know about a destination before writing to it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Supported write modes.
    pub write_modes: WriteModes,
    /// Supported delete modes.
    pub delete_modes: DeleteModes,
    /// Whether updates may leave flagged columns unchanged.
    pub partial_updates: bool,
    /// Whether it merges change streams as [`MergeKey::changes`](crate::MergeKey::changes) says:
    /// each row applied in sequence order only past the row it holds, as an insert, update,
    /// delete or truncate.
    ///
    /// A destination that does not would upsert change rows as data, so the engine refuses to
    /// merge a change stream into it.
    #[serde(default)]
    pub merge_changes: bool,
    /// Whether it drops the tables a commit's
    /// [`drop_tables`](crate::CommitMeta::drop_tables) names, atomically with the commit.
    ///
    /// A destination that does not would leave them in place, so the engine refuses to reset a
    /// stream's tables into it.
    #[serde(default)]
    pub drop_tables: bool,
    /// Nested values stored natively.
    pub nested: NestedSupport,
    /// Logical types stored natively; others are lowered by the engine.
    pub types: BTreeSet<TypeKind>,
    /// Schema changes applied in place.
    pub schema_changes: SchemaChanges,
    /// Identifier rules.
    pub identifiers: IdentifierRules,
    /// Writers the engine may run at once.
    pub max_parallel_writers: NonZeroU16,
}

impl Capabilities {
    /// An append-only destination for scalar types that adds columns, with case-preserving
    /// identifiers of up to 63 ASCII word characters and one writer.
    pub fn minimal() -> Self {
        use TypeKind as K;
        Self {
            write_modes: WriteModes {
                append: true,
                ..WriteModes::default()
            },
            delete_modes: DeleteModes::default(),
            partial_updates: false,
            merge_changes: false,
            drop_tables: false,
            nested: NestedSupport::default(),
            types: BTreeSet::from([
                K::Bool,
                K::Int8,
                K::Int16,
                K::Int32,
                K::Int64,
                K::Float32,
                K::Float64,
                K::Decimal,
                K::Utf8,
                K::Binary,
                K::Date,
                K::Time,
                K::Timestamp,
                K::Duration,
            ]),
            schema_changes: SchemaChanges {
                add_column: true,
                widenings: BTreeSet::new(),
            },
            identifiers: IdentifierRules {
                case: IdentifierCase::Preserve,
                max_len: MINIMAL_IDENTIFIER_LEN,
                chars: IdentifierChars::AsciiWord,
                reserved: BTreeSet::new(),
                reserved_table_prefixes: BTreeSet::new(),
            },
            max_parallel_writers: NonZeroU16::MIN,
        }
    }
}
