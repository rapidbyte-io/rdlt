//! What a destination can store and how it commits.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::num::NonZeroU16;

use serde::{Deserialize, Serialize};

use crate::types::TypeKind;

/// The longest identifier [`Capabilities::minimal`] allows, in bytes.
const MINIMAL_IDENTIFIER_LEN: NonZeroU16 = NonZeroU16::new(63).expect("63 is non-zero");

/// How a destination publishes a commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitKind {
    /// In one database transaction.
    Transactional,
    /// With one conditional write of a manifest that lists published files and state.
    Manifest,
}

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierCase {
    /// Keeps case.
    Preserve,
    /// Folds to lower case.
    Lower,
    /// Folds to upper case.
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

/// Everything the engine needs to know about a destination before writing to it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// How commits are published.
    pub commit: CommitKind,
    /// Supported write modes.
    pub write_modes: WriteModes,
    /// Supported delete modes.
    pub delete_modes: DeleteModes,
    /// Whether updates may leave flagged columns unchanged.
    pub partial_updates: bool,
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
    /// The batch size the destination writes best, in bytes.
    pub preferred_batch_bytes: Option<u64>,
}

impl Capabilities {
    /// A transactional, append-only destination for scalar types that adds columns, with
    /// case-preserving identifiers of up to 63 ASCII word characters and one writer.
    pub fn minimal() -> Self {
        use TypeKind as K;
        Self {
            commit: CommitKind::Transactional,
            write_modes: WriteModes {
                append: true,
                ..WriteModes::default()
            },
            delete_modes: DeleteModes::default(),
            partial_updates: false,
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
            preferred_batch_bytes: None,
        }
    }
}
