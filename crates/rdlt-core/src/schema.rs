//! Versioned table schemas, evolution deltas, and the identifier vocabulary
//! every side of a schema shares.
//!
//! `SchemaHash` is a content hash over a schema's canonical serde form, and
//! column order is part of it — the engine keeps order stable by only ever
//! appending columns. Any change to these types' serde layout is a
//! state-migration event, not a refactor.

use serde::{Deserialize, Serialize};

use crate::id::{SchemaHash, TableName};
use crate::types::LogicalType;

/// System (lineage) column names stamped by the shredder. Present in every schema,
/// non-evolvable.
pub mod system {
    /// The run that wrote the row.
    pub const LOAD_ID: &str = "_rdlt_load_id";
    /// Row identity, derived from content or a declared key. What `Merge` merges
    /// on when no structured primary key is declared.
    pub const ID: &str = "_rdlt_id";
    /// On a child table, the `_rdlt_id` of the row that contained this one.
    pub const PARENT_ID: &str = "_rdlt_parent_id";
    /// Position within the parent's collection, so list order survives
    /// normalization into rows.
    pub const POS: &str = "_rdlt_pos";
    /// The `_rdlt_id` of the ROOT row this one descends from, at any depth. What
    /// makes replacing a whole nested subtree a single keyed operation.
    pub const ROOT_ID: &str = "_rdlt_root_id";

    /// Is this a system column? System columns are stamped by the shredder and
    /// never evolve, so schema evolution and contract enforcement skip them.
    pub fn is_system(name: &str) -> bool {
        matches!(name, LOAD_ID | ID | PARENT_ID | POS | ROOT_ID)
    }
}

/// Where a column's type came from. Recorded for diagnostics — and, like
/// every other field of the schema's canonical serde form, it PARTICIPATES
/// in `TableSchema::content_hash`: a provenance-only change (e.g. a column
/// moving from inferred to hinted) yields a new `SchemaHash`. That hash is
/// a persisted format, so this semantic is deliberate and pinned by test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Chosen by inference from the observed values.
    Inferred,
    /// Pinned by a configured type hint, which inference does not override.
    Hinted,
    /// A lineage column stamped by the shredder.
    System,
}

/// The shape of one column. Nested objects stay structural inside the engine —
/// lowering to flat columns happens at the destination seam; lists of objects are
/// never columns (they become child tables).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ColumnType {
    /// A single value.
    Scalar {
        /// The logical type of the value.
        scalar: LogicalType,
    },
    /// Nested object preserved structurally; fields evolve like top-level columns.
    Struct {
        /// The nested fields, which evolve exactly like top-level columns.
        fields: Vec<Column>,
    },
    /// List of scalars (destinations without native lists get a child table instead —
    /// that decision is made at shred planning, so a persisted schema is already
    /// capability-resolved).
    ScalarList {
        /// The element type; every element shares it, widening as needed.
        item: LogicalType,
    },
}

impl ColumnType {
    /// Shorthand for a scalar column of this logical type.
    pub fn scalar(scalar: LogicalType) -> Self {
        ColumnType::Scalar { scalar }
    }
}

/// One column: its name, shape, nullability, and where its type came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    /// Column name as it appears in the schema, before destination
    /// identifier normalization.
    pub name: String,
    // Wire key stays `type` — the Rust field is `column_type` to read as prose
    // and to avoid the `ty` abbreviation; the serialized format is unchanged.
    /// The column's shape.
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// Whether the column admits NULL. Only ever widens false → true: a column
    /// that has seen a missing value can never go back to being required.
    pub nullable: bool,
    /// Where the type came from. Participates in the content hash.
    pub provenance: Provenance,
}

/// Link from a child table to its parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentLink {
    /// The immediate parent table.
    pub parent: TableName,
    /// 1 = direct child of the root table.
    pub depth: u32,
}

/// One version of one table's schema.
///
/// This type's serde layout is a PERSISTED FORMAT: it is what
/// [`TableSchema::content_hash`] hashes and what the WAL records. Changing the
/// layout is a state-migration event, not a refactor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    /// The table this schema describes.
    pub table: TableName,
    /// `None` for root tables.
    pub parent: Option<ParentLink>,
    /// Columns in a STABLE order: the engine only ever appends, because column
    /// order participates in the content hash.
    pub columns: Vec<Column>,
}

impl TableSchema {
    /// Content hash of the canonical form. Provenance participates in serialization but
    /// the canonical form is the full struct — deterministic because struct fields and
    /// column order are fixed.
    pub fn content_hash(&self) -> SchemaHash {
        let canonical = serde_json::to_vec(self).expect("TableSchema serialization is infallible");
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"rdlt:table-schema:v1\0");
        hasher.update(&canonical);
        SchemaHash::from_bytes(*hasher.finalize().as_bytes())
    }

    /// Find a column by name.
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// One evolution step: `from` (None = table creation) to `to`, as a minimal change set.
/// The only way schemas change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Delta {
    /// The table that evolved.
    pub table: TableName,
    /// The schema version this step started from; `None` when the table was
    /// created by this step.
    pub from: Option<SchemaHash>,
    /// The schema version this step produced.
    pub to: SchemaHash,
    /// The minimal set of changes between the two versions.
    pub changes: Vec<Change>,
}

/// A single schema change, and the complete set of changes rdlt can make.
///
/// There is deliberately no drop, no rename, and no narrowing: those lose data
/// or break readers, so rdlt refuses them rather than performing them. A schema
/// contract decides whether even these three are allowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum Change {
    /// The table did not exist and is being created at this schema.
    CreateTable {
        /// The initial schema.
        schema: TableSchema,
    },
    /// A column appeared that the table did not have. Always nullable: existing
    /// rows have no value for it.
    AddColumn {
        /// The column being added.
        column: Column,
    },
    /// A column's type widened along the lattice to admit a value that did not
    /// fit the old one.
    WidenColumn {
        /// The column's name.
        name: String,
        /// The type before.
        from: ColumnType,
        /// The type after — always strictly wider.
        to: ColumnType,
    },
}

/// Destination identifier constraints — the projection of a destination's
/// capabilities that naming needs. Serialized where those capabilities cross a
/// boundary (the connector handshake, the WAL's rules sidecar).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentRules {
    /// Maximum identifier byte length (e.g. 63 for Postgres).
    pub max_len: usize,
}

/// The floor of the legal range for [`IdentRules::max_len`]. Below it the
/// collision-suffix machinery degenerates: every over-long name normalizes to
/// the same prefix and the candidate space (`16^(max_len-1)`) can be exhausted
/// — and a connector-declared `max_len` is untrusted wire input, so a tiny one
/// must never reach the namer. 8 admits 16⁷ ≈ 2.7×10⁸ suffixes, effectively
/// inexhaustible.
pub const MIN_IDENT_MAX_LEN: usize = 8;
/// The ceiling of the legal range for [`IdentRules::max_len`]. Above it the
/// length is pure bloat (real destinations top out at 255) and per-name work
/// scales with it; 4096 is arbitrary but far past every real engine.
pub const MAX_IDENT_MAX_LEN: usize = 4096;

impl IdentRules {
    /// Validate a rules value that arrived over a trust boundary: the field is
    /// a plain `usize` because serde fills it, and anything outside
    /// [`MIN_IDENT_MAX_LEN`]..=[`MAX_IDENT_MAX_LEN`] is refused typed rather
    /// than allowed to drive the naming probe loop.
    pub fn validate(&self) -> Result<(), String> {
        if !(MIN_IDENT_MAX_LEN..=MAX_IDENT_MAX_LEN).contains(&self.max_len) {
            return Err(format!(
                "identifier max_len {} is outside [{MIN_IDENT_MAX_LEN}, {MAX_IDENT_MAX_LEN}] \
                 — below the floor the collision-suffix space is exhaustible (a host panic \
                 path), above the ceiling is pure per-name bloat; no real destination's \
                 limit leaves this range (postgres 63, snowflake 255)",
                self.max_len
            ));
        }
        Ok(())
    }
}

impl Default for IdentRules {
    fn default() -> Self {
        // Postgres' 63-byte limit is the tightest among the first destinations;
        // using it everywhere keeps names portable.
        Self { max_len: 63 }
    }
}

/// Deterministic short hex digest for building bounded-length identifiers (e.g.
/// destination staging-table names). Stable across runs and machines.
pub fn ident_hash(input: &str, hex_len: usize) -> String {
    blake3::hash(input.as_bytes()).to_hex()[..hex_len.clamp(4, 64)].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> TableSchema {
        TableSchema {
            table: TableName::new("users"),
            parent: None,
            columns: vec![Column {
                name: "id".into(),
                column_type: ColumnType::scalar(LogicalType::Int64),
                nullable: false,
                provenance: Provenance::Inferred,
            }],
        }
    }

    #[test]
    fn equal_schemas_hash_equal() {
        assert_eq!(schema().content_hash(), schema().content_hash());
    }

    #[test]
    fn any_semantic_change_changes_the_hash() {
        let base = schema().content_hash();
        let mut widened = schema();
        widened.columns[0].column_type = ColumnType::scalar(LogicalType::Float64);
        assert_ne!(base, widened.content_hash());

        let mut renamed = schema();
        renamed.columns[0].name = "id2".into();
        assert_ne!(base, renamed.content_hash());
    }

    /// Provenance participates in the hash — a persisted-format semantic:
    /// a provenance-only change is a schema-hash change. If this test
    /// fails, the hash's canonical form changed, which is a
    /// state-migration event, not a refactor.
    #[test]
    fn provenance_participates_in_the_hash() {
        let base = schema().content_hash();
        let mut rehinted = schema();
        assert_eq!(rehinted.columns[0].provenance, Provenance::Inferred);
        rehinted.columns[0].provenance = Provenance::Hinted;
        assert_ne!(base, rehinted.content_hash());
    }

    /// The column's wire key is `type`, whatever the Rust field is called.
    #[test]
    fn column_wire_form_keeps_its_type_key() {
        let wire = serde_json::to_value(&schema().columns[0]).unwrap();
        assert_eq!(wire["type"]["kind"], "scalar");
        assert_eq!(wire["type"]["scalar"]["type"], "int64");
        assert!(wire.get("column_type").is_none());
    }
}

#[cfg(test)]
mod system_tests {
    use super::*;

    #[test]
    fn is_system_recognizes_exactly_the_reserved_prefix() {
        assert!(system::is_system(system::ID));
        assert!(system::is_system(system::LOAD_ID));
        assert!(!system::is_system("id"));
        assert!(!system::is_system("rdlt_id"));
    }
}

#[cfg(test)]
mod ident_tests {
    use super::*;

    /// The rules range gate, at its edges — the floor admits 16⁷ candidate
    /// suffixes (effectively inexhaustible), the ceiling is arbitrary but past
    /// every real destination.
    #[test]
    fn ident_rules_validate_at_their_range_edges() {
        assert!(IdentRules { max_len: 0 }.validate().is_err());
        assert!(
            IdentRules {
                max_len: MIN_IDENT_MAX_LEN - 1
            }
            .validate()
            .is_err()
        );
        assert!(
            IdentRules {
                max_len: MIN_IDENT_MAX_LEN
            }
            .validate()
            .is_ok()
        );
        assert!(IdentRules { max_len: 63 }.validate().is_ok());
        assert!(
            IdentRules {
                max_len: MAX_IDENT_MAX_LEN
            }
            .validate()
            .is_ok()
        );
        assert!(
            IdentRules {
                max_len: MAX_IDENT_MAX_LEN + 1
            }
            .validate()
            .is_err()
        );
        assert!(
            IdentRules {
                max_len: usize::MAX
            }
            .validate()
            .is_err()
        );
    }

    /// The digest is deterministic and clamped to its documented width.
    #[test]
    fn ident_hash_is_stable_and_clamped() {
        assert_eq!(ident_hash("orders", 8), ident_hash("orders", 8));
        assert_eq!(ident_hash("orders", 8).len(), 8);
        assert_eq!(ident_hash("orders", 1).len(), 4, "clamped up to 4");
        assert_eq!(ident_hash("orders", 100).len(), 64, "clamped down to 64");
        assert!(
            ident_hash("orders", 8)
                .chars()
                .all(|c| c.is_ascii_hexdigit())
        );
    }
}
