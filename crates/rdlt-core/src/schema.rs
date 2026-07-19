//! Versioned table schemas and evolution deltas (data-model.md §3).
//!
//! Hashing contract (contracts/persisted-formats.md §5): `SchemaHash` is a content hash
//! over the canonical serde form. Column order is part of the hash — the engine keeps
//! order stable by only appending new columns. Any change to these types' serde layout
//! is a state-migration event, not a refactor.

use serde::{Deserialize, Serialize};

use crate::ids::{SchemaHash, TableName};
use crate::types::LogicalType;

/// System (lineage) column names stamped by the shredder. Present in every schema,
/// non-evolvable.
pub mod system_columns {
    pub const LOAD_ID: &str = "_rdlt_load_id";
    pub const ID: &str = "_rdlt_id";
    pub const PARENT_ID: &str = "_rdlt_parent_id";
    pub const POS: &str = "_rdlt_pos";
    pub const ROOT_ID: &str = "_rdlt_root_id";

    pub fn is_system(name: &str) -> bool {
        matches!(name, LOAD_ID | ID | PARENT_ID | POS | ROOT_ID)
    }
}

/// Where a column's type came from; excluded from the content hash's semantic meaning
/// but recorded for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    Inferred,
    Hinted,
    System,
}

/// The shape of one column. Nested objects stay structural inside the engine
/// (design doc §5.3 — lowering happens at the destination seam); lists of objects are
/// never columns (they become child tables).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ColumnType {
    Scalar {
        scalar: LogicalType,
    },
    /// Nested object preserved structurally; fields evolve like top-level columns.
    Struct {
        fields: Vec<ColumnDef>,
    },
    /// List of scalars (destinations without native lists get a child table instead —
    /// that decision is made at shred planning, so a persisted schema is already
    /// capability-resolved).
    ScalarList {
        item: LogicalType,
    },
}

impl ColumnType {
    pub fn scalar(scalar: LogicalType) -> Self {
        ColumnType::Scalar { scalar }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: ColumnType,
    pub nullable: bool,
    pub provenance: Provenance,
}

/// Link from a child table to its parent (data-model.md §3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentLink {
    pub parent: TableName,
    /// 1 = direct child of the root table.
    pub depth: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub table: TableName,
    /// `None` for root tables.
    pub parent: Option<ParentLink>,
    pub columns: Vec<ColumnDef>,
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

    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// One evolution step: `from` (None = table creation) to `to`, as a minimal change set.
/// The only way schemas change (data-model.md §3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDelta {
    pub table: TableName,
    pub from: Option<SchemaHash>,
    pub to: SchemaHash,
    pub changes: Vec<SchemaChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum SchemaChange {
    CreateTable {
        schema: TableSchema,
    },
    AddColumn {
        column: ColumnDef,
    },
    WidenColumn {
        name: String,
        from: ColumnType,
        to: ColumnType,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> TableSchema {
        TableSchema {
            table: TableName::new("users"),
            parent: None,
            columns: vec![ColumnDef {
                name: "id".into(),
                ty: ColumnType::scalar(LogicalType::Int64),
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
        widened.columns[0].ty = ColumnType::scalar(LogicalType::Float64);
        assert_ne!(base, widened.content_hash());

        let mut renamed = schema();
        renamed.columns[0].name = "id2".into();
        assert_ne!(base, renamed.content_hash());
    }
}
