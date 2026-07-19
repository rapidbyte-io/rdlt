//! Stream shredding: nesting, child-table extraction, lineage stamping
//! (design doc §§5.3–5.4).
//!
//! One `StreamShredder` owns one stream's table family (root + children at any depth).
//! Per micro-batch it observes shapes (infer), assigns row identities, buffers rows per
//! table, then resolves schemas and hands `build` the final layout.

use std::collections::VecDeque;

use rdlt_connector::{DestCapabilities, StreamSpec};
use rdlt_core::identity::{FieldValue, RowIdBuilder, child_row_id};
use rdlt_core::naming::{IdentRules, UniqueNamer, child_table_name};
use rdlt_core::schema::system_columns;
use rdlt_core::{ColumnDef, ParentLink, Provenance, RowId, TableName, TableSchema};
use serde_json::Value;

use super::canon::{canonical_json_bytes, render_scalar};
use super::infer::{ColState, ScalarState};

/// One buffered row awaiting Arrow building.
#[derive(Debug)]
pub(crate) struct BufferedRow {
    pub(crate) value: Value,
    pub(crate) id: RowId,
    pub(crate) parent_id: Option<RowId>,
    pub(crate) root_id: Option<RowId>,
    pub(crate) pos: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct TableBuffer {
    pub(crate) table: TableName,
    pub(crate) parent: Option<ParentLink>,
    /// Column observation states in first-seen order; source key → state.
    pub(crate) columns: Vec<(String, ColState)>,
    /// Source key → normalized column/child name mapping (collision-safe).
    namer: UniqueNamer,
    names: Vec<(String, String)>,
    pub(crate) rows: Vec<BufferedRow>,
}

impl TableBuffer {
    fn new(table: TableName, parent: Option<ParentLink>, rules: IdentRules) -> Self {
        let mut namer = UniqueNamer::new(rules);
        // System columns RESERVE their names: a source field literally named
        // `_rdlt_id` gets suffixed rather than aliasing the lineage column.
        for sys in [
            system_columns::LOAD_ID,
            system_columns::ID,
            system_columns::PARENT_ID,
            system_columns::POS,
            system_columns::ROOT_ID,
        ] {
            namer.reserve(sys);
        }
        Self {
            table,
            parent,
            columns: Vec::new(),
            namer,
            names: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Source key → normalized column name pairs accumulated so far.
    pub(crate) fn name_map(&self) -> &[(String, String)] {
        &self.names
    }

    /// Reverse lookup: normalized column name → source key.
    pub(crate) fn source_key_for(&self, normalized: &str) -> Option<&str> {
        self.names
            .iter()
            .find(|(_, n)| n == normalized)
            .map(|(source, _)| source.as_str())
    }

    /// Policy enforcement: revert one column's observation state to its pre-batch
    /// snapshot (or remove it if the column first appeared this batch).
    pub(crate) fn revert_column(
        &mut self,
        source_key: &str,
        snapshot: Option<&[(String, ColState)]>,
    ) {
        let prior = snapshot.and_then(|columns| {
            columns
                .iter()
                .find(|(key, _)| key == source_key)
                .map(|(_, state)| state.clone())
        });
        match prior {
            Some(state) => {
                if let Some(idx) = self.columns.iter().position(|(k, _)| k == source_key) {
                    self.columns[idx].1 = state;
                }
            }
            None => self.columns.retain(|(k, _)| k != source_key),
        }
    }

    pub(crate) fn column_name(&mut self, source_key: &str) -> String {
        if let Some((_, normalized)) = self.names.iter().find(|(k, _)| k == source_key) {
            return normalized.clone();
        }
        let normalized = self.namer.name_for(source_key);
        self.names.push((source_key.to_owned(), normalized.clone()));
        normalized
    }

    fn state_mut(&mut self, source_key: &str) -> &mut ColState {
        if let Some(idx) = self.columns.iter().position(|(k, _)| k == source_key) {
            &mut self.columns[idx].1
        } else {
            self.columns
                .push((source_key.to_owned(), ColState::Unknown));
            &mut self.columns.last_mut().expect("just pushed").1
        }
    }
}

pub(crate) struct StreamShredder {
    spec: StreamSpec,
    caps: DestCapabilities,
    /// Root first, children after, in first-seen order.
    pub(crate) tables: Vec<TableBuffer>,
    /// Column states per table as of the start of the current micro-batch — the
    /// rollback point for Discard* policy enforcement.
    pub(crate) pre_batch: Vec<Vec<(String, ColState)>>,
}

impl StreamShredder {
    pub(crate) fn new(spec: StreamSpec, caps: DestCapabilities, root_table: TableName) -> Self {
        let mut root = TableBuffer::new(root_table, None, caps.ident_rules);
        // Hints pin root-level scalar columns (they win over inference).
        for (column, ty) in &spec.type_hints {
            *root.state_mut(column) = ColState::Scalar(ScalarState::pinned(*ty));
        }
        Self {
            spec,
            caps,
            tables: vec![root],
            pre_batch: Vec::new(),
        }
    }

    /// Shred one raw-JSON push (NDJSON, a JSON array, or a single document) into
    /// buffered rows across the stream's table family.
    pub(crate) fn push_bytes(&mut self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        // Snapshot observation states: Discard* enforcement rolls offending columns
        // back to exactly this point.
        self.pre_batch = self.tables.iter().map(|t| t.columns.clone()).collect();
        let rows = parse_rows(bytes)?;
        for row in rows {
            self.push_row(row);
        }
        Ok(())
    }

    fn push_row(&mut self, row: Value) {
        // Non-object rows (a bare scalar/array in the feed) wrap into {"value": …} so
        // every table is uniformly an object relation.
        let row = match row {
            Value::Object(_) => row,
            other => {
                let mut map = serde_json::Map::new();
                map.insert("value".to_owned(), other);
                Value::Object(map)
            }
        };

        let root_id = self.row_identity(&row);
        // Breadth-first traversal; every queued entry carries the ORIGINAL root row's
        // id so `_rdlt_root_id` is correct at any nesting depth without parent-chain
        // walks (design doc §5.4).
        struct Queued {
            table_idx: usize,
            row: Value,
            id: RowId,
            parent_id: Option<RowId>,
            root_id: RowId,
            pos: Option<u64>,
        }
        let mut queue: VecDeque<Queued> = VecDeque::new();
        queue.push_back(Queued {
            table_idx: 0,
            row,
            id: root_id,
            parent_id: None,
            root_id,
            pos: None,
        });

        while let Some(entry) = queue.pop_front() {
            let lists_as_columns = self.caps.scalar_lists;
            let is_root = entry.table_idx == 0;

            // Observe every field; discover child tables.
            let obj = entry
                .row
                .as_object()
                .expect("rows are objects by construction");
            let mut child_lists: Vec<(String, Vec<Value>)> = Vec::new();
            for (key, value) in obj {
                let state = self.tables[entry.table_idx].state_mut(key);
                state.observe(value, lists_as_columns);
                if state.is_child_table()
                    && let Value::Array(items) = value
                {
                    child_lists.push((key.clone(), items.clone()));
                }
            }

            // Enqueue child rows.
            for (key, items) in child_lists {
                let child_idx = self.child_table_idx(entry.table_idx, &key);
                for (i, item) in items.into_iter().enumerate() {
                    if item.is_null() {
                        continue;
                    }
                    // Scalar list items in a child table become {"value": …} rows.
                    let child_row = match item {
                        Value::Object(_) => item,
                        other => {
                            let mut map = serde_json::Map::new();
                            map.insert("value".to_owned(), other);
                            Value::Object(map)
                        }
                    };
                    let content = content_hash(&child_row);
                    let child_id = child_row_id(&entry.id, i as u64, &content);
                    queue.push_back(Queued {
                        table_idx: child_idx,
                        row: child_row,
                        id: child_id,
                        parent_id: Some(entry.id),
                        root_id: entry.root_id,
                        pos: Some(i as u64),
                    });
                }
            }

            self.tables[entry.table_idx].rows.push(BufferedRow {
                value: entry.row,
                id: entry.id,
                parent_id: entry.parent_id,
                root_id: if is_root { None } else { Some(entry.root_id) },
                pos: entry.pos,
            });
        }
    }

    fn child_table_idx(&mut self, parent_idx: usize, source_key: &str) -> usize {
        let parent_table = self.tables[parent_idx].table.clone();
        let parent_depth = self.tables[parent_idx]
            .parent
            .as_ref()
            .map_or(0, |p| p.depth);
        let name = child_table_name(parent_table.as_str(), source_key, self.caps.ident_rules);
        let table = TableName::new(name);
        if let Some(idx) = self.tables.iter().position(|t| t.table == table) {
            return idx;
        }
        self.tables.push(TableBuffer::new(
            table,
            Some(ParentLink {
                parent: parent_table,
                depth: parent_depth + 1,
            }),
            self.caps.ident_rules,
        ));
        self.tables.len() - 1
    }

    /// `_rdlt_id` for a root row: key hash when the stream declares a primary key,
    /// content hash otherwise (design doc §5.4).
    fn row_identity(&self, row: &Value) -> RowId {
        match &self.spec.primary_key {
            Some(key_fields) if !key_fields.is_empty() => {
                let mut builder = RowIdBuilder::keyed();
                for field in key_fields {
                    let rendered = row.get(field).and_then(render_scalar);
                    match &rendered {
                        Some(text) => builder.field(field, FieldValue::Bytes(text.as_bytes())),
                        None => builder.field(field, FieldValue::Null),
                    };
                }
                builder.finish()
            }
            _ => content_hash(row),
        }
    }
}

fn content_hash(row: &Value) -> RowId {
    let mut canonical = Vec::new();
    canonical_json_bytes(row, &mut canonical);
    let mut builder = RowIdBuilder::keyless();
    builder.field("", FieldValue::Bytes(&canonical));
    builder.finish()
}

/// Parse a raw-JSON push: NDJSON, a top-level array, or a single document.
fn parse_rows(bytes: &[u8]) -> Result<Vec<Value>, serde_json::Error> {
    let mut rows = Vec::new();
    let stream = serde_json::Deserializer::from_slice(bytes).into_iter::<Value>();
    for doc in stream {
        match doc? {
            Value::Array(items) => rows.extend(items),
            value => rows.push(value),
        }
    }
    Ok(rows)
}

/// Resolve one table's buffered observation state into schema columns:
/// system/lineage columns first, then source columns in first-seen order.
pub(crate) fn resolve_schema(buffer: &mut TableBuffer) -> TableSchema {
    let mut columns: Vec<ColumnDef> = Vec::new();
    let system = |name: &str, ty| ColumnDef {
        name: name.to_owned(),
        ty: rdlt_core::ColumnType::scalar(ty),
        nullable: false,
        provenance: Provenance::System,
    };
    columns.push(system(
        system_columns::LOAD_ID,
        rdlt_core::LogicalType::Utf8,
    ));
    columns.push(system(system_columns::ID, rdlt_core::LogicalType::Utf8));
    if buffer.parent.is_some() {
        columns.push(system(
            system_columns::PARENT_ID,
            rdlt_core::LogicalType::Utf8,
        ));
        columns.push(system(system_columns::POS, rdlt_core::LogicalType::Int64));
        columns.push(system(
            system_columns::ROOT_ID,
            rdlt_core::LogicalType::Utf8,
        ));
    }

    let sources: Vec<(String, Option<rdlt_core::ColumnType>)> = buffer
        .columns
        .iter()
        .map(|(key, state)| (key.clone(), state.resolve()))
        .collect();
    for (source_key, resolved) in sources {
        if let Some(ty) = resolved {
            let name = buffer.column_name(&source_key);
            columns.push(ColumnDef {
                name,
                ty,
                nullable: true,
                provenance: Provenance::Inferred,
            });
        }
    }

    TableSchema {
        table: buffer.table.clone(),
        parent: buffer.parent.clone(),
        columns,
    }
}
