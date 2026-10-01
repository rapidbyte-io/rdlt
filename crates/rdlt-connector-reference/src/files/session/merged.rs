//! Merging a commit's staged files into a merge table's published ones.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;
use rdlt_connector::{ChildTable, CommitMeta, MergeKey, RootKey};

use super::commit::Staging;
use super::{Location, StagedFile};
use crate::files::format::Writer;
use crate::files::manifest::{self, Listed, Manifest, TableFiles};
use crate::files::tables;
use crate::merge::Merged;

/// Replaces, in the child table `child` the commit staged nothing for, the children of the roots
/// its root's staged files publish.
pub(super) fn follow_root(
    location: &Location,
    manifest: &mut Manifest,
    child: &ChildTable,
    meta: &CommitMeta,
    staged: &Staging<'_>,
    created: &mut Vec<String>,
) -> Result<()> {
    let Some(root) = &child.merge.root else {
        return Ok(());
    };
    let name = child.table.to_string();
    let Some(root_files) = staged.get(&(root.table.to_string(), None)) else {
        return Ok(());
    };
    if staged.contains_key(&(name.clone(), None)) {
        return Ok(());
    }
    let table = manifest.tables.entry(name.clone()).or_default();
    let root = Some((root, root_files.as_slice()));
    let merged = merged_rows(location, &name, table, &[], &child.merge, root)?;
    // Nothing staged for the table, so it changes only where its roots' rows drop children.
    if merged.rows.num_rows() == merged.held {
        return Ok(());
    }
    table.files = written(location, &name, "merged", &merged.rows, meta, created)?
        .into_iter()
        .collect();
    Ok(())
}

/// A table's rows once merged, its tombstones for a change stream's, and how many rows its
/// published files held.
pub(super) struct MergedRows {
    pub(super) rows: RecordBatch,
    pub(super) tombstones: Option<RecordBatch>,
    pub(super) held: usize,
}

/// The rows of the table `name` once `files` are merged into its published `table` by `key`, or
/// for a child table once they replace the children of the roots its root's `files` publish.
///
/// The whole table is held in memory while it merges.
pub(super) fn merged_rows(
    location: &Location,
    name: &str,
    table: &TableFiles,
    files: &[&StagedFile],
    key: &MergeKey,
    root: Option<(&RootKey, &[&StagedFile])>,
) -> Result<MergedRows> {
    let schema = tables::read(&location.rdlt, name)?
        .ok_or_else(|| ConnectorError::data(format!("table {name} does not exist")))?;
    let schema = Arc::new(schema.to_arrow());
    // A change stream's staged rows carry the columns that direct its merge, and a truncate's
    // names no key.
    let staged = match &key.changes {
        Some(changes) => crate::merge::written_schema(&schema, changes),
        None => Arc::clone(&schema),
    };
    let read = |files: &mut dyn Iterator<Item = &Listed>, schema: &arrow_schema::SchemaRef| {
        let mut batches = Vec::new();
        for file in files {
            batches.extend(manifest::read(&location.dir, &file.path, schema)?);
        }
        Ok::<_, ConnectorError>(batches)
    };
    let published = read(&mut table.files.iter(), &schema)?;
    let held = published.iter().map(RecordBatch::num_rows).sum();
    let merging = |error: arrow_schema::ArrowError| {
        ConnectorError::data(format!("merging table {name}: {error}"))
    };
    let incoming = read(&mut files.iter().map(|staged| &staged.file), &staged)?;
    let merged = if let Some((root, root_files)) = root {
        let root_schema = tables::read(&location.rdlt, &root.table)?.ok_or_else(|| {
            ConnectorError::data(format!("root table {} does not exist", root.table))
        })?;
        let root_schema = Arc::new(root_schema.to_arrow());
        let roots = read(
            &mut root_files.iter().map(|staged| &staged.file),
            &root_schema,
        )?;
        let rows = crate::merge::merge_children(&schema, &published, &incoming, key, root, &roots)
            .map_err(merging)?;
        Merged {
            rows,
            tombstones: Vec::new(),
        }
    } else {
        let buried = crate::merge::tombstone_schema(&schema, key).map_err(merging)?;
        let buried = read(&mut table.tombstones.iter(), &buried)?;
        crate::merge::merge(&schema, &published, &buried, &incoming, key).map_err(merging)?
    };
    let rows = arrow_select::concat::concat_batches(&schema, &merged.rows).map_err(merging)?;
    let tombstones = merged.tombstones.into_iter().next();
    Ok(MergedRows {
        rows,
        tombstones,
        held,
    })
}

/// Writes `rows` of the table `name` as commit `meta`'s merge, under `kind`: its rows or its
/// tombstones; the file written, which `created` gains, or none for no rows.
pub(super) fn written(
    location: &Location,
    name: &str,
    kind: &str,
    rows: &RecordBatch,
    meta: &CommitMeta,
    created: &mut Vec<String>,
) -> Result<Option<Listed>> {
    if rows.num_rows() == 0 {
        return Ok(None);
    }
    let segment = Location::written_by(kind, meta)?;
    let (names, file) = location.staged(&segment, name, None, 0);
    let dir = location.staging(&names)?;
    let path = format!("{}/{file}", names.join("/"));
    created.push(path.clone());
    let mut writer = Writer::create(location.format, &dir, &file, rows.schema_ref())?;
    writer.write(rows)?;
    let written = writer.finish()?;
    Ok(Some(Listed {
        path,
        rows: written.rows,
        bytes: written.bytes,
    }))
}
