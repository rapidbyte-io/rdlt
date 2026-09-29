//! Merging a commit's staged files into a merge table's published ones.

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::prelude::*;
use rdlt_connector::{ChildTable, CommitMeta, MergeKey, RootKey};

use super::{Location, StagedFile, Staging};
use crate::files::manifest::{Manifest, TableFiles};
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
    table.files = written(location, &name, "merged", &merged.rows, meta)?
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
pub(super) fn merged_rows(
    location: &Location,
    name: &str,
    table: &TableFiles,
    files: &[&StagedFile],
    key: &MergeKey,
    root: Option<(&RootKey, &[&StagedFile])>,
) -> Result<MergedRows> {
    let schema = tables::read(&location.root, name)?
        .ok_or_else(|| ConnectorError::data(format!("table {name} does not exist")))?;
    let schema = Arc::new(schema.to_arrow());
    // A change stream's staged rows carry the columns that direct its merge, and a truncate's
    // names no key.
    let staged = match &key.changes {
        Some(changes) => crate::merge::written_schema(&schema, changes),
        None => Arc::clone(&schema),
    };
    let read = |paths: &mut dyn Iterator<Item = &String>, schema: &arrow_schema::SchemaRef| {
        let mut batches = Vec::new();
        for path in paths {
            batches.extend(location.format.read(&location.root.join(path), schema)?);
        }
        Ok::<_, ConnectorError>(batches)
    };
    let published = read(&mut table.files.iter(), &schema)?;
    let held = published.iter().map(RecordBatch::num_rows).sum();
    let merging = |error: arrow_schema::ArrowError| {
        ConnectorError::data(format!("merging table {name}: {error}"))
    };
    let incoming = read(&mut files.iter().map(|file| &file.path), &staged)?;
    let merged = if let Some((root, root_files)) = root {
        let root_schema = tables::read(&location.root, &root.table)?.ok_or_else(|| {
            ConnectorError::data(format!("root table {} does not exist", root.table))
        })?;
        let root_schema = Arc::new(root_schema.to_arrow());
        let mut roots = Vec::new();
        for file in root_files {
            roots.extend(
                location
                    .format
                    .read(&location.root.join(&file.path), &root_schema)?,
            );
        }
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
/// tombstones; the file written, or none for no rows.
pub(super) fn written(
    location: &Location,
    name: &str,
    kind: &str,
    rows: &RecordBatch,
    meta: &CommitMeta,
) -> Result<Option<String>> {
    if rows.num_rows() == 0 {
        return Ok(None);
    }
    let path = location.staged(&format!("{kind}/{}", meta.commit_seq.get()), name, None, 0);
    location.format.write(&location.root.join(&path), rows)?;
    Ok(Some(path))
}
