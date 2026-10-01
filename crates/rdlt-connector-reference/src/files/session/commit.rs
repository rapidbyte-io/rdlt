//! A commit: the files a session staged become part of the pipeline's next manifest.

use std::collections::BTreeMap;
use std::time::SystemTime;

use parking_lot::Mutex;
use rdlt_connector::GenerationId;
use rdlt_connector::prelude::*;

use super::merged::{follow_root, merged_rows, written};
use super::{Location, Shared, StagedFile, path_key};
use crate::files::manifest::{self, Manifest};
use crate::files::{destination, tables};

/// The files a commit publishes, by table and generation.
pub(super) type Staging<'a> = BTreeMap<(String, Option<GenerationId>), Vec<&'a StagedFile>>;

/// Publishes the files this session staged in `meta`'s segments by creating the next manifest.
pub(super) fn commit(
    location: &Location,
    shared: &Mutex<Shared>,
    meta: &CommitMeta,
) -> Result<Receipt> {
    let mut manifest = manifest::latest(&location.dir)?.unwrap_or_default();
    if manifest.epoch != location.epoch || meta.epoch != location.epoch {
        return Err(ConnectorError::fenced(format!(
            "the pipeline is at epoch {}; this session opened at {}",
            manifest.epoch, location.epoch
        )));
    }
    if let Some(receipt) = manifest.receipt(meta.load_id, meta.commit_seq) {
        return Ok(receipt);
    }
    named(meta)?;
    let (staged, names) = {
        let shared = shared.lock();
        let staged: Vec<StagedFile> = shared
            .staged
            .iter()
            .filter(|file| meta.segments.contains(file.segment))
            .cloned()
            .collect();
        (staged, shared.names.clone())
    };
    publish_all(location, &mut manifest, &staged, meta)?;
    let receipt = put(location, &mut manifest, &staged, names, meta)?;
    shared
        .lock()
        .staged
        .retain(|file| !meta.segments.contains(file.segment));
    // The manifest is the truth: a catalog left behind here is removed by the next open.
    for name in &manifest.dropped {
        let still = || destination::still_dropped(&location.dir, name);
        let (rdlt, wait) = (&location.rdlt, location.lock_wait);
        drop(tables::release(rdlt, name, &location.pipeline, wait, still));
    }
    Ok(receipt)
}

/// Checks that every table `meta` names besides those its segments were staged for is an
/// identifier, before any becomes part of a path or of the manifest.
fn named(meta: &CommitMeta) -> Result<()> {
    for dropped in &meta.drop_tables {
        tables::named(&dropped.name)?;
    }
    for child in &meta.child_tables {
        tables::named(&child.table)?;
        if let Some(root) = &child.merge.root {
            tables::named(&root.table)?;
        }
    }
    Ok(())
}

/// Finishes `manifest` as `meta`'s commit of the `staged` files and creates it as the pipeline's
/// next version; the commit's receipt.
fn put(
    location: &Location,
    manifest: &mut Manifest,
    staged: &[StagedFile],
    names: BTreeMap<String, String>,
    meta: &CommitMeta,
) -> Result<Receipt> {
    manifest.paths.extend(names);
    finish(location, manifest, meta)?;
    manifest.apply(&meta.state_delta);
    let receipt = Receipt {
        load_id: meta.load_id,
        commit_seq: meta.commit_seq,
        committed_at: manifest::truncated(SystemTime::now()),
        rows: staged.iter().map(|staged| staged.file.rows).sum(),
        bytes: staged.iter().map(|staged| staged.file.bytes).sum(),
    };
    manifest.record(&receipt);
    let Some(version) = manifest.version.checked_add(1) else {
        return Err(ConnectorError::data(format!(
            "pipeline {} holds the last manifest version there is",
            location.pipeline
        )));
    };
    manifest.version = version;
    if !manifest::put(&location.dir, manifest)? {
        return Err(ConnectorError::fenced(
            "another session published the pipeline's next manifest first",
        ));
    }
    Ok(receipt)
}

/// Swaps into `manifest` the generations `meta` finishes, and drops from it the tables `meta`
/// drops, each of which another pipeline must not own.
fn finish(location: &Location, manifest: &mut Manifest, meta: &CommitMeta) -> Result<()> {
    for dropped in &meta.drop_tables {
        if let Some(owner) = tables::owner(&location.rdlt, &dropped.name)?
            && owner != location.pipeline.as_str()
        {
            return Err(ConnectorError::table_owned(&dropped.name, &owner));
        }
    }
    for (path, generation) in &meta.finish_generations {
        let Some(name) = manifest.paths.get(&path_key(path)).cloned() else {
            continue;
        };
        let table = manifest.tables.entry(name).or_default();
        table.files = table.generations.remove(generation).unwrap_or_default();
        table.generations.clear();
        table.tombstones.clear();
    }
    for dropped in &meta.drop_tables {
        manifest.tables.remove(&*dropped.name);
        manifest.paths.remove(&path_key(&dropped.path));
        manifest.dropped.insert(dropped.name.to_string());
    }
    Ok(())
}

/// Adds `files`, staged for the table `name` or one generation of it, to what `manifest` lists
/// for it: appended, into their generation, or merged into one new file of the table's rows.
fn publish(
    location: &Location,
    manifest: &mut Manifest,
    name: &str,
    files: &[&StagedFile],
    meta: &CommitMeta,
    staged: &Staging<'_>,
) -> Result<()> {
    let table = manifest.tables.entry(name.to_owned()).or_default();
    let first = &files[0].table;
    let listed = files.iter().map(|staged| staged.file.clone());
    match (&first.generation, &first.merge) {
        (Some(generation), _) => table
            .generations
            .entry(*generation)
            .or_default()
            .extend(listed),
        (None, Some(key)) => {
            let root = key.root.as_ref().map(|root| {
                let files = staged.get(&(root.table.to_string(), None));
                (root, files.map(Vec::as_slice).unwrap_or_default())
            });
            let merged = merged_rows(location, name, table, files, key, root)?;
            table.files = written(location, name, "merged", &merged.rows, meta)?
                .into_iter()
                .collect();
            table.tombstones = match &merged.tombstones {
                Some(tombstones) => written(location, name, "tombstones", tombstones, meta)?
                    .into_iter()
                    .collect(),
                None => Vec::new(),
            };
        }
        (None, None) => table.files.extend(listed),
    }
    Ok(())
}

/// Adds the `staged` files of `meta` to what `manifest` lists for their tables, and has the
/// child tables it lists follow their roots.
fn publish_all(
    location: &Location,
    manifest: &mut Manifest,
    staged: &[StagedFile],
    meta: &CommitMeta,
) -> Result<()> {
    let mut by_table: Staging<'_> = BTreeMap::new();
    for file in staged {
        by_table
            .entry((file.table.name.to_string(), file.table.generation))
            .or_default()
            .push(file);
    }
    for ((name, _), files) in &by_table {
        publish(location, manifest, name, files, meta, &by_table)?;
    }
    for child in &meta.child_tables {
        follow_root(location, manifest, child, meta, &by_table)?;
    }
    Ok(())
}
