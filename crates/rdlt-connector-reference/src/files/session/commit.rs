//! A commit: the files a session staged become part of the pipeline's next manifest, and what
//! that manifest no longer lists is removed once it is durable.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

use parking_lot::Mutex;
use rdlt_connector::GenerationId;
use rdlt_connector::prelude::*;

use super::compact::compact;
use super::merged::{follow_root, merged_rows, written};
use super::{Location, Shared, StagedFile, path_key};
use crate::files::manifest::{self, Manifest};
use crate::files::{destination, tables};
use crate::rooted::Dir;

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
    let mut held: Vec<String> = staged
        .iter()
        .map(|staged| staged.file.path.clone())
        .collect();
    let receipt = if let Some(receipt) = manifest.receipt(meta.load_id, meta.commit_seq) {
        // The commit's manifest exists, and an earlier answer may have failed before the
        // manifest's name was durable: it is made durable before the commit is answered again.
        manifest::settle(&location.dir)?;
        // What that commit superseded may still be there: what no manifest lists goes, as far
        // as it goes.
        drop(destination::discard_superseded(
            &location.dir,
            location.epoch,
        ));
        receipt
    } else {
        held.extend(manifest.files().map(|file| file.path.clone()));
        publish(location, &mut manifest, &staged, names, meta)?
    };
    {
        let mut shared = shared.lock();
        shared
            .staged
            .retain(|file| !meta.segments.contains(file.segment));
        // A dropped table is the session's no longer, until it creates the table again.
        for dropped in &meta.drop_tables {
            shared.names.remove(&path_key(&dropped.path));
        }
    }
    // The manifest is durable: what it no longer lists is read by nothing that follows it. What
    // cannot be removed now is removed by the next open.
    prune(&location.dir, &held);
    release_dropped(location, shared, &manifest);
    Ok(receipt)
}

/// Publishes the `staged` files of commit `meta` in `manifest` and creates it as the pipeline's
/// next version; the commit's receipt.
///
/// A commit that fails removes what it wrote unless its manifest was created after all: a
/// failure after that leaves the manifest, which lists what was written.
fn publish(
    location: &Location,
    manifest: &mut Manifest,
    staged: &[StagedFile],
    names: BTreeMap<String, String>,
    meta: &CommitMeta,
) -> Result<Receipt> {
    named(meta)?;
    let mut created = Vec::new();
    let published = publish_all(location, manifest, staged, meta, &mut created)
        .and_then(|()| put(location, manifest, staged, names, meta));
    if published.is_err() {
        prune(&location.dir, &created);
    }
    published
}

/// Removes the catalogs of the tables `manifest` lists as dropped, each once in the session.
///
/// The manifest is the truth: a catalog left behind here is removed by the next open.
fn release_dropped(location: &Location, shared: &Mutex<Shared>, manifest: &Manifest) {
    for name in &manifest.dropped {
        if shared.lock().released.contains(name) {
            continue;
        }
        let still = || destination::still_dropped(&location.dir, name);
        let (rdlt, wait) = (&location.rdlt, location.lock_wait);
        if tables::release(rdlt, name, &location.pipeline, wait, still).is_ok() {
            shared.lock().released.insert(name.clone());
        }
    }
}

/// Removes those of `paths` the latest manifest of the pipeline whose directory `dir` is does
/// not list; where no manifest reads, nothing is removed.
fn prune(dir: &Dir, paths: &[String]) {
    let Ok(latest) = manifest::latest(dir) else {
        return;
    };
    let listed: BTreeSet<&String> = latest
        .iter()
        .flat_map(Manifest::files)
        .map(|file| &file.path)
        .collect();
    remove(dir, paths.iter().filter(|path| !listed.contains(path)));
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
    // A table the session created again since it dropped it is dropped no longer.
    let dropping = |name: &String| meta.drop_tables.iter().any(|table| *table.name == **name);
    for name in names.values().filter(|name| !dropping(name)) {
        manifest.dropped.remove(name);
    }
    manifest.paths.extend(names);
    finish(location, manifest, meta)?;
    let published = &manifest.tables;
    manifest
        .dropped
        .retain(|name| !published.contains_key(name));
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

/// Names: how many of a staged path's leading names (the staging directory, the epoch and the
/// load) lead to directories every writer of a session shares, which a commit never removes.
const SHARED: usize = 3;

/// Removes the files at `paths` under the pipeline's directory `dir`, and the directories of
/// their own that leaves empty, as far as each goes: a path that cannot be removed is left for
/// the next open.
pub(super) fn remove<'a>(dir: &Dir, paths: impl Iterator<Item = &'a String>) {
    for path in paths {
        let Ok(names) = manifest::staged(path) else {
            continue;
        };
        let Some((file, parents)) = names.split_last() else {
            continue;
        };
        // Each directory leading to the file, the pipeline's own first.
        let mut reached = Vec::with_capacity(parents.len());
        for name in parents {
            let parent = reached.last().unwrap_or(dir);
            let Ok(next) = parent.dir(name) else { break };
            reached.push(next);
        }
        // The file goes where every directory leading to it was reached; a file already gone
        // still leaves its directories to remove.
        if let Some(holding) = reached.get(parents.len().wrapping_sub(1))
            && reached.len() == parents.len()
        {
            drop(holding.remove_file(file));
        }
        // Each directory left empty goes, deepest first, but those writers share.
        for level in (SHARED..reached.len()).rev() {
            if reached[level - 1].remove_dir(parents[level]).is_err() {
                break;
            }
        }
    }
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
fn publish_table(
    location: &Location,
    manifest: &mut Manifest,
    name: &str,
    files: &[&StagedFile],
    meta: &CommitMeta,
    staged: &Staging<'_>,
    created: &mut Vec<String>,
) -> Result<()> {
    let table = manifest.tables.entry(name.to_owned()).or_default();
    let first = &files[0].table;
    let listed = files.iter().map(|staged| staged.file.clone());
    match (&first.generation, &first.merge) {
        (Some(generation), _) => {
            let filling = table.generations.entry(*generation).or_default();
            filling.extend(listed);
            let (generation, added) = (Some(*generation), files.len());
            compact(location, name, generation, filling, added, meta, created);
        }
        (None, Some(key)) => {
            let root = key.root.as_ref().map(|root| {
                let files = staged.get(&(root.table.to_string(), None));
                (root, files.map(Vec::as_slice).unwrap_or_default())
            });
            let merged = merged_rows(location, name, table, files, key, root)?;
            table.files = written(location, name, "merged", &merged.rows, meta, created)?;
            let buried = &merged.tombstones;
            table.tombstones = written(location, name, "tombstones", buried, meta, created)?;
        }
        (None, None) => {
            table.files.extend(listed);
            let added = files.len();
            compact(location, name, None, &mut table.files, added, meta, created);
        }
    }
    Ok(())
}

/// Adds the `staged` files of `meta` to what `manifest` lists for their tables, and has the
/// child tables it lists follow their roots; `created` gains every file this writes.
fn publish_all(
    location: &Location,
    manifest: &mut Manifest,
    staged: &[StagedFile],
    meta: &CommitMeta,
    created: &mut Vec<String>,
) -> Result<()> {
    let mut by_table: Staging<'_> = BTreeMap::new();
    for file in staged {
        by_table
            .entry((file.table.name.to_string(), file.table.generation))
            .or_default()
            .push(file);
    }
    for ((name, _), files) in &by_table {
        publish_table(location, manifest, name, files, meta, &by_table, created)?;
    }
    for child in &meta.child_tables {
        follow_root(location, manifest, child, meta, &by_table, created)?;
    }
    Ok(())
}
