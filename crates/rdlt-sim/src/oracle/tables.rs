//! Each of a stream's tables against the reference model: every row the model expects, as often,
//! each value read back from the cell holding it as the schema it was written under says, in a
//! column whose type holds the value's and is stored as the destination's capabilities say.

mod cells;

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{Capabilities, ColumnKey, ColumnPath, TablePath, TableSchema};
use rdlt_engine::{Nested, WriteMode};
use rdlt_testkit::canon::{Canon, storage};

use super::expected::{self, Expected, Placement};
use super::names;
use super::rows::Group;
use crate::destination::{Published, Stored, published_table, table_paths};
use crate::seed::Seed;
use crate::workload::{Relaxed, Row, SimStream};
use crate::world::World;
use cells::{Cell, fields, read, text};

/// Checks every table of `stream` against the model: its own table holds each of `groups`, rows
/// of `delivered`, every row delivered so far, as often as the group says, or where the runs
/// `stopped` short at most as often; its child tables hold exactly the rows the rows it holds
/// normalize into.
pub(super) fn check(
    world: &World,
    stream: &SimStream,
    groups: &[Group],
    delivered: &[Row],
    stopped: bool,
    seed: Seed,
) {
    let capabilities = world.capabilities();
    let root = vec![stream.name.clone()];
    let mut candidates: BTreeMap<String, &Row> = BTreeMap::new();
    for row in groups.iter().flat_map(|group| &group.rows) {
        candidates.insert(expected::ident(row), row);
    }
    let modeled = expected::tables(
        stream,
        &candidates.values().copied().cloned().collect::<Vec<_>>(),
        delivered,
    );
    let slots = Slot::of(groups, modeled.get(&root).map_or(&[][..], Vec::as_slice));
    let mut lineage: Lineage = BTreeMap::new();
    let table = Table {
        world,
        stream,
        path: &root,
        parents: &[],
        capabilities: &capabilities,
        at_most: stopped,
        seed,
    };
    let (ids, held) = table.check(&slots, &lineage);
    lineage.insert(root.clone(), ids);
    let held: Vec<Row> = candidates
        .iter()
        .flat_map(|(ident, row)| {
            std::iter::repeat_n((*row).clone(), held.get(ident).copied().unwrap_or(0))
        })
        .collect();
    check_children(
        world,
        stream,
        &held,
        delivered,
        &capabilities,
        lineage,
        seed,
    );
}

/// Checks each child table of `stream` against the rows its own table's `held` rows, of
/// `delivered`, normalize into, their parents' identities in `lineage`.
fn check_children(
    world: &World,
    stream: &SimStream,
    held: &[Row],
    delivered: &[Row],
    capabilities: &Capabilities,
    mut lineage: Lineage,
    seed: Seed,
) {
    let expected = expected::tables(stream, held, delivered);
    let mut paths: BTreeSet<(usize, Vec<String>)> = expected
        .keys()
        .map(|path| (path.len(), path.clone()))
        .collect();
    paths.extend(
        table_paths(world, &stream.name)
            .into_iter()
            .map(|path| (path.len(), path)),
    );
    let tables: BTreeSet<Vec<String>> = paths.iter().map(|(_, path)| path.clone()).collect();
    // Parents come first, so each child row's parent is known when it is read. A child's id
    // derives from its parent's and its position alone (spec §7.4), so sibling arrays' rows share
    // ids: each table's are kept apart.
    for (_, path) in paths.iter().filter(|(depth, _)| *depth > 1) {
        let rows = expected.get(path).map_or(&[][..], Vec::as_slice);
        // A child's parent is a row of a table whose path its own extends: the closest one
        // holding the parent's id, as a column may be an array in some batches and an object
        // holding one in others.
        let parents: Vec<Vec<String>> = (1..path.len())
            .rev()
            .map(|length| path[..length].to_vec())
            .filter(|prefix| tables.contains(prefix))
            .collect();
        let table = Table {
            world,
            stream,
            path,
            parents: &parents,
            capabilities,
            at_most: false,
            seed,
        };
        let (ids, _) = table.check(&Slot::each(rows), &lineage);
        lineage.insert(path.clone(), ids);
    }
}

/// Each table's rows' identities by their lineage ids.
type Lineage = BTreeMap<Vec<String>, BTreeMap<String, String>>;

/// Rows a table holds `count` times in all, any of `rows` each time.
struct Slot<'a> {
    rows: Vec<&'a Expected>,
    count: usize,
}

impl<'a> Slot<'a> {
    /// A slot for each of `groups`, holding the rows of `modeled` each group's rows are.
    fn of(groups: &[Group], modeled: &'a [Expected]) -> Vec<Self> {
        let by_ident: BTreeMap<&str, &Expected> = modeled
            .iter()
            .map(|row| (row.ident.as_str(), row))
            .collect();
        groups
            .iter()
            .map(|group| Slot {
                rows: group
                    .rows
                    .iter()
                    .filter_map(|row| by_ident.get(expected::ident(row).as_str()).copied())
                    .collect(),
                count: group.count,
            })
            .collect()
    }

    /// How the table's rows, `held` so often by identity, miscount the slot, if they do: more
    /// often than it says, or, unless `at_most`, less often.
    fn miscounted(&self, held: &BTreeMap<String, usize>, at_most: bool) -> Option<String> {
        let idents: Vec<&str> = self.rows.iter().map(|row| row.ident.as_str()).collect();
        let found: usize = idents
            .iter()
            .map(|ident| held.get(*ident).copied().unwrap_or(0))
            .sum();
        (found > self.count || (found < self.count && !at_most))
            .then(|| format!("rows {idents:?} are held {found} times, not {}", self.count))
    }

    /// A slot for each of `rows`, held as often as it appears.
    fn each(rows: &'a [Expected]) -> Vec<Self> {
        let mut counts: BTreeMap<&str, (&Expected, usize)> = BTreeMap::new();
        for row in rows {
            counts.entry(&row.ident).or_insert((row, 0)).1 += 1;
        }
        counts
            .into_values()
            .map(|(row, count)| Slot {
                rows: vec![row],
                count,
            })
            .collect()
    }
}

/// One table being checked.
struct Table<'a> {
    world: &'a World,
    stream: &'a SimStream,
    path: &'a [String],
    /// The tables a child table's rows' parents may be rows of, closest first.
    parents: &'a [Vec<String>],
    /// What the destination stores.
    capabilities: &'a Capabilities,
    /// Whether the table may hold each slot's rows fewer times than the slot says.
    at_most: bool,
    seed: Seed,
}

impl Table<'_> {
    /// Checks the table's rows against `slots`; returns its rows' identities by lineage id, and
    /// how often it holds each.
    fn check(
        &self,
        slots: &[Slot<'_>],
        lineage: &Lineage,
    ) -> (BTreeMap<String, String>, BTreeMap<String, usize>) {
        let mut ids = BTreeMap::new();
        let mut held: BTreeMap<String, usize> = BTreeMap::new();
        let path = TablePath::new(self.path.iter().map(String::as_str))
            .expect("the model's table paths are valid");
        let Some(published) = published_table(self.world, &path) else {
            let expected: usize = slots.iter().map(|slot| slot.count).sum();
            assert!(
                expected == 0 || self.at_most,
                "seed {}: table {path} is missing; the model expects {expected} rows",
                self.seed,
            );
            return (ids, held);
        };
        self.check_names(&published);
        let mut templates: BTreeMap<&str, &Expected> = BTreeMap::new();
        for row in slots.iter().flat_map(|slot| &slot.rows) {
            templates.insert(&row.ident, row);
        }
        let mut findings = Vec::new();
        for row in &published.rows {
            let Some(ident) = self.identity(row, &published, lineage, &mut ids) else {
                findings.push("a row names no known parent".to_owned());
                continue;
            };
            let columns = fields(row);
            for column in names::unnamed(&columns, &published.names, self.meta()) {
                findings.push(format!(
                    "row {ident} has column {column}, which no name map names"
                ));
            }
            match templates.get(ident.as_str()) {
                Some(template) => self.compare(row, template, &published, &mut findings),
                None => findings.push(format!("row {ident} is not in the model")),
            }
            *held.entry(ident).or_default() += 1;
        }
        findings.extend(
            slots
                .iter()
                .filter_map(|slot| slot.miscounted(&held, self.at_most)),
        );
        assert!(
            findings.is_empty(),
            "seed {}: stream {} ({:?}, {:?}, {:?}, {:?}) table {path}: {} findings, the first \
             {:#?}",
            self.seed,
            self.stream.name,
            self.stream.read,
            self.stream.write,
            self.stream.schema,
            self.stream.pipeline,
            findings.len(),
            &findings[..findings.len().min(8)]
        );
        (ids, held)
    }

    /// The identity of `row`: its root row's id and value, then the array positions leading to
    /// it, read from its lineage; recording its own lineage id.
    fn identity(
        &self,
        row: &Stored,
        published: &Published,
        lineage: &Lineage,
        ids: &mut BTreeMap<String, String>,
    ) -> Option<String> {
        let fields: Vec<String> = row
            .row
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let cell = |name: &str| row.cells.get(name).cloned().unwrap_or(Canon::Null);
        // Metadata columns follow the model's columns: the row's id, and a child's parent's id,
        // root's id and position.
        let (ident, own) = if self.path.len() > 1 {
            let [own, parent, _, idx] = &fields[fields.len() - 4..] else {
                return None;
            };
            let parent_id = text(&cell(parent));
            let parent = self
                .parents
                .iter()
                .find_map(|table| lineage.get(table)?.get(&parent_id))?;
            let idx = text(&cell(idx));
            let ident = format!("{parent}/{}[{idx}]", self.path.join("."));
            (ident, Some(own.clone()))
        } else {
            let number = |column: &str| {
                let physical = published.names.get(&ColumnKey::Source(column.into()))?;
                Some(text(&cell(physical)))
            };
            let ident = format!("{}:{}", number("id")?, number("value")?);
            (
                ident,
                self.stream
                    .normalized()
                    .then(|| fields[fields.len() - 1].clone()),
            )
        };
        if let Some(own) = own {
            ids.insert(text(&cell(&own)), ident.clone());
        }
        Some(ident)
    }

    /// Compares `row`'s values with `template`'s, the row the model expects.
    fn compare(
        &self,
        row: &Stored,
        template: &Expected,
        published: &Published,
        findings: &mut Vec<String>,
    ) {
        let mut columns: BTreeMap<String, Vec<&str>> = BTreeMap::new();
        for (key, physical) in published.names.iter() {
            let path: Vec<&str> = key.column().segments().collect();
            columns.entry(path.join(".")).or_default().push(physical);
        }
        let paths: BTreeSet<&String> = columns.keys().chain(template.columns.keys()).collect();
        let written = TableSchema::from_arrow(&row.row.schema()).expect("stored rows have types");
        for path in paths {
            let sent = template.columns.get(path);
            let physicals = columns.get(path).map_or(&[][..], Vec::as_slice);
            let held: Vec<Cell<'_>> = physicals
                .iter()
                .filter_map(|physical| {
                    let source = sent.and_then(expected::Sent::source);
                    read(row, &written, physical, source)
                })
                .filter(|cell| cell.value != Canon::Null)
                .collect();
            let finding = match (sent, held.as_slice()) {
                (None, []) => None,
                (None, [cell]) => Some(format!(
                    "{path} holds {:?} in {}; the model expects no value",
                    cell.value, cell.physical
                )),
                (Some(sent), []) => Some(format!(
                    "{path} holds no value in {physicals:?}; the model expects {sent:?}"
                )),
                (Some(sent), [cell]) => {
                    let own = published
                        .names
                        .get(&ColumnKey::Source(ColumnPath::from(path.as_str())));
                    self.value(
                        cell,
                        sent,
                        own == Some(cell.physical),
                        template.placed.get(path),
                        self.native(path),
                    )
                    .map(|finding| format!("{path}: {finding}"))
                }
                (_, many) => Some(format!(
                    "{path} holds values in {:?}",
                    many.iter().map(|cell| cell.physical).collect::<Vec<_>>()
                )),
            };
            findings.extend(finding.map(|finding| format!("row of {}: {finding}", template.ident)));
        }
    }

    /// Whether the column at `path` stores nested values natively, where the destination can.
    fn native(&self, path: &str) -> bool {
        let column = self
            .stream
            .drift
            .iter()
            .position(|drift| self.path.len() == 1 && drift.name == path);
        self.stream.resolved(column, Relaxed::default()).nested == Nested::Native
    }

    /// Why `cell` does not hold `sent`, if it does not: it is not where the model `placed` it, its
    /// column's type does not hold the value's, the destination stores the column otherwise than
    /// its capabilities say, with nested values `native` where it can, or the cell means another
    /// value.
    fn value(
        &self,
        cell: &Cell<'_>,
        sent: &expected::Sent,
        own: bool,
        placed: Option<&Placement>,
        native: bool,
    ) -> Option<String> {
        let (physical, logical) = (cell.physical, &cell.logical);
        match placed {
            Some(Placement::Own(expected)) if !own || logical != expected => {
                return Some(format!(
                    "{physical} is {logical}; the model places the value in its own column, of \
                     {expected}"
                ));
            }
            Some(Placement::Variant) if own => {
                return Some(format!(
                    "{physical}, its own column, holds the value, which its type does not hold"
                ));
            }
            _ => {}
        }
        if let Some(source) = sent.source()
            && logical.join(source) != *logical
        {
            return Some(format!(
                "{physical} is {logical}, which does not hold {source}"
            ));
        }
        let stored = storage(logical, native, self.capabilities);
        if cell.lowered != stored {
            return Some(format!(
                "{physical}, {logical}, is stored as {}, not {stored}",
                cell.lowered
            ));
        }
        let expected = sent.meaning(logical);
        (cell.value != expected)
            .then(|| format!("{physical} holds {:?}, not {expected:?}", cell.value))
    }

    /// Checks the table's identifiers against the destination's rules, and each distinct.
    fn check_names(&self, published: &Published) {
        let rules = &self.capabilities.identifiers;
        let mut seen = BTreeSet::new();
        let columns = published.rows.iter().flat_map(fields);
        for name in std::iter::once(published.physical.clone()).chain(columns) {
            if seen.insert(name.clone()) {
                assert!(
                    names::fits(rules, &name),
                    "seed {}: identifier {name:?} of table {:?} breaks the destination's rules \
                     {rules:?}",
                    self.seed,
                    self.path
                );
            }
        }
    }

    /// How many metadata columns follow a row's source columns: the load id and load start, a
    /// merge table's sequence, and a normalized stream's lineage, a child's four columns of it.
    fn meta(&self) -> usize {
        let lineage = match (self.path.len() > 1, self.stream.normalized()) {
            (true, _) => 4,
            (false, true) => 1,
            (false, false) => 0,
        };
        2 + usize::from(self.stream.write == WriteMode::Merge) + lineage
    }
}
