//! Each of a stream's tables against the reference model: every row the model expects, as often,
//! each value read back from the cell holding it as the schema it was written under says, in a
//! column whose type holds the value's and is stored as the destination's capabilities say.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{
    ColumnKey, ColumnPath, Field, IdentifierCase, IdentifierChars, LogicalType, TablePath,
    TableSchema,
};
use rdlt_engine::Nested;
use rdlt_testkit::canon::{Canon, storage};
use rdlt_testkit::decode;

use super::expected::{self, Expected};
use crate::destination::{Published, Stored, published_table, table_paths};
use crate::seed::Seed;
use crate::workload::{Row, SimStream};
use crate::world::World;

/// Checks every table of `stream` against the model's tables for `rows`, the rows its table
/// holds, of `delivered`, every row delivered so far.
pub(super) fn check(
    world: &World,
    stream: &SimStream,
    rows: &[Row],
    delivered: &[Row],
    seed: Seed,
) {
    let expected = expected::tables(stream, rows);
    let uniform = expected::uniform(stream, delivered);
    let none = BTreeMap::new();
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
    let mut lineage: Lineage = BTreeMap::new();
    for (_, path) in &paths {
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
            uniform: if path.len() == 1 { &uniform } else { &none },
            seed,
        };
        let ids = table.check(rows, &lineage);
        lineage.insert(path.clone(), ids);
    }
}

/// Each table's rows' identities by their lineage ids.
type Lineage = BTreeMap<Vec<String>, BTreeMap<String, String>>;

/// One table being checked.
struct Table<'a> {
    world: &'a World,
    stream: &'a SimStream,
    path: &'a [String],
    /// The tables a child table's rows' parents may be rows of, closest first.
    parents: &'a [Vec<String>],
    /// The type each column a single type only ever arrived at has, by path.
    uniform: &'a BTreeMap<String, LogicalType>,
    seed: Seed,
}

impl Table<'_> {
    /// Checks the table's rows against `expected`; returns its rows' identities by lineage id.
    fn check(&self, expected: &[Expected], lineage: &Lineage) -> BTreeMap<String, String> {
        let mut ids = BTreeMap::new();
        let path = TablePath::new(self.path.iter().map(String::as_str))
            .expect("the model's table paths are valid");
        let Some(published) = published_table(self.world, &path) else {
            assert!(
                expected.is_empty(),
                "seed {}: table {path} is missing; the model expects {} rows",
                self.seed,
                expected.len()
            );
            return ids;
        };
        self.check_names(&published);
        let mut templates: BTreeMap<&str, (&Expected, usize)> = BTreeMap::new();
        for row in expected {
            templates.entry(&row.ident).or_insert((row, 0)).1 += 1;
        }
        let mut held: BTreeMap<String, usize> = BTreeMap::new();
        let mut findings = Vec::new();
        for row in &published.rows {
            let Some(ident) = self.identity(row, &published, lineage, &mut ids) else {
                findings.push("a row names no known parent".to_owned());
                continue;
            };
            match templates.get(ident.as_str()) {
                Some((template, _)) => self.compare(row, template, &published, &mut findings),
                None => findings.push(format!("row {ident} is not in the model")),
            }
            *held.entry(ident).or_default() += 1;
        }
        for (ident, (_, count)) in &templates {
            let found = held.get(*ident).copied().unwrap_or(0);
            if found != *count {
                findings.push(format!("row {ident} is held {found} times, not {count}"));
            }
        }
        assert!(
            findings.is_empty(),
            "seed {}: stream {} ({:?}, {:?}, {:?}, {:?}) table {path}: {} findings, the first \
             {:#?}",
            self.seed,
            self.stream.name,
            self.stream.read,
            self.stream.write,
            self.stream.policy,
            self.stream.nested,
            findings.len(),
            &findings[..findings.len().min(8)]
        );
        ids
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
                        self.uniform.get(path),
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

    /// Why `cell` does not hold `sent`, if it does not: its column's type does not hold the
    /// value's, the destination stores the column otherwise than its capabilities say, or the cell
    /// means another value.
    fn value(
        &self,
        cell: &Cell<'_>,
        sent: &expected::Sent,
        own: bool,
        uniform: Option<&LogicalType>,
    ) -> Option<String> {
        let (physical, logical) = (cell.physical, &cell.logical);
        if let Some(uniform) = uniform
            && (!own || logical != uniform)
        {
            return Some(format!(
                "{physical} is {logical}, but every value of its column arrived as {uniform}, \
                 which its own column keeps"
            ));
        }
        if let Some(source) = sent.source()
            && logical.join(source) != *logical
        {
            return Some(format!(
                "{physical} is {logical}, which does not hold {source}"
            ));
        }
        let native = self.stream.nested == Nested::Native;
        let stored = storage(logical, native, &self.world.capabilities);
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

    /// Checks the table's identifiers against the destination's rules: within its length, in its
    /// case and characters, none reserved, and each distinct.
    fn check_names(&self, published: &Published) {
        let rules = &self.world.capabilities.identifiers;
        let mut seen = BTreeSet::new();
        let columns = published.rows.iter().flat_map(|row| {
            let schema = row.row.schema();
            let names: Vec<String> = schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            names
        });
        for name in std::iter::once(published.physical.clone()).chain(columns) {
            if !seen.insert(name.clone()) {
                continue;
            }
            let fits = name.len() <= usize::from(rules.max_len.get())
                && match rules.case {
                    IdentifierCase::Lower => !name.chars().any(char::is_uppercase),
                    IdentifierCase::Upper => !name.chars().any(char::is_lowercase),
                    IdentifierCase::Preserve => true,
                }
                && match rules.chars {
                    IdentifierChars::AsciiWord => {
                        name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    }
                    IdentifierChars::Any => !name.chars().any(char::is_control),
                }
                && !rules
                    .reserved
                    .iter()
                    .any(|word| word.eq_ignore_ascii_case(&name));
            assert!(
                fits,
                "seed {}: identifier {name:?} of table {:?} breaks the destination's rules \
                 {rules:?}",
                self.seed, self.path
            );
        }
    }
}

/// A cell's meaning as text: a number's or a hex id's digits.
fn text(value: &Canon) -> String {
    match value {
        Canon::Number(text) | Canon::Bytes(text) | Canon::Text(text) => text.clone(),
        other => format!("{other:?}"),
    }
}

/// The field of `schema` called `name`.
fn field<'a>(schema: &'a TableSchema, name: &str) -> Option<&'a Field> {
    schema.fields().iter().find(|field| field.name() == name)
}

/// One cell of a row, read.
struct Cell<'a> {
    physical: &'a str,
    /// The logical type it was written with.
    logical: LogicalType,
    /// The type the row stores it as.
    lowered: LogicalType,
    value: Canon,
}

/// The cell of `physical` in `row`, read as the logical type the row was written with, with
/// JSON in it read as `source` says; `None` where the row lacks the column.
fn read<'a>(
    row: &Stored,
    written: &TableSchema,
    physical: &'a str,
    source: Option<&LogicalType>,
) -> Option<Cell<'a>> {
    let array = row.row.column_by_name(physical)?;
    let lowered = field(written, physical)?.logical_type().clone();
    let arrow = row.row.schema();
    let named = Field::lowered_from(arrow.field_with_name(physical).ok()?);
    let logical = named.unwrap_or_else(|| lowered.clone());
    let hint = decode::hint(&logical, source);
    let value = decode::cell(array.as_ref(), 0, &logical, &lowered, &hint);
    Some(Cell {
        physical,
        logical,
        lowered,
        value,
    })
}
