//! What the reference model says each of a stream's tables holds: every kept row's values by
//! column path, normalized into child tables at any depth where the stream normalizes.

mod discards;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::LogicalType;
use rdlt_engine::SchemaPolicy;
use rdlt_testkit::canon::{self, Canon};
use rdlt_testkit::decode;
use rdlt_testkit::drawn::Scalar;
use rdlt_testkit::drawn::json::rendered;
use serde_json::Value;

use super::arrivals::{Arrival, all, arrival, fixed, widest};
use crate::workload::{Drift, Relaxed, Row, SimStream};
pub(super) use discards::{Chance, Discards, discards, dropped, pruned};

/// A value the source sent, which its cell must mean exactly.
#[derive(Clone, Debug)]
pub(super) enum Sent {
    /// A value of an Arrow batch's column, of the column's type.
    Typed(Scalar, LogicalType),
    /// A value of a JSON push, whose type the engine infers.
    Json(Value),
}

impl Sent {
    /// What the value means once held by a column of `column`.
    pub(super) fn meaning(&self, column: &LogicalType) -> Canon {
        match self {
            Self::Typed(value, source) => canon::canonical_into(value, source, column),
            Self::Json(value) => decode::json_as(&value.to_string(), column),
        }
    }

    /// The type the source sent the value as, where it says.
    pub(super) fn source(&self) -> Option<&LogicalType> {
        match self {
            Self::Typed(_, source) => Some(source),
            Self::Json(_) => None,
        }
    }
}

/// Where a value must sit among its column's own and variant columns.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Placement {
    /// In the column's own column, of this type when written.
    Own(LogicalType),
    /// In one of its variant columns.
    Variant,
    /// In any one of them.
    Any,
}

/// One row a table must hold: its identity and each non-null value by column path.
#[derive(Clone, Debug)]
pub(super) struct Expected {
    /// Its root row's id and value, and the array positions leading to it from there.
    pub(super) ident: String,
    /// Its values, by column path, the path's segments joined by dots.
    pub(super) columns: BTreeMap<String, Sent>,
    /// Where each value whose place the model knows must sit, by column path.
    pub(super) placed: BTreeMap<String, Placement>,
    /// The paths of values its policy may have discarded, which the table may lack.
    pub(super) optional: BTreeSet<String>,
}

/// Each table's rows, by the table's path.
pub(super) type Tables = BTreeMap<Vec<String>, Vec<Expected>>;

/// The tables of `stream` holding `rows`, the rows its policies keep, each as often as its table
/// holds it, of `delivered`, every row delivered so far.
pub(super) fn tables(stream: &SimStream, rows: &[Row], delivered: &[Row]) -> Tables {
    let mut tables = Tables::new();
    tables.insert(vec![stream.name.clone()], Vec::new());
    let max_depth = stream.max_depth();
    let owned: Vec<Option<LogicalType>> = (0..stream.drift.len())
        .map(|column| owned(stream, delivered, column))
        .collect();
    for row in rows {
        let mut pending = Pending::default();
        base(stream, row, &mut pending);
        let placed = drifted(stream, row, &owned, &mut pending);
        pending.emit(
            std::slice::from_ref(&stream.name),
            &ident(row),
            max_depth.unwrap_or(0),
            placed,
            &mut tables,
        );
    }
    tables
}

/// Records the values of `row`'s base columns in `pending`: its position, value and key.
fn base(stream: &SimStream, row: &Row, pending: &mut Pending) {
    let int = |value: i64| Sent::Typed(Scalar::Int(value), LogicalType::Int64);
    pending.column(&["id".into()], int(row.id));
    pending.column(&["partition".into()], int(row.partition));
    pending.column(&["offset".into()], int(row.offset));
    pending.column(&["value".into()], int(row.value));
    if row.key.is_some() {
        let partition = usize::try_from(row.partition).unwrap_or(0);
        let logical = stream.key_type(partition, row.delivered);
        pending.column(
            &["key".into()],
            Sent::Typed(row.key_value(logical), logical.clone()),
        );
    }
    if let Some(tag) = &row.tag {
        pending.column(
            &["tag".into()],
            Sent::Typed(Scalar::Utf8(tag.clone()), LogicalType::Utf8),
        );
    }
}

/// Records the values of `row`'s drift columns its policies keep in `pending`; returns where each
/// value of a column whose own column's type is `owned` must sit.
fn drifted(
    stream: &SimStream,
    row: &Row,
    owned: &[Option<LogicalType>],
    pending: &mut Pending,
) -> BTreeMap<String, Placement> {
    let mut placed = BTreeMap::new();
    for (column, drift) in stream.drift.iter().enumerate() {
        let Some(extra) = &row.extras[column] else {
            continue;
        };
        if policy(stream, column) == SchemaPolicy::DiscardValue {
            match changed(stream, row, column) {
                Chance::Surely => continue,
                Chance::Perhaps => {
                    pending.optional.insert(drift.name.clone());
                }
                Chance::Never => {}
            }
        }
        let node = node(stream, drift_type(row, drift), extra);
        let path = vec![drift.name.clone()];
        match stream.max_depth() {
            Some(max_depth) if !stream.whole(column) => pending.place(path, node, 1, max_depth),
            _ => pending.whole(&path, node),
        }
        if let Some(own) = &owned[column] {
            let fits = |arrival: Option<Arrival>| arrival.and_then(|arrival| arrival.fits(own));
            let placement = match (
                fits(arrival(stream, row, column)),
                fits(widest(stream, row, column)),
            ) {
                (Some(false), _) => Placement::Variant,
                (Some(true), Some(true)) => Placement::Own(own.clone()),
                _ => Placement::Any,
            };
            placed.insert(drift.name.clone(), placement);
        }
    }
    placed
}

/// The identity of `row`'s row in its stream's table: its id and value.
pub(super) fn ident(row: &Row) -> String {
    format!("{}:{}", row.id, row.value)
}

/// The policy drift column `column` of `stream` resolves to; how it discards does not depend on
/// what an operator relaxes.
fn policy(stream: &SimStream, column: usize) -> SchemaPolicy {
    stream.resolved(Some(column), Relaxed::default()).policy
}

/// Whether drift column `column`'s batch holding `row` is a change its policy discards: a column
/// its table lacks, of a type, or one the column's fixed type does not hold.
///
/// A value whose own push the fixed type holds is changed only perhaps where it may be shredded
/// together with a push the type does not hold.
fn changed(stream: &SimStream, row: &Row, column: usize) -> Chance {
    if !matches!(
        policy(stream, column),
        SchemaPolicy::DiscardRow | SchemaPolicy::DiscardValue
    ) {
        return Chance::Never;
    }
    let Some(arrival) = arrival(stream, row, column) else {
        return Chance::Never;
    };
    let Some(fixed) = fixed(stream, column) else {
        return if arrival.is_null() {
            Chance::Never
        } else {
            Chance::Surely
        };
    };
    let unfit =
        |arrival: Option<Arrival>| arrival.and_then(|arrival| arrival.fits(&fixed)) == Some(false);
    if unfit(Some(arrival)) {
        Chance::Surely
    } else if unfit(widest(stream, row, column)) {
        Chance::Perhaps
    } else {
        Chance::Never
    }
}

/// The type drift column `column`'s own column has whenever it takes a value, if the model knows
/// it.
///
/// A hinted type never changes; a declared one changes only for a value it does not hold, which a
/// column that discards never takes; an undeclared column that evolves takes the type of the first
/// batch holding it and keeps it while every batch arrives as that type. Values nested in a stream
/// that normalizes have none.
fn owned(stream: &SimStream, delivered: &[Row], column: usize) -> Option<LogicalType> {
    if stream.normalized() && !stream.whole(column) {
        return None;
    }
    let drift = &stream.drift[column];
    if let Some(hint) = &drift.hint {
        return Some(hint.clone());
    }
    let arrivals = all(stream, delivered, column);
    let arrivals: Vec<&Arrival> = arrivals
        .iter()
        .filter(|arrival| !arrival.is_null())
        .collect();
    match (&drift.declared, policy(stream, column)) {
        (Some(declared), SchemaPolicy::DiscardRow | SchemaPolicy::DiscardValue) => {
            Some(declared.clone())
        }
        (Some(declared), _) => arrivals
            .iter()
            .all(|arrival| arrival.fits(declared) == Some(true))
            .then(|| declared.clone()),
        (None, SchemaPolicy::DiscardRow | SchemaPolicy::DiscardValue) => None,
        (None, _) => match arrivals.as_slice() {
            [Arrival::Typed(only)] => Some(only.clone()),
            _ => None,
        },
    }
}

/// The rows `node`, the value of column `name`, adds to child tables, normalized to `max_depth`.
fn child_rows(name: &str, node: Node, max_depth: u8) -> u64 {
    let mut pending = Pending::default();
    pending.place(vec![name.to_owned()], node, 1, max_depth);
    let mut tables = Tables::new();
    pending.emit(
        &[String::new()],
        "",
        max_depth,
        BTreeMap::new(),
        &mut tables,
    );
    tables
        .iter()
        .filter(|(path, _)| path.len() > 1)
        .map(|(_, rows)| rows.len() as u64)
        .sum()
}

/// The values `nodes`, drift columns' by name, count as: each non-null one, or, normalized to
/// `max_depth`, each value column and array item it normalizes into.
fn counted(nodes: Vec<(String, Node)>, max_depth: Option<u8>) -> u64 {
    let nodes = nodes
        .into_iter()
        .filter(|(_, node)| !matches!(node.kind(), Kind::Null));
    let Some(max_depth) = max_depth else {
        return nodes.count() as u64;
    };
    let mut pending = Pending {
        counting: true,
        ..Pending::default()
    };
    for (name, node) in nodes {
        pending.place(vec![name], node, 1, max_depth);
    }
    let items: usize = pending.arrays.iter().map(|(_, items, _)| items.len()).sum();
    (pending.columns.len() + items) as u64
}

/// The type of `drift`'s values in `row`'s batch.
fn drift_type<'a>(row: &Row, drift: &'a Drift) -> &'a LogicalType {
    let partition = usize::try_from(row.partition).unwrap_or(0);
    &drift.shapes[partition][row.delivered]
        .as_ref()
        .expect("a row holding a drift value has its column")
        .logical
}

/// `value` of `logical` as the stream sent it.
fn node(stream: &SimStream, logical: &LogicalType, value: &Scalar) -> Node {
    if stream.json {
        Node::Json(rendered(value))
    } else {
        Node::Typed(value.clone(), logical.clone())
    }
}

/// A value while it normalizes: typed, from Arrow, or JSON.
#[derive(Clone, Debug)]
enum Node {
    Typed(Scalar, LogicalType),
    Json(Value),
}

/// What a node is to normalizing.
enum Kind {
    Null,
    Object(Vec<(String, Node)>),
    Array(Vec<Node>),
    Leaf,
}

impl Node {
    fn kind(&self) -> Kind {
        match self {
            Self::Typed(Scalar::Null, _) | Self::Json(Value::Null) => Kind::Null,
            Self::Typed(Scalar::Struct(fields), LogicalType::Struct(types)) => Kind::Object(
                fields
                    .iter()
                    .filter_map(|(name, inner)| {
                        let field = types.iter().find(|field| field.name() == name)?;
                        let inner = Self::Typed(inner.clone(), field.logical_type().clone());
                        Some((name.clone(), inner))
                    })
                    .collect(),
            ),
            Self::Typed(Scalar::List(items), LogicalType::List(item)) => Kind::Array(
                items
                    .iter()
                    .map(|inner| Self::Typed(inner.clone(), item.logical_type().clone()))
                    .collect(),
            ),
            Self::Json(Value::Object(members)) => Kind::Object(
                members
                    .iter()
                    .map(|(name, inner)| (name.clone(), Self::Json(inner.clone())))
                    .collect(),
            ),
            Self::Json(Value::Array(items)) => Kind::Array(
                items
                    .iter()
                    .map(|inner| Self::Json(inner.clone()))
                    .collect(),
            ),
            _ => Kind::Leaf,
        }
    }

    fn sent(self) -> Sent {
        match self {
            Self::Typed(value, logical) => Sent::Typed(value, logical),
            Self::Json(value) => Sent::Json(value),
        }
    }
}

/// One row's columns and arrays while it normalizes, as the engine's reference normalizer places
/// them.
#[derive(Default)]
struct Pending {
    columns: BTreeMap<String, Sent>,
    arrays: Vec<(Vec<String>, Vec<Node>, u8)>,
    /// The paths of values its policy may have discarded.
    optional: BTreeSet<String>,
    /// Whether the row's values are being counted, rather than stored: JSON's `null` counts.
    counting: bool,
}

impl Pending {
    /// Records `sent` at `path`, unless it is JSON's `null`: a value its batch holds, and its
    /// policy counts, but which means no value once stored.
    fn column(&mut self, path: &[String], sent: Sent) {
        if self.counting || !matches!(sent, Sent::Typed(Scalar::Json(Value::Null), _)) {
            self.columns.insert(path.join("."), sent);
        }
    }

    /// Stores `node` whole at `path`, as a stream that does not normalize does.
    fn whole(&mut self, path: &[String], node: Node) {
        if !matches!(node.kind(), Kind::Null) {
            self.column(path, node.sent());
        }
    }

    fn place(&mut self, path: Vec<String>, node: Node, depth: u8, max_depth: u8) {
        if depth > max_depth {
            return self.whole(&path, node);
        }
        match node.kind() {
            Kind::Null => {}
            Kind::Object(fields) => {
                for (name, field) in fields {
                    let mut field_path = path.clone();
                    field_path.push(name);
                    self.place(field_path, field, depth + 1, max_depth);
                }
            }
            Kind::Array(items) => self.arrays.push((path, items, depth)),
            Kind::Leaf => self.column(&path, node.sent()),
        }
    }

    /// Adds this row, of the table at `table`, with its values `placed`, then the rows of its
    /// arrays' child tables.
    fn emit(
        self,
        table: &[String],
        ident: &str,
        max_depth: u8,
        placed: BTreeMap<String, Placement>,
        tables: &mut Tables,
    ) {
        tables.entry(table.to_vec()).or_default().push(Expected {
            ident: ident.to_owned(),
            columns: self.columns,
            placed,
            optional: self.optional,
        });
        for (path, items, depth) in self.arrays {
            let mut child_table = table.to_vec();
            child_table.extend(path);
            for (position, item) in items.into_iter().enumerate() {
                let child = format!("{ident}/{}[{position}]", child_table.join("."));
                let mut row = Self::default();
                let item_depth = depth + 1;
                match item.kind() {
                    Kind::Object(fields) if item_depth <= max_depth => {
                        for (name, field) in fields {
                            row.place(vec![name], field, item_depth + 1, max_depth);
                        }
                    }
                    Kind::Array(inner) if item_depth <= max_depth => {
                        row.arrays
                            .push((vec!["value".to_owned()], inner, item_depth));
                    }
                    _ => row.whole(&["value".to_owned()], item),
                }
                row.emit(&child_table, &child, max_depth, BTreeMap::new(), tables);
            }
        }
    }
}
