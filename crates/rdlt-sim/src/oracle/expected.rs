//! What the reference model says each of a stream's tables holds: every kept row's values by
//! column path, normalized into child tables at any depth where the stream normalizes.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::LogicalType;
use rdlt_engine::{Nested, SchemaPolicy};
use rdlt_testkit::canon::{self, Canon};
use rdlt_testkit::decode;
use rdlt_testkit::drawn::Scalar;
use rdlt_testkit::drawn::json::rendered;
use serde_json::Value;

use crate::workload::{Drift, Row, SimStream};

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

/// One row a table must hold: its identity and each non-null value by column path.
#[derive(Clone, Debug)]
pub(super) struct Expected {
    /// Its root row's id and value, and the array positions leading to it from there.
    pub(super) ident: String,
    /// Its values, by column path, the path's segments joined by dots.
    pub(super) columns: BTreeMap<String, Sent>,
}

/// Each table's rows, by the table's path.
pub(super) type Tables = BTreeMap<Vec<String>, Vec<Expected>>;

/// The tables of `stream` holding `rows`, the rows its policy keeps, each as often as its table
/// holds it.
pub(super) fn tables(stream: &SimStream, rows: &[Row]) -> Tables {
    let mut tables = Tables::new();
    tables.insert(vec![stream.name.clone()], Vec::new());
    let max_depth = match stream.nested {
        Nested::Normalize { max_depth } => Some(max_depth),
        _ => None,
    };
    for row in rows {
        let ident = format!("{}:{}", row.id, row.value);
        let mut pending = Pending::default();
        let int = |value: i64| Sent::Typed(Scalar::Int(value), LogicalType::Int64);
        pending.column(&["id".into()], int(row.id));
        pending.column(&["partition".into()], int(row.partition));
        pending.column(&["offset".into()], int(row.offset));
        pending.column(&["value".into()], int(row.value));
        if let Some(key) = row.key {
            pending.column(&["key".into()], int(key));
        }
        if stream.policy != SchemaPolicy::DiscardValue {
            for (drift, extra) in stream.drift.iter().zip(&row.extras) {
                let Some(extra) = extra else { continue };
                let node = node(stream, drift_type(row, drift), extra);
                let path = vec![drift.name.clone()];
                match max_depth {
                    Some(max_depth) => pending.place(path, node, 1, max_depth),
                    None => pending.whole(&path, node),
                }
            }
        }
        pending.emit(
            std::slice::from_ref(&stream.name),
            &ident,
            max_depth.unwrap_or(0),
            &mut tables,
        );
    }
    tables
}

/// Whether `stream`'s policy keeps `row`: a stream that discards rows drops every row holding a
/// drift value, as drift columns are never declared.
pub(super) fn kept(stream: &SimStream, row: &Row) -> bool {
    stream.policy != SchemaPolicy::DiscardRow || drift_values(stream, row) == 0
}

/// The drift values `row` holds, which its policy counts as changes: each non-null value, or where
/// the stream normalizes, each value column and array item its values normalize into, so an empty
/// array or an object of nulls holds none.
pub(super) fn drift_values(stream: &SimStream, row: &Row) -> u64 {
    let nodes = stream
        .drift
        .iter()
        .zip(&row.extras)
        .filter_map(|(drift, extra)| Some((drift, extra.as_ref()?)))
        .map(|(drift, extra)| (drift, node(stream, drift_type(row, drift), extra)))
        .filter(|(_, node)| !matches!(node.kind(), Kind::Null));
    let Nested::Normalize { max_depth } = stream.nested else {
        return nodes.count() as u64;
    };
    let mut pending = Pending {
        counting: true,
        ..Pending::default()
    };
    for (drift, node) in nodes {
        pending.place(vec![drift.name.clone()], node, 1, max_depth);
    }
    let items: usize = pending.arrays.iter().map(|(_, items, _)| items.len()).sum();
    (pending.columns.len() + items) as u64
}

/// The type each drift column of `stream` must have where every batch of `rows`, the rows
/// delivered so far, that holds it holds it at one type: a column the policy keeps whole, and which
/// never meets another type, has no reason to widen or split.
pub(super) fn uniform(stream: &SimStream, rows: &[Row]) -> BTreeMap<String, LogicalType> {
    if stream.normalized() || stream.policy != SchemaPolicy::Evolve {
        return BTreeMap::new();
    }
    let mut uniform = BTreeMap::new();
    'columns: for (index, drift) in stream.drift.iter().enumerate() {
        let mut types = BTreeMap::new();
        for row in rows {
            let Some(value) = &row.extras[index] else {
                continue;
            };
            let logical = if stream.json {
                match pushed_type(value) {
                    Pushed::Typed(logical) => logical,
                    Pushed::Null => continue,
                    Pushed::Container => continue 'columns,
                }
            } else {
                drift_type(row, drift).clone()
            };
            if logical != LogicalType::Null {
                types.insert(logical.to_string(), logical);
            }
        }
        if types.len() == 1
            && let Some((_, logical)) = types.pop_first()
        {
            uniform.insert(drift.name.clone(), logical);
        }
    }
    uniform
}

/// What the engine infers for a value pushed in JSON.
#[derive(Debug, PartialEq)]
enum Pushed {
    /// A null, which has no type.
    Null,
    /// A scalar, of this type.
    Typed(LogicalType),
    /// An object or array, whose inferred type this does not model.
    Container,
}

/// What the engine infers for `value`, pushed in JSON.
fn pushed_type(value: &Scalar) -> Pushed {
    match value {
        Scalar::Null => Pushed::Null,
        Scalar::Bool(_) => Pushed::Typed(LogicalType::Bool),
        Scalar::Int(_) => Pushed::Typed(LogicalType::Int64),
        Scalar::Float64(float) if float.is_finite() => Pushed::Typed(LogicalType::Float64),
        // A float JSON cannot hold is pushed as its name.
        Scalar::Float64(_) | Scalar::Utf8(_) => Pushed::Typed(LogicalType::Utf8),
        _ => Pushed::Container,
    }
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

    /// Adds this row, of the table at `table`, then the rows of its arrays' child tables.
    fn emit(self, table: &[String], ident: &str, max_depth: u8, tables: &mut Tables) {
        tables.entry(table.to_vec()).or_default().push(Expected {
            ident: ident.to_owned(),
            columns: self.columns,
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
                row.emit(&child_table, &child, max_depth, tables);
            }
        }
    }
}
