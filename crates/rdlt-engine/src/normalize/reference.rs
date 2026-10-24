//! A reference normalizer over JSON values, written apart from the Arrow one, and the comparison
//! of the two: the same rows in the same tables, each row's lineage naming exactly one row, and no
//! id naming two.
//!
//! A row is known by where it comes from: its record's position among the records, then, for a
//! child row, its array's table and its position in the array, after its parent's. A merge
//! stream's root rows carry their key's hash too, which the reference computes from its own
//! canonical encoding of the JSON values.
//!
//! A value of a JSON push's path that mixes kinds within its chunk is a column of JSON, which
//! normalizing keeps whole: the reference reads which from the fields of the batch each record
//! was shredded into.

pub(crate) mod compared;

use std::collections::BTreeMap;
use std::fmt::Display;

use arrow_schema::{DataType, FieldRef, Fields};
use serde_json::Value as Json;

/// A normalized row: where it comes from, a merge root's key hash, then its non-null columns by
/// path, each as canonical JSON text.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Row {
    pub(crate) at: String,
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) columns: BTreeMap<Vec<String>, String>,
}

/// Every table's rows, by the table's path below the stream's table.
pub(crate) type Tables = BTreeMap<Vec<String>, Vec<Row>>;

/// A record, and the fields of the batch it was shredded into or built in.
#[derive(Clone, Copy)]
pub(crate) struct Record<'a> {
    pub(crate) value: &'a Json,
    pub(crate) fields: &'a Fields,
}

/// Normalizes `records` as a stream with `max_depth` whose root rows are identified by `key`
/// where a merge identifies them, keeping the top-level columns `whole` whole.
pub(crate) fn normalize(
    records: &[Record<'_>],
    max_depth: u8,
    key: &[&str],
    whole: &[&str],
) -> Tables {
    let mut tables = Tables::new();
    for (position, record) in records.iter().enumerate() {
        let object = record.value.as_object().expect("records are objects");
        let key = (!key.is_empty()).then(|| {
            let mut encoding = Vec::new();
            for column in key {
                encode(object.get(*column).unwrap_or(&Json::Null), &mut encoding);
            }
            hash(ROOT, &encoding)
        });
        let mut row = Pending::default();
        for (name, value) in object {
            if whole.contains(&name.as_str()) {
                row.column(vec![name.clone()], value);
            } else {
                let field = record.fields.find(name).map(|(_, field)| field);
                row.place(vec![name.clone()], (value, field), 1, max_depth);
            }
        }
        row.emit(&[], &format!("#{position}"), key, max_depth, &mut tables);
    }
    sorted(tables)
}

/// Where the row at `position` of an array whose items are rows of `table`, in the row at
/// `parent`, comes from.
fn step(parent: &str, table: &[String], position: impl Display) -> String {
    format!("{parent}/{table:?}[{position}]")
}

/// One row's columns and arrays while it normalizes.
#[derive(Default)]
struct Pending<'a> {
    columns: BTreeMap<Vec<String>, String>,
    arrays: Vec<Items<'a>>,
}

/// An array of a row being normalized: its path in the row, its items, their field and depth.
struct Items<'a> {
    path: Vec<String>,
    values: Vec<Json>,
    field: Option<&'a FieldRef>,
    depth: u8,
}

/// Whether `field` is a column of JSON, which holds its values whole whatever they are.
fn json(field: Option<&FieldRef>) -> bool {
    field.is_some_and(|field| {
        field
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str)
            == Some("arrow.json")
    })
}

/// The field of `field`'s member `name`, where `field` is an object.
fn member<'a>(field: Option<&'a FieldRef>, name: &str) -> Option<&'a FieldRef> {
    match field?.data_type() {
        DataType::Struct(fields) => fields.find(name).map(|(_, field)| field),
        _ => None,
    }
}

/// The field of `field`'s items, where `field` is an array.
fn item(field: Option<&FieldRef>) -> Option<&FieldRef> {
    match field?.data_type() {
        DataType::List(item) => Some(item),
        _ => None,
    }
}

impl<'a> Pending<'a> {
    fn column(&mut self, path: Vec<String>, value: &Json) {
        if !value.is_null() {
            self.columns.insert(path, canonical_text(value));
        }
    }

    /// Places `value`, of `field`, at `path`, its depth `depth`.
    fn place(
        &mut self,
        path: Vec<String>,
        (value, field): (&Json, Option<&'a FieldRef>),
        depth: u8,
        max_depth: u8,
    ) {
        if depth > max_depth || json(field) {
            return self.column(path, value);
        }
        match value {
            Json::Object(object) => {
                for (name, inner) in object {
                    let mut inner_path = path.clone();
                    inner_path.push(name.clone());
                    let inner = (inner, member(field, name));
                    self.place(inner_path, inner, depth + 1, max_depth);
                }
            }
            Json::Array(items) => self.arrays.push(Items {
                path,
                values: items.clone(),
                field: item(field),
                depth,
            }),
            _ => self.column(path, value),
        }
    }

    /// Adds this row, of the table at `table`, which comes from `at`, then the rows of its
    /// arrays' child tables.
    fn emit(
        self,
        table: &[String],
        at: &str,
        key: Option<Vec<u8>>,
        max_depth: u8,
        tables: &mut Tables,
    ) {
        tables.entry(table.to_vec()).or_default().push(Row {
            at: at.to_owned(),
            key,
            columns: self.columns,
        });
        for Items {
            path,
            values: items,
            field,
            depth,
        } in self.arrays
        {
            let mut child_table = table.to_vec();
            child_table.extend(path);
            for (position, value) in items.iter().enumerate() {
                let mut row = Pending::default();
                let item_depth = depth + 1;
                let whole = item_depth > max_depth || json(field);
                match value {
                    Json::Object(object) if !whole => {
                        for (name, inner) in object {
                            let inner = (inner, member(field, name));
                            row.place(vec![name.clone()], inner, item_depth + 1, max_depth);
                        }
                    }
                    Json::Array(inner) if !whole => row.arrays.push(Items {
                        path: vec!["value".to_owned()],
                        values: inner.clone(),
                        field: item(field),
                        depth: item_depth,
                    }),
                    _ => row.column(vec!["value".to_owned()], value),
                }
                let child_at = step(at, &child_table, position);
                row.emit(&child_table, &child_at, None, max_depth, tables);
            }
        }
    }
}

/// The tag of a merge root's id.
const ROOT: u8 = 0x01;

/// The BLAKE3 hash of `bytes` after the tag of their `domain`.
fn hash(domain: u8, bytes: &[u8]) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[domain]);
    hasher.update(bytes);
    hasher.finalize().as_bytes().to_vec()
}

/// `bytes` after their length in LEB128.
fn length(out: &mut Vec<u8>, bytes: &[u8]) {
    let mut rest = bytes.len();
    while rest >= 0x80 {
        out.push(u8::try_from(rest & 0x7f).expect("seven bits") | 0x80);
        rest >>= 7;
    }
    out.push(u8::try_from(rest).expect("under 0x80"));
    out.extend_from_slice(bytes);
}

/// The canonical encoding of `value`, as the specification of the Arrow one reads.
fn encode(value: &Json, out: &mut Vec<u8>) {
    match value {
        Json::Null => out.push(b'n'),
        Json::Bool(true) => out.push(b't'),
        Json::Bool(false) => out.push(b'f'),
        Json::Number(number) => {
            let text = match (number.as_i64(), number.as_u64(), number.as_f64()) {
                (Some(integer), _, _) => integer.to_string(),
                (_, Some(integer), _) => integer.to_string(),
                (_, _, Some(float)) => float_text(float),
                _ => unreachable_number(),
            };
            out.push(b'd');
            length(out, text.as_bytes());
        }
        Json::String(text) => {
            out.push(b's');
            length(out, text.as_bytes());
        }
        Json::Array(items) => {
            out.push(b'[');
            for item in items {
                encode(item, out);
            }
            out.push(b']');
        }
        Json::Object(object) => {
            out.push(b'{');
            let mut fields: Vec<(&String, &Json)> = object
                .iter()
                .filter(|(_, value)| !value.is_null())
                .collect();
            fields.sort_by_key(|(name, _)| *name);
            for (name, field) in fields {
                out.push(b'k');
                length(out, name.as_bytes());
                encode(field, out);
            }
            out.push(b'}');
        }
    }
}

/// A float's shortest round-trip text, a tie going to the even one, in plain notation; zero
/// without its sign.
fn float_text(float: f64) -> String {
    if float == 0.0 {
        return "0".to_owned();
    }
    let written = serde_json::to_string(&float).expect("a finite float writes");
    let (mantissa, exponent) = written.split_once('e').unwrap_or((&written, "0"));
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.trim_start_matches('-');
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    // The point's place among the digits, which may lie beyond either end.
    let point =
        i64::try_from(whole.len()).expect("short") + exponent.parse::<i64>().expect("an exponent");
    let width = i64::try_from(digits.len()).expect("short");
    let padded = |zeros: i64| "0".repeat(usize::try_from(zeros).expect("not negative"));
    let plain = if point <= 0 {
        format!("0.{}{digits}", padded(-point))
    } else if point >= width {
        format!("{digits}{}", padded(point - width))
    } else {
        let (before, after) = digits.split_at(usize::try_from(point).expect("within"));
        format!("{before}.{after}")
    };
    let plain = plain.trim_start_matches('0');
    let plain = if plain.contains('.') {
        plain.trim_end_matches('0').trim_end_matches('.')
    } else {
        plain
    };
    let plain = if plain.starts_with('.') {
        format!("0{plain}")
    } else {
        plain.to_owned()
    };
    if negative { format!("-{plain}") } else { plain }
}

fn unreachable_number() -> String {
    panic!("a JSON number is an integer or a float")
}

/// `value` with its numbers in one form and its objects' null fields dropped, as text.
pub(crate) fn canonical_text(value: &Json) -> String {
    serde_json::to_string(&canonical(value)).expect("values render")
}

fn canonical(value: &Json) -> Json {
    match value {
        Json::Number(number) => match number.as_f64() {
            Some(float) if number.is_f64() && float.fract() == 0.0 && float.abs() < 1e18 => {
                #[expect(clippy::cast_possible_truncation, reason = "integral and below 1e18")]
                let integer = float as i64;
                Json::from(integer)
            }
            _ => value.clone(),
        },
        Json::Array(items) => Json::Array(items.iter().map(canonical).collect()),
        Json::Object(object) => Json::Object(
            object
                .iter()
                .filter(|(_, field)| !field.is_null())
                .map(|(name, field)| (name.clone(), canonical(field)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// `tables` with each table's rows in order.
fn sorted(mut tables: Tables) -> Tables {
    for rows in tables.values_mut() {
        rows.sort();
    }
    tables
}
