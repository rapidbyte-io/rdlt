//! A reference normalizer over JSON values, written apart from the Arrow one, for the differential
//! test (§20.4): its ids come from its own canonical encoding of the JSON values.

use std::collections::BTreeMap;

use serde_json::Value as Json;

/// A normalized row: its lineage, then its non-null columns by path, each as canonical JSON text.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Row {
    pub(super) id: Vec<u8>,
    /// The parent's id, the root's id and the row's position in its parent's array.
    pub(super) parent: Option<(Vec<u8>, Vec<u8>, i64)>,
    pub(super) columns: BTreeMap<Vec<String>, String>,
}

/// Every table's rows, by the table's path below the stream's table.
pub(super) type Tables = BTreeMap<Vec<String>, Vec<Row>>;

/// Normalizes `records` as a stream with `max_depth` whose root rows are identified by `key`, or
/// wholly without one, keeping the top-level columns `whole` whole.
pub(super) fn normalize(records: &[Json], max_depth: u8, key: &[&str], whole: &[&str]) -> Tables {
    let mut tables = Tables::new();
    for record in records {
        let object = record.as_object().expect("records are objects");
        let mut encoding = Vec::new();
        if key.is_empty() {
            encode(record, &mut encoding);
        } else {
            for column in key {
                encode(object.get(*column).unwrap_or(&Json::Null), &mut encoding);
            }
        }
        let id = hash(&encoding);
        let mut row = Pending::default();
        for (name, value) in object {
            if whole.contains(&name.as_str()) {
                row.column(vec![name.clone()], value);
            } else {
                row.place(vec![name.clone()], value, 1, max_depth);
            }
        }
        row.emit(&[], &id, None, &id, max_depth, &mut tables);
    }
    tables
}

/// One row's columns and arrays while it normalizes.
#[derive(Default)]
struct Pending {
    columns: BTreeMap<Vec<String>, String>,
    arrays: Vec<(Vec<String>, Vec<Json>, u8)>,
}

impl Pending {
    fn column(&mut self, path: Vec<String>, value: &Json) {
        if !value.is_null() {
            self.columns.insert(path, canonical_text(value));
        }
    }

    fn place(&mut self, path: Vec<String>, value: &Json, depth: u8, max_depth: u8) {
        if depth > max_depth {
            return self.column(path, value);
        }
        match value {
            Json::Object(object) => {
                for (name, field) in object {
                    let mut field_path = path.clone();
                    field_path.push(name.clone());
                    self.place(field_path, field, depth + 1, max_depth);
                }
            }
            Json::Array(items) => self.arrays.push((path, items.clone(), depth)),
            _ => self.column(path, value),
        }
    }

    /// Adds this row, of the table at `table`, then the rows of its arrays' child tables.
    fn emit(
        self,
        table: &[String],
        id: &[u8],
        parent: Option<(Vec<u8>, Vec<u8>, i64)>,
        root: &[u8],
        max_depth: u8,
        tables: &mut Tables,
    ) {
        tables.entry(table.to_vec()).or_default().push(Row {
            id: id.to_vec(),
            parent,
            columns: self.columns,
        });
        for (path, items, depth) in self.arrays {
            let mut child_table = table.to_vec();
            child_table.extend(path);
            for (position, item) in items.iter().enumerate() {
                let idx = i64::try_from(position).expect("arrays are short");
                let mut bytes = id.to_vec();
                bytes.extend_from_slice(&u64::try_from(idx).expect("positive").to_be_bytes());
                let child_id = hash(&bytes);
                let mut row = Pending::default();
                let item_depth = depth + 1;
                match item {
                    Json::Object(object) if item_depth <= max_depth => {
                        for (name, field) in object {
                            row.place(vec![name.clone()], field, item_depth + 1, max_depth);
                        }
                    }
                    Json::Array(inner) if item_depth <= max_depth => {
                        row.arrays
                            .push((vec!["value".to_owned()], inner.clone(), item_depth));
                    }
                    _ => row.column(vec!["value".to_owned()], item),
                }
                let parent = Some((id.to_vec(), root.to_vec(), idx));
                row.emit(&child_table, &child_id, parent, root, max_depth, tables);
            }
        }
    }
}

fn hash(bytes: &[u8]) -> Vec<u8> {
    xxhash_rust::xxh3::xxh3_128(bytes).to_be_bytes().to_vec()
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
            out.extend_from_slice(text.as_bytes());
            out.push(b';');
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
                length(out, name.as_bytes());
                encode(field, out);
            }
            out.push(b'}');
        }
    }
}

/// A float's shortest round-trip text, zero without its sign.
fn float_text(float: f64) -> String {
    if float == 0.0 {
        "0".to_owned()
    } else {
        float.to_string()
    }
}

fn unreachable_number() -> String {
    panic!("a JSON number is an integer or a float")
}

/// `value` with its numbers in one form and its objects' null fields dropped, as text.
pub(super) fn canonical_text(value: &Json) -> String {
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
