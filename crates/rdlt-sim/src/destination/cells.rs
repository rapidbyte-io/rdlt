//! Stored rows as JSON cells, and how they read back as the rows the source sent.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use rdlt_connector::{ConnectorError, LogicalType, MergeKey, NameMap, Result, RootKey};
use serde_json::{Map, Value};

/// One stored row: each column's value, nulls left out.
///
/// A JSON column's value is the JSON it holds, and numbers with no fraction are integers, so a
/// value reads the same whichever column type stored it.
pub type Cells = BTreeMap<String, Value>;

/// The Arrow field metadata key naming an extension type.
const EXTENSION_NAME: &str = "ARROW:extension:name";

/// The rows of `batch` as cells.
pub(crate) fn rows(batch: &RecordBatch) -> Result<Vec<Cells>> {
    let failed =
        |error: &dyn std::fmt::Display| ConnectorError::data(format!("reading a batch: {error}"));
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer.write(batch).map_err(|error| failed(&error))?;
    writer.finish().map_err(|error| failed(&error))?;
    let rendered: Vec<Map<String, Value>> =
        serde_json::from_slice(&writer.into_inner()).map_err(|error| failed(&error))?;
    let schema = batch.schema();
    let json: Vec<&str> = schema
        .fields()
        .iter()
        .filter(|field| {
            field.metadata().get(EXTENSION_NAME).map(String::as_str) == Some("arrow.json")
        })
        .map(|field| field.name().as_str())
        .collect();
    rendered
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(column, value)| {
                    let value = match (&value, json.contains(&column.as_str())) {
                        (Value::String(text), true) => {
                            serde_json::from_str(text).map_err(|error| failed(&error))?
                        }
                        _ => value,
                    };
                    Ok((column, canonical(value)))
                })
                .collect()
        })
        .collect()
}

/// `value` with integral numbers as integers and null object members left out.
pub(crate) fn canonical(value: Value) -> Value {
    match value {
        Value::Number(number) if number.as_i64().is_none() && number.as_u64().is_none() => {
            match number.as_f64() {
                Some(float) if float.fract() == 0.0 && float.abs() < 9e15 => {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "the float is integral and small"
                    )]
                    let integer = float as i64;
                    Value::from(integer)
                }
                _ => Value::Number(number),
            }
        }
        Value::Object(members) => Value::Object(
            members
                .into_iter()
                .filter(|(_, member)| !member.is_null())
                .map(|(name, member)| (name, canonical(member)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(canonical).collect()),
        other => other,
    }
}

/// Merges `incoming` into `published` by `key`: an incoming row replaces the published row with
/// its key, and among incoming rows of one key the greatest sequence wins.
pub(crate) fn merge(published: &mut Vec<Cells>, incoming: Vec<Cells>, key: &MergeKey) {
    let key_of = |row: &Cells| {
        let values: Vec<Value> = key
            .columns
            .iter()
            .map(|column| row.get(column.as_ref()).cloned().unwrap_or(Value::Null))
            .collect();
        Value::Array(values).to_string()
    };
    let seq_of = |row: &Cells| {
        row.get(key.seq.as_ref())
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let mut winners: BTreeMap<String, Cells> = BTreeMap::new();
    for row in incoming {
        let row_key = key_of(&row);
        match winners.get(&row_key) {
            Some(best) if seq_of(best) >= seq_of(&row) => {}
            _ => {
                winners.insert(row_key, row);
            }
        }
    }
    published.retain(|row| !winners.contains_key(&key_of(row)));
    published.extend(winners.into_values());
}

/// Merges `incoming` into `published`, the rows of a child table merging by `key` below `root`,
/// as the root table merges `roots`: every published row of a root among `roots` goes, and the
/// incoming rows of each root's winning row, whose sequence is greatest, take their place.
pub(crate) fn merge_children(
    published: &mut Vec<Cells>,
    incoming: Vec<Cells>,
    key: &MergeKey,
    root: &RootKey,
    roots: &[Cells],
) {
    let text =
        |row: &Cells, column: &str| row.get(column).map(Value::to_string).unwrap_or_default();
    let mut winners: BTreeMap<String, String> = BTreeMap::new();
    for row in roots {
        let (id, seq) = (text(row, &root.id), text(row, &root.seq));
        if winners.get(&id).is_none_or(|best| *best < seq) {
            winners.insert(id, seq);
        }
    }
    let owner = key.columns.first().map_or("", AsRef::as_ref);
    published.retain(|row| !winners.contains_key(&text(row, owner)));
    published.extend(
        incoming
            .into_iter()
            .filter(|row| winners.get(&text(row, owner)) == Some(&text(row, &key.seq))),
    );
}

/// A finding about a stored row.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Finding {
    /// A source column whose value one row holds in two of its columns.
    #[error("column {0} holds a value in two columns")]
    Doubled(String),
    /// A column the table models as JSON or nested holds text that is not JSON.
    #[error("column {0} holds text that is not JSON: {1}")]
    NotJson(String, serde_json::Error),
}

/// `row` with the value of each column `types` models as JSON, a struct or a list but the
/// destination stores as text, per `stored`, parsed back from that text.
pub(crate) fn unlowered(
    row: &Cells,
    types: &BTreeMap<String, LogicalType>,
    stored: &BTreeMap<String, LogicalType>,
) -> std::result::Result<Cells, Finding> {
    row.iter()
        .map(|(column, value)| {
            let nested = matches!(
                types.get(column),
                Some(LogicalType::Json | LogicalType::Struct(_) | LogicalType::List(_))
            ) && stored.get(column) == Some(&LogicalType::Utf8);
            let value = match value {
                Value::String(text) if nested => canonical(
                    serde_json::from_str(text)
                        .map_err(|error| Finding::NotJson(column.clone(), error))?,
                ),
                other => other.clone(),
            };
            Ok((column.clone(), value))
        })
        .collect()
}

/// `row` by source column: each source column's value from whichever of its column and variant
/// columns holds it, through `names`.
pub(crate) fn source_row(
    row: &Cells,
    names: &NameMap,
) -> std::result::Result<Map<String, Value>, Finding> {
    let mut source = Map::new();
    for (key, physical) in names.iter() {
        let Some(value) = row.get(physical) else {
            continue;
        };
        let column: Vec<&str> = key.column().segments().collect();
        let column = column.join(".");
        if source.insert(column.clone(), value.clone()).is_some() {
            return Err(Finding::Doubled(column));
        }
    }
    Ok(source)
}
