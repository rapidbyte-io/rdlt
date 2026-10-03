//! `D-NAMES`: identifiers at the edges of the destination's own rules.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};

use super::{Bench, commit, meta};
use crate::capabilities::{IdentifierCase, IdentifierChars, IdentifierRules};
use crate::destination::{TableChange, TableRef};
use crate::id::{SchemaVersion, SegmentId, TablePath};
use crate::schema::TableSchema;
use crate::testing::Violation;
use crate::types::{Field, LogicalType};

/// The shortest identifiers the clause fits its names in.
pub(super) const SHORTEST: u16 = 32;

/// Pairs of names that compare alike under some equality wider than a destination may declare.
///
/// They are alike by ASCII case, by Unicode's case folding, by Unicode normalization and by
/// compatibility. The clause writes each pair the declared rules keep apart, and needs both read
/// back.
const APART: [(&str, &str); 4] = [
    ("Kept", "kept"),
    ("stra\u{df}e", "strasse"),
    ("\u{e9}t\u{e9}", "e\u{301}te\u{301}"),
    ("\u{fb01}x", "fix"),
];

impl Bench<'_> {
    /// Creates a table whose identifier, and a column's, are as long as the destination allows,
    /// with a column named in letters beyond ASCII when it allows any character, one in mixed
    /// case when it keeps case, and pairs of columns whose names compare alike only under an
    /// equality its rules do not declare, each folded as its rules fold; stages three rows,
    /// commits them, and reads every column back under its name.
    pub(super) async fn names_are_kept(&self) -> Result<(), Violation> {
        let rules = &self.destination.capabilities().identifiers;
        let longest = usize::from(rules.max_len.get());
        let identifier = folded(rules, &padded(self.name(), longest, 'n'));
        let table = TableRef {
            path: TablePath::new([self.name().as_str()]).expect("table paths are valid"),
            name: identifier.clone().into(),
            version: SchemaVersion(1),
            generation: None,
            merge: None,
        };
        let names = names(rules).ok_or(
            "the destination reserves every form of a name its identifiers are long enough for",
        )?;
        let (schema, rows) = table_of(&names);
        let mut opened = self.open(self.destination, 1).await?;
        opened
            .session
            .apply_schema(&TableChange::Create {
                table: table.clone(),
                schema,
            })
            .await
            .map_err(|error| Violation::from(format!("apply_schema: {error}")))?;
        let mut writer = opened
            .session
            .writer(&table)
            .await
            .map_err(|error| Violation::from(format!("writer: {error}")))?;
        writer
            .write(SegmentId(1), rows)
            .await
            .map_err(|error| Violation::from(format!("write: {error}")))?;
        writer
            .flush()
            .await
            .map_err(|error| Violation::from(format!("flush: {error}")))?;
        commit(
            &mut opened.session,
            &meta(self.load_id(1), opened.epoch, &[1], Vec::new()),
        )
        .await?;
        let published = self.read(&table).await?;
        let rows = published.rows();
        if rows != 3 {
            return Err(format!("table {identifier} published {rows} rows, not 3").into());
        }
        for name in &names {
            let kept = published.batches().all(|batch| batch.has(name));
            if !kept {
                return Err(format!("column `{name}` was not published under its name").into());
            }
        }
        Ok(())
    }
}

/// The clause's column names, folded as `rules` fold: an id, one as long as `rules` allow, one
/// beyond ASCII when any character is allowed, and one in mixed case when case is kept; none
/// when `rules` reserve every form of one.
fn names(rules: &IdentifierRules) -> Option<Vec<String>> {
    let longest = usize::from(rules.max_len.get());
    // Folded once: a destination declares as many reserved words as it likes.
    let reserved: BTreeSet<String> = rules
        .reserved
        .iter()
        .map(|word| folded(rules, word))
        .collect();
    let unreserved = |name: String| unreserved(&reserved, longest, name);
    let mut names = vec![
        unreserved(folded(rules, "id"))?,
        folded(rules, &padded("long_".to_owned(), longest, 'g')),
    ];
    if rules.chars == IdentifierChars::Any {
        names.push(unreserved(folded(rules, "données_名前"))?);
    }
    if rules.case == IdentifierCase::Preserve {
        names.push(unreserved("MixedCase".to_owned())?);
    }
    for (one, other) in APART {
        let (one, other) = (folded(rules, one), folded(rules, other));
        let ascii = |name: &str| name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        let admitted = rules.chars == IdentifierChars::Any || (ascii(&one) && ascii(&other));
        let free = |name: &String| !reserved.contains(name) && !names.contains(name);
        if one != other && admitted && free(&one) && free(&other) {
            names.extend([one, other]);
        }
    }
    Some(names)
}

/// `name`, lengthened with `_` until it is none of the `reserved` words; none when the
/// `longest` identifier is still one.
fn unreserved(reserved: &BTreeSet<String>, longest: usize, mut name: String) -> Option<String> {
    while reserved.contains(&name) {
        if name.len() >= longest {
            return None;
        }
        name.push('_');
    }
    Some(name)
}

/// A table of `names`' columns, the first an id and the rest strings, and three rows of it.
fn table_of(names: &[String]) -> (TableSchema, RecordBatch) {
    let mut fields = vec![Field::new(names[0].as_str(), LogicalType::Int64, false)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    for name in &names[1..] {
        fields.push(Field::new(name.as_str(), LogicalType::Utf8, true));
        columns.push(Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("c"),
        ])));
    }
    let schema = TableSchema::new(fields).expect("the names are distinct");
    let rows = RecordBatch::try_from_iter(names.iter().cloned().zip(columns))
        .expect("the rows fit their columns");
    (schema, rows)
}

/// `name` as `rules` fold it.
fn folded(rules: &IdentifierRules, name: &str) -> String {
    match rules.case {
        IdentifierCase::Preserve => name.to_owned(),
        IdentifierCase::Lower => name.to_lowercase(),
        IdentifierCase::Upper => name.to_uppercase(),
    }
}

/// `name`, lengthened with `with` to `longest` bytes.
fn padded(mut name: String, longest: usize, with: char) -> String {
    while name.len() < longest {
        name.push(with);
    }
    name
}
