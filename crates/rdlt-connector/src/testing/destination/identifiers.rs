//! The identifiers the clauses name tables and columns with: the names certification gives them,
//! kept to the destination's identifier rules, as the engine keeps the names it gives.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use crate::capabilities::{IdentifierCase, IdentifierRules};
use crate::change::{OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN};
use crate::meta::{
    DELETED_AT_COLUMN, ID_COLUMN, IS_CURRENT_COLUMN, ROOT_ID_COLUMN, ROW_HASH_COLUMN,
    VALID_FROM_COLUMN, VALID_TO_COLUMN,
};

/// How the name of every table certification creates begins, so an operator knows what it left
/// in a store.
const PREFIX: &str = "certify_";

/// The columns the clauses write, the names clause's aside, by the names certification gives
/// them.
const COLUMNS: [&str; 18] = [
    "id",
    "name",
    "value",
    "epoch",
    "at",
    "extra",
    "small",
    "v",
    SEQ_COLUMN,
    OP_COLUMN,
    UNCHANGED_COLUMN,
    DELETED_AT_COLUMN,
    VALID_FROM_COLUMN,
    VALID_TO_COLUMN,
    IS_CURRENT_COLUMN,
    ROW_HASH_COLUMN,
    ID_COLUMN,
    ROOT_ID_COLUMN,
];

/// The identifiers a destination's rules give the tables and columns certification names.
pub(super) struct Identifiers {
    rules: IdentifierRules,
    /// The words the rules reserve, folded as identifiers are.
    reserved: BTreeSet<String>,
}

impl Identifiers {
    /// The identifiers `rules` give.
    pub(super) fn new(rules: &IdentifierRules) -> Self {
        let reserved = rules.reserved.iter().map(|word| folded(rules, word));
        Self {
            rules: rules.clone(),
            reserved: reserved.collect(),
        }
    }

    /// The longest identifier the rules allow, in bytes.
    pub(super) fn longest(&self) -> usize {
        usize::from(self.rules.max_len.get())
    }

    /// `name` folded as the rules fold it.
    pub(super) fn folded(&self, name: &str) -> String {
        folded(&self.rules, name)
    }

    /// `name` folded, lengthened with `_` until it is no reserved word; none where every such
    /// form within the longest identifier is one.
    pub(super) fn unreserved(&self, name: &str) -> Option<String> {
        unreserved(&self.reserved, self.longest(), self.folded(name))
    }

    /// The identifier of the column certification calls `name`: [unreserved](Self::unreserved),
    /// or folded where no form is, which [`Self::unnamed`] tells of the columns the clauses write.
    pub(super) fn column(&self, name: &str) -> Arc<str> {
        self.unreserved(name)
            .unwrap_or_else(|| self.folded(name))
            .into()
    }

    /// The identifier of the table certification calls `name`, which begins with [`PREFIX`]:
    /// folded, or, where that is longer than the rules allow, the prefix and as much of a digest
    /// of `name` as fits.
    pub(super) fn table(&self, name: &str) -> Arc<str> {
        let whole = self.folded(name);
        if whole.len() <= self.longest() {
            return whole.into();
        }
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);
        let digest = format!("{:016x}", hasher.finish());
        let room = self
            .longest()
            .saturating_sub(PREFIX.len())
            .min(digest.len());
        self.folded(&format!("{PREFIX}{}", &digest[..room])).into()
    }

    /// Why the rules leave a table or column the clauses name no identifier, if they do: they
    /// reserve a prefix or word a table's name may begin with, or every form of a column's
    /// name.
    pub(super) fn unnamed(&self) -> Option<String> {
        let start = self.folded(PREFIX);
        let prefixes = self.rules.reserved_table_prefixes.iter();
        let trapped = prefixes
            .map(|prefix| self.folded(prefix))
            .any(|prefix| start.starts_with(&prefix) || prefix.starts_with(&start))
            || self.reserved.iter().any(|word| word.starts_with(&start));
        if trapped {
            return Some(format!(
                "the destination reserves a prefix or a word of names beginning `{start}`, as \
                 certification names its tables"
            ));
        }
        let column = COLUMNS
            .iter()
            .find(|column| self.unreserved(column).is_none())?;
        Some(format!(
            "the destination reserves every form of the column name `{column}` its identifiers \
             are long enough for"
        ))
    }
}

/// `name` as `rules` fold it.
pub(super) fn folded(rules: &IdentifierRules, name: &str) -> String {
    match rules.case {
        IdentifierCase::Preserve => name.to_owned(),
        IdentifierCase::Lower => name.to_lowercase(),
        IdentifierCase::Upper => name.to_uppercase(),
    }
}

/// `name`, lengthened with `_` until it is none of the `reserved` words; none when the
/// `longest` identifier is still one.
pub(super) fn unreserved(
    reserved: &BTreeSet<String>,
    longest: usize,
    mut name: String,
) -> Option<String> {
    while reserved.contains(&name) {
        if name.len() >= longest {
            return None;
        }
        name.push('_');
    }
    Some(name)
}
