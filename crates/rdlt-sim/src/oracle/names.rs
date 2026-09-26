//! A table's identifiers: each follows the destination's rules, and each column a stored row has is
//! named, as a source column's, one of its variants', or a metadata column.

#[cfg(test)]
mod tests;

use rdlt_connector::{IdentifierCase, IdentifierChars, IdentifierRules, NameMap};

/// Whether `name` follows `rules`: within their length, in their case and characters, and not a
/// word they reserve, compared as they fold case.
pub(super) fn fits(rules: &IdentifierRules, name: &str) -> bool {
    name.len() <= usize::from(rules.max_len.get())
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
        && !rules.reserved.iter().any(|word| fold(rules, word) == name)
}

/// `text` folded to the case `rules` keep identifiers in.
fn fold(rules: &IdentifierRules, text: &str) -> String {
    match rules.case {
        IdentifierCase::Lower => text.to_lowercase(),
        IdentifierCase::Upper => text.to_uppercase(),
        IdentifierCase::Preserve => text.to_owned(),
    }
}

/// Whether `name`, a table's, follows `rules` and starts with none of the prefixes they reserve
/// for the destination's own tables, compared as the rules fold case.
pub(super) fn fits_table(rules: &IdentifierRules, name: &str) -> bool {
    fits(rules, name)
        && !rules
            .reserved_table_prefixes
            .iter()
            .any(|prefix| fold(rules, name).starts_with(&fold(rules, prefix)))
}

/// The columns among `fields`, a stored row's, that `names` does not name, leaving out the last
/// `meta`, its metadata columns: a value in one would escape every check.
pub(super) fn unnamed<'a>(fields: &'a [String], names: &NameMap, meta: usize) -> Vec<&'a str> {
    let is_named = |column: &str| names.iter().any(|(_, physical)| physical == column);
    fields[..fields.len().saturating_sub(meta)]
        .iter()
        .map(String::as_str)
        .filter(|column| !is_named(column))
        .collect()
}
