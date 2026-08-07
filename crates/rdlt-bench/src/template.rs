//! `{{key}}` substitution for pipeline templates and command lines.

use std::collections::BTreeMap;

/// Replace every `{{key}}` with its value. Unknown keys are left intact so a
/// typo surfaces verbatim in the failing command rather than vanishing.
pub fn substitute(template: &str, subs: &BTreeMap<String, String>) -> String {
    let mut out = template.to_owned();
    for (key, value) in subs {
        out = out.replace(&format!("{{{{{key}}}}}"), value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitution_replaces_known_and_keeps_unknown() {
        let mut subs = BTreeMap::new();
        subs.insert("conn".to_owned(), "pg://x".to_owned());
        // `bins` rides the same map as every other key — the `-remote`
        // cells' `{{bins}}/rdlt-connector-<name>` spelling is plain
        // substitution, nothing special-cased.
        subs.insert("bins".to_owned(), "/t/release".to_owned());
        assert_eq!(
            substitute(
                "a {{conn}} b {{typo}} c {{bins}}/rdlt-connector-postgres",
                &subs
            ),
            "a pg://x b {{typo}} c /t/release/rdlt-connector-postgres"
        );
    }
}
