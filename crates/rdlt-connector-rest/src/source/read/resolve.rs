//! Parent-child placeholder resolution (contract RS7 tail): `{token}`
//! substitution into path/params/body from a parent record's fields, plus
//! the `_parent_<field>` embedding. Buffering is BOUNDED — only the
//! referenced placeholder values and declared include fields, never whole
//! parent records.

use std::collections::BTreeMap;

use rdlt_connector::SourceError;
use serde_json::Value;

use crate::source::config::Parent;
use crate::source::read::extract::Selector;

/// One parent record's contribution: resolved placeholder values + the
/// fields to embed into child records.
#[derive(Debug, Clone)]
pub struct ParentValues {
    pub placeholders: BTreeMap<String, String>,
    pub include: Vec<(String, Value)>,
}

/// Extract every parent record's values from one parent PAGE (the records
/// array bytes the read loop already has).
pub fn collect_parent_values(
    records: &bytes::Bytes,
    parent: &Parent,
) -> Result<Vec<ParentValues>, SourceError> {
    let items: Vec<Value> = serde_json::from_slice(records)
        .map_err(|e| SourceError::fatal(format!("parent records reparse: {e}")))?;
    let mut out = Vec::with_capacity(items.len());
    for item in &items {
        let mut placeholders = BTreeMap::new();
        for (token, field_path) in &parent.placeholders {
            let selector = Selector::parse(field_path)
                .map_err(|e| SourceError::fatal(format!("parent placeholder `{token}`: {e}")))?;
            let value = selector.select_one(item).ok_or_else(|| {
                SourceError::fatal(format!(
                    "parent record lacks field `{field_path}` for placeholder `{{{token}}}`"
                ))
            })?;
            let rendered = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                other => {
                    return Err(SourceError::fatal(format!(
                        "parent field `{field_path}` for placeholder `{{{token}}}` is {} — \
                         placeholders take scalars",
                        match other {
                            Value::Array(_) => "an array",
                            Value::Object(_) => "an object",
                            Value::Null => "null",
                            _ => "unsupported",
                        }
                    )));
                }
            };
            placeholders.insert(token.clone(), rendered);
        }
        let mut include = Vec::with_capacity(parent.include.len());
        for field in &parent.include {
            let value = item.get(field).cloned().unwrap_or(Value::Null);
            include.push((field.clone(), value));
        }
        out.push(ParentValues {
            placeholders,
            include,
        });
    }
    Ok(out)
}

/// Substitute `{token}` occurrences in a template string.
pub fn substitute(template: &str, values: &BTreeMap<String, String>) -> String {
    let mut out = template.to_owned();
    for (token, value) in values {
        out = out.replace(&format!("{{{token}}}"), value);
    }
    out
}

/// Human-readable resolved-values summary for failure messages (RS7:
/// child failures NAME the parent's resolved values).
pub fn describe(values: &BTreeMap<String, String>) -> String {
    values
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Embed `_parent_<field>` values into each child record of a page.
/// Collision with an existing child field is a typed error.
pub fn embed_parent_fields(
    records: bytes::Bytes,
    include: &[(String, Value)],
) -> Result<bytes::Bytes, SourceError> {
    if include.is_empty() {
        return Ok(records);
    }
    let mut items: Vec<Value> = serde_json::from_slice(&records)
        .map_err(|e| SourceError::fatal(format!("child records reparse: {e}")))?;
    for item in &mut items {
        let Value::Object(map) = item else {
            return Err(SourceError::fatal(
                "child records must be objects to embed parent fields",
            ));
        };
        for (field, value) in include {
            let key = format!("_parent_{field}");
            if map.contains_key(&key) {
                return Err(SourceError::fatal(format!(
                    "child record already has a `{key}` field — parent include collides"
                )));
            }
            map.insert(key, value.clone());
        }
    }
    let bytes = serde_json::to_vec(&items).map_err(|e| SourceError::fatal(e.to_string()))?;
    Ok(bytes.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parent(placeholders: &[(&str, &str)], include: &[&str]) -> Parent {
        Parent {
            stream: "parents".into(),
            placeholders: placeholders
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            include: include.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn collects_and_substitutes() {
        let records = bytes::Bytes::from(
            serde_json::to_vec(&json!([
                {"id": 7, "name": "ada", "org": {"slug": "acme"}},
            ]))
            .unwrap(),
        );
        let values = collect_parent_values(
            &records,
            &parent(&[("id", "id"), ("org", "org.slug")], &["name"]),
        )
        .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(
            substitute("/orgs/{org}/users/{id}", &values[0].placeholders),
            "/orgs/acme/users/7"
        );
        assert_eq!(describe(&values[0].placeholders), "id=7, org=acme");
    }

    #[test]
    fn missing_field_and_non_scalar_are_typed() {
        let records = bytes::Bytes::from(serde_json::to_vec(&json!([{"id": [1, 2]}])).unwrap());
        let err = collect_parent_values(&records, &parent(&[("id", "id")], &[]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("an array"), "{err}");
        let records = bytes::Bytes::from(serde_json::to_vec(&json!([{"x": 1}])).unwrap());
        let err = collect_parent_values(&records, &parent(&[("id", "id")], &[]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("lacks field `id`"), "{err}");
    }

    #[test]
    fn embeds_parent_fields_and_detects_collisions() {
        let records = bytes::Bytes::from(serde_json::to_vec(&json!([{"a": 1}])).unwrap());
        let out = embed_parent_fields(records.clone(), &[("name".into(), json!("ada"))]).unwrap();
        let items: Vec<Value> = serde_json::from_slice(&out).unwrap();
        assert_eq!(items[0]["_parent_name"], "ada");

        let colliding =
            bytes::Bytes::from(serde_json::to_vec(&json!([{"_parent_name": 1}])).unwrap());
        let err = embed_parent_fields(colliding, &[("name".into(), json!("x"))])
            .unwrap_err()
            .to_string();
        assert!(err.contains("collides"), "{err}");
    }
}
