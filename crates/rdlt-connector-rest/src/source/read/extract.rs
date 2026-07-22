//! Records extraction: the JSONPath SUBSET selector (dot paths + `[*]`
//! wildcards + `[N]` indices), parsed eagerly at config validation.
//! Hand-rolled (~80 lines over serde_json::Value) per the 009 crate-survey
//! rule. The no-selector passthrough — body bytes stream through untouched —
//! is the flagship perf path and is preserved byte-for-byte (RS5).

use bytes::Bytes;
use rdlt_connector::SourceError;
use serde_json::Value;

/// One parsed selector segment.
#[derive(Debug, Clone, PartialEq)]
enum Segment {
    Field(String),
    Index(usize),
    Wildcard,
}

/// A validated selector. `data.items[*].payload` → each payload under every
/// item; selection results flatten across wildcards.
#[derive(Debug, Clone, PartialEq)]
pub struct Selector {
    segments: Vec<Segment>,
    raw: String,
}

impl Selector {
    /// Parse or fail typed, naming the supported subset.
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.is_empty() {
            return Err("empty selector".into());
        }
        let mut segments = Vec::new();
        for part in raw.split('.') {
            if part.is_empty() {
                return Err(format!(
                    "selector `{raw}`: empty segment (supported: dot paths, [*], [N])"
                ));
            }
            // field, field[*], field[N], or bare [*]/[N]
            let (field, brackets) = match part.find('[') {
                Some(at) => (&part[..at], &part[at..]),
                None => (part, ""),
            };
            if !field.is_empty() {
                segments.push(Segment::Field(field.to_owned()));
            }
            let mut rest = brackets;
            while !rest.is_empty() {
                let Some(end) = rest.find(']') else {
                    return Err(format!(
                        "selector `{raw}`: unclosed `[` (supported: dot paths, [*], [N])"
                    ));
                };
                let inner = &rest[1..end];
                match inner {
                    "*" => segments.push(Segment::Wildcard),
                    n => {
                        let idx: usize = n.parse().map_err(|_| {
                            format!(
                                "selector `{raw}`: `[{n}]` is neither `[*]` nor an index \
                                 (supported: dot paths, [*], [N])"
                            )
                        })?;
                        segments.push(Segment::Index(idx));
                    }
                }
                rest = &rest[end + 1..];
                if !rest.is_empty() && !rest.starts_with('[') {
                    return Err(format!("selector `{raw}`: unexpected `{rest}` after `]`"));
                }
            }
        }
        Ok(Self {
            segments,
            raw: raw.to_owned(),
        })
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Select all matches (wildcards flatten).
    pub fn select<'v>(&self, root: &'v Value) -> Vec<&'v Value> {
        let mut current = vec![root];
        for segment in &self.segments {
            let mut next = Vec::new();
            for node in current {
                match segment {
                    Segment::Field(name) => {
                        if let Some(v) = node.get(name) {
                            next.push(v);
                        }
                    }
                    Segment::Index(i) => {
                        if let Some(v) = node.get(i) {
                            next.push(v);
                        }
                    }
                    Segment::Wildcard => {
                        if let Value::Array(items) = node {
                            next.extend(items.iter());
                        }
                    }
                }
            }
            current = next;
        }
        current
    }

    /// Select exactly one scalar-ish value (for cursors/next-urls/totals).
    pub fn select_one<'v>(&self, root: &'v Value) -> Option<&'v Value> {
        let matches = self.select(root);
        matches.first().copied()
    }
}

/// Extracted records: the array bytes + the record count.
#[derive(Debug)]
pub struct Extracted {
    pub records: Bytes,
    pub count: usize,
}

/// Extract records per the stream's selector. Without one, the body must BE
/// a JSON array and streams through untouched (the perf path); with one,
/// one parse + reserialize. No-match is a typed error naming the path and
/// the response's top-level keys (US1-AS3).
pub fn extract_records(
    body: &Bytes,
    selector: Option<&Selector>,
) -> Result<Extracted, SourceError> {
    match selector {
        None => {
            let value: Value = serde_json::from_slice(body)
                .map_err(|e| SourceError::fatal(format!("response is not valid JSON: {e}")))?;
            match value {
                Value::Array(items) => Ok(Extracted {
                    records: body.clone(),
                    count: items.len(),
                }),
                _ => Err(SourceError::fatal(
                    "response body is not a JSON array; set records_path",
                )),
            }
        }
        Some(selector) => {
            let value: Value = serde_json::from_slice(body)
                .map_err(|e| SourceError::fatal(format!("response is not valid JSON: {e}")))?;
            let matches = selector.select(&value);
            if matches.is_empty() {
                let top = top_level_keys(&value);
                return Err(SourceError::fatal(format!(
                    "records_path `{}` matched nothing in the response (top-level: {top})",
                    selector.raw()
                )));
            }
            // One match that is an array = the records array; otherwise the
            // matches themselves are the records (wildcard selection).
            let items: Vec<&Value> = if matches.len() == 1 {
                match matches[0] {
                    Value::Array(items) => items.iter().collect(),
                    _ => {
                        return Err(SourceError::fatal(format!(
                            "records_path `{}` selected a non-array; append [*] to \
                             select elements",
                            selector.raw()
                        )));
                    }
                }
            } else {
                matches
            };
            let count = items.len();
            let records =
                serde_json::to_vec(&items).map_err(|e| SourceError::fatal(e.to_string()))?;
            Ok(Extracted {
                records: records.into(),
                count,
            })
        }
    }
}

fn top_level_keys(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let keys: Vec<&str> = map.keys().map(String::as_str).take(8).collect();
            format!("object with keys [{}]", keys.join(", "))
        }
        Value::Array(items) => format!("array of {} items", items.len()),
        other => value_kind(other).to_owned(),
    }
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn selector_subset_parses_and_rejects() {
        assert!(Selector::parse("data").is_ok());
        assert!(Selector::parse("data.items").is_ok());
        assert!(Selector::parse("data.items[*]").is_ok());
        assert!(Selector::parse("data.items[0].payload").is_ok());
        assert!(Selector::parse("[*]").is_ok());
        for bad in ["", "a..b", "a[", "a[x]", "a[*]b", "a[1]2"] {
            let err = Selector::parse(bad).unwrap_err();
            assert!(
                err.contains("selector") || err.contains("empty"),
                "{bad}: {err}"
            );
        }
    }

    #[test]
    fn selection_flattens_wildcards() {
        let doc = json!({"data": {"items": [
            {"payload": {"id": 1}}, {"payload": {"id": 2}}
        ]}});
        let sel = Selector::parse("data.items[*].payload").unwrap();
        let matches = sel.select(&doc);
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0]["id"], 1);
    }

    #[test]
    fn passthrough_is_byte_identical() {
        let body = Bytes::from_static(b"[{\"a\":1},{\"a\":2}]");
        let out = extract_records(&body, None).unwrap();
        assert_eq!(out.count, 2);
        assert!(std::ptr::eq(
            out.records.as_ref().as_ptr(),
            body.as_ref().as_ptr()
        ));
    }

    #[test]
    fn index_segments_select_and_non_array_shapes_are_typed() {
        let doc = json!({"items": [{"id": 1}, {"id": 2}]});
        let sel = Selector::parse("items[1].id").unwrap();
        assert_eq!(sel.select_one(&doc), Some(&json!(2)));
        // No selector + non-array body: typed with the records_path hint.
        let body = Bytes::from(serde_json::to_vec(&json!({"a": 1})).unwrap());
        let err = extract_records(&body, None).unwrap_err().to_string();
        assert!(err.contains("records_path"), "{err}");
        // Selector landing on a scalar: typed with the [*] hint.
        let sel = Selector::parse("a").unwrap();
        let err = extract_records(&body, Some(&sel)).unwrap_err().to_string();
        assert!(err.contains("non-array") && err.contains("[*]"), "{err}");
    }

    #[test]
    fn no_match_names_path_and_shape() {
        let body = Bytes::from(serde_json::to_vec(&json!({"meta": 1, "rows": []})).unwrap());
        let sel = Selector::parse("data.items").unwrap();
        let err = extract_records(&body, Some(&sel)).unwrap_err().to_string();
        assert!(err.contains("data.items") && err.contains("meta"), "{err}");
    }
}
