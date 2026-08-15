//! The validated config-document seam.
//!
//! Every rdlt connector accepts its configuration as a document —
//! YAML text, JSON text, or an already-parsed `serde_json::Value` from a
//! platform holding configs as data — and every entry point must run the
//! SAME validation, or an invariant the connector's runtime relies on
//! holds for one entry and silently not for another. Before this trait,
//! each connector hand-rolled the three entry points around its own
//! validate gate; the bodies were near-identical eighteen times over,
//! and "validation runs at every entry" was a discipline reviews had to
//! re-verify per crate. [`Document`] makes it a property of the trait:
//! the provided methods parse, then validate, and there is no other path
//! through them.
//!
//! THE SEAM RENDERS NO TEXT. Error types, message prefixes, and every
//! refusal spelling stay in the connector — the associated
//! [`Document::Error`] only has to absorb the two parser errors, which
//! each connector's error type already does with its own frozen wording.
//! (A message-rendering seam was considered and rejected on evidence:
//! the connectors' frozen prefixes disagree in spelling, and one
//! deliberately has none.)

use serde::de::DeserializeOwned;

/// A connector configuration document: parse from any accepted form,
/// then validate — by construction, at every entry point.
///
/// Implementers supply [`Document::validate`] (the ONE gate, holding the
/// crate's own invariants with the crate's own error type and message
/// spellings) and inherit the three entry points. The provided bodies
/// are deliberately not overridable in spirit: overriding one to skip
/// validation would recreate the defect class this trait exists to
/// close.
///
/// # Example
///
/// ```
/// use rdlt_connector_sdk::config::Document;
///
/// #[derive(Debug, serde::Deserialize)]
/// struct Config {
///     url: String,
/// }
///
/// #[derive(Debug, thiserror::Error)]
/// enum ConfigError {
///     #[error("invalid example YAML: {0}")]
///     Yaml(#[from] serde_yaml::Error),
///     #[error("invalid example JSON: {0}")]
///     Json(#[from] serde_json::Error),
///     #[error("invalid example config: {0}")]
///     Invalid(String),
/// }
///
/// impl Document for Config {
///     type Error = ConfigError;
///     fn validate(&self) -> Result<(), Self::Error> {
///         if self.url.is_empty() {
///             return Err(ConfigError::Invalid("`url` must not be empty".into()));
///         }
///         Ok(())
///     }
/// }
///
/// // Every entry point runs the same gate:
/// assert!(Config::from_yaml("url: https://x").is_ok());
/// let refused = Config::from_yaml("url: \"\"").unwrap_err();
/// assert_eq!(
///     refused.to_string(),
///     "invalid example config: `url` must not be empty"
/// );
/// let refused = Config::from_json("{\"url\": \"\"}").unwrap_err();
/// assert!(refused.to_string().contains("must not be empty"));
/// ```
pub trait Document: DeserializeOwned + Sized {
    /// The connector's own configuration error. It absorbs the two
    /// parser errors (keeping the connector's frozen wording via its own
    /// `From` impls) and carries the connector's refusals. `Display` is
    /// required (not just an incidental property of every existing
    /// impl's `thiserror::Error` derive): `serve()` (038) renders a
    /// handshake's config refusal from a `Shell<C>` generic over `C`
    /// alone, with no connector-specific error type to match on.
    type Error: std::fmt::Display + From<serde_yaml::Error> + From<serde_json::Error>;

    /// The ONE validation gate. Every invariant the connector's runtime
    /// relies on is checked here, once — the provided entry points all
    /// route through it, so nothing downstream re-checks or papers over.
    fn validate(&self) -> Result<(), Self::Error>;

    /// Parse from YAML text and validate.
    ///
    /// Two raw-text security gates run BEFORE the parser sees the
    /// text, because YAML deserialization allocates while it parses:
    /// documents over [`crate::yaml::MAX_DOCUMENT_BYTES`] are refused
    /// by size, and YAML anchors/aliases are refused outright
    /// ([`crate::yaml::reject_graph_syntax`] — alias expansion
    /// materializes quadratically, so a byte cap alone cannot bound
    /// it). Both refusals arrive through the YAML arm of
    /// [`Document::Error`], rendered in the connector's own wording
    /// around the gate's one-line reason.
    fn from_yaml(yaml: &str) -> Result<Self, Self::Error> {
        if yaml.len() as u64 > crate::yaml::MAX_DOCUMENT_BYTES {
            let refusal = <serde_yaml::Error as serde::de::Error>::custom(format!(
                "the document is {} bytes, over the {}-byte cap — connector configuration \
                 is hand-written, so a document this size is almost certainly not the \
                 configuration it was passed as",
                yaml.len(),
                crate::yaml::MAX_DOCUMENT_BYTES
            ));
            return Err(refusal.into());
        }
        if let Err(reason) = crate::yaml::reject_graph_syntax(yaml) {
            let refusal = <serde_yaml::Error as serde::de::Error>::custom(reason);
            return Err(refusal.into());
        }
        let document: Self = serde_yaml::from_str(yaml)?;
        document.validate()?;
        Ok(document)
    }

    /// Parse from JSON text and validate — the same document shape and
    /// the same gate as YAML.
    fn from_json(json: &str) -> Result<Self, Self::Error> {
        let document: Self = serde_json::from_str(json)?;
        document.validate()?;
        Ok(document)
    }

    /// The embedder entry point: a platform holding connector configs as
    /// JSON documents passes the `serde_json::Value` directly — no
    /// string round-trip, same gate as every other entry.
    fn from_value(value: serde_json::Value) -> Result<Self, Self::Error> {
        let document: Self = serde_json::from_value(value)?;
        document.validate()?;
        Ok(document)
    }
}

/// The JSON Schema of a config document, GENERATED from its type — the
/// declared schema and the parser cannot drift, because they are the
/// same structs.
///
/// Each connector keeps its own one-line public `config_schema()`
/// delegating here, so its public path stays stable while the body
/// lives once.
#[cfg(feature = "schema")]
pub fn schema_of<T: schemars::JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("a generated schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    struct Probe {
        name: String,
        #[serde(default)]
        limit: u32,
    }

    #[derive(Debug, thiserror::Error)]
    enum ProbeError {
        #[error("probe yaml: {0}")]
        Yaml(#[from] serde_yaml::Error),
        #[error("probe json: {0}")]
        Json(#[from] serde_json::Error),
        #[error("probe config: {0}")]
        Invalid(String),
    }

    impl Document for Probe {
        type Error = ProbeError;
        fn validate(&self) -> Result<(), Self::Error> {
            if self.name.is_empty() {
                return Err(ProbeError::Invalid("`name` must not be empty".into()));
            }
            if self.limit > 100 {
                return Err(ProbeError::Invalid(format!(
                    "`limit` is {} — the maximum is 100",
                    self.limit
                )));
            }
            Ok(())
        }
    }

    /// The trait's whole point: the gate runs in EVERY entry path, so an
    /// invalid document is refused identically from all three.
    #[test]
    fn every_entry_point_runs_the_same_gate() {
        let refused_yaml = Probe::from_yaml("name: \"\"").unwrap_err();
        let refused_json = Probe::from_json("{\"name\": \"\"}").unwrap_err();
        let refused_value = Probe::from_value(serde_json::json!({"name": ""})).unwrap_err();
        for refused in [refused_yaml, refused_json, refused_value] {
            assert_eq!(
                refused.to_string(),
                "probe config: `name` must not be empty"
            );
        }
    }

    /// Parse errors surface through the connector's OWN From impls — the
    /// seam adds no framing of its own. The value path's shape mismatch
    /// is a parse error too, absorbed the same way.
    #[test]
    fn parse_errors_keep_the_connector_wording() {
        let yaml = Probe::from_yaml(": not yaml").unwrap_err();
        assert!(yaml.to_string().starts_with("probe yaml: "), "{yaml}");
        let json = Probe::from_json("not json").unwrap_err();
        assert!(json.to_string().starts_with("probe json: "), "{json}");
        assert!(matches!(yaml, ProbeError::Yaml(_)));
        assert!(matches!(json, ProbeError::Json(_)));
        let value = Probe::from_value(serde_json::json!({"name": 7})).unwrap_err();
        assert!(value.to_string().starts_with("probe json: "), "{value}");
        assert!(matches!(value, ProbeError::Json(_)));
    }

    /// A valid document comes back parsed, from any form, including the
    /// embedder's no-round-trip value path.
    #[test]
    fn valid_documents_parse_from_all_three_forms() {
        let from_yaml = Probe::from_yaml("name: a\nlimit: 7").expect("yaml");
        assert_eq!((from_yaml.name.as_str(), from_yaml.limit), ("a", 7));
        let from_json = Probe::from_json("{\"name\": \"a\"}").expect("json");
        assert_eq!(from_json.limit, 0, "serde defaults apply");
        let from_value =
            Probe::from_value(serde_json::json!({"name": "a", "limit": 100})).expect("value");
        assert_eq!(from_value.limit, 100, "the boundary is inside the gate");
    }

    /// Validation sees the PARSED document — refusals can quote resolved
    /// values, not just raw text.
    #[test]
    fn refusals_quote_the_parsed_value() {
        let refused = Probe::from_value(serde_json::json!({"name": "a", "limit": 101}))
            .unwrap_err()
            .to_string();
        assert_eq!(refused, "probe config: `limit` is 101 — the maximum is 100");
    }

    /// The YAML entry point is a security seat, not just a parser: the
    /// graph gate runs before deserialization, so anchors and aliases
    /// refuse instead of expanding — through the connector's own YAML
    /// error wording, since the seam still renders no text of its own.
    #[test]
    fn from_yaml_refuses_graph_syntax_before_parsing() {
        let refused = Probe::from_yaml("name: &n probe\nalias: *n\n").unwrap_err();
        assert!(matches!(refused, ProbeError::Yaml(_)));
        let rendered = refused.to_string();
        assert!(rendered.starts_with("probe yaml: "), "{rendered}");
        assert!(rendered.contains("anchors and aliases"), "{rendered}");

        // The blinding shape: an apostrophe inside a plain scalar must
        // not disarm the gate for the anchors after it.
        let refused = Probe::from_yaml("name: it's\nbig: &b [x]\nboom: *b\n").unwrap_err();
        assert!(
            refused.to_string().contains("anchors and aliases"),
            "{refused}"
        );

        // And the gate stays a gate, not a filter: ordinary documents
        // with indicator characters as data still parse.
        let parsed = Probe::from_yaml("name: don't#stop&go\n").expect("data indicators parse");
        assert_eq!(parsed.name, "don't#stop&go");
    }

    /// The YAML entry point refuses over-sized documents before any
    /// parsing — configuration is hand-written and measures in
    /// kilobytes, so a multi-megabyte document is a wrong input, not a
    /// big config.
    #[test]
    fn from_yaml_refuses_documents_over_the_size_cap() {
        let mut oversized = String::from("name: ");
        oversized.push_str(&"a".repeat(crate::yaml::MAX_DOCUMENT_BYTES as usize));
        let refused = Probe::from_yaml(&oversized).unwrap_err();
        assert!(matches!(refused, ProbeError::Yaml(_)));
        let rendered = refused.to_string();
        assert!(rendered.contains("over the 8388608-byte cap"), "{rendered}");
    }

    /// The generated schema is the parser's own shape.
    #[cfg(feature = "schema")]
    #[test]
    fn schema_of_reflects_the_document_type() {
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        struct Documented {
            #[allow(dead_code)]
            url: String,
        }
        let schema = schema_of::<Documented>();
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["url"].is_object(), "{schema}");
    }
}
