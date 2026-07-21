//! Destination handle + builder: connection string, dataset, TLS posture.
//! (Feature 008 T001: relocated verbatim from the old single-file module.)

use rdlt_connector::DestError;
use tokio_postgres::Client;

#[derive(Debug, Clone)]
pub struct Postgres {
    pub(super) conn_string: String,
    pub(super) schema: String,
    pub(super) tls: Option<crate::tls::TlsPolicy>,
    pub(super) options: PgDestOptions,
}

impl Postgres {
    /// `conn_string`: e.g. `host=localhost user=postgres password=pw dbname=raw`.
    pub fn connect(conn_string: impl Into<String>) -> Self {
        Self {
            conn_string: conn_string.into(),
            schema: "public".into(),
            tls: None,
            options: PgDestOptions::default(),
        }
    }

    /// Target schema (dataset); created if missing.
    pub fn dataset(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// TLS posture (feature 006, contract tls-policy.md) — the SAME policy
    /// type the source uses; the two directions share one connect path.
    pub fn tls(mut self, policy: crate::tls::TlsPolicy) -> Self {
        self.tls = Some(policy);
        self
    }

    pub(super) async fn client(&self) -> Result<Client, DestError> {
        let crate::tls::ParsedConn { pg, policy } =
            crate::tls::parse_conn(&self.conn_string, self.tls.as_ref())
                .map_err(|e| DestError::fatal(e.to_string()))?;
        match crate::tls::connect(&pg, &policy).await {
            Ok(client) => Ok(client),
            Err(crate::tls::ConnectResult::Config(e)) => Err(DestError::fatal(e.to_string())),
            Err(crate::tls::ConnectResult::Connect(e)) if e.transient => {
                Err(DestError::transient(e.to_string()))
            }
            Err(crate::tls::ConnectResult::Connect(e)) => Err(DestError::fatal(e.to_string())),
        }
    }
}

// ---- Feature 008 US2/US3: destination-side options (data-model.md) ----
//
// Strategy selection NEVER crosses the connector SPI: the engine's WriteMode
// stays frozen; these options choose how THIS destination executes Merge.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// How Merge executes at this destination (contract merge-strategies.md).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Atomic delete-then-insert (the pre-008 behavior; M1).
    #[default]
    DeleteInsert,
    /// Matched keys update in place via conflict-update; requires a unique
    /// index on the merge identity (auto-ensured; M2/M3).
    Upsert,
    /// Full version history with validity columns (contract scd2.md).
    Scd2,
}

/// SCD2 settings (contract scd2.md).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Scd2Options {
    /// Validity-start column added to the target table.
    #[serde(default = "default_valid_from")]
    pub valid_from: String,
    /// Validity-end column; NULL = the active version.
    #[serde(default = "default_valid_to")]
    pub valid_to: String,
    /// What happens to active keys ABSENT from a load (S6).
    #[serde(default)]
    pub absent: AbsentPolicy,
}

impl Default for Scd2Options {
    fn default() -> Self {
        Self {
            valid_from: default_valid_from(),
            valid_to: default_valid_to(),
            absent: AbsentPolicy::default(),
        }
    }
}

fn default_valid_from() -> String {
    "_rdlt_valid_from".into()
}

fn default_valid_to() -> String {
    "_rdlt_valid_to".into()
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AbsentPolicy {
    /// Absent keys keep their active version (incremental feeds are partial).
    #[default]
    Keep,
    /// Absent keys retire at the boundary (full-feed semantics).
    Retire,
}

/// Ordered in-load survivor selection (feature 010, contract
/// merge-refinements.md MR1): among same-identity rows within one load,
/// the row this column ranks first survives. Values beat NULL; ties keep
/// the deterministic arrival-order last-wins. `order` is REQUIRED —
/// survivor selection is too important for an implicit default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DedupSort {
    pub column: String,
    pub order: SortOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    /// Least value survives.
    Asc,
    /// Greatest value survives.
    Desc,
}

/// Per-table overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PgTableOptions {
    #[serde(default)]
    pub merge_strategy: Option<MergeStrategy>,
    /// CDC-style deletion flag column (M4): flagged rows DELETE their key
    /// instead of merging. Boolean columns compare `= TRUE`, other types
    /// `IS NOT NULL`. Not valid with scd2 (S8).
    #[serde(default)]
    pub hard_delete: Option<String>,
    #[serde(default)]
    pub scd2: Option<Scd2Options>,
    /// Ordered survivor selection within one load (feature 010, MR1/MR2).
    #[serde(default)]
    pub dedup_sort: Option<DedupSort>,
    /// Scope replacement (feature 010, MR3–MR5): a non-unique column set;
    /// a merge load replaces every scope present in its delivered rows
    /// (undelivered rows in those scopes disappear), leaving other scopes
    /// untouched. NULL is not a scope. Keyed structured tables only; not
    /// valid with scd2.
    #[serde(default)]
    pub merge_key: Option<Vec<String>>,
}

/// Destination-wide defaults + per-table overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PgDestOptions {
    #[serde(default)]
    pub merge_strategy: MergeStrategy,
    #[serde(default)]
    pub tables: BTreeMap<String, PgTableOptions>,
}

impl PgDestOptions {
    /// The embedder entry point (mirrors the sources' from_value contract).
    pub fn from_value(value: serde_json::Value) -> Result<Self, String> {
        let options: Self = serde_json::from_value(value).map_err(|e| e.to_string())?;
        options.validate()?;
        Ok(options)
    }

    /// Validation errors NAME the offending field (FR-012).
    pub fn validate(&self) -> Result<(), String> {
        for (table, opts) in &self.tables {
            let strategy = opts.merge_strategy.unwrap_or(self.merge_strategy);
            if let Some(col) = &opts.hard_delete {
                if col.trim().is_empty() {
                    return Err(format!("tables.{table}.hard_delete: empty column name"));
                }
                if strategy == MergeStrategy::Scd2 {
                    return Err(format!(
                        "tables.{table}.hard_delete: not valid with merge_strategy scd2 \
                         (contract scd2.md S8 — deletion-as-retirement is future work)"
                    ));
                }
            }
            if let Some(dedup) = &opts.dedup_sort
                && dedup.column.trim().is_empty()
            {
                return Err(format!("tables.{table}.dedup_sort: empty column name"));
            }
            if let Some(scope) = &opts.merge_key {
                if scope.is_empty() {
                    return Err(format!("tables.{table}.merge_key: empty column list"));
                }
                if scope.iter().any(|c| c.trim().is_empty()) {
                    return Err(format!("tables.{table}.merge_key: empty column name"));
                }
                let mut seen = std::collections::BTreeSet::new();
                if let Some(dup) = scope.iter().find(|c| !seen.insert(c.as_str())) {
                    return Err(format!(
                        "tables.{table}.merge_key: column `{dup}` listed twice"
                    ));
                }
                if strategy == MergeStrategy::Scd2 {
                    return Err(format!(
                        "tables.{table}.merge_key: not valid with merge_strategy scd2 \
                         (contract merge-refinements.md MR6 — scd2 retirement has its \
                         own absence policy)"
                    ));
                }
            }
            if let Some(scd2) = &opts.scd2 {
                if strategy != MergeStrategy::Scd2 {
                    return Err(format!(
                        "tables.{table}.scd2: options present but merge_strategy is \
                         {strategy:?} — set merge_strategy: scd2"
                    ));
                }
                if scd2.valid_from.trim().is_empty() || scd2.valid_to.trim().is_empty() {
                    return Err(format!(
                        "tables.{table}.scd2: validity column names must not be empty"
                    ));
                }
                if scd2.valid_from == scd2.valid_to {
                    return Err(format!(
                        "tables.{table}.scd2: valid_from and valid_to must differ"
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn strategy_for(&self, table: &str) -> MergeStrategy {
        self.tables
            .get(table)
            .and_then(|t| t.merge_strategy)
            .unwrap_or(self.merge_strategy)
    }

    pub(super) fn hard_delete_for(&self, table: &str) -> Option<&str> {
        self.tables
            .get(table)
            .and_then(|t| t.hard_delete.as_deref())
    }

    pub(super) fn scd2_for(&self, table: &str) -> Scd2Options {
        self.tables
            .get(table)
            .and_then(|t| t.scd2.clone())
            .unwrap_or_default()
    }

    pub(super) fn dedup_sort_for(&self, table: &str) -> Option<&DedupSort> {
        self.tables.get(table).and_then(|t| t.dedup_sort.as_ref())
    }

    pub(super) fn merge_key_for(&self, table: &str) -> Option<&[String]> {
        self.tables
            .get(table)
            .and_then(|t| t.merge_key.as_deref())
    }
}

impl Postgres {
    /// Strategy/hard-delete/SCD2 options (feature 008). Validated here —
    /// errors name the field.
    pub fn options(mut self, options: PgDestOptions) -> Result<Self, DestError> {
        options.validate().map_err(DestError::fatal)?;
        self.options = options;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_matrix_names_the_field() {
        // scd2 options without the strategy.
        let bad = PgDestOptions::from_value(serde_json::json!({
            "tables": {"t": {"scd2": {}}}
        }))
        .unwrap_err();
        assert!(
            bad.contains("tables.t.scd2") && bad.contains("merge_strategy"),
            "{bad}"
        );

        // hard_delete + scd2 contradiction (S8).
        let bad = PgDestOptions::from_value(serde_json::json!({
            "tables": {"t": {"merge_strategy": "scd2", "hard_delete": "deleted"}}
        }))
        .unwrap_err();
        assert!(
            bad.contains("tables.t.hard_delete") && bad.contains("scd2"),
            "{bad}"
        );

        // Empty hard_delete column.
        let bad = PgDestOptions::from_value(serde_json::json!({
            "tables": {"t": {"hard_delete": "  "}}
        }))
        .unwrap_err();
        assert!(bad.contains("empty column name"), "{bad}");

        // Identical validity columns.
        let bad = PgDestOptions::from_value(serde_json::json!({
            "tables": {"t": {"merge_strategy": "scd2",
                              "scd2": {"valid_from": "v", "valid_to": "v"}}}
        }))
        .unwrap_err();
        assert!(bad.contains("must differ"), "{bad}");

        // Unknown strategy dies at serde; unknown fields likewise.
        assert!(
            PgDestOptions::from_value(serde_json::json!({"merge_strategy": "replace"})).is_err()
        );
        assert!(PgDestOptions::from_value(serde_json::json!({"nope": 1})).is_err());

        // Feature 010 shape matrix (MR6 parse layer).
        let bad = PgDestOptions::from_value(serde_json::json!({
            "tables": {"t": {"dedup_sort": {"column": " ", "order": "desc"}}}
        }))
        .unwrap_err();
        assert!(bad.contains("tables.t.dedup_sort"), "{bad}");
        // order is REQUIRED — no implicit survivor direction.
        assert!(
            PgDestOptions::from_value(serde_json::json!({
                "tables": {"t": {"dedup_sort": {"column": "seq"}}}
            }))
            .is_err()
        );
        for (scope, needle) in [
            (serde_json::json!([]), "empty column list"),
            (serde_json::json!([" "]), "empty column name"),
            (serde_json::json!(["day", "day"]), "listed twice"),
        ] {
            let bad = PgDestOptions::from_value(serde_json::json!({
                "tables": {"t": {"merge_key": scope}}
            }))
            .unwrap_err();
            assert!(bad.contains(needle), "{bad}");
        }
        let bad = PgDestOptions::from_value(serde_json::json!({
            "tables": {"t": {"merge_strategy": "scd2", "merge_key": ["day"]}}
        }))
        .unwrap_err();
        assert!(
            bad.contains("tables.t.merge_key") && bad.contains("scd2"),
            "{bad}"
        );

        // Valid: destination default upsert + per-table scd2.
        let ok = PgDestOptions::from_value(serde_json::json!({
            "merge_strategy": "upsert",
            "tables": {
                "dims": {"merge_strategy": "scd2", "scd2": {"absent": "retire"}},
                "facts": {"hard_delete": "is_deleted",
                           "dedup_sort": {"column": "seq", "order": "desc"},
                           "merge_key": ["day", "tenant"]}
            }
        }))
        .expect("valid options");
        assert_eq!(ok.strategy_for("dims"), MergeStrategy::Scd2);
        assert_eq!(ok.strategy_for("facts"), MergeStrategy::Upsert);
        assert_eq!(ok.strategy_for("other"), MergeStrategy::Upsert);
        assert_eq!(ok.hard_delete_for("facts"), Some("is_deleted"));
        assert_eq!(ok.scd2_for("dims").absent, AbsentPolicy::Retire);
        let dedup = ok.dedup_sort_for("facts").expect("dedup_sort");
        assert_eq!((dedup.column.as_str(), dedup.order), ("seq", SortOrder::Desc));
        assert_eq!(
            ok.merge_key_for("facts"),
            Some(&["day".to_string(), "tenant".to_string()][..])
        );
        assert_eq!(ok.dedup_sort_for("other"), None);
    }
}
