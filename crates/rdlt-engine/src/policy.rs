//! Schema-change policies: what the engine does when incoming data would
//! change a table's schema, resolved per column, table, or run.

use std::collections::BTreeMap;

use rdlt_core::id::TableName;
use serde::{Deserialize, Serialize};

/// What to do when incoming data would change a schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    /// Apply the delta and continue (default).
    Evolve,
    /// Turn the would-be delta into a typed `ContractViolation` before any row of the
    /// violating batch is written.
    ///
    /// Scoped to ONE RUN: the schema this is judged against is what the current
    /// run has established, so a change appearing between runs is not refused —
    /// the destinations apply it as additive DDL. A run that observes fewer
    /// columns than the last is likewise never drift.
    Freeze,
    /// Drop non-conforming rows; count them.
    DiscardRow,
    /// Null out non-conforming values; count them.
    DiscardValue,
}

/// A column addressed for policy overrides.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ColumnRef {
    /// The table the column belongs to.
    pub table: TableName,
    /// The column name.
    pub column: String,
}

/// Policy resolution: column override → table override → default.
///
/// A table with no entry of its own inherits from its ancestors before reaching
/// the default, so a contract on a stream governs the child tables its nested
/// collections create. An explicit entry always wins over an inherited one —
/// inheritance fills a gap, it does not override a decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPolicy {
    /// Applied where no more specific entry matches.
    pub default: PolicyAction,
    /// Per-table overrides. A child table with no entry inherits from its
    /// ancestors before falling back to `default`.
    pub per_table: BTreeMap<TableName, PolicyAction>,
    /// Per-column overrides, the most specific level.
    pub per_column: BTreeMap<ColumnRef, PolicyAction>,
}

impl SchemaPolicy {
    /// Permit schema changes everywhere — the permissive default a first run wants.
    pub fn evolve() -> Self {
        Self::with_default(PolicyAction::Evolve)
    }

    /// Refuse every schema change: a would-be delta becomes a typed
    /// `ContractViolation` before any row of the violating batch is written.
    pub fn freeze() -> Self {
        Self::with_default(PolicyAction::Freeze)
    }

    /// A policy applying one action everywhere, with no overrides yet.
    pub fn with_default(default: PolicyAction) -> Self {
        Self {
            default,
            per_table: BTreeMap::new(),
            per_column: BTreeMap::new(),
        }
    }

    /// Override the action for one table. Its child tables inherit this unless
    /// they carry an entry of their own.
    pub fn table(mut self, table: impl Into<TableName>, action: PolicyAction) -> Self {
        self.per_table.insert(table.into(), action);
        self
    }

    /// Override the action for one column — the most specific level, winning
    /// over both the table entry and the default.
    pub fn column(
        mut self,
        table: impl Into<TableName>,
        column: impl Into<String>,
        action: PolicyAction,
    ) -> Self {
        self.per_column.insert(
            ColumnRef {
                table: table.into(),
                column: column.into(),
            },
            action,
        );
        self
    }

    /// Resolve the action for a change: column override, then table override,
    /// then the default. Pass `column: None` for a table-level change.
    pub fn action_for(&self, table: &TableName, column: Option<&str>) -> PolicyAction {
        if let Some(column) = column {
            let key = ColumnRef {
                table: table.clone(),
                column: column.to_owned(),
            };
            if let Some(action) = self.per_column.get(&key) {
                return *action;
            }
        }
        self.per_table.get(table).copied().unwrap_or(self.default)
    }
}

impl Default for SchemaPolicy {
    fn default() -> Self {
        Self::evolve()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution_order_column_then_table_then_default() {
        let policy = SchemaPolicy::evolve()
            .table("users", PolicyAction::Freeze)
            .column("users", "email", PolicyAction::DiscardValue);
        let users = TableName::new("users");
        let other = TableName::new("orders");
        assert_eq!(
            policy.action_for(&users, Some("email")),
            PolicyAction::DiscardValue
        );
        assert_eq!(
            policy.action_for(&users, Some("name")),
            PolicyAction::Freeze
        );
        assert_eq!(policy.action_for(&users, None), PolicyAction::Freeze);
        assert_eq!(policy.action_for(&other, None), PolicyAction::Evolve);
    }

    /// The named constructors are the API a caller reaches for. `Default` for
    /// this type is `evolve()`, so a `freeze()` that silently fell back to the
    /// default would INVERT the contract, handing Evolve to a caller who asked
    /// to freeze — each constructor is pinned to its own action.
    #[test]
    fn named_constructors_select_their_own_default_action() {
        assert_eq!(SchemaPolicy::freeze().default, PolicyAction::Freeze);
        assert_eq!(SchemaPolicy::evolve().default, PolicyAction::Evolve);
        assert_eq!(
            SchemaPolicy::with_default(PolicyAction::DiscardRow).default,
            PolicyAction::DiscardRow
        );
        assert_eq!(
            SchemaPolicy::with_default(PolicyAction::DiscardValue).default,
            PolicyAction::DiscardValue
        );

        // A fresh policy governs by its default alone — no hidden entries.
        let frozen = SchemaPolicy::freeze();
        assert!(frozen.per_table.is_empty());
        assert!(frozen.per_column.is_empty());
        assert_eq!(
            frozen.action_for(&TableName::new("anything"), None),
            PolicyAction::Freeze,
            "freeze() must actually freeze an unmentioned table"
        );
        assert_eq!(
            frozen.action_for(&TableName::new("anything"), Some("col")),
            PolicyAction::Freeze
        );
    }

    /// `Default` deliberately means Evolve (the permissive setup a first run
    /// wants), and that is exactly why it must never be confused with `freeze()`.
    #[test]
    fn default_is_evolve_and_distinct_from_freeze() {
        assert_eq!(SchemaPolicy::default(), SchemaPolicy::evolve());
        assert_ne!(SchemaPolicy::default(), SchemaPolicy::freeze());
    }
}
