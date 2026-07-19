//! Schema-change policies (spec FR-010, design doc §5.4).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::TableName;

/// What to do when incoming data would change a schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    /// Apply the delta and continue (default).
    Evolve,
    /// Turn the would-be delta into a typed `ContractViolation` before any row of the
    /// violating batch is written.
    Freeze,
    /// Drop non-conforming rows; count them.
    DiscardRow,
    /// Null out non-conforming values; count them.
    DiscardValue,
}

/// A column addressed for policy overrides.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: TableName,
    pub column: String,
}

/// Policy resolution: column override → table override → default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaPolicy {
    pub default: PolicyAction,
    pub per_table: BTreeMap<TableName, PolicyAction>,
    pub per_column: BTreeMap<ColumnRef, PolicyAction>,
}

impl SchemaPolicy {
    pub fn evolve() -> Self {
        Self::with_default(PolicyAction::Evolve)
    }

    pub fn freeze() -> Self {
        Self::with_default(PolicyAction::Freeze)
    }

    pub fn with_default(default: PolicyAction) -> Self {
        Self {
            default,
            per_table: BTreeMap::new(),
            per_column: BTreeMap::new(),
        }
    }

    pub fn table(mut self, table: impl Into<TableName>, action: PolicyAction) -> Self {
        self.per_table.insert(table.into(), action);
        self
    }

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
}
