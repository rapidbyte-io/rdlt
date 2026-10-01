//! Swapping a replace generation in as its table.

use super::super::catalog::GENERATIONS;
use super::super::{Owned, SqlDialect, SqlPlanner, SqlValue, Statement};
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::GenerationId;

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statements swapping `generation` in as the table `owned` names, dropping every other
    /// generation of it; `generations` are the table's generation tables and `base_exists` says
    /// whether the table does.
    ///
    /// A generation that has no table leaves the base table empty. Where the dialect's schema
    /// changes do not commit with its transactions, a swap that renames or drops a generation
    /// table is `Unsupported`: it could not be atomic.
    pub fn swap(
        &self,
        owned: &Owned,
        base_exists: bool,
        generation: GenerationId,
        generations: &[(String, GenerationId)],
    ) -> Result<Vec<Statement>> {
        let base = owned.name();
        // Only generation tables change the schema: renamed in, or dropped.
        if !generations.is_empty() && !self.swaps_atomically() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "the dialect's schema changes do not commit with its transactions, so a replace \
                 generation cannot be swapped in atomically",
            ));
        }
        let statement = |sql: String| Statement {
            sql,
            params: Vec::new(),
        };
        let mut plan = Vec::new();
        let swapped = generations.iter().find(|(_, found)| *found == generation);
        match swapped {
            Some((name, _)) => {
                plan.push(statement(format!(
                    "DROP TABLE IF EXISTS {}",
                    self.quote(base)
                )));
                plan.push(statement(format!(
                    "ALTER TABLE {} RENAME TO {}",
                    self.quote(name),
                    self.quote(base)
                )));
                // The generation's indexes keep its name: the next writer indexes the table
                // under its own.
                for index in [self.key_index_name(name), self.root_index_name(name)] {
                    let sql = self
                        .dialect
                        .drop_index(&self.quote(&index), &self.quote(base));
                    plan.push(statement(sql));
                }
            }
            None if base_exists => {
                plan.push(statement(format!("DELETE FROM {}", self.quote(base))));
            }
            None => {}
        }
        let others = generations.iter().filter(|(_, found)| *found != generation);
        for (name, _) in others {
            plan.push(statement(format!(
                "DROP TABLE IF EXISTS {}",
                self.quote(name)
            )));
        }
        let mut forget = self.sql();
        let base = forget.bind(SqlValue::Text(base.to_owned()));
        forget.push(&format!("DELETE FROM {GENERATIONS} WHERE base = {base}"));
        plan.push(forget.finish());
        Ok(plan)
    }

    /// Whether a replace generation swaps in atomically, as it does where the dialect's schema
    /// changes commit with its transactions.
    ///
    /// Where they do not, a swap renaming or dropping a generation table is refused, so a
    /// destination on that dialect leaves replace out of what it declares.
    pub fn swaps_atomically(&self) -> bool {
        self.dialect.transactional_ddl()
    }
}
