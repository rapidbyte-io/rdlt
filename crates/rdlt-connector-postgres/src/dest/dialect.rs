//! The postgres [`MergeDialect`]: the EXTRACTION SOURCE — every
//! hook keeps the trait default (the defaults ARE this destination's text,
//! golden-pinned in tests/golden_sql.rs) except the arrival column, which is
//! the stage's real `__rdlt_arrival` BIGSERIAL.

use rdlt_connector_sqlcore::MergeDialect;

#[derive(Debug, Clone, Copy)]
pub struct PgDialect;

impl MergeDialect for PgDialect {
    fn arrival_order(&self) -> String {
        super::quote(super::commit::ARRIVAL_COL)
    }
}
