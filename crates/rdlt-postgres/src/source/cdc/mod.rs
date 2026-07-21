//! CDC via logical replication (feature 009): pgoutput decoding, slot
//! lifecycle, and the per-table pass machinery over the SQL peek/advance
//! interface. Contracts: `specs/009-postgres-cdc/contracts/`.

pub(crate) mod pgoutput;
pub mod slot;
