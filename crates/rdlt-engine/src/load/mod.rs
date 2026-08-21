//! The load stage: [`item`] is the vocabulary flowing shred → load; [`loader`]
//! drives one `LoadSession` through ensure → write → commit under the commit
//! and batch policies; [`lower`] is the capability-driven lowering at the
//! destination seam and [`apply`] the two write primitives the live loader and
//! WAL replay both go through.

pub(crate) mod apply;
mod item;
mod loader;
mod lower;
pub(crate) mod session_exit;

pub(crate) use item::LoadItem;
pub(crate) use loader::{Loader, Policies, Sink};
