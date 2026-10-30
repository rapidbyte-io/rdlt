//! What decoding a remote connector's answers holds, charged to the budget before it is decoded.
//!
//! A frame of a read is charged to what pushes may take, its wait counted apart from pushes', no
//! less than the push it carries is charged: the push is admitted within that charge, shrunk to
//! the push's, so the frame's bytes are charged once from before it is decoded until its push is
//! written. Every other answer is charged to the share of answers being decoded until it is
//! decoded. Each charge waits as any request does.

#[cfg(test)]
mod tests;

use rdlt_wire::bounded::{Charge, Charging, Held};
use rdlt_wire::limits::Class;
use rdlt_wire::tonic::Status;

use super::{Denied, MemoryBudget};
use crate::cost::Admitted;

/// Charges what decoding answers holds to a budget.
pub(crate) struct Decoding(pub(crate) MemoryBudget);

impl Charge for Decoding {
    fn charge(&self, class: Class, bytes: usize) -> Charging {
        let budget = self.0.clone();
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        Box::pin(async move {
            let held = match class {
                Class::Data => budget
                    .acquire_decoding(bytes)
                    .await
                    .map(|reservation| Box::new(Admitted::new(bytes, reservation)) as Held),
                _ => budget
                    .acquire_control(bytes)
                    .await
                    .map(|reservation| Box::new(reservation) as Held),
            };
            held.map_err(|denied| match denied {
                Denied::TooLarge(large) => Status::out_of_range(large.to_string()),
                Denied::Exhausted(exhausted) => Status::resource_exhausted(exhausted.to_string()),
            })
        })
    }
}
