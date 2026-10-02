//! What decoding a remote connector's answers holds, charged to the budget before it is decoded.
//!
//! A frame of a read is charged to what pushes may take, which the batch it carries is admitted
//! to next; every other answer to the share of answers being decoded. Each charge waits as any
//! request does, and is released once its message is decoded, before the event a frame carries
//! waits for room, so no charge is held while another is waited for.

#[cfg(test)]
mod tests;

use rdlt_wire::bounded::{Charge, Charging, Held};
use rdlt_wire::limits::Class;
use rdlt_wire::tonic::Status;

use super::{Denied, MemoryBudget};

/// Charges what decoding answers holds to a budget.
pub(crate) struct Decoding(pub(crate) MemoryBudget);

impl Charge for Decoding {
    fn charge(&self, class: Class, bytes: usize) -> Charging {
        let budget = self.0.clone();
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        Box::pin(async move {
            let reserved = match class {
                Class::Data => budget.acquire(bytes).await,
                _ => budget.acquire_control(bytes).await,
            };
            reserved
                .map(|reservation| Box::new(reservation) as Held)
                .map_err(|denied| match denied {
                    Denied::TooLarge(large) => Status::out_of_range(large.to_string()),
                    Denied::Exhausted(exhausted) => {
                        Status::resource_exhausted(exhausted.to_string())
                    }
                })
        })
    }
}
