//! What decoding a remote connector's answers to its control calls holds, charged to the share of
//! answers being decoded before it is decoded.
//!
//! Each charge waits as any request does, and is released once its message is decoded, so no
//! charge is held while another is waited for; a data call's frames are reserved by the read's
//! admission instead.

#[cfg(test)]
mod tests;

use rdlt_wire::bounded::{Charge, Charging, Held};
use rdlt_wire::limits::Class;
use rdlt_wire::tonic::Status;

use super::{Denied, MemoryBudget};

/// Charges what decoding answers holds to a budget.
pub(crate) struct Decoding(pub(crate) MemoryBudget);

impl Charge for Decoding {
    fn charge(&self, _: Class, bytes: usize) -> Charging {
        let budget = self.0.clone();
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        Box::pin(async move {
            budget
                .acquire_control(bytes)
                .await
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
