//! The counts of cores the shred bench's pools run within.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;

/// Each power of two below `host`, then `host`: the pools one stream is shredded on to see how
/// its throughput grows with the cores the host gives it.
pub fn pool_cores(host: NonZeroUsize) -> Vec<NonZeroUsize> {
    const TWO: NonZeroUsize = NonZeroUsize::new(2).expect("two is not zero");
    std::iter::successors(Some(NonZeroUsize::MIN), |count| count.checked_mul(TWO))
        .take_while(|count| *count < host)
        .chain([host])
        .collect()
}
