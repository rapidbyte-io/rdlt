//! What a benchmark reports beside criterion's times: the ratio of paired runs, what a run
//! allocates, and the bytes a run moves.

#[cfg(test)]
mod tests;

use std::fmt;

use super::Mix;

/// Resamples drawn for a bootstrap interval.
const RESAMPLES: usize = 10_000;
/// The seed of every bootstrap, so the same samples give the same interval.
const SEED: u64 = 0x5EED;

/// How many times longer one of two runs measured in pairs takes than the other: the median of
/// the samples' ratios with its 95 % bootstrap interval.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Paired {
    /// The median of the samples' ratios.
    pub median: f64,
    /// The interval's lower end.
    pub low: f64,
    /// The interval's upper end.
    pub high: f64,
    /// The samples the ratios came from.
    pub samples: usize,
}

impl Paired {
    /// The median of `ratios` and its percentile bootstrap interval, or `None` for no ratios.
    ///
    /// # Panics
    ///
    /// Panics where a slice's length does not fit in 64 bits or an index below it in `usize`,
    /// which happens on no platform Rust supports.
    pub fn of(ratios: &[f64]) -> Option<Self> {
        if ratios.is_empty() {
            return None;
        }
        let count = u64::try_from(ratios.len()).expect("a slice's length fits in 64 bits");
        let mut mix = Mix::new(SEED);
        let mut drawn = vec![0.0; ratios.len()];
        let medians = (0..RESAMPLES)
            .map(|_| {
                for slot in &mut drawn {
                    let index = usize::try_from(mix.below(count)).expect("an index below a length");
                    *slot = ratios[index];
                }
                median(&mut drawn)
            })
            .collect();
        let (low, high) = interval(medians);
        Some(Self {
            median: median(&mut ratios.to_vec()),
            low,
            high,
            samples: ratios.len(),
        })
    }
}

impl fmt::Display for Paired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:.3} (95 % interval {:.3} to {:.3}; samples: {})",
            self.median, self.low, self.high, self.samples
        )
    }
}

/// The 2.5th and 97.5th percentiles of `values`.
fn interval(mut values: Vec<f64>) -> (f64, f64) {
    values.sort_by(f64::total_cmp);
    let tail = values.len() / 40;
    (values[tail], values[values.len() - 1 - tail])
}

/// The median of `values`, which it sorts.
fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}
