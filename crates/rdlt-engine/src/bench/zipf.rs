//! Keys drawn from a Zipf distribution, by inverting its cumulative weights.

#[cfg(test)]
mod tests;

use std::num::NonZeroU64;

use super::Mix;

/// Keys from zero below a count, key `k` drawn as often as `1 / (k + 1)^θ` says against every
/// other's.
#[derive(Clone, Debug, PartialEq)]
pub struct Zipf {
    /// The weights of the keys up to each, summed.
    cumulative: Vec<f64>,
}

impl Zipf {
    /// Keys below `items`, skewed by `theta`.
    pub fn new(items: NonZeroU64, theta: f64) -> Self {
        let mut sum = 0.0;
        let cumulative = (1..=items.get())
            .map(|rank| {
                #[expect(clippy::cast_precision_loss, reason = "a rank far below 2^52")]
                let rank = rank as f64;
                sum += rank.powf(-theta);
                sum
            })
            .collect();
        Self { cumulative }
    }

    /// How many keys there are to draw.
    pub fn items(&self) -> u64 {
        self.cumulative.len() as u64
    }

    /// The next key, from `mix`: the first whose summed weight passes a uniform share of all.
    pub fn draw(&self, mix: &mut Mix) -> u64 {
        let total = self.cumulative.last().copied().unwrap_or(0.0);
        let target = unit(mix) * total;
        self.cumulative.partition_point(|&sum| sum <= target) as u64
    }
}

/// A number drawn from `mix`, at least zero and below one.
fn unit(mix: &mut Mix) -> f64 {
    /// Two to the 53rd, one more than the most a float's mantissa holds exactly.
    const SPAN: f64 = 9_007_199_254_740_992.0;
    #[expect(clippy::cast_precision_loss, reason = "53 bits fit a float's mantissa")]
    let drawn = (mix.draw() >> 11) as f64;
    drawn / SPAN
}
