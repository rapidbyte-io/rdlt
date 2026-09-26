//! How a merge stream's key is drawn: shared by its partitions or not, spanning one column or
//! two, and sent as types other than its declared `Int64`.

use rdlt_connector::{DecimalType, LogicalType};
use rdlt_testkit::drawn::Scalar;

use super::{PHASES, Row, SimStream};
use crate::rng::SplitMix64;

impl Row {
    /// The row's key as a value of `logical`.
    pub fn key_value(&self, logical: &LogicalType) -> Scalar {
        let key = self.key.unwrap_or_default();
        match logical {
            LogicalType::Decimal(..) => Scalar::Decimal(key.to_string()),
            LogicalType::Utf8 => Scalar::Utf8(key.to_string()),
            #[expect(clippy::cast_precision_loss, reason = "keys are small")]
            LogicalType::Float64 => Scalar::Float64(key as f64),
            _ => Scalar::Int(key),
        }
    }
}

impl SimStream {
    /// Draws how the stream's merge key behaves: shared by every partition, spanning two
    /// columns, and sent as other types than the `Int64` it is declared as: narrower ones, now and
    /// then a wider decimal, and rarely one its column cannot hold.
    pub(super) fn draw_keys(&mut self, rng: &mut SplitMix64) {
        self.shared_keys = rng.chance(400);
        self.composite = rng.chance(400);
        if self.json {
            return;
        }
        for types in &mut self.key_types {
            for logical in types.iter_mut() {
                if rng.chance(250) {
                    *logical = [LogicalType::Int8, LogicalType::Int16, LogicalType::Int32]
                        [usize::try_from(rng.below(3)).unwrap_or(0)]
                    .clone();
                }
            }
        }
        let partitions = self.key_types.len() as u64;
        let mut somewhere = |rng: &mut SplitMix64, logical: LogicalType| {
            let partition = usize::try_from(rng.below(partitions)).unwrap_or(0);
            let phase = usize::try_from(rng.below(PHASES as u64)).unwrap_or(0);
            self.key_types[partition][phase] = logical;
        };
        if rng.chance(250) {
            let precision = if rng.chance(500) { 20 } else { 38 };
            somewhere(rng, whole_decimal(precision));
        }
        if rng.chance(100) {
            let logical = if rng.chance(500) {
                LogicalType::Utf8
            } else {
                LogicalType::Float64
            };
            somewhere(rng, logical);
        }
    }

    /// The merge key's columns.
    pub fn key_columns(&self) -> Vec<&'static str> {
        if self.composite {
            vec!["key", "tag"]
        } else {
            vec!["key"]
        }
    }

    /// The type `partition`'s batches send the key as in `phase`.
    pub fn key_type(&self, partition: usize, phase: usize) -> &LogicalType {
        &self.key_types[partition][phase]
    }
}

/// A decimal of `precision` digits and none after the point.
fn whole_decimal(precision: u8) -> LogicalType {
    LogicalType::Decimal(DecimalType::new(precision, 0).expect("a valid precision"))
}
