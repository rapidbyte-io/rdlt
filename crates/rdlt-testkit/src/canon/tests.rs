use proptest::prelude::*;

use super::{decimal, float};

/// A double halfway between two decimals of its shortest length, with its exact text, which ends
/// in a 5, and the even one of the two: `whole` plus `odd` over `2^bits`, where the ulp at
/// `whole`'s magnitude lets both decimals one digit shorter than the exact one read back as it.
fn halfway(whole: u64, odd: u64, bits: u32) -> (f64, String, String) {
    let places = usize::try_from(bits).unwrap();
    let digits = odd * 5_u64.pow(bits);
    let exact = format!("{whole}.{digits:0places$}");
    let last = digits / 10;
    let even = last + last % 2;
    let even = decimal(&format!("{whole}.{even:0width$}", width = places - 1));
    #[expect(
        clippy::cast_precision_loss,
        reason = "the whole number and the fraction are both exact in a double"
    )]
    let value = whole as f64 + odd as f64 / f64::from(1_u32 << bits);
    (value, exact, even)
}

/// Doubles halfway between two shortest decimals: a fraction of `bits` bits on a whole number
/// whose ulp puts the tie one digit before the fraction ends.
fn halfways() -> impl Strategy<Value = (f64, String, String)> {
    let at = |bits: u32, low: u32| {
        (1_u64 << low..1_u64 << (low + 2), 0..1_u64 << (bits - 1))
            .prop_map(move |(whole, odd)| halfway(whole, 2 * odd + 1, bits))
    };
    prop_oneof![at(2, 49), at(3, 46), at(4, 43)]
}

#[test]
fn a_double_halfway_between_two_decimals_reads_as_the_even_one() {
    let read = |text: &str| text.parse::<f64>().unwrap();
    assert_eq!(float(read("25761051960532.0625")), "25761051960532.062");
    assert_eq!(float(read("-25761051960532.0625")), "-25761051960532.062");
    assert_eq!(float(read("1053724231278760.25")), "1053724231278760.2");
}

proptest! {
    #[test]
    fn every_double_halfway_between_two_decimals_reads_as_the_even_one(
        (value, exact, even) in halfways(),
        negative in any::<bool>(),
    ) {
        prop_assert_eq!(exact.parse::<f64>().unwrap().to_bits(), value.to_bits());
        let sign = if negative { "-" } else { "" };
        let value = if negative { -value } else { value };
        let even = format!("{sign}{even}");
        prop_assert_eq!(&float(value), &even);
        prop_assert_eq!(decimal(&serde_json::to_string(&value).unwrap()), even);
    }

    #[test]
    fn a_doubles_text_is_the_text_json_writers_write(value in any::<f64>()) {
        let written = serde_json::to_string(&value).unwrap();
        prop_assert_eq!(float(value), decimal(&written));
    }
}
