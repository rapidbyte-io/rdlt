use arrow_array::{Array, Float32Array, Float64Array};
use proptest::prelude::*;

use super::write_float;

fn written(array: &dyn Array, row: usize) -> String {
    let mut out = Vec::new();
    write_float(array, row, &mut out);
    String::from_utf8(out).unwrap()
}

#[test]
fn a_float_is_written_as_its_shortest_text_or_the_name_of_what_it_is() {
    let doubles = Float64Array::from(vec![
        3.0,
        -0.0,
        1e300,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ]);
    let texts = [
        "3.0",
        "-0.0",
        "1e+300",
        "\"NaN\"",
        "\"Infinity\"",
        "\"-Infinity\"",
    ];
    for (row, text) in texts.into_iter().enumerate() {
        assert_eq!(written(&doubles, row), text, "{:?}", doubles.value(row));
    }
    // A 32-bit float is written as the 64-bit float it widens to.
    let singles = Float32Array::from(vec![0.1, f32::NAN]);
    assert_eq!(written(&singles, 0), "0.10000000149011612");
    assert_eq!(written(&singles, 1), "\"NaN\"");
}

proptest! {
    #[test]
    fn a_finite_float_reads_back_as_itself(value in any::<f64>().prop_filter("finite", |v| v.is_finite())) {
        let text = written(&Float64Array::from(vec![value]), 0);
        prop_assert_eq!(text.parse::<f64>().unwrap().to_bits(), value.to_bits());
    }

    #[test]
    fn a_finite_32_bit_float_reads_back_as_the_double_it_widens_to(
        value in any::<f32>().prop_filter("finite", |v| v.is_finite()),
    ) {
        let text = written(&Float32Array::from(vec![value]), 0);
        prop_assert_eq!(text.parse::<f64>().unwrap().to_bits(), f64::from(value).to_bits());
    }
}
