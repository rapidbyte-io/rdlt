use proptest::prelude::*;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Int64Array};
use serde::de::Expected;

use rdlt_connector::json::write_string;

use super::{Key, Rendering, escaped_len, json_text};
use crate::shred::meter::{Meter, Over};

#[test]
fn a_key_says_what_it_expects() {
    let mut rendering = Rendering::default();
    let key = Key {
        rendering: &mut rendering,
        first: true,
        meter: &Meter::reserved(),
    };
    assert_eq!((&key as &dyn Expected).to_string(), "an object key");
}

#[test]
fn scalars_rendered_as_json_text_take_a_buffer_of_their_text_s_size() {
    let values: Int64Array = (0..1_000_i64)
        .map(|value| Some(10_000 + value))
        .chain([None])
        .collect();
    let rendered = json_text(&values);
    let text = rendered.as_string::<i32>();
    assert_eq!((text.value(0), text.value(999)), ("10000", "10999"));
    assert!(text.is_null(1_000));
    let bytes = text.values().len();
    assert_eq!(bytes, 5_000);
    assert!(text.values().capacity() <= bytes.next_multiple_of(64));
}

proptest! {
    #[test]
    fn a_string_is_escaped_as_sonic_rs_escapes_it(
        text in prop_oneof![any::<String>(), "[\\x00-\\x7f]{0,16}", "[\"\\\\/\\x00-\\x1f]{0,8}"],
    ) {
        let mut escaped = String::from("x");
        write_string(&text, &mut escaped);
        prop_assert_eq!(&escaped[1..], sonic_rs::to_string(&text).unwrap());
        prop_assert_eq!(escaped_len(&text), escaped.len() - 1);
    }
}

#[test]
fn rendering_charges_what_its_text_grows_by_before_it_grows() {
    let meter = Meter::new(1_000);
    let mut rendering = Rendering::default();
    rendering.push("abc", &meter).unwrap();
    assert_eq!((rendering.text.capacity(), meter.spent()), (3, 3));
    // Twice what it held, once it must grow.
    rendering.push("d", &meter).unwrap();
    assert_eq!((rendering.text.capacity(), meter.spent()), (6, 6));
    // What it must hold, where that is more than twice.
    rendering.push(&"e".repeat(20), &meter).unwrap();
    assert_eq!((rendering.text.capacity(), meter.spent()), (24, 24));
    rendering.clear();
    rendering.integer(-7_i64, &meter).unwrap();
    assert_eq!(rendering.text, "-7");
    assert_eq!(
        meter.spent(),
        78,
        "room for any integer's digits, its sign too"
    );
    // A text the meter has no room for is left as it was.
    let tight = Meter::new(2);
    let mut empty = Rendering::default();
    assert_eq!(empty.push("xyz", &tight), Err(Over));
    assert_eq!((empty.text.capacity(), tight.spent()), (0, 0));
}

#[test]
fn a_string_is_charged_its_escaped_text_where_the_text_may_grow_for_it() {
    let meter = Meter::new(1_000);
    let mut rendering = Rendering::default();
    rendering.room(110, &meter).unwrap();
    assert_eq!(meter.spent(), 110);
    // Thirty control characters escape to six bytes each, past the capacity.
    rendering.string(&"\u{1}".repeat(30), &meter).unwrap();
    assert_eq!(rendering.text.len(), 30 * 6 + 2);
    assert_eq!((rendering.text.capacity(), meter.spent()), (220, 220));
    // A short string well within the capacity is written without counting its escapes.
    rendering.clear();
    rendering.string("a\"b", &meter).unwrap();
    assert_eq!(
        (rendering.text.as_str(), meter.spent()),
        ("\"a\\\"b\"", 220)
    );
}
