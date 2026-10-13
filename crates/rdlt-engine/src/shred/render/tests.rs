use proptest::prelude::*;

use arrow_array::cast::AsArray;
use arrow_array::{Array, Int64Array};
use serde::de::Expected;

use super::{Key, Rendering, escape, json_text};

#[test]
fn a_key_says_what_it_expects() {
    let mut rendering = Rendering::default();
    let key = Key {
        rendering: &mut rendering,
        first: true,
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
        escape(&text, &mut escaped);
        prop_assert_eq!(&escaped[1..], sonic_rs::to_string(&text).unwrap());
    }
}
