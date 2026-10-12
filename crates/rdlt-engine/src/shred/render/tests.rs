use proptest::prelude::*;

use super::escape;

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
