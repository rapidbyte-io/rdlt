use super::base32;

#[test]
fn bytes_in_base_32_are_rfc_4648_s_lower_case_without_padding() {
    for (bytes, text) in [
        ("", ""),
        ("f", "my"),
        ("foo", "mzxw6"),
        ("foob", "mzxw6yq"),
        ("fooba", "mzxw6ytb"),
        ("foobar", "mzxw6ytboi"),
    ] {
        assert_eq!(base32(bytes.as_bytes()), text, "{bytes}");
    }
}
