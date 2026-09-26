use super::Digest;

#[test]
fn a_digest_shows_as_lowercase_hex() {
    let mut bytes = [0; 32];
    bytes[0] = 0xab;
    bytes[31] = 0x0f;
    let digest = Digest(bytes);
    let hex = format!("ab{}0f", "00".repeat(30));
    assert_eq!(digest.to_string(), hex);
    assert_eq!(format!("{digest:?}"), format!("Digest({hex})"));
}
