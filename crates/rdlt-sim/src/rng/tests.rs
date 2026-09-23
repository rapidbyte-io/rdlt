use super::SplitMix64;

#[test]
fn matches_the_reference_sequence() {
    let mut rng = SplitMix64::new(0);
    assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
    assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
}

#[test]
fn the_same_seed_yields_the_same_sequence() {
    let mut a = SplitMix64::new(42);
    let mut b = SplitMix64::new(42);
    for _ in 0..1000 {
        assert_eq!(a.next_u64(), b.next_u64());
    }
}
