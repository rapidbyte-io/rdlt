use super::Mix;

#[test]
fn a_mix_draws_the_splitmix64_sequence_of_its_seed() {
    let mut mix = Mix::new(0);
    let drawn: Vec<u64> = (0..3).map(|_| mix.draw()).collect();
    assert_eq!(
        drawn,
        [
            0xE220_A839_7B1D_CDAF,
            0x6E78_9E6A_A1B9_65F4,
            0x06C4_5D18_8009_454F
        ]
    );
}

#[test]
fn numbers_drawn_below_a_bound_stay_below_it() {
    for bound in [1, 2, 7, 1000, u64::MAX] {
        let mut mix = Mix::new(7);
        assert!((0..1000).all(|_| mix.below(bound) < bound), "bound {bound}");
    }
}

#[test]
fn a_bound_reduces_the_number_the_seed_draws_next() {
    let (mut bounded, mut plain) = (Mix::new(7), Mix::new(7));
    for bound in [3, 1000, 1 << 40] {
        assert_eq!(bounded.below(bound), plain.draw() % bound);
    }
}
