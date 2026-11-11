use super::Installed;
use crate::seed::Seed;

#[test]
fn seeds_install_their_played_connectors_both_ways() {
    let drawn: Vec<Installed> = (0..64)
        .map(|seed| Installed::drawn(Seed::new(seed)))
        .collect();
    assert!(drawn.contains(&Installed::Binary), "{drawn:?}");
    assert!(drawn.contains(&Installed::Directory), "{drawn:?}");
}
