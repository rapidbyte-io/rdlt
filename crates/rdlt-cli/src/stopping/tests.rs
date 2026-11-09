use rdlt_host::Lingering;

use super::both;

fn lingering(groups: &[u32]) -> Result<(), Lingering> {
    Err(Lingering {
        groups: groups.to_vec(),
    })
}

#[test]
fn two_stops_of_the_same_groups_name_each_lingering_group_once() {
    assert_eq!(both(Ok(()), Ok(())), Ok(()));
    assert_eq!(both(lingering(&[3]), Ok(())), lingering(&[3]));
    assert_eq!(both(Ok(()), lingering(&[3])), lingering(&[3]));
    assert_eq!(
        both(lingering(&[5, 3]), lingering(&[3, 4])),
        lingering(&[3, 4, 5])
    );
}
