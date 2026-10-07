use std::num::NonZeroUsize;

use super::pool_cores;

fn counts(host: usize) -> Vec<usize> {
    pool_cores(NonZeroUsize::new(host).unwrap())
        .into_iter()
        .map(NonZeroUsize::get)
        .collect()
}

#[test]
fn pools_double_from_one_core_to_every_core_the_host_gives() {
    assert_eq!(counts(1), [1]);
    assert_eq!(counts(2), [1, 2]);
    assert_eq!(counts(4), [1, 2, 4]);
    assert_eq!(counts(6), [1, 2, 4, 6]);
    assert_eq!(counts(16), [1, 2, 4, 8, 16]);
}
