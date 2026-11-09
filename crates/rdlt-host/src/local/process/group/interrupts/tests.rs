use tokio::signal::unix::SignalKind;

use super::{among, ignored_in};

#[test]
fn only_the_ignored_mask_of_a_status_counts_and_anything_else_is_none_ignored() {
    assert_eq!(
        ignored_in("Name:\tx\nSigIgn:\t0000000000000001\nSigCgt:\t0000000180000000\n"),
        1
    );
    assert_eq!(ignored_in("SigCgt:\tffffffffffffffff\n"), 0);
    assert_eq!(ignored_in("SigIgn:\tnot hexadecimal\n"), 0);
}

#[test]
fn a_signal_is_found_in_a_mask_by_its_own_bit_alone() {
    assert!(among(SignalKind::hangup(), 0b1));
    assert!(!among(SignalKind::interrupt(), 0b1));
    assert!(among(SignalKind::interrupt(), 0b10));
    assert!(!among(SignalKind::hangup(), 0b10));
    assert!(among(SignalKind::terminate(), 1 << 14));
}
