use super::Allowance;

#[test]
fn what_is_left_is_spent_to_nothing_and_never_below() {
    let allowance = Allowance::new(10, usize::MAX);
    assert!(allowance.spend(0, 0));
    assert!(allowance.spend(4, 0));
    assert!(allowance.spend(6, 0));
    assert!(allowance.spend(0, 0));
    assert!(!allowance.spend(1, 0));
    let allowance = Allowance::new(10, usize::MAX);
    assert!(!allowance.spend(11, 0));
    // What was asked beyond what was left leaves nothing for what follows.
    assert!(!allowance.spend(1, 0));
    assert!(!allowance.spend(usize::MAX, 0));
}

#[test]
fn bytes_and_rows_are_both_spent_whichever_runs_out() {
    let allowance = Allowance::new(10, 3);
    assert!(allowance.spend(4, 1));
    assert_eq!(allowance.left(), (6, 2));
    assert!(!allowance.spend(11, 1));
    assert_eq!(allowance.left(), (0, 1));
    assert!(!allowance.spend(0, 2));
    assert_eq!(allowance.left(), (0, 0));
}
