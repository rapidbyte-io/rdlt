//! Proofs that a budget's shares add up to it, for every capacity.

use super::Shares;

/// A budget's shares add up to it, its data is what pushes and one request for lowering take,
/// and a piece is within a request.
///
/// Each share's bytes beside one request of its class add up without wrapping, so checking
/// that a request fits never overflows.
///
/// Bound: none; every capacity.
#[kani::proof]
#[kani::unwind(9)]
fn a_budget_s_shares_add_up_to_it() {
    let capacity: u64 = kani::any();
    let shares = Shares::of(capacity);
    let parts = u128::from(shares.cursors)
        + u128::from(shares.log)
        + u128::from(shares.tables)
        + u128::from(shares.reads)
        + u128::from(shares.control)
        + u128::from(shares.data);
    assert_eq!(parts, u128::from(capacity));
    assert_eq!(
        u128::from(shares.intake) + u128::from(shares.request),
        u128::from(shares.data)
    );
    assert!(shares.piece <= shares.request);
    let sums = [
        (shares.intake, shares.intake),
        (shares.data, shares.intake),
        (shares.data, shares.request),
        (shares.cursors, shares.cursors),
        (shares.log, shares.log),
        (shares.tables, shares.tables),
        (shares.reads, shares.reads),
        (shares.control, shares.control),
    ];
    for (share, asked) in sums {
        assert!(share.checked_add(asked).is_some());
    }
}
