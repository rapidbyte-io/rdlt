use std::time::Duration;

use rdlt_connector::serve::Served;
use rdlt_connector::{Role, source_factory};
use rdlt_connector_reference::GeneratorSource;

use super::{regrants, respected};
use crate::protocol::Found;
use crate::target::Target;

#[tokio::test]
async fn a_source_whose_partition_is_long_is_certified_without_reading_it_all() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()));
    let config = serde_json::json!({
        "seed": 7,
        "streams": [{ "name": "events", "rows": 1_000_000_000_u64, "partitions": 1, "batch_rows": 100 }],
    })
    .to_string();
    let found = tokio::time::timeout(
        Duration::from_secs(20),
        respected(&target, Role::Source, &config),
    )
    .await
    .expect("the clause ends long before the partition would");
    assert!(matches!(found, Found::Kept));
}

#[test]
fn credit_is_granted_again_as_often_whatever_the_first_frame_spent_and_never_restored() {
    // A frame of one byte spends exactly the byte that bought it: only grants of nothing leave
    // its credit spent. A larger frame takes a byte each time it can.
    assert_eq!(regrants(0), [0, 0, 0]);
    assert_eq!(regrants(1), [0, 0, 0]);
    assert_eq!(regrants(2), [1, 0, 0]);
    assert_eq!(regrants(3), [1, 1, 0]);
    assert_eq!(regrants(4), [1, 1, 1]);
    assert_eq!(regrants(u64::MAX), [1, 1, 1]);
    for spent in 0..8_u64 {
        let granted: u64 = 1 + regrants(spent).iter().sum::<u64>();
        assert!(granted <= spent.max(1), "{spent}: the credit was restored");
    }
}
