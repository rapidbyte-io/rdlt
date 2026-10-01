//! The memory destination keeps every receipt, and answers a commit sent again from its own.

use rdlt_connector::{CommitMeta, Receipt, SegmentId};

use super::owned::{ids, meta, open, stage, store, table};

#[tokio::test]
async fn a_commit_sent_again_however_far_back_in_its_load_is_answered_and_applies_nothing() {
    const COMMITS: u64 = 200;
    let destination = store("receipts-load").await;
    let orders = table("orders", "orders", None);
    let mut session = open(destination.as_ref(), "p", 1).await;
    let mut sent: Vec<(CommitMeta, Receipt)> = Vec::new();
    for commit in 1..=COMMITS {
        let id = i64::try_from(commit).unwrap();
        stage(&mut session, &orders, commit, &[id]).await;
        let meta = meta(&session, 1, commit, &[commit]);
        let landed = session.session.commit(&meta).await;
        sent.push((meta, landed.expect("the commit lands")));
    }
    let loaded: Vec<i64> = (1..=i64::try_from(COMMITS).unwrap()).collect();
    assert_eq!(ids("receipts-load", "orders"), loaded);
    // A row staged since would be published by any commit taken for a new one.
    let unpublished = COMMITS + 1;
    stage(&mut session, &orders, unpublished, &[-1]).await;
    for (meta, receipt) in &sent {
        let again = CommitMeta {
            segments: [SegmentId(unpublished)].into_iter().collect(),
            ..meta.clone()
        };
        let answered = session.session.commit(&again).await;
        assert_eq!(&answered.expect("answered again"), receipt);
    }
    assert_eq!(ids("receipts-load", "orders"), loaded);
}

#[tokio::test]
async fn a_commit_of_a_load_many_loads_back_is_answered_and_applies_nothing() {
    const LOADS: u64 = 40;
    let destination = store("receipts-loads").await;
    let orders = table("orders", "orders", None);
    let mut sent: Vec<(CommitMeta, Receipt)> = Vec::new();
    let mut session = open(destination.as_ref(), "p", 1).await;
    for load in 1..=LOADS {
        session = open(destination.as_ref(), "p", u128::from(load)).await;
        stage(&mut session, &orders, load, &[i64::try_from(load).unwrap()]).await;
        let meta = meta(&session, u128::from(load), 1, &[load]);
        let landed = session.session.commit(&meta).await;
        sent.push((meta, landed.expect("the commit lands")));
    }
    let loaded: Vec<i64> = (1..=i64::try_from(LOADS).unwrap()).collect();
    // A row staged since would be published by any commit taken for a new one.
    let unpublished = LOADS + 1;
    stage(&mut session, &orders, unpublished, &[-1]).await;
    let epoch = session.epoch;
    for (meta, receipt) in &sent {
        let again = CommitMeta {
            epoch,
            segments: [SegmentId(unpublished)].into_iter().collect(),
            ..meta.clone()
        };
        let answered = session.session.commit(&again).await;
        assert_eq!(&answered.expect("answered again"), receipt);
    }
    assert_eq!(ids("receipts-loads", "orders"), loaded);
}
