//! The SQLite destination keeps every receipt no horizon has passed, and answers a commit sent
//! again from its own.

use rdlt_connector::{CommitMeta, Horizon, Receipt, SegmentId};

use super::kit::{Shared, table};

/// The receipts the database keeps for `pipeline`.
fn kept(shared: &Shared, pipeline: &str) -> i64 {
    shared.count(&format!(
        "SELECT count(*) FROM _rdlt_receipts WHERE pipeline = '{pipeline}'"
    ))
}

#[tokio::test]
async fn a_commit_sent_again_however_far_back_in_its_load_is_answered_and_applies_nothing() {
    const COMMITS: u64 = 200;
    let shared = Shared::new().await;
    let orders = table("orders", "orders", false);
    let mut session = shared.open("p", 1).await;
    let mut sent: Vec<(CommitMeta, Receipt)> = Vec::new();
    for commit in 1..=COMMITS {
        session
            .stage(&orders, commit, &[i64::try_from(commit).unwrap()])
            .await;
        let meta = session.meta(&[commit]);
        let receipt = session.commit(&meta).await.expect("the commit lands");
        assert_eq!(receipt.rows, 1);
        sent.push((meta, receipt));
    }
    let loaded: Vec<i64> = (1..=i64::try_from(COMMITS).unwrap()).collect();
    assert_eq!(shared.ids("orders"), loaded);
    assert_eq!(kept(&shared, "p"), i64::try_from(COMMITS).unwrap());
    // A row staged since would be published by any commit taken for a new one.
    let unpublished = COMMITS + 1;
    session.stage(&orders, unpublished, &[-1]).await;
    for (meta, receipt) in &sent {
        let again = CommitMeta {
            segments: [SegmentId(unpublished)].into_iter().collect(),
            ..meta.clone()
        };
        let answered = session.commit(&again).await.expect("answered again");
        assert_eq!(&answered, receipt);
    }
    assert_eq!(shared.ids("orders"), loaded);
    assert_eq!(kept(&shared, "p"), i64::try_from(COMMITS).unwrap());
}

#[tokio::test]
async fn a_commit_of_a_load_many_loads_back_is_answered_and_applies_nothing() {
    const LOADS: u64 = 40;
    let shared = Shared::new().await;
    let orders = table("orders", "orders", false);
    let mut other = shared.open("q", 100).await;
    other.load(&table("others", "others", false), 1, &[1]).await;
    let mut sent: Vec<(CommitMeta, Receipt)> = Vec::new();
    let mut session = shared.open("p", 1).await;
    for load in 1..=LOADS {
        session = shared.open("p", u128::from(load)).await;
        session
            .stage(&orders, load, &[i64::try_from(load).unwrap()])
            .await;
        let meta = session.meta(&[load]);
        let receipt = session.commit(&meta).await.expect("the commit lands");
        sent.push((meta, receipt));
    }
    let loaded: Vec<i64> = (1..=i64::try_from(LOADS).unwrap()).collect();
    assert_eq!(kept(&shared, "p"), i64::try_from(LOADS).unwrap());
    assert_eq!(kept(&shared, "q"), 1);
    // The latest session answers each load's commit again, as a replay of its log asks; a row
    // staged since would be published by any of them taken for a new one.
    let unpublished = LOADS + 1;
    session.stage(&orders, unpublished, &[-1]).await;
    let epoch = session.session.epoch;
    for (meta, receipt) in &sent {
        let again = CommitMeta {
            epoch,
            segments: [SegmentId(unpublished)].into_iter().collect(),
            ..meta.clone()
        };
        let answered = session.commit(&again).await.expect("answered again");
        assert_eq!(&answered, receipt);
    }
    assert_eq!(shared.ids("orders"), loaded);
    assert_eq!(kept(&shared, "p"), i64::try_from(LOADS).unwrap());
}

#[tokio::test]
async fn the_receipts_before_a_commit_s_horizon_are_forgotten_and_the_rest_answered() {
    let shared = Shared::new().await;
    let orders = table("orders", "orders", false);
    let mut other = shared.open("q", 1).await;
    other.load(&table("others", "others", false), 1, &[1]).await;
    let mut session = shared.open("p", 1).await;
    let mut sent: Vec<(CommitMeta, Receipt)> = Vec::new();
    for commit in 1..=5_u64 {
        session
            .stage(&orders, commit, &[i64::try_from(commit).unwrap()])
            .await;
        let mut meta = session.meta(&[commit]);
        // The fifth commit says the engine may repeat no commit before the third.
        meta.horizon = (commit == 5).then(|| Horizon {
            load_id: sent[2].0.load_id,
            commit_seq: sent[2].0.commit_seq,
        });
        let receipt = session.commit(&meta).await.expect("the commit lands");
        sent.push((meta, receipt));
    }
    assert_eq!(kept(&shared, "p"), 3);
    assert_eq!(kept(&shared, "q"), 1, "another pipeline's receipts stay");
    let unpublished = 6;
    session.stage(&orders, unpublished, &[-1]).await;
    for (meta, receipt) in &sent[2..] {
        let again = CommitMeta {
            segments: [SegmentId(unpublished)].into_iter().collect(),
            ..meta.clone()
        };
        let answered = session.commit(&again).await.expect("answered again");
        assert_eq!(&answered, receipt);
    }
    assert_eq!(shared.ids("orders"), [1, 2, 3, 4, 5]);
}
