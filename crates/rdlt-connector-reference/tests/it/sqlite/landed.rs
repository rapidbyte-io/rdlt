//! The SQLite destination keeps the key of the last commit that landed in its pipeline's fence
//! row: it answers that commit sent again with its receipt, and refuses as stale a commit
//! expecting another key landed.

use rdlt_connector::{COMMIT_STALE, CommitMeta, ConnectorErrorKind, Receipt, SegmentId};

use super::kit::{Shared, refusal, table};

/// The landed key the fence row of `pipeline` holds, as its epoch and place.
fn landed(shared: &Shared, pipeline: &str) -> Vec<String> {
    shared.texts(&format!(
        "SELECT landed_epoch || '.' || landed_seq FROM _rdlt_epochs WHERE pipeline = '{pipeline}'"
    ))
}

/// A refusal of a commit expecting another key landed.
fn stale() -> (ConnectorErrorKind, Option<String>) {
    (ConnectorErrorKind::Fenced, Some(COMMIT_STALE.to_owned()))
}

#[tokio::test]
async fn the_landed_commit_sent_again_is_answered_and_an_overtaken_one_refused() {
    const COMMITS: u64 = 5;
    let shared = Shared::new().await;
    let orders = table("orders", "orders", false);
    let mut other = shared.open("q").await;
    other.load(&table("others", "others", false), 1, &[1]).await;
    let mut session = shared.open("p").await;
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
    let (last, receipt) = sent.last().unwrap();
    let key = format!("{}.{}", last.key.epoch, last.key.seq);
    assert_eq!(landed(&shared, "p"), std::slice::from_ref(&key));
    assert_eq!(
        landed(&shared, "q").len(),
        1,
        "another pipeline keeps its own"
    );
    // A row staged since would be published by any commit taken for a new one.
    let unpublished = COMMITS + 1;
    session.stage(&orders, unpublished, &[-1]).await;
    let again = |meta: &CommitMeta| CommitMeta {
        segments: [SegmentId(unpublished)].into_iter().collect(),
        ..meta.clone()
    };
    let answered = session.commit(&again(last)).await;
    assert_eq!(&answered.expect("answered again"), receipt);
    for (meta, _) in &sent[..sent.len() - 1] {
        let overtaken = session.commit(&again(meta)).await;
        assert_eq!(refusal(overtaken), stale(), "{:?}", meta.key);
    }
    assert_eq!(shared.ids("orders"), loaded);
    assert_eq!(landed(&shared, "p"), [key]);
}

#[tokio::test]
async fn the_landed_commit_is_answered_from_a_newer_session_which_finds_it_landed() {
    let shared = Shared::new().await;
    let orders = table("orders", "orders", false);
    let mut first = shared.open("p").await;
    first.stage(&orders, 1, &[1]).await;
    let sent = first.meta(&[1]);
    let receipt = first.commit(&sent).await.expect("the commit lands");
    let mut newer = shared.open("p").await;
    assert_eq!(newer.session.landed.as_ref(), Some(&receipt));
    newer.stage(&orders, 2, &[-1]).await;
    let again = CommitMeta {
        segments: [SegmentId(2)].into_iter().collect(),
        ..sent
    };
    let answered = newer.commit(&again).await;
    assert_eq!(answered.expect("answered from the newer session"), receipt);
    let expecting = newer.meta(&[2]);
    let unexpected = CommitMeta {
        after: None,
        ..expecting.clone()
    };
    assert_eq!(refusal(newer.commit(&unexpected).await), stale());
    newer
        .commit(&expecting)
        .await
        .expect("the commit expecting the landed key lands");
    assert_eq!(shared.ids("orders"), [-1, 1]);
}
