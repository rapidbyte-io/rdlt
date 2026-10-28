//! The memory destination keeps the key of the last commit that landed: it answers that commit
//! sent again with its receipt, and refuses as stale a commit expecting another key landed.

use rdlt_connector::{COMMIT_STALE, CommitMeta, ConnectorErrorKind, Receipt, SegmentId};

use super::owned::{ids, meta, open, refusal, stage, store, table};

#[tokio::test]
async fn the_landed_commit_sent_again_is_answered_and_an_overtaken_one_refused() {
    const COMMITS: u64 = 5;
    let destination = store("landed-load").await;
    let orders = table("orders", "orders", None);
    let mut session = open(destination.as_ref(), "p").await;
    let mut sent: Vec<(CommitMeta, Receipt)> = Vec::new();
    for commit in 1..=COMMITS {
        let id = i64::try_from(commit).unwrap();
        stage(&mut session, &orders, commit, &[id]).await;
        let meta = meta(&session, commit, &[commit]);
        let landed = session.session.commit(&meta).await;
        sent.push((meta, landed.expect("the commit lands")));
    }
    let loaded: Vec<i64> = (1..=i64::try_from(COMMITS).unwrap()).collect();
    assert_eq!(ids("landed-load", "orders"), loaded);
    // A row staged since would be published by any commit taken for a new one.
    let unpublished = COMMITS + 1;
    stage(&mut session, &orders, unpublished, &[-1]).await;
    let again = |meta: &CommitMeta| CommitMeta {
        segments: [SegmentId(unpublished)].into_iter().collect(),
        ..meta.clone()
    };
    let (last, receipt) = sent.last().unwrap();
    let answered = session.session.commit(&again(last)).await;
    assert_eq!(&answered.expect("answered again"), receipt);
    for (meta, _) in &sent[..sent.len() - 1] {
        let overtaken = session.session.commit(&again(meta)).await;
        assert_eq!(
            refusal(overtaken),
            (ConnectorErrorKind::Fenced, Some(COMMIT_STALE.to_owned())),
            "{:?}",
            meta.key
        );
    }
    assert_eq!(ids("landed-load", "orders"), loaded);
}

#[tokio::test]
async fn the_landed_commit_is_answered_from_a_newer_session_which_finds_it_landed() {
    let destination = store("landed-sessions").await;
    let orders = table("orders", "orders", None);
    let mut first = open(destination.as_ref(), "p").await;
    stage(&mut first, &orders, 1, &[1]).await;
    let sent = meta(&first, 1, &[1]);
    let receipt = first.session.commit(&sent).await.expect("the commit lands");
    let mut newer = open(destination.as_ref(), "p").await;
    assert_eq!(newer.landed.as_ref(), Some(&receipt));
    stage(&mut newer, &orders, 2, &[-1]).await;
    let again = CommitMeta {
        segments: [SegmentId(2)].into_iter().collect(),
        ..sent
    };
    let answered = newer.session.commit(&again).await;
    assert_eq!(answered.expect("answered from the newer session"), receipt);
    let unexpected = CommitMeta {
        after: None,
        ..meta(&newer, 1, &[2])
    };
    assert_eq!(
        refusal(newer.session.commit(&unexpected).await),
        (ConnectorErrorKind::Fenced, Some(COMMIT_STALE.to_owned()))
    );
    newer
        .session
        .commit(&meta(&newer, 1, &[2]))
        .await
        .expect("the commit expecting the landed key lands");
    assert_eq!(ids("landed-sessions", "orders"), [-1, 1]);
}

#[tokio::test]
async fn a_commit_naming_every_segment_publishes_what_was_staged_without_walking_each_id() {
    let destination = store("landed-wide").await;
    let orders = table("orders", "orders", None);
    let mut session = open(destination.as_ref(), "p").await;
    stage(&mut session, &orders, 7, &[1, 2]).await;
    let every = rdlt_connector::SegmentSet::try_from(vec![rdlt_connector::SegmentRange {
        first: SegmentId(0),
        last: SegmentId(u64::MAX),
    }])
    .expect("one range");
    let wide = CommitMeta {
        segments: every,
        ..meta(&session, 1, &[])
    };
    // A walk of every id the range names would not end within the test's life.
    let committed = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        session.session.commit(&wide),
    )
    .await
    .expect("the commit ends");
    assert_eq!(committed.expect("the commit lands").rows, 2);
    assert_eq!(ids("landed-wide", "orders"), [1, 2]);
}
