//! The names an object-store log takes, the prefix it takes them beneath, what it refuses to read,
//! and its identity.

use std::io;
use std::sync::Arc;

use object_store::{ObjectStoreExt as _, PutPayload};
use rdlt_testkit::objects::faultless;

use super::{chunk, judged, keys, objects, opened, options, pipeline};
use crate::env::SystemClock;
use crate::wal::WalStore;
use crate::wal::object::ObjectStoreWal;
use crate::wal::object::keys::{Name, parse_name};
use crate::{ErrorKind, ObjectStoreOptions};

#[tokio::test]
async fn a_prefix_that_is_not_one_or_more_plain_segments_is_refused() {
    let long = "a".repeat(513);
    let refused = [
        "", "/", "/logs", "logs/", "a//b", ".", "..", "a/./b", "a/../b", "a b", "a\\b", "ü", "a:b",
        "a*", "a%2F", &long,
    ];
    for prefix in refused {
        let objects = objects(faultless());
        let opened = ObjectStoreWal::open(
            Arc::clone(&objects) as _,
            prefix,
            Arc::new(SystemClock),
            ObjectStoreOptions::default(),
        )
        .await;
        let error = opened.expect_err(prefix);
        assert_eq!(error.code(), Some("wal_prefix_invalid"), "{prefix:?}");
        assert_eq!(error.kind(), ErrorKind::Config, "{prefix:?}");
        assert!(
            objects.calls().is_empty(),
            "{prefix:?}: nothing was asked of the store"
        );
    }
    let longest = "b".repeat(512);
    for prefix in ["a", "rdlt/logs", "A-z_0.9/..x/x..", ".a/b.", &longest] {
        let objects = objects(faultless());
        ObjectStoreWal::open(
            objects,
            prefix,
            Arc::new(SystemClock),
            ObjectStoreOptions::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("{prefix:?}: {error}"));
    }
}

#[tokio::test]
async fn every_object_a_log_writes_lies_beneath_its_prefix() {
    let objects = objects(faultless());
    let wal = ObjectStoreWal::open(
        Arc::clone(&objects) as _,
        "tenant/logs",
        Arc::new(SystemClock),
        options(4),
    )
    .await
    .expect("opens");
    let orders = pipeline("A.b-c_d");
    wal.identity(chunk(9, 0).load).await.expect("names itself");
    crate::conformance::a_publish_racing_a_removal_is_never_left_behind(&wal).await;
    wal.open_log(&orders, chunk(1, 0).load)
        .await
        .expect("opens");
    let mut staged = wal.stage(&orders, chunk(1, 0)).await.expect("stages");
    staged
        .append(bytes::Bytes::from_static(b"in parts"))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
    let held = keys(&objects).await;
    assert!(!held.is_empty());
    assert!(
        held.iter().all(|key| key.starts_with("tenant/logs/")),
        "{held:?}"
    );
    assert!(held.contains(&"tenant/logs/store".to_owned()), "{held:?}");
    let log = format!("tenant/logs/p.A.b-c_d/logs/{}/", chunk(1, 0).load);
    assert!(held.iter().any(|key| key.starts_with(&log)), "{held:?}");
}

#[test]
fn a_chunk_s_names_read_back_only_as_written() {
    assert_eq!(parse_name("00000007.wal"), Some(Name::Head(7)));
    assert_eq!(parse_name("123456789.wal"), Some(Name::Head(123_456_789)));
    let token = 0xabc_u128;
    let body = format!("00000007.{token:032x}.body");
    assert_eq!(parse_name(&body), Some(Name::Body(7, token)));
    for stray in [
        "7.wal",
        "0000007.wal",
        "+0000007.wal",
        "00000007.WAL",
        "00000007.wal.tmp",
        "0000000a.wal",
        "00000007.abc.body",
        "00000007.0000000000000000000000000000ABC.body",
        "00000007.00000000000000000000000000000abc.part",
        "7.00000000000000000000000000000abc.body",
        "00000007..body",
        ".wal",
        "",
    ] {
        assert_eq!(parse_name(stray), None, "{stray:?}");
    }
}

#[tokio::test]
async fn an_object_the_log_never_writes_is_refused_as_a_stray() {
    let objects = objects(faultless());
    let wal = opened(&objects, options(1 << 20)).await;
    let orders = pipeline("strays");
    let load = chunk(1, 0).load;
    wal.open_log(&orders, load).await.expect("opens");
    let inner = objects.inner();
    let log = format!("logs/p.strays/logs/{load}");
    for (stray, listing) in [
        (format!("{log}/notes.txt"), "chunks"),
        ("logs/p.strays/open/notes".to_owned(), "loads"),
        ("logs/p.strays/logs/notes/x".to_owned(), "leftovers"),
    ] {
        let key = stray.as_str().into();
        inner
            .put(&key, PutPayload::from_static(b"x"))
            .await
            .expect("puts");
        let refused = match listing {
            "chunks" => wal.chunks(&orders, load).await.err(),
            "loads" => wal.loads(&orders).await.err(),
            _ => wal.leftovers(&orders).await.err(),
        };
        let refused = refused.unwrap_or_else(|| panic!("{stray}: read"));
        assert_eq!(
            judged(refused),
            (Some("wal_stray".to_owned()), false),
            "{stray}"
        );
        if listing == "chunks" {
            let removal = wal.remove_log(&orders, load).await.expect_err("refused");
            assert_eq!(judged(removal).0.as_deref(), Some("wal_stray"));
        }
        inner.delete(&key).await.expect("deletes");
    }
}

#[tokio::test]
async fn a_store_s_identity_is_its_first_proposal_and_unreadable_where_damaged() {
    let shared = objects(faultless());
    let first = opened(&shared, options(1 << 20)).await;
    let second = opened(&shared, options(1 << 20)).await;
    let (one, two) = (chunk(1, 0).load, chunk(2, 0).load);
    assert_eq!(first.identity(one).await.expect("names"), one);
    assert_eq!(second.identity(two).await.expect("reads"), one);
    let damaged = objects(faultless());
    let key = "logs/store".into();
    damaged
        .inner()
        .put(&key, PutPayload::from_static(b"not a load"))
        .await
        .expect("puts");
    let wal = opened(&damaged, options(1 << 20)).await;
    let refused = wal.identity(two).await.expect_err("damaged");
    assert_eq!(refused.kind(), io::ErrorKind::InvalidData);
}
