//! The names an object-store log takes, the prefix it takes them beneath, what it refuses to read,
//! and its identity.

use std::io;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use object_store::{ObjectStoreExt as _, PutPayload};
use proptest::prelude::*;
use rdlt_connector::LoadId;
use rdlt_testkit::objects::{Fault, Op, faultless};

use super::{always, chunk, judged, keys, objects, opened, options, pipeline};
use crate::env::SystemClock;
use crate::wal::object::ObjectStoreWal;
use crate::wal::object::keys::{Keys, Name, name_in, parse_load, parse_name};
use crate::wal::{Chunk, WalStore};
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
        "00000007.gone",
        "0000000a.wal",
        "00000007.abc.body",
        "00000007.0000000000000000000000000000ABC.body",
        "00000007.00000000000000000000000000000abc.part",
        "7.00000000000000000000000000000abc.body",
        "00000007..body",
        "+0000007.00000000000000000000000000000abc.body",
        "00000007.+0000000000000000000000000000abc.body",
        " 0000007.wal",
        "-0000007.wal",
        ".wal",
        ".body",
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

#[tokio::test]
async fn stores_naming_themselves_at_once_both_take_the_first_create_s_identity() {
    let shared = objects(faultless());
    let first = opened(&shared, options(1 << 20)).await;
    let second = opened(&shared, options(1 << 20)).await;
    // Both find no identity, and both create one: the second is answered taken, and reads it.
    shared.plan(always(Fault::Slow(4), |call| {
        call.op == (Op::Put { create: true })
    }));
    let (one, two) = (chunk(1, 0).load, chunk(2, 0).load);
    let (named, read) = tokio::join!(first.identity(one), second.identity(two));
    assert_eq!(named.expect("names"), one);
    assert_eq!(read.expect("reads"), one);
}

#[tokio::test(start_paused = true)]
async fn a_store_refused_a_look_at_its_identity_or_its_making_reports_the_refusal() {
    let proposed = chunk(1, 0).load;
    for refused in [Op::Get, Op::Put { create: true }] {
        let objects = objects(faultless());
        let wal = opened(&objects, options(1 << 20)).await;
        objects.plan(Box::new(move |call| {
            if call.op == refused {
                Fault::Denied
            } else {
                Fault::None
            }
        }));
        let error = wal.identity(proposed).await.expect_err("refused");
        assert_eq!(
            judged(error),
            (Some("wal_storage_denied".to_owned()), false),
            "{refused:?}"
        );
    }
}

/// Prefixes a log may be given: plain segments, text that may hold `.`, `..`, empty segments,
/// spaces and other characters, and the longest and one byte longer.
fn prefixes() -> impl Strategy<Value = String> {
    prop_oneof![
        "[A-Za-z0-9._-]{1,8}(/[A-Za-z0-9._-]{1,8}){0,3}",
        "[A-Za-z0-9._/ :ü-]{0,24}",
        proptest::sample::select(vec!["..", ".", "a/..", "../a", "a/./b", "a//b", "/a", "a/"])
            .prop_map(str::to_owned),
        Just("a".repeat(512)),
        Just("a".repeat(513)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// A prefix is taken exactly when it is one or more segments of `[A-Za-z0-9._-]`, none of
    /// them empty, `.` or `..`, within 512 bytes; and every key a log writes beneath it lies
    /// beneath it, holds no empty, `.` or `..` segment, and reads back as what it names.
    #[test]
    fn a_prefix_is_taken_exactly_when_it_is_plain_segments_and_keys_beneath_it_read_back(
        prefix in prefixes(),
        id in "[A-Za-z0-9._-]{1,16}",
        number in any::<u64>(),
        token in any::<u128>(),
        random in any::<u128>(),
    ) {
        let segment = |segment: &str| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        };
        let plain = !prefix.is_empty() && prefix.len() <= 512 && prefix.split('/').all(segment);
        match Keys::parse(&prefix) {
            Ok(keys) => {
                prop_assert!(plain, "{:?} was taken", prefix);
                prop_assert_eq!(keys.identity().to_string(), format!("{prefix}/store"));
                let pipeline = pipeline(&id);
                let chunk = Chunk { load: LoadId::from_parts(UNIX_EPOCH, random), number };
                let written = [
                    keys.identity(),
                    keys.probe(token),
                    keys.mark(&pipeline, chunk.load),
                    keys.head(&pipeline, chunk),
                    keys.body(&pipeline, chunk, token),
                ];
                for key in &written {
                    prop_assert!(key.as_ref().starts_with(&format!("{prefix}/")), "{}", key);
                    // A part that was `.`, `..` or empty would show as `%2E` or be dropped.
                    prop_assert!(!key.as_ref().contains('%'), "{}", key);
                    for part in key.as_ref().split('/') {
                        prop_assert!(!part.is_empty() && part != "." && part != "..", "{}", key);
                    }
                }
                let log = keys.log(&pipeline, chunk.load);
                let head = name_in(&log, &keys.head(&pipeline, chunk)).and_then(parse_name);
                prop_assert_eq!(head, Some(Name::Head(number)));
                let body = name_in(&log, &keys.body(&pipeline, chunk, token)).and_then(parse_name);
                prop_assert_eq!(body, Some(Name::Body(number, token)));
                let marks = keys.marks(&pipeline);
                let mark = name_in(&marks, &keys.mark(&pipeline, chunk.load)).and_then(parse_load);
                prop_assert_eq!(mark, Some(chunk.load));
            }
            Err(error) => {
                prop_assert!(!plain, "{:?} was refused", prefix);
                prop_assert_eq!(error.code(), Some("wal_prefix_invalid"));
            }
        }
    }
}
