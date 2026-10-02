use rdlt_connector::limits::MAX_BATCH_ROWS;
use rdlt_connector::{
    ConnectContext, ConnectorError, ConnectorErrorKind, Cursor, PartitionId, Source, source_factory,
};
use serde_json::{Value, json};

use super::super::{Change, ChangedStream, ChangesSource, Position, change};
use super::{orders, stream};
use crate::limits::{MAX_CHANGES, MAX_PARTITIONS, MAX_SNAPSHOT_KEYS, MAX_TRUNCATES};

async fn connect(config: Value) -> Result<Box<dyn Source>, ConnectorError> {
    source_factory::<ChangesSource>()
        .connect(config, ConnectContext::new())
        .await
}

#[tokio::test]
async fn a_stream_past_what_a_source_holds_is_refused() {
    let truncates = |count: u64| (1..=count).collect::<Vec<u64>>();
    let at_limits = json!({
        "name": "orders", "keys": MAX_SNAPSHOT_KEYS, "snapshot_partitions": MAX_PARTITIONS,
        "changes": MAX_CHANGES, "batch_rows": MAX_BATCH_ROWS, "captured": MAX_SNAPSHOT_KEYS,
        "truncates": truncates(MAX_TRUNCATES),
    });
    let config = json!({ "seed": 3, "slot": "at_limits", "streams": [at_limits] });
    connect(config).await.expect("a stream at its limits");
    let past: [(&str, Value); 6] = [
        ("keys", json!(MAX_SNAPSHOT_KEYS + 1)),
        ("snapshot_partitions", json!(MAX_PARTITIONS + 1)),
        ("changes", json!(MAX_CHANGES + 1)),
        ("batch_rows", json!(MAX_BATCH_ROWS + 1)),
        ("captured", json!(MAX_SNAPSHOT_KEYS + 1)),
        ("truncates", json!(truncates(MAX_TRUNCATES + 1))),
    ];
    for (field, value) in past {
        let mut stream = json!({ "name": "orders", "keys": 6, "changes": 40 });
        stream[field] = value;
        let config = json!({ "seed": 3, "slot": "past_limits", "streams": [stream] });
        let refused = connect(config).await.err().expect("past a limit");
        assert_eq!(refused.kind(), ConnectorErrorKind::Config, "{field}");
        assert_eq!(refused.code(), Some("limit_exceeded"), "{field}");
    }
}

#[test]
fn a_change_draws_a_key_however_many_keys_a_stream_names() {
    // Half again as many keys as the first of these is one more than a number holds.
    for keys in [0xAAAA_AAAA_AAAA_AAAA, u64::MAX, 0] {
        let stream = ChangedStream {
            keys,
            truncates: Vec::new(),
            ..stream()
        };
        for position in 1..=20 {
            assert_ne!(change(3, &stream, position), Change::Truncate, "{keys}");
        }
    }
}

#[tokio::test]
async fn a_position_of_a_partition_the_stream_never_has_is_not_acknowledged() {
    let stream = json!({
        "name": "orders", "keys": 6, "snapshot_partitions": 2, "changes": 40,
    });
    let config = json!({ "seed": 3, "slot": "members", "streams": [stream] });
    let source = connect(config.clone()).await.unwrap();
    let at = || {
        Cursor::encode(
            1,
            &Position {
                next: 3,
                done: false,
            },
        )
        .unwrap()
    };
    let id = |id: &str| PartitionId::parse(id).unwrap();
    source
        .committed(&orders(), &[(id("snapshot-1"), at())])
        .await
        .expect("a partition of the snapshot");
    for partition in ["snapshot-2", "snapshot-01", "snapshot-", "p0", "change"] {
        let refused = source
            .committed(&orders(), &[(id("changes"), at()), (id(partition), at())])
            .await
            .expect_err("no such partition");
        assert_eq!(refused.kind(), ConnectorErrorKind::Data, "{partition}");
    }
    // An acknowledgement naming a partition the stream never has kept none of its positions.
    let (_, reader) = rdlt_connector::acknowledging_source_factory::<ChangesSource>()
        .connect_acknowledging(config, ConnectContext::new())
        .await
        .unwrap();
    let told = reader.acknowledged(&orders(), &id("changes")).await;
    assert_eq!(told.unwrap(), None);
}

#[tokio::test]
async fn a_slot_is_kept_only_in_a_file_named_as_a_slot_s() {
    let dir = crate::scratch::tempdir().unwrap();
    let inside = |name: &str| dir.path().join(name);
    let kept_at = |path: std::path::PathBuf| {
        json!({
            "seed": 3, "slot_path": path,
            "streams": [{ "name": "orders", "keys": 6, "changes": 40 }],
        })
    };
    for path in [inside("orders.group"), inside("orders"), inside(".slot")] {
        let refused = connect(kept_at(path)).await.err();
        let refused = refused.expect("the path is refused");
        assert_eq!(refused.kind(), ConnectorErrorKind::Config);
        assert_eq!(refused.code(), Some("keeper_path_invalid"), "{refused}");
    }
    // However the path is written, the file it leads to is the slot's.
    for path in [inside("orders.slot"), inside("./orders.slot")] {
        connect(kept_at(path)).await.expect("the slot's file");
    }
}

#[tokio::test]
async fn a_source_that_forgets_names_the_slot_it_keeps_its_positions_in() {
    let forgets = json!({ "name": "orders", "keys": 6, "changes": 40, "replayable": false });
    let serves_again = json!({ "name": "other", "keys": 6, "changes": 40 });
    let unnamed = json!({ "seed": 3, "streams": [serves_again, forgets] });
    let refused = connect(unnamed.clone()).await.err().expect("no slot");
    assert_eq!(refused.kind(), ConnectorErrorKind::Config);
    assert_eq!(refused.code(), Some("keeper_unnamed"));
    let dir = tempfile::tempdir().unwrap();
    let named = [
        ("slot", json!("named")),
        ("slot_path", json!(dir.path().join("orders.slot"))),
    ];
    for (field, value) in named {
        let mut config = unnamed.clone();
        config[field] = value;
        let connected = connect(config).await;
        connected.unwrap_or_else(|error| panic!("{field} names the slot: {error}"));
    }
    // A source that serves its changes again may share the default slot.
    let config = json!({ "seed": 3, "streams": [serves_again] });
    connect(config).await.expect("the default slot");
}

#[tokio::test]
async fn a_slot_s_name_is_any_but_the_empty_one() {
    let named = |name: &str, replayable: bool| {
        json!({
            "seed": 3, "slot": name,
            "streams": [{ "name": "orders", "keys": 6, "changes": 40,
                          "replayable": replayable }],
        })
    };
    for replayable in [true, false] {
        let refused = connect(named("", replayable)).await.err();
        let refused = refused.expect("the empty name is refused");
        assert_eq!(refused.kind(), ConnectorErrorKind::Config);
        assert_eq!(refused.code(), Some("keeper_name_invalid"));
    }
    // A name is a slot's whatever it looks like: a slot kept in a file is known by its file.
    for name in ["f", "file:", "file:/var/lib/orders.slot", " "] {
        connect(named(name, false))
            .await
            .unwrap_or_else(|error| panic!("{name:?}: {error}"));
    }
}

#[tokio::test]
async fn two_hosts_that_name_one_slot_share_nothing_and_a_slot_goes_with_its_last_source() {
    use rdlt_connector::acknowledging_source_factory;
    let dir = crate::scratch::tempdir().unwrap();
    let stream = json!({ "name": "orders", "keys": 6, "changes": 40, "replayable": false });
    let named = json!({ "seed": 3, "slot": "of_two_hosts", "streams": [stream.clone()] });
    let filed = json!({
        "seed": 3, "slot_path": dir.path().join("orders.slot"), "streams": [stream],
    });
    let serving = |host: Option<&'static str>, config: &Value| {
        let context = host.map_or_else(ConnectContext::new, ConnectContext::serving);
        let factory = acknowledging_source_factory::<ChangesSource>();
        let config = config.clone();
        async move { factory.connect_acknowledging(config, context).await }
    };
    let changes = PartitionId::parse("changes").unwrap();
    let position = Position {
        next: 3,
        done: false,
    };
    let at = || Cursor::encode(1, &position).unwrap();
    let (ours, told) = serving(Some("a.example"), &named).await.unwrap();
    let (_theirs, told_them) = serving(Some("b.example"), &named).await.unwrap();
    let (_own, told_own) = serving(None, &named).await.unwrap();
    ours.committed(&orders(), &[(changes.clone(), at())])
        .await
        .unwrap();
    assert!(
        told.acknowledged(&orders(), &changes)
            .await
            .unwrap()
            .is_some()
    );
    for other in [&told_them, &told_own] {
        assert_eq!(other.acknowledged(&orders(), &changes).await.unwrap(), None);
    }
    // The slot goes with the last source of its host that holds it.
    drop((ours, told));
    let (_anew, told) = serving(Some("a.example"), &named).await.unwrap();
    assert_eq!(told.acknowledged(&orders(), &changes).await.unwrap(), None);
    // A slot's file is one host's while it is held, and the next host's with what it holds.
    let (ours, told) = serving(Some("a.example"), &filed).await.unwrap();
    ours.committed(&orders(), &[(changes.clone(), at())])
        .await
        .unwrap();
    let kept = told.acknowledged(&orders(), &changes).await.unwrap();
    for host in [Some("b.example"), None] {
        let Err(refused) = serving(host, &filed).await else {
            panic!("{host:?} shares the file");
        };
        assert_eq!(refused.kind(), ConnectorErrorKind::Config, "{host:?}");
    }
    drop((ours, told));
    let (_theirs, told) = serving(Some("b.example"), &filed).await.unwrap();
    assert!(kept.is_some());
    assert_eq!(told.acknowledged(&orders(), &changes).await.unwrap(), kept);
}
