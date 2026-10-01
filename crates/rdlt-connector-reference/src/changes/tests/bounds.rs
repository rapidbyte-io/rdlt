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
    let (_, reader) = source_factory::<ChangesSource>()
        .connect_acknowledging(config, ConnectContext::new())
        .await
        .unwrap();
    let told = reader.acknowledged(&orders(), &id("changes")).await;
    assert_eq!(told.unwrap(), None);
}

#[tokio::test]
async fn a_slot_is_kept_only_in_a_file_named_whole_and_as_a_slot_s() {
    let dir = tempfile::tempdir().unwrap();
    let inside = |name: &str| dir.path().join(name);
    let paths = [
        std::path::PathBuf::from("orders.slot"),
        inside("nested/../orders.slot"),
        inside("./orders.slot"),
        inside("orders.group"),
        inside("orders"),
        inside(".slot"),
    ];
    for path in paths {
        let config = json!({
            "seed": 3, "slot_path": path,
            "streams": [{ "name": "orders", "keys": 6, "changes": 40 }],
        });
        let refused = connect(config).await.err().expect("the path is refused");
        assert_eq!(refused.kind(), ConnectorErrorKind::Config);
        assert_eq!(refused.code(), Some("keeper_path_invalid"), "{refused}");
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
async fn a_slot_s_name_is_neither_empty_nor_a_file_keeper_s() {
    for replayable in [true, false] {
        for name in [
            "",
            "file:",
            "file:/var/lib/orders.slot",
            "file:1:2:orders.slot",
        ] {
            let config = json!({
                "seed": 3, "slot": name,
                "streams": [{ "name": "orders", "keys": 6, "changes": 40,
                              "replayable": replayable }],
            });
            let refused = connect(config).await.err().expect("the name is refused");
            assert_eq!(refused.kind(), ConnectorErrorKind::Config, "{name:?}");
            assert_eq!(refused.code(), Some("keeper_name_invalid"), "{name:?}");
        }
    }
    for name in ["f", "File:x", "files", " "] {
        let config = json!({
            "seed": 3, "slot": name,
            "streams": [{ "name": "orders", "keys": 6, "changes": 40, "replayable": false }],
        });
        connect(config)
            .await
            .unwrap_or_else(|error| panic!("{name:?}: {error}"));
    }
}
