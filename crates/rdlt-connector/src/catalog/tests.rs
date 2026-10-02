use super::{Catalog, Checkpointing, InvalidCatalog, Partitioning, ReadMode, StreamSpec};
use crate::id::StreamName;
use crate::limits::MAX_CATALOG_STREAMS;
use crate::schema::{ColumnPath, TableSchema};
use crate::types::{Field, LogicalType};

fn name(text: &str) -> StreamName {
    StreamName::new(text).unwrap()
}

#[test]
fn a_new_stream_has_the_documented_defaults() {
    let spec = StreamSpec::new(name("orders"));
    assert!(spec.supports(ReadMode::Full));
    assert!(!spec.supports(ReadMode::Cdc));
    assert_eq!(spec.partitioning(), Partitioning::Single);
    assert_eq!(spec.checkpointing(), Checkpointing::Natural);
    assert!(spec.is_replayable());
    assert!(
        spec.schema().is_none() && spec.primary_key().is_none() && spec.change_time().is_none()
    );
}

#[test]
fn builders_set_every_property() {
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)]).unwrap();
    let spec = StreamSpec::new(name("orders"))
        .with_schema(schema.clone())
        .with_primary_key(["id"])
        .with_cursor_field("updated_at")
        .with_read_modes([ReadMode::Full, ReadMode::Cdc])
        .with_partitioning(Partitioning::Planned)
        .with_checkpointing(Checkpointing::OnDemand)
        .with_replayable(false)
        .with_change_time("committed_at");
    assert_eq!(spec.schema(), Some(&schema));
    assert_eq!(spec.primary_key(), Some(&[ColumnPath::from("id")][..]));
    assert_eq!(spec.cursor_fields(), &[ColumnPath::from("updated_at")]);
    assert!(spec.supports(ReadMode::Cdc) && !spec.supports(ReadMode::Incremental));
    assert_eq!(spec.partitioning(), Partitioning::Planned);
    assert_eq!(spec.checkpointing(), Checkpointing::OnDemand);
    assert!(!spec.is_replayable());
    assert_eq!(spec.change_time(), Some(&ColumnPath::from("committed_at")));
}

#[test]
fn catalogs_refuse_duplicate_stream_names() {
    let streams = vec![StreamSpec::new(name("a")), StreamSpec::new(name("a"))];
    assert_eq!(
        Catalog::new(streams),
        Err(InvalidCatalog::Duplicate(name("a")))
    );
}

#[test]
fn catalogs_find_streams_and_round_trip_through_json() {
    let catalog =
        Catalog::new(vec![StreamSpec::new(name("a")), StreamSpec::new(name("b"))]).unwrap();
    assert_eq!(catalog.len(), 2);
    assert!(!catalog.is_empty());
    assert!(Catalog::default().is_empty());
    assert_eq!(catalog.get(&name("b")).unwrap().name(), &name("b"));
    assert!(catalog.get(&name("c")).is_none());
    let json = serde_json::to_string(&catalog).unwrap();
    assert_eq!(serde_json::from_str::<Catalog>(&json).unwrap(), catalog);
}

#[test]
fn a_catalog_beyond_its_stream_limit_is_refused() {
    let streams = |count: usize| {
        (0..count)
            .map(|index| StreamSpec::new(name(&format!("s{index}"))))
            .collect::<Vec<_>>()
    };
    let full = Catalog::new(streams(MAX_CATALOG_STREAMS)).unwrap();
    assert_eq!(full.len(), MAX_CATALOG_STREAMS);
    assert_eq!(
        Catalog::new(streams(MAX_CATALOG_STREAMS + 1)),
        Err(InvalidCatalog::TooMany {
            count: MAX_CATALOG_STREAMS + 1,
            limit: MAX_CATALOG_STREAMS
        })
    );
}

#[test]
fn a_catalog_at_its_stream_limit_is_checked_and_searched_in_linear_time() {
    // A check comparing each stream with every earlier one takes minutes at the limit; a set
    // takes milliseconds.
    let streams: Vec<StreamSpec> = (0..MAX_CATALOG_STREAMS)
        .map(|index| StreamSpec::new(name(&format!("s{index:x}"))))
        .collect();
    let started = std::time::Instant::now();
    let catalog = Catalog::new(streams).unwrap();
    for index in 0..MAX_CATALOG_STREAMS {
        assert!(catalog.get(&name(&format!("s{index:x}"))).is_some());
    }
    let elapsed = started.elapsed();
    assert!(elapsed < std::time::Duration::from_secs(10), "{elapsed:?}");
}
