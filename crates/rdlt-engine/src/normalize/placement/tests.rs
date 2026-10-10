use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use rdlt_connector::LogicalType;

use super::{Container, Placement, placement};

#[test]
fn a_container_within_depth_is_taken_apart_and_anything_else_is_a_column() {
    let cases = [
        (Container::Object, 1, 1, Placement::Fields),
        (Container::Array, 1, 1, Placement::Items),
        (Container::Other, 1, 1, Placement::Column),
        (Container::Object, 2, 1, Placement::Column),
        (Container::Array, 2, 1, Placement::Column),
        (Container::Object, 1, 0, Placement::Column),
        (Container::Other, 0, 0, Placement::Column),
    ];
    for (container, depth, max_depth, placed) in cases {
        assert_eq!(
            placement(container, depth, max_depth),
            placed,
            "{container:?} at {depth}"
        );
    }
}

#[test]
fn every_list_layout_and_a_map_is_an_array_and_an_encoded_container_is_neither() {
    let item = || Arc::new(Field::new("item", DataType::Int64, true));
    let object = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, true)]));
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])),
        false,
    ));
    let arrays = [
        DataType::List(item()),
        DataType::LargeList(item()),
        DataType::FixedSizeList(item(), 2),
        DataType::ListView(item()),
        DataType::LargeListView(item()),
        DataType::Map(entries, false),
    ];
    for array in arrays {
        assert_eq!(Container::of_arrow(&array), Container::Array, "{array}");
    }
    assert_eq!(Container::of_arrow(&object), Container::Object);
    let encoded = [
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(object.clone())),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::List(item()), true)),
        ),
        DataType::Int64,
    ];
    for other in encoded {
        assert_eq!(Container::of_arrow(&other), Container::Other, "{other}");
    }
    let list = LogicalType::List(Box::new(rdlt_connector::Field::new(
        "item",
        LogicalType::Int64,
        true,
    )));
    assert_eq!(Container::of_logical(&list), Container::Array);
    assert_eq!(Container::of_logical(&LogicalType::Json), Container::Other);
}
