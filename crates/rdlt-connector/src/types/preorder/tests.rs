use std::sync::Arc;

use proptest::prelude::*;
use rdlt_wire::limits::NESTING_DEPTH;

use super::{Node, Shape, Unread, nodes, whole};
use crate::strategy::logical_type;
use crate::types::{Field, Fields, LogicalType};

/// The node a test reads `logical`, one node of a preorder, as.
fn shape(logical: &LogicalType) -> Shape {
    match logical {
        LogicalType::Struct(fields) => Shape::Struct(fields.len()),
        LogicalType::List(_) => Shape::List,
        leaf => Shape::Leaf(leaf.clone()),
    }
}

/// `logical`'s nodes as a test holds them, its root named as `root` says.
fn listed(root: Option<(&Arc<str>, bool)>, logical: &LogicalType) -> Vec<Node> {
    nodes(root, logical)
        .map(|(named, logical)| {
            (
                named.map(|(name, nullable)| (Arc::clone(name), nullable)),
                shape(logical),
            )
        })
        .take(1 << 16)
        .collect()
}

/// A list `levels` deep, counting its own level, its innermost item an integer.
fn lists(levels: u64) -> LogicalType {
    (1..levels).fold(LogicalType::Int32, |item, _| {
        LogicalType::List(Box::new(Field::new("item", item, true)))
    })
}

proptest! {
    #[test]
    fn a_type_reads_back_from_its_nodes(logical in logical_type(), nullable in any::<bool>()) {
        let read = whole(listed(None, &logical), Ok::<_, ()>);
        prop_assert!(matches!(&read, Ok((None, back)) if *back == logical));
        let name: Arc<str> = Arc::from("c");
        let read = whole(listed(Some((&name, nullable)), &logical), Ok::<_, ()>);
        prop_assert!(matches!(&read, Ok((Some((back, null)), back_type))
            if back.as_ref() == "c" && *null == nullable && *back_type == logical));
    }
}

#[test]
fn a_type_nested_to_the_limit_reads_back_and_one_deeper_however_deep_is_refused() {
    let at = lists(NESTING_DEPTH);
    assert!(matches!(whole(listed(None, &at), Ok::<_, ()>), Ok((None, back)) if back == at));
    for levels in [NESTING_DEPTH + 1, 3_000] {
        let deep = lists(levels);
        assert!(
            matches!(whole(listed(None, &deep), Ok::<_, ()>), Err(Unread::Deep)),
            "{levels}"
        );
    }
}

#[test]
fn nodes_that_are_no_one_type_are_refused() {
    let int = || (Some((Arc::from("a"), true)), Shape::Leaf(LogicalType::Int8));
    let cases: Vec<(Vec<Node>, &str)> = vec![
        (Vec::new(), "ended"),
        (vec![(None, Shape::Struct(2)), int()], "ended"),
        (vec![(None, Shape::Struct(usize::MAX)), int()], "ended"),
        (vec![(None, Shape::List)], "ended"),
        (
            vec![(None, Shape::Leaf(LogicalType::Int8)), int()],
            "beyond",
        ),
        (
            vec![(None, Shape::List), (None, Shape::Leaf(LogicalType::Int8))],
            "unnamed",
        ),
        (vec![(None, Shape::Struct(2)), int(), int()], "fields"),
    ];
    for (listed, why) in cases {
        let read = whole(listed, Ok::<_, ()>);
        let found = match read {
            Err(Unread::Ended) => "ended",
            Err(Unread::Beyond) => "beyond",
            Err(Unread::Unnamed) => "unnamed",
            Err(Unread::Fields(_)) => "fields",
            Err(Unread::Deep | Unread::Node(())) => "deep or node",
            Ok(_) => "read",
        };
        assert_eq!(found, why);
    }
    // A node its reader refuses stops the read with the reader's error.
    let refused = whole(vec![0_u8], |_| Err::<Node, _>("refused"));
    assert!(matches!(refused, Err(Unread::Node("refused"))));
    let empty = Fields::new(Vec::new()).unwrap();
    assert!(matches!(
        whole(vec![(None, Shape::Struct(0))], Ok::<_, ()>),
        Ok((None, LogicalType::Struct(fields))) if fields == empty
    ));
}

#[test]
fn an_unread_node_says_why() {
    let text = |unread: Unread<&str>| unread.to_string();
    assert!(text(Unread::Deep).contains(&NESTING_DEPTH.to_string()));
    assert_eq!(text(Unread::Ended), "a type ends before the nodes it names");
    assert_eq!(text(Unread::Beyond), "a type has nodes beyond its own");
    assert_eq!(text(Unread::Unnamed), "a type's field has no name");
    assert_eq!(text(Unread::Node("x")), "x");
}
