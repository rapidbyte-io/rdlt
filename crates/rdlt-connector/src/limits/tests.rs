use super::{
    MAX_ACKNOWLEDGEABLE, MAX_RESERVED_BYTES, MAX_RESERVED_PREFIXES, MAX_RESERVED_WORDS,
    MIN_IDENTIFIER_LEN,
};

#[test]
fn the_acknowledgeable_positions_have_their_specified_value() {
    assert_eq!(MAX_ACKNOWLEDGEABLE, 1 << 18);
}

/// The words of the comment above the line of the catalog's protocol file that is
/// `declaration`, as one line.
fn documented(declaration: &str) -> String {
    let proto = include_str!("../../../rdlt-wire/proto/rdlt/connector/v1/catalog.proto");
    let lines: Vec<&str> = proto.lines().collect();
    let at = lines
        .iter()
        .position(|line| line.trim() == declaration)
        .unwrap_or_else(|| panic!("`{declaration}` is declared"));
    let mut comment: Vec<&str> = lines[..at]
        .iter()
        .rev()
        .take_while(|line| line.trim_start().starts_with("//"))
        .copied()
        .collect();
    comment.reverse();
    comment
        .iter()
        .flat_map(|line| line.split_whitespace())
        .filter(|word| *word != "//")
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn the_protocol_states_the_bounds_of_a_destinations_identifiers_and_writers() {
    let max_len = documented("uint32 max_len = 2;");
    assert!(
        max_len.contains(&format!("from {MIN_IDENTIFIER_LEN} to {}", u16::MAX)),
        "{max_len:?}"
    );
    let words = documented("repeated string reserved = 4;");
    let prefixes = documented("repeated string reserved_table_prefixes = 5;");
    for (comment, most) in [
        (words, MAX_RESERVED_WORDS),
        (prefixes, MAX_RESERVED_PREFIXES),
    ] {
        let bound = format!("at most {most}, each of 1 to {MAX_RESERVED_BYTES} bytes");
        assert!(comment.contains(&bound), "{bound:?} is not in {comment:?}");
    }
    let writers = documented("uint32 max_parallel_writers = 8;");
    assert!(
        writers.contains(&format!("from 1 to {}", u16::MAX)),
        "{writers:?}"
    );
}
