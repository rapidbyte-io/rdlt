use bytes::Bytes;

use super::{built, fitted, holds_text, sized};
use crate::shred::observe::{Observed, Shape};
use crate::shred::records::chunks;
use crate::shred::tests::limits;
use crate::shred::{again, join, parse};

/// The shape `text`'s records were observed to hold, and their rows.
fn observed(text: &str) -> (Shape, u64) {
    let chunk = chunks(&[Bytes::from(text.to_owned())], 1 << 30)
        .unwrap()
        .remove(0);
    let rows = u64::try_from(chunk.rows).unwrap();
    (parse(chunk, limits()).unwrap().shape, rows)
}

/// Records of every kind, nested in objects and lists, with nulls, empty and missing values.
const KINDS: &[&str] = &[
    r#"{"b":true,"i":1,"w":18446744073709551615,"h":100000000000000000000,"f":0.5,"t":"text"}"#,
    r#"{"v":1234567890123456789012345678901234567890,"n":null,"j":1,"o":{"p":1,"q":{"r":"s"}}}"#,
    r#"{"j":"x","l":[1,2,3],"m":[[1],[],null],"a":[{"x":1},{"y":"z"},{}],"e":[],"o":null}"#,
    r#"{"g":12345678901234567890123456789012345678901234567890123456789012345678901234567890}"#,
    r#"{"b":null,"t":null,"a":[null,{"x":2,"z":[true]}],"k":[0.5,1.5],"s":["a",null,"bc"]}"#,
];

#[test]
fn a_batch_built_again_holds_no_more_than_its_build_was_reckoned_to_take() {
    for chunk_bytes in [1, 64, 1 << 20] {
        let pushes: Vec<Bytes> = KINDS.iter().map(|text| Bytes::from(*text)).collect();
        let mut parsed: Vec<_> = chunks(&pushes, chunk_bytes)
            .unwrap()
            .into_iter()
            .map(|chunk| parse(chunk, limits()).unwrap())
            .collect();
        let (joined, plans, _) = join(&parsed, limits()).unwrap();
        for (chunk, plan) in parsed.drain(..).zip(plans) {
            let rows = u64::try_from(chunk.chunk.rows).unwrap();
            let text = u64::try_from(chunk.chunk.bytes).unwrap();
            // Each column's buffers are rounded up to 64 bytes, which is not reckoned.
            let rounding = 3 * 64 * (joined.columns() + 1);
            let reckoned = built(&joined, &chunk.shape, rows).bytes + 2 * text + rounding;
            let local = sized(&joined, &chunk.shape);
            let columns = again(&chunk.chunk, &local, plan.exact || chunk.exact).unwrap();
            let held: usize = columns
                .iter()
                .map(|column| column.get_array_memory_size())
                .sum();
            assert!(
                u64::try_from(held).unwrap() <= reckoned,
                "{held} > {reckoned} in chunks of {chunk_bytes}"
            );
        }
    }
}

#[test]
fn cells_are_rows_under_columns_of_values_at_every_level() {
    let (shape, rows) = observed(
        r#"{"a":1,"n":null,"o":{"p":1,"q":null},"l":[1,2,3],"m":[{"x":1},{"y":2}]}
{"a":2,"l":[],"m":null}"#,
    );
    // a and o.p twice; l's three items; m's two items under x and y.
    assert_eq!(built(&shape, &shape, rows).cells, 2 + 2 + 3 + 2 * 2);
    // Against a shape that holds more, a chunk lacking columns still takes a cell in each.
    let (more, _) = observed(r#"{"z":1,"a":1}"#);
    let mut joined = shape.clone();
    joined.join(&more);
    assert_eq!(built(&joined, &shape, rows).cells, 2 + 2 + 3 + 2 * 2 + 2);
}

#[test]
fn fitting_a_chunk_takes_the_columns_it_lacks_and_the_integers_it_casts() {
    let (local, rows) = observed(r#"{"a":1,"o":{"p":1}}"#);
    let (other, _) = observed(r#"{"a":0.5,"o":{"p":1,"q":true},"z":"x"}"#);
    let mut joined = local.clone();
    joined.join(&other);
    assert_eq!(fitted(&local, &local, rows), 0);
    let mut only_cast = local.clone();
    only_cast.join(&observed(r#"{"a":0.5}"#).0);
    let cast = fitted(&only_cast, &local, rows);
    assert!(cast > 0);
    assert!(
        fitted(&joined, &local, rows) > cast,
        "and o.q and z as nulls"
    );
}

#[test]
fn a_shape_holds_text_where_a_column_of_text_or_json_lies_at_any_depth() {
    let (ints, _) = observed(r#"{"a":{"x":[1]},"b":1}"#);
    let (json, _) = observed(r#"{"a":{"x":["t"]},"b":"x"}"#);
    let mut joined = ints.clone();
    joined.join(&json);
    assert!(holds_text(&json) && holds_text(&joined));
    assert!(!holds_text(&ints));
}

#[test]
fn a_shape_sized_for_a_chunk_takes_the_items_its_lists_held() {
    let (local, _) = observed(r#"{"l":[[1,2],[3]],"o":{"m":[1]}}"#);
    let (other, _) = observed(r#"{"l":[[4,5,6,7,8]],"o":{"m":[1,2,3,4]},"n":[1]}"#);
    let mut joined = local.clone();
    joined.join(&other);
    let sized = sized(&joined, &local);
    let Some(Observed::Array(item, 2)) = sized.get("l") else {
        panic!("l sized for its two arrays: {sized:?}");
    };
    assert!(matches!(item.as_ref(), Observed::Array(_, 3)));
    assert!(matches!(sized.get("n"), Some(Observed::Array(_, 0))));
    let Some(Observed::Object(object)) = sized.get("o") else {
        panic!("an object");
    };
    assert!(matches!(object.get("m"), Some(Observed::Array(_, 1))));
}
