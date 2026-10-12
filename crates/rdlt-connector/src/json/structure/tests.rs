use proptest::prelude::*;

use super::{Mark, Structure};

/// The marks a per-byte walk finds: the reference every scanner copied.
fn walked(text: &[u8]) -> Vec<Mark> {
    let (mut marks, mut at) = (Vec::new(), 0);
    while at < text.len() {
        match text[at] {
            b'"' => {
                let start = at;
                at += 1;
                while at < text.len() && text[at] != b'"' {
                    at += if text[at] == b'\\' { 2 } else { 1 };
                }
                at = (at + 1).min(text.len());
                marks.push(Mark::Text(start..at));
                continue;
            }
            b'[' | b'{' => marks.push(Mark::Open(at)),
            b']' => marks.push(Mark::Close(at, true)),
            b'}' => marks.push(Mark::Close(at, false)),
            b',' => marks.push(Mark::Comma(at)),
            _ => {}
        }
        at += 1;
    }
    marks
}

fn json_bytes() -> impl Strategy<Value = Vec<u8>> {
    let piece = prop_oneof![
        Just(b"\"".to_vec()),
        Just(b"\\".to_vec()),
        Just(b"\\\\".to_vec()),
        Just(b"\\\"".to_vec()),
        Just(b"[".to_vec()),
        Just(b"]".to_vec()),
        Just(b"{".to_vec()),
        Just(b"}".to_vec()),
        Just(b",".to_vec()),
        Just(b"\n".to_vec()),
        Just(b"\r\n".to_vec()),
        Just(b" ".to_vec()),
        "[a-z0-9:.e+-]{0,8}".prop_map(String::into_bytes),
    ];
    prop::collection::vec(piece, 0..64).prop_map(|pieces| pieces.concat())
}

proptest! {
    #[test]
    fn a_structure_s_marks_are_a_per_byte_walk_s(text in json_bytes()) {
        prop_assert_eq!(Structure::new(&text, 0).collect::<Vec<_>>(), walked(&text));
    }
}

#[test]
fn a_string_ending_in_escaped_backslashes_closes_at_its_quote() {
    let text = br#"["a\\\\", "b\"c"],"#;
    let expected = [
        Mark::Open(0),
        Mark::Text(1..8),
        Mark::Comma(8),
        Mark::Text(10..16),
        Mark::Close(16, true),
        Mark::Comma(17),
    ];
    assert_eq!(walked(text), expected);
    let marks: Vec<Mark> = Structure::new(text, 0).collect();
    assert_eq!(marks, expected);
}

#[test]
fn a_string_that_never_closes_runs_to_the_end() {
    let marks: Vec<Mark> = Structure::new(br#"{"a"#, 0).collect();
    assert_eq!(marks, [Mark::Open(0), Mark::Text(1..3)]);
}
