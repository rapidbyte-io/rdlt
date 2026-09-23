use proptest::prelude::*;

use super::{CommentKind, scan};

type Expected = &'static [(usize, CommentKind, &'static str)];

fn comments(source: &str) -> Vec<(usize, CommentKind, String)> {
    scan(source)
        .comments
        .into_iter()
        .map(|c| (c.line, c.kind, c.text))
        .collect()
}

#[test]
fn separates_comments_from_code() {
    use CommentKind::{Block, InnerDoc, Line, OuterDoc};
    let cases: &[(&str, Expected)] = &[
        ("let a = 1; // one\n", &[(1, Line, "// one")]),
        (
            "/// doc\n//! inner\n//// line\n",
            &[
                (1, OuterDoc, "/// doc"),
                (2, InnerDoc, "//! inner"),
                (3, Line, "//// line"),
            ],
        ),
        ("let s = \"// not a comment\";\n", &[]),
        ("let s = \"a\\\"b // still string\";\n", &[]),
        ("let s = r#\"/* not */ \"quoted\" // no\"#;\n", &[]),
        ("let s = br\"// no\";\n", &[]),
        ("let q = '\"'; // real\n", &[(1, Line, "// real")]),
        ("let q = '\\''; // real\n", &[(1, Line, "// real")]),
        ("fn f<'a>(x: &'a str) {} // real\n", &[(1, Line, "// real")]),
        ("let r#type = 1; // real\n", &[(1, Line, "// real")]),
        (
            "a\n/* x /* y */ z */ b // c\n",
            &[(2, Block, "/* x /* y */ z */"), (2, Line, "// c")],
        ),
        (
            "let s = \"x\ny\"; // line two\n",
            &[(2, Line, "// line two")],
        ),
        (
            "// café ünïcode ✓\nlet s = \"żółw\"; // ok\n",
            &[(1, Line, "// café ünïcode ✓"), (2, Line, "// ok")],
        ),
    ];
    for (source, expected) in cases {
        let expected: Vec<_> = expected
            .iter()
            .map(|(l, k, t)| (*l, *k, (*t).to_owned()))
            .collect();
        assert_eq!(comments(source), expected, "source: {source:?}");
    }
}

#[test]
fn code_keeps_lines_and_drops_comment_text() {
    let scanned = scan("fn a() {} // x\n/* y\n z */\nfn b() {}\n");
    assert_eq!(scanned.code.lines().count(), 4);
    assert!(!scanned.code.contains('x') && !scanned.code.contains('y'));
    assert_eq!(scanned.code_lines(), 2);
    assert_eq!(scanned.comment_lines(), 3);
}

#[test]
fn unterminated_literals_and_comments_do_not_panic() {
    for source in [
        "/* open", "\"open", "r#\"open", "'", "'\\", "b'", "/", "r", "br#", "\"\\",
    ] {
        let scanned = scan(source);
        assert!(!scanned.code.contains('\n'), "source: {source:?}");
    }
}

#[test]
fn an_escaped_line_break_is_not_a_char_literal() {
    let scanned = scan("'\\\n' // after");
    assert_eq!(scanned.code.matches('\n').count(), 1);
    assert_eq!(scanned.comments.len(), 1);
    assert_eq!(scanned.comments[0].line, 2);
}

proptest! {
    #[test]
    fn scanning_preserves_line_breaks(source in "\\PC{0,64}(\n\\PC{0,64}){0,8}") {
        let scanned = scan(&source);
        prop_assert_eq!(scanned.code.matches('\n').count(), source.matches('\n').count());
    }
}
