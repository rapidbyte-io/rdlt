use bytes::Bytes;
use proptest::prelude::*;
use pyo3::types::PyAnyMethods as _;
use pyo3::{Bound, PyAny, Python};

use super::{JsonLimits, Unwritten, each_push};
use crate::embedded;

const WIDE: JsonLimits = JsonLimits {
    push_bytes: 1 << 20,
    depth: 64,
};

fn pushes(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    limits: JsonLimits,
) -> Result<Vec<Bytes>, Unwritten> {
    let mut written = Vec::new();
    let finished = each_push(py, rows, limits, |push| {
        written.push(push);
        true
    })?;
    assert!(finished, "nothing here asks the writing to stop");
    Ok(written)
}

fn loads<'py>(py: Python<'py>, json: &str) -> Bound<'py, PyAny> {
    py.import("json")
        .unwrap()
        .call_method1("loads", (json,))
        .unwrap()
}

fn evaluated<'py>(py: Python<'py>, code: &str) -> Bound<'py, PyAny> {
    py.eval(&std::ffi::CString::new(code).unwrap(), None, None)
        .unwrap()
}

fn lines(pushed: &[Bytes]) -> Vec<serde_json::Value> {
    pushed
        .iter()
        .flat_map(|push| {
            push.split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
        })
        .map(|line| serde_json::from_slice(line).expect("each line is JSON"))
        .collect()
}

fn json_value() -> impl Strategy<Value = serde_json::Value> {
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::from),
        any::<i64>().prop_map(serde_json::Value::from),
        any::<u64>().prop_map(serde_json::Value::from),
        any::<f64>()
            .prop_filter("finite", |float| float.is_finite())
            .prop_map(serde_json::Value::from),
        "\\PC{0,12}".prop_map(serde_json::Value::from),
    ];
    leaf.prop_recursive(4, 32, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5).prop_map(serde_json::Value::from),
            prop::collection::btree_map("\\PC{1,8}", inner, 0..5)
                .prop_map(|map| serde_json::Value::Object(map.into_iter().collect())),
        ]
    })
}

proptest! {
    #[test]
    fn a_row_parsed_from_json_is_written_back_as_that_json(
        fields in prop::collection::btree_map("\\PC{1,8}", json_value(), 0..6)
    ) {
        let row = serde_json::Value::Object(fields.into_iter().collect());
        embedded::with(|py| {
            let parsed = loads(py, &row.to_string());
            let written = pushes(py, &parsed, WIDE).expect("a JSON row writes");
            prop_assert_eq!(lines(&written), vec![row.clone()]);
            Ok(())
        })?;
    }
}

#[test]
fn rows_are_cut_into_pushes_within_the_limit_and_none_is_lost() {
    embedded::with(|py| {
        let rows = evaluated(py, "[{'n': n, 'pad': 'x' * 40} for n in range(100)]");
        let limits = JsonLimits {
            push_bytes: 512,
            depth: 64,
        };
        let written = pushes(py, &rows, limits).unwrap();
        assert!(written.len() > 1);
        assert!(written.iter().all(|push| push.len() <= 512));
        let ns: Vec<i64> = lines(&written)
            .iter()
            .map(|row| row["n"].as_i64().unwrap())
            .collect();
        assert_eq!(ns, (0..100).collect::<Vec<_>>());
    });
}

#[test]
fn a_full_push_is_handed_over_before_the_next_row_is_written() {
    embedded::with(|py| {
        let rows = evaluated(py, "[{'pad': 'x' * 100} for n in range(10)]");
        let limits = JsonLimits {
            push_bytes: 256,
            depth: 64,
        };
        let mut seen = 0;
        let finished = each_push(py, &rows, limits, |_| {
            seen += 1;
            false
        })
        .unwrap();
        assert!(!finished, "the read went away");
        assert_eq!(seen, 1, "the writing stopped at the first refused push");
    });
}

#[test]
fn python_s_own_values_take_their_json_forms() {
    embedded::with(|py| {
        let row = evaluated(
            py,
            "{'big': 2**80, 'one': 1.0, 'yes': True, 'when': __import__('datetime').date(2026, 11, 6), \
             'at': __import__('datetime').datetime(2026, 11, 6, 12, 30), 'cost': __import__('decimal').Decimal('12.50'), \
             'id': __import__('uuid').UUID(int=1), 'tags': ('a', 'b')}",
        );
        let written = lines(&pushes(py, &row, WIDE).unwrap());
        let text = std::str::from_utf8(&pushes(py, &row, WIDE).unwrap()[0])
            .unwrap()
            .to_owned();
        assert!(text.contains("\"big\":1208925819614629174706176"), "{text}");
        assert!(text.contains("\"one\":1.0"), "{text}");
        assert!(text.contains("\"cost\":12.50"), "{text}");
        assert_eq!(written[0]["yes"], true);
        assert_eq!(written[0]["when"], "2026-11-06");
        assert_eq!(written[0]["at"], "2026-11-06T12:30:00");
        assert_eq!(written[0]["id"], "00000000-0000-0000-0000-000000000001");
        assert_eq!(written[0]["tags"], serde_json::json!(["a", "b"]));
    });
}

#[test]
fn a_python_subclass_keeps_its_value_whatever_its_str_says() {
    embedded::with(|py| {
        let row = evaluated(
            py,
            "(lambda B, D: {'big': B(2**80), 'cost': D('12.50')})(\
             type('B', (int,), {'__str__': lambda self: '1,\"injected\":2'}), \
             type('D', (__import__('decimal').Decimal,), {'__str__': lambda self: '9}\\n{\"evil\":1'}))",
        );
        let text = std::str::from_utf8(&pushes(py, &row, WIDE).unwrap()[0])
            .unwrap()
            .to_owned();
        assert_eq!(text, "{\"big\":1208925819614629174706176,\"cost\":12.50}\n");
    });
}

#[test]
fn what_has_no_json_form_is_refused_naming_its_path_and_type_never_its_value() {
    embedded::with(|py| {
        for (code, expected) in [
            ("{'payload': b'hunter2'}", "json_unsupported_type"),
            ("{1: 'hunter2'}", "json_key_not_text"),
            ("{'ratio': float('nan')}", "json_not_finite"),
            (
                "{'cost': __import__('decimal').Decimal('Infinity')}",
                "json_not_finite",
            ),
            ("{'name': 'hunter2\\udc80'}", "json_not_text"),
            ("['hunter2']", "json_not_object"),
            ("{'set': {'hunter2'}}", "json_unsupported_type"),
        ] {
            let error = pushes(py, &evaluated(py, code), WIDE).expect_err(code);
            let connector = error.connector_error();
            assert_eq!(connector.code(), Some(expected), "{code}");
            assert!(
                !connector.to_string().contains("hunter2"),
                "{code}: {connector}"
            );
        }
    });
}

#[test]
fn a_row_nests_as_deep_as_the_limit_and_no_deeper() {
    embedded::with(|py| {
        let nested = |levels: usize| {
            evaluated(
                py,
                &format!("{}1{}", "{'a': ".repeat(levels), "}".repeat(levels)),
            )
        };
        let limits = JsonLimits {
            push_bytes: 1 << 20,
            depth: 8,
        };
        assert!(pushes(py, &nested(8), limits).is_ok());
        let error = pushes(py, &nested(9), limits).expect_err("too deep");
        assert!(matches!(error, Unwritten::Deep { row: 1, limit: 8 }));
    });
}

#[test]
fn a_row_that_holds_itself_is_refused_as_too_deep() {
    embedded::with(|py| {
        let row = evaluated(py, "(lambda d: (d.update(me=d), d)[1])({})");
        let error = pushes(py, &row, WIDE).expect_err("a cycle");
        assert_eq!(error.connector_error().code(), Some("json_too_deep"));
    });
}

#[test]
fn a_row_larger_than_a_push_is_refused() {
    embedded::with(|py| {
        let row = evaluated(py, "{'pad': 'x' * 600}");
        let error = pushes(
            py,
            &row,
            JsonLimits {
                push_bytes: 512,
                depth: 64,
            },
        )
        .expect_err("too large");
        assert_eq!(error.connector_error().code(), Some("json_push_bytes"));
    });
}

#[test]
fn a_row_stops_being_written_once_it_is_past_a_push() {
    embedded::with(|py| {
        let row = evaluated(py, "{'pad': [1] * 1_000_000}");
        let error = pushes(
            py,
            &row,
            JsonLimits {
                push_bytes: 512,
                depth: 64,
            },
        )
        .expect_err("too large");
        assert_eq!(error.connector_error().code(), Some("json_push_bytes"));
        let Unwritten::Large { bytes, .. } = error else {
            panic!("{error}")
        };
        assert!(bytes < 2048, "written to {bytes} bytes");
    });
}

#[test]
fn a_string_past_a_push_is_refused_before_it_is_encoded() {
    embedded::with(|py| {
        // Eight newlines encode to sixteen bytes: the size told is of what was never encoded.
        let row = evaluated(py, "{'a': '\\n' * 8}");
        let error = pushes(
            py,
            &row,
            JsonLimits {
                push_bytes: 8,
                depth: 64,
            },
        )
        .expect_err("too large");
        assert!(
            matches!(error, Unwritten::Large { bytes: 13, .. }),
            "{error}"
        );
    });
}

#[test]
fn a_string_that_only_reaches_the_limit_is_measured_encoded() {
    embedded::with(|py| {
        let row = evaluated(py, "{'a': 'x'}");
        let error = pushes(
            py,
            &row,
            JsonLimits {
                push_bytes: 6,
                depth: 64,
            },
        )
        .expect_err("too large");
        assert!(
            matches!(error, Unwritten::Large { bytes: 10, .. }),
            "{error}"
        );
    });
}

#[test]
fn a_row_exactly_at_the_limit_before_a_value_is_measured_whole() {
    embedded::with(|py| {
        let row = evaluated(py, "{'a': 1}");
        let error = pushes(
            py,
            &row,
            JsonLimits {
                push_bytes: 5,
                depth: 64,
            },
        )
        .expect_err("too large");
        assert!(
            matches!(error, Unwritten::Large { bytes: 8, .. }),
            "{error}"
        );
    });
}

#[test]
fn a_limit_too_large_for_this_platform_holds_all_there_is() {
    let limits = JsonLimits::new(u64::MAX, u64::MAX);
    assert_eq!(limits.push_bytes, usize::MAX);
    assert_eq!(limits.depth, usize::MAX);
    let limits = JsonLimits::new(4096, 9);
    assert_eq!(limits.push_bytes, 4096);
    assert_eq!(limits.depth, 9);
}

#[test]
fn a_refused_value_says_its_row_its_path_and_its_type() {
    embedded::with(|py| {
        let rows = evaluated(py, "[{'ok': 1}, {'inner': {'payload': b'x'}}]");
        let error = pushes(py, &rows, WIDE).expect_err("refused");
        let Unwritten::Unsupported {
            row,
            path,
            type_name,
        } = error
        else {
            panic!("{error:?}")
        };
        assert_eq!(
            (row, path.as_str(), type_name.as_str()),
            (2, "$.inner.payload", "bytes")
        );
    });
}

#[test]
fn a_list_nests_as_deep_as_the_limit_and_no_deeper() {
    embedded::with(|py| {
        let nested = |lists: usize| {
            evaluated(
                py,
                &format!("{{'a': {}1{}}}", "[".repeat(lists), "]".repeat(lists)),
            )
        };
        let limits = JsonLimits {
            push_bytes: 1 << 20,
            depth: 8,
        };
        assert!(pushes(py, &nested(7), limits).is_ok());
        assert!(matches!(
            pushes(py, &nested(8), limits).expect_err("too deep"),
            Unwritten::Deep { row: 1, limit: 8 }
        ));
    });
}

#[test]
fn a_push_holds_exactly_its_limit_and_rows_that_fill_it_share_one() {
    embedded::with(|py| {
        let row = evaluated(py, "{'a': 'xxxx'}");
        let line = "{\"a\":\"xxxx\"}\n";
        let limits = |push_bytes| JsonLimits {
            push_bytes,
            depth: 64,
        };
        assert_eq!(pushes(py, &row, limits(line.len())).unwrap().len(), 1);
        assert_eq!(
            pushes(py, &row, limits(line.len() - 1))
                .expect_err("one byte over")
                .connector_error()
                .code(),
            Some("json_push_bytes")
        );
        let rows = evaluated(py, "[{'a': 'xxxx'}, {'a': 'xxxx'}]");
        let written = pushes(py, &rows, limits(line.len() * 2)).unwrap();
        assert_eq!(written.len(), 1, "two rows that fill a push share it");
        assert_eq!(written[0].len(), line.len() * 2);
    });
}
