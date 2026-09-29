use std::collections::BTreeSet;

use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use rdlt_connector::{LogicalType, TypeKind};

use serde_json::json;

use super::{Encoding, KINDS, Scalar, Shape, json as pushed, values};

/// Every type and encoding the shape of a column or anything inside it takes.
fn seen(shape: &Shape, into: &mut BTreeSet<(TypeKind, String)>) {
    let encoding = match shape.encoding {
        Encoding::FixedSize(_) => "FixedSize".to_owned(),
        other => format!("{other:?}"),
    };
    into.insert((shape.logical.kind(), encoding));
    for child in &shape.children {
        seen(child, into);
    }
}

#[test]
fn the_drawn_shapes_hold_every_type_in_every_encoding() {
    let mut runner = TestRunner::deterministic();
    let strategy = values::shape(2);
    let mut drawn = BTreeSet::new();
    for _ in 0..20_000 {
        let shape = strategy.new_tree(&mut runner).expect("a shape").current();
        seen(&shape, &mut drawn);
    }
    let mut expected = BTreeSet::new();
    let every = |kind: TypeKind, encodings: &[&'static str]| -> Vec<(TypeKind, String)> {
        ["Plain", "Dictionary", "RunEnd"]
            .iter()
            .chain(encodings)
            .map(|encoding| (kind, (*encoding).to_owned()))
            .collect()
    };
    for kind in KINDS {
        let extra: &[&'static str] = match kind {
            TypeKind::Int16 | TypeKind::Int32 | TypeKind::Int64 => &["Unsigned"],
            TypeKind::Float32 => &["Half"],
            TypeKind::Decimal => &["Unsigned", "Decimal32", "Decimal64", "Decimal256"],
            TypeKind::Utf8 | TypeKind::Json => &["Large", "View"],
            TypeKind::Binary => &["Large", "View", "FixedSize"],
            TypeKind::Date => &["Date64"],
            _ => &[],
        };
        match kind {
            TypeKind::Struct => {
                expected.insert((kind, "Plain".to_owned()));
            }
            TypeKind::List => expected.extend(
                ["Plain", "Large", "View", "LargeView", "FixedSize", "Map"]
                    .map(|encoding| (kind, encoding.to_owned())),
            ),
            _ => expected.extend(every(kind, extra)),
        }
    }
    let missing: Vec<_> = expected.difference(&drawn).collect();
    assert!(missing.is_empty(), "never drawn: {missing:?}");
}

#[test]
fn pushed_shapes_hold_only_types_json_holds() {
    fn json_types(shape: &Shape) -> bool {
        let leaf = matches!(
            shape.logical.kind(),
            TypeKind::Bool | TypeKind::Int64 | TypeKind::Float64 | TypeKind::Utf8
        );
        let nested = matches!(shape.logical.kind(), TypeKind::Struct | TypeKind::List);
        *shape == pushed::integers()
            || (shape.encoding == Encoding::Plain
                && (leaf || nested)
                && shape.children.iter().all(json_types))
    }
    let mut runner = TestRunner::deterministic();
    let strategy = pushed::shape(3);
    for _ in 0..2_000 {
        let shape = strategy.new_tree(&mut runner).expect("a shape").current();
        assert!(json_types(&shape), "{shape:?}");
    }
}

#[test]
fn values_render_as_json_names_what_json_cannot_hold() {
    let value = Scalar::Struct(vec![
        ("x".to_owned(), Scalar::Float64(f64::NAN)),
        ("y".to_owned(), Scalar::Null),
        (
            "z".to_owned(),
            Scalar::List(vec![Scalar::Float64(f64::NEG_INFINITY), Scalar::Int(-3)]),
        ),
    ]);
    assert_eq!(
        pushed::rendered(&value),
        json!({"x": "NaN", "y": null, "z": ["-Infinity", -3]})
    );
}

/// The edges a thousand draws of a float column of `logical` meet: NaN, each infinity, and zero
/// below zero.
fn edges(logical: LogicalType) -> BTreeSet<&'static str> {
    let mut runner = TestRunner::deterministic();
    let shape = Shape {
        logical,
        encoding: Encoding::Plain,
        children: Vec::new(),
    };
    let strategy = values::value(&shape, false);
    let mut met = BTreeSet::new();
    for _ in 0..1_000 {
        let value = match strategy.new_tree(&mut runner).expect("a value").current() {
            Scalar::Float32(value) => f64::from(value),
            Scalar::Float64(value) => value,
            other => panic!("a float column drew {other:?}"),
        };
        met.extend(match value {
            _ if value.is_nan() => Some("NaN"),
            f64::INFINITY => Some("infinity"),
            f64::NEG_INFINITY => Some("-infinity"),
            _ if value == 0.0 && value.is_sign_negative() => Some("-0"),
            _ => None,
        });
    }
    met
}

#[test]
fn a_float_column_draws_nan_infinities_and_negative_zero() {
    let every = BTreeSet::from(["NaN", "infinity", "-infinity", "-0"]);
    assert_eq!(edges(LogicalType::Float64), every);
    assert_eq!(edges(LogicalType::Float32), every);
}

#[test]
fn a_decimal_s_json_text_has_its_scale_and_no_leading_zeros() {
    let decimal = |precision, scale| {
        LogicalType::Decimal(rdlt_connector::DecimalType::new(precision, scale).unwrap())
    };
    let text = |digits: &str, logical: &LogicalType| {
        pushed::text(&Scalar::Decimal(digits.to_owned()), logical)
    };
    assert_eq!(text("12345", &decimal(10, 2)), "123.45");
    assert_eq!(text("-5", &decimal(10, 2)), "-0.05");
    assert_eq!(text("007", &decimal(10, 0)), "7");
    assert_eq!(text("-000", &decimal(10, 2)), "0.00");
    let wide = "1".repeat(90);
    assert_eq!(
        text(&format!("00{wide}"), &pushed::integers().logical),
        wide
    );
    let nested = LogicalType::List(Box::new(rdlt_connector::Field::new(
        "item",
        decimal(4, 1),
        true,
    )));
    let list = Scalar::List(vec![Scalar::Decimal("15".to_owned()), Scalar::Null]);
    assert_eq!(pushed::text(&list, &nested), "[1.5,null]");
}

#[test]
fn integers_of_64_bits_draw_both_those_a_float_holds_and_those_it_would_round() {
    let mut runner = TestRunner::deterministic();
    let shape = Shape {
        logical: LogicalType::Int64,
        encoding: Encoding::Plain,
        children: Vec::new(),
    };
    let strategy = values::value(&shape, false);
    let (mut exact, mut rounds) = (0, 0);
    for _ in 0..1_000 {
        match strategy.new_tree(&mut runner).expect("a value").current() {
            Scalar::Int(value) if value.unsigned_abs() <= 1 << 53 => exact += 1,
            Scalar::Int(_) => rounds += 1,
            other => panic!("an integer column drew {other:?}"),
        }
    }
    assert!(
        exact > 300 && rounds > 300,
        "{exact} exact, {rounds} rounding"
    );
}

#[test]
fn whole_numbers_pushed_as_json_reach_every_width() {
    let mut runner = TestRunner::deterministic();
    let strategy = values::value(&pushed::integers(), false);
    let mut widths = BTreeSet::new();
    for _ in 0..2_000 {
        let Scalar::Decimal(digits) = strategy.new_tree(&mut runner).expect("a value").current()
        else {
            panic!("whole numbers are decimals");
        };
        let width = digits.trim_start_matches('-').len();
        assert!(!digits.trim_start_matches('-').starts_with('0') || width == 1);
        widths.insert(match width {
            ..=18 => "64 bits",
            19..=38 => "38 digits",
            39..=76 => "76 digits",
            _ => "beyond",
        });
    }
    assert_eq!(widths.len(), 4, "{widths:?}");
}
