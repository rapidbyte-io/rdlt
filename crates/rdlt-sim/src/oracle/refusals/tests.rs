use std::collections::BTreeSet;

use rdlt_connector::{Capabilities, DecimalType, LogicalType, SchemaChanges, TypeKind};
use rdlt_engine::{Nested, SchemaPolicy};

use super::keys::key_step;
use super::{Arrival, Outcome, Own, Rules, Step, UNSUPPORTED, orders, outcome};

/// A destination storing every scalar type natively that widens only `widenings`.
fn capabilities(widenings: &[(TypeKind, TypeKind)]) -> Capabilities {
    let mut capabilities = Capabilities::minimal();
    capabilities.schema_changes = SchemaChanges {
        add_column: true,
        widenings: widenings.iter().copied().collect::<BTreeSet<_>>(),
    };
    capabilities
}

fn rules(policy: SchemaPolicy, refuses: bool, capabilities: &Capabilities) -> Rules<'_> {
    Rules {
        policy,
        refuses,
        nested: Nested::Native,
        hint: None,
        capabilities,
    }
}

fn typed(logical: LogicalType) -> Arrival {
    Arrival::Typed(logical)
}

/// `rules`' steps over plain types: columns whose exactness the test leaves aside, as inexact.
fn plain<'a>(
    rules: &'a Rules<'a>,
) -> impl Fn(Option<&LogicalType>, &Arrival) -> Step<LogicalType> + 'a {
    move |current, arrival| {
        let inexact = Arrival::Typed(LogicalType::Int64);
        let current = current.map(|logical| Own::new(logical.clone(), Some(&inexact)));
        match rules.step(current.as_ref(), arrival) {
            Step::To(own) => Step::To(own.logical),
            Step::Refused => Step::Refused,
            Step::Unknown => Step::Unknown,
        }
    }
}

#[test]
fn every_order_of_the_arrivals_is_followed_once() {
    let mut three = orders(3);
    assert_eq!(three.len(), 6);
    three.sort();
    three.dedup();
    assert_eq!(three.len(), 6);
    assert_eq!(orders(0), [Vec::<usize>::new()]);
}

#[test]
fn a_frozen_column_refuses_a_new_column_and_any_type_it_does_not_hold() {
    let capabilities = capabilities(&[(TypeKind::Int32, TypeKind::Int64)]);
    let frozen = rules(SchemaPolicy::Freeze, false, &capabilities);
    let int32 = LogicalType::Int32;
    assert_eq!(plain(&frozen)(None, &typed(int32.clone())), Step::Refused);
    assert_eq!(
        plain(&frozen)(Some(&int32), &typed(LogicalType::Int16)),
        Step::To(int32.clone()),
        "a frozen column still takes values it holds"
    );
    assert_eq!(
        plain(&frozen)(Some(&int32), &typed(LogicalType::Int64)),
        Step::Refused,
        "even where the destination could widen it"
    );
}

#[test]
fn an_evolving_column_widens_where_it_can_and_else_is_refused_only_when_told() {
    let capabilities = capabilities(&[(TypeKind::Int32, TypeKind::Int64)]);
    let int32 = LogicalType::Int32;
    for refuses in [false, true] {
        let evolving = rules(SchemaPolicy::Evolve, refuses, &capabilities);
        assert_eq!(
            plain(&evolving)(Some(&int32), &typed(LogicalType::Int64)),
            Step::To(LogicalType::Int64)
        );
        let variant = plain(&evolving)(Some(&int32), &typed(LogicalType::Utf8));
        let expected = if refuses {
            Step::Refused
        } else {
            Step::To(int32.clone())
        };
        assert_eq!(variant, expected, "refuses {refuses}");
    }
}

#[test]
fn a_hinted_column_never_widens() {
    let capabilities = capabilities(&[(TypeKind::Int32, TypeKind::Int64)]);
    let hinted = Rules {
        hint: Some(LogicalType::Int32),
        ..rules(SchemaPolicy::Evolve, true, &capabilities)
    };
    assert_eq!(
        plain(&hinted)(None, &typed(LogicalType::Int64)),
        Step::Refused,
        "a new hinted column takes its hint, which the batch does not fit"
    );
    assert_eq!(
        plain(&hinted)(None, &typed(LogicalType::Int8)),
        Step::To(LogicalType::Int32)
    );
}

#[test]
fn a_destination_that_cannot_add_columns_refuses_a_new_one() {
    let mut capabilities = capabilities(&[]);
    capabilities.schema_changes.add_column = false;
    let evolving = rules(SchemaPolicy::Evolve, true, &capabilities);
    assert_eq!(
        plain(&evolving)(None, &typed(LogicalType::Int8)),
        Step::Refused
    );
    assert_eq!(
        plain(&evolving)(Some(&LogicalType::Int8), &typed(LogicalType::Int8)),
        Step::To(LogicalType::Int8)
    );
}

#[test]
fn only_a_json_column_surely_holds_a_pushed_container() {
    let capabilities = capabilities(&[]);
    let evolving = rules(SchemaPolicy::Evolve, true, &capabilities);
    assert_eq!(
        plain(&evolving)(Some(&LogicalType::Json), &Arrival::Container),
        Step::To(LogicalType::Json)
    );
    assert_eq!(
        plain(&evolving)(Some(&LogicalType::Int8), &Arrival::Container),
        Step::Refused
    );
    assert_eq!(plain(&evolving)(None, &Arrival::Container), Step::Unknown);
}

#[test]
fn a_refusal_only_some_orders_meet_may_happen_but_need_not() {
    // Int8 widens to Int16 and Int16 to Int32, but Int8 not straight to Int32: Int16 first
    // widens the column in two steps, Int32 first is refused.
    let capabilities = capabilities(&[
        (TypeKind::Int8, TypeKind::Int16),
        (TypeKind::Int16, TypeKind::Int32),
    ]);
    let evolving = rules(SchemaPolicy::Evolve, true, &capabilities);
    let step =
        |current: Option<&LogicalType>, arrival: &Arrival| plain(&evolving)(current, arrival);
    let both = [vec![typed(LogicalType::Int16), typed(LogicalType::Int32)]];
    assert_eq!(
        outcome(Some(LogicalType::Int8), &both, UNSUPPORTED, step),
        Outcome::may(UNSUPPORTED)
    );
    let only = [vec![typed(LogicalType::Int32)]];
    assert_eq!(
        outcome(Some(LogicalType::Int8), &only, UNSUPPORTED, step),
        Outcome::must(UNSUPPORTED)
    );
    let fitting = [vec![typed(LogicalType::Int16)]];
    assert_eq!(
        outcome(Some(LogicalType::Int8), &fitting, UNSUPPORTED, step),
        Outcome::default()
    );
}

#[test]
fn a_phase_starts_from_every_type_the_last_one_may_have_left() {
    let capabilities = capabilities(&[
        (TypeKind::Int8, TypeKind::Int16),
        (TypeKind::Int16, TypeKind::Int32),
    ]);
    let evolving = rules(SchemaPolicy::Evolve, true, &capabilities);
    let step =
        |current: Option<&LogicalType>, arrival: &Arrival| plain(&evolving)(current, arrival);
    // The first phase widens Int8 to Int16, so the second's Int32 widens it again.
    let phases = [
        vec![typed(LogicalType::Int16)],
        vec![typed(LogicalType::Int32)],
    ];
    assert_eq!(
        outcome(Some(LogicalType::Int8), &phases, UNSUPPORTED, step),
        Outcome::default()
    );
}

#[test]
fn a_key_widens_where_the_destination_can_and_is_refused_otherwise() {
    let decimal = LogicalType::Decimal(DecimalType::new(20, 0).expect("a valid decimal"));
    let widening = capabilities(&[(TypeKind::Int64, TypeKind::Decimal)]);
    let int64 = LogicalType::Int64;
    let native = Nested::Native;
    assert_eq!(
        key_step(
            Some(&int64),
            &typed(decimal.clone()),
            false,
            native,
            &widening
        ),
        Step::To(decimal.clone())
    );
    assert_eq!(
        key_step(
            Some(&int64),
            &typed(LogicalType::Int8),
            false,
            native,
            &widening
        ),
        Step::To(int64.clone()),
        "a narrower key fits"
    );
    let fixed = capabilities(&[]);
    assert_eq!(
        key_step(Some(&int64), &typed(decimal.clone()), false, native, &fixed),
        Step::Refused
    );
    assert_eq!(
        key_step(
            Some(&int64),
            &typed(LogicalType::Utf8),
            false,
            native,
            &widening
        ),
        Step::Refused,
        "a key never becomes JSON"
    );
    assert_eq!(
        key_step(Some(&int64), &typed(decimal), true, native, &widening),
        Step::Refused,
        "a frozen key never widens"
    );
    assert_eq!(
        key_step(
            Some(&int64),
            &typed(LogicalType::Int8),
            true,
            native,
            &widening
        ),
        Step::To(int64.clone()),
        "though a narrower one still fits"
    );
}

#[test]
fn a_decimal_key_stored_as_text_widens_only_where_its_values_render_alike() {
    let decimal = |precision, scale| {
        LogicalType::Decimal(DecimalType::new(precision, scale).expect("a valid decimal"))
    };
    let mut text = capabilities(&[]);
    text.types.remove(&TypeKind::Decimal);
    let native = Nested::Native;
    let current = decimal(10, 2);
    assert_eq!(
        key_step(Some(&current), &typed(decimal(20, 2)), false, native, &text),
        Step::To(decimal(20, 2))
    );
    assert_eq!(
        key_step(Some(&current), &typed(decimal(12, 4)), false, native, &text),
        Step::Refused,
        "1.50 renders as 1.5000 once the scale grows"
    );
}

#[test]
fn floats_after_exact_integers_take_a_variant_and_never_widen_the_column() {
    let mut widening = capabilities(&[]);
    widening
        .schema_changes
        .widenings
        .insert((TypeKind::Int64, TypeKind::Float64));
    let floats = typed(LogicalType::Float64);
    for refuses in [false, true] {
        let evolving = rules(SchemaPolicy::Evolve, refuses, &widening);
        let exact = evolving.step(None, &Arrival::ExactInt);
        let Step::To(exact) = exact else {
            panic!("a new column takes exact integers: {exact:?}");
        };
        assert_eq!(
            exact,
            Own::new(LogicalType::Int64, Some(&Arrival::ExactInt))
        );
        assert!(exact.exact);
        let expected = if refuses {
            Step::Refused
        } else {
            Step::To(exact.clone())
        };
        assert_eq!(evolving.step(Some(&exact), &floats), expected, "{refuses}");
    }
}

#[test]
fn an_integer_a_float_would_round_ends_a_column_s_exactness() {
    let capabilities = capabilities(&[]);
    let evolving = rules(SchemaPolicy::Evolve, false, &capabilities);
    let exact = Own::new(LogicalType::Int64, Some(&Arrival::ExactInt));
    let Step::To(rounded) = evolving.step(Some(&exact), &typed(LogicalType::Int64)) else {
        panic!("a column of integers holds integers");
    };
    assert!(!rounded.exact);
    assert_eq!(
        evolving.step(Some(&rounded), &Arrival::ExactInt),
        Step::To(rounded.clone()),
        "exactness, once lost, stays lost"
    );
    let float = Own::new(LogicalType::Float64, None);
    assert_eq!(
        evolving.step(Some(&float), &Arrival::ExactInt),
        Step::To(float.clone()),
        "a column of floats takes exact integers"
    );
}
