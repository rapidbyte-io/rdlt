use std::collections::BTreeSet;

use rdlt_connector::{Capabilities, DecimalType, LogicalType, SchemaChanges, TypeKind};
use rdlt_engine::{Nested, SchemaPolicy};

use super::keys::key_step;
use super::{Arrival, Outcome, Rules, Step, UNSUPPORTED, orders, outcome};

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
    assert_eq!(frozen.step(None, &typed(int32.clone())), Step::Refused);
    assert_eq!(
        frozen.step(Some(&int32), &typed(LogicalType::Int16)),
        Step::To(int32.clone()),
        "a frozen column still takes values it holds"
    );
    assert_eq!(
        frozen.step(Some(&int32), &typed(LogicalType::Int64)),
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
            evolving.step(Some(&int32), &typed(LogicalType::Int64)),
            Step::To(LogicalType::Int64)
        );
        let variant = evolving.step(Some(&int32), &typed(LogicalType::Utf8));
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
        hinted.step(None, &typed(LogicalType::Int64)),
        Step::Refused,
        "a new hinted column takes its hint, which the batch does not fit"
    );
    assert_eq!(
        hinted.step(None, &typed(LogicalType::Int8)),
        Step::To(LogicalType::Int32)
    );
}

#[test]
fn a_destination_that_cannot_add_columns_refuses_a_new_one() {
    let mut capabilities = capabilities(&[]);
    capabilities.schema_changes.add_column = false;
    let evolving = rules(SchemaPolicy::Evolve, true, &capabilities);
    assert_eq!(
        evolving.step(None, &typed(LogicalType::Int8)),
        Step::Refused
    );
    assert_eq!(
        evolving.step(Some(&LogicalType::Int8), &typed(LogicalType::Int8)),
        Step::To(LogicalType::Int8)
    );
}

#[test]
fn only_a_json_column_surely_holds_a_pushed_container() {
    let capabilities = capabilities(&[]);
    let evolving = rules(SchemaPolicy::Evolve, true, &capabilities);
    assert_eq!(
        evolving.step(Some(&LogicalType::Json), &Arrival::Container),
        Step::To(LogicalType::Json)
    );
    assert_eq!(
        evolving.step(Some(&LogicalType::Int8), &Arrival::Container),
        Step::Refused
    );
    assert_eq!(evolving.step(None, &Arrival::Container), Step::Unknown);
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
    let step = |current: Option<&LogicalType>, arrival: &Arrival| evolving.step(current, arrival);
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
    let step = |current: Option<&LogicalType>, arrival: &Arrival| evolving.step(current, arrival);
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
