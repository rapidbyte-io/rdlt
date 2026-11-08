use rdlt_connector::LogicalType;
use rdlt_testkit::drawn::Scalar;

use super::{Arrival, pushed, shredded};
use crate::rng::SplitMix64;
use crate::swarm::Features;
use crate::workload::{Reshaping, Row, SimStream, Workload};

#[test]
fn pushed_scalars_are_inferred_as_json_holds_them() {
    let typed = Arrival::Typed;
    assert_eq!(pushed(&Scalar::Float64(1.5)), typed(LogicalType::Float64));
    assert_eq!(pushed(&Scalar::Int(3)), Arrival::ExactInt);
    assert_eq!(pushed(&Scalar::Int(-(1 << 53))), Arrival::ExactInt);
    assert_eq!(
        pushed(&Scalar::Int((1 << 53) + 1)),
        typed(LogicalType::Int64),
        "an integer a float would round"
    );
    assert_eq!(pushed(&Scalar::Bool(true)), typed(LogicalType::Bool));
    assert_eq!(
        pushed(&Scalar::Float64(f64::NAN)),
        typed(LogicalType::Utf8),
        "a float JSON cannot hold is pushed as its name"
    );
    assert_eq!(pushed(&Scalar::Null), typed(LogicalType::Null));
    let decimal = |precision| {
        typed(LogicalType::Decimal(
            rdlt_connector::DecimalType::new(precision, 0).unwrap(),
        ))
    };
    let whole = |digits: &str| pushed(&Scalar::Decimal(digits.to_owned()));
    assert_eq!(whole("-9223372036854775808"), typed(LogicalType::Int64));
    assert_eq!(whole("12"), Arrival::ExactInt);
    assert_eq!(whole("18446744073709551615"), decimal(20));
    assert_eq!(whole("-9223372036854775809"), decimal(38));
    assert_eq!(whole(&"9".repeat(38)), decimal(38));
    assert_eq!(whole(&"9".repeat(39)), decimal(76));
    assert_eq!(whole(&format!("-{}", "9".repeat(76))), decimal(76));
    assert_eq!(whole(&"9".repeat(77)), typed(LogicalType::Json));
    assert_eq!(pushed(&Scalar::List(Vec::new())), Arrival::Container);
}

#[test]
fn a_push_mixing_types_arrives_as_their_join() {
    let typed = Arrival::Typed;
    let int = typed(LogicalType::Int64);
    assert_eq!(
        int.clone().join(typed(LogicalType::Utf8)),
        typed(LogicalType::Json)
    );
    assert_eq!(int.clone().join(typed(LogicalType::Null)), int);
    assert_eq!(
        Arrival::Container.join(int),
        typed(LogicalType::Json),
        "a container joins a scalar as JSON"
    );
    assert_eq!(
        Arrival::Container.join(typed(LogicalType::Null)),
        Arrival::Container
    );
}

#[test]
fn only_json_surely_holds_a_pushed_container() {
    assert_eq!(Arrival::Container.fits(&LogicalType::Json), Some(true));
    assert_eq!(Arrival::Container.fits(&LogicalType::Int64), Some(false));
    let list = LogicalType::List(Box::new(rdlt_connector::Field::new(
        "item",
        LogicalType::Int64,
        true,
    )));
    assert_eq!(Arrival::Container.fits(&list), None);
}

#[test]
fn a_push_may_arrive_as_wide_as_the_pushes_the_engine_may_shred_with_it() {
    let mut mixed = 0;
    for seed in 0..300 {
        let workload = Workload::generate(&mut SplitMix64::new(seed), Features::ALL);
        for stream in workload.streams.iter().filter(|stream| stream.json) {
            for phase in 0..crate::workload::PHASES {
                for partition in 0..stream.partitions.len() {
                    for row in stream.read(partition, phase) {
                        for column in 0..stream.drift.len() {
                            let (Some(own), Some(wide)) = (
                                super::arrival(stream, row, column),
                                super::widest(stream, row, column),
                            ) else {
                                continue;
                            };
                            assert_eq!(own.clone().join(wide.clone()), wide, "seed {seed}");
                            mixed += usize::from(own != wide);
                        }
                    }
                }
            }
        }
    }
    assert!(
        mixed > 0,
        "some pushes differ in type from those around them"
    );
}

#[test]
fn exact_integers_join_floats_as_floats_and_other_integers_as_integers() {
    let typed = Arrival::Typed;
    let exact = Arrival::ExactInt;
    assert_eq!(exact.clone().join(Arrival::ExactInt), Arrival::ExactInt);
    assert_eq!(
        exact.clone().join(typed(LogicalType::Null)),
        Arrival::ExactInt
    );
    assert_eq!(
        exact.clone().join(typed(LogicalType::Float64)),
        typed(LogicalType::Float64)
    );
    assert_eq!(
        typed(LogicalType::Int64).join(exact.clone()),
        typed(LogicalType::Int64)
    );
    assert_eq!(
        exact.clone().join(typed(LogicalType::Utf8)),
        typed(LogicalType::Json)
    );
    assert_eq!(
        typed(LogicalType::Int64).join(typed(LogicalType::Float64)),
        typed(LogicalType::Json),
        "a float would round some of the integers"
    );
    assert_eq!(exact.fits(&LogicalType::Float64), Some(true));
    assert_eq!(exact.fits(&LogicalType::Int64), Some(true));
    assert_eq!(exact.fits(&LogicalType::Int32), Some(false));
    assert_eq!(
        typed(LogicalType::Int64).fits(&LogicalType::Float64),
        Some(false)
    );
}

/// A row of id `id` holding `value` in its one drift column.
fn holding(id: i64, value: Scalar) -> Row {
    Row {
        id,
        partition: 0,
        offset: id,
        value: 0,
        key: None,
        tag: None,
        extras: vec![Some(value)],
        delivered: 0,
    }
}

#[test]
fn a_json_column_s_integers_are_judged_whether_pushed_as_integers_or_whole_numbers() {
    let inexact = || Scalar::Decimal("357782901226230031".to_owned());
    let typed = Arrival::Typed(LogicalType::Int64);
    let rows = |values: Vec<Scalar>| -> Vec<Row> {
        let values = values.into_iter().zip(1..);
        values.map(|(value, id)| holding(id, value)).collect()
    };
    let workload = Workload::generate(&mut SplitMix64::new(0), Features::ALL);
    let mut stream = workload.streams[0].clone();
    stream.reshaping = Reshaping::default();
    let arriving = |stream: &SimStream, values| shredded(stream, &rows(values), 0);
    assert_eq!(
        arriving(&stream, vec![Scalar::Null, inexact()]),
        Some(typed.clone())
    );
    assert_eq!(
        arriving(&stream, vec![Scalar::Int(3), inexact()]),
        Some(typed.clone())
    );
    assert_eq!(
        arriving(&stream, vec![Scalar::Decimal("12".to_owned())]),
        Some(Arrival::ExactInt)
    );
    // The filter drops the row of id 2, whose value a float would round.
    stream.reshaping.filter = Some(2);
    assert_eq!(
        arriving(&stream, vec![Scalar::Int(3), inexact()]),
        Some(Arrival::ExactInt)
    );
}

#[test]
fn a_declared_schema_s_integers_are_exact_since_it_holds_no_values() {
    assert_eq!(Arrival::declared(&LogicalType::Int64), Arrival::ExactInt);
    assert_eq!(
        Arrival::declared(&LogicalType::Int64).fits(&LogicalType::Float64),
        Some(true),
        "a hint of floats holds a declared column of integers until values say otherwise"
    );
    assert_eq!(
        Arrival::declared(&LogicalType::Utf8),
        Arrival::Typed(LogicalType::Utf8)
    );
}
