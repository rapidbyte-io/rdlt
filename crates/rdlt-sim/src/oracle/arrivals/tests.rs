use rdlt_connector::LogicalType;
use rdlt_testkit::drawn::Scalar;

use super::{Arrival, pushed};

#[test]
fn pushed_scalars_are_inferred_as_json_holds_them() {
    let typed = Arrival::Typed;
    assert_eq!(pushed(&Scalar::Float64(1.5)), typed(LogicalType::Float64));
    assert_eq!(pushed(&Scalar::Int(3)), typed(LogicalType::Int64));
    assert_eq!(pushed(&Scalar::Bool(true)), typed(LogicalType::Bool));
    assert_eq!(
        pushed(&Scalar::Float64(f64::NAN)),
        typed(LogicalType::Utf8),
        "a float JSON cannot hold is pushed as its name"
    );
    assert_eq!(pushed(&Scalar::Null), typed(LogicalType::Null));
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
        let workload = crate::workload::Workload::generate(
            &mut crate::rng::SplitMix64::new(seed),
            crate::swarm::Features::ALL,
        );
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
