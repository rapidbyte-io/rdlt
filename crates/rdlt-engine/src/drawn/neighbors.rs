//! Batches whose columns change type from batch to batch the way sources' do: each column keeps a
//! base type and takes, in each batch, it, a neighbor the lattice joins it with, or another type,
//! so tables widen, gain variants and convert values in every way the lattice allows.

use std::sync::Arc;

use proptest::prelude::*;
use rdlt_connector::{DecimalType, Field, Fields, LogicalType, TimeUnit};

use super::values::{shape, value};
use super::{Drawn, Encoding, Shape};

/// The source columns batches draw from.
const NAMES: [&str; 3] = ["a", "b", "c"];

const UNITS: [TimeUnit; 4] = [
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

fn plain(logical: LogicalType, children: Vec<Shape>) -> Shape {
    Shape {
        logical,
        encoding: Encoding::Plain,
        children,
    }
}

fn decimal(precision: u8, scale: u8) -> LogicalType {
    LogicalType::Decimal(DecimalType::new(precision, scale).expect("a valid decimal"))
}

/// A type the lattice joins `shape`'s with without widening it to `Json`: a wider integer, a float
/// or decimal holding it, another unit or zone, a date's timestamp, a struct with a field changed,
/// added or removed, or a list of an item's neighbor.
pub(crate) fn neighbor(shape: &Shape) -> BoxedStrategy<Shape> {
    use LogicalType as T;
    let leaf = |types: Vec<LogicalType>| {
        proptest::sample::select(types)
            .prop_map(|logical| plain(logical, Vec::new()))
            .boxed()
    };
    let unit = || proptest::sample::select(UNITS.to_vec());
    let zone = proptest::sample::select(vec![
        None,
        Some("UTC"),
        Some("+05:30"),
        Some("America/Havana"),
        Some("America/Sao_Paulo"),
        Some("Europe/Berlin"),
        Some("Asia/Tehran"),
    ]);
    match &shape.logical {
        T::Int8 => leaf(vec![
            T::Int16,
            T::Int32,
            T::Int64,
            T::Float64,
            decimal(5, 2),
        ]),
        T::Int16 => leaf(vec![T::Int32, T::Int64, T::Float64, decimal(7, 1)]),
        T::Int32 => leaf(vec![T::Int64, T::Float64, decimal(12, 2)]),
        T::Int64 => leaf(vec![decimal(19, 0), decimal(25, 4)]),
        T::Float32 => leaf(vec![T::Float64]),
        T::Decimal(d) => {
            let wider = decimal(
                (d.precision() + 3).min(76),
                (d.scale() + 1).min(d.precision() + 3).min(76),
            );
            leaf(vec![wider, T::Int32, T::Int64])
        }
        T::Date | T::Timestamp(..) => (unit(), zone)
            .prop_map(|(unit, zone)| plain(T::Timestamp(unit, zone.map(Arc::from)), Vec::new()))
            .boxed(),
        T::Time(_) => unit()
            .prop_map(|unit| plain(T::Time(unit), Vec::new()))
            .boxed(),
        T::Duration(_) => unit()
            .prop_map(|unit| plain(T::Duration(unit), Vec::new()))
            .boxed(),
        T::Struct(fields) => structure(fields, &shape.children),
        T::List(item) => {
            let nullable = item.is_nullable();
            neighbor(&shape.children[0])
                .prop_map(move |child| {
                    let item = Field::new("item", child.logical.clone(), nullable);
                    plain(T::List(Box::new(item)), vec![child])
                })
                .boxed()
        }
        _ => Just(shape.clone()).boxed(),
    }
}

/// A struct like `fields`, of `children`, with one field changed to a neighbor, one added or one
/// removed.
fn structure(fields: &Fields, children: &[Shape]) -> BoxedStrategy<Shape> {
    let members: Vec<(Field, Shape)> = fields
        .iter()
        .cloned()
        .zip(children.iter().cloned())
        .collect();
    let rebuild = |members: Vec<(Field, Shape)>| {
        let (fields, children): (Vec<Field>, Vec<Shape>) = members.into_iter().unzip();
        plain(
            LogicalType::Struct(Fields::new(fields).expect("distinct names")),
            children,
        )
    };
    let count = members.len();
    let changed = {
        let members = members.clone();
        (0..count)
            .prop_flat_map(move |index| {
                let members = members.clone();
                neighbor(&members[index].1).prop_map(move |child| {
                    let mut members = members.clone();
                    let field = &members[index].0;
                    members[index] = (
                        Field::new(field.name(), child.logical.clone(), field.is_nullable()),
                        child,
                    );
                    rebuild(members)
                })
            })
            .boxed()
    };
    let added = {
        let members = members.clone();
        shape(0)
            .prop_map(move |child| {
                let mut members = members.clone();
                if !members.iter().any(|(field, _)| field.name() == "w") {
                    members.push((Field::new("w", child.logical.clone(), true), child));
                }
                rebuild(members)
            })
            .boxed()
    };
    let removed = {
        let members = members.clone();
        (0..count)
            .prop_map(move |index| {
                let mut members = members.clone();
                if members.len() > 1 {
                    members.remove(index);
                }
                rebuild(members)
            })
            .boxed()
    };
    prop_oneof![2 => changed, 1 => added, 1 => removed].boxed()
}

/// One to four batches over [`NAMES`]: each batch holds some of the columns, each of its base
/// type, a neighbor of it, or now and then another type.
pub(crate) fn batches() -> impl Strategy<Value = Vec<Drawn>> {
    proptest::collection::vec(shape(2), NAMES.len()).prop_flat_map(|bases| {
        let batch = {
            let bases = bases.clone();
            proptest::sample::subsequence((0..NAMES.len()).collect::<Vec<_>>(), 1..=NAMES.len())
                .prop_flat_map(move |columns| {
                    let shapes: Vec<BoxedStrategy<Shape>> = columns
                        .iter()
                        .map(|column| {
                            let base = bases[*column].clone();
                            prop_oneof![
                                2 => Just(base.clone()),
                                2 => neighbor(&base),
                                1 => shape(2),
                            ]
                            .boxed()
                        })
                        .collect();
                    (Just(columns), shapes)
                })
                .prop_flat_map(|(columns, shapes)| {
                    let row: Vec<_> = shapes.iter().map(|shape| value(shape, true)).collect();
                    let named: Vec<(String, Shape)> = columns
                        .iter()
                        .map(|column| NAMES[*column].to_owned())
                        .zip(shapes)
                        .collect();
                    (Just(named), proptest::collection::vec(row, 0..6))
                })
        };
        proptest::collection::vec(batch, 1..5)
    })
}
