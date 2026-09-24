use super::{Capabilities, CommitKind, SchemaChanges};
use crate::types::{DecimalType, Field, Fields, LogicalType, TimeUnit, TypeKind};

#[test]
fn minimal_capabilities_are_append_only_scalars() {
    let minimal = Capabilities::minimal();
    assert_eq!(minimal.commit, CommitKind::Transactional);
    assert!(minimal.write_modes.append && !minimal.write_modes.merge);
    assert!(minimal.types.contains(&TypeKind::Int64));
    assert!(!minimal.types.contains(&TypeKind::Struct) && !minimal.types.contains(&TypeKind::Json));
    assert_eq!(minimal.max_parallel_writers.get(), 1);
}

#[test]
fn capabilities_round_trip_through_json() {
    let mut capabilities = Capabilities::minimal();
    capabilities
        .schema_changes
        .widenings
        .insert((TypeKind::Int32, TypeKind::Int64));
    capabilities
        .identifiers
        .reserved
        .insert("select".to_owned());
    let json = serde_json::to_string(&capabilities).unwrap();
    assert_eq!(
        serde_json::from_str::<Capabilities>(&json).unwrap(),
        capabilities
    );
}

#[test]
fn all_schema_changes_cover_every_widening_the_lattice_makes() {
    use LogicalType as T;
    let decimal = |p, s| T::Decimal(DecimalType::new(p, s).unwrap());
    let structure =
        |name: &str, t: T| T::Struct(Fields::new(vec![Field::new(name, t, true)]).unwrap());
    let list = |t: T| T::List(Box::new(Field::new("item", t, true)));
    let samples = [
        T::Bool,
        T::Int8,
        T::Int16,
        T::Int32,
        T::Int64,
        T::Float32,
        T::Float64,
        decimal(10, 2),
        decimal(30, 0),
        T::Utf8,
        T::Date,
        T::Timestamp(TimeUnit::Millisecond, None),
        T::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        T::Time(TimeUnit::Second),
        T::Time(TimeUnit::Nanosecond),
        T::Duration(TimeUnit::Second),
        T::Duration(TimeUnit::Millisecond),
        structure("a", T::Int32),
        structure("b", T::Utf8),
        list(T::Int32),
        list(T::Int64),
    ];
    let all = SchemaChanges::all();
    assert!(all.add_column);
    let mut made = std::collections::BTreeSet::new();
    for a in &samples {
        for b in &samples {
            let joined = a.join(b);
            if joined != *a && joined != T::Json {
                made.insert((a.kind(), joined.kind()));
            }
        }
    }
    assert_eq!(
        all.widenings, made,
        "exactly the widenings the lattice makes"
    );
    assert!(all.widens(TypeKind::Int8, TypeKind::Int16));
    assert!(!all.widens(TypeKind::Int64, TypeKind::Float64));
    assert!(!SchemaChanges::default().widens(TypeKind::Int8, TypeKind::Int16));
}
