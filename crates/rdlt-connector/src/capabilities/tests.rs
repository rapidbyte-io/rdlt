use super::{Capabilities, CommitKind};
use crate::types::TypeKind;

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
