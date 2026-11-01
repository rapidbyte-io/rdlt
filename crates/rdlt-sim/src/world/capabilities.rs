//! The destination a world draws: the types, nested values and widenings it stores, whether it
//! adds columns, how many writers it takes, and the identifier rules it names things under.

use std::collections::BTreeSet;

use rdlt_connector::{Capabilities, IdentifierCase, IdentifierChars, SchemaChanges, TypeKind};

use crate::rng::SplitMix64;
use crate::swarm::Features;

/// Destination capabilities drawn from `rng`: which types it stores natively, whether it stores
/// JSON, which widenings and nested types it stores, whether it adds columns, and the identifier
/// rules it names tables and columns under.
pub(super) fn capabilities(rng: &mut SplitMix64, features: Features) -> Capabilities {
    let mut capabilities = Capabilities::minimal();
    if features.narrow {
        // Text always among them, every other type stored natively or as text.
        capabilities
            .types
            .retain(|kind| *kind == TypeKind::Utf8 || rng.chance(500));
    }
    capabilities.write_modes.replace = true;
    capabilities.write_modes.merge = true;
    capabilities.drop_tables = true;
    if rng.chance(700) {
        capabilities.nested.json = true;
        capabilities.types.insert(TypeKind::Json);
    }
    if rng.chance(500) {
        capabilities.nested.structs = true;
        capabilities.types.insert(TypeKind::Struct);
    }
    if rng.chance(500) {
        capabilities.nested.lists = true;
        capabilities.types.insert(TypeKind::List);
    }
    if rng.chance(500) {
        capabilities.types.insert(TypeKind::Uuid);
    }
    let all = SchemaChanges::all();
    capabilities.schema_changes.widenings = match rng.below(3) {
        0 => all.widenings,
        1 => BTreeSet::new(),
        _ => all
            .widenings
            .into_iter()
            .filter(|_| rng.chance(500))
            .collect(),
    };
    capabilities.identifiers.case = match rng.below(3) {
        0 => IdentifierCase::Preserve,
        1 => IdentifierCase::Lower,
        _ => IdentifierCase::Upper,
    };
    let max_len = [63, 32, 16][usize::try_from(rng.below(3)).unwrap_or(0)];
    capabilities.identifiers.max_len =
        std::num::NonZeroU16::new(max_len).expect("identifier lengths are positive");
    if rng.chance(300) {
        capabilities.identifiers.reserved.insert("value".to_owned());
    }
    if features.settings && rng.chance(250) {
        capabilities.schema_changes.add_column = false;
    }
    if features.identifiers {
        identifiers(rng, &mut capabilities);
    }
    capabilities.max_parallel_writers =
        std::num::NonZeroU16::new(u16::try_from(1 + rng.below(4)).unwrap_or(1))
            .expect("writer counts are positive");
    capabilities
}

/// Identifier rules beyond the common ones: any characters, more reserved words, and reserved
/// table prefixes.
fn identifiers(rng: &mut SplitMix64, capabilities: &mut Capabilities) {
    let rules = &mut capabilities.identifiers;
    if rng.chance(500) {
        rules.chars = IdentifierChars::Any;
    }
    for word in ["id", "key", "offset", "_rdlt_load_epoch", "d0"] {
        if rng.chance(250) {
            rules.reserved.insert(word.to_owned());
        }
    }
    for prefix in ["s", "S0", "_s", "tmp_"] {
        if rng.chance(300) {
            rules.reserved_table_prefixes.insert(prefix.to_owned());
        }
    }
}
