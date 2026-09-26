use rdlt_engine::{Nested, OnUnsupported, SchemaPolicy};

use super::{Level, Relaxed, resolve};

const POLICIES: [Option<SchemaPolicy>; 5] = [
    None,
    Some(SchemaPolicy::Evolve),
    Some(SchemaPolicy::Freeze),
    Some(SchemaPolicy::DiscardRow),
    Some(SchemaPolicy::DiscardValue),
];

const ON_UNSUPPORTED: [Option<OnUnsupported>; 3] = [
    None,
    Some(OnUnsupported::VariantColumn),
    Some(OnUnsupported::Refuse),
];

/// Every level of settings with no nested setting.
fn levels() -> Vec<Level> {
    POLICIES
        .into_iter()
        .flat_map(|policy| {
            ON_UNSUPPORTED.into_iter().map(move |on_unsupported| Level {
                policy,
                on_unsupported,
                nested: None,
            })
        })
        .collect()
}

#[test]
fn each_setting_comes_from_the_most_specific_level_setting_it() {
    let column = Level {
        policy: Some(SchemaPolicy::Freeze),
        ..Level::default()
    };
    let stream = Level {
        policy: Some(SchemaPolicy::DiscardRow),
        on_unsupported: Some(OnUnsupported::Refuse),
        nested: None,
    };
    let pipeline = Level {
        nested: Some(Nested::Json),
        ..Level::default()
    };
    let resolved = resolve(&[column, stream, pipeline]);
    assert_eq!(resolved.policy, SchemaPolicy::Freeze);
    assert_eq!(resolved.on_unsupported, OnUnsupported::Refuse);
    assert_eq!(resolved.nested, Nested::Json);
    let defaults = resolve(&[Level::default()]);
    assert_eq!(
        (defaults.policy, defaults.on_unsupported, defaults.nested),
        (
            SchemaPolicy::Evolve,
            OnUnsupported::VariantColumn,
            Nested::Native
        )
    );
}

#[test]
fn relaxed_levels_resolve_as_the_relaxation_of_what_they_resolved_to() {
    // The plan relaxes a stream's own and its columns' levels; the model relaxes what they
    // resolve to. The two must agree for every combination.
    let relaxations = [false, true].into_iter().flat_map(|frozen| {
        [false, true]
            .into_iter()
            .map(move |refused| Relaxed { frozen, refused })
    });
    for relaxed in relaxations {
        for column in levels() {
            for stream in levels() {
                for pipeline in levels() {
                    let mut expected = resolve(&[column, stream, pipeline]);
                    if relaxed.frozen && expected.policy == SchemaPolicy::Freeze {
                        expected.policy = SchemaPolicy::Evolve;
                    }
                    if relaxed.refused && expected.on_unsupported == OnUnsupported::Refuse {
                        expected.on_unsupported = OnUnsupported::VariantColumn;
                    }
                    let planned = [
                        column.relaxed(relaxed),
                        stream.relaxing(pipeline, relaxed),
                        pipeline,
                    ];
                    assert_eq!(
                        resolve(&planned),
                        expected,
                        "{column:?} {stream:?} {pipeline:?} {relaxed:?}"
                    );
                }
            }
        }
    }
}
