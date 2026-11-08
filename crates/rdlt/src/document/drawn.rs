//! Drawn documents, for the round trip: every key a document takes but code transforms and SQL,
//! whose engine transforms compare by identity, and masks by key, which need a store.

use std::time::Duration;

use proptest::collection::{btree_map, vec};
use proptest::option;
use proptest::prelude::*;

use crate::{
    Column, Commit, Connector, Document, Hint, Literal, MaskStep, MaskWith, Nested, OnUnsupported,
    Resources, Retry, SchemaPolicy, SchemaRules, Side, Stream, TransformStep, Until, WriteMode,
};

fn name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,8}"
}

fn policy() -> impl Strategy<Value = SchemaPolicy> {
    prop_oneof![
        Just(SchemaPolicy::Evolve),
        Just(SchemaPolicy::Freeze),
        Just(SchemaPolicy::DiscardRow),
        Just(SchemaPolicy::DiscardValue),
    ]
}

fn nested() -> impl Strategy<Value = Nested> {
    prop_oneof![
        Just(Nested::Native),
        Just(Nested::Json),
        (1_u8..=8).prop_map(|max_depth| Nested::Normalize { max_depth }),
    ]
}

fn rules() -> impl Strategy<Value = SchemaRules> {
    let unsupported = prop_oneof![
        Just(OnUnsupported::VariantColumn),
        Just(OnUnsupported::Refuse)
    ];
    (option::of(policy()), option::of(unsupported)).prop_map(|(policy, unsupported)| {
        let rules = SchemaRules::new();
        let rules = match policy {
            Some(policy) => rules.policy(policy),
            None => rules,
        };
        match unsupported {
            Some(unsupported) => rules.on_unsupported(unsupported),
            None => rules,
        }
    })
}

fn step() -> impl Strategy<Value = TransformStep> {
    let constant = any::<i32>().prop_map(|value| Literal::Integer(i64::from(value)));
    prop_oneof![
        btree_map(name(), name(), 1..3).prop_map(TransformStep::rename),
        vec(name(), 1..3)
            .prop_map(|columns| { TransformStep::Mask(MaskStep::new(columns, MaskWith::Null)) }),
        btree_map(name(), constant, 1..3).prop_map(TransformStep::constant),
    ]
}

fn stream() -> impl Strategy<Value = Stream> {
    (
        name(),
        option::of(rules()),
        option::of(nested()),
        btree_map(name(), option::of(policy()), 0..3),
        vec(name(), 0..3),
        vec(step(), 0..3),
        option::of(vec(name(), 0..3)),
    )
        .prop_map(|(name, rules, nested, columns, exclude, steps, included)| {
            let mut stream = Stream::new(name).exclude(exclude);
            stream.included = included;
            if let Some(rules) = rules {
                stream = stream.schema(rules);
            }
            if let Some(nested) = nested {
                stream = stream.nested(nested);
            }
            for (column, policy) in columns {
                let entry = Column::new().hint(Hint::Int64);
                stream = stream.column(
                    column,
                    match policy {
                        Some(policy) => entry.policy(policy),
                        None => entry,
                    },
                );
            }
            steps.into_iter().fold(stream, Stream::transform)
        })
}

/// A drawn document of the reference memory connectors.
pub(crate) fn documents() -> impl Strategy<Value = Document> {
    (
        vec(stream(), 1..4),
        option::of(rules()),
        option::of(prop_oneof![
            Just(WriteMode::Append),
            Just(WriteMode::Replace)
        ]),
        option::of((1_u64..=3_600).prop_map(|secs| Commit::new().every(Duration::from_secs(secs)))),
        option::of((64_u64..=1_024).prop_map(|mib| Resources::new().memory(mib << 20).writers(4))),
        option::of((1_u32..=9).prop_map(|attempts| Retry::new().max_attempts(attempts))),
        prop_oneof![
            Just(Until::Exhausted),
            Just(Until::Forever),
            (1_u64..=600).prop_map(|secs| Until::For(Duration::from_secs(secs))),
        ],
    )
        .prop_map(|(streams, rules, write, commit, resources, retry, until)| {
            let memory = || Side::new(Connector::new("io.rapidbyte.memory"));
            let mut document = Document::new("drawn", memory(), memory());
            document.streams = streams;
            if let Some(rules) = rules {
                document.defaults = document.defaults.schema(rules);
            }
            if let Some(write) = write {
                document.defaults = document.defaults.write(write);
            }
            document.commit = commit;
            document.resources = resources.unwrap_or_default();
            document.retry = retry;
            document.until = until;
            document
        })
}
