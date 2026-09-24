use super::{Nested, OnUnsupported, Resolved, SchemaPolicy, SchemaSettings, resolve};

/// A8: one resolution over column, table, stream and pipeline.
#[test]
fn policy_inherits_column_table_stream_pipeline() {
    let pipeline = SchemaSettings::new()
        .policy(SchemaPolicy::Freeze)
        .on_unsupported(OnUnsupported::Refuse)
        .nested(Nested::Json);
    let stream = SchemaSettings::new().policy(SchemaPolicy::DiscardRow);
    let table = SchemaSettings::new().on_unsupported(OnUnsupported::VariantColumn);
    let column = SchemaSettings::new().policy(SchemaPolicy::DiscardValue);
    let cases = [
        ([None, None, None, None], Resolved::default()),
        (
            [None, None, None, Some(&pipeline)],
            Resolved {
                policy: SchemaPolicy::Freeze,
                on_unsupported: OnUnsupported::Refuse,
                nested: Nested::Json,
            },
        ),
        (
            [None, None, Some(&stream), Some(&pipeline)],
            Resolved {
                policy: SchemaPolicy::DiscardRow,
                on_unsupported: OnUnsupported::Refuse,
                nested: Nested::Json,
            },
        ),
        (
            [None, Some(&table), Some(&stream), Some(&pipeline)],
            Resolved {
                policy: SchemaPolicy::DiscardRow,
                on_unsupported: OnUnsupported::VariantColumn,
                nested: Nested::Json,
            },
        ),
        (
            [Some(&column), Some(&table), Some(&stream), Some(&pipeline)],
            Resolved {
                policy: SchemaPolicy::DiscardValue,
                on_unsupported: OnUnsupported::VariantColumn,
                nested: Nested::Json,
            },
        ),
    ];
    for (levels, expected) in cases {
        assert_eq!(resolve(levels), expected);
    }
}

#[test]
fn defaults_evolve_add_variant_columns_and_store_nested_values_natively() {
    assert_eq!(
        resolve([None; 4]),
        Resolved {
            policy: SchemaPolicy::Evolve,
            on_unsupported: OnUnsupported::VariantColumn,
            nested: Nested::Native,
        }
    );
}
