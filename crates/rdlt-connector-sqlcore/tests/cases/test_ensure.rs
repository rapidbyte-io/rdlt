//! The ensure-step pins, lifted from the module's former inline tests.

#[cfg(test)]
mod tests {
    use rdlt_connector::WriteMode;
    use rdlt_connector::core::TableSchema;
    use rdlt_connector::core::{ColumnDef, ColumnType, LogicalType, Provenance, TableName};
    use rdlt_connector_sqlcore::ensure::*;
    use rdlt_connector_sqlcore::options::DestOptions;
    use rdlt_connector_sqlcore::options::MergeStrategy;
    use rdlt_connector_sqlcore::protocol::FullLoadPublish;

    fn col(name: &str, scalar: LogicalType) -> ColumnDef {
        ColumnDef {
            name: name.to_owned(),
            column_type: ColumnType::Scalar { scalar },
            nullable: true,
            provenance: Provenance::Inferred,
        }
    }

    fn schema(columns: Vec<ColumnDef>) -> TableSchema {
        TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns,
        }
    }

    fn merge() -> WriteMode {
        WriteMode::Merge {
            key: vec!["id".to_owned()],
        }
    }

    #[test]
    fn append_on_a_direct_destination_plans_one_leg() {
        let plan = table_plan(
            &schema(vec![col("id", LogicalType::Int64)]),
            &WriteMode::Append,
            FullLoadPublish::DirectToTarget,
            None,
        );
        assert_eq!(
            plan,
            vec![
                EnsureStep::Table { leg: Leg::Target },
                EnsureStep::Column {
                    leg: Leg::Target,
                    column: 0
                },
            ]
        );
    }

    #[test]
    fn append_on_a_staged_destination_plans_both_legs() {
        // The same mode, a different publish path — and the stage leg appears.
        // This is the rule the commit planner also consults; they must agree.
        let plan = table_plan(
            &schema(vec![col("id", LogicalType::Int64)]),
            &WriteMode::Append,
            FullLoadPublish::Staged,
            None,
        );
        assert_eq!(
            plan.iter()
                .filter(|s| matches!(s, EnsureStep::Table { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn merge_always_stages_whatever_the_publish_path() {
        for publish in [FullLoadPublish::DirectToTarget, FullLoadPublish::Staged] {
            assert!(stages(&merge(), publish), "merge stages under {publish:?}");
        }
    }

    #[test]
    fn a_changed_type_plans_a_widen_directly_after_its_column() {
        let before = schema(vec![col("id", LogicalType::Int64)]);
        let after = schema(vec![col("id", LogicalType::Utf8)]);
        let plan = table_plan(
            &after,
            &WriteMode::Append,
            FullLoadPublish::DirectToTarget,
            Some(&before),
        );
        assert_eq!(
            plan,
            vec![
                EnsureStep::Table { leg: Leg::Target },
                EnsureStep::Column {
                    leg: Leg::Target,
                    column: 0
                },
                EnsureStep::Widen {
                    leg: Leg::Target,
                    column: 0
                },
            ]
        );
    }

    #[test]
    fn an_unchanged_type_plans_no_widen() {
        let same = schema(vec![col("id", LogicalType::Int64)]);
        let plan = table_plan(
            &same,
            &WriteMode::Append,
            FullLoadPublish::DirectToTarget,
            Some(&same),
        );
        assert!(!plan.iter().any(|s| matches!(s, EnsureStep::Widen { .. })));
    }

    #[test]
    fn scd2_plans_both_validity_columns_before_any_index() {
        let options = DestOptions {
            merge_strategy: Some(MergeStrategy::Scd2),
            ..DestOptions::default()
        };
        let plan = merge_plan(
            &options,
            &schema(vec![col("id", LogicalType::Int64)]),
            &merge(),
        )
        .expect("valid options");
        let from = plan
            .iter()
            .position(|s| matches!(s, EnsureStep::Validity(Validity::From)))
            .expect("valid_from");
        let to = plan
            .iter()
            .position(|s| matches!(s, EnsureStep::Validity(Validity::To)))
            .expect("valid_to");
        let first_index = plan
            .iter()
            .position(|s| matches!(s, EnsureStep::Index(_)))
            .unwrap_or(usize::MAX);
        assert!(from < to && to < first_index, "{plan:?}");
    }

    #[test]
    fn a_non_merge_mode_plans_nothing_but_still_validates() {
        let plan = merge_plan(
            &DestOptions::default(),
            &schema(vec![col("id", LogicalType::Int64)]),
            &WriteMode::Append,
        )
        .expect("default options are valid");
        assert!(plan.is_empty());

        let refused = DestOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            ..DestOptions::default()
        };
        merge_plan(
            &refused,
            &schema(vec![col("id", LogicalType::Int64)]),
            &WriteMode::Append,
        )
        .expect_err("a merge strategy under Append is a config error");
    }
}
