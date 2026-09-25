use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{
    DataType, Field as ArrowField, Fields as ArrowFields, TimeUnit as ArrowTimeUnit,
};
use proptest::prelude::*;

use super::{DecimalType, Field, Fields, LogicalType, TimeUnit, TypeError, TypeKind};

fn decimal(precision: u8, scale: u8) -> LogicalType {
    LogicalType::Decimal(DecimalType::new(precision, scale).unwrap())
}

fn structure(fields: &[(&str, LogicalType, bool)]) -> LogicalType {
    let fields = fields
        .iter()
        .map(|(name, ty, nullable)| Field::new(*name, ty.clone(), *nullable))
        .collect();
    LogicalType::Struct(Fields::new(fields).unwrap())
}

fn list(item: LogicalType, nullable: bool) -> LogicalType {
    LogicalType::List(Box::new(Field::new("item", item, nullable)))
}

fn utc(unit: TimeUnit) -> LogicalType {
    LogicalType::Timestamp(unit, Some("UTC".into()))
}

#[test]
fn joins_follow_the_lattice_table() {
    use LogicalType as T;
    let cases = [
        (T::Null, T::Int32, T::Int32),
        (T::Int8, T::Int64, T::Int64),
        (T::Int16, T::Int32, T::Int32),
        (T::Int16, decimal(4, 1), decimal(6, 1)),
        (
            T::Duration(TimeUnit::Second),
            T::Duration(TimeUnit::Millisecond),
            T::Duration(TimeUnit::Millisecond),
        ),
        (T::Json, structure(&[("a", T::Bool, true)]), T::Json),
        (T::Int32, T::Float32, T::Float64),
        (T::Int64, T::Float32, T::Json),
        (T::Float64, T::Int64, T::Json),
        (T::Float32, T::Float64, T::Float64),
        (T::Int32, decimal(10, 2), decimal(12, 2)),
        (decimal(10, 2), decimal(5, 4), decimal(12, 4)),
        (decimal(76, 0), decimal(10, 5), T::Json),
        (T::Float64, decimal(10, 2), T::Json),
        (
            T::Timestamp(TimeUnit::Millisecond, None),
            T::Timestamp(TimeUnit::Microsecond, None),
            T::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            T::Timestamp(TimeUnit::Second, Some("Europe/Warsaw".into())),
            T::Timestamp(TimeUnit::Second, None),
            utc(TimeUnit::Second),
        ),
        (
            T::Date,
            utc(TimeUnit::Microsecond),
            utc(TimeUnit::Microsecond),
        ),
        (
            T::Time(TimeUnit::Second),
            T::Time(TimeUnit::Nanosecond),
            T::Time(TimeUnit::Nanosecond),
        ),
        (T::Utf8, T::Int64, T::Json),
        (T::Uuid, T::Utf8, T::Json),
        (T::Json, T::Bool, T::Json),
        (
            list(T::Int8, false),
            list(T::Int64, true),
            list(T::Int64, true),
        ),
        (list(T::Int8, false), T::Int8, T::Json),
    ];
    for (a, b, expected) in cases {
        assert_eq!(a.join(&b), expected, "{a} ∨ {b}");
        assert_eq!(b.join(&a), expected, "{b} ∨ {a}");
    }
}

#[test]
fn struct_joins_merge_fields_and_make_missing_ones_nullable() {
    let a = structure(&[
        ("id", LogicalType::Int32, false),
        ("name", LogicalType::Utf8, false),
    ]);
    let b = structure(&[
        ("tags", list(LogicalType::Utf8, false), false),
        ("id", LogicalType::Int64, false),
    ]);
    let expected = structure(&[
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
        ("tags", list(LogicalType::Utf8, false), true),
    ]);
    assert_eq!(a.join(&b), expected);
}

#[test]
fn list_joins_do_not_depend_on_arrival_order() {
    let nested_int = list(list(LogicalType::Int8, false), false);
    let nested_wide = list(list(LogicalType::Int64, false), false);
    assert_eq!(nested_int.join(&nested_wide), nested_wide.join(&nested_int));
}

#[test]
fn invalid_decimals_and_duplicate_fields_are_rejected() {
    assert_eq!(
        DecimalType::new(0, 0),
        Err(TypeError::Decimal {
            precision: 0,
            scale: 0
        })
    );
    assert_eq!(
        DecimalType::new(77, 0),
        Err(TypeError::Decimal {
            precision: 77,
            scale: 0
        })
    );
    assert_eq!(
        DecimalType::new(5, 6),
        Err(TypeError::Decimal {
            precision: 5,
            scale: 6
        })
    );
    let duplicate = vec![
        Field::new("a", LogicalType::Int8, false),
        Field::new("a", LogicalType::Utf8, false),
    ];
    assert_eq!(
        Fields::new(duplicate),
        Err(TypeError::DuplicateField {
            name: "a".to_owned()
        })
    );
    let two = Fields::new(vec![
        Field::new("a", LogicalType::Int8, false),
        Field::new("b", LogicalType::Utf8, true),
    ])
    .unwrap();
    assert_eq!((two.len(), two.is_empty()), (2, false));
    assert!(Fields::new(Vec::new()).unwrap().is_empty());
    assert!(serde_json::from_str::<DecimalType>(r#"{"precision":5,"scale":6}"#).is_err());
}

#[test]
fn kinds_drop_type_parameters() {
    assert_eq!(decimal(18, 2).kind(), TypeKind::Decimal);
    assert_eq!(utc(TimeUnit::Second).kind(), TypeKind::Timestamp);
    assert_eq!(list(LogicalType::Utf8, true).kind(), TypeKind::List);
    assert_eq!(LogicalType::Uuid.kind(), TypeKind::Uuid);
}

#[test]
fn types_display_readably() {
    assert_eq!(decimal(18, 2).to_string(), "decimal(18, 2)");
    assert_eq!(LogicalType::Int64.to_string(), "int64");
    assert_eq!(list(LogicalType::Utf8, true).to_string(), "list<utf8>");
    assert_eq!(
        LogicalType::Time(TimeUnit::Second).to_string(),
        "time(Second)"
    );
    assert_eq!(
        utc(TimeUnit::Millisecond).to_string(),
        "timestamp(Millisecond, UTC)"
    );
    assert_eq!(
        LogicalType::Timestamp(TimeUnit::Nanosecond, None).to_string(),
        "timestamp(Nanosecond)"
    );
    assert_eq!(
        LogicalType::Duration(TimeUnit::Microsecond).to_string(),
        "duration(Microsecond)"
    );
    assert_eq!(
        structure(&[("a", LogicalType::Bool, true)]).to_string(),
        "struct<a: bool>"
    );
}

#[test]
fn uuid_and_json_carry_extension_names_in_arrow() {
    let uuid = Field::new("id", LogicalType::Uuid, false).to_arrow();
    assert_eq!(uuid.data_type(), &DataType::FixedSizeBinary(16));
    assert_eq!(
        uuid.metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.uuid")
    );
    let json = Field::new("doc", LogicalType::Json, true).to_arrow();
    assert_eq!(json.data_type(), &DataType::Utf8);
    assert_eq!(
        json.metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.json")
    );
}

#[test]
fn encoded_uuid_and_json_columns_keep_their_extension_types() {
    let encoded = |field: ArrowField, key: DataType| {
        let value = field.data_type().clone();
        field.with_data_type(DataType::Dictionary(Box::new(key), Box::new(value)))
    };
    let uuid = Field::new("id", LogicalType::Uuid, false).to_arrow();
    let json = Field::new("doc", LogicalType::Json, true).to_arrow();
    let run_ends = |field: ArrowField| {
        let value = Arc::new(ArrowField::new("values", field.data_type().clone(), true));
        let ends = Arc::new(ArrowField::new("run_ends", DataType::Int32, false));
        field.with_data_type(DataType::RunEndEncoded(ends, value))
    };
    for (field, logical) in [
        (encoded(uuid.clone(), DataType::Int8), LogicalType::Uuid),
        (encoded(json.clone(), DataType::Int32), LogicalType::Json),
        (run_ends(uuid), LogicalType::Uuid),
        (run_ends(json), LogicalType::Json),
    ] {
        assert_eq!(
            Field::from_arrow(&field).unwrap().logical_type(),
            &logical,
            "{field:?}"
        );
    }
}

#[test]
fn decimals_use_the_narrowest_arrow_decimal() {
    assert_eq!(decimal(38, 2).to_arrow(), DataType::Decimal128(38, 2));
    assert_eq!(decimal(39, 2).to_arrow(), DataType::Decimal256(39, 2));
}

#[test]
fn arrow_encodings_normalize_to_plain_logical_types() {
    let entries = ArrowField::new(
        "entries",
        DataType::Struct(ArrowFields::from(vec![
            ArrowField::new("key", DataType::Utf8, false),
            ArrowField::new("value", DataType::Int32, true),
        ])),
        false,
    );
    let cases = [
        (DataType::LargeUtf8, LogicalType::Utf8),
        (DataType::Utf8View, LogicalType::Utf8),
        (
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            LogicalType::Utf8,
        ),
        (
            DataType::RunEndEncoded(
                Arc::new(ArrowField::new("run_ends", DataType::Int32, false)),
                Arc::new(ArrowField::new("values", DataType::Int64, true)),
            ),
            LogicalType::Int64,
        ),
        (DataType::UInt8, LogicalType::Int16),
        (DataType::UInt32, LogicalType::Int64),
        (DataType::UInt64, decimal(20, 0)),
        (DataType::Float16, LogicalType::Float32),
        (DataType::FixedSizeBinary(8), LogicalType::Binary),
        (DataType::Date64, LogicalType::Date),
        (
            DataType::Time32(ArrowTimeUnit::Millisecond),
            LogicalType::Time(TimeUnit::Millisecond),
        ),
        (
            DataType::LargeList(Arc::new(ArrowField::new("element", DataType::Int8, true))),
            list(LogicalType::Int8, true),
        ),
        (
            DataType::Map(Arc::new(entries), false),
            list(
                structure(&[
                    ("key", LogicalType::Utf8, false),
                    ("value", LogicalType::Int32, true),
                ]),
                false,
            ),
        ),
    ];
    for (data_type, expected) in cases {
        let field = Field::from_arrow(&ArrowField::new("f", data_type.clone(), true)).unwrap();
        assert_eq!(field.logical_type(), &expected, "{data_type}");
    }
}

#[test]
fn arrow_types_without_a_logical_equivalent_are_refused() {
    let interval = ArrowField::new(
        "span",
        DataType::Interval(arrow_schema::IntervalUnit::DayTime),
        true,
    );
    let error = Field::from_arrow(&interval).unwrap_err();
    assert_eq!(error.field, "span");
    let negative_scale = ArrowField::new("n", DataType::Decimal128(10, -2), true);
    assert!(Field::from_arrow(&negative_scale).is_err());
    let unknown_extension =
        ArrowField::new("u", DataType::FixedSizeBinary(16), false).with_metadata(HashMap::from([
            ("ARROW:extension:name".to_owned(), "other".to_owned()),
        ]));
    assert_eq!(
        Field::from_arrow(&unknown_extension)
            .unwrap()
            .logical_type(),
        &LogicalType::Binary
    );
}

fn unit() -> impl Strategy<Value = TimeUnit> {
    prop_oneof![
        Just(TimeUnit::Second),
        Just(TimeUnit::Millisecond),
        Just(TimeUnit::Microsecond),
        Just(TimeUnit::Nanosecond),
    ]
}

fn logical_type() -> impl Strategy<Value = LogicalType> {
    use LogicalType as T;
    let zone = prop_oneof![
        Just(None),
        Just(Some(Arc::<str>::from("UTC"))),
        Just(Some(Arc::<str>::from("Asia/Tokyo")))
    ];
    let leaf = prop_oneof![
        Just(T::Null),
        Just(T::Bool),
        Just(T::Int8),
        Just(T::Int16),
        Just(T::Int32),
        Just(T::Int64),
        Just(T::Float32),
        Just(T::Float64),
        (1u8..=76)
            .prop_flat_map(|p| (Just(p), 0..=p))
            .prop_map(|(p, s)| decimal(p, s)),
        Just(T::Utf8),
        Just(T::Binary),
        Just(T::Date),
        unit().prop_map(T::Time),
        (unit(), zone).prop_map(|(u, z)| T::Timestamp(u, z)),
        unit().prop_map(T::Duration),
        Just(T::Uuid),
        Just(T::Json),
    ];
    leaf.prop_recursive(3, 16, 3, |inner| {
        prop_oneof![
            (inner.clone(), any::<bool>()).prop_map(|(item, nullable)| list(item, nullable)),
            proptest::collection::btree_map(
                prop_oneof![Just("a"), Just("b"), Just("c")],
                (inner, any::<bool>()),
                0..3
            )
            .prop_map(|fields| {
                let fields = fields
                    .into_iter()
                    .map(|(name, (ty, nullable))| Field::new(name, ty, nullable))
                    .collect();
                LogicalType::Struct(Fields::new(fields).unwrap())
            }),
        ]
    })
}

proptest! {
    #[test]
    fn join_is_commutative(a in logical_type(), b in logical_type()) {
        prop_assert_eq!(a.join(&b), b.join(&a));
    }

    #[test]
    fn join_is_associative(a in logical_type(), b in logical_type(), c in logical_type()) {
        prop_assert_eq!(a.join(&b).join(&c), a.join(&b.join(&c)));
    }

    #[test]
    fn join_is_idempotent_with_null_as_identity_and_json_on_top(a in logical_type()) {
        prop_assert_eq!(a.join(&a), a.clone());
        prop_assert_eq!(LogicalType::Null.join(&a), a.clone());
        prop_assert_eq!(LogicalType::Json.join(&a), LogicalType::Json);
    }

    #[test]
    fn a_join_absorbs_both_inputs(a in logical_type(), b in logical_type()) {
        let joined = a.join(&b);
        prop_assert_eq!(joined.join(&a), joined.clone());
        prop_assert_eq!(joined.join(&b), joined);
    }

    #[test]
    fn logical_types_round_trip_through_arrow(a in logical_type(), nullable in any::<bool>()) {
        let field = Field::new("f", a, nullable);
        prop_assert_eq!(Field::from_arrow(&field.to_arrow()).unwrap(), field);
    }

    #[test]
    fn logical_types_round_trip_through_json(a in logical_type()) {
        let json = serde_json::to_string(&a).unwrap();
        prop_assert_eq!(serde_json::from_str::<LogicalType>(&json).unwrap(), a);
    }
}

#[test]
fn a_written_column_stored_as_another_type_names_its_logical_type() {
    let named = |value: &str| {
        let metadata = HashMap::from([(super::LOGICAL_TYPE_KEY.to_owned(), value.to_owned())]);
        ArrowField::new("day", DataType::Utf8, true).with_metadata(metadata)
    };
    let date = serde_json::to_string(&LogicalType::Date).unwrap();
    assert_eq!(Field::lowered_from(&named(&date)), Some(LogicalType::Date));
    assert_eq!(Field::lowered_from(&named("not json")), None);
    let plain = ArrowField::new("day", DataType::Utf8, true);
    assert_eq!(Field::lowered_from(&plain), None);
}
