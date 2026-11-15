use rdlt_connector::{
    ConnectorErrorKind, Field, Fields, LogicalType, SchemaVersion, TableChange, TablePath,
    TableRef, TableSchema,
};

use super::changed;

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["t"]).expect("a valid table path"),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn structure(names: &[&str]) -> LogicalType {
    let fields = names
        .iter()
        .map(|name| Field::new(*name, LogicalType::Int64, true))
        .collect();
    LogicalType::Struct(Fields::new(fields).unwrap())
}

fn create(logical: LogicalType) -> TableChange {
    let schema = TableSchema::new(vec![Field::new("meta", logical, true)]).unwrap();
    TableChange::Create {
        table: table(),
        schema,
    }
}

#[test]
fn a_declared_type_out_of_canonical_form_is_refused_and_one_in_it_is_held_again() {
    let element = LogicalType::List(Box::new(Field::new("element", LogicalType::Int64, true)));
    for declared in [structure(&["b", "a"]), element] {
        let refused = changed(None, &create(declared.clone())).unwrap_err();
        assert_eq!(refused.kind(), ConnectorErrorKind::Data, "{declared}");
        let created = changed(None, &create(declared.canonical())).unwrap();
        let again = changed(Some(&created), &create(declared.canonical())).unwrap();
        assert_eq!(again, created, "{declared}");
        let widen = TableChange::Widen {
            table: table(),
            column: "meta".into(),
            from: declared.canonical(),
            to: declared.clone(),
        };
        assert!(changed(Some(&created), &widen).is_err(), "{declared}");
    }
}
