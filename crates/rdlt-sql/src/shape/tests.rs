use crate::{Sql, SqlErrorKind};

/// Types a cast may name, as SQL spells them.
const LISTED: &[&str] = &[
    "BOOLEAN",
    "BOOL",
    "TINYINT",
    "SMALLINT",
    "INT2",
    "INT",
    "INTEGER",
    "INT4",
    "BIGINT",
    "INT8",
    "TINYINT UNSIGNED",
    "SMALLINT UNSIGNED",
    "INT UNSIGNED",
    "INTEGER UNSIGNED",
    "BIGINT UNSIGNED",
    "REAL",
    "FLOAT",
    "FLOAT4",
    "DOUBLE",
    "DOUBLE PRECISION",
    "FLOAT8",
    "DECIMAL",
    "DECIMAL(10, 2)",
    "NUMERIC(38, 4)",
    "CHAR",
    "VARCHAR",
    "TEXT",
    "STRING",
    "DATE",
    "TIME",
    "TIME WITHOUT TIME ZONE",
    "TIMESTAMP",
    "TIMESTAMP(0)",
    "TIMESTAMP(3)",
    "TIMESTAMP(6)",
    "TIMESTAMP(9)",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP(3) WITHOUT TIME ZONE",
];

/// Other names of listed types, which DataFusion does not plan as they are written.
const SPELLED: &[&str] = &[
    "CHARACTER",
    "CHARACTER(3)",
    "CHARACTER VARYING(10)",
    "CHAR VARYING(10)",
    "NVARCHAR(10)",
    "CHARACTER LARGE OBJECT",
    "CHAR LARGE OBJECT",
    "CLOB",
    "TINYTEXT",
    "MEDIUMTEXT",
    "LONGTEXT",
    "DEC",
    "DEC(10, 2)",
    "DATETIME",
    "DATETIME(6)",
    "TIMESTAMP_NTZ",
    "DATETIME64(3)",
    "DATE32",
    "INT16",
    "INT32",
    "INT64",
    "UINT8",
    "UINT16",
    "UINT32",
    "UINT64",
    "UTINYINT",
    "USMALLINT",
    "UBIGINT",
    "FLOAT32",
    "FLOAT64",
    "SIGNED",
    "SIGNED INTEGER",
    "UNSIGNED",
    "UNSIGNED INTEGER",
];

#[test]
fn a_cast_to_a_listed_scalar_type_parses_however_it_is_spelled() {
    for listed in LISTED.iter().chain(SPELLED) {
        for text in [
            format!("SELECT CAST(n AS {listed}) AS x FROM input"),
            format!("SELECT TRY_CAST(n AS {listed}) AS x FROM input"),
            format!("SELECT n::{listed} AS x FROM input"),
            format!("SELECT n FROM input WHERE CAST(n AS {listed}) IS NOT NULL"),
        ] {
            assert!(Sql::parse(&text).is_ok(), "{text}");
        }
    }
}

#[test]
fn a_cast_to_any_other_type_is_refused_naming_no_type() {
    let others = [
        "INT[]",
        "INT[3]",
        "ARRAY<INT>",
        "STRUCT<a INT>",
        "MAP(INT, INT)",
        "INTERVAL",
        "JSON",
        "UUID",
        "DATETIME(5)",
        "DATETIME64(3, 'UTC')",
        "TIMESTAMP(5)",
        "TIMESTAMP(1) WITH TIME ZONE",
        "TIME(2)",
        "TIME(0)",
        "TIME WITH TIME ZONE",
        "TIMETZ",
        "BIGNUMERIC",
        "MEDIUMINT",
        "BYTEA",
        "secret_type",
    ];
    for other in others {
        let text = format!("SELECT CAST(n AS {other}) AS x FROM input");
        let refused = Sql::parse(&text).unwrap_err();
        assert_eq!(refused.kind(), SqlErrorKind::Shape, "{text}: {refused}");
        assert!(!refused.to_string().contains("secret"), "{refused}");
    }
}
