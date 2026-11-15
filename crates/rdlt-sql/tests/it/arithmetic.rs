//! Integer arithmetic and casts against a model of their own rather than DataFusion's executor,
//! which wraps where a SQL transform fails: each value exact, computed over `i128`, and a row
//! failing where its exact value lies beyond the type its operation gives, for every integer
//! width, signed and unsigned, at the edges of each type and between them.

use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{
    ArrowPrimitiveType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use arrow_select::filter::filter_record_batch;
use proptest::collection::vec;
use proptest::option;
use proptest::prelude::*;
use rdlt_engine::Transform;
use rdlt_sql::Sql;

/// An integer type a column holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Int {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
}

/// Every integer type, the narrowest first and the signed first of each width.
const INTS: [Int; 8] = [
    Int::I8,
    Int::U8,
    Int::I16,
    Int::U16,
    Int::I32,
    Int::U32,
    Int::I64,
    Int::U64,
];

impl Int {
    fn signed(self) -> bool {
        matches!(self, Self::I8 | Self::I16 | Self::I32 | Self::I64)
    }

    fn bits(self) -> u32 {
        match self {
            Self::I8 | Self::U8 => 8,
            Self::I16 | Self::U16 => 16,
            Self::I32 | Self::U32 => 32,
            Self::I64 | Self::U64 => 64,
        }
    }

    fn least(self) -> i128 {
        if self.signed() {
            -(1 << (self.bits() - 1))
        } else {
            0
        }
    }

    fn greatest(self) -> i128 {
        let bits = if self.signed() {
            self.bits() - 1
        } else {
            self.bits()
        };
        (1 << bits) - 1
    }

    fn holds(self, value: i128) -> bool {
        (self.least()..=self.greatest()).contains(&value)
    }

    /// The narrowest type holding every value of both, which an operator on the two gives: none
    /// where only a decimal does, as for `BIGINT UNSIGNED` beside a signed type.
    fn common(self, other: Self) -> Option<Self> {
        let both = [
            self.least(),
            self.greatest(),
            other.least(),
            other.greatest(),
        ];
        INTS.into_iter()
            .find(|int| both.iter().all(|value| int.holds(*value)))
    }

    /// The name a query casts to it by.
    fn name(self) -> &'static str {
        match self {
            Self::I8 => "TINYINT",
            Self::U8 => "TINYINT UNSIGNED",
            Self::I16 => "SMALLINT",
            Self::U16 => "SMALLINT UNSIGNED",
            Self::I32 => "INT",
            Self::U32 => "INT UNSIGNED",
            Self::I64 => "BIGINT",
            Self::U64 => "BIGINT UNSIGNED",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::I8 => DataType::Int8,
            Self::U8 => DataType::UInt8,
            Self::I16 => DataType::Int16,
            Self::U16 => DataType::UInt16,
            Self::I32 => DataType::Int32,
            Self::U32 => DataType::UInt32,
            Self::I64 => DataType::Int64,
            Self::U64 => DataType::UInt64,
        }
    }

    /// Its least and greatest values, those next to them, zero, one and, where it is signed,
    /// minus one.
    fn edges(self) -> Vec<i128> {
        let (least, greatest) = (self.least(), self.greatest());
        let mut edges = vec![least, least + 1, 0, 1, greatest - 1, greatest];
        if self.signed() {
            edges.push(-1);
        }
        edges.sort_unstable();
        edges.dedup();
        edges
    }

    /// A value of it, an edge drawn as often as any other.
    fn drawn(self) -> impl Strategy<Value = i128> {
        prop_oneof![
            proptest::sample::select(self.edges()),
            self.least()..=self.greatest()
        ]
    }

    /// A column of `values`, each within it.
    fn array(self, values: &[Option<i128>]) -> ArrayRef {
        match self {
            Self::I8 => Arc::new(Int8Array::from(narrowed::<i8>(values))),
            Self::U8 => Arc::new(UInt8Array::from(narrowed::<u8>(values))),
            Self::I16 => Arc::new(Int16Array::from(narrowed::<i16>(values))),
            Self::U16 => Arc::new(UInt16Array::from(narrowed::<u16>(values))),
            Self::I32 => Arc::new(Int32Array::from(narrowed::<i32>(values))),
            Self::U32 => Arc::new(UInt32Array::from(narrowed::<u32>(values))),
            Self::I64 => Arc::new(Int64Array::from(narrowed::<i64>(values))),
            Self::U64 => Arc::new(UInt64Array::from(narrowed::<u64>(values))),
        }
    }
}

/// `values`, each as the integer type `T`.
fn narrowed<T: TryFrom<i128>>(values: &[Option<i128>]) -> Vec<Option<T>>
where
    T::Error: Debug,
{
    let narrowed = values
        .iter()
        .map(|value| value.map(|value| T::try_from(value).expect("within its type")));
    narrowed.collect()
}

/// The values of `array`, of any integer type.
fn widened(array: &dyn Array) -> Vec<Option<i128>> {
    match array.data_type() {
        DataType::Int8 => values::<Int8Type>(array),
        DataType::UInt8 => values::<UInt8Type>(array),
        DataType::Int16 => values::<Int16Type>(array),
        DataType::UInt16 => values::<UInt16Type>(array),
        DataType::Int32 => values::<Int32Type>(array),
        DataType::UInt32 => values::<UInt32Type>(array),
        DataType::Int64 => values::<Int64Type>(array),
        DataType::UInt64 => values::<UInt64Type>(array),
        other => panic!("{other} is no integer type"),
    }
}

fn values<T: ArrowPrimitiveType>(array: &dyn Array) -> Vec<Option<i128>>
where
    T::Native: Into<i128>,
{
    let values = array.as_primitive::<T>().iter();
    values.map(|value| value.map(Into::into)).collect()
}

/// An operation a query makes of the column `a`, and of `b` where it takes two.
#[derive(Clone, Copy, Debug)]
enum Operation {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Negate,
    Abs,
    Cast(Int),
}

/// Every operation of two columns.
const OPERATORS: [Operation; 5] = [
    Operation::Add,
    Operation::Subtract,
    Operation::Multiply,
    Operation::Divide,
    Operation::Remainder,
];

/// What a row gives: a value, a null among them, or a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Given {
    Value(Option<i128>),
    Failure,
}

impl Operation {
    /// The query projecting it as `y`.
    fn query(self) -> String {
        let y = match self {
            Self::Add => "a + b".to_owned(),
            Self::Subtract => "a - b".to_owned(),
            Self::Multiply => "a * b".to_owned(),
            Self::Divide => "a / b".to_owned(),
            Self::Remainder => "a % b".to_owned(),
            Self::Negate => "-a".to_owned(),
            Self::Abs => "abs(a)".to_owned(),
            Self::Cast(to) => format!("CAST(a AS {})", to.name()),
        };
        format!("SELECT {y} AS y FROM input")
    }

    /// What it gives `a` and `b` exactly, where it gives a value: a division by zero gives none.
    fn exact(self, a: i128, b: i128) -> Option<i128> {
        match self {
            Self::Add => a.checked_add(b),
            Self::Subtract => a.checked_sub(b),
            Self::Multiply => a.checked_mul(b),
            Self::Divide => a.checked_div(b),
            Self::Remainder => a.checked_rem(b),
            Self::Negate => a.checked_neg(),
            Self::Abs => a.checked_abs(),
            Self::Cast(_) => Some(a),
        }
    }

    /// The type it gives over `a` of `left` and `b` of `right`, where it gives an integer.
    fn typed(self, left: Int, right: Int) -> Option<Int> {
        match self {
            Self::Negate | Self::Abs => Some(left),
            Self::Cast(to) => Some(to),
            _ => left.common(right),
        }
    }

    /// Whether it reads `b` as well as `a`.
    fn binary(self) -> bool {
        matches!(
            self,
            Self::Add | Self::Subtract | Self::Multiply | Self::Divide | Self::Remainder
        )
    }

    /// What it gives a row of `a` and `b`, as a value of `to`.
    fn given(self, to: Int, a: Option<i128>, b: Option<i128>) -> Given {
        let operands = if self.binary() {
            a.zip(b)
        } else {
            a.map(|a| (a, 0))
        };
        let Some((a, b)) = operands else {
            return Given::Value(None);
        };
        match self.exact(a, b) {
            Some(value) if to.holds(value) => Given::Value(Some(value)),
            _ => Given::Failure,
        }
    }
}

/// Asserts that `sql`, the query of `operation`, gives over `batch` what the model does, as a
/// column of `to`: a batch fails where one of its rows does, each such row fails alone, and the
/// rest give their values.
fn check(operation: Operation, sql: &Sql, batch: &RecordBatch, to: Int) {
    let (a, b) = (widened(batch.column(0)), widened(batch.column(1)));
    let given: Vec<Given> = a
        .iter()
        .zip(&b)
        .map(|(a, b)| operation.given(to, *a, *b))
        .collect();
    let query = operation.query();
    let Ok(output) = sql.apply(batch.clone()) else {
        let failing = given.iter().map(|given| *given == Given::Failure);
        let failing: Vec<bool> = failing.collect();
        assert!(
            failing.contains(&true),
            "{query} failed over {a:?} and {b:?}"
        );
        for row in (0..batch.num_rows()).filter(|row| failing[*row]) {
            let alone = sql.apply(batch.slice(row, 1));
            let (a, b) = (a[row], b[row]);
            assert!(
                alone.is_err(),
                "{query} over {a:?} and {b:?} gave {alone:?}"
            );
        }
        let passing = failing.iter().map(|failing| !failing);
        let passing = BooleanArray::from(passing.collect::<Vec<_>>());
        let rest = filter_record_batch(batch, &passing).expect("an answer a row");
        return check(operation, sql, &rest, to);
    };
    let y = output.column(0);
    assert_eq!(y.data_type(), &to.data_type(), "{query}");
    let gave: Vec<Given> = widened(y).into_iter().map(Given::Value).collect();
    assert_eq!(gave, given, "{query} over {a:?} and {b:?}");
}

/// Rows of `a` of `left` and `b` of `right`: every pair of their edges, a null beside an edge of
/// each, and drawn rows.
fn rows(left: Int, right: Int) -> impl Strategy<Value = Vec<(Option<i128>, Option<i128>)>> {
    let mut edges = Vec::new();
    for a in left.edges() {
        for b in right.edges() {
            edges.push((Some(a), Some(b)));
        }
    }
    edges.push((None, Some(right.least())));
    edges.push((Some(left.least()), None));
    let drawn = vec((option::of(left.drawn()), option::of(right.drawn())), 0..16);
    drawn.prop_map(move |drawn| {
        let mut rows = edges.clone();
        rows.extend(drawn);
        rows
    })
}

#[test]
fn integer_arithmetic_gives_its_exact_value_or_fails_where_its_type_cannot_hold_it() {
    for left in INTS {
        for right in INTS {
            let mut operations = OPERATORS.to_vec();
            if left == right {
                operations.extend([Operation::Negate, Operation::Abs]);
                operations.extend(INTS.map(Operation::Cast));
            }
            let planned: Vec<(Operation, Sql, Int)> = operations
                .into_iter()
                .filter(|operation| left.signed() || !matches!(operation, Operation::Negate))
                .filter_map(|operation| {
                    let to = operation.typed(left, right)?;
                    let sql = Sql::parse(&operation.query()).expect("the query parses");
                    Some((operation, sql, to))
                })
                .collect();
            for seed in 0..u64::from(rdlt_testkit::cases(8)) {
                let rows = rdlt_testkit::draw::draw(&rows(left, right), seed);
                let (a, b): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
                let batch =
                    RecordBatch::try_from_iter([("a", left.array(&a)), ("b", right.array(&b))])
                        .expect("the columns are of one length");
                for (operation, sql, to) in &planned {
                    check(*operation, sql, &batch, *to);
                }
            }
        }
    }
}

#[test]
fn an_unsigned_integer_is_not_negated() {
    for int in INTS.into_iter().filter(|int| !int.signed()) {
        let batch = RecordBatch::try_from_iter([("a", int.array(&[Some(1)]))]).expect("one column");
        let sql = Sql::parse(&Operation::Negate.query()).expect("the query parses");
        assert!(sql.output(batch.schema_ref()).is_err(), "{int:?}");
    }
}
