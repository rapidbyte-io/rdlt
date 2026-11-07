//! Drawn batches through one to three drawn steps, ops and planned SQL, over drawn columns, with
//! a drawn key and allow-list, within a drawn room: the stage accepts or refuses the batch's
//! schema, and once it accepts it, it runs the batch, refusing nothing but a row too large for
//! the room, and gives the same answer twice.

#![forbid(unsafe_code)]
#![no_main]

use arrow_array::RecordBatch;
use libfuzzer_sys::arbitrary::{Error, Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use rdlt_connector::{Secret, StreamName};
use rdlt_engine::bench::{Transformed, Untransformed};
use rdlt_engine::{Literal, Mask, Op, StreamPlan, TransformStep, codes};
use rdlt_sql::Sql;
use rdlt_testkit::draw::draw;
use rdlt_testkit::drawn::json::{batch, drawn};

/// Names a step may give a column, ones SQL must quote and fold among them.
const NAMES: [&str; 5] = ["renamed", "x y", "q\"uote", "Ünï", "UPPER"];

fuzz_target!(|data: &[u8]| {
    let mut input = Unstructured::new(data);
    let Ok(Drawn {
        plan,
        key,
        batch,
        room,
    }) = planned(&mut input)
    else {
        return;
    };
    let key: Vec<&str> = key.iter().map(String::as_str).collect();
    let Ok(transformed) = Transformed::try_new(&plan, &key, batch) else {
        return;
    };
    let once = answer(&transformed, room);
    assert_eq!(
        once,
        answer(&transformed, room),
        "the stage answers one batch two ways"
    );
});

/// A drawn stream: its plan, its key, a batch and the room its stage runs within.
struct Drawn {
    plan: StreamPlan,
    key: Vec<String>,
    batch: RecordBatch,
    room: u64,
}

/// A stream of one to three steps, a key, an allow-list and a room, all drawn from `input`.
fn planned(input: &mut Unstructured<'_>) -> Result<Drawn> {
    let batch = batch(&draw(&drawn(), input.arbitrary::<u64>()?));
    let mut columns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    if columns.is_empty() {
        return Err(Error::NotEnoughData);
    }
    let mut plan = StreamPlan::new(StreamName::new("s").expect("a valid name"));
    let key = if input.arbitrary()? {
        vec![input.choose(&columns)?.clone()]
    } else {
        Vec::new()
    };
    if input.ratio(1, 4)? {
        let mut listed = Vec::new();
        for column in &columns {
            if input.arbitrary()? {
                listed.push(column.as_str());
            }
        }
        if !listed.is_empty() {
            plan = plan.include_only(listed);
        }
    }
    for _ in 0..input.int_in_range(1..=3)? {
        let column = input.choose(&columns)?.clone();
        let name = (*input.choose(&NAMES)?).to_owned();
        let (step, made) = step(input, &column, &name)?;
        plan = plan.transform(step);
        columns.extend(made);
    }
    let room = if input.arbitrary()? {
        u64::MAX
    } else {
        u64::from(input.arbitrary::<u16>()?) * 16
    };
    Ok(Drawn {
        plan,
        key,
        batch,
        room,
    })
}

/// A drawn step over `column`, and the name of the column it makes, `name`, where it makes one.
fn step(
    input: &mut Unstructured<'_>,
    column: &str,
    name: &str,
) -> Result<(TransformStep, Option<String>)> {
    let (column_sql, name_sql) = (quoted(column), quoted(name));
    let made = Some(name.to_owned());
    let sql = |text: String| Sql::parse(&text).map(TransformStep::from).ok();
    let (step, made) = match input.int_in_range(0..=8)? {
        0 => (Some(Op::rename([(column, name)]).into()), made),
        1 => {
            let constant = Op::constant([(name, Literal::Text("c".to_owned()))]);
            (Some(constant.into()), made)
        }
        2 => {
            let key = Secret::new("k".to_owned());
            (Some(Op::mask([column], Mask::hmac_sha256(key)).into()), None)
        }
        3 => (Some(Op::mask([column], Mask::redact("***")).into()), None),
        4 => (Some(Op::mask([column], Mask::Null).into()), None),
        5 => (
            sql(format!("SELECT * FROM input WHERE {column_sql} IS NOT NULL")),
            None,
        ),
        6 => (
            sql(format!(
                "SELECT * FROM input WHERE CASE WHEN {column_sql} IS NULL THEN FALSE END"
            )),
            None,
        ),
        7 => (
            sql(format!(
                "SELECT *, CAST({column_sql} AS VARCHAR) AS {name_sql} FROM input"
            )),
            made,
        ),
        _ => (
            sql(format!(
                "SELECT {column_sql} FROM input WHERE {column_sql} IS NULL"
            )),
            None,
        ),
    };
    let step = step.ok_or(Error::IncorrectFormat)?;
    Ok((step, made))
}

/// `name` quoted as a SQL identifier.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// What the stage makes of the batch within `room`: its batches as they print, where a float
/// that is not a number equals itself, or a row too large for the room.
///
/// # Panics
///
/// Panics on any other refusal: the stage accepted the batch's schema, and the batch holds no
/// flags a transform could refuse.
fn answer(transformed: &Transformed, room: u64) -> String {
    match transformed.batches_within(room) {
        Ok(batches) => format!("{batches:?}"),
        Err(Untransformed::Refused(error)) if error.code() == Some(codes::ROW_EXCEEDS_BUDGET) => {
            assert!(room != u64::MAX, "a row is too large for no limit: {error:?}");
            "a row too large for the room".to_owned()
        }
        Err(other) => panic!("the stage refused a batch whose schema it accepted: {other:?}"),
    }
}
