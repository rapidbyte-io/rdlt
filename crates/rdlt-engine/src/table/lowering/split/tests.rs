use std::error::Error as _;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::ArrowError;
use rdlt_connector::LogicalType;

use super::{Fitted, Splits};
use crate::shred::ShredError;
use crate::shred::values::{self, Joined};
use crate::table::resolve::{Rest, Route};

#[test]
fn a_value_its_own_column_cannot_read_fails_with_the_shredder_s_error_as_its_cause() {
    let fitted = Fitted(vec![(
        0,
        StringArray::from(vec!["{"]),
        BooleanArray::from(vec![true]),
        Joined::default(),
    )]);
    let error = fitted
        .own(0, &LogicalType::Int64)
        .expect_err("the text is no JSON");
    assert!(matches!(error, ArrowError::ExternalError(_)), "{error:?}");
    let cause = error
        .source()
        .and_then(|cause| cause.downcast_ref::<ShredError>());
    assert!(matches!(cause, Some(ShredError::Invalid(_))), "{error:?}");
}

#[test]
fn a_split_column_is_found_by_its_position_and_one_not_fitted_is_refused() {
    let positions = [1, 4, 9];
    let fitted = Fitted(
        positions
            .iter()
            .map(|position| {
                let texts = StringArray::from(vec![position.to_string()]);
                let (fits, joined) = values::fitting(&texts, &LogicalType::Int64);
                (*position, texts, fits, joined)
            })
            .collect(),
    );
    for position in positions {
        let own = fitted.own(position, &LogicalType::Int64).unwrap();
        let own = own.as_primitive::<Int64Type>();
        assert_eq!(own.value(0), i64::try_from(position).unwrap());
    }
    for position in [0, 2, 5, 10, usize::MAX] {
        assert!(
            matches!(fitted.rest(position), Err(ArrowError::ComputeError(_))),
            "{position}"
        );
        assert!(
            fitted.own(position, &LogicalType::Int64).is_err(),
            "{position}"
        );
    }
    assert!(Fitted::default().rest(0).is_err());
}

#[test]
fn a_plan_s_split_columns_are_fitted_and_filtered_each_at_its_position() {
    // Split columns at positions 1, 4 and 7, among columns routed elsewhere.
    let routes = [
        Route::Column(0),
        Route::Split {
            own: 1,
            rest: Rest::Column(5),
        },
        Route::Column(2),
        Route::DiscardValues,
        Route::Split {
            own: 3,
            rest: Rest::DiscardValues,
        },
        Route::Skip,
        Route::Column(4),
        Route::Split {
            own: 6,
            rest: Rest::DiscardRows,
        },
    ];
    let types = vec![LogicalType::Int64; 7];
    let splits = Splits::of(&routes, &types);
    let columns = (0..routes.len()).map(|position| {
        let texts = [
            position.to_string(),
            "\"text\"".to_owned(),
            (position * 10).to_string(),
        ];
        let column: ArrayRef = Arc::new(StringArray::from_iter_values(texts));
        (format!("c{position}"), column)
    });
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let fitted = splits.fit(&batch, false).unwrap();
    let fitted = fitted
        .filtered(&BooleanArray::from(vec![false, true, true]))
        .unwrap();
    for position in [1, 4, 7] {
        let own = fitted.own(position, &LogicalType::Int64).unwrap();
        let own = own.as_primitive::<Int64Type>();
        assert!(own.is_null(0), "{position}");
        assert_eq!(own.value(1), i64::try_from(position * 10).unwrap());
        let rest = fitted.rest(position).unwrap();
        let rest = rest.as_string::<i32>();
        assert_eq!(rest.value(0), "\"text\"", "{position}");
        assert!(rest.is_null(1), "{position}");
    }
    for position in [0, 2, 3, 5, 6] {
        assert!(fitted.rest(position).is_err(), "{position}");
    }
}

/// A fit that is null in a row whose value bit is set keeps that row, as a null fit in any row
/// does; only a row whose fit is false is dropped.
#[test]
fn a_row_whose_fit_is_null_is_kept_whatever_its_value_bit() {
    let routes = [Route::Split {
        own: 0,
        rest: Rest::DiscardRows,
    }];
    let splits = Splits::of(&routes, &[LogicalType::Int64]);
    let values = BooleanBuffer::from(vec![true, true, false, false]);
    let nulls = NullBuffer::from(vec![true, false, true, false]);
    let fits = BooleanArray::new(values, Some(nulls));
    let texts = StringArray::from(vec!["1", "2", "x", "y"]);
    let fitted = Fitted(vec![(0, texts, fits, Joined::default())]);
    let kept = splits.kept(&fitted, 4).expect("the split drops rows");
    assert_eq!(
        kept.values().iter().take(4).collect::<Vec<_>>(),
        [true, true, false, true]
    );
}

/// Values read after the rows holding others were dropped read as a fit of the kept rows alone
/// reads them: a dropped row's float or its field the kept rows lack, or its text beside kept
/// numbers a JSON field renders, changes nothing a kept value becomes.
#[test]
fn values_kept_after_a_drop_read_as_a_fit_of_the_kept_rows_alone_reads_them() {
    use rdlt_connector::{Field, Fields};
    let wider = LogicalType::Struct(
        Fields::new(vec![
            Field::new("a", LogicalType::Float64, true),
            Field::new("z", LogicalType::Int64, true),
        ])
        .unwrap(),
    );
    let json =
        LogicalType::Struct(Fields::new(vec![Field::new("a", LogicalType::Json, true)]).unwrap());
    let cases = [
        (
            wider,
            vec![
                Some(r#"{"a":1}"#),
                Some(r#"{"a":0.5,"z":7}"#),
                Some(r#"{"a":2}"#),
                Some("1"),
                None,
            ],
        ),
        (
            json,
            vec![
                Some(r#"{"a":1.50}"#),
                Some(r#"{"a":"x"}"#),
                Some(r#"{"a":1e2}"#),
                Some("1"),
                None,
            ],
        ),
    ];
    let kept = BooleanArray::from(vec![true, false, true, true, true]);
    for (own, texts) in cases {
        let texts: ArrayRef = Arc::new(StringArray::from(texts));
        let batch = RecordBatch::try_from_iter([("c", texts)]).unwrap();
        let splits = Splits(vec![(0, own.clone(), Rest::DiscardRows)]);
        let dropped = splits.fit(&batch, false).unwrap().filtered(&kept).unwrap();
        let alone = arrow_select::filter::filter_record_batch(&batch, &kept).unwrap();
        let alone = splits.fit(&alone, false).unwrap();
        assert_eq!(
            dropped.own(0, &own).unwrap().to_data(),
            alone.own(0, &own).unwrap().to_data(),
            "{own:?}"
        );
    }
}

/// A filter that drops no row keeps the join fitting took, observing no value again; one that
/// drops a row observes the kept values that fit once more.
#[test]
fn only_a_filter_that_drops_a_row_observes_the_kept_values_again() {
    let texts: ArrayRef = Arc::new(StringArray::from(vec![Some("1"), Some("2"), Some("x")]));
    let batch = RecordBatch::try_from_iter([("c", texts)]).unwrap();
    let splits = Splits(vec![(0, LogicalType::Int64, Rest::DiscardRows)]);
    let observed = |kept: Vec<bool>| {
        let fitted = splits.fit(&batch, false).unwrap();
        values::OBSERVED.with(|observed| observed.set(0));
        fitted.filtered(&BooleanArray::from(kept)).unwrap();
        values::OBSERVED.with(std::cell::Cell::get)
    };
    assert_eq!(observed(vec![true, true, true]), 0);
    assert_eq!(observed(vec![true, false, true]), 1);
}
