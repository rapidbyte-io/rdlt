use std::error::Error as _;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::ArrowError;
use rdlt_connector::LogicalType;

use super::{Fitted, Splits};
use crate::shred::ShredError;
use crate::table::resolve::{Rest, Route};

#[test]
fn a_value_its_own_column_cannot_read_fails_with_the_shredder_s_error_as_its_cause() {
    let fitted = Fitted(vec![(
        0,
        StringArray::from(vec!["{"]),
        BooleanArray::from(vec![true]),
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
    let fitted = |text: &str| {
        (
            StringArray::from(vec![text]),
            BooleanArray::from(vec![true]),
        )
    };
    let positions = [1, 4, 9];
    let fitted = Fitted(
        positions
            .iter()
            .map(|position| {
                let (texts, fits) = fitted(&position.to_string());
                (*position, texts, fits)
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
