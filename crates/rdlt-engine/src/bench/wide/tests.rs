use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use super::{Form, Wide};
use crate::Cores;
use crate::bench::{PUSH_BYTES, Replayed};

#[test]
fn a_wide_table_loads_every_row_of_its_pushes_on_every_run() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    for form in [Form::Arrow, Form::Json] {
        let wide = Wide::try_new(cores, form, 100, 2).unwrap();
        let per_push = wide.bytes() / 2;
        let target = PUSH_BYTES as u64;
        assert!(
            per_push > target / 2 && per_push < target * 2,
            "{form:?}: {per_push}"
        );
        assert!(wide.rows() >= 2, "{form:?}");
        assert_eq!(wide.rows(), Wide::rows_of(form, 100, 2), "{form:?}");
        for _ in 0..2 {
            assert_eq!(wide.run(), wide.rows(), "{form:?}");
        }
    }
}

#[test]
fn the_ids_of_a_wide_table_s_batches_run_on_from_one_to_the_next() {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let windows = u32::from(Wide::SPARSE / Wide::KEPT);
    for wide in [
        Wide::try_new(cores, Form::Arrow, 10, 3).unwrap(),
        Wide::keeping(cores, 300, 100, 3).unwrap(),
        Wide::sparse(cores, Form::Arrow, windows + 1).unwrap(),
    ] {
        let Replayed::Batches(batches) = &wide.replayed else {
            panic!("Arrow pushes are batches");
        };
        // Each push's first column is an id column, `id_<round>` shifted by its round beyond
        // the first ten columns.
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                let schema = batch.schema();
                let name = schema.field(0).name();
                let round: i64 = name
                    .strip_prefix("id_")
                    .map_or(0, |round| round.parse().unwrap());
                assert!(round > 0 || name == "id", "{name}");
                let column = batch.column(0).as_primitive::<Int64Type>();
                column.values().iter().map(move |id| id - round)
            })
            .collect();
        let rows = i64::try_from(wide.rows()).unwrap();
        assert_eq!(ids, (0..rows).collect::<Vec<_>>());
    }
}

fn cores() -> Cores {
    Cores::new(NonZeroUsize::new(2).unwrap())
}

/// The names of the columns each push of `wide` holds, and its rows.
fn pushed(wide: &Wide) -> Vec<(BTreeSet<String>, usize)> {
    match &wide.replayed {
        Replayed::Batches(batches) => batches
            .iter()
            .map(|batch| {
                let schema = batch.schema();
                let fields = schema.fields().iter().map(|field| field.name().clone());
                (fields.collect(), batch.num_rows())
            })
            .collect(),
        Replayed::Json(pushes) | Replayed::Rows(pushes) => pushes
            .iter()
            .map(|push| {
                let lines: Vec<&[u8]> = push.split(|byte| *byte == b'\n').collect();
                let first: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_slice(lines[0]).unwrap();
                for line in lines.iter().filter(|line| !line.is_empty()) {
                    let row: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_slice(line).unwrap();
                    assert!(row.keys().eq(first.keys()));
                }
                let rows = lines.iter().filter(|line| !line.is_empty()).count();
                (first.keys().cloned().collect(), rows)
            })
            .collect(),
    }
}

#[test]
fn a_kept_table_pushes_its_kept_columns_then_the_wide_bench_s_pushes() {
    assert_eq!((Wide::PUSHES, Wide::KEPT_PUSHES), (8, 9));
}

#[test]
fn a_table_kept_to_its_first_columns_discards_every_value_of_the_rest() {
    let per_push = Wide::rows_of(Form::Arrow, Wide::SPARSE, 1);
    let kept = Wide::keeping(cores(), 300, 100, 3).unwrap();
    assert_eq!(
        (kept.rows(), kept.discards()),
        (3 * per_push, 400 * per_push)
    );
    let widths: Vec<usize> = pushed(&kept)
        .iter()
        .map(|(columns, _)| columns.len())
        .collect();
    assert_eq!(widths, [100, 300, 300]);
    let all = Wide::keeping(cores(), 100, 100, 3).unwrap();
    assert_eq!((all.rows(), all.discards()), (3 * per_push, 0));
    let rows = 3 * per_push;
    for _ in 0..2 {
        assert_eq!(kept.run(), rows);
        assert_eq!(all.run(), rows);
    }
}

#[test]
fn a_sparse_table_s_pushes_take_each_window_of_its_columns_in_turn() {
    let windows = usize::from(Wide::SPARSE / Wide::KEPT);
    assert_eq!(usize::try_from(Wide::SPARSE_PUSHES).unwrap(), 2 * windows);
    let per_push = Wide::rows_of(Form::Arrow, Wide::SPARSE, 1);
    for form in [Form::Arrow, Form::Json] {
        let count = u32::try_from(windows + 2).unwrap();
        let sparse = Wide::sparse(cores(), form, count).unwrap();
        assert_eq!(sparse.rows(), per_push * u64::from(count), "{form:?}");
        let made = pushed(&sparse);
        let mut all = BTreeSet::new();
        for (index, (columns, rows)) in made.iter().enumerate() {
            assert_eq!(columns.len(), usize::from(Wide::KEPT), "{form:?}");
            assert_eq!(u64::try_from(*rows).unwrap(), per_push, "{form:?}");
            if index >= windows {
                assert_eq!(*columns, made[index - windows].0, "{form:?}");
            } else {
                assert!(columns.is_disjoint(&all), "{form:?}");
            }
            all.extend(columns.iter().cloned());
        }
        assert_eq!(all.len(), usize::from(Wide::SPARSE), "{form:?}");
    }
}

#[test]
fn a_sparse_table_loads_every_row_of_its_pushes() {
    for form in [Form::Arrow, Form::Json] {
        let sparse = Wide::sparse(cores(), form, 3).unwrap();
        assert_eq!(sparse.discards(), 0);
        for _ in 0..2 {
            assert_eq!(sparse.run(), sparse.rows(), "{form:?}");
        }
    }
}
