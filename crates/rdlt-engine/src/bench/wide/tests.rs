use std::num::NonZeroUsize;

use super::{Form, Wide};
use crate::Cores;
use crate::bench::PUSH_BYTES;

#[test]
fn a_wide_table_loads_every_row_of_its_pushes_on_every_run() {
    let cores = Cores::from_count(NonZeroUsize::new(2).unwrap());
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
    let cores = Cores::from_count(NonZeroUsize::new(2).unwrap());
    let wide = Wide::try_new(cores, Form::Arrow, 10, 3).unwrap();
    let crate::bench::Replayed::Batches(batches) = &wide.replayed else {
        panic!("Arrow pushes are batches");
    };
    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    let rows = i64::try_from(wide.rows()).unwrap();
    assert_eq!(ids, (0..rows).collect::<Vec<_>>());
}
