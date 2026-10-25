use std::num::NonZeroUsize;

use super::Mixed;
use crate::Cores;

fn cores() -> Cores {
    Cores::new(NonZeroUsize::new(2).unwrap())
}

#[test]
fn a_stream_its_source_cannot_read_again_loads_beside_those_it_can() {
    for mixed in [
        Mixed::new(cores(), Mixed::CELL.tested()).unwrap(),
        Mixed::replayable(cores(), Mixed::CELL.tested()).unwrap(),
    ] {
        for _ in 0..2 {
            let report = mixed.run();
            assert_eq!(report.rows, u64::from(Mixed::CELL.tested().rows) * 4);
            assert_eq!(report.attempted, 1);
        }
    }
}
