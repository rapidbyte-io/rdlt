# JSON shredding throughput

The spec gates JSON shredding at four times the old engine on one core, and at 0.7·N times one
core for one stream over N ≤ 8 cores (§21.1). Both are ratios measured on one machine, so this
record keeps the method with the numbers.

## Method

- **Corpus.** 200 000 nested rows of about 181 bytes (36.2 MB), written by the script below; the
  criterion bench's `nested` corpus follows the same shape.
- **Old engine.** `rdlt.old` at `0249668f`, through its production shred path
  `rdlt_engine::fuzzing::bench_shred_bytes`, over 8 MiB pushes cut at line ends, built with fat
  LTO and one codegen unit, best of five runs. The harness is below.
- **This engine.** `cargo bench -p rdlt-engine --features bench --bench shred` with
  `RDLT_SHRED_CORPUS` naming the corpus (`shred/file`): 8 MiB pushes, 1 MiB chunks, the bench
  profile's fat LTO, criterion's mean.
- **Cores.** `shred_cores/N` shreds the `nested` corpus on a pool of N threads. The development
  machine mixes core types, so scaling is measured on cores of one type, with `taskset`.

```python
"""Nested ~170-byte JSON lines rows, deterministic: gen.py ROWS OUT."""
import json, random, sys
random.seed(7)
n, out = int(sys.argv[1]), sys.argv[2]
cities = ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"]
with open(out, "w") as f:
    for i in range(n):
        row = {"id": i, "name": f"user-{i:07d}", "score": round(random.random() * 1000, 2),
               "active": i % 3 == 0, "created_at": f"2026-09-{1 + i % 28:02d}T12:{i % 60:02d}:00Z",
               "profile": {"city": random.choice(cities), "zip": f"{random.randint(10000, 99999)}",
                           "geo": {"lat": round(random.uniform(49, 55), 5), "lon": round(random.uniform(14, 24), 5)}}}
        f.write(json.dumps(row, separators=(",", ":")) + "\n")
```

The old engine's harness is a binary crate with `rdlt-engine = { path = "<rdlt.old>/crates/rdlt-engine" }`
and `[profile.release] lto = "fat"`, `codegen-units = 1`:

```rust
use std::time::Instant;

fn main() {
    let path = std::env::args().nth(1).expect("corpus path");
    let bytes = std::fs::read(&path).expect("corpus");
    let mut pushes = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        let mut end = (start + 8 * 1024 * 1024).min(bytes.len());
        while end < bytes.len() && bytes[end - 1] != b'\n' {
            end += 1;
        }
        pushes.push(&bytes[start..end]);
        start = end;
    }
    let mut best = f64::MAX;
    for _ in 0..5 {
        let began = Instant::now();
        for push in &pushes {
            rdlt_engine::fuzzing::bench_shred_bytes(push);
        }
        best = best.min(began.elapsed().as_secs_f64());
    }
    let mb = bytes.len() as f64 / 1e6;
    println!("old engine: {:.1} MB/s", mb / best);
}
```

## Results

Intel Core Ultra X7 358H (4 performance, 8 efficient and 4 low-power cores), 2026-09-24.

| Measure | Old engine | This engine | Ratio | Gate |
|---|---|---|---|---|
| Nested corpus, one core | 137.5 MB/s (best of several runs of 130.6–137.5) | 736.7 MB/s | 5.36× | ≥ 4× |

| Corpus (one core) | Throughput |
|---|---|
| `nested` | 721 MiB/s |
| `flat_narrow` | 510 MiB/s |
| `flat_wide` (200 columns) | 517 MiB/s |
| `string_heavy` | 589 MiB/s |

| Cores | One core | N cores | Scaling | Gate |
|---|---|---|---|---|
| 8 efficient cores (`taskset -c 4-11`) | 515.7 MiB/s | 3144 MiB/s | 6.10× | ≥ 5.6× |
| 4 performance cores (`taskset -c 0-3`) | 725.2 MiB/s | 2184 MiB/s | 3.01× | ≥ 2.8× |

Unpinned, the pool's eighth thread lands on slower cores than the first, so eight threads reach
3.97× the fastest core: the mix of cores, not the shredder, sets that figure.
