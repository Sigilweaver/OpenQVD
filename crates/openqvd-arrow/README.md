# openqvd-arrow

Arrow adapters for [`openqvd`](https://crates.io/crates/openqvd). The core crate
has no Arrow dependency; this crate converts a `Qvd` to an Arrow `RecordBatch`
and an Arrow `RecordBatch` to a `WriteTable`.

Each supported Arrow major has its own feature and module:

| Feature    | Module               | Arrow crates |
|------------|----------------------|--------------|
| `arrow-59` | `openqvd_arrow::v59` | 59           |
| `arrow-60` | `openqvd_arrow::v60` | 60           |

```toml
openqvd-arrow = { version = "2", features = ["arrow-60"] }
```

```rust
use openqvd::{Qvd, ReadOptions};
use openqvd_arrow::{v60, BatchOptions};

let qvd = Qvd::open("data.qvd", &ReadOptions::default())?;
let batch = v60::to_record_batch(&qvd, &BatchOptions::default())?;
```

Features may be combined, so a program that links two Arrow majors can use
both. No Arrow type appears outside the `vNN` modules.
