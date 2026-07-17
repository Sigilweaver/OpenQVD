---
title: Rust quickstart
sidebar_label: Rust quickstart
---

# Rust quickstart

```rust
use openqvd::Qvd;

let qvd = Qvd::from_path("data.qvd").unwrap();
println!("table {:?} with {} rows", qvd.table_name(), qvd.num_rows());
for row in qvd.rows() {
    for (field, value) in qvd.fields().iter().zip(row) {
        println!("  {} = {:?}", field.name, value);
    }
}
```

The reader is strict: any deviation from `SPEC.md` produces a
[`QvdError`] rather than silent misinterpretation. Use
[`Qvd::checked_rows`] instead of [`Qvd::rows`] if you also want an
out-of-range symbol index surfaced as an error rather than `None`.

## Writing

```rust
use openqvd::{Column, Value, WriteTable};

let table = WriteTable::new(
    "Orders",
    vec![
        Column::new("Id", vec![Some(Value::Int(1)), Some(Value::Int(2))]),
        Column::new("Region", vec![Some(Value::Str("West".into())), None]),
    ],
).unwrap();

table.write_to_path("out.qvd").unwrap();
```

Or re-serialise an already-parsed file:

```rust
let qvd = Qvd::from_path("data.qvd").unwrap();
qvd.write_to_path("copy.qvd").unwrap();
```

## Arrow integration

With the `arrow` feature enabled, a `Qvd` converts to and from
`arrow_array::RecordBatch`:

```toml
openqvd = { version = "1", features = ["arrow"] }
```

See the [Python API reference](./python-api.md) for the equivalent
PyArrow-native surface (the `arrow` feature is what backs it).
