# OpenQVD

[![CI](https://github.com/Sigilweaver/OpenQVD/actions/workflows/ci.yml/badge.svg)](https://github.com/Sigilweaver/OpenQVD/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.20470599.svg)](https://doi.org/10.5281/zenodo.20470599)
[![crates.io](https://img.shields.io/crates/v/openqvd.svg)](https://crates.io/crates/openqvd)
[![PyPI](https://img.shields.io/pypi/v/openqvd.svg)](https://pypi.org/project/openqvd/)
[![docs.rs](https://img.shields.io/docsrs/openqvd)](https://docs.rs/openqvd)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust MSRV](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![Docs](https://img.shields.io/badge/docs-sigilweaver.app-blue.svg)](https://sigilweaver.app/openqvd/docs/)

A free, open, clean-room specification and implementation of the Qlik QVD
binary file format, derived purely by binary analysis of publicly available
sample files. The goal is a Rust reader and writer that the data science
community can use without depending on any proprietary Qlik tooling.

The specification was developed against the
[QVD-Sources](https://github.com/Sigilweaver/QVD-Sources) corpus - a curated
collection of ~1,100 publicly available `.qvd` files gathered from GitHub.

## Status

Seven stages complete:

1. XML header and envelope structure. (Spec section 1.)
2. Per-field symbol table encoding. (Spec section 2.)
3. Bit-packed row index encoding. (Spec section 3.)
4. Validation against the full public corpus via a clean-room Python
   decoder.
5. Rust reader prototype (`crates/openqvd`) with edge-case tests.
6. Writer + semantic round-trip tests.
7. Python bindings (`crates/openqvd-py`) - Arrow PyCapsule interface, PyArrow, Polars, Pandas.

See `SPEC.md` for the current specification and `NOTES.md` for the
working log of observations.

### Rust usage

```toml
# Cargo.toml
[dependencies]
openqvd = "2"

# Arrow RecordBatch conversion lives in a separate crate with one feature per
# Arrow major (arrow-59, arrow-60, ...):
openqvd-arrow = { version = "2", features = ["arrow-60"] }
```

```rust
use openqvd::{Qvd, ReadOptions};

let qvd = Qvd::open("data.qvd", &ReadOptions::default()).unwrap();
println!("{} rows", qvd.num_rows());
for row in qvd.rows() {
    let row = row.unwrap(); // Vec<Option<Value>>; Err on a corrupt file
}
```

Reading is strict by default. `ReadOptions` carries the column projection and
a `Leniency` set naming the checks a caller accepts skipping; each tolerated
deviation is reported through `Qvd::warnings()`. Errors are `QvdError` with
a typed `kind()`. Public enums and data structs are `#[non_exhaustive]`.

### Reader

The Rust reader parses **1,044 of 1,047** valid public QVD samples. The
three remaining files are deliberately-corrupted test fixtures from
third-party projects (two named `damaged.qvd`, one with invalid UTF-8).
11 unit + integration tests cover bias-based NULL, 2+6 bit packing,
zero-width fields, every symbol type byte, unknown-type rejection,
overlapping bit-fields rejection, inconsistent root `Length`
rejection, and the LF-terminator header variant.

### Writer

A compliant writer is implemented in `crates/openqvd::writer`. Running
`read -> write -> read` over the entire corpus yields **1,093 of 1,093
valid files semantically equivalent** (same row count, same field
names, byte-for-byte equal cell values). 11 writer tests cover NULL
handling, all five symbol types, zero-width collapse for constant
columns, 500-distinct wide columns, NUL-in-string rejection,
uneven-column rejection, and deterministic output.

### Python bindings

`crates/openqvd-py` is a [maturin](https://maturin.rs/) mixed-layout
package that exposes a pure-Python API on top of the Rust library.

**Install (development)**

```sh
cd crates/openqvd-py
uv venv .venv && source .venv/bin/activate
uv pip install maturin pyarrow polars pandas duckdb
maturin develop
```

**Usage**

```python
import openqvd

# Read. The result is an openqvd.Table: it implements the Arrow PyCapsule
# interface (__arrow_c_stream__), so pyarrow, Polars, DuckDB and others accept
# it directly. No Arrow library is needed unless you call a to_* helper.
table = openqvd.read("data.qvd")
arrow_table = table.to_pyarrow()
polars_df = table.to_polars()
print(table.num_rows, table.column_names, table.warnings)
table = openqvd.read("data.qvd", columns=["OrderId", "Amount"])

# Predicate pushdown (filtering at the Rust level, before Arrow conversion)
table = openqvd.read("data.qvd", filters=[
    {"column": "Region", "op": "eq", "value": "West"},
    {"column": "Status", "op": "is_in", "value": ["Open", "Pending"]},
    {"column": "Notes",  "op": "is_not_null"},
])

# Inspect metadata only (no row decoding)
info = openqvd.schema("data.qvd")
print(info.table_name, info.num_rows)
print([f.name for f in info.fields])

# Write anything that exports Arrow data (pyarrow, Polars, openqvd.Table, ...)
openqvd.write(table, "out.qvd")
openqvd.write(table, "out.qvd", table_name="Orders")

# Polars (import registers pl.read_qvd, pl.scan_qvd, df.qvd.write)
import openqvd.polars
import polars as pl

df = pl.read_qvd("data.qvd")
lf = pl.scan_qvd("data.qvd", columns=["A", "B"])
df = pl.read_qvd("data.qvd", filters=[{"column": "A", "op": "eq", "value": "x"}])
df.qvd.write("out.qvd")

# Pandas (needs pyarrow too)
df = openqvd.read("data.qvd").to_pandas()
```

The Python bindings read **1,044 of 1,047** valid corpus files (99.7%),
matching the Rust reader baseline. The 3 failures are deliberately-
corrupted test fixtures.

### DuckDB integration

```python
import duckdb
import openqvd.duckdb as qdb

con = duckdb.connect()

# Register a QVD file as a SQL view
qdb.register(con, "orders", "orders.qvd")
con.execute("SELECT COUNT(*) FROM orders WHERE Region = 'West'").fetchone()

# Or get a relation directly
rel = qdb.to_relation("orders.qvd", con)

# Write a DuckDB query result to a QVD file
qdb.from_query(
    "SELECT id, amount FROM orders WHERE status = 'Open'",
    "open_orders.qvd",
    con=con,
)
```

Install with `pip install openqvd[duckdb]`. DuckDB support is provided through
Arrow interop; a native `read_qvd()` SQL table function would require a C++
extension, which is out of scope.

**Arrow type mapping**

| QVD NumberFormat/Type | Arrow type |
|---|---|
| `DATE`, every value a whole day | `Date32` (Qlik epoch → Unix epoch) |
| `DATE`, any value with a time of day | `Timestamp(Microsecond, None)` |
| `TIMESTAMP` | `Timestamp(Microsecond, None)` |
| `TIME`, every value in [0, 1) days | `Duration(Microsecond)` |
| `TIME`, any value outside [0, 1) | `Timestamp(Microsecond, None)` (the values carry a date) |
| `INTERVAL` | `Duration(Microsecond)` |
| Int / DualInt symbols | `Int64` |
| Float / DualFloat symbols | `Float64` |
| String symbols | `LargeUtf8` |
| Empty symbol table | `Null` |

The type is decided by the data as well as the format hint, so one logical
DATE column can be `Date32` in one extract and `Timestamp` in the next. Read
`openqvd.schema(path).fields[i].dtype` (Rust: `Qvd::column_type`) to see the
resolved type, and the `qvd.number_format_type` Arrow field metadata for the
original hint. DATE, TIMESTAMP, TIME and INTERVAL columns must hold finite
numbers; text or NaN is an error unless the `non_numeric_temporal` lenient
check is allowed, which returns that column as text and warns.

### CLI

The `openqvd` binary provides end-user tooling:

```
openqvd stat <file>           # header summary (fields, widths, rows)
openqvd head <file> [--rows N]  # first N rows
openqvd csv  <file>           # every row as tab-separated text
openqvd json <file>           # one JSON object per row
openqvd rewrite <in> <out>    # read then re-serialise through the writer
```

## Non-goals

- Executing, shipping, or linking any proprietary Qlik code.
- Reading closed or encrypted QVD variants (if they exist).
- Parsing QVW, QVF, or QVS files (those are separate formats).

## License

The **software** (all `.rs`, `.py` source files) is licensed under
[Apache-2.0](LICENSE).

The **specification** (`SPEC.md`) is licensed under
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
