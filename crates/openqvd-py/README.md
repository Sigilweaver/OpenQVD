# OpenQVD

Read and write Qlik QVD files from Python.

A clean-room Rust implementation of the QVD binary format, built against the
[QVD-Sources](https://github.com/Sigilweaver/QVD-Sources) corpus — a curated
collection of ~1,100 publicly available `.qvd` files. No Qlik software required.

## Status

Seven stages complete:

1. XML header and envelope structure.
2. Per-field symbol table encoding.
3. Bit-packed row index encoding.
4. Validation against the full public corpus via a clean-room Python decoder.
5. Rust reader prototype with edge-case tests.
6. Writer + semantic round-trip tests.
7. Python bindings — PyArrow, Polars, Pandas.

## Install

```bash
pip install openqvd                   # core (PyArrow)
pip install openqvd[polars]           # + Polars
pip install openqvd[duckdb]           # + DuckDB
pip install openqvd[all]              # everything
```

## Usage

```python
import openqvd

# Read as a PyArrow Table
table = openqvd.read("data.qvd")

# Project columns at the Rust level
table = openqvd.read("data.qvd", columns=["OrderId", "Amount"])

# Predicate pushdown — filtering happens before Arrow conversion
table = openqvd.read("data.qvd", filters=[
    {"column": "Region",  "op": "eq",         "value": "West"},
    {"column": "Status",  "op": "is_in",       "value": ["Open", "Pending"]},
    {"column": "Notes",   "op": "is_not_null"},
])

# Inspect metadata only (no row decoding)
info = openqvd.schema("data.qvd")
print(info.table_name, info.num_rows)
print([f.name for f in info.fields])

# Write from a PyArrow Table
openqvd.write(table, "out.qvd")
openqvd.write(table, "out.qvd", table_name="Orders")
```

All five QVD symbol types are supported: Int, Float, String, DualInt, and
DualFloat. NULL values, DATE / TIMESTAMP / TIME number formats, and the
LF-terminator header variant are all handled correctly.

## Polars integration

Importing `openqvd.polars` automatically monkey-patches `polars` with QVD
support (no-op if polars is not installed).

```python
import openqvd.polars   # registers pl.read_qvd, pl.scan_qvd, df.qvd.write
import polars as pl

# Eager read — returns a DataFrame
df = pl.read_qvd("data.qvd")

# Lazy scan with projection and predicate pushdown
lf = pl.scan_qvd("data.qvd", columns=["A", "B"])
df = lf.filter(pl.col("A") > 10).collect()

# Filtered eager read
df = pl.read_qvd(
    "data.qvd",
    filters=[{"column": "A", "op": "eq", "value": "x"}],
)

# Write a DataFrame directly
df.qvd.write("out.qvd")
```

## Pandas integration

Pandas is supported through PyArrow:

```python
df = openqvd.read("data.qvd").to_pandas()
```

## DuckDB integration

```python
import duckdb
import openqvd.duckdb as qdb

con = duckdb.connect()

# Register a QVD file as a SQL view
qdb.register(con, "orders", "orders.qvd")
con.execute("SELECT COUNT(*) FROM orders WHERE Region = 'West'").fetchone()

# Or get a relation directly (no SQL name needed)
rel = qdb.to_relation("orders.qvd", con)

# Write a DuckDB query result to a QVD file
qdb.from_query(
    "SELECT id, amount FROM orders WHERE status = 'Open'",
    "open_orders.qvd",
    con=con,
)

# Relation input also accepted
qdb.from_query(rel, "all_orders.qvd")
```

DuckDB support is provided through Arrow interop. A native DuckDB table
function (`SELECT * FROM read_qvd('file.qvd')`) would require a C++ extension
and is out of scope for this package.

## Arrow type mapping

| QVD NumberFormat / symbol type | Arrow type                      |
| ------------------------------ | ------------------------------- |
| `DATE`                         | `Date32` (Qlik epoch → Unix)    |
| `TIMESTAMP`                    | `Timestamp(Microsecond, None)`  |
| `TIME`                         | `Duration(Microsecond)`         |
| Int / DualInt symbols          | `Int64`                         |
| Float / DualFloat symbols      | `Float64`                       |
| String symbols                 | `LargeUtf8`                     |
| Empty symbol table             | `Null`                          |

## CLI

The `openqvd` binary ships with the Rust crate:

```
openqvd stat <file>              # header summary (fields, widths, rows)
openqvd head <file> [--rows N]   # first N rows
openqvd csv  <file>              # every row as tab-separated text
openqvd json <file>              # one JSON object per row
openqvd rewrite <in> <out>       # read then re-serialise through the writer
```

## Testing

The reader has been validated against **1,044 of 1,047** valid corpus files
(99.7%). The three failures are deliberately-corrupted test fixtures from
third-party projects (two named `damaged.qvd`, one with invalid UTF-8).

The writer produces semantically equivalent output (`read → write → read`) for
**1,093 of 1,093** valid corpus files.

## License

AGPL-3.0-or-later — see [LICENSE](https://github.com/Sigilweaver/OpenQVD/blob/main/LICENSE).

The **specification** (`SPEC.md`) is licensed under
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
