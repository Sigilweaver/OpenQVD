# OpenQVD

A free, open, clean-room specification and implementation of the Qlik QVD
binary file format, derived purely by binary analysis of publicly available
sample files. The goal is a Rust reader and writer that the data science
community can use without depending on any proprietary Qlik tooling.

## Status

Seven stages complete:

1. XML header and envelope structure. (Spec section 1.)
2. Per-field symbol table encoding. (Spec section 2.)
3. Bit-packed row index encoding. (Spec section 3.)
4. Validation against the full public corpus via a clean-room Python
   decoder.
5. Rust reader prototype (`crates/openqvd`) with edge-case tests.
6. Writer + semantic round-trip tests.
7. Python bindings (`crates/openqvd-py`) — PyArrow, Polars, Pandas.

See `SPEC.md` for the current specification and `NOTES.md` for the
working log of observations.

### Reader

The Rust reader parses **1,044 of 1,047** valid public QVD samples. The
three remaining files are deliberately-corrupted test fixtures from
third-party projects (two named `damaged.qvd`, one with invalid UTF-8).
10 unit + integration tests cover bias-based NULL, 2+6 bit packing,
zero-width fields, every symbol type byte, unknown-type rejection,
overlapping bit-fields rejection, inconsistent root `Length`
rejection, and the LF-terminator header variant.

### Writer

A compliant writer is implemented in `crates/openqvd::writer`. Running
`read -> write -> read` over the entire corpus yields **1,093 of 1,093
valid files semantically equivalent** (same row count, same field
names, byte-for-byte equal cell values). 9 writer tests cover NULL
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
uv pip install maturin pyarrow polars pandas
maturin develop
```

**Usage**

```python
import openqvd

# Read as a PyArrow Table
table = openqvd.read("data.qvd")
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

# Write from a PyArrow Table
openqvd.write(table, "out.qvd")
openqvd.write(table, "out.qvd", table_name="Orders")

# Polars (import registers pl.read_qvd, pl.scan_qvd, df.qvd.write)
import openqvd.polars
import polars as pl

df = pl.read_qvd("data.qvd")
lf = pl.scan_qvd("data.qvd", columns=["A", "B"])
df = pl.read_qvd("data.qvd", filters=[{"column": "A", "op": "eq", "value": "x"}])
df.qvd.write("out.qvd")

# Pandas (via PyArrow)
df = openqvd.read("data.qvd").to_pandas()
```

The Python bindings read **1,044 of 1,047** valid corpus files (99.7%),
matching the Rust reader baseline. The 3 failures are deliberately-
corrupted test fixtures.

**Arrow type mapping**

| QVD NumberFormat/Type | Arrow type |
|---|---|
| `DATE` | `Date32` (Qlik epoch → Unix epoch) |
| `TIMESTAMP` | `Timestamp(Microsecond, None)` |
| `TIME` | `Duration(Microsecond)` |
| Int / DualInt symbols | `Int64` |
| Float / DualFloat symbols | `Float64` |
| String symbols | `LargeUtf8` |
| Empty symbol table | `Null` |

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

AGPL-3.0-or-later. See `LICENSE`.
