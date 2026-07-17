---
title: Python API reference
sidebar_label: Python API reference
---

# Python API reference

The `openqvd` wheel is a [maturin](https://maturin.rs/) mixed-layout
package: a PyO3 extension built from the same Rust reader/writer that
backs the CLI, wrapped in a small pure-Python layer that returns
[PyArrow](https://arrow.apache.org/docs/python/) tables.

```python
import openqvd
```

## Module functions

| Function | Signature | Description |
| --- | --- | --- |
| `read` | `read(path, *, columns=None, filters=None) -> pyarrow.Table` | Read a QVD file. `columns` restricts which symbol tables get decoded (projection pushdown). `filters` resolves predicates against symbol tables before row decoding (predicate pushdown). |
| `write` | `write(data, path, *, table_name=None) -> None` | Write a `pyarrow.Table` or `pyarrow.RecordBatch` to a QVD file. `table_name` defaults to the file stem of `path`. |
| `schema` | `schema(path) -> Schema` | Read only the header: table name, row count, and field metadata. Does not decode any symbol tables or row data. |

```python
table = openqvd.read("data.qvd")
table = openqvd.read("data.qvd", columns=["OrderId", "Amount"])
table = openqvd.read("data.qvd", filters=[
    {"column": "Region", "op": "eq", "value": "West"},
    {"column": "Status", "op": "is_in", "value": ["Open", "Pending"]},
    {"column": "Notes",  "op": "is_not_null"},
])

openqvd.write(table, "out.qvd")
openqvd.write(table, "out.qvd", table_name="Orders")
```

### Filter dicts

Each entry in `filters` is a dict with:

| Key | Value |
| --- | --- |
| `column` | Column name (`str`). |
| `op` | One of `eq`, `is_in`, `not_in`, `is_null`, `is_not_null`. |
| `value` | `str` for `eq`; `list[str]` for `is_in`/`not_in`; omitted for the null checks. |

## `Schema`

Returned by `schema()`.

| Member | Type | Description |
| --- | --- | --- |
| `table_name` | `str` | Logical table name from the QVD header. |
| `num_rows` | `int` | Row count. |
| `fields` | `list[FieldInfo]` | One entry per column, in column order. |

## `FieldInfo`

| Member | Type | Description |
| --- | --- | --- |
| `name` | `str` | Column name. |
| `number_format_type` | `str` | The `NumberFormat/Type` hint (`UNKNOWN`, `INTEGER`, `REAL`, `DATE`, ...). Informational; doesn't determine physical encoding. |
| `n_symbols` | `int` | Number of entries in this field's symbol table. |
| `tags` | `list[str]` | Qlik tag hints, e.g. `$numeric`, `$text`, `$key`. |
| `bit_width` | `int` | Width in bits of the packed row-index for this field. |
| `bias` | `int` | Signed bias added to the unpacked bit-field to get the symbol index. |

```python
info = openqvd.schema("data.qvd")
print(info.table_name, info.num_rows)
for f in info.fields:
    print(f.name, f.number_format_type, f.n_symbols, f.tags)
```

## Polars (`openqvd.polars`)

Importing `openqvd.polars` registers `pl.read_qvd`, `pl.scan_qvd`, and
a `.qvd` namespace on `DataFrame`/`LazyFrame`.

| Function | Signature | Description |
| --- | --- | --- |
| `read_qvd` | `read_qvd(path, *, columns=None, filters=None) -> pl.DataFrame` | Eager read. |
| `scan_qvd` | `scan_qvd(path, *, columns=None, filters=None) -> pl.LazyFrame` | Reads eagerly under the hood (column/predicate pushdown still applies at the Rust level) and wraps the result as a `LazyFrame`. |
| `DataFrame.qvd.write` | `write(path, *, table_name=None) -> None` | Write a `DataFrame` back to QVD. |
| `LazyFrame.qvd.collect_and_write` | `collect_and_write(path, *, table_name=None) -> None` | Collect the `LazyFrame` and write the result to QVD. |

```python
import openqvd.polars
import polars as pl

df = pl.read_qvd("data.qvd")
lf = pl.scan_qvd("data.qvd", columns=["A", "B"])
df = pl.read_qvd("data.qvd", filters=[{"column": "A", "op": "eq", "value": "x"}])
df.qvd.write("out.qvd")
```

## Pandas

No dedicated module: convert from the PyArrow table.

```python
df = openqvd.read("data.qvd").to_pandas()
```

## DuckDB (`openqvd.duckdb`)

Install with `pip install "openqvd[duckdb]"`. Provided through Arrow
interop, not a native `read_qvd()` SQL table function (that would need
a C++ extension, out of scope for this project).

| Function | Signature | Description |
| --- | --- | --- |
| `register` | `register(con, name, path) -> None` | Register a QVD file as a named view on an existing DuckDB connection. |
| `to_relation` | `to_relation(path, con=None, *, view_name=None) -> DuckDBPyRelation` | Load a QVD file as a DuckDB relation, optionally also registering it under `view_name`. |
| `from_query` | `from_query(source, path, *, con=None, table_name=None) -> None` | Write a SQL query string or an existing `DuckDBPyRelation` to a QVD file. |

```python
import duckdb
import openqvd.duckdb as qdb

con = duckdb.connect()

qdb.register(con, "orders", "orders.qvd")
con.execute("SELECT COUNT(*) FROM orders WHERE Region = 'West'").fetchone()

rel = qdb.to_relation("orders.qvd", con)

qdb.from_query(
    "SELECT id, amount FROM orders WHERE status = 'Open'",
    "open_orders.qvd",
    con=con,
)
```

## Next

- [Python quickstart](./quickstart-python.md)
- [CLI reference](./cli.md)
- [Format](./format.md)
