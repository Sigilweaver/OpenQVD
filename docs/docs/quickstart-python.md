---
title: Python quickstart
sidebar_label: Python quickstart
---

# Python quickstart

```sh
pip install openqvd
```

```python
import openqvd

# Read as a PyArrow Table
table = openqvd.read("data.qvd")
table = openqvd.read("data.qvd", columns=["OrderId", "Amount"])

# Predicate pushdown (filtering before Arrow conversion)
table = openqvd.read("data.qvd", filters=[
    {"column": "Region", "op": "eq", "value": "West"},
    {"column": "Status", "op": "is_in", "value": ["Open", "Pending"]},
])

# Inspect metadata only (no row decoding)
info = openqvd.schema("data.qvd")
print(info.table_name, info.num_rows, [f.name for f in info.fields])

# Write from a PyArrow Table
openqvd.write(table, "out.qvd")
```

## With Polars

```python
import openqvd.polars  # registers pl.read_qvd, pl.scan_qvd, df.qvd.write()
import polars as pl

df = pl.read_qvd("data.qvd")
lf = pl.scan_qvd("data.qvd", columns=["A", "B"])
df.qvd.write("out.qvd")
```

## With Pandas

```python
df = openqvd.read("data.qvd").to_pandas()
```

See the [Python API reference](./python-api.md) for the full surface,
including the DuckDB integration.
