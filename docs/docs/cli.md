---
title: CLI reference
sidebar_label: CLI reference
---

# CLI reference

```console
$ openqvd --help
```

| Subcommand | What it does |
|---|---|
| `stat`    | Print a header summary: table name, row count, record size, and every field's bit offset/width/bias/symbol count. |
| `head`    | Print the first N rows (default 10) as tab-separated values, with a header row. |
| `csv`     | Print every row as tab-separated values. NULLs become empty fields. |
| `json`    | Print one JSON object per line, one key per field. |
| `rewrite` | Read a file and re-serialise it through the writer. Useful for round-trip smoke testing. |

## Subcommand details

### `stat <file>`

```console
$ openqvd stat data.qvd
table: Orders
rows:  3
record_byte_size: 1  row_block: offset=27 length=3
fields (2):
  [ 0] OrderId                           bits@0+2   bias=0    n_sym=3       type=UNKNOWN  tags=
  [ 1] Region                            bits@2+2   bias=-2   n_sym=2       type=UNKNOWN  tags=
```

(`Region` has `bias=-2` because one of the three rows is NULL for that
column - see [Format: NULL representation](./format.md#null-representation).)

### `head <file> [--rows N]`

```console
$ openqvd head data.qvd --rows 5
```

Prints a tab-separated header row followed by the first `N` rows
(default 10).

### `csv <file>`

```console
$ openqvd csv data.qvd > data.tsv
```

Despite the name, output is tab-separated for simplicity - there is no
quoting to get wrong. NULL cells are empty fields; embedded tabs and
newlines in string values are replaced with a single space.

### `json <file>`

```console
$ openqvd json data.qvd > data.jsonl
```

One JSON object per line (JSON Lines), field name to value. `DualInt`
and `DualFloat` symbols emit their numeric component (falling back to
the text form only when the number is non-finite); NULL becomes
`null`.

### `rewrite <in> <out>`

```console
$ openqvd rewrite data.qvd roundtrip.qvd
wrote roundtrip.qvd
```

Reads `<in>` and re-serialises it through [`Qvd::write_to_path`](./quickstart-rust.md#writing).
The output is semantically equivalent to the input (same row count,
field names, and cell values) but is not guaranteed to be byte-
identical, since the writer chooses its own bit-packing and symbol
layout.
