# Changelog

## 1.0.0

Initial public release.

### Reader

- Clean-room Rust reader for Qlik QVD (.qvd) files.
- Parses **1,044 of 1,047** valid public corpus files (99.7%). The three
  failures are deliberately-corrupted test fixtures from third-party projects.
- Handles bias-based NULL encoding, 2+6-bit packing, zero-width fields,
  all five symbol types, and the LF-terminator header variant.
- Projection pushdown: skip symbol decoding for unrequested columns.
- Predicate pushdown: symbol-level filtering before row decoding.
- `checked_rows()` iterator with out-of-range error surfacing.

### Writer

- `Qvd::to_bytes`, `write_to_path`, and `WriteTable` API.
- Semantic round-trip: `read → write → read` yields **1,093 of 1,093** files
  semantically equivalent.
- NULL handling, all five symbol types, zero-width collapse for constant
  columns, NUL-in-string rejection, uneven-column rejection, deterministic
  output.

### Python bindings (`openqvd`)

- `openqvd.read()`, `openqvd.write()`, `openqvd.schema()` — PyArrow-native.
- Projection and predicate pushdown from Python.
- Polars integration: `pl.read_qvd`, `pl.scan_qvd`, `df.qvd.write`.
- Pandas integration via PyArrow.
- Arrow type mapping: DATE → Date32, TIMESTAMP → Timestamp(µs), TIME →
  Duration(µs), Int/Float/String/Null symbols mapped automatically.

### CLI

- `openqvd stat`, `head`, `csv`, `json`, `rewrite` subcommands.

### Specification

- `SPEC.md`: seven-section format specification covering XML header, symbol
  tables, bit-packed rows, and writer semantics.
