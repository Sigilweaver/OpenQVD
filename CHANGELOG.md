# Changelog

All notable changes to this project will be documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- `QvdError` is `#[non_exhaustive]` and every variant carries structured
  context (`HeaderNotFound`, `InvalidHeaderValue { element, value, expected }`,
  `RowBlockLengthMismatch`, `SymbolIndexOutOfRange`, `ColumnNotFound`, and so
  on). The `BadHeader(String)`, `Xml(String)` and `Structure(String)` variants
  are gone. `QvdError::kind()` returns a `#[non_exhaustive]` `ErrorKind`
  (`Io`, `Header`, `Structure`, `Symbol`, `Schema`, `Conversion`,
  `Unsupported`) for coarse handling.
- `Qvd::open(path, &ReadOptions)` and `Qvd::parse(bytes, &ReadOptions)`
  replace `from_path`, `from_path_projected`, `from_bytes` and
  `from_bytes_projected`. `ReadOptions` holds the column projection and a
  `Leniency`.
- `Leniency` is one set of named `LenientCheck`s. The default is strict.
  Anything a lenient read tolerates is returned by `Qvd::warnings()` as a
  `Warning` with a `WarningKind`.
- `Qvd::rows()` yields `Result<Vec<Cell>, QvdError>`; `checked_rows()` and
  `CheckedRowIter` are removed. A symbol index outside the symbol table is an
  error instead of a NULL.
- `Qvd::to_write_table()` returns a `Result`. `Qvd::symbols()` and
  `Qvd::header_size()` are no longer public.
- `WriteTable` fields are private; use `name()` and `columns()`.
  `record_batch_to_write_table` takes `&WriteOptions`.
- `Value`, `Dual`, `NumberFormat`, `FieldHeader`, `TableHeader`, `Column`,
  `Filter` and `ColumnFilter` are `#[non_exhaustive]`. Build them with
  `Dual::new`, `NumberFormat::new(..).with_*`, `Column::new` and
  `Filter::new`.
- Python: every error derives from `openqvd.QvdError` (a `ValueError`), with
  `QvdIOError` (also `OSError`; `QvdFileNotFoundError` and
  `QvdPermissionError` are also `FileNotFoundError` and `PermissionError`),
  `QvdFormatError` (`QvdHeaderError`, `QvdStructureError`, `QvdSymbolError`),
  `QvdSchemaError`, `QvdConversionError` and `QvdUnsupportedError`. The class
  is chosen from the Rust error kind, not from the message text; previously
  an unknown symbol type surfaced as `RuntimeError`.
- Python: `read`, `schema` and `write` take a keyword-only `leniency`
  (`openqvd.Leniency` or an iterable of check names); `schema` is keyword-only
  after `path`. `Schema.warnings` and `openqvd.QvdWarning` report what a
  lenient read tolerated.

- Arrow is out of the core crate. `openqvd` has no Arrow dependency and no
  `arrow` feature. The new `openqvd-arrow` crate converts between `Qvd` and
  Arrow `RecordBatch`es, with one feature and module per Arrow major
  (`arrow-59` / `openqvd_arrow::v59`, `arrow-60` / `v60`); both can be enabled
  at once. A new Arrow major is a new feature, not a new openqvd major.
  `Qvd::to_record_batch`, `to_record_batch_filtered` and the root-level
  `record_batch_to_write_table` are replaced by
  `vNN::to_record_batch(&qvd, &BatchOptions)`,
  `vNN::record_batch_to_write_table` and `vNN::record_batches_to_write_table`.
- The core crate exposes Arrow-free decoded columns: `Qvd::column(i)` returns a
  `ColumnData` (a `ColumnType`, one converted value per distinct symbol, and
  the symbol index of each row), and `Qvd::select_rows(&[Filter])` returns the
  rows matching filters. `Filter` and `ColumnFilter` moved here from the
  Arrow module.
- Python: `openqvd.read` returns `openqvd.Table`, not `pyarrow.Table`. It has
  `num_rows`, `column_names`, `warnings`, `__arrow_c_stream__`,
  `__arrow_c_schema__`, `to_pyarrow()`, `to_polars()` and `to_pandas()`.
  `openqvd.write` accepts any object with `__arrow_c_stream__` or
  `__arrow_c_array__`.
- Python: pyarrow is no longer a dependency. New extras: `pyarrow`, `polars`,
  `pandas`, `duckdb`, `all`. Minimums: pyarrow 15, Polars 1.10, pandas 2.2,
  DuckDB 1.2 (each checked with the test suite).
- Python: wheels are `abi3` (`cp310-abi3`): one wheel per platform covers every
  CPython 3.10 and later. PyPy is no longer a declared target.
- Python: `pl.from_arrow(openqvd.read(...))` returns a `Series`, not a
  `DataFrame`; use `Table.to_polars()` or `polars.DataFrame(table)`.

### Fixed

- XML entity references and CDATA in the header were silently deleted:
  `P&amp;L` read as `PL` and `Customer&apos;s` as `Customers`. Predefined
  entities, numeric character references and CDATA are resolved; any other
  entity is an error. Text is no longer trimmed, so `<Thou> </Thou>` keeps its
  space (S1-2).
- A missing, self-closing or duplicated numeric header element (per-field
  `BitOffset`, `BitWidth`, `Bias`, `NoOfSymbols`, `Offset`, `Length`; root
  `RecordByteSize`, `NoOfRecords`, `Offset`, `Length`) used to default to 0.
  That decoded every row as symbol 0, or opened a file with 0 rows. Each is now
  an error (S1-7).
- `<Compression>` is read; a non-empty value is `UnsupportedCompression`
  instead of being decoded as if uncompressed (S1-8).
- A body containing a CRLF-style header terminator could split an LF-terminated
  header at the wrong place. The earliest terminator wins, and the root
  element must be `QvdTableHeader` (S3-3).
- `Qvd::to_bytes` and `openqvd rewrite`: `to_bytes` returns the original header
  bytes followed by the body, so build number, creator, lineage, per-field
  comments and unknown elements survive (S2-5). `openqvd rewrite` and the
  `round_trip` example now re-encode through the writer, as documented.
- `write` wrote only the first batch of a table whose column could not be
  combined into one chunk; every batch is now converted (S2-4).
- A filter on a column holding NaN no longer drops the NaN rows: rows are
  matched by symbol index (S2-3, the NaN half).
- Filtering a table that has an all-NULL column no longer fails with an Arrow
  length error.
- Reading a column that was left out of `ReadOptions::columns` is an error
  (`FieldNotDecoded`) instead of an all-NULL column. This covers `column()`,
  the Arrow adapters, filters, `Qvd::rows()` (first item is the error) and
  `to_write_table()` (S2-6).
- `read(columns=[])` returns a table with no columns and the right row count
  instead of an Arrow error.
- `openqvd.duckdb.to_relation(path)` without a connection returned a relation
  on a closed connection (DuckDB 1.2); it now uses DuckDB's default connection.
  `view_name` without `con` is a `ValueError`.

### Behavior changes

- Files whose header lacks any numeric element, repeats one, has a non-empty
  `<Compression>`, or uses an undefined XML entity no longer open. None of the
  1,044 readable corpus files does.
- Field and table names containing `&`, `<`, `>`, `'` or `"` (stored as
  entities) now read with those characters; before they read with the
  characters removed. 18 corpus files carry such entities.
- `<Thou> </Thou>` reads as a single space, not empty.
- The error for a header that is not a QVD document names the unexpected root
  element.

- Asking for a column that is not in the file now fails when the file is
  opened, for every projection, not only when converting to Arrow.
- Python `schema()` reads the header only and no longer decodes symbol
  tables, so it does not fail on damaged symbol data.
- The `validate_all` and `round_trip` examples decode every row, so index
  corruption is no longer invisible to corpus runs.

### Changed

- `rust-version` is 1.88, the highest minimum among the dependencies (Arrow 60).
  CI runs clippy on all targets and a job that installs the declared Python
  minimums (pyarrow 15, polars 1.10, pandas 2.2, duckdb 1.2). Dependabot no
  longer proposes Arrow major bumps for the version-named features.

- Python tests read the corpus location from `OPENQVD_CORPUS_DIR` instead of
  a fixed path, and fail instead of skipping when `REQUIRE_CORPUS=1` and a
  corpus file is missing.
- Bumped `quick-xml` to 0.42 and `thiserror` to 2.0.21. QVD header parsing
  behaves as before.

## [2.0.0] - 2026-10-17

### Breaking

- Python 3.9 is no longer supported; Python 3.10 or later is required.
- The Rust `arrow` feature now exposes Arrow 59 types instead of Arrow 58
  types. Downstream Rust applications must use matching Arrow dependencies.
- Python dependency minimums are now PyArrow 25.0.1, Polars 1.43.2,
  Pandas 2.3.3, and DuckDB 1.5.5 for the corresponding integrations.

### Fixed

- Pin the Python build backend to Maturin 1.14.1, which safely relocates the
  workspace README into the sdist instead of creating parent-relative
  archive paths. Include the workspace Apache-2.0 LICENSE in both the
  sdist and wheel through the Python project's license file metadata.

### Security

- `decode_field_symbols` (`symbols.rs`) sized its per-field `Vec` from the
  file-controlled `NoOfSymbols` count before the bounded decode loop ran, so
  a field claiming billions of symbols backed by only a few actual bytes
  could force a multi-GB up-front allocation. The pre-allocation is now
  capped at the actual symbol region's length, since every symbol consumes
  at least one byte.
- `Qvd::to_write_table` (`reader.rs`) sized each column's `Vec` from
  `NoOfRecords` directly. `NoOfRecords` is bounded by the row block's real
  size whenever `RecordByteSize > 0`, but a degenerate header with
  `RecordByteSize == 0` leaves it unconstrained, so a crafted count could
  force the same class of oversized allocation. Pre-allocation is now
  capped at a generous fixed ceiling (256 MiB worth of rows); larger row
  counts are still produced correctly via the Vec's normal growth.
  Fixes #1. (@Nabejo)

### Added

- Docusaurus docs site (`docs/`), deploying to
  `sigilweaver.app/openqvd/docs/` for parity with the other readers in
  the portfolio. Covers intro, install, CLI/Rust/Python quickstarts, a
  full CLI reference, a Python API reference (core `read`/`write`/
  `schema` plus the Polars and DuckDB integrations), and a format
  orientation page pointing into `SPEC.md`. A `Docs` badge was added to
  the root README. (@Nabejo)
- Zenodo DOI badge and `identifiers:` entry in `CITATION.cff`.
- `.github/PULL_REQUEST_TEMPLATE.md` and a bug report issue template.
- `cargo-audit` CI workflow (`.github/workflows/audit.yml`), running on
  every push/PR touching `Cargo.lock`/manifests plus a weekly schedule.
- `scripts/check-release-ready.sh` and `RELEASING.md`: pre-tag check that
  the target commit's most recent `ci.yml` and `audit.yml` runs both
  completed successfully, plus a short release checklist (closes #7).

### Changed

- `CONTRIBUTING.md` overhaul: PRs of any size are welcome without an
  issue first, plus explicit Conventional Commits, ASCII-only, and
  security-reporting requirements.
- Replaced remaining em-dashes with ASCII hyphens across docs and
  source comments.
- Bumped `quick-xml` to 0.41 and `pyo3`/`pyo3-arrow` to 0.29/0.19
  (with `arrow-*` bumped to 59 to match), clearing the RUSTSEC
  advisories tracked in #4; `.github/workflows/audit.yml` no longer
  ignores them.
- CI (`.github/workflows/ci.yml`) now runs the Rust and Python test
  matrices on `macos-latest` and `windows-latest` in addition to
  `ubuntu-latest`, matching the platforms the wheel-building workflow
  ships for (#2). Unverified on real Windows/macOS runners as of this
  change; watch the first CI run on `main`.

## [1.2.0] - 2026-05-31

### Added

- `CITATION.cff`: author identity (Nathan Riley + ORCID) and a
  scaffolded `identifiers:` block ready for the Zenodo concept DOI.
- `SECURITY.md` with private GHSA reporting policy.
- `CONTRIBUTING.md` with PR checklist and DCO.
- README badges (CI, license, MSRV, crates.io, PyPI).

### Changed

- **Relicensed from AGPL-3.0-or-later to Apache-2.0.** The `LICENSE`
  file, all crate and Python package manifests, and all source-file
  references have been updated. The specification remains
  CC-BY-SA-4.0.
- **Panic surface eliminated (WP17).** Library and CLI no longer
  call `unwrap()` in production code: a new `bytes` helper module
  (`read_i32/f64_le`) returns `Error::Parse` with byte offset,
  Arrow downcasts use `ok_or_else(...)`, and CLI `writeln!` /
  `write!` calls propagate via `?`. Library crates carry
  `#![cfg_attr(not(test), warn(clippy::unwrap_used,
  clippy::expect_used))]`.
- README badge block unified across the Sigilweaver portfolio.
- Documentation consolidated to a single root README; manifests
  point at `../../README.md`.

## [1.1.0] - 2026-04-22

### Added

- **DuckDB integration** - new `openqvd.duckdb` module with three helpers:
  - `openqvd.duckdb.register(con, name, path)` - register a QVD file as a named
    view on an existing DuckDB connection (reads eagerly into Arrow, then registers).
  - `openqvd.duckdb.to_relation(path, con=None, *, view_name=None)` - load a QVD
    file as a DuckDB relation; optionally register it under a SQL view name.
  - `openqvd.duckdb.from_query(source, path, *, con=None, table_name=None)` - write
    a DuckDB SQL string or `DuckDBPyRelation` to a QVD file via Arrow interop.
    Normalises both `pyarrow.Table` and `pyarrow.RecordBatchReader` return shapes
    from `rel.arrow()` across DuckDB versions.
- New `duckdb` optional extra (`pip install openqvd[duckdb]`) pulls in DuckDB +
  PyArrow. The `all` extra now includes DuckDB as well.
- 8 new pytest tests in `tests/test_duckdb.py` covering `to_relation`,
  `register`, `from_query` with SQL string and relation inputs, table-name
  embedding, error paths, and a full round-trip.

---

## 1.0.0 - 2026-04-20

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

- `openqvd.read()`, `openqvd.write()`, `openqvd.schema()` - PyArrow-native.
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
