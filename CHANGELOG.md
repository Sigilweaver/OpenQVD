# Changelog

All notable changes to this project will be documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Changed

- `CONTRIBUTING.md` overhaul: PRs of any size are welcome without an
  issue first, plus explicit Conventional Commits, ASCII-only, and
  security-reporting requirements.
- Replaced remaining em-dashes with ASCII hyphens across docs and
  source comments.

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
