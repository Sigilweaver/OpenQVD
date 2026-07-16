---
title: Install
sidebar_label: Install
---

# Install

## Command-line tool

```sh
cargo install openqvd
openqvd --help
```

## Python

```sh
pip install openqvd
```

Wheels bundle PyArrow-backed reading and writing. Add `[duckdb]` for
the DuckDB integration:

```sh
pip install "openqvd[duckdb]"
```

## Rust library

```toml
[dependencies]
openqvd = "1"

# Enable Arrow integration (PyArrow, RecordBatch, type inference):
openqvd = { version = "1", features = ["arrow"] }
```

MSRV: Rust 1.75.

## From source

```sh
git clone https://github.com/Sigilweaver/OpenQVD
cd OpenQVD
cargo build --workspace --release
./target/release/openqvd --help
```

For the Python bindings:

```sh
cd crates/openqvd-py
uv venv .venv && source .venv/bin/activate
uv pip install maturin pyarrow polars pandas duckdb
maturin develop --release --features pyo3/extension-module
```
