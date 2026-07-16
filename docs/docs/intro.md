---
title: Introduction
sidebar_label: Intro
slug: /
---

# OpenQVD

**A free, open, clean-room specification and implementation of the Qlik
QVD binary file format.**

OpenQVD is derived purely by binary analysis of a public corpus of
~1,100 `.qvd` files gathered from GitHub (see
[QVD-Sources](https://github.com/Sigilweaver/QVD-Sources)). It ships
no Qlik code or binaries, and reading or writing a `.qvd` file never
requires QlikView, Qlik Sense, or any other Qlik product to be
installed.

## Why

QVD is Qlik's native columnar export format, but reading one outside
the Qlik ecosystem otherwise means round-tripping through a Qlik
install. OpenQVD lets the data science and data engineering community
read and write `.qvd` files directly from Rust, Python, or a
standalone CLI.

## What you get

- A Rust reader and writer (`openqvd` crate) that parses **1,044 of
  1,047** valid public corpus files and round-trips **1,093 of 1,093**
  semantically.
- Python bindings (`openqvd` on PyPI) built on PyArrow, with first-class
  Polars, Pandas, and DuckDB integration.
- Projection and predicate pushdown: skip symbol-table decoding for
  unrequested columns, and filter before row decoding.
- A single-binary `openqvd` CLI for quick inspection and format
  conversion.
- `SPEC.md`: a from-scratch specification of the XML header, symbol
  tables, and bit-packed row encoding, covered in [Format](./format.md).

## Get started

- [Install](./install.md)
- [CLI quickstart](./quickstart-cli.md)
- [Rust quickstart](./quickstart-rust.md)
- [Python quickstart](./quickstart-python.md)
