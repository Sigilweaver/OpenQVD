# OpenQVD

A free, open, clean-room specification and implementation of the Qlik QVD
binary file format, derived purely by binary analysis of publicly available
sample files. The goal is a Rust reader and writer that the data science
community can use without depending on any proprietary Qlik tooling.

## Status

All six originally planned stages are complete:

1. XML header and envelope structure. (Spec section 1.)
2. Per-field symbol table encoding. (Spec section 2.)
3. Bit-packed row index encoding. (Spec section 3.)
4. Validation against the full public corpus via a clean-room Python
   decoder.
5. Rust reader prototype (`crates/openqvd`) with edge-case tests.
6. Writer + semantic round-trip tests.

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
