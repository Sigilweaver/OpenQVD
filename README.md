# OpenQVD

A free, open, clean-room specification and implementation of the Qlik QVD
binary file format, derived purely by binary analysis of publicly available
sample files. The goal is a Rust reader and writer that the data science
community can use without depending on any proprietary Qlik tooling.

## Status

This project is in the **reverse engineering** phase. The specification is
being built in stages directly from binary observation of a large corpus of
public QVD files. No existing open source QVD parsers have been consulted,
by design, to keep the derivation clean.

See `SPEC.md` for the current specification draft and `NOTES.md` for the
working log of observations.

## Non-goals

- Executing, shipping, or linking any proprietary Qlik code.
- Reading closed or encrypted QVD variants (if they exist).
- Parsing QVW, QVF, or QVS files (those are separate formats).

## Stages

1. XML header and envelope structure.
2. Per-field symbol table encoding (numeric, string, dual).
3. Bit-packed row index encoding.
4. Validation against paired CSV ground truth.
5. Rust reader prototype.
6. Writer and round-trip tests.

## License

AGPL-3.0-or-later. See `LICENSE`.
