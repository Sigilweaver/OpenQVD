# OpenQVD reverse-engineering notes

Running log of what has been observed, what has been confirmed, and what
is still open. The specification in `SPEC.md` should never contain a claim
that is not backed by an entry here.

## Corpus

- Source: `../QVD-Sources/downloads/`, ~1,089 `.qvd` files.
- 6 of those are Git LFS pointer files (skipped).
- 36 carry a `.qvd` extension but are actually CSV or QVS scripts
  (skipped: they do not start with `<?xml` or `<QvdTableHeader`).
- **1,047 files** parse as real QVD headers. 1,043 use CRLF line endings
  and a `\r\n\x00` header terminator; 4 use LF and `\n\x00`.
- `QvBuildNo` spans the 11000s and 50000s. No structural differences have
  been observed across builds for the features in Stage 1 and 2.

## Stage 1: XML header envelope (confirmed)

- Terminator `</QvdTableHeader>\r\n\x00` or `</QvdTableHeader>\n\x00`.
- `<Compression>` is present and empty in every sample.
- Root `Offset` + `Length` describe the row block inside the body.
- Per-field `Offset` + `Length` describe the symbol table inside the body.
- Symbol tables and the row block do not overlap in any observed file.

## Stage 2: symbol entries (confirmed)

Type byte histogram across 1,045 files (~25 million entries):

| byte | meaning | count |
|---:|---|---:|
| `0x01` | int32 LE | 3,361,575 |
| `0x02` | float64 LE | 922,225 |
| `0x04` | nul-terminated UTF-8 string | 10,633,806 |
| `0x05` | int32 LE + nul-terminated UTF-8 string | 10,252,234 |
| `0x06` | float64 LE + nul-terminated UTF-8 string | 453,558 |
| other | unknown | ~7 (one damaged file) |

All ~25M entries fit this table cleanly.

Example round-trip, file
`korolmi/qvdfile/qvdfile/data/tab1.qvd` field `ID`:

- Symbols at `Offset=0, Length=40`:
  - `06 48 e1 7a 14 ae c7 5e 40  31 32 33 2e 31 32 00`
    -> dual float64 `123.12`, string `"123.12"`
  - `05 7c 00 00 00 31 32 34 00` -> dual int32 124, string `"124"`
  - `05 fe ff ff ff 2d 32 00`    -> dual int32 -2, string `"-2"`
  - `05 01 00 00 00 31 00`       -> dual int32 1, string `"1"`

Five rows, `BitWidth=3, Bias=-2`:

| byte | bits[0..3) | stored | index = stored + Bias | value |
|---|---|---|---|---|
| `02` | 010 | 2 | 0 | `123.12` |
| `0b` | 011 | 3 | 1 | `124` |
| `14` | 100 | 4 | 2 | `-2` |
| `1d` | 101 | 5 | 3 | `1` |
| `20` | 000 | 0 | -2 | NULL |

Confirms the `stored + Bias` rule and the negative-index-means-NULL rule.

## Stage 3: bit packing (confirmed)

Example `ProductGroup.qvd`: `RecordByteSize=2`, 17 rows, two fields at
(`BitOffset=0, BitWidth=8`) and (`BitOffset=8, BitWidth=8`). Row bytes
`00 00 01 01 02 02 ... 10 10` decode with LSB-first byte ordering and
produce identity mappings (0->0, 1->1, ..., 16->16) in each field, as
expected.

Non-byte-aligned widths are common: 816 of 1,045 files have at least one
field whose width is not a multiple of 8. Widths of 1..8 bits are the
most common; rarer packings (ex. 2+6, 3+5, 1+7) also appear. The
examination script confirms that reading each field as a little-endian
bit-field starting at the documented offset reproduces sensible indices
that then resolve against the symbol table.

## Open questions (to investigate next)

1. What is the exact interpretation of `NumberFormat/Type` values like
   `FIX`, `MONEY`, `INTERVAL`? Does it affect how dual floats should be
   displayed?
2. Are there `Tags` combinations that affect physical representation or
   only semantics?
3. Is there any alignment padding inside the symbol block? Currently the
   examination script successfully reads every symbol entry immediately
   adjacent to the previous one; no padding has been observed.
4. Do any QVDs in the wild carry a non-empty `<Compression>` element? So
   far no samples exist.
5. Are there multi-table QVD files anywhere? None observed.

## Open oddities

- `Hankiiee/CodeCoverage/.../mynewfile3.qvd`: ~7 symbol entries begin
  with unknown type bytes including `0x35`, `0xaf`, `0xb5`, `0xb8`,
  `0xc2`, `0x30`. File appears to be corrupted or truncated. Needs a
  closer look, but should not affect the spec.

## Stage 4: end-to-end decode validation (confirmed)

A pure-Python decoder (`re/decode.py`) implemented strictly from
`SPEC.md` was run over the full corpus with `--max-rows 100`. Result:

- 1,044 files decoded cleanly.
- 3 files failed, all expected corruption:
  - `ptarmiganlabs/ctrl-q-qvd-viewer/test-data/misc/damaged.qvd`
    (intentionally corrupted test fixture, filename says so).
  - `MuellerConstantin/qvd4js/__tests__/data/damaged.qvd` (same).
  - `Hankiiee/.../mynewfile3.qvd` (previously flagged).

The decoder enforces every rule in the spec and fails loudly on any
deviation: bit-field overlap, out-of-range symbol indices, trailing
bytes in a symbol table, mis-sized row block, unknown type byte, or
truncated string payload. None of these triggered on a non-damaged
file.

This is strong evidence that Stages 1..3 of the spec are complete and
correct for the uncompressed, single-table QVD files present in the
public corpus.

## Stage 5: Rust reader prototype (first pass)

A Rust crate in `crates/openqvd/` implements the spec independently of
the Python decoder. It exposes `Qvd::from_path` / `Qvd::from_bytes`, a
typed `Value` enum covering the five symbol kinds (int, float, string,
dual-int, dual-float), and a row iterator that yields `Vec<Option<Value>>`
with explicit NULL handling for bias-based nulls.

Validation: the `validate_all` example parses 1,044 of 1,047 files in
the corpus; the 3 failures are the same damaged files that failed the
Python decoder. Unit test `minimal.rs` round-trips a hand-authored
header+symbols+rows payload built from the spec alone.

Known limitations in this first pass:
- Rows block larger than 16 bytes per record uses a slower fallback
  path; still correct, but could be faster.
- `Tags` and `NumberFormat` are preserved but not interpreted.
- No writer yet.

## Stage 6: Writer and round-trip

The writer (`crates/openqvd/src/writer.rs`) implements SPEC section 7.
Per-column plan:

1. Symbols are deduplicated by value and ordered by first occurrence.
   Float keys use `to_bits()` so `-0.0` and `+0.0` are distinct and
   NaN-distinct bit patterns survive round-trip.
2. Bias is `-2` when any NULL is present, else `0`. The smallest
   `BitWidth` that fits `(n_symbols - 1) + (-bias)` is chosen. A column
   with one symbol and no NULLs collapses to `BitWidth=0`.
3. Bit offsets are assigned in declaration order; symbol-block offsets
   likewise; the record byte size is `ceil(total_bits / 8)`.

Row packing uses a u128 fast path when the record fits, and a per-field
byte-level bit writer otherwise. The XML header emits only the minimal
element set from SPEC 7.3 and is byte-deterministic for a given input.

### Corpus round-trip

The `round_trip` example walks a directory, for each QVD runs
`read -> write -> read`, and compares cells one at a time. A size cap
keeps memory bounded (the current `to_write_table` materialises all
cells, which amplifies memory for multi-million-row files; future
improvement would stream directly from reader symbol tables).

Result over `/workspaces/Sigilweaver/QVD-Sources/downloads` with a
128 MiB cap:

```
total=1145 ok=1093 skipped=49 oversize=0 fail=3
```

- 1093 valid files round-trip with cell-for-cell equivalence.
- 3 failures are the same damaged fixtures that already fail the reader.
- 49 skipped are non-QVD bytes (LFS pointers, misnamed CSV/QVS).

Round-trip parity also holds on smaller caps (8 MiB: 1053/1053), so the
writer is not sensitive to corpus subsets.

### CLI

A small `openqvd` binary wraps the library with five subcommands:
`stat`, `head`, `csv`, `json`, `rewrite`. This is the first
user-facing surface and makes the project useful as a standalone tool,
not just a library.

### Remaining follow-ups

- Stream `to_write_table` to remove the memory amplification for very
  large files (>128 MiB). None exist in the public corpus today.
- Enumerate `NumberFormat/Type` values across the corpus and document
  their semantic meaning in SPEC.
- Enumerate `Tags` usage.
- Replace the silent `None` on out-of-range bit-field index with a
  checked API; no corpus file exercises this path today.
