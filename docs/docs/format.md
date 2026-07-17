---
title: Format
sidebar_label: Format
---

# Format

This is a high-level orientation map. The full, byte-level
specification - derived by binary analysis of ~1,045 corpus files - is
`SPEC.md` in the repository root: see it rendered on
[GitHub](https://github.com/Sigilweaver/OpenQVD/blob/main/SPEC.md).

## Physical layout

A `.qvd` file is four regions back to back:

```
+-------------------------------------------------+
| 1. XML Table Header (UTF-8)                     |
|    <?xml ... ?>\r\n<QvdTableHeader> ... </QvdTableHeader>\r\n
+-------------------------------------------------+
| 2. Single byte: 0x00 (header terminator)        |
+-------------------------------------------------+
| 3. Symbols block (concatenated per field)       |
|    field i's symbols live at                    |
|    body[QvdFieldHeader.Offset_i ..], length      |
|    QvdFieldHeader.Length_i bytes                |
+-------------------------------------------------+
| 4. Row index block                              |
|    starts at body[Offset], length Length bytes  |
|    NoOfRecords records, RecordByteSize bytes each|
+-------------------------------------------------+
```

`body` is everything after the header terminator; all offsets inside
it are relative to the first byte after the `0x00`. Every multi-byte
number in the binary body is little-endian.

## Symbol tables

Each field owns a symbol table: a sequence of `NoOfSymbols` typed
entries, one of five type bytes:

| Byte | Type | Encoding |
| --- | --- | --- |
| `0x01` | Int | `i32` little-endian |
| `0x02` | Float | `f64` little-endian |
| `0x04` | String | NUL-terminated UTF-8 |
| `0x05` | DualInt | `i32` little-endian, then a NUL-terminated UTF-8 text form |
| `0x06` | DualFloat | `f64` little-endian, then a NUL-terminated UTF-8 text form |

Dual types carry both a number and a display string (for example the
number `1` displayed as `"Jan"`); which one a consumer wants depends
on context, so OpenQVD's `Value` enum preserves both.

## Bit-packed rows

Rather than storing symbol values inline, each row is a fixed-size,
bit-packed record: field `i`'s row value is a symbol-table index
occupying `BitWidth_i` bits starting at bit `BitOffset_i` (LSB-first).
The unpacked bit-field value plus the field's signed `Bias` gives the
symbol index. A field with `BitWidth = 0` always resolves to symbol
index 0 for every row - the standard encoding for a constant column.

## NULL representation

There is no dedicated NULL bit. A row resolves to NULL when the
unpacked bit-field value plus the field's (negative) `Bias` is less
than zero; no symbol-table entry is consulted in that case. The common
case observed in the corpus is `Bias = -2` with a stored value of `0`,
which keeps index `0` addressing a real (if otherwise unused)
symbol-table slot for tooling that doesn't special-case NULL.

## Writing

A compliant writer must preserve the consistency rules SPEC.md
section 1.4 lists: each field's symbol table is a contiguous range
disjoint from the row block, the root `Length` equals
`NoOfRecords * RecordByteSize`, and per-row bit-fields tile the record
without overlapping. OpenQVD's writer (`crates/openqvd::writer`)
chooses its own bit-packing and symbol layout - a round trip through
it is not guaranteed to be byte-identical to the input, only
semantically equivalent (same row count, field names, and cell
values).

## Where to read the code

- `crates/openqvd/src/header.rs` - XML header parsing and consistency
  checks
- `crates/openqvd/src/symbols.rs` - symbol table decoding
- `crates/openqvd/src/reader.rs` - bit-field extraction and row
  iteration
- `crates/openqvd/src/writer.rs` - `WriteTable`/`Column` and the
  writer
- `crates/openqvd/src/arrow.rs` - Arrow `RecordBatch` conversion
  (feature-gated)
