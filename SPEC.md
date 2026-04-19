# OpenQVD file format specification (DRAFT)

Status: work in progress, derived by binary analysis of a public corpus of
~1,045 QVD files spanning Qlik build numbers 11000..50700+.

This document is being written in stages. Each section is only added once
the claim is supported by direct observation in the corpus. Speculation is
marked explicitly.

## 0. Overview

A `.qvd` file contains a single tabular dataset with a symbol-indexed,
bit-packed column store representation. The physical layout is:

```
+-------------------------------------------------+
| 1. XML Table Header (UTF-8)                     |
|    starts with '<?xml ... ?>\r\n<QvdTableHeader>'|
|    ends   with '</QvdTableHeader>\r\n'          |
+-------------------------------------------------+
| 2. Single byte: 0x00 (header terminator)        |
+-------------------------------------------------+
| 3. Symbols block (concatenated per field)       |
|    field i symbols live at                      |
|    body[<QvdFieldHeader.Offset>_i ..]           |
|    of length <QvdFieldHeader.Length>_i bytes    |
+-------------------------------------------------+
| 4. Row index block                              |
|    starts at body[<Offset>]                     |
|    length <Length> bytes                        |
|    contains <NoOfRecords> records               |
|    each of <RecordByteSize> bytes               |
+-------------------------------------------------+
```

Where `body = file[header_byte_length..]`. All byte offsets inside the body
are relative to the first byte after the `0x00` header terminator.

All multi-byte numeric fields in the binary body are little-endian.

## 1. XML Table Header

### 1.1 Terminator

The end of the XML header is marked by the byte sequence

```
</QvdTableHeader>\r\n\x00
```

In the observed corpus (1,045 valid files) the CRLF form accounts for
1,043 files and the LF form (`</QvdTableHeader>\n\x00`) accounts for 4.
Readers should accept either.

The single NUL byte (`0x00`) after the closing tag is the header
terminator. It is never part of the XML and is not part of the body.

### 1.2 Encoding and framing

The XML is UTF-8 encoded. The declaration in every observed file is:

```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
```

The root element is `<QvdTableHeader>`. Line endings inside the header are
CRLF in almost all files.

### 1.3 Elements used by a reader

A reader needs only the following elements. Unknown or empty elements are
tolerated but ignored.

Root-level:

| Element | Meaning |
|---|---|
| `TableName` | Logical table name. |
| `NoOfRecords` | Number of rows in the row index block (decimal). |
| `RecordByteSize` | Size of each packed row in bytes (decimal). |
| `Offset` | Byte offset, inside the body, where the row index block starts. |
| `Length` | Byte length of the row index block. Must equal `NoOfRecords * RecordByteSize`. |
| `Fields/QvdFieldHeader` | Repeated, one per column, in column order. |
| `Compression` | Observed empty (`<Compression></Compression>`) in all 1,045 files. See 1.5. |
| `QvBuildNo` | Qlik build that wrote the file. Informational only. |
| `CreateUtcTime`, `CreatorDoc` | Informational provenance. |

Per `QvdFieldHeader`:

| Element | Meaning |
|---|---|
| `FieldName` | Column name (arbitrary UTF-8). |
| `BitOffset` | Bit position inside a row where this field's packed index starts, measured from the least significant bit of byte 0 of the row. |
| `BitWidth` | Width of the packed index in bits. May be zero (see 3.3). |
| `Bias` | Signed integer added to the unpacked bit-field value to obtain the symbol-table index. Usually 0; sometimes -2 when the column permits NULL. |
| `NoOfSymbols` | Number of entries in this field's symbol table. |
| `Offset` | Byte offset, inside the body, where this field's symbol table begins. |
| `Length` | Byte length of this field's symbol table. |
| `NumberFormat/Type` | Logical format hint (`UNKNOWN`, `INTEGER`, `REAL`, `FIX`, `MONEY`, `DATE`, `TIME`, `TIMESTAMP`, `INTERVAL`, `ASCII`, ...). Does not determine physical encoding; see Section 2. |
| `Tags` | Whitespace-delimited hints such as `$numeric`, `$integer`, `$text`, `$key`, `$ascii`, `$date`. |

### 1.4 Consistency rules

All of the following hold in every observed valid file and a compliant
writer must preserve them:

1. For each field, the symbol table is a contiguous byte range fully
   inside the body and disjoint from the row index block.
2. `Length` at the root equals `NoOfRecords * RecordByteSize`.
3. For every row, the bit-fields tile the record exactly: the union of
   `[BitOffset_i, BitOffset_i + BitWidth_i)` over all fields covers some
   subset of `[0, 8 * RecordByteSize)` and never overlaps. Bits left
   untouched after the last field are always zero in the observed corpus.
4. Fields are listed in ascending `BitOffset` in every observed file.

### 1.5 Compression

No file in the corpus carries a non-empty `<Compression>` element. The
element is always present and always empty. A reader MAY reject any
non-empty value until a sample is obtained; this specification does not
cover compressed variants.

## 2. Symbols block

The symbols block is the concatenation of one symbol table per field, laid
out in the order given by the field's `Offset`. Each symbol table contains
`NoOfSymbols` symbol entries, in order of the field's symbol-index 0..N-1.

### 2.1 Symbol entry

Every symbol entry begins with a single **type byte** and is followed by a
type-specific payload. The following type bytes are present across the
corpus; the counts are symbol-entry occurrences across 1,045 files:

| Type byte | Meaning | Payload | Count observed |
|---:|---|---|---:|
| `0x01` | Int32 | 4 bytes, signed, little-endian | 3,361,575 |
| `0x02` | Float64 | 8 bytes, IEEE 754, little-endian | 922,225 |
| `0x04` | String | UTF-8 bytes, terminated by a single `0x00` | 10,633,806 |
| `0x05` | Dual int32 + string | 4 bytes int32 LE, then NUL-terminated UTF-8 string | 10,252,234 |
| `0x06` | Dual float64 + string | 8 bytes float64 LE, then NUL-terminated UTF-8 string | 453,558 |

The "dual" types carry both the numeric value and the exact textual
representation that Qlik should display. Readers that only need the numeric
value may discard the string; readers that want to preserve round-trip
fidelity must keep both.

A trailing `0x00` is always present at the end of string payloads, and it
is not included in the logical string value.

Strings are stored UTF-8. Non-ASCII bytes (for example, multi-byte
sequences) appear literally; there is no length prefix.

### 2.2 Symbol indices

Inside a field's symbol table, the first entry has index 0, the second has
index 1, and so on up to `NoOfSymbols - 1`. These indices are what the row
index block references (after applying `Bias`, see Section 3).

### 2.3 Outliers

Across the entire corpus, fewer than 10 symbol entries out of ~25 million
start with an unrecognised type byte. All are confined to a single file
(`Hankiiee/CodeCoverage/.../mynewfile3.qvd`) whose symbols region appears
damaged. A compliant reader should report a clear error when an unknown
type byte is encountered and refuse to guess.

## 3. Row index block

The row index block immediately follows the symbols block (by the root
`Offset`) and contains `NoOfRecords` rows, each exactly `RecordByteSize`
bytes. Rows are stored back-to-back with no separators.

### 3.1 Bit packing

A row is a little-endian multi-byte integer of `8 * RecordByteSize` bits.
Bit 0 is the least significant bit of byte 0, bit 8 is the least
significant bit of byte 1, and so on.

For a field with `BitOffset = o` and `BitWidth = w`:

```
stored = (row >> o) & ((1 << w) - 1)
```

`stored` is an unsigned integer in `[0, 2^w)`.

### 3.2 Applying the bias

The actual symbol-table index for this row's value of the field is:

```
index = stored + Bias
```

- If `index` is in `[0, NoOfSymbols)`, the field's value is the symbol at
  that index.
- If `index` is negative (only possible when `Bias < 0`), the field's
  value is NULL.

Observed `Bias` values in the corpus: 0 (in the vast majority of fields)
and -2 (in 252 files, most commonly on columns that semantically allow
NULL).

### 3.3 Zero-width fields

A field may have `BitWidth = 0`. In that case `stored` is always 0 and the
row always references symbol index 0 (plus `Bias`). This is how Qlik
stores columns where every row has the same value: only one symbol, no
per-row storage.

### 3.4 Record size

`RecordByteSize` equals `ceil(total_bit_width / 8)` where
`total_bit_width` is the sum of `BitWidth` across all fields. Any unused
high bits in the last byte are zero.

Observed record sizes in the corpus range from 1 byte to 121 bytes, with
the majority in the 1..16 byte range.

## 4. NULL representation

There are two disjoint ways a value can be NULL:

1. **Bias-based NULL**: the field has `Bias < 0` and the row's stored
   value is such that `stored + Bias < 0`. No symbol-table entry is
   consulted.
2. **Missing row** (theoretical): none observed, but structurally any row
   whose bit-field resolves to an out-of-range positive index would also
   be invalid. A reader should treat any `index >= NoOfSymbols` as a
   corruption error.

Qlik's "empty string" and "zero" are ordinary non-NULL values and appear
as dedicated symbols when used.

## 5. Endianness and alignment summary

- All binary integers and floats inside the body are little-endian.
- There is no alignment padding anywhere in the body.
- The header is byte-oriented XML; whitespace inside the XML is
  insignificant for reading but preserved in observed files for Qlik
  round-trip.

## 6. What this spec does not yet cover

- Exact provenance of `CreateUtcTime` and related timestamp fields.
- Full enumeration of `NumberFormat/Type` values and how readers should
  map them to user-facing types.
- Behaviour of `Tags` (`$key`, `$ascii`, etc.) beyond informational use.
- Any compressed variant (no sample present in corpus).
- Multi-table QVDs (none observed; QVD appears to be single-table by
  design).

Future stages will expand these sections once enough signal is available.

## 7. Writing a QVD file

A writer's job is to produce a byte stream that a conforming reader
parses back to the same logical table. OpenQVD's writer targets
**semantic round-trip**: every value read from a source file, when
re-written and re-read, compares equal by the reader's typed API. Exact
byte-level round-trip is not a goal and is not achievable in general
(Qlik's choice of bit widths, symbol ordering, and XML whitespace is not
uniquely determined by the data).

### 7.1 Responsibilities

Given a table (columns, per-column cells where each cell is one of the
five [`Value`] variants or NULL), a writer must:

1. Choose a symbol ordering per column and assign contiguous indices
   `0..NoOfSymbols`. Distinct values must receive distinct indices. NULL
   is not stored as a symbol.
2. Decide whether the column allows NULL. If it does, emit `Bias = -2`
   and reserve the two lowest bit patterns (stored values 0 and 1) for
   NULL (any `index + Bias < 0` decodes as NULL). Otherwise emit
   `Bias = 0`.
3. Compute `BitWidth` as the smallest non-negative integer such that
   `NoOfSymbols + (-Bias) <= 2^BitWidth`, with a special case of
   `BitWidth = 0` when the column has exactly one possible stored value
   (one symbol and no NULLs).
4. Assign `BitOffset` in column order, starting at 0, packing fields
   contiguously with no gaps. Compute `RecordByteSize =
   ceil(sum(BitWidth) / 8)`.
5. Emit each column's symbol table as the concatenation of encoded
   entries (see 7.2) and record its `Offset` and `Length` in the body.
6. Emit the row block: for each row, pack
   `(index_i - Bias_i) << BitOffset_i` for all columns i into an
   integer of `8 * RecordByteSize` bits, little-endian, and emit that
   many bytes.
7. Write the XML header (see 7.3), a single `0x00` terminator, the
   concatenated symbol tables, and the row block, in that order.

Unused high bits in the last byte of a row MUST be zero.

### 7.2 Choosing the symbol type byte

A writer SHOULD choose the narrowest type byte that preserves the
logical value:

- `Value::Int(i32)` -> type byte `0x01`.
- `Value::Float(f64)` -> type byte `0x02`.
- `Value::Str(s)` -> type byte `0x04`.
- `Value::DualInt { number, text }` -> type byte `0x05`.
- `Value::DualFloat { number, text }` -> type byte `0x06`.

Strings MUST be valid UTF-8 and MUST NOT contain an internal `0x00`
byte (the symbol terminator reserved by the format). A writer that
receives a string containing `0x00` MUST signal an error, not silently
truncate.

### 7.3 XML header minimum content

A reader produced from this spec requires only the elements listed in
section 1.3. A writer MUST emit, at minimum:

- `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` followed by
  CRLF.
- `<QvdTableHeader>` root.
- One `<TableName>`.
- `<Fields>` containing one `<QvdFieldHeader>` per column, each with:
  `FieldName`, `BitOffset`, `BitWidth`, `Bias`, `NumberFormat/Type`,
  `NoOfSymbols`, `Offset`, `Length`, `Tags`.
- `<Compression></Compression>` (always empty per section 1.5).
- `<RecordByteSize>`, `<NoOfRecords>`, `<Offset>`, `<Length>`.

All numeric elements are decimal ASCII. Line endings SHOULD be CRLF
(`\r\n`). The header MUST be followed immediately by a single `0x00`
byte and then the body.

A writer MAY additionally emit informational elements (`QvBuildNo`,
`CreatorDoc`, `CreateUtcTime`, etc.); readers ignore unknown or empty
elements.

### 7.4 Determinism

For a given input table, a compliant writer SHOULD be deterministic:
given the same rows in the same order, it should produce the same bytes
on every run. This allows downstream tooling to rely on content
hashing. OpenQVD's reference writer achieves this by:

- Sorting symbols within a column by first-occurrence order, not by
  value. First-occurrence ordering has the useful property that the
  most common values tend to live at low indices, which makes row
  bytes compress well with downstream tools.
- Emitting XML attributes and elements in a fixed order.
- Not embedding timestamps by default.

## Appendix A. Worked example

File: a minimal two-symbol, two-row QVD.

```
-- header (truncated, shown schematically) --
<QvdTableHeader>
  <TableName>Currency</TableName>
  <Fields>
    <QvdFieldHeader>
      <FieldName>Currency</FieldName>
      <BitOffset>0</BitOffset>
      <BitWidth>8</BitWidth>
      <Bias>0</Bias>
      <NoOfSymbols>2</NoOfSymbols>
      <Offset>0</Offset>
      <Length>9</Length>
      <NumberFormat><Type>UNKNOWN</Type>...</NumberFormat>
    </QvdFieldHeader>
  </Fields>
  <Compression></Compression>
  <RecordByteSize>1</RecordByteSize>
  <NoOfRecords>2</NoOfRecords>
  <Offset>9</Offset>
  <Length>2</Length>
</QvdTableHeader>

-- body --
00000000  04 4c 43 00 04 55 53 44  00                   symbols region
00000009  00 01                                         row block (2 rows)
```

Symbol table for `Currency`:

| Index | Raw bytes | Decoded |
|---:|---|---|
| 0 | `04 4c 43 00` | string `"LC"` |
| 1 | `04 55 53 44 00` | string `"USD"` |

Row 0: `00` = stored 0, index 0 -> `"LC"`.
Row 1: `01` = stored 1, index 1 -> `"USD"`.

## Appendix B. Build numbers observed

`QvBuildNo` is an informational field. The corpus contains files from at
least the following builds (truncated, top 15 by count): 50500, 11282,
12354, 50689, 11414, 50504, 50655, 12664, 50522, 50600, 11922, 12018,
50501, 50506, 50642.

No structural differences have been observed that correlate with build
number within the scope covered by this draft.
