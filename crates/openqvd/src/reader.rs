use std::fs;
use std::path::Path;

use crate::error::QvdError;
use crate::header::{parse, FieldHeader, TableHeader};
use crate::options::ReadOptions;
use crate::symbols::decode_field_symbols;
use crate::value::{Cell, Value};
use crate::warning::Warning;

/// Upper bound on how many rows [`Qvd::to_write_table`] will eagerly
/// pre-allocate `Vec` capacity for, per column. 256 MiB worth of `Cell`s is
/// far beyond any real QVD extract's row count and cheap to allocate, while
/// still ruling out a crafted `NoOfRecords` forcing a multi-GB allocation
/// (see the comment at its use site for why `NoOfRecords` alone isn't a
/// trustworthy bound here).
const MAX_PREALLOC_ROWS: usize = 256 * 1024 * 1024 / std::mem::size_of::<Cell>();
const _: () = assert!(MAX_PREALLOC_ROWS > 0 && MAX_PREALLOC_ROWS < u32::MAX as usize);

/// An in-memory QVD file with headers, symbol tables, and the packed row
/// index block.
///
/// Open one with [`Qvd::open`] or [`Qvd::parse`]. Strict reading is the
/// default; see [`ReadOptions`].
#[derive(Debug)]
pub struct Qvd {
    header: TableHeader,
    /// The header exactly as it appeared in the file, terminator included.
    header_raw: Vec<u8>,
    /// The bytes of the file after the header terminator.
    body: Vec<u8>,
    /// One decoded symbol table per field, in field order.
    symbol_tables: Vec<Vec<Value>>,
    /// Whether each field's symbol table was decoded (false for fields
    /// outside `ReadOptions::columns`).
    decoded: Vec<bool>,
    /// Deviations tolerated because `ReadOptions::leniency` allowed them.
    warnings: Vec<Warning>,
}

impl Qvd {
    /// Read and parse a QVD file from disk.
    pub fn open(path: impl AsRef<Path>, options: &ReadOptions) -> Result<Self, QvdError> {
        let bytes = fs::read(path.as_ref())?;
        Self::parse(bytes, options)
    }

    /// Parse a QVD file from a byte vector already in memory.
    ///
    /// When `options.columns` is set, symbol tables are decoded only for the
    /// named columns; the others keep an empty table and must not be read.
    /// This avoids decoding large string tables that will never be used.
    pub fn parse(mut bytes: Vec<u8>, options: &ReadOptions) -> Result<Self, QvdError> {
        let (header, header_bytes) = parse(&bytes)?;
        let body: Vec<u8> = bytes.split_off(header_bytes);
        let header_raw = bytes;

        if let Some(names) = &options.columns {
            for n in names {
                if !header.fields.iter().any(|f| f.name == *n) {
                    return Err(QvdError::ColumnNotFound { name: n.clone() });
                }
            }
        }
        let warnings: Vec<Warning> = Vec::new();

        // Structural invariants required by the spec.
        let expected_len = header
            .no_of_records
            .checked_mul(header.record_byte_size)
            .ok_or(QvdError::RowBlockSizeOverflow)?;
        if expected_len != header.row_block_length {
            return Err(QvdError::RowBlockLengthMismatch {
                declared: header.row_block_length,
                expected: expected_len,
            });
        }
        // Bit-field coverage check.
        let total_bits = header.record_byte_size.saturating_mul(8);
        let mut used = vec![false; total_bits as usize];
        for f in &header.fields {
            if f.bit_width == 0 {
                continue;
            }
            let out_of_range = || QvdError::FieldBitsOutOfRange {
                field: f.name.clone(),
                bit_offset: f.bit_offset,
                bit_width: f.bit_width,
                record_bits: total_bits,
            };
            let end = f
                .bit_offset
                .checked_add(f.bit_width)
                .ok_or_else(out_of_range)?;
            if end > total_bits {
                return Err(out_of_range());
            }
            for b in f.bit_offset..end {
                if used[b as usize] {
                    return Err(QvdError::FieldBitsOverlap {
                        field: f.name.clone(),
                        bit: b,
                    });
                }
                used[b as usize] = true;
            }
        }

        let mut symbol_tables = Vec::with_capacity(header.fields.len());
        let mut decoded = Vec::with_capacity(header.fields.len());
        for f in &header.fields {
            let dominated = match &options.columns {
                None => false,
                Some(names) => !names.contains(&f.name),
            };
            decoded.push(!dominated);
            if dominated {
                symbol_tables.push(Vec::new());
            } else {
                symbol_tables.push(decode_field_symbols(&body, header_bytes, f)?);
            }
        }

        // Row block must lie within the body.
        let row_start = u64::from(header.row_block_offset);
        let row_end = row_start + u64::from(header.row_block_length);
        if row_end > body.len() as u64 {
            return Err(QvdError::RowBlockOutOfRange {
                start: row_start,
                end: row_end,
                body_len: body.len(),
            });
        }

        Ok(Self {
            header,
            header_raw,
            body,
            symbol_tables,
            decoded,
            warnings,
        })
    }

    /// Table name from `<TableName>`.
    pub fn table_name(&self) -> &str {
        &self.header.table_name
    }

    /// Number of rows.
    pub fn num_rows(&self) -> u32 {
        self.header.no_of_records
    }

    /// Fields in column order.
    pub fn fields(&self) -> &[FieldHeader] {
        &self.header.fields
    }

    /// Full table header.
    pub fn header(&self) -> &TableHeader {
        &self.header
    }

    /// Deviations that a lenient read tolerated. Always empty for a strict
    /// read.
    pub fn warnings(&self) -> &[Warning] {
        &self.warnings
    }

    /// Whether field `index` had its symbol table decoded (false for fields
    /// outside `ReadOptions::columns`).
    pub fn is_decoded(&self, index: usize) -> bool {
        self.decoded.get(index).copied().unwrap_or(false)
    }

    /// Decoded symbol table for the given field index.
    pub(crate) fn symbols(&self, field_index: usize) -> Option<&[Value]> {
        self.symbol_tables.get(field_index).map(|v| v.as_slice())
    }

    /// Iterate rows. Each row is a `Vec<Cell>` with one entry per field in
    /// the same order as [`Self::fields`]. `None` denotes a NULL.
    ///
    /// If any field was left out of `ReadOptions::columns` its symbol table
    /// was never decoded, so the first item is
    /// `Err(QvdError::FieldNotDecoded)` and the iterator ends. Use
    /// [`Self::column`] for projected reads.
    ///
    /// A row that refers to a symbol that does not exist yields
    /// `Err(QvdError::SymbolIndexOutOfRange { .. })`; corruption is never
    /// turned into a NULL.
    pub fn rows(&self) -> impl Iterator<Item = Result<Vec<Cell>, QvdError>> + '_ {
        Rows { qvd: self, next: 0 }
    }

    /// The file as bytes: the original header, untouched, followed by the
    /// body. Nothing the reader does not interpret (build number, creator,
    /// lineage, per-field comments, unknown elements) is lost. To produce a
    /// new file from the decoded values use [`Self::to_write_table`].
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::QvdError> {
        let mut out = Vec::with_capacity(self.header_raw.len() + self.body.len());
        out.extend_from_slice(&self.header_raw);
        out.extend_from_slice(&self.body);
        Ok(out)
    }

    /// Write the QVD back to a file on disk. Uses [`Self::to_bytes`].
    pub fn write_to_path(&self, path: impl AsRef<std::path::Path>) -> Result<(), crate::QvdError> {
        use std::io::Write;
        let bytes = self.to_bytes()?;
        let mut f = std::fs::File::create(path.as_ref())?;
        f.write_all(&bytes)?;
        Ok(())
    }

    /// Materialise this QVD as a [`crate::WriteTable`], one column per
    /// field, in the same order. Useful for round-trip testing and
    /// programmatic rewriting.
    pub fn to_write_table(&self) -> Result<crate::WriteTable, QvdError> {
        if let Some(i) = self.decoded.iter().position(|d| !d) {
            return Err(QvdError::FieldNotDecoded {
                field: self.header.fields[i].name.clone(),
            });
        }
        let n_rows = self.num_rows() as usize;
        // `no_of_records` is bounded by the actual row block size in
        // `from_bytes_impl` whenever `record_byte_size > 0` (row_block_length
        // = no_of_records * record_byte_size <= body.len()). But a
        // degenerate header with `record_byte_size == 0` leaves
        // `no_of_records` unconstrained by any real file size, so a crafted
        // count could otherwise force a huge up-front `Vec::with_capacity`
        // per column here. Cap the pre-allocation at a generous fixed
        // ceiling instead of trusting `n_rows` directly; rows beyond it are
        // still produced correctly via the push loop below, just through
        // the Vec's normal growth.
        let prealloc = n_rows.min(MAX_PREALLOC_ROWS);
        let mut columns: Vec<crate::Column> = self
            .header
            .fields
            .iter()
            .map(|f| crate::Column {
                name: f.name.clone(),
                cells: Vec::with_capacity(prealloc),
                number_format: f.number_format.clone(),
                tags: f.tags.clone(),
            })
            .collect();
        for row in self.rows() {
            for (i, cell) in row?.into_iter().enumerate() {
                columns[i].cells.push(cell);
            }
        }
        crate::WriteTable::new(self.header.table_name.clone(), columns)
    }
}

/// Iterator over decoded rows.
struct Rows<'a> {
    qvd: &'a Qvd,
    next: u32,
}

impl Iterator for Rows<'_> {
    type Item = Result<Vec<Cell>, QvdError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.qvd.header.no_of_records {
            return None;
        }
        if self.next == 0 {
            // A column outside `ReadOptions::columns` has no symbol table.
            // Reading it as NULL would be indistinguishable from real NULLs
            // (and `to_write_table` would launder it into a valid file), so
            // the first row reports it and the iterator ends.
            if let Some(i) = self.qvd.decoded.iter().position(|d| !d) {
                self.next = self.qvd.header.no_of_records;
                return Some(Err(QvdError::FieldNotDecoded {
                    field: self.qvd.header.fields[i].name.clone(),
                }));
            }
        }
        let idx = self.next as usize;
        self.next += 1;
        let rec = self.qvd.record(idx);
        let rec_int = le_bits_to_u128(rec);
        let mut out: Vec<Cell> = Vec::with_capacity(self.qvd.header.fields.len());
        for (i, f) in self.qvd.header.fields.iter().enumerate() {
            match self.qvd.resolve(idx, i, f, rec, rec_int) {
                Ok(None) => out.push(None),
                Ok(Some(sym)) => out.push(Some(self.qvd.symbol_tables[i][sym].clone())),
                Err(e) => return Some(Err(e)),
            }
        }
        Some(Ok(out))
    }
}

impl Qvd {
    /// Bytes of record `idx` inside the row block.
    fn record(&self, idx: usize) -> &[u8] {
        let rbs = self.header.record_byte_size as usize;
        let rows_off = self.header.row_block_offset as usize;
        &self.body[rows_off + idx * rbs..rows_off + (idx + 1) * rbs]
    }

    /// Resolve field `i` of record `idx` to a symbol index, `None` for NULL.
    fn resolve(
        &self,
        idx: usize,
        i: usize,
        f: &FieldHeader,
        rec: &[u8],
        rec_int: u128,
    ) -> Result<Option<usize>, QvdError> {
        let rbs = rec.len();
        let stored = if f.bit_width == 0 {
            0
        } else if rbs * 8 <= 128 {
            let mask = if f.bit_width == 128 {
                u128::MAX
            } else {
                (1u128 << f.bit_width) - 1
            };
            ((rec_int >> f.bit_offset) & mask) as i128
        } else {
            extract_bits_wide(rec, f.bit_offset, f.bit_width) as i128
        };
        let index = stored + f.bias as i128;
        if index < 0 {
            return Ok(None);
        }
        let ui = index as usize;
        if ui >= self.symbol_tables[i].len() {
            return Err(QvdError::SymbolIndexOutOfRange {
                row: idx as u64,
                field: f.name.clone(),
                index: ui as u64,
                symbols: self.symbol_tables[i].len(),
            });
        }
        Ok(Some(ui))
    }

    /// Symbol index of every row for field `field`, with [`NULL_INDEX`]
    /// marking NULL. Errors if the field was not decoded or a row points
    /// outside the symbol table.
    pub(crate) fn column_indices(&self, field: usize) -> Result<Vec<u32>, QvdError> {
        let f = &self.header.fields[field];
        if !self.decoded[field] {
            return Err(QvdError::FieldNotDecoded {
                field: f.name.clone(),
            });
        }
        let n = self.header.no_of_records as usize;
        let mut out: Vec<u32> = Vec::with_capacity(n.min(MAX_PREALLOC_ROWS));
        for idx in 0..n {
            let rec = self.record(idx);
            let rec_int = le_bits_to_u128(rec);
            out.push(match self.resolve(idx, field, f, rec, rec_int)? {
                None => NULL_INDEX,
                Some(sym) => u32::try_from(sym).map_err(|_| QvdError::LayoutOverflow {
                    what: "symbol index",
                })?,
            });
        }
        Ok(out)
    }
}

/// Marks a NULL in the index vectors built by `Qvd::column_indices`.
pub(crate) const NULL_INDEX: u32 = u32::MAX;

fn le_bits_to_u128(bytes: &[u8]) -> u128 {
    let mut v: u128 = 0;
    for (i, &b) in bytes.iter().enumerate().take(16) {
        v |= (b as u128) << (i * 8);
    }
    v
}

fn extract_bits_wide(rec: &[u8], bit_offset: u32, bit_width: u32) -> u128 {
    // Slow path: only used if rec is longer than 16 bytes.
    let start_byte = (bit_offset / 8) as usize;
    let end_byte = (bit_offset + bit_width).div_ceil(8) as usize;
    let mut chunk: u128 = 0;
    for (i, &b) in rec[start_byte..end_byte].iter().enumerate() {
        chunk |= (b as u128) << (i * 8);
    }
    let shift = bit_offset - (start_byte as u32) * 8;
    let mask = if bit_width == 128 {
        u128::MAX
    } else {
        (1u128 << bit_width) - 1
    };
    (chunk >> shift) & mask
}
