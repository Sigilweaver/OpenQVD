use std::fs;
use std::path::Path;

use crate::error::QvdError;
use crate::header::{parse, FieldHeader, TableHeader};
use crate::symbols::decode_field_symbols;
use crate::value::{Cell, Value};

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

/// An in-memory QVD file with headers, symbol tables, and the packed row
/// index block.
#[derive(Debug)]
pub struct Qvd {
    header: TableHeader,
    header_bytes: usize,
    /// The bytes of the file after the header terminator.
    body: Vec<u8>,
    /// One decoded symbol table per field, in field order.
    symbol_tables: Vec<Vec<Value>>,
}

impl Qvd {
    /// Read and parse a QVD file from disk.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, QvdError> {
        let bytes = fs::read(path.as_ref())?;
        Self::from_bytes(bytes)
    }

    /// Read and parse a QVD file from disk, decoding only the listed columns.
    ///
    /// See [`Self::from_bytes_projected`] for details.
    pub fn from_path_projected(path: impl AsRef<Path>, needed: &[&str]) -> Result<Self, QvdError> {
        let bytes = fs::read(path.as_ref())?;
        Self::from_bytes_projected(bytes, needed)
    }

    /// Parse a QVD file from a byte vector already in memory.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, QvdError> {
        Self::from_bytes_impl(bytes, None)
    }

    /// Parse a QVD file, decoding symbol tables only for the named columns.
    ///
    /// Columns not in `needed` get an empty symbol table (every cell will
    /// resolve to `None`).  This avoids the cost of decoding large string
    /// symbol tables for columns that will never be read.
    pub fn from_bytes_projected(bytes: Vec<u8>, needed: &[&str]) -> Result<Self, QvdError> {
        Self::from_bytes_impl(bytes, Some(needed))
    }

    fn from_bytes_impl(bytes: Vec<u8>, needed: Option<&[&str]>) -> Result<Self, QvdError> {
        let (header, header_bytes) = parse(&bytes)?;
        let body: Vec<u8> = bytes[header_bytes..].to_vec();

        // Structural invariants required by the spec.
        let expected_len = header
            .no_of_records
            .checked_mul(header.record_byte_size)
            .ok_or_else(|| QvdError::structure("n_records * record_byte_size overflow"))?;
        if expected_len != header.row_block_length {
            return Err(QvdError::structure(format!(
                "row block length {} != n_records * record_byte_size {}",
                header.row_block_length, expected_len
            )));
        }
        // Bit-field coverage check.
        let total_bits = header.record_byte_size.saturating_mul(8);
        let mut used = vec![false; total_bits as usize];
        for f in &header.fields {
            if f.bit_width == 0 {
                continue;
            }
            let end = f
                .bit_offset
                .checked_add(f.bit_width)
                .ok_or_else(|| QvdError::structure("bit_offset+bit_width overflow"))?;
            if end > total_bits {
                return Err(QvdError::structure(format!(
                    "field {:?} bits [{}..{}) exceed record size {} bits",
                    f.name, f.bit_offset, end, total_bits
                )));
            }
            for b in f.bit_offset..end {
                if used[b as usize] {
                    return Err(QvdError::structure(format!(
                        "field {:?} overlaps another at bit {}",
                        f.name, b
                    )));
                }
                used[b as usize] = true;
            }
        }

        let mut symbol_tables = Vec::with_capacity(header.fields.len());
        for f in &header.fields {
            let dominated = match needed {
                None => false,
                Some(names) => !names.iter().any(|n| *n == f.name),
            };
            if dominated {
                symbol_tables.push(Vec::new());
            } else {
                symbol_tables.push(decode_field_symbols(&body, header_bytes, f)?);
            }
        }

        // Row block must lie within the body.
        let row_end = (header.row_block_offset as usize)
            .checked_add(header.row_block_length as usize)
            .ok_or_else(|| QvdError::structure("row block offset+length overflow"))?;
        if row_end > body.len() {
            return Err(QvdError::structure(format!(
                "row block [{}..{}) exceeds body len {}",
                header.row_block_offset,
                row_end,
                body.len()
            )));
        }

        Ok(Self {
            header,
            header_bytes,
            body,
            symbol_tables,
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

    /// Byte length of the XML header including the trailing `0x00`.
    pub fn header_size(&self) -> usize {
        self.header_bytes
    }

    /// Decoded symbol table for the given field index.
    pub fn symbols(&self, field_index: usize) -> Option<&[Value]> {
        self.symbol_tables.get(field_index).map(|v| v.as_slice())
    }

    /// Iterate rows. Each row is a `Vec<Cell>` with one entry per field in
    /// the same order as [`Self::fields`]. `None` denotes a NULL.
    pub fn rows(&self) -> RowIter<'_> {
        RowIter { qvd: self, next: 0 }
    }

    /// Iterate rows, returning a `Result` for each. An out-of-range symbol
    /// index that the infallible [`Self::rows`] iterator would swallow as
    /// `None` is surfaced here as a [`QvdError`].
    pub fn checked_rows(&self) -> CheckedRowIter<'_> {
        CheckedRowIter { qvd: self, next: 0 }
    }

    /// Write the QVD back to a byte vector, re-using the already-parsed body
    /// and regenerating the XML header. This is memory-efficient for large
    /// files because it does not materialise an intermediate WriteTable; it
    /// copies the body bytes once and produces a byte-level-equivalent output.
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::QvdError> {
        let xml = self.build_xml_header();
        let mut out = Vec::with_capacity(xml.len() + 1 + self.body.len());
        out.extend_from_slice(xml.as_bytes());
        out.push(0x00);
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

    fn build_xml_header(&self) -> String {
        use std::fmt::Write as FmtWrite;
        let h = &self.header;
        let mut s = String::new();
        let _ = write!(
            s,
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n\
<QvdTableHeader>\r\n\
  <TableName>{}</TableName>\r\n\
  <Fields>\r\n",
            xml_escape(&h.table_name)
        );
        for f in &h.fields {
            let nf = &f.number_format;
            let _ = write!(
                s,
                "    <QvdFieldHeader>\r\n\
      <FieldName>{name}</FieldName>\r\n\
      <BitOffset>{bo}</BitOffset>\r\n\
      <BitWidth>{bw}</BitWidth>\r\n\
      <Bias>{bias}</Bias>\r\n\
      <NumberFormat>\r\n\
        <Type>{ty}</Type>\r\n\
        <nDec>{ndec}</nDec>\r\n\
        <UseThou>{ut}</UseThou>\r\n\
        <Fmt>{fmt}</Fmt>\r\n\
        <Dec>{dec}</Dec>\r\n\
        <Thou>{thou}</Thou>\r\n\
      </NumberFormat>\r\n\
      <NoOfSymbols>{ns}</NoOfSymbols>\r\n\
      <Offset>{off}</Offset>\r\n\
      <Length>{len}</Length>\r\n",
                name = xml_escape(&f.name),
                bo = f.bit_offset,
                bw = f.bit_width,
                bias = f.bias,
                ty = xml_escape(if nf.r#type.is_empty() {
                    "UNKNOWN"
                } else {
                    &nf.r#type
                }),
                ndec = xml_escape(if nf.n_dec.is_empty() { "0" } else { &nf.n_dec }),
                ut = xml_escape(if nf.use_thou.is_empty() {
                    "0"
                } else {
                    &nf.use_thou
                }),
                fmt = xml_escape(&nf.fmt),
                dec = xml_escape(&nf.dec),
                thou = xml_escape(&nf.thou),
                ns = f.no_of_symbols,
                off = f.offset,
                len = f.length,
            );
            if f.tags.is_empty() {
                s.push_str("      <Tags/>\r\n");
            } else {
                s.push_str("      <Tags>\r\n");
                for t in &f.tags {
                    let _ = write!(s, "        <String>{}</String>\r\n", xml_escape(t));
                }
                s.push_str("      </Tags>\r\n");
            }
            s.push_str("    </QvdFieldHeader>\r\n");
        }
        let _ = write!(
            s,
            "  </Fields>\r\n\
  <Compression></Compression>\r\n\
  <RecordByteSize>{rbs}</RecordByteSize>\r\n\
  <NoOfRecords>{nr}</NoOfRecords>\r\n\
  <Offset>{off}</Offset>\r\n\
  <Length>{len}</Length>\r\n\
</QvdTableHeader>\r\n",
            rbs = h.record_byte_size,
            nr = h.no_of_records,
            off = h.row_block_offset,
            len = h.row_block_length,
        );
        s
    }

    /// Materialise this QVD as a [`crate::WriteTable`], one column per
    /// field, in the same order. Useful for round-trip testing and
    /// programmatic rewriting.
    pub fn to_write_table(&self) -> crate::WriteTable {
        let n_rows = self.num_rows() as usize;
        let mut columns: Vec<crate::Column> = self
            .header
            .fields
            .iter()
            .map(|f| crate::Column {
                name: f.name.clone(),
                cells: Vec::with_capacity(n_rows),
                number_format: f.number_format.clone(),
                tags: f.tags.clone(),
            })
            .collect();
        for row in self.rows() {
            for (i, cell) in row.into_iter().enumerate() {
                columns[i].cells.push(cell);
            }
        }
        crate::WriteTable {
            name: self.header.table_name.clone(),
            columns,
        }
    }
}

/// Iterator over decoded rows.
pub struct RowIter<'a> {
    qvd: &'a Qvd,
    next: u32,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = Vec<Cell>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.qvd.header.no_of_records {
            return None;
        }
        let idx = self.next as usize;
        self.next += 1;
        let rbs = self.qvd.header.record_byte_size as usize;
        let rows_off = self.qvd.header.row_block_offset as usize;
        let rec = &self.qvd.body[rows_off + idx * rbs..rows_off + (idx + 1) * rbs];
        // Treat the record as a little-endian unsigned integer up to 128 bits.
        // For widths larger than 128 bits we fall back to u128 chunks.
        let rec_int = le_bits_to_u128(rec);
        let mut out: Vec<Cell> = Vec::with_capacity(self.qvd.header.fields.len());
        for (i, f) in self.qvd.header.fields.iter().enumerate() {
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
                out.push(None);
            } else if index as usize >= self.qvd.symbol_tables[i].len() {
                // Per spec this is a corruption error. We still want the
                // iterator to be infallible, so we encode this as None.
                // Callers that care should use the checked API in a future
                // stage.
                out.push(None);
            } else {
                out.push(Some(self.qvd.symbol_tables[i][index as usize].clone()));
            }
        }
        Some(out)
    }
}

/// Checked iterator over decoded rows.
pub struct CheckedRowIter<'a> {
    qvd: &'a Qvd,
    next: u32,
}

impl<'a> Iterator for CheckedRowIter<'a> {
    type Item = Result<Vec<Cell>, QvdError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.qvd.header.no_of_records {
            return None;
        }
        let idx = self.next as usize;
        self.next += 1;
        let rbs = self.qvd.header.record_byte_size as usize;
        let rows_off = self.qvd.header.row_block_offset as usize;
        let rec = &self.qvd.body[rows_off + idx * rbs..rows_off + (idx + 1) * rbs];
        let rec_int = le_bits_to_u128(rec);
        let mut out: Vec<Cell> = Vec::with_capacity(self.qvd.header.fields.len());
        for (i, f) in self.qvd.header.fields.iter().enumerate() {
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
                out.push(None);
            } else {
                let ui = index as usize;
                match self.qvd.symbol_tables[i].get(ui) {
                    Some(v) => out.push(Some(v.clone())),
                    None => {
                        return Some(Err(QvdError::structure(format!(
                            "row {idx}: field {:?} symbol index {ui} is out of range (n={})",
                            f.name,
                            self.qvd.symbol_tables[i].len()
                        ))));
                    }
                }
            }
        }
        Some(Ok(out))
    }
}

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
    let end_byte = ((bit_offset + bit_width + 7) / 8) as usize;
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
