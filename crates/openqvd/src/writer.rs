//! QVD writer: produces a byte stream that conforming readers parse back
//! to the same logical table. See SPEC.md section 7.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;

use crate::error::QvdError;
use crate::value::{Cell, Value};

/// A single column of logical values, ready to be written.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name (becomes `<FieldName>`).
    pub name: String,
    /// One entry per row. `None` becomes NULL (bias=-2).
    pub cells: Vec<Cell>,
    /// Optional `NumberFormat/Type` hint. Defaults to `"UNKNOWN"`.
    pub number_format_type: Option<String>,
    /// Optional `Tags` content. Defaults to empty.
    pub tags: Option<String>,
}

impl Column {
    /// Convenience constructor.
    pub fn new(name: impl Into<String>, cells: Vec<Cell>) -> Self {
        Self {
            name: name.into(),
            cells,
            number_format_type: None,
            tags: None,
        }
    }
}

/// A logical table ready to be encoded.
#[derive(Debug, Clone)]
pub struct WriteTable {
    /// `<TableName>`.
    pub name: String,
    /// Columns in declaration order.
    pub columns: Vec<Column>,
}

impl WriteTable {
    /// Construct a table. All columns must have the same length.
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Result<Self, QvdError> {
        if let Some(first) = columns.first() {
            let n = first.cells.len();
            for c in &columns {
                if c.cells.len() != n {
                    return Err(QvdError::structure(format!(
                        "column {:?} has {} cells, expected {}",
                        c.name,
                        c.cells.len(),
                        n
                    )));
                }
            }
        }
        Ok(Self {
            name: name.into(),
            columns,
        })
    }

    /// Number of rows (0 if no columns).
    pub fn num_rows(&self) -> usize {
        self.columns.first().map(|c| c.cells.len()).unwrap_or(0)
    }

    /// Serialise to a byte vector.
    pub fn to_bytes(&self) -> Result<Vec<u8>, QvdError> {
        encode(self)
    }

    /// Serialise to a file on disk.
    pub fn write_to_path(&self, path: impl AsRef<Path>) -> Result<(), QvdError> {
        let bytes = self.to_bytes()?;
        let mut f = fs::File::create(path.as_ref())?;
        f.write_all(&bytes)?;
        Ok(())
    }
}

/// Per-column plan derived by `prepare`.
struct ColumnPlan {
    /// Symbols in first-occurrence order.
    symbols: Vec<Value>,
    /// For each input row, the stored bit pattern (0..2^bit_width).
    stored: Vec<u64>,
    /// Encoded symbol table bytes.
    symbol_bytes: Vec<u8>,
    bit_offset: u32,
    bit_width: u32,
    bias: i32,
    offset_in_body: u32,
    length_in_body: u32,
}

fn plan_column(col: &Column) -> Result<ColumnPlan, QvdError> {
    // First-occurrence ordering.
    let mut index_of: HashMap<SymbolKey, u32> = HashMap::new();
    let mut symbols: Vec<Value> = Vec::new();
    let mut has_null = false;

    let mut indices: Vec<Option<u32>> = Vec::with_capacity(col.cells.len());
    for cell in &col.cells {
        match cell {
            None => {
                has_null = true;
                indices.push(None);
            }
            Some(v) => {
                let key = SymbolKey::from(v);
                let idx = if let Some(&i) = index_of.get(&key) {
                    i
                } else {
                    let i = symbols.len() as u32;
                    index_of.insert(key, i);
                    symbols.push(v.clone());
                    i
                };
                indices.push(Some(idx));
            }
        }
    }

    let n_symbols = symbols.len() as u64;
    let bias: i32 = if has_null { -2 } else { 0 };
    // stored = index - bias, so max stored = (n_symbols - 1) + (-bias).
    // With bias=-2 and has_null, stored 0 and 1 both encode NULL; we emit 0.
    let max_stored: u64 = if n_symbols == 0 && has_null {
        1 // room for "0 is NULL"
    } else if n_symbols == 0 {
        0
    } else {
        (n_symbols - 1) + ((-bias) as u64)
    };
    let bit_width: u32 = if n_symbols <= 1 && !has_null {
        0
    } else {
        let mut w = 0u32;
        while ((1u64 << w) - 1) < max_stored {
            w += 1;
            if w > 64 {
                return Err(QvdError::structure(
                    "column cardinality exceeds 64-bit bit width",
                ));
            }
        }
        w
    };

    let stored: Vec<u64> = indices
        .into_iter()
        .map(|opt| match opt {
            Some(i) => (i as i64 - bias as i64) as u64,
            None => 0u64, // NULL: stored + bias = -2 < 0
        })
        .collect();

    let symbol_bytes = encode_symbols(&symbols)?;
    let length_in_body = symbol_bytes.len() as u32;

    Ok(ColumnPlan {
        symbols,
        stored,
        symbol_bytes,
        bit_offset: 0,
        bit_width,
        bias,
        offset_in_body: 0,
        length_in_body,
    })
}

#[derive(Hash, Eq, PartialEq)]
enum SymbolKey {
    Int(i32),
    Float(u64),
    Str(String),
    DualInt(i32, String),
    DualFloat(u64, String),
}

impl From<&Value> for SymbolKey {
    fn from(v: &Value) -> Self {
        match v {
            Value::Int(i) => SymbolKey::Int(*i),
            Value::Float(f) => SymbolKey::Float(f.to_bits()),
            Value::Str(s) => SymbolKey::Str(s.clone()),
            Value::DualInt(d) => SymbolKey::DualInt(d.number, d.text.clone()),
            Value::DualFloat(d) => SymbolKey::DualFloat(d.number.to_bits(), d.text.clone()),
        }
    }
}

fn encode_symbols(symbols: &[Value]) -> Result<Vec<u8>, QvdError> {
    let mut out = Vec::with_capacity(symbols.len() * 8);
    for s in symbols {
        match s {
            Value::Int(i) => {
                out.push(0x01);
                out.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                out.push(0x02);
                out.extend_from_slice(&f.to_le_bytes());
            }
            Value::Str(s) => {
                out.push(0x04);
                write_cstring(&mut out, s)?;
            }
            Value::DualInt(d) => {
                out.push(0x05);
                out.extend_from_slice(&d.number.to_le_bytes());
                write_cstring(&mut out, &d.text)?;
            }
            Value::DualFloat(d) => {
                out.push(0x06);
                out.extend_from_slice(&d.number.to_le_bytes());
                write_cstring(&mut out, &d.text)?;
            }
        }
    }
    Ok(out)
}

fn write_cstring(out: &mut Vec<u8>, s: &str) -> Result<(), QvdError> {
    if s.as_bytes().contains(&0x00) {
        return Err(QvdError::structure(
            "string symbol contains NUL byte, which is reserved as the symbol terminator",
        ));
    }
    out.extend_from_slice(s.as_bytes());
    out.push(0x00);
    Ok(())
}

fn encode(table: &WriteTable) -> Result<Vec<u8>, QvdError> {
    // 1. Plan each column.
    let mut plans: Vec<ColumnPlan> = table
        .columns
        .iter()
        .map(plan_column)
        .collect::<Result<_, _>>()?;

    // 2. Assign bit offsets in declaration order.
    let mut bit_cursor: u32 = 0;
    for p in plans.iter_mut() {
        p.bit_offset = bit_cursor;
        bit_cursor = bit_cursor
            .checked_add(p.bit_width)
            .ok_or_else(|| QvdError::structure("bit layout overflow"))?;
    }
    let record_bits = bit_cursor;
    let record_byte_size = (record_bits + 7) / 8;

    // 3. Assign symbol-block offsets in declaration order.
    let mut body_cursor: u32 = 0;
    for p in plans.iter_mut() {
        p.offset_in_body = body_cursor;
        body_cursor = body_cursor
            .checked_add(p.length_in_body)
            .ok_or_else(|| QvdError::structure("body layout overflow"))?;
    }
    let row_block_offset = body_cursor;
    let n_rows = table.num_rows() as u32;
    let row_block_length = record_byte_size
        .checked_mul(n_rows)
        .ok_or_else(|| QvdError::structure("row block size overflow"))?;

    // 4. Build the XML header.
    let xml = build_xml_header(table, &plans, record_byte_size, n_rows, row_block_offset, row_block_length);

    // 5. Emit bytes.
    let mut out = Vec::with_capacity(xml.len() + 1 + (body_cursor + row_block_length) as usize);
    out.extend_from_slice(xml.as_bytes());
    out.push(0x00);
    for p in &plans {
        out.extend_from_slice(&p.symbol_bytes);
    }

    // 6. Pack rows.
    let rbs = record_byte_size as usize;
    if rbs > 0 && record_bits <= 128 {
        // Fast path: build each record in a u128.
        for row in 0..(n_rows as usize) {
            let mut rec: u128 = 0;
            for p in &plans {
                if p.bit_width == 0 {
                    continue;
                }
                let stored = p.stored[row] as u128;
                let mask = if p.bit_width == 128 {
                    u128::MAX
                } else {
                    (1u128 << p.bit_width) - 1
                };
                rec |= (stored & mask) << p.bit_offset;
            }
            for i in 0..rbs {
                out.push(((rec >> (i * 8)) & 0xFF) as u8);
            }
        }
    } else if rbs > 0 {
        // Wide path: write each field's bits directly into a byte buffer.
        for row in 0..(n_rows as usize) {
            let mut buf = vec![0u8; rbs];
            for p in &plans {
                if p.bit_width == 0 {
                    continue;
                }
                write_bits(&mut buf, p.bit_offset, p.bit_width, p.stored[row]);
            }
            out.extend_from_slice(&buf);
        }
    }

    // Discourage unused-field warnings on `symbols` (we may expose it later).
    let _ = plans.iter().map(|p| &p.symbols).count();

    Ok(out)
}

fn write_bits(buf: &mut [u8], bit_offset: u32, bit_width: u32, value: u64) {
    // Spread `value`'s lower `bit_width` bits into `buf` starting at
    // bit_offset (LSB-first, little-endian byte order).
    let mut remaining = bit_width;
    let mut pos = bit_offset;
    let mut src = value;
    while remaining > 0 {
        let byte_idx = (pos / 8) as usize;
        let in_byte_off = pos % 8;
        let can_take = (8 - in_byte_off).min(remaining);
        let mask = ((1u64 << can_take) - 1) as u8;
        let chunk = (src as u8) & mask;
        buf[byte_idx] |= chunk << in_byte_off;
        src >>= can_take;
        pos += can_take;
        remaining -= can_take;
    }
}

fn build_xml_header(
    table: &WriteTable,
    plans: &[ColumnPlan],
    record_byte_size: u32,
    n_rows: u32,
    row_block_offset: u32,
    row_block_length: u32,
) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n");
    s.push_str("<QvdTableHeader>\r\n");
    s.push_str(&format!(
        "  <TableName>{}</TableName>\r\n",
        xml_escape(&table.name)
    ));
    s.push_str("  <Fields>\r\n");
    for (col, plan) in table.columns.iter().zip(plans) {
        let nft = col.number_format_type.as_deref().unwrap_or("UNKNOWN");
        let tags = col.tags.as_deref().unwrap_or("");
        s.push_str("    <QvdFieldHeader>\r\n");
        s.push_str(&format!(
            "      <FieldName>{}</FieldName>\r\n",
            xml_escape(&col.name)
        ));
        s.push_str(&format!(
            "      <BitOffset>{}</BitOffset>\r\n",
            plan.bit_offset
        ));
        s.push_str(&format!(
            "      <BitWidth>{}</BitWidth>\r\n",
            plan.bit_width
        ));
        s.push_str(&format!("      <Bias>{}</Bias>\r\n", plan.bias));
        s.push_str(&format!(
            "      <NumberFormat><Type>{}</Type></NumberFormat>\r\n",
            xml_escape(nft)
        ));
        s.push_str(&format!(
            "      <NoOfSymbols>{}</NoOfSymbols>\r\n",
            plan.symbols.len()
        ));
        s.push_str(&format!(
            "      <Offset>{}</Offset>\r\n",
            plan.offset_in_body
        ));
        s.push_str(&format!(
            "      <Length>{}</Length>\r\n",
            plan.length_in_body
        ));
        s.push_str(&format!("      <Tags>{}</Tags>\r\n", xml_escape(tags)));
        s.push_str("    </QvdFieldHeader>\r\n");
    }
    s.push_str("  </Fields>\r\n");
    s.push_str("  <Compression></Compression>\r\n");
    s.push_str(&format!(
        "  <RecordByteSize>{}</RecordByteSize>\r\n",
        record_byte_size
    ));
    s.push_str(&format!(
        "  <NoOfRecords>{}</NoOfRecords>\r\n",
        n_rows
    ));
    s.push_str(&format!("  <Offset>{}</Offset>\r\n", row_block_offset));
    s.push_str(&format!("  <Length>{}</Length>\r\n", row_block_length));
    s.push_str("</QvdTableHeader>\r\n");
    s
}

fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}
