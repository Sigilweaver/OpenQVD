use crate::error::QvdError;
use crate::header::FieldHeader;
use crate::value::{Dual, Value};

pub(crate) fn decode_field_symbols(
    body: &[u8],
    body_offset_in_file: usize,
    field: &FieldHeader,
) -> Result<Vec<Value>, QvdError> {
    let start = field.offset as usize;
    let end = start
        .checked_add(field.length as usize)
        .ok_or_else(|| QvdError::structure("field offset+length overflow"))?;
    if end > body.len() {
        return Err(QvdError::structure(format!(
            "field {:?} symbol region [{start}..{end}) exceeds body len {}",
            field.name,
            body.len()
        )));
    }
    let region = &body[start..end];
    let mut cursor = 0usize;
    // `no_of_symbols` is a file-controlled count read before this loop runs;
    // a crafted field could declare billions of symbols backed by only a
    // few actual bytes. Every symbol consumes at least one byte (the type
    // tag), so `region.len()` is a tight, always-correct upper bound on how
    // many symbols this region can actually hold - use it instead of
    // trusting the declared count directly.
    let mut out: Vec<Value> = Vec::with_capacity((field.no_of_symbols as usize).min(region.len()));
    for _ in 0..field.no_of_symbols {
        if cursor >= region.len() {
            return Err(QvdError::structure(format!(
                "field {:?}: ran out of symbol bytes",
                field.name
            )));
        }
        let (value, next) = read_symbol(region, cursor, body_offset_in_file + start)?;
        cursor = next;
        out.push(value);
    }
    if cursor != region.len() {
        return Err(QvdError::structure(format!(
            "field {:?}: {} trailing bytes in symbol region",
            field.name,
            region.len() - cursor
        )));
    }
    Ok(out)
}

fn read_symbol(
    region: &[u8],
    start: usize,
    region_file_offset: usize,
) -> Result<(Value, usize), QvdError> {
    let tb = region[start];
    let p = start + 1;
    match tb {
        0x01 => {
            let v = crate::bytes::read_i32_le(region, p, "i32 symbol")?;
            Ok((Value::Int(v), p + 4))
        }
        0x02 => {
            let v = crate::bytes::read_f64_le(region, p, "f64 symbol")?;
            Ok((Value::Float(v), p + 8))
        }
        0x04 => {
            let end = region[p..]
                .iter()
                .position(|&b| b == 0)
                .map(|i| p + i)
                .ok_or_else(|| QvdError::structure("unterminated string symbol"))?;
            let s = std::str::from_utf8(&region[p..end])
                .map_err(|_| QvdError::Utf8 {
                    offset: region_file_offset + start,
                })?
                .to_owned();
            Ok((Value::Str(s), end + 1))
        }
        0x05 => {
            let number = crate::bytes::read_i32_le(region, p, "dual-int prefix")?;
            let s_start = p + 4;
            let end = region[s_start..]
                .iter()
                .position(|&b| b == 0)
                .map(|i| s_start + i)
                .ok_or_else(|| QvdError::structure("unterminated dual-int string"))?;
            let text = std::str::from_utf8(&region[s_start..end])
                .map_err(|_| QvdError::Utf8 {
                    offset: region_file_offset + start,
                })?
                .to_owned();
            Ok((Value::DualInt(Dual { number, text }), end + 1))
        }
        0x06 => {
            let number = crate::bytes::read_f64_le(region, p, "dual-float prefix")?;
            let s_start = p + 8;
            let end = region[s_start..]
                .iter()
                .position(|&b| b == 0)
                .map(|i| s_start + i)
                .ok_or_else(|| QvdError::structure("unterminated dual-float string"))?;
            let text = std::str::from_utf8(&region[s_start..end])
                .map_err(|_| QvdError::Utf8 {
                    offset: region_file_offset + start,
                })?
                .to_owned();
            Ok((Value::DualFloat(Dual { number, text }), end + 1))
        }
        other => Err(QvdError::UnknownSymbolType {
            byte: other,
            offset: region_file_offset + start,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::NumberFormat;

    fn field(no_of_symbols: u32, length: u32) -> FieldHeader {
        FieldHeader {
            name: "f".into(),
            bit_offset: 0,
            bit_width: 0,
            bias: 0,
            no_of_symbols,
            offset: 0,
            length,
            number_format: NumberFormat::default(),
            tags: Vec::new(),
        }
    }

    /// Regression test: `no_of_symbols` is a file-controlled count read
    /// before the bounded decode loop below runs. Before capping the
    /// pre-allocation at `region.len()`, a field claiming billions of
    /// symbols backed by only a couple of actual bytes would force a
    /// multi-GB `Vec::with_capacity` up front, regardless of how much
    /// symbol data actually follows. It should instead run out of symbol
    /// bytes and error immediately.
    #[test]
    fn huge_no_of_symbols_with_tiny_region_errors_without_large_alloc() {
        let body = [0x04, 0x00]; // one empty string symbol, 2 bytes
        let f = field(u32::MAX, body.len() as u32);
        let err = decode_field_symbols(&body, 0, &f).unwrap_err();
        assert!(err.to_string().contains("ran out of symbol bytes"), "{err}");
    }
}
