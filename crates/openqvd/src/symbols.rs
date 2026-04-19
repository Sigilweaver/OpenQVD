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
    let mut out: Vec<Value> = Vec::with_capacity(field.no_of_symbols as usize);
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

fn read_symbol(region: &[u8], start: usize, region_file_offset: usize) -> Result<(Value, usize), QvdError> {
    let tb = region[start];
    let p = start + 1;
    match tb {
        0x01 => {
            let bytes: [u8; 4] = region
                .get(p..p + 4)
                .ok_or_else(|| QvdError::structure("truncated i32 symbol"))?
                .try_into()
                .unwrap();
            Ok((Value::Int(i32::from_le_bytes(bytes)), p + 4))
        }
        0x02 => {
            let bytes: [u8; 8] = region
                .get(p..p + 8)
                .ok_or_else(|| QvdError::structure("truncated f64 symbol"))?
                .try_into()
                .unwrap();
            Ok((Value::Float(f64::from_le_bytes(bytes)), p + 8))
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
            let bytes: [u8; 4] = region
                .get(p..p + 4)
                .ok_or_else(|| QvdError::structure("truncated dual-int prefix"))?
                .try_into()
                .unwrap();
            let number = i32::from_le_bytes(bytes);
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
            let bytes: [u8; 8] = region
                .get(p..p + 8)
                .ok_or_else(|| QvdError::structure("truncated dual-float prefix"))?
                .try_into()
                .unwrap();
            let number = f64::from_le_bytes(bytes);
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
