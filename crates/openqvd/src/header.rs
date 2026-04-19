use crate::error::QvdError;

/// Parsed representation of a `<QvdFieldHeader>` element.
#[derive(Debug, Clone)]
pub struct FieldHeader {
    /// `FieldName` element (column name).
    pub name: String,
    /// `BitOffset`: LSB-first bit position inside a row.
    pub bit_offset: u32,
    /// `BitWidth`: number of bits used to store the symbol index.
    pub bit_width: u32,
    /// `Bias`: added to the unpacked bit-field to obtain the symbol index.
    pub bias: i32,
    /// `NoOfSymbols`: number of entries in this field's symbol table.
    pub no_of_symbols: u32,
    /// `Offset`: start of the symbol table, relative to the body.
    pub offset: u32,
    /// `Length`: byte length of the symbol table.
    pub length: u32,
    /// `NumberFormat/Type`: informational format hint (e.g. `INTEGER`).
    pub number_format_type: String,
    /// `Tags`: whitespace-delimited Qlik-specific hints.
    pub tags: String,
}

/// Parsed representation of `<QvdTableHeader>` plus its fields.
#[derive(Debug, Clone)]
pub struct TableHeader {
    /// `TableName`.
    pub table_name: String,
    /// `QvBuildNo`, if present.
    pub build_no: Option<String>,
    /// All fields in the order they appear in the header.
    pub fields: Vec<FieldHeader>,
    /// `RecordByteSize`.
    pub record_byte_size: u32,
    /// `NoOfRecords`.
    pub no_of_records: u32,
    /// Root `Offset` (into body).
    pub row_block_offset: u32,
    /// Root `Length`.
    pub row_block_length: u32,
}

/// Parse the XML table header from a byte slice. Returns the header and
/// the number of leading bytes (including the `0x00` terminator) that it
/// occupies in the file.
pub(crate) fn parse(full: &[u8]) -> Result<(TableHeader, usize), QvdError> {
    const END_CRLF: &[u8] = b"</QvdTableHeader>\r\n\x00";
    const END_LF: &[u8] = b"</QvdTableHeader>\n\x00";

    let (end_idx, term_len) = if let Some(p) = find_subslice(full, END_CRLF) {
        (p, END_CRLF.len())
    } else if let Some(p) = find_subslice(full, END_LF) {
        (p, END_LF.len())
    } else {
        return Err(QvdError::bad_header("no QvdTableHeader terminator"));
    };

    let xml_end = end_idx + b"</QvdTableHeader>".len();
    let xml = std::str::from_utf8(&full[..xml_end])
        .map_err(|_| QvdError::bad_header("header is not valid utf-8"))?;
    let header_bytes_total = end_idx + term_len;

    let parsed = parse_xml(xml)?;
    Ok((parsed, header_bytes_total))
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    (0..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

fn parse_xml(xml: &str) -> Result<TableHeader, QvdError> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut r = Reader::from_str(xml);
    r.config_mut().trim_text(true);

    let mut stack: Vec<String> = Vec::new();
    let mut text_buf = String::new();

    let mut table_name = String::new();
    let mut build_no: Option<String> = None;
    let mut record_byte_size: u32 = 0;
    let mut no_of_records: u32 = 0;
    let mut row_block_offset: u32 = 0;
    let mut row_block_length: u32 = 0;
    let mut fields: Vec<FieldHeader> = Vec::new();
    let mut current: Option<FieldHeader> = None;
    let mut in_number_format = false;

    loop {
        match r.read_event().map_err(|e| QvdError::Xml(e.to_string()))? {
            Event::Start(e) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if name == "QvdFieldHeader" {
                    current = Some(FieldHeader {
                        name: String::new(),
                        bit_offset: 0,
                        bit_width: 0,
                        bias: 0,
                        no_of_symbols: 0,
                        offset: 0,
                        length: 0,
                        number_format_type: String::new(),
                        tags: String::new(),
                    });
                } else if name == "NumberFormat" {
                    in_number_format = true;
                }
                stack.push(name);
                text_buf.clear();
            }
            Event::End(e) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                // Parent context is the element *above* this one, i.e. the
                // second-to-last entry on the stack.
                let parent = stack
                    .get(stack.len().saturating_sub(2))
                    .cloned()
                    .unwrap_or_default();
                let txt = std::mem::take(&mut text_buf);

                match (parent.as_str(), name.as_str()) {
                    ("QvdTableHeader", "TableName") => table_name = txt,
                    ("QvdTableHeader", "QvBuildNo") => {
                        build_no = Some(txt).filter(|s| !s.is_empty())
                    }
                    ("QvdTableHeader", "RecordByteSize") => record_byte_size = parse_u32(&txt)?,
                    ("QvdTableHeader", "NoOfRecords") => no_of_records = parse_u32(&txt)?,
                    ("QvdTableHeader", "Offset") => row_block_offset = parse_u32(&txt)?,
                    ("QvdTableHeader", "Length") => row_block_length = parse_u32(&txt)?,
                    ("QvdFieldHeader", field) => {
                        if let Some(f) = current.as_mut() {
                            match field {
                                "FieldName" => f.name = txt,
                                "BitOffset" => f.bit_offset = parse_u32(&txt)?,
                                "BitWidth" => f.bit_width = parse_u32(&txt)?,
                                "Bias" => f.bias = parse_i32(&txt)?,
                                "NoOfSymbols" => f.no_of_symbols = parse_u32(&txt)?,
                                "Offset" => f.offset = parse_u32(&txt)?,
                                "Length" => f.length = parse_u32(&txt)?,
                                "Tags" => f.tags = txt,
                                _ => {}
                            }
                        }
                    }
                    ("NumberFormat", "Type") => {
                        if let Some(f) = current.as_mut() {
                            f.number_format_type = txt;
                        }
                    }
                    _ => {}
                }
                if name == "NumberFormat" {
                    in_number_format = false;
                }
                if name == "QvdFieldHeader" {
                    if let Some(f) = current.take() {
                        fields.push(f);
                    }
                }
                stack.pop();
            }
            Event::Text(t) => {
                let s = t
                    .unescape()
                    .map_err(|e| QvdError::Xml(e.to_string()))?
                    .into_owned();
                text_buf.push_str(&s);
            }
            Event::Empty(e) => {
                // Self-closing elements carry no text. We still need to
                // record zero-valued fields, which the logic above handles
                // via Start/End.
                let _ = e;
            }
            Event::Eof => break,
            _ => {}
        }
    }
    let _ = in_number_format;

    if table_name.is_empty() && fields.is_empty() {
        return Err(QvdError::bad_header("no TableName and no fields found"));
    }
    Ok(TableHeader {
        table_name,
        build_no,
        fields,
        record_byte_size,
        no_of_records,
        row_block_offset,
        row_block_length,
    })
}

fn parse_u32(s: &str) -> Result<u32, QvdError> {
    s.trim()
        .parse::<u32>()
        .map_err(|e| QvdError::bad_header(format!("expected u32, got {s:?}: {e}")))
}

fn parse_i32(s: &str) -> Result<i32, QvdError> {
    s.trim()
        .parse::<i32>()
        .map_err(|e| QvdError::bad_header(format!("expected i32, got {s:?}: {e}")))
}
