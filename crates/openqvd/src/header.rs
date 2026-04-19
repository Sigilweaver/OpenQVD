use crate::error::QvdError;

/// Parsed `<NumberFormat>` element. Every observed file contains the
/// same six sub-elements. Values are informational only: they do not
/// change how bytes are decoded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NumberFormat {
    /// `Type` (e.g. `UNKNOWN`, `INTEGER`, `REAL`, `DATE`, `TIMESTAMP`).
    pub r#type: String,
    /// `nDec` (number of decimal places, as a string).
    pub n_dec: String,
    /// `UseThou` (`"0"` or `"1"`).
    pub use_thou: String,
    /// `Fmt` (display pattern).
    pub fmt: String,
    /// `Dec` (decimal separator, single character or empty).
    pub dec: String,
    /// `Thou` (thousands separator, single character or empty).
    pub thou: String,
}

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
    /// `NumberFormat` sub-element (informational).
    pub number_format: NumberFormat,
    /// `Tags/String` children. Usually Qlik markers such as
    /// `$numeric`, `$text`, `$key`.
    pub tags: Vec<String>,
}

impl FieldHeader {
    /// Shortcut for `self.number_format.r#type` (the only NumberFormat
    /// field most callers care about).
    pub fn number_format_type(&self) -> &str {
        &self.number_format.r#type
    }
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
    let mut in_tags = false;

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
                        number_format: NumberFormat::default(),
                        tags: Vec::new(),
                    });
                } else if name == "NumberFormat" {
                    in_number_format = true;
                } else if name == "Tags" {
                    in_tags = true;
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
                                _ => {}
                            }
                        }
                    }
                    ("NumberFormat", sub) => {
                        if let Some(f) = current.as_mut() {
                            match sub {
                                "Type" => f.number_format.r#type = txt,
                                "nDec" => f.number_format.n_dec = txt,
                                "UseThou" => f.number_format.use_thou = txt,
                                "Fmt" => f.number_format.fmt = txt,
                                "Dec" => f.number_format.dec = txt,
                                "Thou" => f.number_format.thou = txt,
                                _ => {}
                            }
                        }
                    }
                    ("Tags", "String") => {
                        if let Some(f) = current.as_mut() {
                            if !txt.is_empty() {
                                f.tags.push(txt);
                            }
                        }
                    }
                    _ => {}
                }
                if name == "NumberFormat" {
                    in_number_format = false;
                }
                if name == "Tags" {
                    in_tags = false;
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
    let _ = in_tags;

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
