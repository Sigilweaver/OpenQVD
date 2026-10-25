use crate::error::QvdError;

/// Parsed `<NumberFormat>` element. Every observed file contains the
/// same six sub-elements. Values are informational only: they do not
/// change how bytes are decoded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
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

impl NumberFormat {
    /// A format of the given `Type` (e.g. `"DATE"`) with every other
    /// sub-element empty.
    pub fn new(r#type: impl Into<String>) -> Self {
        Self {
            r#type: r#type.into(),
            ..Self::default()
        }
    }

    /// Set `nDec`.
    #[must_use]
    pub fn with_n_dec(mut self, n_dec: impl Into<String>) -> Self {
        self.n_dec = n_dec.into();
        self
    }

    /// Set `UseThou`.
    #[must_use]
    pub fn with_use_thou(mut self, use_thou: impl Into<String>) -> Self {
        self.use_thou = use_thou.into();
        self
    }

    /// Set `Fmt`.
    #[must_use]
    pub fn with_fmt(mut self, fmt: impl Into<String>) -> Self {
        self.fmt = fmt.into();
        self
    }

    /// Set `Dec`.
    #[must_use]
    pub fn with_dec(mut self, dec: impl Into<String>) -> Self {
        self.dec = dec.into();
        self
    }

    /// Set `Thou`.
    #[must_use]
    pub fn with_thou(mut self, thou: impl Into<String>) -> Self {
        self.thou = thou.into();
        self
    }
}

/// Parsed representation of a `<QvdFieldHeader>` element.
#[derive(Debug, Clone)]
#[non_exhaustive]
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
#[non_exhaustive]
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

    // The header ends at the earliest terminator of either style. Searching
    // for one style over the whole file first could pick a match inside the
    // body.
    let crlf = find_subslice(full, END_CRLF).map(|p| (p, END_CRLF.len()));
    let lf = find_subslice(full, END_LF).map(|p| (p, END_LF.len()));
    let (end_idx, term_len) = match (crlf, lf) {
        (Some(a), Some(b)) => {
            if a.0 <= b.0 {
                a
            } else {
                b
            }
        }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => return Err(QvdError::HeaderNotFound),
    };

    let xml_end = end_idx + b"</QvdTableHeader>".len();
    let xml = std::str::from_utf8(&full[..xml_end]).map_err(|_| QvdError::HeaderNotUtf8)?;
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

/// Store `value` in `slot`, rejecting a second occurrence.
fn set_once<T>(
    slot: &mut Option<T>,
    value: T,
    element: &str,
    field: Option<&str>,
) -> Result<(), QvdError> {
    if slot.is_some() {
        return Err(QvdError::DuplicateElement {
            element: element.to_owned(),
            field: field.map(str::to_owned),
        });
    }
    *slot = Some(value);
    Ok(())
}

fn require<T>(slot: Option<T>, element: &str, field: Option<&str>) -> Result<T, QvdError> {
    slot.ok_or_else(|| QvdError::MissingElement {
        element: element.to_owned(),
        field: field.map(str::to_owned),
    })
}

/// A `<QvdFieldHeader>` being assembled; every element must appear once.
#[derive(Default)]
struct FieldBuilder {
    name: Option<String>,
    bit_offset: Option<u32>,
    bit_width: Option<u32>,
    bias: Option<i32>,
    no_of_symbols: Option<u32>,
    offset: Option<u32>,
    length: Option<u32>,
    number_format: NumberFormat,
    tags: Vec<String>,
}

impl FieldBuilder {
    fn build(self) -> Result<FieldHeader, QvdError> {
        let name = require(self.name, "FieldName", None)?;
        let f = Some(name.as_str());
        Ok(FieldHeader {
            bit_offset: require(self.bit_offset, "BitOffset", f)?,
            bit_width: require(self.bit_width, "BitWidth", f)?,
            bias: require(self.bias, "Bias", f)?,
            no_of_symbols: require(self.no_of_symbols, "NoOfSymbols", f)?,
            offset: require(self.offset, "Offset", f)?,
            length: require(self.length, "Length", f)?,
            number_format: self.number_format,
            tags: self.tags,
            name,
        })
    }
}

fn parse_xml(xml: &str) -> Result<TableHeader, QvdError> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut r = Reader::from_str(xml);
    // Text is kept exactly as written: names and separators may carry
    // significant spaces (`<Thou> </Thou>`). Numeric elements are trimmed
    // when parsed.

    let mut stack: Vec<String> = Vec::new();
    let mut text_buf = String::new();

    let mut table_name: Option<String> = None;
    let mut build_no: Option<String> = None;
    let mut compression: Option<String> = None;
    let mut record_byte_size: Option<u32> = None;
    let mut no_of_records: Option<u32> = None;
    let mut row_block_offset: Option<u32> = None;
    let mut row_block_length: Option<u32> = None;
    let mut fields: Vec<FieldHeader> = Vec::new();
    let mut current: Option<FieldBuilder> = None;
    let mut seen_root = false;

    // `Start`, `Empty` and `End` share one handler pair so that a
    // self-closing element is exactly an element with empty text.
    macro_rules! open_element {
        ($name:expr) => {{
            let name: String = $name;
            if !seen_root {
                if name != "QvdTableHeader" {
                    return Err(QvdError::UnexpectedRoot { found: name });
                }
                seen_root = true;
            }
            if name == "QvdFieldHeader" {
                current = Some(FieldBuilder::default());
            }
            stack.push(name);
            text_buf.clear();
        }};
    }
    macro_rules! close_element {
        () => {{
            let name = stack.pop().unwrap_or_default();
            let parent = stack.last().cloned().unwrap_or_default();
            let txt = std::mem::take(&mut text_buf);
            match (parent.as_str(), name.as_str()) {
                ("QvdTableHeader", "TableName") => {
                    set_once(&mut table_name, txt, "TableName", None)?
                }
                ("QvdTableHeader", "QvBuildNo") => set_once(&mut build_no, txt, "QvBuildNo", None)?,
                ("QvdTableHeader", "Compression") => {
                    set_once(&mut compression, txt, "Compression", None)?
                }
                ("QvdTableHeader", "RecordByteSize") => set_once(
                    &mut record_byte_size,
                    parse_u32("RecordByteSize", &txt)?,
                    "RecordByteSize",
                    None,
                )?,
                ("QvdTableHeader", "NoOfRecords") => set_once(
                    &mut no_of_records,
                    parse_u32("NoOfRecords", &txt)?,
                    "NoOfRecords",
                    None,
                )?,
                ("QvdTableHeader", "Offset") => set_once(
                    &mut row_block_offset,
                    parse_u32("Offset", &txt)?,
                    "Offset",
                    None,
                )?,
                ("QvdTableHeader", "Length") => set_once(
                    &mut row_block_length,
                    parse_u32("Length", &txt)?,
                    "Length",
                    None,
                )?,
                ("QvdFieldHeader", element) => {
                    if let Some(f) = current.as_mut() {
                        let fname = f.name.clone();
                        let fname = fname.as_deref();
                        match element {
                            "FieldName" => set_once(&mut f.name, txt, "FieldName", None)?,
                            "BitOffset" => set_once(
                                &mut f.bit_offset,
                                parse_u32("BitOffset", &txt)?,
                                "BitOffset",
                                fname,
                            )?,
                            "BitWidth" => set_once(
                                &mut f.bit_width,
                                parse_u32("BitWidth", &txt)?,
                                "BitWidth",
                                fname,
                            )?,
                            "Bias" => {
                                set_once(&mut f.bias, parse_i32("Bias", &txt)?, "Bias", fname)?
                            }
                            "NoOfSymbols" => set_once(
                                &mut f.no_of_symbols,
                                parse_u32("NoOfSymbols", &txt)?,
                                "NoOfSymbols",
                                fname,
                            )?,
                            "Offset" => set_once(
                                &mut f.offset,
                                parse_u32("Offset", &txt)?,
                                "Offset",
                                fname,
                            )?,
                            "Length" => set_once(
                                &mut f.length,
                                parse_u32("Length", &txt)?,
                                "Length",
                                fname,
                            )?,
                            // NumberFormat, Tags and any element this crate
                            // does not interpret (for example Comment).
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
            if name == "QvdFieldHeader" {
                if let Some(f) = current.take() {
                    fields.push(f.build()?);
                }
            }
        }};
    }

    loop {
        match r.read_event().map_err(QvdError::xml)? {
            Event::Start(e) => {
                open_element!(e.name().as_ref().to_owned());
            }
            Event::End(_) => close_element!(),
            Event::Empty(e) => {
                open_element!(e.name().as_ref().to_owned());
                close_element!();
            }
            Event::Text(t) => text_buf.push_str(&t),
            Event::CData(c) => text_buf.push_str(&c),
            Event::GeneralRef(rf) => {
                // `&amp;`, `&lt;`, `&gt;`, `&apos;`, `&quot;` and numeric
                // character references. Anything else is a DTD-defined
                // entity, which a QVD header has no business using.
                if let Some(ch) = rf.resolve_char_ref().map_err(QvdError::xml)? {
                    text_buf.push(ch);
                } else if let Some(txt) = quick_xml::escape::resolve_predefined_entity(&rf) {
                    text_buf.push_str(txt);
                } else {
                    return Err(QvdError::UnknownEntity {
                        entity: rf.to_string(),
                    });
                }
            }
            Event::Eof => break,
            // Carry no header data.
            Event::Decl(_) | Event::PI(_) | Event::Comment(_) | Event::DocType(_) => {}
        }
    }

    if !seen_root {
        return Err(QvdError::HeaderEmpty);
    }
    if let Some(c) = &compression {
        if !c.trim().is_empty() {
            return Err(QvdError::UnsupportedCompression { value: c.clone() });
        }
    }
    let table_name = require(table_name, "TableName", None)?;
    if table_name.is_empty() && fields.is_empty() {
        return Err(QvdError::HeaderEmpty);
    }
    Ok(TableHeader {
        table_name,
        build_no: build_no.filter(|s| !s.is_empty()),
        fields,
        record_byte_size: require(record_byte_size, "RecordByteSize", None)?,
        no_of_records: require(no_of_records, "NoOfRecords", None)?,
        row_block_offset: require(row_block_offset, "Offset", None)?,
        row_block_length: require(row_block_length, "Length", None)?,
    })
}

fn parse_u32(element: &str, s: &str) -> Result<u32, QvdError> {
    s.trim()
        .parse::<u32>()
        .map_err(|_| QvdError::InvalidHeaderValue {
            element: element.to_owned(),
            value: s.to_owned(),
            expected: "u32",
        })
}

fn parse_i32(element: &str, s: &str) -> Result<i32, QvdError> {
    s.trim()
        .parse::<i32>()
        .map_err(|_| QvdError::InvalidHeaderValue {
            element: element.to_owned(),
            value: s.to_owned(),
            expected: "i32",
        })
}
