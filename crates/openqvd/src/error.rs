//! Error type for the crate.
//!
//! [`QvdError`] is `#[non_exhaustive]`: new variants are added in minor
//! releases, so always keep a wildcard arm. For coarse handling match on
//! [`QvdError::kind`], which returns an equally non-exhaustive
//! [`ErrorKind`].

use std::io;

/// Coarse classification of a [`QvdError`].
///
/// This is what bindings (and most callers) should branch on. Both this enum
/// and [`QvdError`] are `#[non_exhaustive]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Reading or writing a file failed at the operating-system level.
    Io,
    /// The XML table header is missing, unparsable or has bad values.
    Header,
    /// The binary layout contradicts the header or the specification.
    Structure,
    /// A symbol table entry is malformed.
    Symbol,
    /// A requested column does not exist, or table shapes disagree.
    Schema,
    /// A value cannot be converted to or from the requested type.
    Conversion,
    /// The input uses a feature this crate does not implement.
    Unsupported,
}

impl ErrorKind {
    /// Stable lower-case name, e.g. `"header"`. Used by the Python binding.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::Io => "io",
            ErrorKind::Header => "header",
            ErrorKind::Structure => "structure",
            ErrorKind::Symbol => "symbol",
            ErrorKind::Schema => "schema",
            ErrorKind::Conversion => "conversion",
            ErrorKind::Unsupported => "unsupported",
        }
    }
}

/// Errors produced by the QVD reader and writer.
///
/// Every variant carries structured context (what was expected, what was
/// found, where). The `Display` text is for humans; match on the variant or
/// on [`QvdError::kind`], never on the text.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum QvdError {
    /// Underlying I/O failure.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// No `</QvdTableHeader>` terminator followed by a NUL byte was found.
    #[error("invalid QVD header: no QvdTableHeader terminator")]
    HeaderNotFound,

    /// The header bytes are not valid UTF-8.
    #[error("invalid QVD header: header is not valid utf-8")]
    HeaderNotUtf8,

    /// The header has neither a table name nor any field.
    #[error("invalid QVD header: no TableName and no fields found")]
    HeaderEmpty,

    /// The document's root element is not `QvdTableHeader`.
    #[error("invalid QVD header: root element is <{found}>, expected <QvdTableHeader>")]
    UnexpectedRoot {
        /// The root element that was found.
        found: String,
    },

    /// A required header element is missing.
    #[error("invalid QVD header: missing <{element}>{}", field_suffix(.field))]
    MissingElement {
        /// Element name, e.g. `BitWidth`.
        element: String,
        /// The field it belongs to, for per-field elements.
        field: Option<String>,
    },

    /// A header element appears more than once.
    #[error("invalid QVD header: duplicate <{element}>{}", field_suffix(.field))]
    DuplicateElement {
        /// Element name.
        element: String,
        /// The field it belongs to, for per-field elements.
        field: Option<String>,
    },

    /// The header uses an XML entity other than the five predefined ones
    /// and numeric character references.
    #[error("invalid QVD header: unknown XML entity &{entity};")]
    UnknownEntity {
        /// Entity name without `&` and `;`.
        entity: String,
    },

    /// The table is compressed; this crate reads only uncompressed tables.
    #[error("unsupported QVD feature: <Compression> is {value:?}, only empty is supported")]
    UnsupportedCompression {
        /// The element's text.
        value: String,
    },

    /// The XML could not be parsed.
    #[error("xml parse error: {source}")]
    Xml {
        /// The underlying XML error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// A numeric header element did not contain a number of the expected
    /// type.
    #[error("invalid QVD header: element <{element}> expected {expected}, got {value:?}")]
    InvalidHeaderValue {
        /// Element name, e.g. `BitWidth`.
        element: String,
        /// The text that was found.
        value: String,
        /// What was expected, e.g. `"u32"`.
        expected: &'static str,
    },

    /// `NoOfRecords * RecordByteSize` does not fit in 32 bits.
    #[error("invalid QVD structure: n_records * record_byte_size overflow")]
    RowBlockSizeOverflow,

    /// The root `Length` disagrees with `NoOfRecords * RecordByteSize`.
    #[error(
        "invalid QVD structure: row block length {declared} != n_records * record_byte_size {expected}"
    )]
    RowBlockLengthMismatch {
        /// Value of the root `<Length>` element.
        declared: u32,
        /// `NoOfRecords * RecordByteSize`.
        expected: u32,
    },

    /// The row block extends past the end of the file body.
    #[error("invalid QVD structure: row block [{start}..{end}) exceeds body len {body_len}")]
    RowBlockOutOfRange {
        /// First byte of the row block, relative to the body.
        start: u64,
        /// One past the last byte of the row block.
        end: u64,
        /// Length of the body in bytes.
        body_len: usize,
    },

    /// Bytes follow the row block.
    #[error("invalid QVD structure: {extra} trailing bytes after the row block")]
    TrailingBytes {
        /// Number of bytes after the declared row block.
        extra: u64,
    },

    /// A field is wider than any symbol index can need.
    #[error(
        "invalid QVD structure: field {field:?} BitWidth {bit_width} exceeds the maximum {max}"
    )]
    BitWidthOutOfRange {
        /// Field name.
        field: String,
        /// `BitWidth`.
        bit_width: u32,
        /// The widest accepted field.
        max: u32,
    },

    /// A field uses a `Bias` other than the two the format is known to use.
    #[error("unsupported QVD feature: field {field:?} has Bias {bias}; only 0 and -2 are known")]
    UnsupportedBias {
        /// Field name.
        field: String,
        /// `Bias`.
        bias: i32,
    },

    /// A symbol table lies (partly) inside the row block.
    #[error(
        "invalid QVD structure: field {field:?} symbol region [{start}..{end}) reaches the row block at {row_block_start}"
    )]
    SymbolRegionInRowBlock {
        /// Field name.
        field: String,
        /// First byte of the region.
        start: u64,
        /// One past the last byte.
        end: u64,
        /// First byte of the row block.
        row_block_start: u64,
    },

    /// Two symbol tables overlap.
    #[error("invalid QVD structure: field {field:?} symbol region at {start} overlaps another")]
    SymbolRegionOverlap {
        /// Field name.
        field: String,
        /// First byte of the region.
        start: u64,
    },

    /// Bytes between symbol tables, or between the last table and the row
    /// block, belong to no field.
    #[error("invalid QVD structure: bytes [{start}..{end}) in front of the row block belong to no symbol table")]
    SymbolRegionGap {
        /// First unclaimed byte.
        start: u64,
        /// One past the last unclaimed byte.
        end: u64,
    },

    /// A record has a bit set that no field claims.
    #[error("invalid QVD structure: row {row} byte {byte} has bits set outside every field")]
    UnusedBitsSet {
        /// Zero-based row.
        row: u64,
        /// Byte within the record.
        byte: u32,
    },

    /// A field's bit range does not fit in a record, or overflows.
    #[error(
        "invalid QVD structure: field {field:?} bits [{bit_offset}..+{bit_width}) exceed record size {record_bits} bits"
    )]
    FieldBitsOutOfRange {
        /// Field name.
        field: String,
        /// `BitOffset`.
        bit_offset: u32,
        /// `BitWidth`.
        bit_width: u32,
        /// Record size in bits.
        record_bits: u32,
    },

    /// Two fields claim the same bit of a record.
    #[error("invalid QVD structure: field {field:?} overlaps another at bit {bit}")]
    FieldBitsOverlap {
        /// Field name.
        field: String,
        /// The first shared bit.
        bit: u32,
    },

    /// A field's symbol region extends past the end of the file body.
    #[error(
        "invalid QVD structure: field {field:?} symbol region [{start}..{end}) exceeds body len {body_len}"
    )]
    SymbolRegionOutOfRange {
        /// Field name.
        field: String,
        /// First byte of the region, relative to the body.
        start: u64,
        /// One past the last byte.
        end: u64,
        /// Length of the body in bytes.
        body_len: usize,
    },

    /// A field's symbol table has fewer entries than `NoOfSymbols`.
    #[error("invalid QVD structure: field {field:?}: ran out of symbol bytes")]
    SymbolTableTruncated {
        /// Field name.
        field: String,
    },

    /// A field's symbol region has bytes left over after `NoOfSymbols`
    /// entries.
    #[error("invalid QVD structure: field {field:?}: {extra} trailing bytes in symbol region")]
    SymbolTableTrailingBytes {
        /// Field name.
        field: String,
        /// Number of unconsumed bytes.
        extra: usize,
    },

    /// A row refers to a symbol index beyond the field's symbol table.
    #[error(
        "invalid QVD structure: row {row}: field {field:?} symbol index {index} is out of range (n={symbols})"
    )]
    SymbolIndexOutOfRange {
        /// Zero-based row number.
        row: u64,
        /// Field name.
        field: String,
        /// The offending index.
        index: u64,
        /// Number of symbols in the field's table.
        symbols: usize,
    },

    /// A fixed-size value runs past the end of its region.
    #[error("invalid QVD structure: truncated {what} at byte {offset}")]
    Truncated {
        /// What was being read, e.g. `"i32 symbol"`.
        what: &'static str,
        /// Byte offset where it starts.
        offset: usize,
    },

    /// A string symbol has no NUL terminator.
    #[error("invalid QVD structure: unterminated {what}")]
    UnterminatedString {
        /// Which symbol kind, e.g. `"string symbol"`.
        what: &'static str,
    },

    /// An unknown symbol type byte was encountered in the body.
    #[error("unknown symbol type byte 0x{byte:02x} at offset {offset}")]
    UnknownSymbolType {
        /// The offending type byte.
        byte: u8,
        /// Byte offset within the file where the type byte lives.
        offset: usize,
    },

    /// Invalid UTF-8 in a symbol string.
    #[error("invalid utf-8 in symbol at offset {offset}")]
    Utf8 {
        /// Byte offset within the file where the string began.
        offset: usize,
    },

    /// A requested or filtered column does not exist.
    #[error("column {name:?} not found")]
    ColumnNotFound {
        /// The requested name.
        name: String,
    },

    /// A column was read that is outside `ReadOptions::columns`, so its
    /// symbol table was never decoded.
    #[error("column {field:?} was not decoded; include it in ReadOptions::columns")]
    FieldNotDecoded {
        /// Field name.
        field: String,
    },

    /// Two columns have the same name; lookups by name would silently pick
    /// one.
    #[error("duplicate column name {name:?}")]
    DuplicateColumn {
        /// The repeated name.
        name: String,
    },

    /// A column has a different number of cells than the first column.
    #[error("column {column:?} has {found} cells, expected {expected}")]
    ColumnLengthMismatch {
        /// Column name.
        column: String,
        /// Cells in this column.
        found: usize,
        /// Cells in the first column.
        expected: usize,
    },

    /// A column has more distinct values than the format can index.
    #[error("column {column:?}: cardinality exceeds the 64-bit bit width limit")]
    CardinalityTooLarge {
        /// Column name.
        column: String,
    },

    /// A string contains a NUL byte, which the format reserves as the
    /// symbol terminator.
    #[error("string symbol contains NUL byte, which is reserved as the symbol terminator")]
    NulInString,

    /// A layout quantity does not fit in the 32 bits the header allows.
    #[error("{what} does not fit in 32 bits")]
    LayoutOverflow {
        /// What overflowed, e.g. `"row block size"`.
        what: &'static str,
    },

    /// A column typed DATE, TIMESTAMP or TIME holds a symbol that is not a
    /// finite number.
    #[error(
        "column {field:?} is typed {hint} but symbol {index} is {symbol}; \
         allow the non_numeric_temporal lenient check to read it as text"
    )]
    NonNumericTemporal {
        /// Column name.
        field: String,
        /// The `NumberFormat/Type` hint.
        hint: String,
        /// The offending symbol, rendered.
        symbol: String,
        /// Its position in the symbol table.
        index: u64,
    },

    /// A text symbol reached a numeric conversion. Column typing makes this
    /// unreachable; it is reported instead of being read as 0.
    #[error("column {field:?}: symbol {symbol} is not a number")]
    TextInNumericColumn {
        /// Column name.
        field: String,
        /// The symbol, rendered.
        symbol: String,
    },

    /// A date or time value does not fit the target type.
    #[error("column {field:?}: value {value} is outside the supported date/time range")]
    TemporalOutOfRange {
        /// Column name.
        field: String,
        /// The Qlik serial.
        value: f64,
    },

    /// An Arrow timestamp has a time zone, which QVD cannot store.
    #[error(
        "column {column:?}: timestamp has time zone {timezone:?}; convert it to a naive timestamp \
         first, or allow the timezone_to_utc lenient check to store UTC wall-clock time"
    )]
    TimezoneNotSupported {
        /// Column name.
        column: String,
        /// The Arrow time zone string.
        timezone: String,
    },

    /// A value cannot be stored in QVD without changing it.
    #[error("column {column:?}: value {value} cannot be stored exactly: {detail}")]
    ValueNotRepresentable {
        /// Column name.
        column: String,
        /// The value, rendered.
        value: String,
        /// Why not.
        detail: &'static str,
    },

    /// An Arrow type has no defined mapping to a QVD value.
    #[error("column {column:?}: arrow type {data_type} has no QVD mapping")]
    UnsupportedArrowType {
        /// Column name.
        column: String,
        /// The Arrow type, rendered by Arrow.
        data_type: String,
    },

    /// Arrow rejected the data while building or reading a batch.
    #[error("arrow error: {detail}")]
    Arrow {
        /// Arrow's message.
        detail: String,
    },
}

fn field_suffix(field: &Option<String>) -> String {
    match field {
        Some(f) => format!(" in field {f:?}"),
        None => String::new(),
    }
}

impl QvdError {
    /// Coarse classification, for branching and for language bindings.
    pub fn kind(&self) -> ErrorKind {
        use QvdError::*;
        match self {
            Io(_) => ErrorKind::Io,
            HeaderNotFound
            | HeaderNotUtf8
            | HeaderEmpty
            | UnexpectedRoot { .. }
            | MissingElement { .. }
            | DuplicateElement { .. }
            | UnknownEntity { .. }
            | Xml { .. }
            | InvalidHeaderValue { .. } => ErrorKind::Header,
            UnsupportedCompression { .. } | UnsupportedBias { .. } => ErrorKind::Unsupported,
            RowBlockSizeOverflow
            | RowBlockLengthMismatch { .. }
            | RowBlockOutOfRange { .. }
            | FieldBitsOutOfRange { .. }
            | FieldBitsOverlap { .. }
            | SymbolRegionOutOfRange { .. }
            | SymbolTableTruncated { .. }
            | SymbolTableTrailingBytes { .. }
            | SymbolIndexOutOfRange { .. }
            | TrailingBytes { .. }
            | BitWidthOutOfRange { .. }
            | SymbolRegionInRowBlock { .. }
            | SymbolRegionOverlap { .. }
            | SymbolRegionGap { .. }
            | UnusedBitsSet { .. } => ErrorKind::Structure,
            Truncated { .. }
            | UnterminatedString { .. }
            | UnknownSymbolType { .. }
            | Utf8 { .. } => ErrorKind::Symbol,
            ColumnNotFound { .. }
            | FieldNotDecoded { .. }
            | DuplicateColumn { .. }
            | ColumnLengthMismatch { .. } => ErrorKind::Schema,
            NonNumericTemporal { .. }
            | TextInNumericColumn { .. }
            | TimezoneNotSupported { .. }
            | ValueNotRepresentable { .. }
            | TemporalOutOfRange { .. }
            | CardinalityTooLarge { .. }
            | NulInString
            | LayoutOverflow { .. }
            | UnsupportedArrowType { .. }
            | Arrow { .. } => ErrorKind::Conversion,
        }
    }

    pub(crate) fn xml(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        QvdError::Xml {
            source: Box::new(e),
        }
    }
}
