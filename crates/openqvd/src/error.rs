use std::fmt;
use std::io;

/// Errors produced by the QVD reader.
#[derive(Debug, thiserror::Error)]
pub enum QvdError {
    /// Underlying I/O failure.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// The XML header was not found or was malformed.
    #[error("invalid QVD header: {0}")]
    BadHeader(String),

    /// The XML could not be parsed.
    #[error("xml parse error: {0}")]
    Xml(String),

    /// A structural invariant from the spec was violated.
    #[error("invalid QVD structure: {0}")]
    Structure(String),

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
}

impl QvdError {
    pub(crate) fn bad_header(msg: impl fmt::Display) -> Self {
        QvdError::BadHeader(msg.to_string())
    }
    pub(crate) fn structure(msg: impl fmt::Display) -> Self {
        QvdError::Structure(msg.to_string())
    }
}
