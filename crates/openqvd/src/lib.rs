//! OpenQVD: clean-room reader for Qlik QVD files.
//!
//! Implements the specification in `SPEC.md` (AGPL-3.0-or-later). Derived
//! entirely from binary analysis of a public corpus; no existing QVD
//! parsers have been consulted.
//!
//! # Quick start
//!
//! ```no_run
//! use openqvd::Qvd;
//!
//! let qvd = Qvd::from_path("data.qvd").unwrap();
//! println!("table {:?} with {} rows", qvd.table_name(), qvd.num_rows());
//! for row in qvd.rows() {
//!     for (field, value) in qvd.fields().iter().zip(row) {
//!         println!("  {} = {:?}", field.name, value);
//!     }
//! }
//! ```
//!
//! The reader is strict: any deviation from the spec produces a
//! [`QvdError`] rather than silent misinterpretation.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod error;
mod header;
mod reader;
mod symbols;
mod value;
mod writer;

pub use error::QvdError;
pub use header::{FieldHeader, NumberFormat, TableHeader};
pub use reader::Qvd;
pub use value::{Dual, Value};
pub use writer::{Column, WriteTable};
