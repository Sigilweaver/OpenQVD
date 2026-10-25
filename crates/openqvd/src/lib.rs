//! OpenQVD: clean-room reader for Qlik QVD files.
//!
//! Implements the specification in `SPEC.md` (Apache-2.0). Derived
//! entirely from binary analysis of a public corpus; no existing QVD
//! parsers have been consulted.
//!
//! # Quick start
//!
//! ```no_run
//! use openqvd::{Qvd, ReadOptions};
//!
//! let qvd = Qvd::open("data.qvd", &ReadOptions::default()).unwrap();
//! println!("table {:?} with {} rows", qvd.table_name(), qvd.num_rows());
//! for row in qvd.rows() {
//!     let row = row.unwrap();
//!     for (field, value) in qvd.fields().iter().zip(row) {
//!         println!("  {} = {:?}", field.name, value);
//!     }
//! }
//! ```
//!
//! The reader is strict by default: any deviation from the spec produces a
//! [`QvdError`] rather than silent misinterpretation. [`ReadOptions`] carries
//! an explicit [`Leniency`] for the few checks a caller may choose to skip;
//! everything a lenient read tolerates is reported as a [`Warning`].
//!
//! All public enums and data structs are `#[non_exhaustive]` and gain
//! variants and fields in minor releases: keep a wildcard arm when matching.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod column;
mod error;
mod filter;
mod header;
mod options;
mod reader;
pub mod serial;
mod symbols;
mod value;
mod warning;
mod writer;

pub(crate) mod bytes;

pub use column::{ColumnData, ColumnType, ColumnValues};
pub use error::{ErrorKind, QvdError};
pub use filter::{ColumnFilter, Filter, FilterValue};
pub use header::{FieldHeader, NumberFormat, TableHeader};
pub use options::{Leniency, LenientCheck, ReadOptions, UnknownCheck, WriteOptions};
pub use reader::Qvd;
pub use value::{Cell, Dual, Value};
pub use warning::{Warning, WarningKind};
pub use writer::{Column, WriteTable};
