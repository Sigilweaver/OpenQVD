//! Arrow adapters for [`openqvd`].
//!
//! Enable one feature per Arrow major (`arrow-59`, `arrow-60`, ...). Each
//! feature adds a module of the same shape (`v59`, `v60`, ...) with:
//!
//! - `to_record_batch(&Qvd, &BatchOptions) -> Result<RecordBatch, QvdError>`
//! - `record_batch_to_write_table(&RecordBatch, &WriteOptions)`
//! - `record_batches_to_write_table(schema, batches, &WriteOptions)`
//!
//! The version-independent option type [`BatchOptions`] lives at the crate
//! root. A new Arrow major is a new feature and module; existing modules do
//! not change.
//!
//! Every module is built from the same source file (`imp.rs`), so an Arrow
//! major whose API differs needs `cfg` branches inside that file, not a
//! second copy.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use openqvd::Filter;

/// Options for converting a [`openqvd::Qvd`] to a record batch.
///
/// `#[non_exhaustive]` with a `Default` that selects every column and every
/// row.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct BatchOptions {
    /// Output columns, in this order (`None` = all, in file order). A name
    /// that is not in the file is an error.
    pub columns: Option<Vec<String>>,
    /// Keep only rows for which every filter holds. Filter columns do not
    /// need to be output columns, but must have been decoded.
    pub filters: Vec<Filter>,
}

impl BatchOptions {
    /// Every column, every row.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict the output columns.
    #[must_use]
    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Keep only rows matching `filters`.
    #[must_use]
    pub fn with_filters(mut self, filters: Vec<Filter>) -> Self {
        self.filters = filters;
        self
    }
}

macro_rules! arrow_version {
    ($feature:literal, $module:ident, $array:ident, $schema:ident, $cast:ident) => {
        #[cfg(feature = $feature)]
        #[doc = concat!("Adapters for the Arrow ", $feature, " crates.")]
        pub mod $module {
            use $array as arrow_array;
            use $cast as arrow_cast;
            use $schema as arrow_schema;
            include!("imp.rs");
        }
    };
}

arrow_version!(
    "arrow-59",
    v59,
    arrow_array_59,
    arrow_schema_59,
    arrow_cast_59
);
arrow_version!(
    "arrow-60",
    v60,
    arrow_array_60,
    arrow_schema_60,
    arrow_cast_60
);
