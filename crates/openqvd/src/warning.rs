//! Warnings: what a lenient read tolerated.

use std::fmt;

/// What kind of deviation a [`Warning`] reports.
///
/// `#[non_exhaustive]`; unknown future kinds can also arrive as
/// [`WarningKind::Other`] carrying a stable code.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WarningKind {
    /// A DATE, TIMESTAMP or TIME column held symbols that are not finite
    /// numbers and is exposed as text. See
    /// [`crate::LenientCheck::NonNumericTemporal`].
    NonNumericTemporal,
    /// A kind identified only by a code.
    Other(String),
}

impl WarningKind {
    /// Stable snake_case code for this kind.
    pub fn code(&self) -> &str {
        match self {
            WarningKind::NonNumericTemporal => "non_numeric_temporal",
            WarningKind::Other(code) => code,
        }
    }
}

/// One deviation that a lenient read tolerated.
///
/// Strict reads never produce warnings; they return an error instead.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Warning {
    /// What happened.
    pub kind: WarningKind,
    /// The affected column, when the warning is about one.
    pub column: Option<String>,
    /// The affected zero-based row, when the warning is about one.
    pub row: Option<u64>,
    /// Human-readable detail.
    pub detail: String,
}

impl Warning {
    /// A warning of `kind` with `detail` and no column or row.
    pub fn new(kind: WarningKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            column: None,
            row: None,
            detail: detail.into(),
        }
    }

    /// Attach the affected column.
    #[must_use]
    pub fn in_column(mut self, column: impl Into<String>) -> Self {
        self.column = Some(column.into());
        self
    }

    /// Attach the affected row.
    #[must_use]
    pub fn at_row(mut self, row: u64) -> Self {
        self.row = Some(row);
        self
    }
}

impl fmt::Display for Warning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind.code())?;
        if let Some(c) = &self.column {
            write!(f, " column {c:?}")?;
        }
        if let Some(r) = self.row {
            write!(f, " row {r}")?;
        }
        write!(f, ": {}", self.detail)
    }
}
