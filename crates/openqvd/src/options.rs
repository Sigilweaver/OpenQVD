//! Options for reading, and the single [`Leniency`] set that controls how
//! strict the reader is.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

/// One check that strict mode enforces and lenient mode may skip.
///
/// A lenient read never skips a check silently: every deviation it tolerates
/// is reported as a [`crate::Warning`]. Checks exist here only where real
/// files would otherwise stop opening. The enum is `#[non_exhaustive]`; new
/// checks are added in minor releases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum LenientCheck {
    /// A column typed DATE, TIMESTAMP or TIME holds symbols that are not
    /// finite numbers. Lenient reading exposes the column as text.
    NonNumericTemporal,
}

impl LenientCheck {
    /// Every check this version of the crate knows about.
    pub const ALL: &'static [LenientCheck] = &[LenientCheck::NonNumericTemporal];

    /// Stable snake_case name, e.g. `"non_numeric_temporal"`.
    pub fn as_str(self) -> &'static str {
        match self {
            LenientCheck::NonNumericTemporal => "non_numeric_temporal",
        }
    }
}

impl fmt::Display for LenientCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returned by [`LenientCheck::from_str`] for an unknown name.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UnknownCheck {
    name: String,
}

impl UnknownCheck {
    /// The name that was not recognised.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for UnknownCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown lenient check {:?}", self.name)
    }
}

impl std::error::Error for UnknownCheck {}

impl FromStr for LenientCheck {
    type Err = UnknownCheck;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        LenientCheck::ALL
            .iter()
            .copied()
            .find(|c| c.as_str() == s)
            .ok_or_else(|| UnknownCheck { name: s.to_owned() })
    }
}

/// The set of [`LenientCheck`]s a caller accepts. The default is the empty
/// set, which is strict mode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Leniency {
    allowed: BTreeSet<LenientCheck>,
}

impl Leniency {
    /// Strict mode: no check may be skipped.
    pub fn strict() -> Self {
        Self::default()
    }

    /// Every check this version knows about may be skipped. Because new
    /// checks join this set over time, name the checks you rely on with
    /// [`Self::with`] when you need a fixed behavior.
    pub fn lenient() -> Self {
        Self {
            allowed: LenientCheck::ALL.iter().copied().collect(),
        }
    }

    /// Allow one more check.
    #[must_use]
    pub fn with(mut self, check: LenientCheck) -> Self {
        self.allowed.insert(check);
        self
    }

    /// Whether `check` may be skipped.
    pub fn allows(&self, check: LenientCheck) -> bool {
        self.allowed.contains(&check)
    }

    /// True when no check may be skipped.
    pub fn is_strict(&self) -> bool {
        self.allowed.is_empty()
    }

    /// The allowed checks, in a stable order.
    pub fn checks(&self) -> impl Iterator<Item = LenientCheck> + '_ {
        self.allowed.iter().copied()
    }
}

/// Options for [`crate::Qvd::open`] and [`crate::Qvd::parse`].
///
/// `#[non_exhaustive]` with a `Default` that is strict and reads every
/// column. Build one with `ReadOptions::default()` and the `with_*` methods.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ReadOptions {
    /// Decode symbol tables only for these columns (`None` = all). A name
    /// that is not in the file is an error.
    pub columns: Option<Vec<String>>,
    /// Which checks may be skipped.
    pub leniency: Leniency,
}

impl ReadOptions {
    /// Strict options that read every column.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restrict decoding to the named columns.
    #[must_use]
    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Set the leniency.
    #[must_use]
    pub fn with_leniency(mut self, leniency: Leniency) -> Self {
        self.leniency = leniency;
        self
    }
}

/// Options for converting external data (for example an Arrow batch) into a
/// [`crate::WriteTable`].
///
/// `#[non_exhaustive]` with a `Default` that is strict.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WriteOptions {
    /// Value of `<TableName>` in the written file.
    pub table_name: String,
    /// Which checks may be skipped.
    pub leniency: Leniency,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            table_name: "table".to_owned(),
            leniency: Leniency::strict(),
        }
    }
}

impl WriteOptions {
    /// Strict options with the default table name.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the table name.
    #[must_use]
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.table_name = name.into();
        self
    }

    /// Set the leniency.
    #[must_use]
    pub fn with_leniency(mut self, leniency: Leniency) -> Self {
        self.leniency = leniency;
        self
    }
}
