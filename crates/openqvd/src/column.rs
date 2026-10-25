//! Arrow-free column access: the decoded form every adapter (Arrow, Python,
//! ...) converts from.
//!
//! A [`ColumnData`] is dictionary-shaped. `symbols` holds one converted value
//! per distinct symbol and `indices` maps each row to a symbol (or NULL).
//! Converting once per symbol keeps wide string columns cheap and means no
//! adapter re-implements the type rules.

use crate::error::QvdError;
use crate::header::FieldHeader;
use crate::options::{Leniency, LenientCheck};
use crate::reader::{Qvd, NULL_INDEX};
use crate::value::Value;
use crate::warning::{Warning, WarningKind};

/// Qlik date serial epoch: days from 30 Dec 1899 to 1 Jan 1970.
pub(crate) const QLIK_EPOCH_OFFSET: i32 = 25_569;

/// The logical type of a decoded column.
///
/// The type is decided by `NumberFormat/Type` and by the data, and is part
/// of the output contract:
///
/// - `DATE`: `Date32` when every value is a whole number of days, otherwise
///   `TimestampMicros` (a `Date(Now())` keeps its time of day; nothing is
///   dropped). One value with a fraction changes the whole column, so the
///   type of the same logical column can differ between two extracts. Read
///   it with [`Qvd::column_type`] (Python: `FieldInfo.dtype`) before relying
///   on it.
/// - `TIMESTAMP`: `TimestampMicros`.
/// - `TIME`: `DurationMicros` when every value is in `[0, 1)` days,
///   otherwise `TimestampMicros` (the values carry a date).
/// - `INTERVAL`: `DurationMicros`; may exceed a day.
/// - These four hints require finite numbers: text or NaN is an error, or
///   with the `non_numeric_temporal` leniency the column is `Utf8` plus a
///   warning.
/// - Any other hint: Int and DualInt symbols give `Int64`, Float and
///   DualFloat give `Float64`, any string symbol gives `Utf8`, and an empty
///   symbol table gives `Null`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ColumnType {
    /// No symbols: every row is NULL.
    Null,
    /// 64-bit signed integers.
    Int64,
    /// 64-bit floats.
    Float64,
    /// UTF-8 text.
    Utf8,
    /// Days since 1970-01-01.
    Date32,
    /// Microseconds since 1970-01-01T00:00:00, no time zone.
    TimestampMicros,
    /// Microseconds since midnight (a Qlik time of day).
    DurationMicros,
}

impl ColumnType {
    /// Stable snake_case name, e.g. `"timestamp_us"`.
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnType::Null => "null",
            ColumnType::Int64 => "int64",
            ColumnType::Float64 => "float64",
            ColumnType::Utf8 => "utf8",
            ColumnType::Date32 => "date32",
            ColumnType::TimestampMicros => "timestamp_us",
            ColumnType::DurationMicros => "duration_us",
        }
    }
}

/// One converted value per distinct symbol of a column.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ColumnValues {
    /// No symbols.
    Null,
    /// See [`ColumnType::Int64`].
    Int64(Vec<i64>),
    /// See [`ColumnType::Float64`].
    Float64(Vec<f64>),
    /// See [`ColumnType::Utf8`].
    Utf8(Vec<String>),
    /// See [`ColumnType::Date32`].
    Date32(Vec<i32>),
    /// See [`ColumnType::TimestampMicros`].
    TimestampMicros(Vec<i64>),
    /// See [`ColumnType::DurationMicros`].
    DurationMicros(Vec<i64>),
}

/// A decoded column: converted symbols plus the symbol index of every row.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ColumnData {
    /// Column name.
    pub name: String,
    /// Logical type, matching the variant of `symbols`.
    pub dtype: ColumnType,
    /// One converted value per distinct symbol.
    pub symbols: ColumnValues,
    indices: Vec<u32>,
}

impl ColumnData {
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// True when the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Index into `symbols` for `row`, or `None` for NULL.
    ///
    /// Panics if `row >= self.len()`.
    pub fn index(&self, row: usize) -> Option<usize> {
        match self.indices[row] {
            NULL_INDEX => None,
            i => Some(i as usize),
        }
    }

    /// A copy of this column keeping only `rows` (ascending row numbers).
    ///
    /// Panics if a row number is `>= self.len()`.
    pub fn select(&self, rows: &[u32]) -> ColumnData {
        let mut out = self.clone();
        out.indices = rows.iter().map(|&r| self.indices[r as usize]).collect();
        out
    }

    /// Symbol index of every row in order, `None` for NULL.
    pub fn iter(&self) -> impl Iterator<Item = Option<usize>> + '_ {
        self.indices.iter().map(|&i| match i {
            NULL_INDEX => None,
            i => Some(i as usize),
        })
    }
}

/// The finite numeric value of a symbol, `None` for text and for NaN/Inf.
fn finite_number(v: &Value) -> Option<f64> {
    let n = match v {
        Value::Int(i) => f64::from(*i),
        Value::DualInt(d) => f64::from(d.number),
        Value::Float(f) => *f,
        Value::DualFloat(d) => d.number,
        _ => return None,
    };
    n.is_finite().then_some(n)
}

/// What the column's type is, and any lenient decision taken to get there.
///
/// Called once per decoded field when a file is opened. A column that
/// `NumberFormat/Type` calls DATE, TIMESTAMP or TIME must hold only finite
/// numbers; otherwise it is an error, or, when the caller allowed
/// [`LenientCheck::NonNumericTemporal`], text plus a warning.
pub(crate) fn resolve_type(
    field: &FieldHeader,
    symbols: &[Value],
    leniency: &Leniency,
    warnings: &mut Vec<Warning>,
) -> Result<ColumnType, QvdError> {
    let hint = field.number_format.r#type.as_str();
    if matches!(hint, "DATE" | "TIMESTAMP" | "TIME" | "INTERVAL") {
        let bad: Vec<(usize, &Value)> = symbols
            .iter()
            .enumerate()
            .filter(|(_, v)| finite_number(v).is_none())
            .collect();
        if let Some(&(first_idx, first)) = bad.first() {
            if !leniency.allows(LenientCheck::NonNumericTemporal) {
                return Err(QvdError::NonNumericTemporal {
                    field: field.name.clone(),
                    hint: hint.to_owned(),
                    symbol: describe(first),
                    index: first_idx as u64,
                });
            }
            warnings.push(
                Warning::new(
                    WarningKind::NonNumericTemporal,
                    format!(
                        "{} of {} symbols are not finite numbers (first: {}); \
                         the {hint} column is returned as text (numbers without \
                         a dual text are formatted by Rust, e.g. 45000.5)",
                        bad.len(),
                        symbols.len(),
                        describe(first)
                    ),
                )
                .in_column(field.name.clone()),
            );
            return Ok(ColumnType::Utf8);
        }
        let dtype = match hint {
            // A DATE whose values carry a time of day keeps it: the column
            // becomes a timestamp rather than losing the fraction.
            "DATE" if symbols.iter().all(has_whole_days) => ColumnType::Date32,
            "DATE" | "TIMESTAMP" => ColumnType::TimestampMicros,
            // A time of day is a fraction of a day. A TIME column with a
            // value outside [0, 1) holds date-times (Qlik's Time(Now())
            // keeps the date), so, like DATE, it becomes a timestamp.
            "TIME" if symbols.iter().all(is_time_of_day) => ColumnType::DurationMicros,
            "TIME" => ColumnType::TimestampMicros,
            // INTERVAL is a length of time and may exceed a day.
            _ => ColumnType::DurationMicros,
        };
        // Reject values that do not fit the target type now, at open.
        convert(&field.name, dtype, symbols)?;
        return Ok(dtype);
    }
    Ok(infer_from_values(symbols))
}

fn is_time_of_day(v: &Value) -> bool {
    finite_number(v).is_some_and(|n| (0.0..1.0).contains(&n))
}

fn has_whole_days(v: &Value) -> bool {
    finite_number(v).is_some_and(|n| n.fract() == 0.0)
}

fn describe(v: &Value) -> String {
    match v {
        Value::Str(s) => format!("text {s:?}"),
        other => format!("{other:?}"),
    }
}

fn infer_from_values(symbols: &[Value]) -> ColumnType {
    if symbols.is_empty() {
        return ColumnType::Null;
    }
    let mut has_float = false;
    for s in symbols {
        match s {
            Value::Str(_) => return ColumnType::Utf8,
            Value::Float(_) | Value::DualFloat(_) => has_float = true,
            _ => {}
        }
    }
    if has_float {
        ColumnType::Float64
    } else {
        ColumnType::Int64
    }
}

fn text_in_numeric_column(field: &str, v: &Value) -> QvdError {
    // Unreachable for a resolved type (a text symbol makes the column
    // Utf8); reported rather than defaulted to 0 if that ever changes.
    QvdError::TextInNumericColumn {
        field: field.to_owned(),
        symbol: describe(v),
    }
}

fn value_as_i64(field: &str, v: &Value) -> Result<i64, QvdError> {
    Ok(match v {
        Value::Int(i) => i64::from(*i),
        Value::DualInt(d) => i64::from(d.number),
        Value::Float(f) => *f as i64,
        Value::DualFloat(d) => d.number as i64,
        other => return Err(text_in_numeric_column(field, other)),
    })
}

fn value_as_f64(field: &str, v: &Value) -> Result<f64, QvdError> {
    Ok(match v {
        Value::Float(f) => *f,
        Value::DualFloat(d) => d.number,
        Value::Int(i) => f64::from(*i),
        Value::DualInt(d) => f64::from(d.number),
        other => return Err(text_in_numeric_column(field, other)),
    })
}

fn value_as_str(v: &Value) -> String {
    match v {
        Value::Str(s) => s.clone(),
        Value::DualInt(d) => d.text.clone(),
        Value::DualFloat(d) => d.text.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
    }
}

fn qlik_days_to_unix(field: &str, n: f64) -> Result<i32, QvdError> {
    crate::serial::serial_to_unix_days(n).ok_or_else(|| QvdError::TemporalOutOfRange {
        field: field.to_owned(),
        value: n,
    })
}

fn days_to_micros(field: &str, days: f64) -> Result<i64, QvdError> {
    crate::serial::duration_days_to_micros(days).ok_or_else(|| QvdError::TemporalOutOfRange {
        field: field.to_owned(),
        value: days,
    })
}

fn convert(field: &str, dtype: ColumnType, symbols: &[Value]) -> Result<ColumnValues, QvdError> {
    Ok(match dtype {
        ColumnType::Int64 => ColumnValues::Int64(
            symbols
                .iter()
                .map(|v| value_as_i64(field, v))
                .collect::<Result<_, _>>()?,
        ),
        ColumnType::Float64 => ColumnValues::Float64(
            symbols
                .iter()
                .map(|v| value_as_f64(field, v))
                .collect::<Result<_, _>>()?,
        ),
        ColumnType::Utf8 => ColumnValues::Utf8(symbols.iter().map(value_as_str).collect()),
        ColumnType::Date32 => ColumnValues::Date32(
            symbols
                .iter()
                .map(|v| qlik_days_to_unix(field, value_as_f64(field, v)?))
                .collect::<Result<_, _>>()?,
        ),
        ColumnType::TimestampMicros => ColumnValues::TimestampMicros(
            symbols
                .iter()
                .map(|v| {
                    days_to_micros(
                        field,
                        value_as_f64(field, v)? - f64::from(QLIK_EPOCH_OFFSET),
                    )
                })
                .collect::<Result<_, _>>()?,
        ),
        ColumnType::DurationMicros => ColumnValues::DurationMicros(
            symbols
                .iter()
                .map(|v| days_to_micros(field, value_as_f64(field, v)?))
                .collect::<Result<_, _>>()?,
        ),
        _ => ColumnValues::Null,
    })
}

impl Qvd {
    /// Index of the field called `name`.
    pub fn field_index(&self, name: &str) -> Result<usize, QvdError> {
        self.fields()
            .iter()
            .position(|f| f.name == name)
            .ok_or_else(|| QvdError::ColumnNotFound {
                name: name.to_owned(),
            })
    }

    /// Decode column `index` (a position in [`Qvd::fields`]).
    ///
    /// Errors if the column was left out of `ReadOptions::columns` or a row
    /// points outside its symbol table.
    pub fn column(&self, index: usize) -> Result<ColumnData, QvdError> {
        let field = self
            .fields()
            .get(index)
            .ok_or_else(|| QvdError::ColumnNotFound {
                name: format!("#{index}"),
            })?;
        let indices = self.column_indices(index)?;
        let symbols = self.symbols(index).unwrap_or(&[]);
        let dtype = self.column_type(index)?;
        Ok(ColumnData {
            name: field.name.clone(),
            dtype,
            symbols: convert(&field.name, dtype, symbols)?,
            indices,
        })
    }
}
