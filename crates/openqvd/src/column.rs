//! Arrow-free column access: the decoded form every adapter (Arrow, Python,
//! ...) converts from.
//!
//! A [`ColumnData`] is dictionary-shaped. `symbols` holds one converted value
//! per distinct symbol and `indices` maps each row to a symbol (or NULL).
//! Converting once per symbol keeps wide string columns cheap and means no
//! adapter re-implements the type rules.

use crate::error::QvdError;
use crate::header::FieldHeader;
use crate::reader::{Qvd, NULL_INDEX};
use crate::value::Value;

/// Qlik date serial epoch: days from 30 Dec 1899 to 1 Jan 1970.
pub(crate) const QLIK_EPOCH_OFFSET: i32 = 25_569;

/// The logical type of a decoded column.
///
/// For well-formed files the type is determined by `NumberFormat/Type` and
/// the symbol kinds: DATE gives `Date32`, TIMESTAMP gives `TimestampMicros`,
/// TIME gives `DurationMicros`; otherwise Int and DualInt symbols give
/// `Int64`, Float and DualFloat give `Float64`, any string symbol gives
/// `Utf8`, and an empty symbol table gives `Null`.
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

fn infer_type(field: &FieldHeader, symbols: &[Value]) -> ColumnType {
    match field.number_format.r#type.as_str() {
        "DATE" => return ColumnType::Date32,
        "TIMESTAMP" => return ColumnType::TimestampMicros,
        "TIME" => return ColumnType::DurationMicros,
        _ => {}
    }
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

fn value_as_i64(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i as i64,
        Value::DualInt(d) => d.number as i64,
        Value::Float(f) => *f as i64,
        Value::DualFloat(d) => d.number as i64,
        Value::Str(s) => s.parse().unwrap_or(0),
    }
}

fn value_as_f64(v: &Value) -> f64 {
    match v {
        Value::Float(f) => *f,
        Value::DualFloat(d) => d.number,
        Value::Int(i) => *i as f64,
        Value::DualInt(d) => d.number as f64,
        Value::Str(s) => s.parse().unwrap_or(0.0),
    }
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

fn convert(dtype: ColumnType, symbols: &[Value]) -> ColumnValues {
    match dtype {
        ColumnType::Int64 => ColumnValues::Int64(symbols.iter().map(value_as_i64).collect()),
        ColumnType::Float64 => ColumnValues::Float64(symbols.iter().map(value_as_f64).collect()),
        ColumnType::Utf8 => ColumnValues::Utf8(symbols.iter().map(value_as_str).collect()),
        ColumnType::Date32 => ColumnValues::Date32(
            symbols
                .iter()
                .map(|v| value_as_i64(v) as i32 - QLIK_EPOCH_OFFSET)
                .collect(),
        ),
        ColumnType::TimestampMicros => ColumnValues::TimestampMicros(
            symbols
                .iter()
                .map(|v| ((value_as_f64(v) - QLIK_EPOCH_OFFSET as f64) * 86_400_000_000.0) as i64)
                .collect(),
        ),
        ColumnType::DurationMicros => ColumnValues::DurationMicros(
            symbols
                .iter()
                .map(|v| (value_as_f64(v) * 86_400_000_000.0) as i64)
                .collect(),
        ),
        _ => ColumnValues::Null,
    }
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
        let dtype = infer_type(field, symbols);
        Ok(ColumnData {
            name: field.name.clone(),
            dtype,
            symbols: convert(dtype, symbols),
            indices,
        })
    }
}
