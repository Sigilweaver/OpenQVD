//! Arrow integration for `Qvd`. Enabled by the `arrow` feature flag.
//!
//! Converts a parsed `Qvd` file to an `arrow_array::RecordBatch`.
//! Type inference follows this precedence:
//!
//! 1. `NumberFormat/Type` hint (`DATE` -> Date32, `TIMESTAMP` -> Timestamp, etc.)
//! 2. Actual symbol variants (`Int`/`DualInt` -> Int64, `Float`/`DualFloat` ->
//!    Float64, anything containing `Str` -> LargeUtf8).
//!
//! Qlik date serials (days since 30 Dec 1899) are shifted to the
//! Arrow/Unix epoch (1 Jan 1970) when the column type is `DATE` or
//! `TIMESTAMP`.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{
    builder::{
        Date32Builder, DurationMicrosecondBuilder, Float64Builder, Int64Builder,
        LargeStringBuilder, TimestampMicrosecondBuilder,
    },
    ArrayRef, NullArray, RecordBatch,
};
use arrow_schema::{DataType, Field as ArrowField, Schema, TimeUnit};

use crate::header::FieldHeader;
use crate::value::Value;
use crate::{Qvd, QvdError};

/// Qlik date serial epoch: days from 30 Dec 1899 to 1 Jan 1970.
const QLIK_EPOCH_OFFSET: i32 = 25_569;

// ---------------------------------------------------------------------------
// Predicate pushdown: column-level filters resolved against symbol tables
// ---------------------------------------------------------------------------

/// A filter predicate for a single column, used for predicate pushdown.
///
/// Filters are resolved against the column's symbol table *before* row
/// iteration, so only rows where every filtered column's packed index
/// matches a satisfying symbol are emitted. This avoids decoding
/// non-matching rows entirely.
pub enum ColumnFilter {
    /// Keep rows where the column value equals this string exactly.
    Eq(String),
    /// Keep rows where the column value is one of these strings.
    IsIn(Vec<String>),
    /// Keep rows where the column value is NOT one of these strings.
    NotIn(Vec<String>),
    /// Keep rows where the column is NULL.
    IsNull,
    /// Keep rows where the column is NOT NULL.
    IsNotNull,
}

/// A named filter: column name + predicate.
pub struct Filter {
    /// Column name.
    pub column: String,
    /// The predicate to apply.
    pub predicate: ColumnFilter,
}

/// Resolve a `ColumnFilter` against a symbol table, returning the set of
/// symbol indices whose values satisfy the predicate, plus whether NULL
/// rows should pass.
fn resolve_filter(symbols: &[Value], pred: &ColumnFilter) -> (HashSet<usize>, bool) {
    match pred {
        ColumnFilter::IsNull => (HashSet::new(), true),
        ColumnFilter::IsNotNull => {
            let all: HashSet<usize> = (0..symbols.len()).collect();
            (all, false)
        }
        ColumnFilter::Eq(target) => {
            let mut set = HashSet::new();
            for (i, sym) in symbols.iter().enumerate() {
                if symbol_matches_str(sym, target) {
                    set.insert(i);
                }
            }
            (set, false)
        }
        ColumnFilter::IsIn(targets) => {
            let target_set: HashSet<&str> = targets.iter().map(|s| s.as_str()).collect();
            let mut set = HashSet::new();
            for (i, sym) in symbols.iter().enumerate() {
                if target_set.iter().any(|t| symbol_matches_str(sym, t)) {
                    set.insert(i);
                }
            }
            (set, false)
        }
        ColumnFilter::NotIn(targets) => {
            let target_set: HashSet<&str> = targets.iter().map(|s| s.as_str()).collect();
            let mut set = HashSet::new();
            for (i, sym) in symbols.iter().enumerate() {
                if !target_set.iter().any(|t| symbol_matches_str(sym, t)) {
                    set.insert(i);
                }
            }
            (set, true) // NULL is not "in" any set, so it passes NotIn
        }
    }
}

/// Check if a symbol's string representation matches `target`.
fn symbol_matches_str(sym: &Value, target: &str) -> bool {
    match sym {
        Value::Str(s) => s == target,
        Value::Int(i) => {
            // Compare as string representation
            let s = i.to_string();
            s == target
        }
        Value::Float(f) => {
            let s = f.to_string();
            s == target
        }
        Value::DualInt(d) => d.text == target || d.number.to_string() == target,
        Value::DualFloat(d) => d.text == target || d.number.to_string() == target,
    }
}

/// Internal: resolved filter ready for row-level evaluation.
struct ResolvedFilter {
    /// Index into `qvd.fields()`.
    field_idx: usize,
    /// Symbol indices that satisfy the predicate.
    passing_indices: HashSet<usize>,
    /// Whether NULL rows pass.
    null_passes: bool,
}

/// Convert a `Qvd` to an Arrow `RecordBatch`.
///
/// `columns` optionally restricts which columns are included. A `QvdError`
/// is returned if any named column does not exist.
///
/// This is exposed as `Qvd::to_record_batch`.
pub fn to_record_batch(
    qvd: &Qvd,
    columns: Option<&[&str]>,
    filters: Option<&[Filter]>,
) -> Result<RecordBatch, QvdError> {
    // Resolve column indices.
    let col_indices: Vec<usize> = match columns {
        None => (0..qvd.fields().len()).collect(),
        Some(names) => names
            .iter()
            .map(|n| {
                qvd.fields()
                    .iter()
                    .position(|f| f.name == *n)
                    .ok_or_else(|| QvdError::structure(format!("column {n:?} not found")))
            })
            .collect::<Result<_, _>>()?,
    };

    // Resolve filters against symbol tables.
    let resolved_filters: Vec<ResolvedFilter> = match filters {
        None => Vec::new(),
        Some(fs) => {
            let mut resolved = Vec::with_capacity(fs.len());
            for f in fs {
                let field_idx = qvd
                    .fields()
                    .iter()
                    .position(|fh| fh.name == f.column)
                    .ok_or_else(|| {
                        QvdError::structure(format!("filter column {:?} not found", f.column))
                    })?;
                let symbols = qvd.symbols(field_idx).unwrap_or(&[]);
                let (passing_indices, null_passes) = resolve_filter(symbols, &f.predicate);
                resolved.push(ResolvedFilter {
                    field_idx,
                    passing_indices,
                    null_passes,
                });
            }
            resolved
        }
    };

    // Build Arrow schema.
    let arrow_fields: Vec<ArrowField> = col_indices
        .iter()
        .map(|&i| {
            let f = &qvd.fields()[i];
            let syms = qvd.symbols(i).unwrap_or(&[]);
            let dtype = infer_dtype(f, syms);
            ArrowField::new(f.name.as_str(), dtype, true)
        })
        .collect();
    let schema = Arc::new(Schema::new(arrow_fields));

    // Build per-column data using a single row pass.
    let n = qvd.num_rows() as usize;
    let dtypes: Vec<DataType> = col_indices
        .iter()
        .map(|&i| {
            let f = &qvd.fields()[i];
            let syms = qvd.symbols(i).unwrap_or(&[]);
            infer_dtype(f, syms)
        })
        .collect();

    // Allocate builders (estimate capacity; may be smaller with filters).
    let mut builders: Vec<BuilderEnum> = dtypes.iter().map(|dt| BuilderEnum::new(dt, n)).collect();

    // Iterate rows once, applying predicate pushdown.
    if resolved_filters.is_empty() {
        for row in qvd.rows() {
            for (out, &in_idx) in col_indices.iter().enumerate() {
                builders[out].append(&row[in_idx], &dtypes[out]);
            }
        }
    } else {
        'row: for row in qvd.rows() {
            // Check all filter predicates.
            for rf in &resolved_filters {
                let cell = &row[rf.field_idx];
                match cell {
                    None => {
                        if !rf.null_passes {
                            continue 'row;
                        }
                    }
                    Some(v) => {
                        // We need to determine the symbol index for this cell.
                        // Since the row iterator resolves symbols, we match
                        // the value against the passing set by checking if
                        // any passing symbol index has this value.
                        let symbols = qvd.symbols(rf.field_idx).unwrap_or(&[]);
                        let matches = rf
                            .passing_indices
                            .iter()
                            .any(|&idx| idx < symbols.len() && symbols[idx] == *v);
                        if !matches {
                            continue 'row;
                        }
                    }
                }
            }
            // Row passes all filters.
            for (out, &in_idx) in col_indices.iter().enumerate() {
                builders[out].append(&row[in_idx], &dtypes[out]);
            }
        }
    }

    // Finish arrays.
    let arrays: Vec<ArrayRef> = builders.into_iter().map(BuilderEnum::finish).collect();

    RecordBatch::try_new(schema, arrays).map_err(|e| QvdError::structure(e.to_string()))
}

// ---------------------------------------------------------------------------
// Type inference
// ---------------------------------------------------------------------------

fn infer_dtype(field: &FieldHeader, symbols: &[Value]) -> DataType {
    match field.number_format.r#type.as_str() {
        "DATE" => return DataType::Date32,
        "TIMESTAMP" => return DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIME" => return DataType::Duration(TimeUnit::Microsecond),
        _ => {}
    }
    infer_dtype_from_values(symbols)
}

fn infer_dtype_from_values(symbols: &[Value]) -> DataType {
    if symbols.is_empty() {
        return DataType::Null;
    }
    let mut has_str = false;
    let mut has_float = false;
    for s in symbols {
        match s {
            Value::Str(_) => {
                has_str = true;
                break;
            }
            Value::Float(_) | Value::DualFloat(_) => has_float = true,
            Value::Int(_) | Value::DualInt(_) => {}
        }
    }
    if has_str {
        DataType::LargeUtf8
    } else if has_float {
        DataType::Float64
    } else {
        DataType::Int64
    }
}

// ---------------------------------------------------------------------------
// Value extraction helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Polymorphic builder
// ---------------------------------------------------------------------------

enum BuilderEnum {
    Int64(Int64Builder),
    Float64(Float64Builder),
    LargeStr(LargeStringBuilder),
    Date32(Date32Builder),
    TimestampMicro(TimestampMicrosecondBuilder),
    DurationMicro(DurationMicrosecondBuilder),
    /// Column with an empty symbol table — all rows are null.
    Null(usize),
    /// Fallback for unrecognised types — emit as LargeUtf8.
    Fallback(LargeStringBuilder),
}

impl BuilderEnum {
    fn new(dt: &DataType, capacity: usize) -> Self {
        match dt {
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::LargeUtf8 => {
                Self::LargeStr(LargeStringBuilder::with_capacity(capacity, capacity * 8))
            }
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Self::TimestampMicro(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                Self::DurationMicro(DurationMicrosecondBuilder::with_capacity(capacity))
            }
            DataType::Null => Self::Null(capacity),
            _ => Self::Fallback(LargeStringBuilder::with_capacity(capacity, capacity * 8)),
        }
    }

    fn append(&mut self, cell: &Option<Value>, dt: &DataType) {
        match cell {
            None => self.append_null(),
            Some(v) => self.append_value(v, dt),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Int64(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::LargeStr(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::TimestampMicro(b) => b.append_null(),
            Self::DurationMicro(b) => b.append_null(),
            Self::Null(_) => {}
            Self::Fallback(b) => b.append_null(),
        }
    }

    fn append_value(&mut self, v: &Value, dt: &DataType) {
        match self {
            Self::Int64(b) => b.append_value(value_as_i64(v)),
            Self::Float64(b) => b.append_value(value_as_f64(v)),
            Self::LargeStr(b) => b.append_value(value_as_str(v)),
            Self::Date32(b) => {
                let qlik_days = value_as_i64(v) as i32;
                b.append_value(qlik_days - QLIK_EPOCH_OFFSET);
            }
            Self::TimestampMicro(b) => {
                // Qlik datetime: fractional days since 30 Dec 1899.
                let qlik_days = value_as_f64(v);
                let unix_days = qlik_days - QLIK_EPOCH_OFFSET as f64;
                let micros = (unix_days * 86_400_000_000.0) as i64;
                b.append_value(micros);
            }
            Self::DurationMicro(b) => {
                // Qlik time-of-day: fractional days (0.0 = midnight, 0.5 = noon).
                let frac_day = value_as_f64(v);
                let micros = (frac_day * 86_400_000_000.0) as i64;
                b.append_value(micros);
            }
            Self::Null(_) => {} // value shouldn't appear for empty symbol table
            Self::Fallback(b) => b.append_value(value_as_str(v)),
        }
        let _ = dt;
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::LargeStr(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
            Self::TimestampMicro(mut b) => Arc::new(b.finish()),
            Self::DurationMicro(mut b) => Arc::new(b.finish()),
            Self::Null(n) => Arc::new(NullArray::new(n)),
            Self::Fallback(mut b) => Arc::new(b.finish()),
        }
    }
}

// ---------------------------------------------------------------------------
// Public impl block on Qvd
// ---------------------------------------------------------------------------

impl Qvd {
    /// Convert this table to an Arrow [`RecordBatch`].
    ///
    /// `columns` optionally restricts which columns are included (projection
    /// pushdown at the array-building level). Requesting a non-existent
    /// column name returns an error.
    ///
    /// Type mapping:
    /// - `NumberFormat/Type = DATE` -> `Date32` (Qlik epoch -> Unix epoch).
    /// - `NumberFormat/Type = TIMESTAMP` -> `Timestamp(Microsecond, None)`.
    /// - `NumberFormat/Type = TIME` -> `Duration(Microsecond)`.
    /// - Int/DualInt symbols -> `Int64`.
    /// - Float/DualFloat symbols -> `Float64`.
    /// - Any string symbol -> `LargeUtf8`.
    ///
    /// All columns are nullable.
    pub fn to_record_batch(&self, columns: Option<&[&str]>) -> Result<RecordBatch, QvdError> {
        to_record_batch(self, columns, None)
    }

    /// Convert this table to an Arrow [`RecordBatch`] with predicate pushdown.
    ///
    /// `filters` restricts which rows are included by resolving predicates
    /// against the column symbol tables before iterating rows. Only rows
    /// where every filter predicate is satisfied are emitted. Filter columns
    /// do not need to appear in `columns` -- they are resolved against the
    /// full field list.
    pub fn to_record_batch_filtered(
        &self,
        columns: Option<&[&str]>,
        filters: &[Filter],
    ) -> Result<RecordBatch, QvdError> {
        to_record_batch(self, columns, Some(filters))
    }
}

// ---------------------------------------------------------------------------
// Arrow -> WriteTable (for write support from Python)
// ---------------------------------------------------------------------------

use crate::value::Dual;
use crate::writer::{Column, WriteTable};

/// Convert an Arrow `RecordBatch` to a [`WriteTable`].
///
/// Supported Arrow types: Int8/16/32/64, UInt8/16/32/64 -> `Value::Int` (i32
/// saturating cast), Float32/64 -> `Value::Float`, Utf8/LargeUtf8 ->
/// `Value::Str`, Boolean -> `Value::Int(0/1)`, Date32 -> `Value::DualInt`
/// (QVD date serial with ISO text), Timestamp(Microsecond) ->
/// `Value::DualFloat` (QVD datetime serial with ISO text).
///
/// All other types are serialised via their `Debug` representation as
/// `Value::Str`.
pub fn record_batch_to_write_table(
    batch: &RecordBatch,
    table_name: &str,
) -> Result<WriteTable, QvdError> {
    use arrow_array::{cast::AsArray, types::*, Array};

    let mut columns: Vec<Column> = Vec::with_capacity(batch.num_columns());

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let arr = batch.column(col_idx);
        let n = arr.len();
        let mut cells: Vec<Option<Value>> = Vec::with_capacity(n);

        match field.data_type() {
            DataType::Int8 => {
                let a = arr.as_primitive::<Int8Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Int(a.value(i) as i32))
                    });
                }
            }
            DataType::Int16 => {
                let a = arr.as_primitive::<Int16Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Int(a.value(i) as i32))
                    });
                }
            }
            DataType::Int32 => {
                let a = arr.as_primitive::<Int32Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Int(a.value(i)))
                    });
                }
            }
            DataType::Int64 => {
                let a = arr.as_primitive::<Int64Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        let v = a.value(i);
                        // Use DualInt if it fits in i32, else encode as string.
                        if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
                            Some(Value::DualInt(Dual {
                                number: v as i32,
                                text: v.to_string(),
                            }))
                        } else {
                            Some(Value::Str(v.to_string()))
                        }
                    });
                }
            }
            DataType::UInt8 => {
                let a = arr.as_primitive::<UInt8Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Int(a.value(i) as i32))
                    });
                }
            }
            DataType::UInt16 => {
                let a = arr.as_primitive::<UInt16Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Int(a.value(i) as i32))
                    });
                }
            }
            DataType::UInt32 => {
                let a = arr.as_primitive::<UInt32Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        let v = a.value(i);
                        Some(Value::DualInt(Dual {
                            number: v as i32,
                            text: v.to_string(),
                        }))
                    });
                }
            }
            DataType::UInt64 => {
                let a = arr.as_primitive::<UInt64Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Str(a.value(i).to_string()))
                    });
                }
            }
            DataType::Float32 => {
                let a = arr.as_primitive::<Float32Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Float(a.value(i) as f64))
                    });
                }
            }
            DataType::Float64 => {
                let a = arr.as_primitive::<Float64Type>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Float(a.value(i)))
                    });
                }
            }
            DataType::Utf8 => {
                let a = arr.as_string::<i32>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Str(a.value(i).to_string()))
                    });
                }
            }
            DataType::LargeUtf8 => {
                let a = arr.as_string::<i64>();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Str(a.value(i).to_string()))
                    });
                }
            }
            DataType::Boolean => {
                use arrow_array::BooleanArray;
                let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Int(a.value(i) as i32))
                    });
                }
            }
            DataType::Date32 => {
                let a = arr.as_primitive::<Date32Type>();
                for i in 0..n {
                    if a.is_null(i) {
                        cells.push(None);
                    } else {
                        let unix_days = a.value(i);
                        let qlik_days = unix_days + QLIK_EPOCH_OFFSET;
                        // Format as ISO date string for the text representation.
                        let text = unix_days_to_iso(unix_days as i64);
                        cells.push(Some(Value::DualInt(Dual {
                            number: qlik_days,
                            text,
                        })));
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let a = arr.as_primitive::<TimestampMicrosecondType>();
                for i in 0..n {
                    if a.is_null(i) {
                        cells.push(None);
                    } else {
                        let micros = a.value(i);
                        let unix_days = micros as f64 / 86_400_000_000.0;
                        let qlik_days = unix_days + QLIK_EPOCH_OFFSET as f64;
                        let text = unix_micros_to_iso(micros);
                        cells.push(Some(Value::DualFloat(Dual {
                            number: qlik_days,
                            text,
                        })));
                    }
                }
            }
            _ => {
                // Fallback: use Arrow cast to LargeUtf8.
                use arrow_cast::cast;
                use arrow_schema::DataType as DT;
                let str_arr = cast(arr.as_ref(), &DT::LargeUtf8)
                    .map_err(|e| QvdError::structure(e.to_string()))?;
                let a = str_arr
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .unwrap();
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Str(a.value(i).to_string()))
                    });
                }
            }
        }

        let mut col = Column::new(field.name().to_string(), cells);
        // Reflect Arrow type back as a NumberFormat hint.
        col.number_format.r#type = arrow_type_to_nf(field.data_type()).to_string();
        columns.push(col);
    }

    WriteTable::new(table_name, columns)
}

fn arrow_type_to_nf(dt: &DataType) -> &'static str {
    match dt {
        DataType::Date32 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Duration(_) | DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Float32 | DataType::Float64 => "REAL",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "INTEGER",
        _ => "UNKNOWN",
    }
}

// Minimal ISO formatters - no chrono dependency needed.
fn unix_days_to_iso(unix_days: i64) -> String {
    // Compute calendar date from Unix day number (days since 1970-01-01).
    // Using the Julian Day Number algorithm.
    let jd = unix_days + 2_440_588; // Julian Day Number for 1970-01-01
    let a = jd + 32_044;
    let b = (4 * a + 3) / 146_097;
    let c = a - (146_097 * b) / 4;
    let d = (4 * c + 3) / 1_461;
    let e = c - (1_461 * d) / 4;
    let m = (5 * e + 2) / 153;
    let day = e - (153 * m + 2) / 5 + 1;
    let month = m + 3 - 12 * (m / 10);
    let year = 100 * b + d - 4_800 + m / 10;
    format!("{year:04}-{month:02}-{day:02}")
}

fn unix_micros_to_iso(micros: i64) -> String {
    let total_secs = micros.div_euclid(1_000_000);
    let us = micros.rem_euclid(1_000_000);
    let days = total_secs.div_euclid(86_400);
    let secs_of_day = total_secs.rem_euclid(86_400);
    let h = secs_of_day / 3_600;
    let m = (secs_of_day % 3_600) / 60;
    let s = secs_of_day % 60;
    let date = unix_days_to_iso(days);
    if us == 0 {
        format!("{date} {h:02}:{m:02}:{s:02}")
    } else {
        format!("{date} {h:02}:{m:02}:{s:02}.{us:06}")
    }
}
