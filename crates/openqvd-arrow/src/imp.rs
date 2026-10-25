use std::sync::Arc;

use arrow_array::{
    builder::LargeStringBuilder, cast::AsArray, types::*, Array, ArrayRef,
    Date32Array, DurationMicrosecondArray, Float64Array, Int64Array, NullArray, RecordBatch,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit};

use openqvd::{
    Column, ColumnData, ColumnValues, Dual, Qvd, QvdError, Value, WriteOptions, WriteTable,
};

use crate::BatchOptions;

/// Qlik date serial epoch: days from 30 Dec 1899 to 1 Jan 1970.
const QLIK_EPOCH_OFFSET: i32 = 25_569;

fn arrow_err(e: impl std::fmt::Display) -> QvdError {
    QvdError::Arrow {
        detail: e.to_string(),
    }
}

/// Convert a `Qvd` to an Arrow [`RecordBatch`].
///
/// Type mapping, for well-formed files:
/// - `NumberFormat/Type = DATE` gives `Date32` (Qlik epoch to Unix epoch).
/// - `TIMESTAMP` gives `Timestamp(Microsecond, None)`.
/// - `TIME` gives `Duration(Microsecond)`.
/// - Int/DualInt symbols give `Int64`; Float/DualFloat give `Float64`.
/// - Any string symbol gives `LargeUtf8`.
/// - An empty symbol table gives `Null`.
///
/// All columns are nullable.
pub fn to_record_batch(qvd: &Qvd, options: &BatchOptions) -> Result<RecordBatch, QvdError> {
    let indices: Vec<usize> = match &options.columns {
        None => (0..qvd.fields().len()).collect(),
        Some(names) => names
            .iter()
            .map(|n| qvd.field_index(n))
            .collect::<Result<_, _>>()?,
    };
    let rows = if options.filters.is_empty() {
        None
    } else {
        Some(qvd.select_rows(&options.filters)?)
    };

    let mut fields: Vec<ArrowField> = Vec::with_capacity(indices.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(indices.len());
    for i in indices {
        let mut col = qvd.column(i)?;
        if let Some(r) = &rows {
            col = col.select(r);
        }
        let array = column_to_array(&col)?;
        fields.push(ArrowField::new(
            col.name.as_str(),
            array.data_type().clone(),
            true,
        ));
        arrays.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(arrow_err)
}

fn column_to_array(col: &ColumnData) -> Result<ArrayRef, QvdError> {
    macro_rules! gather {
        ($array:ident, $vals:expr) => {
            Arc::new($array::from_iter(col.iter().map(|o| o.map(|i| $vals[i])))) as ArrayRef
        };
    }
    Ok(match &col.symbols {
        ColumnValues::Int64(v) => gather!(Int64Array, v),
        ColumnValues::Float64(v) => gather!(Float64Array, v),
        ColumnValues::Date32(v) => gather!(Date32Array, v),
        ColumnValues::TimestampMicros(v) => gather!(TimestampMicrosecondArray, v),
        ColumnValues::DurationMicros(v) => gather!(DurationMicrosecondArray, v),
        ColumnValues::Utf8(v) => {
            let mut b = LargeStringBuilder::with_capacity(col.len(), col.len() * 8);
            for o in col.iter() {
                match o {
                    Some(i) => b.append_value(&v[i]),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ColumnValues::Null => Arc::new(NullArray::new(col.len())),
        _ => {
            return Err(arrow_err(format!(
                "column {:?}: no Arrow mapping for this column kind",
                col.name
            )))
        }
    })
}

/// Convert an Arrow `RecordBatch` to a [`WriteTable`].
///
/// See [`record_batches_to_write_table`] for several batches.
pub fn record_batch_to_write_table(
    batch: &RecordBatch,
    options: &WriteOptions,
) -> Result<WriteTable, QvdError> {
    record_batches_to_write_table(batch.schema(), std::slice::from_ref(batch), options)
}

/// Convert Arrow batches sharing `schema` to one [`WriteTable`]. Every batch
/// is converted; none is dropped.
///
/// Supported Arrow types: Int8/16/32/64, UInt8/16/32/64, Float32/64,
/// Utf8/LargeUtf8, Boolean, Date32 and Timestamp(Microsecond).
pub fn record_batches_to_write_table(
    schema: SchemaRef,
    batches: &[RecordBatch],
    options: &WriteOptions,
) -> Result<WriteTable, QvdError> {
    let mut columns: Vec<Column> = Vec::with_capacity(schema.fields().len());
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let mut cells: Vec<Option<Value>> = Vec::new();
        for batch in batches {
            if batch.schema().fields().len() != schema.fields().len()
                || batch.schema().field(col_idx).data_type() != field.data_type()
            {
                return Err(arrow_err("batches do not share one schema"));
            }
            convert_array(batch.column(col_idx), field.data_type(), &mut cells)?;
        }
        let mut col = Column::new(field.name().to_string(), cells);
        // Reflect Arrow type back as a NumberFormat hint.
        col.number_format.r#type = arrow_type_to_nf(field.data_type()).to_string();
        columns.push(col);
    }
    WriteTable::new(options.table_name.clone(), columns)
}

fn convert_array(
    arr: &ArrayRef,
    data_type: &DataType,
    cells: &mut Vec<Option<Value>>,
) -> Result<(), QvdError> {
    let n = arr.len();
    cells.reserve(n);
    match data_type {
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
                            Some(Value::DualInt(Dual::new(v as i32, v.to_string())))
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
                        Some(Value::DualInt(Dual::new(v as i32, v.to_string())))
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
                let a =
                    arr.as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| QvdError::Arrow {
                            detail: "expected BooleanArray".to_owned(),
                        })?;
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
                        cells.push(Some(Value::DualInt(Dual::new(qlik_days, text))));
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
                        cells.push(Some(Value::DualFloat(Dual::new(qlik_days, text))));
                    }
                }
            }
            _ => {
                // Fallback: use Arrow cast to LargeUtf8.
                use arrow_cast::cast;
                use arrow_schema::DataType as DT;
                let str_arr = cast(arr.as_ref(), &DT::LargeUtf8).map_err(|e| QvdError::Arrow {
                    detail: e.to_string(),
                })?;
                let a = str_arr
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .ok_or_else(|| QvdError::Arrow {
                        detail: "cast to LargeUtf8 produced unexpected type".to_owned(),
                    })?;
                for i in 0..n {
                    cells.push(if a.is_null(i) {
                        None
                    } else {
                        Some(Value::Str(a.value(i).to_string()))
                    });
                }
            }
        }

    Ok(())
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
