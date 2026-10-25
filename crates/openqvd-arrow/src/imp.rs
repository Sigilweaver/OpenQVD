use std::sync::Arc;

use arrow_array::{
    builder::LargeStringBuilder, cast::AsArray, types::*, Array, ArrayRef, ArrowPrimitiveType,
    Date32Array, DurationMicrosecondArray, Float64Array, Int64Array, NullArray, RecordBatch,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit};

use openqvd::{
    Column, ColumnData, ColumnValues, Dual, Leniency, LenientCheck, Qvd, QvdError, Value, Warning,
    WarningKind, WriteOptions, WriteTable,
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
        // The QVD format hint is kept next to the resolved type: a DATE
        // column may arrive as Date32 or Timestamp depending on its data.
        let metadata = std::collections::HashMap::from([(
            "qvd.number_format_type".to_owned(),
            qvd.fields()[i].number_format.r#type.clone(),
        )]);
        fields.push(
            ArrowField::new(col.name.as_str(), array.data_type().clone(), true)
                .with_metadata(metadata),
        );
        arrays.push(array);
    }
    // A batch with no columns still has rows (`columns=[]` is a legal request).
    let row_count = match &rows {
        Some(r) => r.len(),
        None => qvd.num_rows() as usize,
    };
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(arrow_err)
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
/// See [`record_batches_to_write_table`] for the type mapping.
pub fn record_batch_to_write_table(
    batch: &RecordBatch,
    options: &WriteOptions,
) -> Result<WriteTable, QvdError> {
    record_batches_to_write_table(batch.schema(), std::slice::from_ref(batch), options)
}

/// Convert Arrow batches sharing `schema` to one [`WriteTable`]. Every batch
/// is converted; none is dropped.
///
/// The accepted Arrow types and what they become are fixed; anything else is
/// `QvdError::UnsupportedArrowType`, never a text rendering:
///
/// | Arrow type | QVD value |
/// |---|---|
/// | Int8/16/32, UInt8/16 | integer |
/// | Int64, UInt32, UInt64 | integer when it fits 32 bits, else a dual (float number, exact decimal text); an error beyond 2^53 |
/// | Float32/64 | float |
/// | Decimal128/256 | dual (float number, exact decimal text); an error unless the unscaled value is below 2^53 and the scale at most 22 |
/// | Utf8, LargeUtf8, Utf8View | string |
/// | Boolean | integer 0/1 |
/// | Date32, Date64 (whole days) | dual (day serial, ISO date) |
/// | Date32, Date64 | only years 0001 to 9999 |
/// | Timestamp(any unit) | dual (day serial, ISO date-time), years 0001 to 9999; a time zone is an error unless `timezone_to_utc` is allowed; nanosecond digits are an error unless `sub_microsecond_precision` is allowed |
/// | Time32, Time64 | dual (fraction of a day, `h:mm:ss`), format `TIME` |
/// | Duration | dual (fraction of a day, `h:mm:ss`, hours may exceed 24), format `INTERVAL` |
/// | Dictionary | its values, converted by the row above |
/// | Null | NULL |
///
/// Day serials are `f64`, so timestamps resolve to about a microsecond near
/// 1970 and to tens of microseconds near year 9999.
///
/// Reading the result back does not always give the Arrow type that went in
/// (the values are exact): an Int64 column with any value beyond 32 bits is
/// read as Float64, Decimal as Float64 (the exact text is stored but the
/// reader returns the number), Float32 as the widened f64, and a dictionary
/// column carries no format hint, so a dictionary of timestamps reads back
/// as floats.
///
/// Anything tolerated because of `options.leniency` is listed in
/// [`WriteTable::warnings`].
pub fn record_batches_to_write_table(
    schema: SchemaRef,
    batches: &[RecordBatch],
    options: &WriteOptions,
) -> Result<WriteTable, QvdError> {
    let mut columns: Vec<Column> = Vec::with_capacity(schema.fields().len());
    let mut warnings: Vec<Warning> = Vec::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let mut cells: Vec<Option<Value>> = Vec::new();
        for batch in batches {
            if batch.schema().fields().len() != schema.fields().len()
                || batch.schema().field(col_idx).data_type() != field.data_type()
            {
                return Err(arrow_err("batches do not share one schema"));
            }
            let mut ctx = Ctx {
                column: field.name(),
                leniency: &options.leniency,
                warnings: &mut warnings,
                rounded: std::cell::Cell::new(false),
            };
            convert_array(&mut ctx, batch.column(col_idx), &mut cells)?;
        }
        let mut col = Column::new(field.name().to_string(), cells);
        // Reflect Arrow type back as a NumberFormat hint.
        col.number_format.r#type = arrow_type_to_nf(field.data_type()).to_string();
        columns.push(col);
    }
    Ok(WriteTable::new(options.table_name.clone(), columns)?.with_warnings(warnings))
}

/// What a conversion needs to report errors and lenient decisions.
struct Ctx<'a> {
    column: &'a str,
    leniency: &'a Leniency,
    warnings: &'a mut Vec<Warning>,
    /// Set when a nanosecond value was rounded (lenient only).
    rounded: std::cell::Cell<bool>,
}

impl Ctx<'_> {
    fn not_representable(
        &self,
        value: impl std::fmt::Display,
        detail: &'static str,
    ) -> QvdError {
        QvdError::ValueNotRepresentable {
            column: self.column.to_owned(),
            value: value.to_string(),
            detail,
        }
    }
}

/// Largest integer magnitude an f64 holds exactly.
const MAX_EXACT: i64 = 1 << 53;
const MICROS_PER_DAY: i128 = 86_400_000_000;

/// Push one cell per element, mapping non-null natives with `f`.
fn push_prim<T: ArrowPrimitiveType>(
    arr: &ArrayRef,
    cells: &mut Vec<Option<Value>>,
    mut f: impl FnMut(T::Native) -> Result<Value, QvdError>,
) -> Result<(), QvdError> {
    let a = arr.as_primitive::<T>();
    for i in 0..a.len() {
        cells.push(if a.is_null(i) {
            None
        } else {
            Some(f(a.value(i))?)
        });
    }
    Ok(())
}

/// An integer as a QVD value: plain when it fits 32 bits, otherwise a dual of
/// the exact decimal text and its (exact) float number.
fn integer_value(ctx: &Ctx<'_>, v: i128) -> Result<Value, QvdError> {
    if let Ok(i) = i32::try_from(v) {
        return Ok(Value::Int(i));
    }
    if v.unsigned_abs() > MAX_EXACT as u128 {
        return Err(ctx.not_representable(v, "magnitude above 2^53 is not exact as a float"));
    }
    Ok(Value::DualFloat(Dual::new(v as f64, v.to_string())))
}

fn float_serial(micros_since_unix: i128) -> f64 {
    // One rounding: the exact integer count of microseconds since the Qlik
    // epoch, divided once. (Dividing first and adding the epoch offset
    // afterwards rounds twice and can drift by half a microsecond.)
    let since_qlik = micros_since_unix + i128::from(QLIK_EPOCH_OFFSET) * MICROS_PER_DAY;
    since_qlik as f64 / MICROS_PER_DAY as f64
}

/// First and last days (since 1970-01-01) the writer accepts: 0001-01-01 to
/// 9999-12-31. Outside that the ISO text is not a date, and far from 1970 an
/// f64 day serial no longer resolves a microsecond.
const MIN_UNIX_DAY: i64 = -719_162;
const MAX_UNIX_DAY: i64 = 2_932_896;

/// A timestamp as a (serial, ISO text) dual. The serial is an f64, so far
/// from 1970 it resolves only to tens of microseconds; the value must still
/// be one the reader accepts, so it is checked by reading it back.
fn timestamp_value(ctx: &Ctx<'_>, micros: i64) -> Result<Value, QvdError> {
    let day = i128::from(micros).div_euclid(MICROS_PER_DAY);
    if day < i128::from(MIN_UNIX_DAY) || day > i128::from(MAX_UNIX_DAY) {
        return Err(ctx.not_representable(micros, "timestamp outside years 0001 to 9999"));
    }
    let serial = float_serial(i128::from(micros));
    if openqvd::serial::serial_to_unix_micros(serial).is_none() {
        return Err(ctx.not_representable(micros, "timestamp outside the readable range"));
    }
    Ok(Value::DualFloat(Dual::new(serial, unix_micros_to_iso(micros))))
}

fn time_value(ctx: &Ctx<'_>, micros: i128) -> Result<Value, QvdError> {
    let micros = i64::try_from(micros)
        .map_err(|_| ctx.not_representable(micros, "time value out of range"))?;
    let days = micros as f64 / MICROS_PER_DAY as f64;
    if openqvd::serial::duration_days_to_micros(days).is_none() {
        return Err(ctx.not_representable(micros, "time value outside the readable range"));
    }
    Ok(Value::DualFloat(Dual::new(days, duration_text(micros))))
}

fn date_value(ctx: &Ctx<'_>, unix_days: i32) -> Result<Value, QvdError> {
    if !(MIN_UNIX_DAY..=MAX_UNIX_DAY).contains(&i64::from(unix_days)) {
        return Err(ctx.not_representable(unix_days, "date outside years 0001 to 9999"));
    }
    let serial = unix_days
        .checked_add(QLIK_EPOCH_OFFSET)
        .ok_or_else(|| ctx.not_representable(unix_days, "date out of range"))?;
    Ok(Value::DualInt(Dual::new(
        serial,
        unix_days_to_iso(i64::from(unix_days)),
    )))
}

/// Nanoseconds to microseconds. A remainder is a value the format cannot
/// hold: an error, or with the `sub_microsecond_precision` leniency a
/// rounding that is reported once per column.
fn nanos_to_micros(ctx: &Ctx<'_>, ns: i64) -> Result<i64, QvdError> {
    if ns.rem_euclid(1000) == 0 {
        return Ok(ns.div_euclid(1000));
    }
    if !ctx.leniency.allows(LenientCheck::SubMicrosecondPrecision) {
        return Err(ctx.not_representable(
            ns,
            "nanosecond digits below the microsecond; allow the sub_microsecond_precision \
             lenient check to round",
        ));
    }
    ctx.rounded.set(true);
    Ok(round_nanos_to_micros(ns))
}

fn to_micros(ctx: &Ctx<'_>, v: i64, per_unit: i64) -> Result<i64, QvdError> {
    v.checked_mul(per_unit)
        .ok_or_else(|| ctx.not_representable(v, "timestamp out of range"))
}

fn convert_array(
    ctx: &mut Ctx<'_>,
    arr: &ArrayRef,
    cells: &mut Vec<Option<Value>>,
) -> Result<(), QvdError> {
    let n = arr.len();
    cells.reserve(n);
    match arr.data_type() {
        DataType::Null => cells.resize(cells.len() + n, None),
        DataType::Int8 => push_prim::<Int8Type>(arr, cells, |v| Ok(Value::Int(i32::from(v))))?,
        DataType::Int16 => push_prim::<Int16Type>(arr, cells, |v| Ok(Value::Int(i32::from(v))))?,
        DataType::Int32 => push_prim::<Int32Type>(arr, cells, |v| Ok(Value::Int(v)))?,
        DataType::Int64 => push_prim::<Int64Type>(arr, cells, |v| integer_value(ctx, i128::from(v)))?,
        DataType::UInt8 => push_prim::<UInt8Type>(arr, cells, |v| Ok(Value::Int(i32::from(v))))?,
        DataType::UInt16 => {
            push_prim::<UInt16Type>(arr, cells, |v| Ok(Value::Int(i32::from(v))))?
        }
        DataType::UInt32 => {
            push_prim::<UInt32Type>(arr, cells, |v| integer_value(ctx, i128::from(v)))?
        }
        DataType::UInt64 => {
            push_prim::<UInt64Type>(arr, cells, |v| integer_value(ctx, i128::from(v)))?
        }
        DataType::Float32 => {
            push_prim::<Float32Type>(arr, cells, |v| Ok(Value::Float(f64::from(v))))?
        }
        DataType::Float64 => push_prim::<Float64Type>(arr, cells, |v| Ok(Value::Float(v)))?,
        DataType::Decimal128(_, scale) => {
            let scale = *scale;
            push_prim::<Decimal128Type>(arr, cells, |v| decimal_value(ctx, v, scale))?
        }
        DataType::Decimal256(_, scale) => {
            let scale = *scale;
            push_prim::<Decimal256Type>(arr, cells, |v| {
                let narrow = v
                    .to_i128()
                    .ok_or_else(|| ctx.not_representable(v, "decimal too wide"))?;
                decimal_value(ctx, narrow, scale)
            })?
        }
        DataType::Utf8 => {
            let a = arr.as_string::<i32>();
            cells.extend(a.iter().map(|o| o.map(|s| Value::Str(s.to_owned()))));
        }
        DataType::LargeUtf8 => {
            let a = arr.as_string::<i64>();
            cells.extend(a.iter().map(|o| o.map(|s| Value::Str(s.to_owned()))));
        }
        DataType::Utf8View => {
            let a = arr.as_string_view();
            cells.extend(a.iter().map(|o| o.map(|s| Value::Str(s.to_owned()))));
        }
        DataType::Boolean => {
            let a = arr.as_boolean();
            cells.extend(a.iter().map(|o| o.map(|b| Value::Int(i32::from(b)))));
        }
        DataType::Date32 => {
            let ctx = &*ctx;
            push_prim::<Date32Type>(arr, cells, |d| date_value(ctx, d))?
        }
        DataType::Date64 => {
            let ctx = &*ctx;
            push_prim::<Date64Type>(arr, cells, |ms| {
            if ms.rem_euclid(86_400_000) != 0 {
                return Err(ctx.not_representable(
                    ms,
                    "Date64 value is not a whole number of days; use a timestamp",
                ));
            }
            let d = i32::try_from(ms.div_euclid(86_400_000))
                .map_err(|_| ctx.not_representable(ms, "date out of range"))?;
            date_value(ctx, d)
        })?
        }
        DataType::Timestamp(unit, tz) => {
            if let Some(tz) = tz {
                if !ctx.leniency.allows(LenientCheck::TimezoneToUtc) {
                    return Err(QvdError::TimezoneNotSupported {
                        column: ctx.column.to_owned(),
                        timezone: tz.to_string(),
                    });
                }
                ctx.warnings.push(
                    Warning::new(
                        WarningKind::TimezoneToUtc,
                        format!("time zone {tz} dropped; values are stored as UTC wall-clock time"),
                    )
                    .in_column(ctx.column),
                );
            }
            let ctx = &*ctx;
            match unit {
                TimeUnit::Second => push_prim::<TimestampSecondType>(arr, cells, |v| {
                    timestamp_value(ctx, to_micros(ctx, v, 1_000_000)?)
                })?,
                TimeUnit::Millisecond => push_prim::<TimestampMillisecondType>(arr, cells, |v| {
                    timestamp_value(ctx, to_micros(ctx, v, 1_000)?)
                })?,
                TimeUnit::Microsecond => push_prim::<TimestampMicrosecondType>(arr, cells, |v| {
                    timestamp_value(ctx, v)
                })?,
                TimeUnit::Nanosecond => push_prim::<TimestampNanosecondType>(arr, cells, |v| {
                    timestamp_value(ctx, nanos_to_micros(ctx, v)?)
                })?,
            }
        }
        DataType::Time32(unit) => {
            let ctx = &*ctx;
            match unit {
                TimeUnit::Second => push_prim::<Time32SecondType>(arr, cells, |v| {
                    time_value(ctx, i128::from(v) * 1_000_000)
                })?,
                _ => push_prim::<Time32MillisecondType>(arr, cells, |v| {
                    time_value(ctx, i128::from(v) * 1_000)
                })?,
            }
        }
        DataType::Time64(unit) => {
            let ctx = &*ctx;
            match unit {
                TimeUnit::Microsecond => {
                    push_prim::<Time64MicrosecondType>(arr, cells, |v| time_value(ctx, i128::from(v)))?
                }
                _ => push_prim::<Time64NanosecondType>(arr, cells, |v| {
                    time_value(ctx, i128::from(nanos_to_micros(ctx, v)?))
                })?,
            }
        }
        DataType::Duration(unit) => {
            let ctx = &*ctx;
            match unit {
                TimeUnit::Second => push_prim::<DurationSecondType>(arr, cells, |v| {
                    time_value(ctx, i128::from(v) * 1_000_000)
                })?,
                TimeUnit::Millisecond => push_prim::<DurationMillisecondType>(arr, cells, |v| {
                    time_value(ctx, i128::from(v) * 1_000)
                })?,
                TimeUnit::Microsecond => push_prim::<DurationMicrosecondType>(arr, cells, |v| {
                    time_value(ctx, i128::from(v))
                })?,
                TimeUnit::Nanosecond => push_prim::<DurationNanosecondType>(arr, cells, |v| {
                    time_value(ctx, i128::from(nanos_to_micros(ctx, v)?))
                })?,
            }
        }
        DataType::Dictionary(_, value_type) => {
            // Unpack the dictionary into its values and convert those.
            let values = arrow_cast::cast(arr.as_ref(), value_type).map_err(arrow_err)?;
            return convert_array_after_cast(ctx, &values, cells);
        }
        other => {
            return Err(QvdError::UnsupportedArrowType {
                column: ctx.column.to_owned(),
                data_type: other.to_string(),
            })
        }
    }
    if ctx.rounded.get()
        && !ctx
            .warnings
            .iter()
            .any(|w| w.kind == WarningKind::SubMicrosecondPrecision && w.column.as_deref() == Some(ctx.column))
    {
        ctx.warnings.push(
            Warning::new(
                WarningKind::SubMicrosecondPrecision,
                "nanosecond values were rounded to the nearest microsecond",
            )
            .in_column(ctx.column),
        );
    }
    Ok(())
}

/// A dictionary's values may themselves be a dictionary only in exotic data;
/// unpacking recurses through `convert_array`, which handles that.
fn convert_array_after_cast(
    ctx: &mut Ctx<'_>,
    values: &ArrayRef,
    cells: &mut Vec<Option<Value>>,
) -> Result<(), QvdError> {
    convert_array(ctx, values, cells)
}

fn round_nanos_to_micros(ns: i64) -> i64 {
    let q = ns.div_euclid(1000);
    let r = ns.rem_euclid(1000);
    if r >= 500 {
        q + 1
    } else {
        q
    }
}

fn decimal_value(ctx: &Ctx<'_>, v: i128, scale: i8) -> Result<Value, QvdError> {
    // Move to a non-negative scale first.
    let (v, scale) = if scale < 0 {
        let factor = 10i128
            .checked_pow(u32::from(scale.unsigned_abs()))
            .and_then(|f| v.checked_mul(f))
            .ok_or_else(|| ctx.not_representable(v, "decimal too large"))?;
        (factor, 0u32)
    } else {
        (v, scale as u32)
    };
    if v.unsigned_abs() > MAX_EXACT as u128 {
        return Err(ctx.not_representable(
            decimal_text(v, scale),
            "more than 15 significant digits: not exact as a float",
        ));
    }
    if scale > 22 {
        return Err(ctx.not_representable(decimal_text(v, scale), "scale above 22"));
    }
    // Both operands are exact in f64, so the division is the nearest f64 to
    // the true decimal value.
    let number = v as f64 / 10f64.powi(scale as i32);
    Ok(Value::DualFloat(Dual::new(number, decimal_text(v, scale))))
}

fn decimal_text(v: i128, scale: u32) -> String {
    let sign = if v < 0 { "-" } else { "" };
    let digits = v.unsigned_abs().to_string();
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let scale = scale as usize;
    let padded = if digits.len() <= scale {
        format!("{}{digits}", "0".repeat(scale + 1 - digits.len()))
    } else {
        digits
    };
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{sign}{int}.{frac}")
}

fn duration_text(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let abs = micros.unsigned_abs();
    let total_secs = abs / 1_000_000;
    let us = abs % 1_000_000;
    let (h, m, s) = (total_secs / 3_600, (total_secs % 3_600) / 60, total_secs % 60);
    if us == 0 {
        format!("{sign}{h:02}:{m:02}:{s:02}")
    } else {
        format!("{sign}{h:02}:{m:02}:{s:02}.{us:06}")
    }
}

fn arrow_type_to_nf(dt: &DataType) -> &'static str {
    match dt {
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        // Durations may exceed a day; a TIME column is a time of day.
        DataType::Duration(_) => "INTERVAL",
        DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => "REAL",
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
