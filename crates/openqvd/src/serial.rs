//! Conversions between Qlik day serials and Unix-based integers.
//!
//! Readers use these to turn stored serials into Arrow-style values;
//! writers use them to check that what they are about to store reads back.
//! All of them return `None` for a value outside the target range.

/// Days from 30 Dec 1899 (Qlik day 0) to 1 Jan 1970.
pub(crate) const QLIK_EPOCH_OFFSET: i32 = 25_569;

const MICROS_PER_DAY: f64 = 86_400_000_000.0;

/// Whole days since 1970-01-01 for a Qlik day serial. The fraction is
/// floored, so a serial of -0.5 is the day before day 0.
pub fn serial_to_unix_days(serial: f64) -> Option<i32> {
    let days = serial.floor();
    if !(f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&days) {
        return None;
    }
    (days as i32).checked_sub(QLIK_EPOCH_OFFSET)
}

/// Microseconds since 1970-01-01T00:00:00 for a Qlik day serial, rounded to
/// the nearest microsecond. (Truncating lands 1 us early for about 30% of
/// whole seconds, because a day count is not exactly representable.)
pub fn serial_to_unix_micros(serial: f64) -> Option<i64> {
    duration_days_to_micros(serial - f64::from(QLIK_EPOCH_OFFSET))
}

/// Microseconds in a (fractional) number of days, rounded to the nearest
/// microsecond.
pub fn duration_days_to_micros(days: f64) -> Option<i64> {
    let micros = (days * MICROS_PER_DAY).round();
    // The upper bound is exclusive: 2^63 as f64 is just out of range.
    if !micros.is_finite() || micros < i64::MIN as f64 || micros >= i64::MAX as f64 {
        return None;
    }
    Some(micros as i64)
}
