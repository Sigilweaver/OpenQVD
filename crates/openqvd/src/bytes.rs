//! Tiny byte-slice helpers that return typed errors on truncated input,
//! replacing `slice.get(a..b).ok_or(...)?.try_into().unwrap()` chains.

use crate::error::QvdError;

#[inline]
fn slice_at<const N: usize>(
    bytes: &[u8],
    offset: usize,
    what: &'static str,
) -> Result<[u8; N], QvdError> {
    bytes
        .get(offset..offset + N)
        .ok_or_else(|| QvdError::structure(format!("truncated {what} at byte {offset}")))?
        .try_into()
        .map_err(|_| QvdError::structure(format!("internal: slice_at length mismatch for {what}")))
}

#[inline]
pub(crate) fn read_i32_le(
    bytes: &[u8],
    offset: usize,
    what: &'static str,
) -> Result<i32, QvdError> {
    Ok(i32::from_le_bytes(slice_at::<4>(bytes, offset, what)?))
}

#[inline]
pub(crate) fn read_f64_le(
    bytes: &[u8],
    offset: usize,
    what: &'static str,
) -> Result<f64, QvdError> {
    Ok(f64::from_le_bytes(slice_at::<8>(bytes, offset, what)?))
}
