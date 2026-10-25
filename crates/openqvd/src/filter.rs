//! Row filters, resolved against symbol tables so only the packed index of
//! each row is compared.

use std::collections::HashSet;

use crate::error::QvdError;
use crate::reader::{Qvd, NULL_INDEX};
use crate::value::Value;

/// A value a filter compares against. Numbers compare numerically with
/// numeric symbols (so `Float(1.0)` matches the integer 1), text compares
/// with string symbols and with the text of dual symbols.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum FilterValue {
    /// Text.
    Str(String),
    /// An integer.
    Int(i64),
    /// A float. NaN matches NaN symbols.
    Float(f64),
}

impl From<&str> for FilterValue {
    fn from(s: &str) -> Self {
        FilterValue::Str(s.to_owned())
    }
}
impl From<String> for FilterValue {
    fn from(s: String) -> Self {
        FilterValue::Str(s)
    }
}
impl From<i64> for FilterValue {
    fn from(i: i64) -> Self {
        FilterValue::Int(i)
    }
}
impl From<f64> for FilterValue {
    fn from(f: f64) -> Self {
        FilterValue::Float(f)
    }
}

/// A predicate on one column.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ColumnFilter {
    /// Keep rows where the column value equals this value.
    Eq(FilterValue),
    /// Keep rows where the column value is one of these values.
    IsIn(Vec<FilterValue>),
    /// Keep rows where the column value is NOT one of these values.
    NotIn(Vec<FilterValue>),
    /// Keep rows where the column is NULL.
    IsNull,
    /// Keep rows where the column is NOT NULL.
    IsNotNull,
}

/// A named filter: column name + predicate.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Filter {
    /// Column name.
    pub column: String,
    /// The predicate to apply.
    pub predicate: ColumnFilter,
}

impl Filter {
    /// A filter applying `predicate` to the column called `column`.
    pub fn new(column: impl Into<String>, predicate: ColumnFilter) -> Self {
        Self {
            column: column.into(),
            predicate,
        }
    }
}

/// Symbol indices that satisfy `pred`, and whether NULL rows pass.
fn resolve(symbols: &[Value], pred: &ColumnFilter) -> (HashSet<usize>, bool) {
    let matching = |targets: &[FilterValue]| -> HashSet<usize> {
        symbols
            .iter()
            .enumerate()
            .filter(|(_, s)| targets.iter().any(|t| symbol_matches(s, t)))
            .map(|(i, _)| i)
            .collect()
    };
    match pred {
        ColumnFilter::IsNull => (HashSet::new(), true),
        ColumnFilter::IsNotNull => ((0..symbols.len()).collect(), false),
        ColumnFilter::Eq(target) => (matching(std::slice::from_ref(target)), false),
        ColumnFilter::IsIn(targets) => (matching(targets), false),
        ColumnFilter::NotIn(targets) => {
            let hit = matching(targets);
            // NULL is not "in" any set, so it passes NotIn.
            (
                (0..symbols.len()).filter(|i| !hit.contains(i)).collect(),
                true,
            )
        }
    }
}

fn number_of(sym: &Value) -> Option<f64> {
    match sym {
        Value::Int(i) => Some(f64::from(*i)),
        Value::Float(f) => Some(*f),
        Value::DualInt(d) => Some(f64::from(d.number)),
        Value::DualFloat(d) => Some(d.number),
        _ => None,
    }
}

fn text_of(sym: &Value) -> Option<&str> {
    match sym {
        Value::Str(s) => Some(s),
        Value::DualInt(d) => Some(&d.text),
        Value::DualFloat(d) => Some(&d.text),
        _ => None,
    }
}

fn symbol_matches(sym: &Value, target: &FilterValue) -> bool {
    match target {
        FilterValue::Str(t) => text_of(sym) == Some(t.as_str()),
        FilterValue::Int(t) => match sym {
            // Exact for integers; floats compare only if the integer is
            // exactly representable.
            Value::Int(i) => i64::from(*i) == *t,
            Value::DualInt(d) => i64::from(d.number) == *t,
            _ => number_of(sym).is_some_and(|n| {
                // 2^63 is not an i64; `as` would saturate it onto i64::MAX.
                const TWO_63: f64 = 9_223_372_036_854_775_808.0;
                n.fract() == 0.0 && (-TWO_63..TWO_63).contains(&n) && n as i64 == *t
            }),
        },
        FilterValue::Float(t) => {
            number_of(sym).is_some_and(|n| n == *t || (n.is_nan() && t.is_nan()))
        }
    }
}

impl Qvd {
    /// Row numbers (ascending) for which every filter holds.
    ///
    /// Each filter's column must have been decoded. Rows are compared by
    /// symbol index, so a NaN symbol matches the same way every other symbol
    /// does.
    pub fn select_rows(&self, filters: &[Filter]) -> Result<Vec<u32>, QvdError> {
        let n = self.num_rows();
        let mut keep: Vec<bool> = vec![true; n as usize];
        for f in filters {
            let fi = self.field_index(&f.column)?;
            let (pass, null_passes) = resolve(self.symbols(fi).unwrap_or(&[]), &f.predicate);
            for (row, idx) in self.column_indices(fi)?.into_iter().enumerate() {
                let ok = if idx == NULL_INDEX {
                    null_passes
                } else {
                    pass.contains(&(idx as usize))
                };
                keep[row] &= ok;
            }
        }
        Ok(keep
            .iter()
            .enumerate()
            .filter(|(_, k)| **k)
            .map(|(i, _)| i as u32)
            .collect())
    }
}
