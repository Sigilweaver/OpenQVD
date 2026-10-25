//! Row filters, resolved against symbol tables so only the packed index of
//! each row is compared.

use std::collections::HashSet;

use crate::error::QvdError;
use crate::reader::{Qvd, NULL_INDEX};
use crate::value::Value;

/// A predicate on one column.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
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
    let matching = |targets: &[String]| -> HashSet<usize> {
        let t: HashSet<&str> = targets.iter().map(|s| s.as_str()).collect();
        symbols
            .iter()
            .enumerate()
            .filter(|(_, s)| t.iter().any(|x| symbol_matches_str(s, x)))
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

fn symbol_matches_str(sym: &Value, target: &str) -> bool {
    match sym {
        Value::Str(s) => s == target,
        Value::Int(i) => i.to_string() == target,
        Value::Float(f) => f.to_string() == target,
        Value::DualInt(d) => d.text == target || d.number.to_string() == target,
        Value::DualFloat(d) => d.text == target || d.number.to_string() == target,
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
