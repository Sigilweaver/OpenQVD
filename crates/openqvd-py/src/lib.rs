//! PyO3 bindings for OpenQVD.
//!
//! Exposed as `openqvd._openqvd` (the private extension module). The public
//! Python API lives in `openqvd/__init__.py` which re-exports everything
//! through a friendlier interface.

#![allow(unsafe_code)] // required by pyo3 proc-macros

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn to_py(e: openqvd::QvdError) -> PyErr {
    let s = e.to_string();
    if s.contains("io error") || s.contains("os error") {
        PyIOError::new_err(s)
    } else if s.contains("structure") || s.contains("xml") || s.contains("invalid") {
        PyValueError::new_err(s)
    } else {
        PyRuntimeError::new_err(s)
    }
}

// ---------------------------------------------------------------------------
// Python types
// ---------------------------------------------------------------------------

/// A lightweight handle to a parsed QVD file's metadata.
///
/// Returned by `openqvd.schema()`. Holds the table name, field names,
/// number-format types, tags, and row count without decoding any symbol
/// tables or row data.
#[pyclass(module = "openqvd._openqvd", name = "Schema", from_py_object)]
#[derive(Clone)]
struct PyQvdSchema {
    #[pyo3(get)]
    table_name: String,
    #[pyo3(get)]
    num_rows: u32,
    #[pyo3(get)]
    fields: Vec<PyFieldInfo>,
}

#[pymethods]
impl PyQvdSchema {
    fn __repr__(&self) -> String {
        format!(
            "Schema(table={:?}, rows={}, fields=[{}])",
            self.table_name,
            self.num_rows,
            self.fields
                .iter()
                .map(|f| format!("{:?}", f.name))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    /// Return column names in order.
    fn column_names(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name.clone()).collect()
    }
}

/// Metadata for a single QVD field.
#[pyclass(module = "openqvd._openqvd", name = "FieldInfo", from_py_object)]
#[derive(Clone)]
struct PyFieldInfo {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    number_format_type: String,
    #[pyo3(get)]
    n_symbols: u32,
    #[pyo3(get)]
    tags: Vec<String>,
    #[pyo3(get)]
    bit_width: u32,
    #[pyo3(get)]
    bias: i32,
}

#[pymethods]
impl PyFieldInfo {
    fn __repr__(&self) -> String {
        format!(
            "FieldInfo(name={:?}, type={:?}, n_symbols={}, tags={:?})",
            self.name, self.number_format_type, self.n_symbols, self.tags
        )
    }
}

// ---------------------------------------------------------------------------
// Filter helpers
// ---------------------------------------------------------------------------

/// Convert a Python filter dict list into Rust `Filter` objects.
///
/// Expected format per filter dict:
///   {"column": "col_name", "op": "eq"|"is_in"|"not_in"|"is_null"|"is_not_null", "value": ...}
///
/// `value` is a `str` for "eq", a `list[str]` for "is_in"/"not_in",
/// and unused/absent for "is_null"/"is_not_null".
fn parse_py_filters(
    _py: Python<'_>,
    filters: &Bound<'_, pyo3::types::PyList>,
) -> PyResult<Vec<openqvd::Filter>> {
    use pyo3::types::{PyDict, PyString};

    let mut result = Vec::with_capacity(filters.len());
    for item in filters.iter() {
        let dict = item.cast::<PyDict>().map_err(|_| {
            PyValueError::new_err(
                "each filter must be a dict with 'column', 'op', and optionally 'value'",
            )
        })?;
        let column: String = dict
            .get_item("column")?
            .ok_or_else(|| PyValueError::new_err("filter dict missing 'column' key"))?
            .extract()?;
        let op: String = dict
            .get_item("op")?
            .ok_or_else(|| PyValueError::new_err("filter dict missing 'op' key"))?
            .extract()?;
        let predicate = match op.as_str() {
            "eq" | "==" => {
                let val: String = dict
                    .get_item("value")?
                    .ok_or_else(|| PyValueError::new_err("filter 'eq' requires a 'value' key"))?
                    .str()?
                    .to_string();
                openqvd::ColumnFilter::Eq(val)
            }
            "is_in" | "in" => {
                let vals_obj = dict.get_item("value")?.ok_or_else(|| {
                    PyValueError::new_err("filter 'is_in' requires a 'value' key")
                })?;
                let vals: Vec<String> = vals_obj
                    .try_iter()?
                    .map(|v| {
                        let v = v?;
                        // Try str() first, fall back to repr
                        if let Ok(s) = v.cast::<PyString>() {
                            Ok(s.to_string())
                        } else {
                            Ok(v.str()?.to_string())
                        }
                    })
                    .collect::<PyResult<_>>()?;
                openqvd::ColumnFilter::IsIn(vals)
            }
            "not_in" => {
                let vals_obj = dict.get_item("value")?.ok_or_else(|| {
                    PyValueError::new_err("filter 'not_in' requires a 'value' key")
                })?;
                let vals: Vec<String> = vals_obj
                    .try_iter()?
                    .map(|v| {
                        let v = v?;
                        if let Ok(s) = v.cast::<PyString>() {
                            Ok(s.to_string())
                        } else {
                            Ok(v.str()?.to_string())
                        }
                    })
                    .collect::<PyResult<_>>()?;
                openqvd::ColumnFilter::NotIn(vals)
            }
            "is_null" => openqvd::ColumnFilter::IsNull,
            "is_not_null" => openqvd::ColumnFilter::IsNotNull,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown filter op {other:?}; use 'eq', 'is_in', 'not_in', 'is_null', or 'is_not_null'"
                )));
            }
        };
        result.push(openqvd::Filter { column, predicate });
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Core read function
// ---------------------------------------------------------------------------

/// Read a QVD file and return an Arrow RecordBatch.
///
/// Parameters
/// ----------
/// path : str
///     Path to the .qvd file.
/// columns : list[str] | None
///     Column names to include. ``None`` (default) returns all columns.
/// filters : list[dict] | None
///     Predicate pushdown filters. Each dict has keys ``column`` (str),
///     ``op`` (one of ``"eq"``, ``"is_in"``, ``"not_in"``, ``"is_null"``,
///     ``"is_not_null"``), and ``value`` (str for eq, list[str] for
///     is_in/not_in, absent for null checks). Filters are resolved against
///     the column symbol table before row iteration, so non-matching rows
///     are skipped without full decoding.
///
/// Returns
/// -------
/// pyarrow.RecordBatch
///     The decoded table data.
#[pyfunction]
#[pyo3(signature = (path, columns=None, filters=None))]
fn read(
    py: Python<'_>,
    path: &str,
    columns: Option<Vec<String>>,
    filters: Option<Bound<'_, pyo3::types::PyList>>,
) -> PyResult<PyRecordBatch> {
    // Compute the set of columns whose symbol tables we actually need.
    let rust_filters = match filters {
        Some(ref f) if !f.is_empty() => Some(parse_py_filters(py, f)?),
        _ => None,
    };

    let needed: Option<Vec<String>> = match (&columns, &rust_filters) {
        (None, None) => None, // need all columns
        _ => {
            let mut set = std::collections::HashSet::new();
            if let Some(cols) = &columns {
                for c in cols {
                    set.insert(c.clone());
                }
            }
            if let Some(fs) = &rust_filters {
                for f in fs {
                    set.insert(f.column.clone());
                }
            }
            // If only filters are set (no projection), we still need all
            // columns for the output, so don't restrict symbol decoding.
            if columns.is_none() {
                None
            } else {
                Some(set.into_iter().collect())
            }
        }
    };

    let qvd = match &needed {
        Some(names) => {
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            openqvd::Qvd::from_path_projected(path, &refs).map_err(to_py)?
        }
        None => openqvd::Qvd::from_path(path).map_err(to_py)?,
    };

    let col_refs: Option<Vec<&str>> = columns
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect());
    let batch = match rust_filters {
        Some(ref fs) if !fs.is_empty() => qvd
            .to_record_batch_filtered(col_refs.as_deref(), fs)
            .map_err(to_py)?,
        _ => qvd.to_record_batch(col_refs.as_deref()).map_err(to_py)?,
    };
    Ok(PyRecordBatch::new(batch))
}

/// Read only the schema/metadata of a QVD file without decoding row data.
///
/// Parameters
/// ----------
/// path : str
///     Path to the .qvd file.
///
/// Returns
/// -------
/// Schema
///     Metadata object with `.table_name`, `.num_rows`, `.fields`.
#[pyfunction]
fn schema(path: &str) -> PyResult<PyQvdSchema> {
    let qvd = openqvd::Qvd::from_path(path).map_err(to_py)?;
    let fields: Vec<PyFieldInfo> = qvd
        .fields()
        .iter()
        .map(|f| PyFieldInfo {
            name: f.name.clone(),
            number_format_type: f.number_format.r#type.clone(),
            n_symbols: f.no_of_symbols,
            tags: f.tags.clone(),
            bit_width: f.bit_width,
            bias: f.bias,
        })
        .collect();
    Ok(PyQvdSchema {
        table_name: qvd.table_name().to_string(),
        num_rows: qvd.num_rows(),
        fields,
    })
}

// ---------------------------------------------------------------------------
// Write function
// ---------------------------------------------------------------------------

/// Write an Arrow RecordBatch (or any object with __arrow_c_array__) to a
/// QVD file.
///
/// Parameters
/// ----------
/// data : pyarrow.RecordBatch
///     Data to write.
/// path : str
///     Destination file path.
/// table_name : str, optional
///     Logical table name embedded in the QVD header. Defaults to the
///     file stem of ``path``.
#[pyfunction]
#[pyo3(signature = (data, path, table_name=None))]
fn write(data: PyRecordBatch, path: &str, table_name: Option<&str>) -> PyResult<()> {
    let batch = data.into_inner();
    let stem = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("table");
    let name = table_name.unwrap_or(stem);
    let wt = openqvd::record_batch_to_write_table(&batch, name).map_err(to_py)?;
    wt.write_to_path(path).map_err(to_py)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Module definition
// ---------------------------------------------------------------------------

/// OpenQVD low-level Python extension.
///
/// Use `openqvd` (the public package) rather than importing this module
/// directly.
#[pymodule]
fn _openqvd(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(schema, m)?)?;
    m.add_function(wrap_pyfunction!(write, m)?)?;
    m.add_class::<PyQvdSchema>()?;
    m.add_class::<PyFieldInfo>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
