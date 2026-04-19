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
///
/// Returns
/// -------
/// pyarrow.RecordBatch
///     The decoded table data.
#[pyfunction]
#[pyo3(signature = (path, columns=None))]
fn read(path: &str, columns: Option<Vec<String>>) -> PyResult<PyRecordBatch> {
    let qvd = openqvd::Qvd::from_path(path).map_err(to_py)?;
    let col_refs: Option<Vec<&str>> = columns
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect());
    let batch = qvd
        .to_record_batch(col_refs.as_deref())
        .map_err(to_py)?;
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
