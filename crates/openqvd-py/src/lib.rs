//! PyO3 bindings for OpenQVD.
//!
//! Exposed as `openqvd._openqvd` (the private extension module). The public
//! Python API lives in `openqvd/__init__.py` which re-exports everything
//! through a friendlier interface.

#![allow(unsafe_code)] // required by pyo3 proc-macros

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

/// Name of the `openqvd.exceptions` class for an error kind. Kinds this
/// binding does not know yet fall back to the base class, so a new Rust kind
/// is never reported as something unrelated.
fn exception_name(kind: openqvd::ErrorKind) -> &'static str {
    use openqvd::ErrorKind as K;
    match kind {
        K::Io => "QvdIOError",
        K::Header => "QvdHeaderError",
        K::Structure => "QvdStructureError",
        K::Symbol => "QvdSymbolError",
        K::Schema => "QvdSchemaError",
        K::Conversion => "QvdConversionError",
        K::Unsupported => "QvdUnsupportedError",
        _ => "QvdError",
    }
}

/// Map a Rust error to the matching `openqvd.exceptions` class by its typed
/// kind, never by its message text.
fn to_py(e: openqvd::QvdError) -> PyErr {
    Python::attach(|py| {
        let build = || -> PyResult<PyErr> {
            let module = py.import("openqvd.exceptions")?;
            if let openqvd::QvdError::Io(io) = &e {
                use std::io::ErrorKind as IoKind;
                let name = match io.kind() {
                    IoKind::NotFound => "QvdFileNotFoundError",
                    IoKind::PermissionDenied => "QvdPermissionError",
                    _ => "QvdIOError",
                };
                let cls = module.getattr(name)?;
                let text = io.to_string();
                let exc = match io.raw_os_error() {
                    Some(errno) => cls.call1((errno, text))?,
                    None => cls.call1((text,))?,
                };
                return Ok(PyErr::from_value(exc));
            }
            let cls = module.getattr(exception_name(e.kind()))?;
            Ok(PyErr::from_value(cls.call1((e.to_string(),))?))
        };
        match build() {
            Ok(err) => err,
            Err(import_err) => import_err,
        }
    })
}

/// Parse leniency check names passed from Python.
fn parse_leniency(names: Option<Vec<String>>) -> PyResult<openqvd::Leniency> {
    let mut leniency = openqvd::Leniency::strict();
    for name in names.unwrap_or_default() {
        let check: openqvd::LenientCheck = name
            .parse()
            .map_err(|e: openqvd::UnknownCheck| PyValueError::new_err(e.to_string()))?;
        leniency = leniency.with(check);
    }
    Ok(leniency)
}

/// Names of every lenient check this build knows about.
#[pyfunction]
fn lenient_checks() -> Vec<&'static str> {
    openqvd::LenientCheck::ALL
        .iter()
        .map(|c| c.as_str())
        .collect()
}

/// One deviation that a lenient read tolerated.
#[pyclass(
    module = "openqvd._openqvd",
    name = "ReadWarning",
    frozen,
    eq,
    from_py_object
)]
#[derive(Clone, PartialEq)]
struct PyReadWarning {
    /// Stable snake_case code, e.g. ``"non_numeric_temporal"``.
    #[pyo3(get)]
    kind: String,
    /// Affected column name, if the warning is about one.
    #[pyo3(get)]
    column: Option<String>,
    /// Affected zero-based row, if the warning is about one.
    #[pyo3(get)]
    row: Option<u64>,
    /// Human-readable detail.
    #[pyo3(get)]
    detail: String,
}

#[pymethods]
impl PyReadWarning {
    fn __repr__(&self) -> String {
        format!(
            "ReadWarning(kind={:?}, column={:?}, row={:?}, detail={:?})",
            self.kind, self.column, self.row, self.detail
        )
    }

    fn __str__(&self) -> String {
        let mut s = self.kind.clone();
        if let Some(c) = &self.column {
            s.push_str(&format!(" column {c:?}"));
        }
        if let Some(r) = self.row {
            s.push_str(&format!(" row {r}"));
        }
        s.push_str(&format!(": {}", self.detail));
        s
    }
}

impl From<&openqvd::Warning> for PyReadWarning {
    fn from(w: &openqvd::Warning) -> Self {
        Self {
            kind: w.kind.code().to_owned(),
            column: w.column.clone(),
            row: w.row,
            detail: w.detail.clone(),
        }
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
    #[pyo3(get)]
    warnings: Vec<PyReadWarning>,
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
    #[getter]
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
        result.push(openqvd::Filter::new(column, predicate));
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Core read function
// ---------------------------------------------------------------------------

/// Read a QVD file and return an Arrow RecordBatch plus any warnings.
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
/// leniency : list[str] | None
///     Names of the lenient checks to allow. ``None`` means strict.
///
/// Returns
/// -------
/// tuple[pyarrow.RecordBatch, list[ReadWarning]]
///     The decoded table data and what a lenient read tolerated.
#[pyfunction]
#[pyo3(signature = (path, columns=None, filters=None, leniency=None))]
fn read(
    py: Python<'_>,
    path: &str,
    columns: Option<Vec<String>>,
    filters: Option<Bound<'_, pyo3::types::PyList>>,
    leniency: Option<Vec<String>>,
) -> PyResult<(PyRecordBatch, Vec<PyReadWarning>)> {
    let leniency = parse_leniency(leniency)?;
    let rust_filters = match filters {
        Some(ref f) if !f.is_empty() => Some(parse_py_filters(py, f)?),
        _ => None,
    };

    // Symbol tables are decoded only for the projected columns plus the
    // filter columns. Without a projection every column is needed for the
    // output, so nothing is restricted.
    let mut options = openqvd::ReadOptions::default().with_leniency(leniency);
    if let Some(cols) = &columns {
        let mut set: Vec<String> = cols.clone();
        if let Some(fs) = &rust_filters {
            for f in fs {
                if !set.contains(&f.column) {
                    set.push(f.column.clone());
                }
            }
        }
        options = options.with_columns(set);
    }
    let qvd = openqvd::Qvd::open(path, &options).map_err(to_py)?;

    let col_refs: Option<Vec<&str>> = columns
        .as_ref()
        .map(|v| v.iter().map(|s| s.as_str()).collect());
    let batch = match rust_filters {
        Some(ref fs) if !fs.is_empty() => qvd
            .to_record_batch_filtered(col_refs.as_deref(), fs)
            .map_err(to_py)?,
        _ => qvd.to_record_batch(col_refs.as_deref()).map_err(to_py)?,
    };
    let warnings = qvd.warnings().iter().map(PyReadWarning::from).collect();
    Ok((PyRecordBatch::new(batch), warnings))
}

/// Read only the schema/metadata of a QVD file without decoding symbol
/// tables or row data.
///
/// Parameters
/// ----------
/// path : str
///     Path to the .qvd file.
/// leniency : list[str] | None
///     Names of the lenient checks to allow. ``None`` means strict.
///
/// Returns
/// -------
/// Schema
///     Metadata object with `.table_name`, `.num_rows`, `.fields`,
///     `.warnings`.
#[pyfunction]
#[pyo3(signature = (path, leniency=None))]
fn schema(path: &str, leniency: Option<Vec<String>>) -> PyResult<PyQvdSchema> {
    let options = openqvd::ReadOptions::default()
        .with_columns(Vec::<String>::new())
        .with_leniency(parse_leniency(leniency)?);
    let qvd = openqvd::Qvd::open(path, &options).map_err(to_py)?;
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
        warnings: qvd.warnings().iter().map(PyReadWarning::from).collect(),
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
/// leniency : list[str] | None
///     Names of the lenient checks to allow. ``None`` means strict.
#[pyfunction]
#[pyo3(signature = (data, path, table_name=None, leniency=None))]
fn write(
    data: PyRecordBatch,
    path: &str,
    table_name: Option<&str>,
    leniency: Option<Vec<String>>,
) -> PyResult<()> {
    let batch = data.into_inner();
    let stem = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("table");
    let options = openqvd::WriteOptions::default()
        .with_table_name(table_name.unwrap_or(stem))
        .with_leniency(parse_leniency(leniency)?);
    let wt = openqvd::record_batch_to_write_table(&batch, &options).map_err(to_py)?;
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
    m.add_function(wrap_pyfunction!(lenient_checks, m)?)?;
    m.add_class::<PyReadWarning>()?;
    m.add_class::<PyQvdSchema>()?;
    m.add_class::<PyFieldInfo>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
