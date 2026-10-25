//! PyO3 bindings for OpenQVD.
//!
//! Exposed as `openqvd._openqvd` (the private extension module). The public
//! Python API lives in `openqvd/__init__.py` which re-exports everything
//! through a friendlier interface.

#![allow(unsafe_code)] // required by pyo3 proc-macros

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods};

use arrow_array::ffi::{from_ffi, FFI_ArrowArray};
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::SchemaRef;
use openqvd_arrow::v60 as arrow_v;
use std::ffi::CString;

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
    /// Logical type the column decodes to, e.g. ``"timestamp_us"``. A DATE
    /// column is ``"date32"`` or, if any value has a time of day,
    /// ``"timestamp_us"``.
    #[pyo3(get)]
    dtype: String,
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
// Arrow PyCapsule interface (https://arrow.apache.org/docs/format/CDataInterface/PyCapsules.html)
// ---------------------------------------------------------------------------

fn arrow_err(e: impl std::fmt::Display) -> PyErr {
    to_py(openqvd::QvdError::Arrow {
        detail: e.to_string(),
    })
}

fn capsule_name(name: &str) -> PyResult<CString> {
    CString::new(name).map_err(arrow_err)
}

/// Decoded table data handed to Python as an Arrow stream.
///
/// Private; the public `openqvd.Table` wraps it.
#[pyclass(module = "openqvd._openqvd", name = "_Batches", frozen)]
struct PyBatches {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

#[pymethods]
impl PyBatches {
    #[getter]
    fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Arrow C stream capsule. ``requested_schema`` is accepted and ignored:
    /// the data is always produced in its own schema, which the consumer
    /// must handle (allowed by the PyCapsule specification).
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader = RecordBatchIterator::new(
            self.batches.clone().into_iter().map(Ok),
            self.schema.clone(),
        );
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        // The destructor owns `stream`; dropping it releases the stream
        // unless a consumer already moved it out.
        PyCapsule::new_with_value_and_destructor(
            py,
            stream,
            c"arrow_array_stream",
            |_stream, _ctx| {},
        )
    }

    /// Arrow C schema capsule.
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let schema = FFI_ArrowSchema::try_from(self.schema.as_ref()).map_err(arrow_err)?;
        PyCapsule::new_with_value_and_destructor(py, schema, c"arrow_schema", |_schema, _ctx| {})
    }
}

/// Take the contents out of a capsule produced by another library.
///
/// # Safety contract
/// The capsule must have been created by an implementation of the Arrow
/// PyCapsule interface with the given name; the name is checked.
fn take_capsule<T>(capsule: &Bound<'_, PyAny>, name: &str, empty: T) -> PyResult<T> {
    let capsule = capsule.cast::<PyCapsule>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(format!("expected a {name:?} PyCapsule"))
    })?;
    let cname = capsule_name(name)?;
    let ptr = capsule.pointer_checked(Some(cname.as_c_str()))?;
    // SAFETY: the name matched, so the pointer refers to a live `T` laid out
    // as the Arrow C Data Interface specifies. Replacing it with an empty
    // (released) value transfers ownership to us, as the specification
    // requires of a consumer.
    Ok(unsafe { std::ptr::replace(ptr.as_ptr() as *mut T, empty) })
}

/// Read every batch from any object that implements `__arrow_c_stream__` or
/// `__arrow_c_array__`.
fn batches_from_py(obj: &Bound<'_, PyAny>) -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
    if obj.hasattr("__arrow_c_stream__")? {
        let cap = obj.call_method0("__arrow_c_stream__")?;
        let stream = take_capsule(&cap, "arrow_array_stream", FFI_ArrowArrayStream::empty())?;
        let reader = ArrowArrayStreamReader::try_new(stream).map_err(arrow_err)?;
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>().map_err(arrow_err)?;
        Ok((schema, batches))
    } else if obj.hasattr("__arrow_c_array__")? {
        let pair = obj.call_method0("__arrow_c_array__")?;
        let (schema_cap, array_cap): (Bound<'_, PyAny>, Bound<'_, PyAny>) = pair.extract()?;
        let schema = take_capsule(&schema_cap, "arrow_schema", FFI_ArrowSchema::empty())?;
        let array = take_capsule(&array_cap, "arrow_array", FFI_ArrowArray::empty())?;
        // SAFETY: schema and array come from the same producer and were
        // moved out of their capsules above.
        let data = unsafe { from_ffi(array, &schema) }.map_err(arrow_err)?;
        let batch = RecordBatch::from(StructArray::from(data));
        Ok((batch.schema(), vec![batch]))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(
            "expected an object implementing __arrow_c_stream__ or __arrow_c_array__ \
             (for example a pyarrow or polars table)",
        ))
    }
}

// ---------------------------------------------------------------------------
// Filter helpers
// ---------------------------------------------------------------------------

/// A Python filter value: ``int``, ``float`` or ``str``. ``bool`` and every
/// other type are rejected rather than matched by coincidence of their text.
fn py_filter_value(v: &Bound<'_, PyAny>) -> PyResult<openqvd::FilterValue> {
    use pyo3::types::{PyBool, PyFloat, PyInt, PyString};
    if v.is_instance_of::<PyBool>() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "filter values must be int, float or str, not bool",
        ));
    }
    if v.is_instance_of::<PyInt>() {
        return v
            .extract::<i64>()
            .map(openqvd::FilterValue::Int)
            .map_err(|_| {
                PyValueError::new_err(format!("filter integer {v} does not fit in 64 bits"))
            });
    }
    if v.is_instance_of::<PyFloat>() {
        return Ok(openqvd::FilterValue::Float(v.extract::<f64>()?));
    }
    if v.is_instance_of::<PyString>() {
        return Ok(openqvd::FilterValue::Str(v.extract::<String>()?));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "filter values must be int, float or str, not {}",
        v.get_type().name()?
    )))
}

/// Convert a Python filter dict list into Rust `Filter` objects.
///
/// Expected format per filter dict:
///   {"column": "col_name", "op": "eq"|"is_in"|"not_in"|"is_null"|"is_not_null", "value": ...}
///
/// `value` is an `int`, `float` or `str` for "eq", a list of those for
/// "is_in"/"not_in", and absent for "is_null"/"is_not_null".
fn parse_py_filters(
    _py: Python<'_>,
    filters: &Bound<'_, pyo3::types::PyList>,
) -> PyResult<Vec<openqvd::Filter>> {
    use pyo3::types::PyDict;

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
        let values = |name: &str| -> PyResult<Vec<openqvd::FilterValue>> {
            let obj = dict.get_item("value")?.ok_or_else(|| {
                PyValueError::new_err(format!("filter '{name}' requires a 'value' key"))
            })?;
            if obj.is_instance_of::<pyo3::types::PyString>() {
                return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                    "filter '{name}' needs a list of values, not a single string"
                )));
            }
            obj.try_iter()?.map(|v| py_filter_value(&v?)).collect()
        };
        let predicate = match op.as_str() {
            "eq" => {
                let val = dict
                    .get_item("value")?
                    .ok_or_else(|| PyValueError::new_err("filter 'eq' requires a 'value' key"))?;
                openqvd::ColumnFilter::Eq(py_filter_value(&val)?)
            }
            "is_in" => openqvd::ColumnFilter::IsIn(values("is_in")?),
            "not_in" => openqvd::ColumnFilter::NotIn(values("not_in")?),
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

/// Read a QVD file and return its data as an Arrow stream plus any warnings.
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
///     ``"is_not_null"``), and ``value`` (int, float or str for eq, a list of
///     those for is_in/not_in, absent for null checks). Numbers compare
///     numerically, strings compare with text. Filters are resolved against
///     the column symbol table before row iteration, so non-matching rows
///     are skipped without full decoding.
/// leniency : list[str] | None
///     Names of the lenient checks to allow. ``None`` means strict.
///
/// Returns
/// -------
/// tuple[_Batches, list[ReadWarning]]
///     The decoded table data and what a lenient read tolerated.
#[pyfunction]
#[pyo3(signature = (path, columns=None, filters=None, leniency=None))]
fn read(
    py: Python<'_>,
    path: &str,
    columns: Option<Vec<String>>,
    filters: Option<Bound<'_, pyo3::types::PyList>>,
    leniency: Option<Vec<String>>,
) -> PyResult<(PyBatches, Vec<PyReadWarning>)> {
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

    let mut batch_options = openqvd_arrow::BatchOptions::new();
    if let Some(cols) = columns {
        batch_options = batch_options.with_columns(cols);
    }
    if let Some(fs) = rust_filters {
        batch_options = batch_options.with_filters(fs);
    }
    let batch = arrow_v::to_record_batch(&qvd, &batch_options).map_err(to_py)?;
    let warnings = qvd.warnings().iter().map(PyReadWarning::from).collect();
    Ok((
        PyBatches {
            schema: batch.schema(),
            batches: vec![batch],
        },
        warnings,
    ))
}

/// Read the schema/metadata of a QVD file: header, symbol tables (to resolve
/// each column's type) and a validation pass over the rows, but no values.
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
    let options = openqvd::ReadOptions::default().with_leniency(parse_leniency(leniency)?);
    let qvd = openqvd::Qvd::open(path, &options).map_err(to_py)?;
    let fields: Vec<PyFieldInfo> = qvd
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            Ok(PyFieldInfo {
                name: f.name.clone(),
                number_format_type: f.number_format.r#type.clone(),
                n_symbols: f.no_of_symbols,
                tags: f.tags.clone(),
                bit_width: f.bit_width,
                bias: f.bias,
                dtype: qvd.column_type(i).map_err(to_py)?.as_str().to_owned(),
            })
        })
        .collect::<PyResult<_>>()?;
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

/// Write Arrow data to a QVD file.
///
/// Parameters
/// ----------
/// data : object
///     Anything implementing ``__arrow_c_stream__`` or ``__arrow_c_array__``
///     (a pyarrow Table, RecordBatch or RecordBatchReader, a polars
///     DataFrame, an ``openqvd.Table``, ...). Every batch is written.
/// path : str
///     Destination file path.
/// table_name : str, optional
///     Logical table name embedded in the QVD header. Defaults to the
///     file stem of ``path``.
/// leniency : list[str] | None
///     Names of the lenient checks to allow. ``None`` means strict.
///
/// Returns
/// -------
/// list[ReadWarning]
///     What a lenient write tolerated.
#[pyfunction]
#[pyo3(signature = (data, path, table_name=None, leniency=None))]
fn write(
    data: &Bound<'_, PyAny>,
    path: &str,
    table_name: Option<&str>,
    leniency: Option<Vec<String>>,
) -> PyResult<Vec<PyReadWarning>> {
    let (schema, batches) = batches_from_py(data)?;
    let stem = std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("table");
    let options = openqvd::WriteOptions::default()
        .with_table_name(table_name.unwrap_or(stem))
        .with_leniency(parse_leniency(leniency)?);
    let wt = arrow_v::record_batches_to_write_table(schema, &batches, &options).map_err(to_py)?;
    wt.write_to_path(path).map_err(to_py)?;
    Ok(wt.warnings().iter().map(PyReadWarning::from).collect())
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
    m.add_class::<PyBatches>()?;
    m.add_class::<PyQvdSchema>()?;
    m.add_class::<PyFieldInfo>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
