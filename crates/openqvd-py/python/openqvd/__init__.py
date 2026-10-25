"""
openqvd - Clean-room reader/writer for Qlik QVD files.

Quick start
-----------
Read as a PyArrow Table::

    import openqvd
    table = openqvd.read("data.qvd")

Read selected columns only (projection pushdown)::

    table = openqvd.read("data.qvd", columns=["OrderId", "Amount"])

Inspect metadata without loading data::

    info = openqvd.schema("data.qvd")
    print(info.table_name, info.num_rows, [f.name for f in info.fields])

Write from a PyArrow Table::

    openqvd.write(table, "out.qvd")
    openqvd.write(table, "out.qvd", table_name="MyTable")

Convert to/from Polars::

    import openqvd.polars  # registers pl.read_qvd, pl.scan_qvd, df.qvd.write()
    import polars as pl

    df = pl.read_qvd("data.qvd")
    lf = pl.scan_qvd("data.qvd", columns=["A", "B"])
    df.qvd.write("out.qvd")

Convert to/from Pandas::

    df = openqvd.read("data.qvd").to_pandas()

DuckDB integration::

    import duckdb
    import openqvd.duckdb as qdb

    con = duckdb.connect()
    qdb.register(con, "orders", "orders.qvd")
    con.execute("SELECT COUNT(*) FROM orders").fetchone()
    qdb.from_query("SELECT id FROM orders", "ids.qvd", con=con)

"""

from __future__ import annotations

import warnings as _warnings
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

# The Rust extension module.
from openqvd._openqvd import (  # noqa: F401
    FieldInfo,
    Schema,
    __version__,
    read as _read,
    schema as _schema,
    write as _write,
)
from openqvd.exceptions import (  # noqa: F401
    QvdConversionError,
    QvdError,
    QvdFileNotFoundError,
    QvdFormatError,
    QvdHeaderError,
    QvdIOError,
    QvdPermissionError,
    QvdSchemaError,
    QvdStructureError,
    QvdSymbolError,
    QvdUnsupportedError,
    QvdWarning,
)
from openqvd.options import Leniency, LeniencyLike, ReadWarning, _check_names

if TYPE_CHECKING:
    import pyarrow as pa


__all__ = [
    "read",
    "write",
    "schema",
    "Schema",
    "FieldInfo",
    "Leniency",
    "LeniencyLike",
    "ReadWarning",
    "QvdError",
    "QvdIOError",
    "QvdFileNotFoundError",
    "QvdPermissionError",
    "QvdFormatError",
    "QvdHeaderError",
    "QvdStructureError",
    "QvdSymbolError",
    "QvdSchemaError",
    "QvdConversionError",
    "QvdUnsupportedError",
    "QvdWarning",
    "__version__",
]


def schema(path: Union[str, Path], *, leniency: LeniencyLike = None) -> Schema:
    """Read a QVD file's metadata without decoding symbol tables or rows.

    Parameters
    ----------
    path:
        Path to the ``.qvd`` file.
    leniency:
        A :class:`Leniency` (or iterable of check names) naming the checks
        that may be skipped. ``None`` (default) is strict.

    Returns
    -------
    Schema
        ``.table_name``, ``.num_rows``, ``.fields`` and ``.warnings``.

    Raises
    ------
    QvdError
        If the file is not a valid QVD. See :mod:`openqvd.exceptions`.
    """
    result = _schema(str(path), _check_names(leniency))
    _emit_warnings(result.warnings)
    return result


def _emit_warnings(found: List[ReadWarning]) -> None:
    for w in found:
        _warnings.warn(str(w), QvdWarning, stacklevel=3)


def read(
    path: Union[str, Path],
    *,
    columns: Optional[List[str]] = None,
    filters: Optional[List[dict]] = None,
    leniency: LeniencyLike = None,
) -> "pa.Table":
    """Read a QVD file as a PyArrow Table.

    Parameters
    ----------
    path:
        Path to the ``.qvd`` file.
    columns:
        Optional list of column names to load. When specified, symbol
        tables for excluded columns are not decoded at all (projection
        pushdown). Pass ``None`` (default) to load all columns.
    filters:
        Optional list of predicate-pushdown filter dicts. Each dict has:

        - ``"column"``: column name (str)
        - ``"op"``: one of ``"eq"``, ``"is_in"``, ``"not_in"``,
          ``"is_null"``, ``"is_not_null"``
        - ``"value"``: str for ``"eq"``, list[str] for ``"is_in"``/
          ``"not_in"``, omitted for null checks.

        Filters are resolved against the column's symbol table *before*
        row iteration, so non-matching rows are skipped efficiently.
    leniency:
        A :class:`Leniency` (or iterable of check names) naming the checks
        that may be skipped. ``None`` (default) is strict. Every deviation a
        lenient read tolerates is reported with :class:`QvdWarning`.

    Returns
    -------
    pyarrow.Table
        The decoded table data.

    Raises
    ------
    QvdFileNotFoundError
        If ``path`` does not exist (also a :class:`FileNotFoundError`).
    QvdFormatError
        If the file is not a valid QVD.
    QvdSchemaError
        If a requested column is not found.
    QvdError
        Base class of all of the above (and a :class:`ValueError`).

    Examples
    --------
    >>> import openqvd
    >>> table = openqvd.read("data.qvd")
    >>> table = openqvd.read("data.qvd", columns=["col1", "col2"])
    >>> table = openqvd.read("data.qvd", filters=[{"column": "Status", "op": "eq", "value": "Active"}])
    """
    import pyarrow as pa

    batch, found = _read(str(path), columns, filters, _check_names(leniency))
    _emit_warnings(found)
    return pa.table(batch)


def write(
    data: "pa.Table | pa.RecordBatch",
    path: Union[str, Path],
    *,
    table_name: Optional[str] = None,
    leniency: LeniencyLike = None,
) -> None:
    """Write a PyArrow Table or RecordBatch to a QVD file.

    Parameters
    ----------
    data:
        PyArrow Table or RecordBatch to serialise.
    path:
        Destination file path.
    table_name:
        Logical table name stored in the QVD header.  Defaults to the
        file stem of ``path``.
    leniency:
        A :class:`Leniency` (or iterable of check names) naming the checks
        that may be skipped. ``None`` (default) is strict.

    Examples
    --------
    >>> import pyarrow as pa, openqvd
    >>> t = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    >>> openqvd.write(t, "out.qvd")
    """
    import pyarrow as pa

    if isinstance(data, pa.Table):
        # Combine chunks so pyo3-arrow receives a single RecordBatch.
        batches = data.combine_chunks().to_batches()
        if batches:
            data = batches[0]
        else:
            # Empty table: build an empty RecordBatch from the schema.
            arrays = [pa.array([], type=f.type) for f in data.schema]
            data = pa.record_batch(arrays, schema=data.schema)
    _write(data, str(path), table_name, _check_names(leniency))
