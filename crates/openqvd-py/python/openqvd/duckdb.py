"""
openqvd.duckdb - DuckDB integration for OpenQVD.

Provides three helpers that bridge QVD files and DuckDB via Arrow interop.
A native DuckDB table function (``SELECT * FROM read_qvd('file.qvd')``) would
require a C++ extension and is out of scope; these helpers give equivalent
ergonomics from Python.

Usage::

    import duckdb
    import openqvd.duckdb as qdb

    con = duckdb.connect()

    # Register a QVD file as a SQL view
    qdb.register(con, "orders", "orders.qvd")
    con.execute("SELECT COUNT(*) FROM orders WHERE Region = 'West'").fetchone()

    # Or get a DuckDB relation directly
    rel = qdb.to_relation("orders.qvd", con)

    # Write a DuckDB query result to a QVD file
    qdb.from_query("SELECT id, amount FROM orders WHERE status = 'Open'", "open.qvd", con=con)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union


def to_relation(
    path: Union[str, Path],
    con=None,
    *,
    view_name: Optional[str] = None,
):
    """Load a QVD file into DuckDB and return a DuckDB relation.

    Parameters
    ----------
    path:
        Path to the ``.qvd`` file.
    con:
        An existing :class:`duckdb.DuckDBPyConnection`. When ``None``, a new
        in-memory connection is created and returned as part of the relation.
    view_name:
        When provided, the data is also registered as a view on the connection
        under this name so it can be queried by SQL.

    Returns
    -------
    duckdb.DuckDBPyRelation

    Examples
    --------
    >>> import duckdb, openqvd.duckdb as qdb
    >>> con = duckdb.connect()
    >>> rel = qdb.to_relation("data.qvd", con, view_name="data")
    >>> con.execute("SELECT COUNT(*) FROM data").fetchone()
    """
    import duckdb

    import openqvd

    if con is None:
        con = duckdb.connect()

    table = openqvd.read(str(path))
    if view_name is not None:
        con.register(view_name, table)
        return con.table(view_name)
    return con.from_arrow(table)


def register(con, name: str, path: Union[str, Path]) -> None:
    """Register a QVD file as a named view on a DuckDB connection.

    The file is read eagerly into an Arrow table and registered; subsequent
    SQL queries against ``name`` do not re-read the file.

    Parameters
    ----------
    con:
        An open :class:`duckdb.DuckDBPyConnection`.
    name:
        The SQL view name to register.
    path:
        Path to the ``.qvd`` file.

    Examples
    --------
    >>> import duckdb, openqvd.duckdb as qdb
    >>> con = duckdb.connect()
    >>> qdb.register(con, "sales", "sales.qvd")
    >>> con.execute("SELECT SUM(Amount) FROM sales").fetchone()
    """
    import openqvd

    table = openqvd.read(str(path))
    con.register(name, table)


def from_query(
    source,
    path: Union[str, Path],
    *,
    con=None,
    table_name: Optional[str] = None,
) -> None:
    """Write a DuckDB query or relation to a QVD file.

    Parameters
    ----------
    source:
        One of:

        - A SQL query string (requires ``con``).
        - A :class:`duckdb.DuckDBPyRelation`.
    path:
        Destination ``.qvd`` file path.
    con:
        Required when ``source`` is a SQL string.
    table_name:
        Logical table name stored in the QVD header. Defaults to the file stem
        of ``path``.

    Examples
    --------
    >>> import duckdb, openqvd.duckdb as qdb
    >>> con = duckdb.connect()
    >>> qdb.register(con, "orders", "orders.qvd")
    >>> qdb.from_query("SELECT id, amount FROM orders", "subset.qvd", con=con)
    """
    import duckdb

    import openqvd

    if isinstance(source, str):
        if con is None:
            raise ValueError(
                "from_query() requires a 'con' argument when 'source' is a SQL string"
            )
        rel = con.sql(source)
    elif isinstance(source, duckdb.DuckDBPyRelation):
        rel = source
    else:
        raise TypeError(
            f"'source' must be a SQL string or DuckDBPyRelation, got {type(source)}"
        )

    # DuckDB's .arrow() may return a pa.Table or a RecordBatchReader depending
    # on version; normalise to a Table.
    arrow_obj = rel.arrow()
    if hasattr(arrow_obj, "read_all"):
        table = arrow_obj.read_all()
    else:
        table = arrow_obj

    openqvd.write(table, path, table_name=table_name)
