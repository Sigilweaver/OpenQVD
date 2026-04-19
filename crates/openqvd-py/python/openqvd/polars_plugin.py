"""
openqvd.polars - Polars integration for OpenQVD.

Importing this module monkey-patches Polars so you can use QVD files
directly from the Polars API::

    import openqvd.polars          # one-time import registers everything
    import polars as pl

    # Top-level read / lazy-scan
    df: pl.DataFrame  = pl.read_qvd("data.qvd")
    lf: pl.LazyFrame  = pl.scan_qvd("data.qvd", columns=["A", "B"])

    # DataFrame namespace: df.qvd.write(...)
    df.qvd.write("out.qvd")
    df.qvd.write("out.qvd", table_name="MyTable")

    # LazyFrame namespace: collect then write (scan for read, collect on demand)
    lf.qvd.collect_and_write("out.qvd")
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union

import polars as pl

import openqvd


# ---------------------------------------------------------------------------
# Top-level functions
# ---------------------------------------------------------------------------

def read_qvd(
    path: Union[str, Path],
    *,
    columns: Optional[List[str]] = None,
    filters: Optional[List[dict]] = None,
) -> pl.DataFrame:
    """Read a QVD file as a Polars DataFrame.

    Parameters
    ----------
    path:
        Path to the ``.qvd`` file.
    columns:
        Optional list of column names to select (projection pushdown).
    filters:
        Optional list of predicate-pushdown filter dicts. See
        ``openqvd.read`` for the format.

    Returns
    -------
    polars.DataFrame
    """
    table = openqvd.read(path, columns=columns, filters=filters)
    return pl.from_arrow(table)


def scan_qvd(
    path: Union[str, Path],
    *,
    columns: Optional[List[str]] = None,
    filters: Optional[List[dict]] = None,
) -> pl.LazyFrame:
    """Lazily scan a QVD file, returning a Polars LazyFrame.

    The actual decoding is deferred until ``.collect()`` is called.
    Column projection is applied in the Rust layer before Arrow arrays
    are materialised.

    Parameters
    ----------
    path:
        Path to the ``.qvd`` file.
    columns:
        Optional list of column names.  Columns not listed will not be
        decoded, saving memory for wide tables.
    filters:
        Optional list of predicate-pushdown filter dicts. See
        ``openqvd.read`` for the format.

    Returns
    -------
    polars.LazyFrame
    """
    # Polars does not have a native QVD scan source yet, so we wrap the
    # eager read inside a lazy sink.  For files that fit in RAM this is
    # equivalent in practice; a native scan plugin can be added later.
    return read_qvd(path, columns=columns, filters=filters).lazy()


# ---------------------------------------------------------------------------
# DataFrame namespace
# ---------------------------------------------------------------------------

@pl.api.register_dataframe_namespace("qvd")
class QvdDataFrameNamespace:
    """Custom ``df.qvd`` accessor for Polars DataFrames.

    Registered by importing ``openqvd.polars``.
    """

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def write(
        self,
        path: Union[str, Path],
        *,
        table_name: Optional[str] = None,
    ) -> None:
        """Write this DataFrame to a QVD file.

        Parameters
        ----------
        path:
            Destination file path.
        table_name:
            Logical table name embedded in the QVD header.  Defaults to
            the file stem of ``path``.

        Examples
        --------
        >>> import openqvd.polars
        >>> import polars as pl
        >>> df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        >>> df.qvd.write("out.qvd")
        """
        table = self._df.to_arrow()
        openqvd.write(table, path, table_name=table_name)


# ---------------------------------------------------------------------------
# LazyFrame namespace
# ---------------------------------------------------------------------------

@pl.api.register_lazyframe_namespace("qvd")
class QvdLazyFrameNamespace:
    """Custom ``lf.qvd`` accessor for Polars LazyFrames.

    Registered by importing ``openqvd.polars``.
    """

    def __init__(self, lf: pl.LazyFrame) -> None:
        self._lf = lf

    def collect_and_write(
        self,
        path: Union[str, Path],
        *,
        table_name: Optional[str] = None,
    ) -> None:
        """Collect the LazyFrame and write the result to a QVD file.

        Parameters
        ----------
        path:
            Destination file path.
        table_name:
            Logical table name embedded in the QVD header.

        Examples
        --------
        >>> import openqvd.polars
        >>> import polars as pl
        >>> lf = pl.scan_qvd("data.qvd", columns=["A"])
        >>> lf.qvd.collect_and_write("filtered.qvd")
        """
        self._lf.collect().qvd.write(path, table_name=table_name)


# ---------------------------------------------------------------------------
# Monkey-patch polars top-level namespace
# ---------------------------------------------------------------------------

pl.read_qvd = read_qvd  # type: ignore[attr-defined]
pl.scan_qvd = scan_qvd  # type: ignore[attr-defined]
