"""The result of :func:`openqvd.read`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, Sequence

if TYPE_CHECKING:
    import pandas
    import polars
    import pyarrow

    from openqvd.options import ReadWarning

__all__ = ["Table"]


class Table:
    """A decoded QVD table.

    ``Table`` carries no pyarrow or polars type. It implements the
    `Arrow PyCapsule interface
    <https://arrow.apache.org/docs/format/CDataInterface/PyCapsules.html>`_
    (``__arrow_c_stream__``, ``__arrow_c_schema__``), so any library that
    consumes Arrow data accepts it directly, for example
    ``pyarrow.table(t)``, ``polars.DataFrame(t)`` or ``duckdb.from_arrow(t)``.
    The ``to_*`` helpers import their library only when called.

    Attributes
    ----------
    warnings:
        What a lenient read tolerated, as :class:`openqvd.ReadWarning`
        objects. Empty for a strict read.
    """

    __slots__ = ("_batches", "warnings")

    def __init__(self, batches: Any, warnings: Sequence["ReadWarning"] = ()) -> None:
        self._batches = batches
        self.warnings: List["ReadWarning"] = list(warnings)

    @property
    def num_rows(self) -> int:
        """Number of rows."""
        return self._batches.num_rows

    @property
    def column_names(self) -> List[str]:
        """Column names in file order."""
        return self._batches.column_names

    @property
    def num_columns(self) -> int:
        """Number of columns."""
        return len(self.column_names)

    def __len__(self) -> int:
        return self.num_rows

    def __repr__(self) -> str:
        return (
            f"Table(rows={self.num_rows}, columns={self.column_names!r}, "
            f"warnings={len(self.warnings)})"
        )

    def __arrow_c_stream__(self, requested_schema: Optional[Any] = None) -> Any:
        """Arrow C stream capsule. ``requested_schema`` is ignored."""
        return self._batches.__arrow_c_stream__(requested_schema)

    def __arrow_c_schema__(self) -> Any:
        """Arrow C schema capsule."""
        return self._batches.__arrow_c_schema__()

    def to_pyarrow(self) -> "pyarrow.Table":
        """Convert to a ``pyarrow.Table`` (requires pyarrow >= 15)."""
        import pyarrow as pa

        return pa.RecordBatchReader.from_stream(self).read_all()

    def to_polars(self) -> "polars.DataFrame":
        """Convert to a ``polars.DataFrame`` (requires polars)."""
        import polars as pl

        return pl.DataFrame(self)

    def to_pandas(self) -> "pandas.DataFrame":
        """Convert to a ``pandas.DataFrame`` (requires pandas and pyarrow)."""
        return self.to_pyarrow().to_pandas()
