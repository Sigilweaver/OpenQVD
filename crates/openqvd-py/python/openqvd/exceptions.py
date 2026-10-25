"""Exceptions and warnings raised by openqvd.

Everything derives from :class:`QvdError`, which is a :class:`ValueError`, so
``except ValueError`` keeps working. Errors that come from the operating
system are also :class:`OSError`. New subclasses may be added in minor
releases; catch :class:`QvdError` (or the group you care about) rather than
listing leaves.

::

    QvdError(ValueError)
    +-- QvdIOError(QvdError, OSError)
    |   +-- QvdFileNotFoundError(QvdIOError, FileNotFoundError)
    |   +-- QvdPermissionError(QvdIOError, PermissionError)
    +-- QvdFormatError              the bytes are not a valid QVD file
    |   +-- QvdHeaderError          XML header missing or malformed
    |   +-- QvdStructureError       binary layout contradicts the header
    |   +-- QvdSymbolError          a symbol table entry is malformed
    +-- QvdSchemaError              unknown column, mismatched table shape
    +-- QvdConversionError          value cannot be converted to/from QVD
    +-- QvdUnsupportedError         valid input using an unimplemented feature
"""

from __future__ import annotations

__all__ = [
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
]


class QvdError(ValueError):
    """Base class for every error raised by openqvd."""


class QvdIOError(QvdError, OSError):
    """Reading or writing a file failed at the operating-system level."""

    def __init__(self, *args: object) -> None:
        # Without this, CPython versions that find ValueError's __init__
        # first in the MRO leave errno and filename unset.
        OSError.__init__(self, *args)


class QvdFileNotFoundError(QvdIOError, FileNotFoundError):
    """The QVD file does not exist."""


class QvdPermissionError(QvdIOError, PermissionError):
    """The QVD file cannot be accessed with the current permissions."""


class QvdFormatError(QvdError):
    """The input is not a valid QVD file."""


class QvdHeaderError(QvdFormatError):
    """The XML table header is missing, unparsable or has bad values."""


class QvdStructureError(QvdFormatError):
    """The binary layout contradicts the header or the specification."""


class QvdSymbolError(QvdFormatError):
    """A symbol table entry is malformed."""


class QvdSchemaError(QvdError):
    """A requested column does not exist, or table shapes disagree."""


class QvdConversionError(QvdError):
    """A value cannot be converted to or from the requested type."""


class QvdUnsupportedError(QvdError):
    """The input uses a feature openqvd does not implement."""


class QvdWarning(UserWarning):
    """Issued for each deviation a lenient read tolerated."""
