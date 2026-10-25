"""Exception hierarchy and option plumbing. Uses only generated files, so it
runs without the QVD corpus."""

import warnings

import pyarrow as pa
import pytest

import openqvd
from openqvd import exceptions


@pytest.fixture
def good_qvd(tmp_path):
    p = tmp_path / "good.qvd"
    openqvd.write(pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}), p)
    return p


def test_hierarchy():
    assert issubclass(exceptions.QvdError, ValueError)
    assert issubclass(exceptions.QvdIOError, OSError)
    assert issubclass(exceptions.QvdIOError, exceptions.QvdError)
    assert issubclass(exceptions.QvdFileNotFoundError, FileNotFoundError)
    assert issubclass(exceptions.QvdPermissionError, PermissionError)
    for name in ("QvdHeaderError", "QvdStructureError", "QvdSymbolError"):
        assert issubclass(getattr(exceptions, name), exceptions.QvdFormatError)
    for name in (
        "QvdFormatError",
        "QvdSchemaError",
        "QvdConversionError",
        "QvdUnsupportedError",
    ):
        assert issubclass(getattr(exceptions, name), exceptions.QvdError)
    for name in exceptions.__all__:
        assert getattr(openqvd, name) is getattr(exceptions, name)


def test_missing_file_is_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError) as info:
        openqvd.read(tmp_path / "nope.qvd")
    assert isinstance(info.value, openqvd.QvdError)
    assert isinstance(info.value, openqvd.QvdFileNotFoundError)
    assert info.value.errno == 2
    with pytest.raises(openqvd.QvdFileNotFoundError):
        openqvd.schema(tmp_path / "nope.qvd")


def test_directory_is_io_error(tmp_path):
    with pytest.raises(openqvd.QvdIOError):
        openqvd.read(tmp_path)


def test_not_a_qvd_is_header_error(tmp_path):
    p = tmp_path / "bad.qvd"
    p.write_bytes(b"this is not a qvd file")
    with pytest.raises(openqvd.QvdHeaderError):
        openqvd.read(p)
    with pytest.raises(openqvd.QvdFormatError):
        openqvd.schema(p)


def test_truncated_file_is_structure_error(good_qvd, tmp_path):
    data = good_qvd.read_bytes()
    p = tmp_path / "trunc.qvd"
    p.write_bytes(data[:-2])
    with pytest.raises(openqvd.QvdStructureError):
        openqvd.read(p)


def test_unknown_symbol_type_is_symbol_error(tmp_path, good_qvd):
    data = bytearray(good_qvd.read_bytes())
    header_end = data.index(b"</QvdTableHeader>")
    body = data.index(b"\x00", header_end) + 1
    assert data[body] in (0x01, 0x02, 0x04, 0x05, 0x06)
    data[body] = 0x7F
    p = tmp_path / "sym.qvd"
    p.write_bytes(bytes(data))
    with pytest.raises(openqvd.QvdSymbolError):
        openqvd.read(p)


def test_unknown_column_is_schema_error(good_qvd):
    with pytest.raises(openqvd.QvdSchemaError, match="not found"):
        openqvd.read(good_qvd, columns=["missing"])
    with pytest.raises(ValueError):
        openqvd.read(good_qvd, columns=["missing"])


def test_unsupported_arrow_type_is_not_a_runtime_error(tmp_path):
    # Whatever the writer decides about this type, it must surface as an
    # openqvd error or succeed, never as a bare RuntimeError.
    t = pa.table({"x": pa.array([b"ab", b"cd"], type=pa.binary())})
    try:
        openqvd.write(t, tmp_path / "bin.qvd")
    except Exception as e:  # noqa: BLE001
        assert isinstance(e, openqvd.QvdError), type(e)


def test_mapping_is_by_kind_not_message(tmp_path):
    """Words the old substring matcher keyed on ("io error", "invalid", "xml",
    "structure") in a column name must not change the exception class."""
    p = tmp_path / "w.qvd"
    openqvd.write(pa.table({"a": [1]}), p)
    with pytest.raises(openqvd.QvdSchemaError) as info:
        openqvd.read(p, columns=["io error structure xml invalid"])
    assert type(info.value) is openqvd.QvdSchemaError


def test_leniency_basics():
    assert openqvd.Leniency.strict().checks == frozenset()
    assert not openqvd.Leniency.strict()
    assert openqvd.Leniency.lenient().checks == openqvd.Leniency.CHECKS
    assert openqvd.Leniency(["non_numeric_temporal"]) == openqvd.Leniency.lenient()
    with pytest.raises(ValueError, match="unknown lenient check"):
        openqvd.Leniency(["nope"])


def test_leniency_argument_forms(good_qvd):
    openqvd.read(good_qvd)
    openqvd.read(good_qvd, leniency=None)
    openqvd.read(good_qvd, leniency=openqvd.Leniency.lenient())
    openqvd.read(good_qvd, leniency=["non_numeric_temporal"])
    with pytest.raises(ValueError, match="unknown lenient check"):
        openqvd.read(good_qvd, leniency=["nope"])
    with pytest.raises(TypeError):
        openqvd.read(good_qvd, leniency="non_numeric_temporal")
    with pytest.raises(TypeError):
        openqvd.read(good_qvd, True)  # options are keyword-only


def test_strict_read_has_no_warnings(good_qvd):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        openqvd.read(good_qvd)
        s = openqvd.schema(good_qvd)
    assert s.warnings == []


def test_schema_does_not_decode_symbols(good_qvd, tmp_path):
    """schema() is metadata only: corrupting a symbol table must not matter."""
    data = bytearray(good_qvd.read_bytes())
    header_end = data.index(b"</QvdTableHeader>")
    body = data.index(b"\x00", header_end) + 1
    data[body] = 0x7F
    p = tmp_path / "sym.qvd"
    p.write_bytes(bytes(data))
    s = openqvd.schema(p)
    assert s.num_rows == 3
    assert s.column_names == ["a", "b"]
