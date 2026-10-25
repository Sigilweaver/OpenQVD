"""Lenient reading: errors by default, warnings when a check is allowed."""

import pyarrow as pa
import pytest

import openqvd


@pytest.fixture
def text_in_date_column(tmp_path):
    """A column typed DATE that holds text next to numbers."""
    src = tmp_path / "src.qvd"
    openqvd.write(pa.table({"d": ["N/A", "also text"]}), src)
    data = src.read_bytes()
    assert b"<Type>UNKNOWN</Type>" in data
    p = tmp_path / "date_text.qvd"
    p.write_bytes(data.replace(b"<Type>UNKNOWN</Type>", b"<Type>DATE</Type>"))
    return p


def test_strict_is_an_error(text_in_date_column):
    with pytest.raises(openqvd.QvdConversionError, match="non_numeric_temporal"):
        openqvd.read(text_in_date_column)


def test_lenient_returns_text_and_warns(text_in_date_column):
    with pytest.warns(openqvd.QvdWarning, match="non_numeric_temporal"):
        t = openqvd.read(text_in_date_column, leniency=openqvd.Leniency.lenient())
    assert t.to_pyarrow().column("d").to_pylist() == ["N/A", "also text"]
    assert len(t.warnings) == 1
    w = t.warnings[0]
    assert isinstance(w, openqvd.ReadWarning)
    assert (w.kind, w.column, w.row) == ("non_numeric_temporal", "d", None)
    assert "DATE column is returned as text" in w.detail


def test_named_check(text_in_date_column):
    with pytest.warns(openqvd.QvdWarning):
        openqvd.read(text_in_date_column, leniency=["non_numeric_temporal"])


def test_schema_does_not_decode_so_does_not_warn(text_in_date_column):
    s = openqvd.schema(text_in_date_column)
    assert s.warnings == []
    assert s.fields[0].number_format_type == "DATE"


def test_projecting_the_column_away_avoids_the_check(text_in_date_column, tmp_path):
    src = tmp_path / "two.qvd"
    openqvd.write(pa.table({"d": ["x"], "k": [1]}), src)
    data = src.read_bytes().replace(
        b"<Type>UNKNOWN</Type>", b"<Type>DATE</Type>", 1
    )
    p = tmp_path / "two_date.qvd"
    p.write_bytes(data)
    t = openqvd.read(p, columns=["k"])
    assert t.column_names == ["k"]
