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


def test_schema_follows_the_same_rules(text_in_date_column):
    with pytest.raises(openqvd.QvdConversionError):
        openqvd.schema(text_in_date_column)
    with pytest.warns(openqvd.QvdWarning):
        s = openqvd.schema(text_in_date_column, leniency=openqvd.Leniency.lenient())
    assert s.fields[0].number_format_type == "DATE"
    assert s.fields[0].dtype == "utf8"
    assert s.warnings[0].kind == "non_numeric_temporal"


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


def _retype(tmp_path, array, hint, name):
    src = tmp_path / f"{name}_src.qvd"
    openqvd.write(pa.table({"c": array}), src)
    p = tmp_path / f"{name}.qvd"
    p.write_bytes(
        src.read_bytes().replace(
            b"<Type>REAL</Type>", f"<Type>{hint}</Type>".encode()
        )
    )
    return p


def test_dtype_follows_the_data_and_reaches_arrow_metadata(tmp_path):
    whole = _retype(tmp_path, pa.array([45000.0, 45001.0]), "DATE", "whole")
    assert openqvd.schema(whole).fields[0].dtype == "date32"
    fractional = _retype(tmp_path, pa.array([45000.5]), "DATE", "frac")
    assert openqvd.schema(fractional).fields[0].dtype == "timestamp_us"
    t = openqvd.read(fractional).to_pyarrow()
    assert t.schema.field("c").type == pa.timestamp("us")
    assert t.schema.field("c").metadata == {b"qvd.number_format_type": b"DATE"}


def test_time_with_a_date_part_is_a_timestamp(tmp_path):
    # Time(Now()) keeps the date: 45000.75 days is not a time of day.
    p = _retype(tmp_path, pa.array([45000.75, 45001.25]), "TIME", "t1")
    assert openqvd.schema(p).fields[0].dtype == "timestamp_us"
    p2 = _retype(tmp_path, pa.array([0.25, 0.75]), "TIME", "t2")
    assert openqvd.schema(p2).fields[0].dtype == "duration_us"
