"""Writing: every accepted Arrow type is stored exactly; the rest raise."""

import datetime as dt
import decimal

import pyarrow as pa
import pytest

import openqvd


def roundtrip(tmp_path, array):
    p = tmp_path / "t.qvd"
    openqvd.write(pa.table({"c": array}), p)
    return openqvd.read(p).to_pyarrow().column("c")


def test_nanosecond_timestamps_stay_timestamps(tmp_path):
    ns = pa.array([1_700_000_000_000_000_000], pa.timestamp("ns"))
    out = roundtrip(tmp_path, ns)
    assert out.type == pa.timestamp("us")
    assert out.to_pylist() == [dt.datetime(2023, 11, 14, 22, 13, 20)]


def test_whole_second_timestamps_round_trip_exactly(tmp_path):
    base = 1_700_000_000_000_000
    values = [base + i * 1_000_000 for i in range(3000)]
    out = roundtrip(tmp_path, pa.array(values, pa.timestamp("us")))
    assert out.cast(pa.int64()).to_pylist() == values


def test_time_and_duration(tmp_path):
    out = roundtrip(tmp_path, pa.array([dt.time(12, 34, 56)], pa.time64("us")))
    assert out.type == pa.duration("us")
    assert out.cast(pa.int64()).to_pylist() == [45_296_000_000]
    out = roundtrip(tmp_path, pa.array([3600], pa.duration("s")))
    assert out.cast(pa.int64()).to_pylist() == [3_600_000_000]


def test_unsigned_and_large_integers(tmp_path):
    out = roundtrip(tmp_path, pa.array([3_000_000_000, 7], pa.uint32()))
    assert out.to_pylist() == [3_000_000_000.0, 7.0]
    out = roundtrip(tmp_path, pa.array([1, 5_000_000_000], pa.int64()))
    assert out.to_pylist() == [1.0, 5_000_000_000.0]
    for bad in (pa.array([2**53 + 1], pa.int64()), pa.array([2**64 - 1], pa.uint64())):
        with pytest.raises(openqvd.QvdConversionError, match="cannot be stored exactly"):
            openqvd.write(pa.table({"c": bad}), tmp_path / "bad.qvd")


def test_decimals(tmp_path):
    arr = pa.array([decimal.Decimal("123.45"), None], pa.decimal128(10, 2))
    assert roundtrip(tmp_path, arr).to_pylist() == [123.45, None]


def test_timezone_is_an_error_unless_allowed(tmp_path):
    arr = pa.array([1_000_000], pa.timestamp("us", tz="Europe/Paris"))
    with pytest.raises(openqvd.QvdConversionError, match="time zone"):
        openqvd.write(pa.table({"c": arr}), tmp_path / "tz.qvd")
    with pytest.warns(openqvd.QvdWarning, match="timezone_to_utc"):
        openqvd.write(
            pa.table({"c": arr}),
            tmp_path / "tz.qvd",
            leniency=["timezone_to_utc"],
        )
    out = openqvd.read(tmp_path / "tz.qvd").to_pyarrow().column("c")
    assert out.to_pylist() == [dt.datetime(1970, 1, 1, 0, 0, 1)]


def test_unmapped_type_is_an_error_not_text(tmp_path):
    with pytest.raises(openqvd.QvdConversionError, match="no QVD mapping"):
        openqvd.write(pa.table({"c": pa.array([b"ab"], pa.binary())}), tmp_path / "b.qvd")
    with pytest.raises(openqvd.QvdError):
        openqvd.write(pa.table({"c": pa.array([[1]], pa.list_(pa.int64()))}), tmp_path / "l.qvd")
    assert not (tmp_path / "b.qvd").exists()


def test_dictionary_is_unpacked(tmp_path):
    arr = pa.array(["a", "b", "a", None]).dictionary_encode()
    assert roundtrip(tmp_path, arr).to_pylist() == ["a", "b", "a", None]


def test_polars_datetime_ns_and_decimal(tmp_path):
    pl = pytest.importorskip("polars")
    df = pl.DataFrame(
        {
            "t": pl.Series([dt.datetime(2023, 11, 14, 22, 13, 20)], dtype=pl.Datetime("ns")),
            "d": pl.Series([decimal.Decimal("1.50")], dtype=pl.Decimal(10, 2)),
        }
    )
    p = tmp_path / "pl.qvd"
    openqvd.write(df, p)
    t = openqvd.read(p).to_pyarrow()
    assert t.column("t").to_pylist() == [dt.datetime(2023, 11, 14, 22, 13, 20)]
    assert t.column("d").to_pylist() == [1.5]
