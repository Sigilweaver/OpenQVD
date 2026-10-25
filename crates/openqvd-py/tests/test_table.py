"""openqvd.Table and the Arrow PyCapsule interface. Generated data only."""

import subprocess
import sys

import pyarrow as pa
import pytest

import openqvd


@pytest.fixture
def qvd_path(tmp_path):
    p = tmp_path / "t.qvd"
    openqvd.write(
        pa.table({"i": [1, 2, None], "s": ["a", "b", "c"], "f": [1.5, 2.5, 3.5]}), p
    )
    return p


def test_read_returns_table(qvd_path):
    t = openqvd.read(qvd_path)
    assert isinstance(t, openqvd.Table)
    assert t.num_rows == 3 and len(t) == 3
    assert t.num_columns == 3
    assert t.column_names == ["i", "s", "f"]
    assert t.warnings == []
    assert "rows=3" in repr(t)


def test_to_pyarrow(qvd_path):
    t = openqvd.read(qvd_path).to_pyarrow()
    assert isinstance(t, pa.Table)
    assert t.column("i").to_pylist() == [1, 2, None]
    assert t.column("s").to_pylist() == ["a", "b", "c"]


def test_pyarrow_consumes_capsule_directly(qvd_path):
    t = openqvd.read(qvd_path)
    assert pa.table(t).num_rows == 3
    assert pa.RecordBatchReader.from_stream(t).read_all().num_columns == 3
    assert pa.schema(t).names == ["i", "s", "f"]


def test_stream_can_be_consumed_more_than_once(qvd_path):
    t = openqvd.read(qvd_path)
    assert pa.table(t).equals(pa.table(t))


def test_polars(qvd_path):
    pl = pytest.importorskip("polars")
    t = openqvd.read(qvd_path)
    df = t.to_polars()
    assert isinstance(df, pl.DataFrame)
    assert df["s"].to_list() == ["a", "b", "c"]
    assert pl.DataFrame(t).shape == (3, 3)


def test_pandas(qvd_path):
    pytest.importorskip("pandas")
    assert list(openqvd.read(qvd_path).to_pandas()["s"]) == ["a", "b", "c"]


def test_duckdb(qvd_path):
    duckdb = pytest.importorskip("duckdb")
    t = openqvd.read(qvd_path)
    assert duckdb.from_arrow(t).count("*").fetchall()[0][0] == 3


def test_write_accepts_any_capsule_producer(tmp_path):
    tbl = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    outs = {
        "table": tbl,
        "batch": tbl.to_batches()[0],
        "reader": pa.RecordBatchReader.from_batches(tbl.schema, tbl.to_batches()),
        "openqvd": None,
    }
    for name, data in outs.items():
        p = tmp_path / f"{name}.qvd"
        if name == "openqvd":
            openqvd.write(tbl, tmp_path / "src.qvd")
            data = openqvd.read(tmp_path / "src.qvd")
        openqvd.write(data, p)
        assert openqvd.read(p).to_pyarrow().column("y").to_pylist() == ["a", "b", "c"]


def test_write_polars_dataframe(tmp_path):
    pl = pytest.importorskip("polars")
    p = tmp_path / "pl.qvd"
    openqvd.write(pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}), p)
    assert openqvd.read(p).to_pyarrow().column("x").to_pylist() == [1, 2]


def test_every_batch_is_written(tmp_path):
    """A multi-batch input must not lose rows after the first batch."""
    b1 = pa.RecordBatch.from_pydict({"x": [1, 2], "y": ["a", "b"]})
    b2 = pa.RecordBatch.from_pydict({"x": [3, 4, 5], "y": ["c", "d", "e"]})
    reader = pa.RecordBatchReader.from_batches(b1.schema, [b1, b2])
    p = tmp_path / "multi.qvd"
    openqvd.write(reader, p)
    t = openqvd.read(p).to_pyarrow()
    assert t.column("x").to_pylist() == [1, 2, 3, 4, 5]
    assert t.column("y").to_pylist() == ["a", "b", "c", "d", "e"]
    chunked = pa.Table.from_batches([b1, b2])
    assert chunked.column("x").num_chunks == 2
    p2 = tmp_path / "chunked.qvd"
    openqvd.write(chunked, p2)
    assert openqvd.read(p2).num_rows == 5


def test_write_rejects_non_arrow_objects(tmp_path):
    with pytest.raises(TypeError, match="__arrow_c_stream__"):
        openqvd.write({"x": [1]}, tmp_path / "bad.qvd")


def test_import_does_not_import_pyarrow_or_polars():
    code = (
        "import sys, openqvd, openqvd.exceptions, openqvd.options;"
        "assert 'pyarrow' not in sys.modules;"
        "assert 'polars' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_read_with_filters_and_columns(qvd_path):
    t = openqvd.read(
        qvd_path,
        columns=["s"],
        filters=[{"column": "i", "op": "is_not_null"}],
    )
    assert t.column_names == ["s"]
    assert t.to_pyarrow().column("s").to_pylist() == ["a", "b"]
