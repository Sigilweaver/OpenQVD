"""Tests for the Polars integration (openqvd.polars)."""

import os
import tempfile

import polars as pl
import pytest

import openqvd.polars  # noqa: F401 - registers pl.read_qvd etc.


class TestPolarsRead:
    """pl.read_qvd() tests."""

    def test_read_qvd(self, sample_qvd):
        df = pl.read_qvd(sample_qvd)
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (4, 2)
        assert df.columns == ["Sales Rep Name", "SREP_ID"]

    def test_read_qvd_projection(self, sample_qvd):
        df = pl.read_qvd(sample_qvd, columns=["Sales Rep Name"])
        assert df.shape == (4, 1)
        assert df.columns == ["Sales Rep Name"]

    def test_read_qvd_filters(self, sample_qvd):
        df = pl.read_qvd(
            sample_qvd,
            filters=[{"column": "Sales Rep Name", "op": "eq", "value": "Sam"}],
        )
        assert df.shape == (1, 2)
        assert df["Sales Rep Name"].to_list() == ["Sam"]


class TestPolarsScan:
    """pl.scan_qvd() tests."""

    def test_scan_qvd(self, sample_qvd):
        lf = pl.scan_qvd(sample_qvd)
        assert isinstance(lf, pl.LazyFrame)
        df = lf.collect()
        assert df.shape == (4, 2)

    def test_scan_qvd_with_columns(self, sample_qvd):
        lf = pl.scan_qvd(sample_qvd, columns=["SREP_ID"])
        df = lf.collect()
        assert df.shape == (4, 1)
        assert df.columns == ["SREP_ID"]

    def test_scan_qvd_with_filters(self, sample_qvd):
        lf = pl.scan_qvd(
            sample_qvd,
            filters=[{"column": "SREP_ID", "op": "eq", "value": "11"}],
        )
        df = lf.collect()
        assert df.shape == (1, 2)


class TestPolarsNamespace:
    """df.qvd.write() and lf.qvd.collect_and_write() tests."""

    def test_dataframe_write(self, sample_qvd):
        df = pl.read_qvd(sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            df.qvd.write(out)
            df2 = pl.read_qvd(out)
            assert df2.shape == df.shape
            assert df2.columns == df.columns
        finally:
            os.unlink(out)

    def test_dataframe_write_with_table_name(self, sample_qvd):
        df = pl.read_qvd(sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            df.qvd.write(out, table_name="TestTable")
            import openqvd
            s = openqvd.schema(out)
            assert s.table_name == "TestTable"
        finally:
            os.unlink(out)

    def test_lazyframe_collect_and_write(self, sample_qvd):
        lf = pl.scan_qvd(sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            lf.qvd.collect_and_write(out)
            df = pl.read_qvd(out)
            assert df.shape == (4, 2)
        finally:
            os.unlink(out)
