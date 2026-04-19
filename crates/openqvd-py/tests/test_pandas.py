"""Tests for Pandas integration (via PyArrow bridge)."""

import pytest

import openqvd


class TestPandas:
    """Pandas conversion tests."""

    def test_to_pandas(self, sample_qvd):
        import pandas as pd

        tbl = openqvd.read(sample_qvd)
        df = tbl.to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (4, 2)
        assert list(df.columns) == ["Sales Rep Name", "SREP_ID"]

    def test_pandas_values(self, sample_qvd):
        import pandas as pd

        df = openqvd.read(sample_qvd).to_pandas()
        assert list(df["Sales Rep Name"]) == ["Sam", "Sue", "Sal", "Jim"]

    def test_pandas_with_projection(self, sample_qvd):
        import pandas as pd

        df = openqvd.read(sample_qvd, columns=["SREP_ID"]).to_pandas()
        assert df.shape == (4, 1)
        assert list(df.columns) == ["SREP_ID"]

    def test_pandas_with_nulls(self, multi_type_qvd):
        import pandas as pd

        df = openqvd.read(multi_type_qvd).to_pandas()
        assert df.shape[0] == 5
        # ID column has a null
        assert df["ID"].isna().sum() >= 1
