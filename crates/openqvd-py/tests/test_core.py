"""Tests for the core openqvd.read / openqvd.schema / openqvd.write API."""

import tempfile
import os

import pyarrow as pa
import pytest

import openqvd


class TestRead:
    """openqvd.read() tests."""

    def test_basic_read(self, sample_qvd):
        tbl = openqvd.read(sample_qvd)
        assert isinstance(tbl, pa.Table)
        assert tbl.num_rows == 4
        assert tbl.num_columns == 2

    def test_column_names(self, sample_qvd):
        tbl = openqvd.read(sample_qvd)
        assert tbl.schema.names == ["Sales Rep Name", "SREP_ID"]

    def test_projection_pushdown(self, sample_qvd):
        tbl = openqvd.read(sample_qvd, columns=["Sales Rep Name"])
        assert tbl.num_columns == 1
        assert tbl.schema.names == ["Sales Rep Name"]
        assert tbl.num_rows == 4

    def test_projection_nonexistent_column_raises(self, sample_qvd):
        with pytest.raises(ValueError, match="not found"):
            openqvd.read(sample_qvd, columns=["NONEXISTENT"])

    def test_read_with_nulls(self, multi_type_qvd):
        tbl = openqvd.read(multi_type_qvd)
        assert tbl.num_rows == 5
        # The ID column has a NULL in the last row
        id_col = tbl.column("ID")
        assert id_col.null_count >= 1

    def test_read_nonexistent_file_raises(self):
        with pytest.raises(Exception):
            openqvd.read("/nonexistent/path/file.qvd")


class TestSchema:
    """openqvd.schema() tests."""

    def test_schema_basic(self, sample_qvd):
        s = openqvd.schema(sample_qvd)
        assert s.table_name == "SalesReps"
        assert s.num_rows == 4
        assert len(s.fields) == 2

    def test_schema_field_info(self, sample_qvd):
        s = openqvd.schema(sample_qvd)
        assert s.fields[0].name == "Sales Rep Name"
        assert s.fields[1].name == "SREP_ID"
        assert s.fields[0].n_symbols == 4
        assert s.fields[1].n_symbols == 4

    def test_schema_column_names(self, sample_qvd):
        s = openqvd.schema(sample_qvd)
        assert s.column_names() == ["Sales Rep Name", "SREP_ID"]

    def test_schema_repr(self, sample_qvd):
        s = openqvd.schema(sample_qvd)
        r = repr(s)
        assert "SalesReps" in r
        assert "Sales Rep Name" in r


class TestPredicatePushdown:
    """Tests for predicate/filter pushdown."""

    def test_filter_eq(self, sample_qvd):
        tbl = openqvd.read(
            sample_qvd,
            filters=[{"column": "Sales Rep Name", "op": "eq", "value": "Sam"}],
        )
        assert tbl.num_rows == 1
        assert tbl.column("Sales Rep Name").to_pylist() == ["Sam"]

    def test_filter_is_in(self, sample_qvd):
        tbl = openqvd.read(
            sample_qvd,
            filters=[
                {"column": "Sales Rep Name", "op": "is_in", "value": ["Sam", "Sue"]},
            ],
        )
        assert tbl.num_rows == 2
        names = tbl.column("Sales Rep Name").to_pylist()
        assert "Sam" in names
        assert "Sue" in names

    def test_filter_not_in(self, sample_qvd):
        tbl = openqvd.read(
            sample_qvd,
            filters=[
                {"column": "Sales Rep Name", "op": "not_in", "value": ["Sam"]},
            ],
        )
        assert tbl.num_rows == 3
        names = tbl.column("Sales Rep Name").to_pylist()
        assert "Sam" not in names

    def test_filter_combined_with_projection(self, sample_qvd):
        tbl = openqvd.read(
            sample_qvd,
            columns=["Sales Rep Name"],
            filters=[{"column": "SREP_ID", "op": "eq", "value": "11"}],
        )
        assert tbl.num_columns == 1
        assert tbl.num_rows == 1
        assert tbl.column("Sales Rep Name").to_pylist() == ["Sam"]

    def test_filter_no_match(self, sample_qvd):
        tbl = openqvd.read(
            sample_qvd,
            filters=[{"column": "Sales Rep Name", "op": "eq", "value": "NOBODY"}],
        )
        assert tbl.num_rows == 0

    def test_filter_invalid_op_raises(self, sample_qvd):
        with pytest.raises(ValueError, match="unknown filter op"):
            openqvd.read(
                sample_qvd,
                filters=[{"column": "Sales Rep Name", "op": "BADOP", "value": "x"}],
            )

    def test_filter_missing_column_raises(self, sample_qvd):
        with pytest.raises((ValueError, Exception)):
            openqvd.read(
                sample_qvd,
                filters=[{"column": "NONEXISTENT", "op": "eq", "value": "x"}],
            )


class TestWrite:
    """openqvd.write() round-trip tests."""

    def test_write_round_trip(self, sample_qvd):
        tbl = openqvd.read(sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            openqvd.write(tbl, out)
            tbl2 = openqvd.read(out)
            assert tbl2.num_rows == tbl.num_rows
            assert tbl2.num_columns == tbl.num_columns
            assert tbl2.schema.names == tbl.schema.names
        finally:
            os.unlink(out)

    def test_write_with_table_name(self, sample_qvd):
        tbl = openqvd.read(sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            openqvd.write(tbl, out, table_name="CustomName")
            s = openqvd.schema(out)
            assert s.table_name == "CustomName"
        finally:
            os.unlink(out)

    def test_write_preserves_values(self, sample_qvd):
        tbl = openqvd.read(sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            openqvd.write(tbl, out)
            tbl2 = openqvd.read(out)
            for col_name in tbl.schema.names:
                orig = tbl.column(col_name).to_pylist()
                written = tbl2.column(col_name).to_pylist()
                assert orig == written, f"column {col_name!r} mismatch"
        finally:
            os.unlink(out)

    def test_write_with_nulls(self, multi_type_qvd):
        tbl = openqvd.read(multi_type_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            openqvd.write(tbl, out)
            tbl2 = openqvd.read(out)
            assert tbl2.num_rows == tbl.num_rows
            # Check that NULLs are preserved
            for col_name in tbl.schema.names:
                orig = tbl.column(col_name).to_pylist()
                written = tbl2.column(col_name).to_pylist()
                assert orig == written, f"column {col_name!r} values differ"
        finally:
            os.unlink(out)
