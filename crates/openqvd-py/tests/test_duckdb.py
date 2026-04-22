"""Tests for the DuckDB integration (openqvd.duckdb)."""

import os
import tempfile

import pyarrow as pa
import pytest

import openqvd
import openqvd.duckdb as qdb

duckdb = pytest.importorskip("duckdb")


class TestToRelation:
    """qdb.to_relation() tests."""

    def test_returns_relation(self, sample_qvd):
        con = duckdb.connect()
        rel = qdb.to_relation(sample_qvd, con)
        assert rel is not None
        # DuckDB relation has a fetchall / fetchdf interface
        rows = rel.fetchall()
        assert len(rows) == 4

    def test_creates_connection_when_none(self, sample_qvd):
        rel = qdb.to_relation(sample_qvd)
        assert rel is not None
        assert len(rel.fetchall()) == 4

    def test_view_name_registers_sql_view(self, sample_qvd):
        con = duckdb.connect()
        qdb.to_relation(sample_qvd, con, view_name="sales_reps")
        count = con.execute("SELECT COUNT(*) FROM sales_reps").fetchone()[0]
        assert count == 4

    def test_view_columns_match(self, sample_qvd):
        con = duckdb.connect()
        qdb.to_relation(sample_qvd, con, view_name="sr")
        cols = [row[0] for row in con.execute("DESCRIBE sr").fetchall()]
        assert "Sales Rep Name" in cols
        assert "SREP_ID" in cols


class TestRegister:
    """qdb.register() tests."""

    def test_register_makes_table_queryable(self, sample_qvd):
        con = duckdb.connect()
        qdb.register(con, "reps", sample_qvd)
        count = con.execute("SELECT COUNT(*) FROM reps").fetchone()[0]
        assert count == 4

    def test_register_filtered_query(self, sample_qvd):
        con = duckdb.connect()
        qdb.register(con, "reps", sample_qvd)
        rows = con.execute(
            "SELECT \"Sales Rep Name\" FROM reps WHERE SREP_ID = '11'"
        ).fetchall()
        assert len(rows) == 1

    def test_register_multiple_files(self, sample_qvd, multi_type_qvd):
        con = duckdb.connect()
        qdb.register(con, "reps", sample_qvd)
        qdb.register(con, "mixed", multi_type_qvd)
        r1 = con.execute("SELECT COUNT(*) FROM reps").fetchone()[0]
        r2 = con.execute("SELECT COUNT(*) FROM mixed").fetchone()[0]
        assert r1 == 4
        assert r2 == 5


class TestFromQuery:
    """qdb.from_query() tests."""

    def test_sql_string_writes_qvd(self, sample_qvd):
        con = duckdb.connect()
        qdb.register(con, "reps", sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            qdb.from_query('SELECT * FROM reps', out, con=con)
            result = openqvd.read(out)
            assert result.num_rows == 4
        finally:
            os.unlink(out)

    def test_sql_projection_writes_subset(self, sample_qvd):
        con = duckdb.connect()
        qdb.register(con, "reps", sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            qdb.from_query('SELECT "Sales Rep Name" FROM reps', out, con=con)
            result = openqvd.read(out)
            assert result.num_columns == 1
            assert result.schema.names == ["Sales Rep Name"]
        finally:
            os.unlink(out)

    def test_relation_input_writes_qvd(self, sample_qvd):
        con = duckdb.connect()
        rel = qdb.to_relation(sample_qvd, con)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            qdb.from_query(rel, out)
            result = openqvd.read(out)
            assert result.num_rows == 4
        finally:
            os.unlink(out)

    def test_table_name_embedded_in_header(self, sample_qvd):
        con = duckdb.connect()
        qdb.register(con, "reps", sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            qdb.from_query('SELECT * FROM reps', out, con=con, table_name="SalesReps")
            s = openqvd.schema(out)
            assert s.table_name == "SalesReps"
        finally:
            os.unlink(out)

    def test_sql_string_without_con_raises(self, sample_qvd):
        with pytest.raises(ValueError, match="con"):
            qdb.from_query("SELECT 1", "/tmp/noop.qvd")

    def test_invalid_source_type_raises(self, sample_qvd):
        with pytest.raises(TypeError):
            qdb.from_query(42, "/tmp/noop.qvd")

    def test_roundtrip_via_duckdb(self, sample_qvd):
        """Read QVD → register → filter via SQL → write QVD → re-read."""
        original = openqvd.read(sample_qvd)
        con = duckdb.connect()
        qdb.register(con, "reps", sample_qvd)
        with tempfile.NamedTemporaryFile(suffix=".qvd", delete=False) as tmp:
            out = tmp.name
        try:
            qdb.from_query("SELECT * FROM reps", out, con=con)
            roundtripped = openqvd.read(out)
            assert roundtripped.num_rows == original.num_rows
            assert roundtripped.schema.names == original.schema.names
        finally:
            os.unlink(out)
