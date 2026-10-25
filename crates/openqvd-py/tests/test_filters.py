"""Typed filter values. Generated files only."""

import pyarrow as pa
import pytest

import openqvd


@pytest.fixture
def qvd(tmp_path):
    p = tmp_path / "f.qvd"
    openqvd.write(
        pa.table(
            {
                "f": [1.0, 2.5, 1e20, float("nan"), None],
                "i": [1, 2, 3, 4, 5],
                "s": ["a", "b", "1", "a", None],
            }
        ),
        p,
    )
    return p


def col(qvd, filters, name="i"):
    return openqvd.read(qvd, filters=filters).to_pyarrow().column(name).to_pylist()


def test_float_value_matches_float_column(qvd):
    # "1.0" vs Rust's "1" used to match nothing, silently.
    assert col(qvd, [{"column": "f", "op": "eq", "value": 1.0}]) == [1]
    assert col(qvd, [{"column": "f", "op": "eq", "value": 1}]) == [1]
    assert col(qvd, [{"column": "f", "op": "eq", "value": 1e20}]) == [3]
    assert col(qvd, [{"column": "i", "op": "eq", "value": 2.0}]) == [2]


def test_nan_and_null_rows_are_not_lost(qvd):
    assert col(qvd, [{"column": "f", "op": "is_not_null"}]) == [1, 2, 3, 4]
    assert col(qvd, [{"column": "f", "op": "not_in", "value": [1.0]}]) == [2, 3, 4, 5]
    assert col(qvd, [{"column": "f", "op": "eq", "value": float("nan")}]) == [4]


def test_is_in_mixed_numbers(qvd):
    assert col(qvd, [{"column": "i", "op": "is_in", "value": [1, 3.0, 9]}]) == [1, 3]


def test_text_is_text(qvd):
    assert col(qvd, [{"column": "s", "op": "eq", "value": "1"}]) == [3]
    assert col(qvd, [{"column": "i", "op": "eq", "value": "1"}]) == []


def test_bad_values_raise(qvd):
    for bad in (True, None, [1], b"x", object()):
        with pytest.raises(TypeError):
            openqvd.read(qvd, filters=[{"column": "i", "op": "eq", "value": bad}])
    with pytest.raises(TypeError):
        openqvd.read(qvd, filters=[{"column": "s", "op": "is_in", "value": "ab"}])
    with pytest.raises(ValueError, match="64 bits"):
        openqvd.read(qvd, filters=[{"column": "i", "op": "eq", "value": 2**70}])


def test_undocumented_aliases_are_gone(qvd):
    for op in ("==", "in"):
        with pytest.raises(ValueError, match="unknown filter op"):
            openqvd.read(qvd, filters=[{"column": "i", "op": op, "value": 1}])
