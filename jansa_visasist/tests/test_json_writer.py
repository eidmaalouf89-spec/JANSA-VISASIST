"""Tests for _convert_for_json and JSON serialization safety.

Covers every type that can appear in a DataFrame cell:
scalars, None, NaN, Timestamp, containers (list/dict/tuple/ndarray),
and nested combinations.
"""

import json
import math
import os
import tempfile

import numpy as np
import pandas as pd
import pytest

from jansa_visasist.outputs.json_writer import (
    _convert_for_json,
    write_master_dataset_json,
)


# ── _convert_for_json unit tests ──────────────────────────────


class TestConvertForJson:
    """Exhaustive type coverage for the JSON converter."""

    # --- Scalars ---

    def test_string(self):
        assert _convert_for_json("hello") == "hello"

    def test_empty_string(self):
        assert _convert_for_json("") == ""

    def test_int(self):
        assert _convert_for_json(42) == 42

    def test_float(self):
        assert _convert_for_json(3.14) == 3.14

    def test_bool(self):
        assert _convert_for_json(True) is True
        assert _convert_for_json(False) is False

    # --- None / Missing ---

    def test_none(self):
        assert _convert_for_json(None) is None

    def test_float_nan(self):
        assert _convert_for_json(float("nan")) is None

    def test_float_inf(self):
        assert _convert_for_json(float("inf")) is None
        assert _convert_for_json(float("-inf")) is None

    def test_numpy_nan(self):
        assert _convert_for_json(np.nan) is None

    def test_pandas_na(self):
        assert _convert_for_json(pd.NA) is None

    def test_pandas_nat(self):
        assert _convert_for_json(pd.NaT) is None

    # --- Timestamps ---

    def test_pandas_timestamp(self):
        ts = pd.Timestamp("2024-03-15 10:30:00")
        result = _convert_for_json(ts)
        assert isinstance(result, str)
        assert result.startswith("2024-03-15")

    def test_pandas_nat_timestamp(self):
        """pd.NaT is a valid Timestamp-like; must become None."""
        assert _convert_for_json(pd.NaT) is None

    # --- numpy scalars ---

    def test_numpy_int64(self):
        result = _convert_for_json(np.int64(7))
        assert result == 7
        assert isinstance(result, int)

    def test_numpy_float64(self):
        result = _convert_for_json(np.float64(2.5))
        assert result == 2.5
        assert isinstance(result, float)

    def test_numpy_float64_nan(self):
        assert _convert_for_json(np.float64("nan")) is None

    def test_numpy_bool(self):
        result = _convert_for_json(np.bool_(True))
        assert result is True

    # --- Containers (the crash scenario) ---

    def test_list_passthrough(self):
        result = _convert_for_json(["a", "b", "c"])
        assert result == ["a", "b", "c"]

    def test_empty_list(self):
        assert _convert_for_json([]) == []

    def test_tuple_to_list(self):
        result = _convert_for_json(("x", "y"))
        assert result == ["x", "y"]

    def test_dict_passthrough(self):
        result = _convert_for_json({"key": "val"})
        assert result == {"key": "val"}

    def test_numpy_array(self):
        arr = np.array([1, 2, 3])
        result = _convert_for_json(arr)
        assert result == [1, 2, 3]

    def test_numpy_array_with_nan(self):
        arr = np.array([1.0, np.nan, 3.0])
        result = _convert_for_json(arr)
        assert result == [1.0, None, 3.0]

    # --- Nested containers ---

    def test_list_containing_nan(self):
        result = _convert_for_json([1, float("nan"), "ok"])
        assert result == [1, None, "ok"]

    def test_dict_containing_nan(self):
        result = _convert_for_json({"a": 1, "b": float("nan")})
        assert result == {"a": 1, "b": None}

    def test_nested_list_dict(self):
        obj = {"items": [1, None, {"sub": np.float64("nan")}]}
        result = _convert_for_json(obj)
        assert result == {"items": [1, None, {"sub": None}]}

    def test_list_with_timestamp(self):
        ts = pd.Timestamp("2024-01-01")
        result = _convert_for_json([ts, None, "x"])
        assert isinstance(result[0], str)
        assert result[1] is None
        assert result[2] == "x"

    # --- The exact crash scenario: pd.isna on a list ──

    def test_list_does_not_trigger_pd_isna(self):
        """This was the original crash: pd.isna([]) raises ValueError."""
        # Must not raise
        result = _convert_for_json([])
        assert result == []

    def test_numpy_empty_array_does_not_crash(self):
        """np.array([]) also makes pd.isna ambiguous."""
        result = _convert_for_json(np.array([]))
        assert result == []


# ── Round-trip through JSON serialization ──────────────────────


class TestJsonRoundTrip:
    """Verify that _convert_for_json output is actually JSON-serializable."""

    @pytest.mark.parametrize("value", [
        None,
        "text",
        42,
        3.14,
        True,
        float("nan"),
        pd.Timestamp("2024-06-01"),
        pd.NaT,
        pd.NA,
        np.int64(99),
        np.float64("nan"),
        np.array([1, 2]),
        ["a", "b"],
        {"k": "v"},
        (1, 2, 3),
        {"nested": [1, float("nan"), {"deep": np.float64(0.5)}]},
    ])
    def test_serializable(self, value):
        """Every converted value must survive json.dumps without error."""
        converted = _convert_for_json(value)
        # Must not raise
        result = json.dumps(converted)
        assert isinstance(result, str)


# ── Full write_master_dataset_json with container columns ──────


class TestWriteMasterDatasetJson:
    """Integration: write a DataFrame with list/dict columns to JSON."""

    def test_dataframe_with_list_column(self):
        """DataFrame containing list-valued cells serializes cleanly."""
        df = pd.DataFrame({
            "row_id": ["0_10", "0_11"],
            "row_quality": ["OK", "WARNING"],
            "row_quality_details": [[], ["unparseable_document"]],
            "assigned_approvers": [
                ["MOEX_GEMO", "ARCHI_MOX"],
                ["MOEX_GEMO"],
            ],
            "document": ["P17_T2_DOC_001", None],
            "some_float": [1.5, float("nan")],
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_master_dataset_json(df, tmpdir)
            assert os.path.exists(path)

            with open(path, encoding="utf-8") as f:
                data = json.load(f)

            assert len(data) == 2

            # List columns survived
            assert data[0]["assigned_approvers"] == ["MOEX_GEMO", "ARCHI_MOX"]
            assert data[0]["row_quality_details"] == []
            assert data[1]["row_quality_details"] == ["unparseable_document"]

            # NaN → None
            assert data[1]["some_float"] is None

            # None preserved
            assert data[1]["document"] is None

    def test_dataframe_with_numpy_array_column(self):
        """numpy array cells convert to lists."""
        df = pd.DataFrame({
            "values": [np.array([10, 20]), np.array([30])],
        })

        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_master_dataset_json(df, tmpdir)

            with open(path, encoding="utf-8") as f:
                data = json.load(f)

            assert data[0]["values"] == [10, 20]
            assert data[1]["values"] == [30]
