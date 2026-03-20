"""
Unit tests for JSON output writer.
Tests jansa_visasist.outputs.json_writer._convert_for_json.
"""

import math
import pytest
import numpy as np
import pandas as pd

from jansa_visasist.outputs.json_writer import _convert_for_json


class TestConvertForJson:
    """_convert_for_json type handling."""

    def test_none_passthrough(self):
        assert _convert_for_json(None) is None

    def test_np_nan_to_none(self):
        assert _convert_for_json(np.nan) is None

    def test_np_int64_to_int(self):
        result = _convert_for_json(np.int64(42))
        assert result == 42
        assert isinstance(result, int)

    def test_np_float64_to_float(self):
        result = _convert_for_json(np.float64(3.14))
        assert result == 3.14
        assert isinstance(result, float)

    def test_float_nan_to_none(self):
        assert _convert_for_json(float('nan')) is None

    def test_float_inf_to_none(self):
        assert _convert_for_json(float('inf')) is None

    def test_list_containing_nan(self):
        result = _convert_for_json([1, float('nan'), 3])
        assert result == [1, None, 3]

    def test_dict_with_nan_values(self):
        result = _convert_for_json({"a": 1, "b": float('nan')})
        assert result == {"a": 1, "b": None}

    def test_pd_nat_to_none(self):
        assert _convert_for_json(pd.NaT) is None

    def test_regular_string_passthrough(self):
        assert _convert_for_json("hello") == "hello"

    def test_regular_int_passthrough(self):
        assert _convert_for_json(42) == 42

    def test_regular_bool_passthrough(self):
        assert _convert_for_json(True) is True

    def test_np_bool_to_bool(self):
        result = _convert_for_json(np.bool_(True))
        assert result is True

    def test_nested_dict_with_nan(self):
        result = _convert_for_json({"outer": {"inner": float('nan')}})
        assert result == {"outer": {"inner": None}}

    def test_pd_timestamp_to_iso(self):
        ts = pd.Timestamp("2024-06-15")
        result = _convert_for_json(ts)
        assert "2024-06-15" in result

    def test_np_ndarray_to_list(self):
        arr = np.array([1, 2, 3])
        result = _convert_for_json(arr)
        assert result == [1, 2, 3]
