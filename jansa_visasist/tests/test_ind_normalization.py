"""
Unit tests for M2 Step 2: Revision Index Normalization.
Tests jansa_visasist.pipeline.m2.ind_normalization.
"""

import pytest
import pandas as pd

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.pipeline.m2.ind_normalization import (
    _alpha_to_sort_order,
    compute_sort_orders,
)


@pytest.fixture
def ctx(tmp_path):
    return Module2Context(output_dir=str(tmp_path))


class TestAlphaToSortOrder:
    """_alpha_to_sort_order conversion."""

    @pytest.mark.parametrize("alpha,expected", [
        ("A", 1), ("B", 2), ("Z", 26),
        ("AA", 27), ("AB", 28),
    ])
    def test_alpha_conversion(self, alpha, expected):
        assert _alpha_to_sort_order(alpha) == expected

    def test_lowercase_treated_as_uppercase(self):
        assert _alpha_to_sort_order("a") == 1
        assert _alpha_to_sort_order("aa") == 27


class TestComputeSortOrders:
    """compute_sort_orders on DataFrame."""

    def _make_df(self, inds):
        return pd.DataFrame({
            "ind": inds,
            "document_raw": ["raw"] * len(inds),
            "doc_family_key": ["FAM"] * len(inds),
            "source_sheet": ["LOT_01"] * len(inds),
            "source_row": list(range(1, len(inds) + 1)),
            "row_id": [f"1_{i}" for i in range(1, len(inds) + 1)],
        })

    def test_numeric_ind(self, ctx):
        df = self._make_df(["1", "5"])
        result = compute_sort_orders(df, ctx)
        assert result.at[0, "ind_sort_order"] == 1
        assert result.at[1, "ind_sort_order"] == 5

    def test_alpha_ind(self, ctx):
        df = self._make_df(["A", "C"])
        result = compute_sort_orders(df, ctx)
        assert result.at[0, "ind_sort_order"] == 1
        assert result.at[1, "ind_sort_order"] == 3

    def test_null_ind(self, ctx):
        """Null IND → sort_order=0, MISSING_IND anomaly logged."""
        df = self._make_df([None])
        result = compute_sort_orders(df, ctx)
        assert result.at[0, "ind_sort_order"] == 0
        assert len(ctx.anomaly_log) == 1
        assert ctx.anomaly_log[0].anomaly_type == "MISSING_IND"

    def test_empty_string(self, ctx):
        """Empty string IND → sort_order=0."""
        df = self._make_df([""])
        result = compute_sort_orders(df, ctx)
        assert result.at[0, "ind_sort_order"] == 0
