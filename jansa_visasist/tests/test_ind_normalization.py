"""Unit tests for Step 2: ind_sort_order computation."""

import pandas as pd
import pytest

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.pipeline.m2.ind_normalization import compute_sort_orders, _alpha_to_sort_order


def _ctx():
    return Module2Context(output_dir="/tmp/test")


def _make_df(ind_values):
    rows = []
    for i, ind in enumerate(ind_values):
        rows.append({
            "ind": ind, "doc_family_key": f"FAM{i}",
            "source_sheet": "LOT 01", "source_row": 10 + i, "row_id": f"0_{10+i}",
            "document_raw": "DOC",
        })
    return pd.DataFrame(rows)


class TestAlphaConversion:
    """[SPEC] Alpha-to-number function."""
    def test_single_letters(self):
        assert _alpha_to_sort_order("A") == 1
        assert _alpha_to_sort_order("B") == 2
        assert _alpha_to_sort_order("Z") == 26

    def test_multi_letter(self):
        assert _alpha_to_sort_order("AA") == 27
        assert _alpha_to_sort_order("AB") == 28
        assert _alpha_to_sort_order("AZ") == 52

    def test_case_insensitive(self):
        assert _alpha_to_sort_order("a") == 1
        assert _alpha_to_sort_order("aa") == 27


class TestSortOrders:
    """[SPEC] Three defined cases."""
    def test_alpha_single(self):
        df = _make_df(["A", "B", "C"])
        df = compute_sort_orders(df, _ctx())
        assert list(df["ind_sort_order"]) == [1, 2, 3]

    def test_alpha_multi(self):
        df = _make_df(["Z", "AA"])
        df = compute_sort_orders(df, _ctx())
        assert list(df["ind_sort_order"]) == [26, 27]

    def test_numeric(self):
        df = _make_df(["1", "02", "10"])
        df = compute_sort_orders(df, _ctx())
        assert list(df["ind_sort_order"]) == [1, 2, 10]

    def test_null_ind(self):
        df = _make_df([None])
        ctx = _ctx()
        df = compute_sort_orders(df, ctx)
        assert df.iloc[0]["ind_sort_order"] == 0
        assert len(ctx.anomaly_log) == 1
        assert ctx.anomaly_log[0].anomaly_type == "MISSING_IND"

    def test_empty_string_ind(self):
        """Empty string after cleaning should also trigger MISSING_IND."""
        df = _make_df([""])
        # ind="" is already null from M1 per GP2, but test the safeguard
        df.at[0, "ind"] = ""
        ctx = _ctx()
        df = compute_sort_orders(df, ctx)
        assert df.iloc[0]["ind_sort_order"] == 0


class TestSafeguardFallback:
    """[SAFEGUARD] Unexpected format - not spec-defined."""
    def test_mixed_alphanumeric(self):
        df = _make_df(["1A"])
        ctx = _ctx()
        df = compute_sort_orders(df, ctx)
        assert df.iloc[0]["ind_sort_order"] == 0
