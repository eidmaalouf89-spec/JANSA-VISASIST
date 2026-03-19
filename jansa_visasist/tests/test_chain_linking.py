"""Unit tests for Step 3: chain linking."""

import pandas as pd
import pytest

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.config_m2 import NULL_IND_LABEL
from jansa_visasist.pipeline.m2.chain_linking import link_chains


def _ctx():
    return Module2Context(output_dir="/tmp/test")


def _make_df(rows):
    for r in rows:
        r.setdefault("duplicate_flag", "UNIQUE")
    return pd.DataFrame(rows)


class TestChainLinking:
    def test_abc_chain(self):
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10"},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "source_sheet": "LOT 01", "source_row": 11, "row_id": "0_11"},
            {"doc_family_key": "FAM1", "ind": "C", "ind_sort_order": 3,
             "source_sheet": "LOT 01", "source_row": 12, "row_id": "0_12"},
        ])
        df = link_chains(df, _ctx())

        assert df.iloc[0]["previous_version_key"] is None
        assert df.iloc[0]["is_latest"] == False  # noqa
        assert "FAM1::A::LOT 01" in df.iloc[1]["previous_version_key"]
        assert "FAM1::B::LOT 01" in df.iloc[2]["previous_version_key"]
        assert df.iloc[2]["is_latest"] == True  # noqa
        assert df.iloc[0]["revision_count"] == 3

    def test_gap_ac_links_directly(self):
        """A->C with B missing: chain links A->C directly."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10"},
            {"doc_family_key": "FAM1", "ind": "C", "ind_sort_order": 3,
             "source_sheet": "LOT 01", "source_row": 12, "row_id": "0_12"},
        ])
        df = link_chains(df, _ctx())
        # C's previous should be A's version key
        assert "FAM1::A::LOT 01" in df.iloc[1]["previous_version_key"]
        assert df.iloc[1]["is_latest"] == True  # noqa

    def test_single_revision(self):
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10"},
        ])
        df = link_chains(df, _ctx())
        assert df.iloc[0]["is_latest"] == True  # noqa
        assert df.iloc[0]["previous_version_key"] is None
        assert df.iloc[0]["revision_count"] == 1

    def test_null_ind_uses_label(self):
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": None, "ind_sort_order": 0,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10"},
        ])
        df = link_chains(df, _ctx())
        assert f"::{NULL_IND_LABEL}::" in df.iloc[0]["doc_version_key"]
        assert "::None::" not in df.iloc[0]["doc_version_key"]
        assert "::::" not in df.iloc[0]["doc_version_key"]

    def test_duplicates_share_version_key(self):
        """[V2] Duplicate rows share same doc_version_key."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10",
             "duplicate_flag": "UNIQUE"},
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 11, "row_id": "0_11",
             "duplicate_flag": "DUPLICATE"},
        ])
        df = link_chains(df, _ctx())
        assert df.iloc[0]["doc_version_key"] == df.iloc[1]["doc_version_key"]
        # row_id still distinguishes them
        assert df.iloc[0]["row_id"] != df.iloc[1]["row_id"]

    def test_version_key_format(self):
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10"},
        ])
        df = link_chains(df, _ctx())
        assert df.iloc[0]["doc_version_key"] == "FAM1::A::LOT 01"

    def test_is_latest_on_all_duplicates_at_max(self):
        """All rows at highest sort order get is_latest=true."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10"},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "source_sheet": "LOT 01", "source_row": 11, "row_id": "0_11"},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "source_sheet": "LOT 01", "source_row": 12, "row_id": "0_12"},
        ])
        df = link_chains(df, _ctx())
        assert df.iloc[0]["is_latest"] == False  # noqa
        assert df.iloc[1]["is_latest"] == True  # noqa
        assert df.iloc[2]["is_latest"] == True  # noqa
