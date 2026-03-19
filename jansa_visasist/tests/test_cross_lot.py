"""Unit tests for Step 4: cross-lot detection."""

import pandas as pd
import pytest

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.config_m2 import UNPARSEABLE_PREFIX
from jansa_visasist.pipeline.m2.cross_lot import detect_cross_lot


def _ctx():
    return Module2Context(output_dir="/tmp/test")


class TestCrossLot:
    def test_single_sheet_false(self):
        df = pd.DataFrame([
            {"doc_family_key": "FAM1", "source_sheet": "LOT 01"},
        ])
        df = detect_cross_lot(df, _ctx())
        assert df.iloc[0]["is_cross_lot"] == False  # noqa
        assert df.iloc[0]["cross_lot_sheets"] is None

    def test_multi_sheet_true(self):
        df = pd.DataFrame([
            {"doc_family_key": "FAM1", "source_sheet": "LOT 01"},
            {"doc_family_key": "FAM1", "source_sheet": "LOT 06"},
        ])
        df = detect_cross_lot(df, _ctx())
        assert df.iloc[0]["is_cross_lot"] == True  # noqa
        assert df.iloc[0]["cross_lot_sheets"] == ["LOT 01", "LOT 06"]

    def test_cross_lot_sheets_sorted(self):
        df = pd.DataFrame([
            {"doc_family_key": "FAM1", "source_sheet": "LOT 06"},
            {"doc_family_key": "FAM1", "source_sheet": "LOT 01"},
            {"doc_family_key": "FAM1", "source_sheet": "LOT 03"},
        ])
        df = detect_cross_lot(df, _ctx())
        assert df.iloc[0]["cross_lot_sheets"] == ["LOT 01", "LOT 03", "LOT 06"]

    def test_unparseable_always_false(self):
        df = pd.DataFrame([
            {"doc_family_key": f"{UNPARSEABLE_PREFIX}abc123", "source_sheet": "LOT 42"},
        ])
        df = detect_cross_lot(df, _ctx())
        assert df.iloc[0]["is_cross_lot"] == False  # noqa
        assert df.iloc[0]["cross_lot_sheets"] is None

    def test_cross_lot_sheets_never_empty_list(self):
        """GP2: cross_lot_sheets must be null, not [] when not cross-lot."""
        df = pd.DataFrame([
            {"doc_family_key": "FAM1", "source_sheet": "LOT 01"},
            {"doc_family_key": "FAM2", "source_sheet": "LOT 01"},
            {"doc_family_key": "FAM2", "source_sheet": "LOT 06"},
        ])
        df = detect_cross_lot(df, _ctx())
        # FAM1 is single-sheet
        fam1 = df[df["doc_family_key"] == "FAM1"]
        assert fam1.iloc[0]["cross_lot_sheets"] is None
        # Not an empty list
        assert fam1.iloc[0]["cross_lot_sheets"] != []

    def test_independent_families(self):
        df = pd.DataFrame([
            {"doc_family_key": "FAM1", "source_sheet": "LOT 01"},
            {"doc_family_key": "FAM2", "source_sheet": "LOT 06"},
        ])
        df = detect_cross_lot(df, _ctx())
        assert df.iloc[0]["is_cross_lot"] == False  # noqa
        assert df.iloc[1]["is_cross_lot"] == False  # noqa
