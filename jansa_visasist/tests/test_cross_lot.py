"""
Unit tests for M2 Step 4: Cross-Lot Detection.
Tests jansa_visasist.pipeline.m2.cross_lot.detect_cross_lot.
"""

import pytest
import pandas as pd

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.pipeline.m2.cross_lot import detect_cross_lot


@pytest.fixture
def ctx(tmp_path):
    return Module2Context(output_dir=str(tmp_path))


def _make_df(rows):
    base = {"doc_family_key": "FAM1", "source_sheet": "LOT_01"}
    records = []
    for i, overrides in enumerate(rows):
        row = dict(base)
        row.update(overrides)
        records.append(row)
    return pd.DataFrame(records)


class TestDetectCrossLot:
    """M2 Step 4: cross-lot detection."""

    def test_single_sheet(self, ctx):
        """Family in one sheet → is_cross_lot=False, cross_lot_sheets=None."""
        df = _make_df([
            {"doc_family_key": "FAM1", "source_sheet": "LOT_01"},
            {"doc_family_key": "FAM1", "source_sheet": "LOT_01"},
        ])
        result = detect_cross_lot(df, ctx)
        assert (result["is_cross_lot"] == False).all()
        assert result["cross_lot_sheets"].isna().all(), "GP2: must be None for non-cross-lot"

    def test_multiple_sheets(self, ctx):
        """Family in two sheets → is_cross_lot=True, cross_lot_sheets is list."""
        df = _make_df([
            {"doc_family_key": "FAM1", "source_sheet": "LOT_A"},
            {"doc_family_key": "FAM1", "source_sheet": "LOT_B"},
        ])
        result = detect_cross_lot(df, ctx)
        assert (result["is_cross_lot"] == True).all()
        for _, row in result.iterrows():
            sheets = row["cross_lot_sheets"]
            assert isinstance(sheets, list)
            assert "LOT_A" in sheets
            assert "LOT_B" in sheets

    def test_gp2_enforcement(self, ctx):
        """GP2: non-cross-lot rows MUST have cross_lot_sheets=None."""
        df = _make_df([
            {"doc_family_key": "FAM1", "source_sheet": "LOT_A"},
            {"doc_family_key": "FAM1", "source_sheet": "LOT_B"},
            {"doc_family_key": "FAM2", "source_sheet": "LOT_A"},
        ])
        result = detect_cross_lot(df, ctx)
        # FAM2 is single-sheet
        fam2 = result[result["doc_family_key"] == "FAM2"]
        assert (fam2["is_cross_lot"] == False).all()
        assert fam2["cross_lot_sheets"].isna().all()
