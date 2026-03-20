"""
Unit tests for M2 Step 5: Duplicate Detection.
Tests jansa_visasist.pipeline.m2.duplicate_detection.detect_duplicates.
"""

import pytest
import pandas as pd

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.pipeline.m2.duplicate_detection import detect_duplicates


@pytest.fixture
def ctx(tmp_path):
    return Module2Context(output_dir=str(tmp_path))


def _make_df(rows):
    """Build a DataFrame suitable for duplicate detection.
    Each row dict should have at minimum: doc_family_key, ind, source_sheet.
    """
    base = {
        "document": "DOC", "document_raw": "DOC",
        "doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
        "source_sheet": "LOT_01", "source_row": 1, "row_id": "1_1",
        "titre": "Test", "visa_global": None,
    }
    records = []
    for i, overrides in enumerate(rows):
        row = dict(base, row_id=f"1_{i+1}", source_row=i+1)
        row.update(overrides)
        records.append(row)
    return pd.DataFrame(records)


class TestDetectDuplicates:
    """M2 Step 5: duplicate detection."""

    def test_all_unique(self, ctx):
        """Single row per group → all UNIQUE."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "source_sheet": "LOT_01"},
            {"doc_family_key": "FAM2", "ind": "A", "source_sheet": "LOT_01"},
        ])
        result = detect_duplicates(df, ctx)
        assert (result["duplicate_flag"] == "UNIQUE").all()

    def test_exact_duplicates(self, ctx):
        """Two identical rows → first UNIQUE, second DUPLICATE."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "source_sheet": "LOT_01",
             "titre": "Same", "visa_global": "VAO"},
            {"doc_family_key": "FAM1", "ind": "A", "source_sheet": "LOT_01",
             "titre": "Same", "visa_global": "VAO"},
        ])
        result = detect_duplicates(df, ctx)
        flags = result["duplicate_flag"].tolist()
        assert flags[0] == "UNIQUE"
        assert flags[1] == "DUPLICATE"

    def test_suspect(self, ctx):
        """Same key but different field values → both SUSPECT."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "source_sheet": "LOT_01",
             "titre": "Version 1"},
            {"doc_family_key": "FAM1", "ind": "A", "source_sheet": "LOT_01",
             "titre": "Version 2"},
        ])
        result = detect_duplicates(df, ctx)
        assert (result["duplicate_flag"] == "SUSPECT").all()

    def test_excluded_columns(self, ctx):
        """row_id and source_row differences don't affect comparison."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "source_sheet": "LOT_01",
             "titre": "Same", "row_id": "1_1", "source_row": 1},
            {"doc_family_key": "FAM1", "ind": "A", "source_sheet": "LOT_01",
             "titre": "Same", "row_id": "1_2", "source_row": 2},
        ])
        result = detect_duplicates(df, ctx)
        # row_id and source_row are excluded from comparison, so these
        # should be DUPLICATE (all other fields match)
        flags = result["duplicate_flag"].tolist()
        assert "DUPLICATE" in flags, f"Expected DUPLICATE, got {flags}"
