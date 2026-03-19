"""Unit tests for Step 5: duplicate detection."""

import pandas as pd
import pytest

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.pipeline.m2.duplicate_detection import detect_duplicates


def _ctx():
    return Module2Context(output_dir="/tmp/test")


def _base_row(row_id, source_row, **overrides):
    row = {
        "doc_family_key": "FAM1", "ind_sort_order": 1,
        "document": "DOC1", "document_raw": "DOC1",
        "ind": "A", "ind_raw": "A",
        "source_sheet": "LOT 01", "source_row": source_row,
        "row_id": row_id, "row_quality": "OK",
        "row_quality_details": [], "visa_global": None,
        "visa_global_raw": None, "lot": "B001",
        "titre": "Title", "assigned_approvers": ["MOEX_GEMO"],
    }
    row.update(overrides)
    return row


class TestDuplicateDetection:
    def test_single_row_unique(self):
        df = pd.DataFrame([_base_row("0_10", 10)])
        df = detect_duplicates(df, _ctx())
        assert df.iloc[0]["duplicate_flag"] == "UNIQUE"

    def test_identical_rows_duplicate(self):
        df = pd.DataFrame([
            _base_row("0_10", 10),
            _base_row("0_11", 11),
        ])
        ctx = _ctx()
        df = detect_duplicates(df, ctx)
        assert df.iloc[0]["duplicate_flag"] == "UNIQUE"
        assert df.iloc[1]["duplicate_flag"] == "DUPLICATE"
        assert ctx.duplicate_exact_count == 1

    def test_differing_rows_suspect(self):
        df = pd.DataFrame([
            _base_row("0_10", 10, visa_global="VAO"),
            _base_row("0_11", 11, visa_global="REF"),
        ])
        ctx = _ctx()
        df = detect_duplicates(df, ctx)
        assert df.iloc[0]["duplicate_flag"] == "SUSPECT"
        assert df.iloc[1]["duplicate_flag"] == "SUSPECT"
        assert ctx.duplicate_suspect_count == 2

    def test_null_equals_null(self):
        """Two nulls in same column = equal (not a difference)."""
        df = pd.DataFrame([
            _base_row("0_10", 10, visa_global=None),
            _base_row("0_11", 11, visa_global=None),
        ])
        df = detect_duplicates(df, _ctx())
        assert df.iloc[0]["duplicate_flag"] == "UNIQUE"
        assert df.iloc[1]["duplicate_flag"] == "DUPLICATE"

    def test_raw_field_difference_suspect(self):
        """[V2] Rows differing only in _raw field -> SUSPECT."""
        df = pd.DataFrame([
            _base_row("0_10", 10, visa_global_raw="vao", visa_global="VAO"),
            _base_row("0_11", 11, visa_global_raw="VAO", visa_global="VAO"),
        ])
        df = detect_duplicates(df, _ctx())
        assert df.iloc[0]["duplicate_flag"] == "SUSPECT"
        assert df.iloc[1]["duplicate_flag"] == "SUSPECT"

    def test_row_quality_difference_suspect(self):
        """[V2] Rows differing only in row_quality -> SUSPECT."""
        df = pd.DataFrame([
            _base_row("0_10", 10, row_quality="OK"),
            _base_row("0_11", 11, row_quality="WARNING"),
        ])
        df = detect_duplicates(df, _ctx())
        assert df.iloc[0]["duplicate_flag"] == "SUSPECT"
        assert df.iloc[1]["duplicate_flag"] == "SUSPECT"

    def test_never_deletes_rows(self):
        df = pd.DataFrame([
            _base_row("0_10", 10),
            _base_row("0_11", 11),
        ])
        original_len = len(df)
        df = detect_duplicates(df, _ctx())
        assert len(df) == original_len

    def test_different_families_independent(self):
        df = pd.DataFrame([
            _base_row("0_10", 10, doc_family_key="FAM1"),
            _base_row("0_11", 11, doc_family_key="FAM2"),
        ])
        df = detect_duplicates(df, _ctx())
        assert df.iloc[0]["duplicate_flag"] == "UNIQUE"
        assert df.iloc[1]["duplicate_flag"] == "UNIQUE"

    def test_anomaly_logged_exact(self):
        df = pd.DataFrame([_base_row("0_10", 10), _base_row("0_11", 11)])
        ctx = _ctx()
        detect_duplicates(df, ctx)
        exact_logs = [a for a in ctx.anomaly_log if a.anomaly_type == "DUPLICATE_EXACT"]
        assert len(exact_logs) == 1

    def test_anomaly_logged_suspect(self):
        df = pd.DataFrame([
            _base_row("0_10", 10, titre="A"),
            _base_row("0_11", 11, titre="B"),
        ])
        ctx = _ctx()
        detect_duplicates(df, ctx)
        suspect_logs = [a for a in ctx.anomaly_log if a.anomaly_type == "DUPLICATE_SUSPECT"]
        assert len(suspect_logs) == 1
        assert "titre" in suspect_logs[0].details["differing_columns"]
