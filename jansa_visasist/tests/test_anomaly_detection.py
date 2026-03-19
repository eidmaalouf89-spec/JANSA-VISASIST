"""Unit tests for Step 6: anomaly detection."""

import pandas as pd
import pytest

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.config_m2 import UNPARSEABLE_PREFIX
from jansa_visasist.pipeline.m2.anomaly_detection import detect_anomalies


def _ctx():
    return Module2Context(output_dir="/tmp/test")


def _make_df(rows):
    for r in rows:
        r.setdefault("duplicate_flag", "UNIQUE")
        r.setdefault("is_latest", False)
        r.setdefault("revision_count", 1)
        r.setdefault("doc_version_key", "")
        r.setdefault("previous_version_key", None)
        r.setdefault("is_cross_lot", False)
        r.setdefault("cross_lot_sheets", None)
    return pd.DataFrame(rows)


class TestRevisionGap:
    def test_gap_detected(self):
        """A(1), C(3) -> REVISION_GAP (jump=2)."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10",
             "date_diffusion": "2024-01-01"},
            {"doc_family_key": "FAM1", "ind": "C", "ind_sort_order": 3,
             "source_sheet": "LOT 01", "source_row": 12, "row_id": "0_12",
             "date_diffusion": "2024-03-01"},
        ])
        ctx = _ctx()
        detect_anomalies(df, ctx)
        gaps = [a for a in ctx.anomaly_log if a.anomaly_type == "REVISION_GAP"]
        assert len(gaps) == 1
        assert gaps[0].details["jump_size"] == 2

    def test_no_gap_consecutive(self):
        """A(1), B(2) -> no gap."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10",
             "date_diffusion": "2024-01-01"},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "source_sheet": "LOT 01", "source_row": 11, "row_id": "0_11",
             "date_diffusion": "2024-02-01"},
        ])
        ctx = _ctx()
        detect_anomalies(df, ctx)
        gaps = [a for a in ctx.anomaly_log if a.anomaly_type == "REVISION_GAP"]
        assert len(gaps) == 0


class TestLateFirst:
    def test_late_first_detected(self):
        """First IND is D (sort_order=4) -> LATE_FIRST_APPEARANCE."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "D", "ind_sort_order": 4,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10",
             "date_diffusion": "2024-01-01"},
        ])
        ctx = _ctx()
        detect_anomalies(df, ctx)
        lates = [a for a in ctx.anomaly_log if a.anomaly_type == "LATE_FIRST_APPEARANCE"]
        assert len(lates) == 1
        assert lates[0].details["first_sort_order"] == 4

    def test_no_late_first_starts_at_1(self):
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10",
             "date_diffusion": "2024-01-01"},
        ])
        ctx = _ctx()
        detect_anomalies(df, ctx)
        lates = [a for a in ctx.anomaly_log if a.anomaly_type == "LATE_FIRST_APPEARANCE"]
        assert len(lates) == 0


class TestDateRegression:
    def test_regression_detected(self):
        """Later revision has earlier date."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10",
             "date_diffusion": "2024-06-01"},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "source_sheet": "LOT 01", "source_row": 11, "row_id": "0_11",
             "date_diffusion": "2024-01-01"},
        ])
        ctx = _ctx()
        detect_anomalies(df, ctx)
        regs = [a for a in ctx.anomaly_log if a.anomaly_type == "DATE_REGRESSION"]
        assert len(regs) == 1

    def test_no_regression_ascending(self):
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10",
             "date_diffusion": "2024-01-01"},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "source_sheet": "LOT 01", "source_row": 11, "row_id": "0_11",
             "date_diffusion": "2024-06-01"},
        ])
        ctx = _ctx()
        detect_anomalies(df, ctx)
        regs = [a for a in ctx.anomaly_log if a.anomaly_type == "DATE_REGRESSION"]
        assert len(regs) == 0

    def test_null_dates_skipped(self):
        """Null dates are not compared."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10",
             "date_diffusion": None},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "source_sheet": "LOT 01", "source_row": 11, "row_id": "0_11",
             "date_diffusion": "2024-01-01"},
        ])
        ctx = _ctx()
        detect_anomalies(df, ctx)
        regs = [a for a in ctx.anomaly_log if a.anomaly_type == "DATE_REGRESSION"]
        assert len(regs) == 0


class TestUnparseableSkipped:
    def test_unparseable_no_anomalies(self):
        """UNPARSEABLE families are skipped in Step 6."""
        df = _make_df([
            {"doc_family_key": f"{UNPARSEABLE_PREFIX}abc123", "ind": None,
             "ind_sort_order": 0, "source_sheet": "LOT 42", "source_row": 244,
             "row_id": "24_244", "date_diffusion": None},
        ])
        ctx = _ctx()
        detect_anomalies(df, ctx)
        assert len(ctx.anomaly_log) == 0


class TestNoFalsePositives:
    def test_clean_data_no_anomalies(self):
        """Clean sequential data produces zero anomalies."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "source_sheet": "LOT 01", "source_row": 10, "row_id": "0_10",
             "date_diffusion": "2024-01-01"},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "source_sheet": "LOT 01", "source_row": 11, "row_id": "0_11",
             "date_diffusion": "2024-02-01"},
            {"doc_family_key": "FAM1", "ind": "C", "ind_sort_order": 3,
             "source_sheet": "LOT 01", "source_row": 12, "row_id": "0_12",
             "date_diffusion": "2024-03-01"},
        ])
        ctx = _ctx()
        detect_anomalies(df, ctx)
        assert len(ctx.anomaly_log) == 0
