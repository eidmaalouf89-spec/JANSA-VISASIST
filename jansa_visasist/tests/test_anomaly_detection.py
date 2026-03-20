"""
Unit tests for M2 Step 6: Anomaly Detection.
Tests jansa_visasist.pipeline.m2.anomaly_detection.detect_anomalies.
"""

import pytest
import pandas as pd

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.pipeline.m2.anomaly_detection import detect_anomalies
from jansa_visasist.config_m2 import UNPARSEABLE_PREFIX


@pytest.fixture
def ctx(tmp_path):
    return Module2Context(output_dir=str(tmp_path))


def _make_df(rows):
    """Build a sorted DataFrame suitable for anomaly detection.
    Assumes chain_linking has already run (sorted by family/sheet/ind_sort_order).
    """
    base = {
        "doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
        "source_sheet": "LOT_01", "source_row": 1, "row_id": "1_1",
        "date_diffusion": None,
    }
    records = []
    for i, overrides in enumerate(rows):
        row = dict(base, row_id=f"1_{i+1}", source_row=i+1)
        row.update(overrides)
        records.append(row)
    df = pd.DataFrame(records)
    df = df.sort_values(["doc_family_key", "source_sheet", "ind_sort_order", "source_row"])
    df = df.reset_index(drop=True)
    return df


class TestDetectAnomalies:
    """M2 Step 6: anomaly detection."""

    def test_revision_gap(self, ctx):
        """Family with ind_sort_order [1, 3] (gap at 2) → REVISION_GAP anomaly."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1},
            {"doc_family_key": "FAM1", "ind": "C", "ind_sort_order": 3},
        ])
        detect_anomalies(df, ctx)
        gap_anomalies = [a for a in ctx.anomaly_log if a.anomaly_type == "REVISION_GAP"]
        assert len(gap_anomalies) == 1
        assert gap_anomalies[0].details["jump_size"] == 2

    def test_late_first_appearance(self, ctx):
        """Family starting at sort_order 4 → LATE_FIRST_APPEARANCE anomaly."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "D", "ind_sort_order": 4},
        ])
        detect_anomalies(df, ctx)
        late_anomalies = [a for a in ctx.anomaly_log if a.anomaly_type == "LATE_FIRST_APPEARANCE"]
        assert len(late_anomalies) == 1
        assert late_anomalies[0].details["first_sort_order"] == 4

    def test_date_regression(self, ctx):
        """Later revision with earlier date → DATE_REGRESSION anomaly."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "date_diffusion": "2024-06-01"},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "date_diffusion": "2024-05-01"},  # earlier date = regression
        ])
        detect_anomalies(df, ctx)
        reg_anomalies = [a for a in ctx.anomaly_log if a.anomaly_type == "DATE_REGRESSION"]
        assert len(reg_anomalies) == 1

    def test_no_anomalies_clean_chain(self, ctx):
        """Clean sequential chain (A→B, dates ascending) → no anomalies."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
             "date_diffusion": "2024-01-01"},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2,
             "date_diffusion": "2024-02-01"},
        ])
        detect_anomalies(df, ctx)
        assert len(ctx.anomaly_log) == 0

    def test_unparseable_excluded(self, ctx):
        """Families starting with UNPARSEABLE:: are skipped."""
        df = _make_df([
            {"doc_family_key": UNPARSEABLE_PREFIX + "abc123",
             "ind": None, "ind_sort_order": 0},
        ])
        detect_anomalies(df, ctx)
        assert len(ctx.anomaly_log) == 0
