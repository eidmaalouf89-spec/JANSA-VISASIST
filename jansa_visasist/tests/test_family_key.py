"""
Unit tests for M2 Step 1: Family Key Construction.
Tests jansa_visasist.pipeline.m2.family_key.build_family_keys.
"""

import pytest
import pandas as pd

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.pipeline.m2.family_key import build_family_keys
from jansa_visasist.config_m2 import UNPARSEABLE_PREFIX


@pytest.fixture
def ctx(tmp_path):
    return Module2Context(output_dir=str(tmp_path))


def _make_df(rows):
    """Build a minimal DataFrame with required M2 columns."""
    base_cols = {"document": None, "document_raw": None,
                 "source_sheet": "LOT_01", "source_row": 1, "row_id": "1_1"}
    records = []
    for i, overrides in enumerate(rows):
        row = dict(base_cols, row_id=f"1_{i+1}", source_row=i+1)
        row.update(overrides)
        records.append(row)
    return pd.DataFrame(records)


class TestBuildFamilyKeys:
    """M2 Step 1: doc_family_key construction."""

    def test_primary_path(self, ctx):
        """Valid document → underscores stripped for family key."""
        df = _make_df([{"document": "P17_T2_IN_EXE_TEST"}])
        result = build_family_keys(df, ctx)
        assert result.at[0, "doc_family_key"] == "P17T2INEXETEST"

    def test_null_document_fallback(self, ctx):
        """Null document → UNPARSEABLE:: prefix with hash; anomaly logged."""
        df = _make_df([{"document": None, "document_raw": "bad_value"}])
        result = build_family_keys(df, ctx)
        key = result.at[0, "doc_family_key"]
        assert key.startswith(UNPARSEABLE_PREFIX), f"Expected UNPARSEABLE prefix, got {key!r}"
        assert len(ctx.anomaly_log) == 1
        assert ctx.anomaly_log[0].anomaly_type == "UNPARSEABLE_DOCUMENT"

    def test_unique_hashes_for_different_null_rows(self, ctx):
        """Two different null-document rows produce different keys."""
        df = _make_df([
            {"document": None, "document_raw": "x", "source_sheet": "LOT_A", "source_row": 1},
            {"document": None, "document_raw": "x", "source_sheet": "LOT_A", "source_row": 2},
        ])
        result = build_family_keys(df, ctx)
        key1 = result.at[0, "doc_family_key"]
        key2 = result.at[1, "doc_family_key"]
        assert key1 != key2, "Different null rows should produce unique hashes"

    def test_no_grouping_with_valid(self, ctx):
        """Unparseable keys never match valid family keys."""
        df = _make_df([
            {"document": "P17_T2_TEST"},
            {"document": None, "document_raw": None},
        ])
        result = build_family_keys(df, ctx)
        valid_key = result.at[0, "doc_family_key"]
        unparse_key = result.at[1, "doc_family_key"]
        assert valid_key != unparse_key
        assert not unparse_key.startswith(valid_key)
