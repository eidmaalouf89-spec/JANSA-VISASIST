"""
Unit tests for M2 Step 3: Family Grouping & Chain Linking.
Tests jansa_visasist.pipeline.m2.chain_linking.link_chains.
"""

import pytest
import pandas as pd

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.pipeline.m2.chain_linking import link_chains


@pytest.fixture
def ctx(tmp_path):
    return Module2Context(output_dir=str(tmp_path))


def _make_df(rows):
    base = {
        "doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1,
        "source_sheet": "LOT_01", "source_row": 1, "row_id": "1_1",
    }
    records = []
    for i, overrides in enumerate(rows):
        row = dict(base, row_id=f"1_{i+1}", source_row=i+1)
        row.update(overrides)
        records.append(row)
    return pd.DataFrame(records)


class TestLinkChains:
    """M2 Step 3: chain linking."""

    def test_single_revision(self, ctx):
        """One row per family → is_latest=True, revision_count=1, no previous."""
        df = _make_df([{"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1}])
        result = link_chains(df, ctx)
        assert result.at[0, "is_latest"] == True
        assert result.at[0, "revision_count"] == 1
        assert result.at[0, "previous_version_key"] is None

    def test_two_revisions(self, ctx):
        """A then B → B is latest, A links to B, revision_count=2."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1},
            {"doc_family_key": "FAM1", "ind": "B", "ind_sort_order": 2},
        ])
        result = link_chains(df, ctx)
        result = result.sort_values("ind_sort_order").reset_index(drop=True)

        # Row A (sort_order=1): not latest
        assert result.at[0, "is_latest"] == False
        assert result.at[0, "revision_count"] == 2
        assert result.at[0, "previous_version_key"] is None  # first in chain

        # Row B (sort_order=2): latest
        assert result.at[1, "is_latest"] == True
        assert result.at[1, "revision_count"] == 2
        assert result.at[1, "previous_version_key"] is not None  # links to A

    def test_three_revisions_with_gap(self, ctx):
        """A, C (no B) → C is latest, A→C link, revision_count=2."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": "A", "ind_sort_order": 1},
            {"doc_family_key": "FAM1", "ind": "C", "ind_sort_order": 3},
        ])
        result = link_chains(df, ctx)
        result = result.sort_values("ind_sort_order").reset_index(drop=True)

        assert result.at[0, "is_latest"] == False
        assert result.at[1, "is_latest"] == True
        assert result.at[1, "previous_version_key"] is not None

    def test_null_ind_gets_processed(self, ctx):
        """Null IND (sort_order=0) still gets processed."""
        df = _make_df([
            {"doc_family_key": "FAM1", "ind": None, "ind_sort_order": 0},
        ])
        result = link_chains(df, ctx)
        assert "doc_version_key" in result.columns
        assert "is_latest" in result.columns
        assert result.at[0, "is_latest"] == True
