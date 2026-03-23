"""Tests for NM5-GED: Revision Linking & Active Dataset."""

import pandas as pd
import pytest

from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.nm5_revision import compute_active_dataset


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_doc_df(rows):
    """Build a minimal ged_long-like DataFrame for NM5 testing."""
    defaults = {
        'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A',
        'indice_sort_order': 1, 'version_number': 1,
        'lot': 'I003', 'batiment': 'GE',
        'mission': 'BET1', 'mission_type': 'REVIEWER',
        'response_status': 'NOT_RESPONDED',
        'assignment_type': 'UNKNOWN_REQUIRED',
        'final_response_status': 'NOT_RESPONDED',
        'date_depot': pd.NaT,
        'doc_version_key': None,
    }
    full_rows = []
    for r in rows:
        row = dict(defaults)
        row.update(r)
        # Auto-generate doc_version_key if not provided
        if row['doc_version_key'] is None:
            row['doc_version_key'] = f"{row['famille_key']}::{row['indice']}::{row['version_number']}"
        full_rows.append(row)
    return pd.DataFrame(full_rows)


# ---------------------------------------------------------------------------
# Contract validation
# ---------------------------------------------------------------------------

class TestNM5Contract:
    def test_missing_columns_raises(self):
        df = pd.DataFrame({'doc_id': [1]})
        with pytest.raises(ContractError, match='NM5 input contract'):
            compute_active_dataset(df)

    def test_valid_contract_passes(self):
        df = _make_doc_df([{}])
        enriched, doc_level, active = compute_active_dataset(df)
        assert len(enriched) > 0


# ---------------------------------------------------------------------------
# Revision chain logic
# ---------------------------------------------------------------------------

class TestRevisionChain:
    def test_single_doc_is_active(self):
        """famille_key with one doc → is_active=True, revision_count=1."""
        df = _make_doc_df([{'doc_id': 1}])
        enriched, doc_level, active = compute_active_dataset(df)
        row = doc_level[doc_level['doc_id'] == 1].iloc[0]
        assert bool(row['is_active']) is True
        assert row['revision_count'] == 1
        assert pd.isna(row['previous_indice']) or row['previous_indice'] is None

    def test_two_revisions_latest_is_active(self):
        """Two INDICE values → only latest is active."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'B', 'indice_sort_order': 2},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        assert doc_level[doc_level['doc_id'] == 1].iloc[0]['is_active'] == False
        assert doc_level[doc_level['doc_id'] == 2].iloc[0]['is_active'] == True

    def test_version_within_indice(self):
        """Multiple versions of same INDICE → only latest version is active."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1, 'version_number': 1},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1, 'version_number': 2},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        assert doc_level[doc_level['doc_id'] == 1].iloc[0]['is_active'] == False
        assert doc_level[doc_level['doc_id'] == 2].iloc[0]['is_active'] == True

    def test_revision_gap_detected(self):
        """A, C (B missing) → has_revision_gap=True."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'C', 'indice_sort_order': 3},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        assert doc_level[doc_level['doc_id'] == 1].iloc[0]['has_revision_gap'] == True
        assert doc_level[doc_level['doc_id'] == 2].iloc[0]['has_revision_gap'] == True

    def test_no_revision_gap_contiguous(self):
        """A, B → no gap."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'B', 'indice_sort_order': 2},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        assert doc_level[doc_level['doc_id'] == 1].iloc[0]['has_revision_gap'] == False

    def test_previous_indice_computed(self):
        """Previous indice correctly tracked in chain."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'B', 'indice_sort_order': 2},
            {'doc_id': 3, 'famille_key': 'FK_A', 'indice': 'C', 'indice_sort_order': 3},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        doc_level_sorted = doc_level.sort_values('indice_sort_order')
        assert pd.isna(doc_level_sorted.iloc[0]['previous_indice']) or doc_level_sorted.iloc[0]['previous_indice'] is None
        assert doc_level_sorted.iloc[1]['previous_indice'] == 'A'
        assert doc_level_sorted.iloc[2]['previous_indice'] == 'B'


# ---------------------------------------------------------------------------
# Active dataset uniqueness key: (famille_key, lot, batiment)
# ---------------------------------------------------------------------------

class TestActiveDatasetUniqueness:
    def test_same_fk_different_batiment_both_active(self):
        """[V1.1-P4] Same famille_key, different batiment → both active."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1, 'batiment': 'GE'},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1, 'batiment': 'BX'},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        assert doc_level[doc_level['doc_id'] == 1].iloc[0]['is_active'] == True
        assert doc_level[doc_level['doc_id'] == 2].iloc[0]['is_active'] == True

    def test_same_fk_same_batiment_only_latest_active(self):
        """Same (famille_key, lot, batiment) → only latest INDICE active."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1, 'batiment': 'GE'},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'B', 'indice_sort_order': 2, 'batiment': 'GE'},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        assert doc_level[doc_level['doc_id'] == 1].iloc[0]['is_active'] == False
        assert doc_level[doc_level['doc_id'] == 2].iloc[0]['is_active'] == True


# ---------------------------------------------------------------------------
# Cross-lot detection
# ---------------------------------------------------------------------------

class TestCrossLot:
    def test_cross_lot_detected(self):
        """Same famille_key in different lots → is_cross_lot=True."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'lot': 'I003'},
            {'doc_id': 2, 'famille_key': 'FK_A', 'lot': 'B041'},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        assert doc_level[doc_level['doc_id'] == 1].iloc[0]['is_cross_lot'] == True

    def test_single_lot_not_cross(self):
        """Single lot → is_cross_lot=False."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'lot': 'I003'},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        assert doc_level.iloc[0]['is_cross_lot'] == False


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_null_indice_included_if_only_doc(self):
        """R-NM5-05: indice_sort_order=0 included if only doc in famille_key."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': '', 'indice_sort_order': 0},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        assert doc_level.iloc[0]['is_active'] == True

    def test_active_dataset_filters_correctly(self):
        """Active dataset only contains rows for active documents."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1},
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1, 'mission': 'BET2'},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'B', 'indice_sort_order': 2},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        # doc_id 2 is the only active doc
        assert set(active['doc_id'].unique()) == {2}
        # doc_id 1 had 2 rows — they're excluded from active
        assert len(active[active['doc_id'] == 1]) == 0

    def test_revision_count_only_latest_versions(self):
        """R-NM5-03: revision_count only counts INDICE where is_latest_version."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1, 'version_number': 1},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1, 'version_number': 2},
            {'doc_id': 3, 'famille_key': 'FK_A', 'indice': 'B', 'indice_sort_order': 2, 'version_number': 1},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        # Latest versions: doc 2 (A v2) and doc 3 (B v1) → 2 distinct INDICE
        assert doc_level.iloc[0]['revision_count'] == 2

    def test_exact_duplicate_flagged(self):
        """R-NM5-04: Same doc_version_key → EXACT_DUPLICATE flag."""
        df = _make_doc_df([
            {'doc_id': 1, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1,
             'version_number': 1, 'doc_version_key': 'FK_A::A::1'},
            {'doc_id': 2, 'famille_key': 'FK_A', 'indice': 'A', 'indice_sort_order': 1,
             'version_number': 1, 'doc_version_key': 'FK_A::A::1'},
        ])
        enriched, doc_level, active = compute_active_dataset(df)
        for _, row in doc_level.iterrows():
            assert 'EXACT_DUPLICATE' in row['anomaly_flags']
