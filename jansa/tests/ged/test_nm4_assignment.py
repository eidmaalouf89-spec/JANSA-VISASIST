"""Tests for NM4-GED: Assignment Classification & Circuit Mapping.

Updated for built-in circuit matrix integration.
"""

import pandas as pd
import pytest

from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.nm4_assignment import (
    classify_assignments,
    _classify_assignment,
    _check_keyword_activation,
    _resolve_final_status,
    KEYWORD_ACTIVATION,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(rows):
    """Build a minimal ged_long-like DataFrame from row dicts."""
    defaults = {
        'doc_id': 1, 'mission': '0-BET Structure', 'mission_type': 'REVIEWER',
        'response_status': 'NOT_RESPONDED', 'reponse_raw': None,
        'reponse_normalized': None, 'commentaire': None,
        'lot': 'I003', 'type_doc': 'NDC', 'batiment': 'GE',
        'specialite': 'GOE', 'repondant': 'Someone',
        'famille_key': 'P17_T2_GE_EXE_LGD_GOE_I003_NDC_TZ_TX_028000',
        'indice': 'A', 'indice_sort_order': 1, 'version_number': 1,
        'date_reponse': pd.NaT,
    }
    full_rows = []
    for r in rows:
        row = dict(defaults)
        row.update(r)
        full_rows.append(row)
    return pd.DataFrame(full_rows)


# ---------------------------------------------------------------------------
# Contract validation
# ---------------------------------------------------------------------------

class TestNM4Contract:
    def test_missing_columns_raises(self):
        df = pd.DataFrame({'doc_id': [1]})
        with pytest.raises(ContractError, match='NM4 input contract'):
            classify_assignments(df)

    def test_valid_contract_passes(self):
        df = _make_df([{}])
        ged, summary = classify_assignments(df)
        assert len(ged) > 0


# ---------------------------------------------------------------------------
# Assignment classification with built-in circuit matrix
# ---------------------------------------------------------------------------

class TestAssignmentClassification:
    def test_reviewer_matched_gets_required_visa(self):
        """BET Structure (TERRELL) + lot 03 + NDC → REQUIRED_VISA via matrix."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Structure',
            'lot': 'I003', 'type_doc': 'NDC',
        }])
        ged, summary = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA'
        assert ged.iloc[0]['assignment_source'] == 'MATRIX'

    def test_reviewer_informational_from_matrix(self):
        """ARCHITECTE (HARDEL) + lot 03 + COF → INFORMATIONAL via matrix."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-ARCHITECTE',
            'lot': 'I003', 'type_doc': 'COF',
        }])
        ged, summary = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'INFORMATIONAL'
        assert ged.iloc[0]['assignment_source'] == 'MATRIX'

    def test_reviewer_unmatched_mission_gets_unknown_required(self):
        """Mission not in reviewer dict → UNKNOWN_REQUIRED + DISCIPLINE_FALLBACK."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': 'Unknown Reviewer',
            'lot': 'I003', 'type_doc': 'NDC',
        }])
        ged, summary = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'UNKNOWN_REQUIRED'
        assert ged.iloc[0]['assignment_source'] == 'DISCIPLINE_FALLBACK'

    def test_reviewer_unmatched_lot_type_doc_gets_unknown_required(self):
        """Known reviewer but lot+type_doc not in matrix → UNKNOWN_REQUIRED."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Ascenseur',
            'lot': 'I004', 'type_doc': 'NDC',
        }])
        # ASCAUDIT for lot 04 NDC is not in circuit → UNKNOWN_REQUIRED
        ged, summary = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'UNKNOWN_REQUIRED'
        assert ged.iloc[0]['assignment_source'] == 'DISCIPLINE_FALLBACK'

    def test_sas_excluded(self):
        """R-NM4-07: SAS rows get NOT_ASSIGNED."""
        df = _make_df([{'mission_type': 'SAS', 'mission': '0-SAS'}])
        ged, summary = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'NOT_ASSIGNED'

    def test_moex_excluded(self):
        """R-NM4-07: MOEX rows get NOT_ASSIGNED."""
        df = _make_df([{'mission_type': 'MOEX', 'mission': "Maître d'Oeuvre EXE"}])
        ged, summary = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'NOT_ASSIGNED'

    def test_unknown_mission_type_excluded(self):
        """UNKNOWN mission_type → NOT_ASSIGNED."""
        df = _make_df([{'mission_type': 'UNKNOWN', 'mission': 'Something'}])
        ged, summary = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'NOT_ASSIGNED'

    def test_unknown_assignment_flag_on_fallback(self):
        """UNKNOWN_REQUIRED always carries UNKNOWN_ASSIGNMENT flag."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': 'Unknown Reviewer',
        }])
        ged, summary = classify_assignments(df)
        flags = summary.iloc[0]['inference_flags']
        assert 'UNKNOWN_ASSIGNMENT' in flags

    def test_matrix_match_no_unknown_assignment_flag(self):
        """REQUIRED_VISA from matrix should NOT carry UNKNOWN_ASSIGNMENT flag."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Structure',
            'lot': 'I003', 'type_doc': 'NDC',
        }])
        ged, summary = classify_assignments(df)
        flags = summary.iloc[0]['inference_flags']
        assert 'UNKNOWN_ASSIGNMENT' not in flags


# ---------------------------------------------------------------------------
# Circuit matrix lookup paths
# ---------------------------------------------------------------------------

class TestCircuitLookupPaths:
    def test_lot_wildcard_match(self):
        """Lot with '*' wildcard type_doc → matches any type_doc."""
        # Lot 09 has ('09', '*') → HARDEL: _I, LE_SOMMER: _V, SOCOTEC: _V, ...
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-ARCHITECTE',
            'lot': 'I009', 'type_doc': 'ANYTHING',
        }])
        ged, summary = classify_assignments(df)
        # HARDEL in ('09', '*') is INFORMATIONAL
        assert ged.iloc[0]['assignment_type'] == 'INFORMATIONAL'
        assert ged.iloc[0]['assignment_source'] == 'MATRIX'

    def test_parent_lot_fallback(self):
        """Sub-lot 12A → parent lot 12 lookup."""
        # Lot 12A → parent lot 12, ('12', '*') has HARDEL: _I
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-ARCHITECTE',
            'lot': 'A12A', 'type_doc': 'PLN',
        }])
        ged, summary = classify_assignments(df)
        # HARDEL in ('12', '*') = INFORMATIONAL
        assert ged.iloc[0]['assignment_type'] == 'INFORMATIONAL'
        assert ged.iloc[0]['assignment_source'] == 'MATRIX'

    def test_global_type_doc_match(self):
        """GLOBAL type_doc matches when lot doesn't have the type_doc."""
        # Lot 02 has no '*' wildcard and no ('02', 'NOT') entry,
        # so GLOBAL kicks in: ('GLOBAL', 'NOT') → LE_SOMMER: REQUIRED_VISA
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-AMO HQE',
            'lot': 'I002', 'type_doc': 'NOT',
        }])
        ged, summary = classify_assignments(df)
        # LE_SOMMER in ('GLOBAL', 'NOT') = REQUIRED_VISA
        assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA'
        assert ged.iloc[0]['assignment_source'] == 'MATRIX'

    def test_lot_alias_06B_to_09(self):
        """Lot B06B → lot_family '06B' → alias to '09' → match."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-Bureau de Contrôle',
            'lot': 'B06B', 'type_doc': 'NDC',
        }])
        ged, summary = classify_assignments(df)
        # SOCOTEC in ('09', '*') = REQUIRED_VISA
        assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA'
        assert ged.iloc[0]['assignment_source'] == 'MATRIX'

    def test_egis_fluides_multiple_missions(self):
        """BET CVC, BET Electricité, BET Plomberie, BET SPK all map to EGIS_FLUIDES."""
        for mission_role in ['BET CVC', 'BET Electricité', 'BET Plomberie', 'BET SPK']:
            df = _make_df([{
                'mission_type': 'REVIEWER', 'mission': f'0-{mission_role}',
                'lot': 'I041', 'type_doc': 'NDC',
            }])
            ged, _ = classify_assignments(df)
            assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA', \
                f'{mission_role} should map to REQUIRED_VISA via EGIS_FLUIDES'
            # Source may be MATRIX or DATA_OVERRIDE depending on override rules
            assert ged.iloc[0]['assignment_source'] in ('MATRIX', 'DATA_OVERRIDE')


# ---------------------------------------------------------------------------
# HM handling (R-NM4-06)
# ---------------------------------------------------------------------------

class TestHMHandling:
    def test_hm_excluded_from_relevant_reviewers(self):
        """R-NM4-06: HM reviewers removed from relevant_reviewers."""
        df = _make_df([
            {'doc_id': 1, 'mission': 'BET1', 'response_status': 'RESPONDED_HM'},
            {'doc_id': 1, 'mission': 'BET2', 'response_status': 'RESPONDED_APPROVE'},
        ])
        ged, summary = classify_assignments(df)
        row = summary[summary['doc_id'] == 1].iloc[0]
        assert 'BET1' in row['hm_reviewers']
        assert 'BET1' not in row['relevant_reviewers']
        assert 'BET2' in row['relevant_reviewers']

    def test_hm_not_counted_as_missing(self):
        """R-NM4-06: HM not counted as not_responded."""
        df = _make_df([
            {'doc_id': 1, 'mission': 'BET1', 'response_status': 'RESPONDED_HM'},
            {'doc_id': 1, 'mission': 'BET2', 'response_status': 'NOT_RESPONDED'},
        ])
        ged, summary = classify_assignments(df)
        row = summary[summary['doc_id'] == 1].iloc[0]
        assert row['not_responded'] == 1  # Only BET2
        assert row['hm_count'] == 1

    def test_all_hm_gives_empty_relevant(self):
        """Edge: All reviewers HM → relevant_reviewers = []."""
        df = _make_df([
            {'doc_id': 1, 'mission': 'BET1', 'response_status': 'RESPONDED_HM'},
            {'doc_id': 1, 'mission': 'BET2', 'response_status': 'RESPONDED_HM'},
        ])
        ged, summary = classify_assignments(df)
        row = summary[summary['doc_id'] == 1].iloc[0]
        assert row['relevant_reviewers'] == []
        assert row['hm_count'] == 2

    def test_hm_not_counted_as_blocking(self):
        """R-NM4-06: HM not counted as blocking."""
        df = _make_df([
            {'doc_id': 1, 'mission': 'BET1', 'response_status': 'RESPONDED_HM'},
        ])
        ged, summary = classify_assignments(df)
        row = summary[summary['doc_id'] == 1].iloc[0]
        assert row['responded_reject'] == 0
        assert row['blocking_reviewers'] == []


# ---------------------------------------------------------------------------
# UNKNOWN_REQUIRED cases
# ---------------------------------------------------------------------------

class TestUnknownRequired:
    def test_unknown_required_in_assigned_reviewers(self):
        """UNKNOWN_REQUIRED participates in consensus like REQUIRED_VISA."""
        df = _make_df([
            {'doc_id': 1, 'mission': 'UnmappedBET', 'response_status': 'NOT_RESPONDED'},
        ])
        ged, summary = classify_assignments(df)
        row = summary[summary['doc_id'] == 1].iloc[0]
        assert 'UnmappedBET' in row['assigned_reviewers']
        assert 'UnmappedBET' in row['relevant_reviewers']
        assert row['not_responded'] == 1

    def test_unknown_required_reject_is_blocking(self):
        """UNKNOWN_REQUIRED with RESPONDED_REJECT counts as blocking."""
        df = _make_df([
            {'doc_id': 1, 'mission': 'UnmappedBET', 'response_status': 'RESPONDED_REJECT'},
        ])
        ged, summary = classify_assignments(df)
        row = summary[summary['doc_id'] == 1].iloc[0]
        assert row['responded_reject'] == 1
        assert 'UnmappedBET' in row['blocking_reviewers']


# ---------------------------------------------------------------------------
# Final response status resolution
# ---------------------------------------------------------------------------

class TestFinalResponseStatus:
    def test_not_assigned_gives_not_applicable(self):
        assert _resolve_final_status('NOT_ASSIGNED', 'NOT_RESPONDED') == 'NOT_APPLICABLE'

    def test_informational_gives_not_applicable(self):
        """R-NM4-03: INFORMATIONAL → NOT_APPLICABLE."""
        assert _resolve_final_status('INFORMATIONAL', 'RESPONDED_APPROVE') == 'NOT_APPLICABLE'

    def test_conditional_not_responded_gives_not_triggered(self):
        """R-NM4-04: CONDITIONAL + NOT_RESPONDED → CONDITIONAL_NOT_TRIGGERED."""
        assert _resolve_final_status('CONDITIONAL', 'NOT_RESPONDED') == 'CONDITIONAL_NOT_TRIGGERED'

    def test_required_visa_passes_through(self):
        assert _resolve_final_status('REQUIRED_VISA', 'RESPONDED_APPROVE') == 'RESPONDED_APPROVE'
        assert _resolve_final_status('REQUIRED_VISA', 'RESPONDED_REJECT') == 'RESPONDED_REJECT'

    def test_unknown_required_passes_through(self):
        assert _resolve_final_status('UNKNOWN_REQUIRED', 'NOT_RESPONDED') == 'NOT_RESPONDED'


# ---------------------------------------------------------------------------
# Keyword activation
# ---------------------------------------------------------------------------

class TestKeywordActivation:
    def test_keyword_activates(self):
        assert _check_keyword_activation(
            ['Voir le rapport acoustique'], 'AVLS'
        ) is True

    def test_keyword_case_insensitive(self):
        assert _check_keyword_activation(
            ['rapport ACOUSTIQUE joint'], 'AVLS'
        ) is True

    def test_no_match_returns_false(self):
        assert _check_keyword_activation(
            ['nothing relevant here'], 'AVLS'
        ) is False

    def test_keyword_wrong_mission(self):
        """Keyword matches but target mission doesn't match."""
        assert _check_keyword_activation(
            ['acoustique'], 'SOCOTEC'
        ) is False

    def test_empty_comments(self):
        assert _check_keyword_activation([], 'AVLS') is False
        assert _check_keyword_activation([None, None], 'AVLS') is False


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_doc_with_zero_reviewer_rows(self):
        """Document with only SAS/MOEX rows → empty assigned_reviewers."""
        df = _make_df([
            {'doc_id': 1, 'mission': '0-SAS', 'mission_type': 'SAS'},
        ])
        ged, summary = classify_assignments(df)
        row = summary[summary['doc_id'] == 1].iloc[0]
        assert row['assigned_reviewers'] == []
        assert row['relevant_reviewers'] == []

    def test_multiple_docs(self):
        """Multiple documents each get their own summary."""
        df = _make_df([
            {'doc_id': 1, 'mission': 'BET1', 'response_status': 'RESPONDED_APPROVE'},
            {'doc_id': 2, 'mission': 'BET2', 'response_status': 'NOT_RESPONDED'},
        ])
        ged, summary = classify_assignments(df)
        assert len(summary) == 2
        assert set(summary['doc_id']) == {1, 2}

    def test_mixed_matrix_and_unknown(self):
        """Doc with both matrix-matched and unmatched reviewers."""
        df = _make_df([
            {'doc_id': 1, 'mission': '0-BET Structure', 'lot': 'I003',
             'type_doc': 'NDC', 'response_status': 'RESPONDED_APPROVE'},
            {'doc_id': 1, 'mission': 'UnmappedBET', 'lot': 'I003',
             'type_doc': 'NDC', 'response_status': 'NOT_RESPONDED'},
        ])
        ged, summary = classify_assignments(df)
        # BET Structure → REQUIRED_VISA (matrix), UnmappedBET → UNKNOWN_REQUIRED
        types = dict(zip(ged['mission'], ged['assignment_type']))
        assert types['0-BET Structure'] == 'REQUIRED_VISA'
        assert types['UnmappedBET'] == 'UNKNOWN_REQUIRED'

    def test_deterministic_output(self):
        """Same input → same output (determinism check)."""
        df = _make_df([
            {'doc_id': 1, 'mission': '0-BET Structure', 'lot': 'I003',
             'type_doc': 'NDC', 'response_status': 'RESPONDED_APPROVE'},
            {'doc_id': 1, 'mission': '0-Bureau de Contrôle', 'lot': 'I003',
             'type_doc': 'NDC', 'response_status': 'NOT_RESPONDED'},
        ])
        ged1, sum1 = classify_assignments(df)
        ged2, sum2 = classify_assignments(df)
        assert list(ged1['assignment_type']) == list(ged2['assignment_type'])
        assert list(ged1['assignment_source']) == list(ged2['assignment_source'])

    def test_building_prefix_stripping(self):
        """Different building prefixes for same lot family → same assignment."""
        for prefix in ['I', 'A', 'B', 'H']:
            df = _make_df([{
                'mission_type': 'REVIEWER', 'mission': '0-BET Structure',
                'lot': f'{prefix}003', 'type_doc': 'NDC',
            }])
            ged, _ = classify_assignments(df)
            assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA', \
                f'Prefix {prefix} should give same result'


# ---------------------------------------------------------------------------
# Project override rules
# ---------------------------------------------------------------------------

class TestProjectOverrides:
    def test_override_fires_for_unmatched_circuit(self):
        """MAT+lot_31: BET Acoustique not in circuit for MAT+31 → override fires."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Acoustique',
            'lot': 'I031', 'type_doc': 'MAT',
        }])
        ged, _ = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA'
        assert ged.iloc[0]['assignment_source'] == 'DATA_OVERRIDE'

    def test_override_lot_42_plomberie(self):
        """DET+lot_42: BET Plomberie → REQUIRED_VISA via override."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Plomberie',
            'lot': 'B042', 'type_doc': 'DET',
        }])
        ged, _ = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA'
        assert ged.iloc[0]['assignment_source'] == 'DATA_OVERRIDE'

    def test_override_lot_08_facade(self):
        """DET+lot_08: BET Façade → REQUIRED_VISA via override."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Façade',
            'lot': 'A008', 'type_doc': 'DET',
        }])
        ged, _ = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA'

    def test_override_sublot_12A(self):
        """MAT+lot_12A: BET Acoustique → REQUIRED_VISA via override."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Acoustique',
            'lot': 'B12A', 'type_doc': 'MAT',
        }])
        ged, _ = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA'

    def test_override_does_not_apply_to_non_reviewer(self):
        """MOEX is still NOT_ASSIGNED even if override exists for lot."""
        df = _make_df([{
            'mission_type': 'MOEX', 'mission': "0-Maître d'Oeuvre EXE",
            'lot': 'I031', 'type_doc': 'MAT',
        }])
        ged, _ = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'NOT_ASSIGNED'

    def test_override_no_flag_added(self):
        """Override matches should NOT add UNKNOWN_ASSIGNMENT flag."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Acoustique',
            'lot': 'I031', 'type_doc': 'MAT',
        }])
        ged, summary = classify_assignments(df)
        flags = summary.iloc[0]['inference_flags']
        assert 'UNKNOWN_ASSIGNMENT' not in flags

    def test_circuit_still_works_for_existing_entries(self):
        """Circuit matrix entries still resolve normally."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Structure',
            'lot': 'I003', 'type_doc': 'NDC',
        }])
        ged, _ = classify_assignments(df)
        assert ged.iloc[0]['assignment_type'] == 'REQUIRED_VISA'
        # This should come from MATRIX since ('03', 'NDC') has TERRELL
        assert ged.iloc[0]['assignment_source'] == 'MATRIX'

    def test_no_override_for_unmatched_combo(self):
        """A (type_doc, lot) not in overrides falls to circuit then fallback."""
        df = _make_df([{
            'mission_type': 'REVIEWER', 'mission': '0-BET Ascenseur',
            'lot': 'I004', 'type_doc': 'NDC',
        }])
        ged, _ = classify_assignments(df)
        # ASCAUDIT for lot 04 NDC: not in overrides, not in circuit
        assert ged.iloc[0]['assignment_type'] == 'UNKNOWN_REQUIRED'

    def test_deterministic_with_overrides(self):
        """Overrides produce deterministic output."""
        df = _make_df([
            {'doc_id': 1, 'mission': '0-BET Acoustique', 'lot': 'I031',
             'type_doc': 'MAT', 'response_status': 'RESPONDED_APPROVE'},
            {'doc_id': 1, 'mission': '0-BET Electricité', 'lot': 'I031',
             'type_doc': 'MAT', 'response_status': 'NOT_RESPONDED'},
        ])
        ged1, _ = classify_assignments(df)
        ged2, _ = classify_assignments(df)
        assert list(ged1['assignment_type']) == list(ged2['assignment_type'])
        assert list(ged1['assignment_source']) == list(ged2['assignment_source'])
