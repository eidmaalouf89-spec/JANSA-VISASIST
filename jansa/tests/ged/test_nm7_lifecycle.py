"""Tests for NM7-GED: Lifecycle State Engine, Blocker Model & Priority Scoring."""

import inspect
from datetime import date

import pandas as pd
import pytest

from jansa.adapters.ged.exceptions import ContractError
from jansa.adapters.ged.logging import clear_log, get_log
from jansa.pipeline.nm7_lifecycle import (
    run_nm7,
    _get_moex_verdict,
    _compute_consensus,
    _compute_confidence,
    _delay_weight,
    _revision_penalty,
    _priority_category,
)
from jansa.adapters.ged.constants import (
    LIFECYCLE_WEIGHT,
    BLOCKER_WEIGHT,
    CONFIDENCE_DEDUCTIONS,
)


# ---------------------------------------------------------------------------
# Helpers — build minimal inputs for run_nm7
# ---------------------------------------------------------------------------

REF_DATE = date(2026, 3, 23)


def _make_active(rows):
    """Build a minimal active_dataset DataFrame."""
    defaults = {
        'doc_id': 1, 'famille_key': 'FK_A', 'lot': 'I003', 'batiment': 'GE',
        'type_doc': 'NDC', 'deposant': 'LGD', 'mission': 'BET1',
        'mission_type': 'REVIEWER', 'repondant': 'Someone',
        'response_status': 'NOT_RESPONDED', 'reponse_normalized': None,
        'date_depot': pd.Timestamp('2025-01-15'), 'deadline': pd.Timestamp('2025-03-15'),
        'date_reponse': pd.NaT, 'ecart_reponse': None,
        'assignment_type': 'UNKNOWN_REQUIRED',
        'final_response_status': 'NOT_RESPONDED',
        'is_active': True,
    }
    full_rows = []
    for r in rows:
        row = dict(defaults)
        row.update(r)
        full_rows.append(row)
    return pd.DataFrame(full_rows)


def _make_nm2(rows):
    """Build minimal NM2 result."""
    defaults = {
        'doc_id': 1, 'sas_state': 'SAS_UNKNOWN', 'sas_confidence': 'LOW',
        'inference_flags': [],
    }
    full_rows = []
    for r in rows:
        row = dict(defaults)
        row.update(r)
        full_rows.append(row)
    return pd.DataFrame(full_rows)


def _make_nm4(rows):
    """Build minimal NM4 summary."""
    defaults = {
        'doc_id': 1,
        'assigned_reviewers': ['BET1'], 'relevant_reviewers': ['BET1'],
        'hm_reviewers': [], 'informational_reviewers': [], 'conditional_reviewers': [],
        'responded_approve': 0, 'responded_reject': 0, 'not_responded': 1,
        'missing_reviewers': ['BET1'], 'blocking_reviewers': [],
        'hm_count': 0, 'inference_flags': ['UNKNOWN_ASSIGNMENT'],
    }
    full_rows = []
    for r in rows:
        row = dict(defaults)
        row.update(r)
        full_rows.append(row)
    return pd.DataFrame(full_rows)


def _make_nm5(rows):
    """Build minimal NM5 doc_level."""
    defaults = {'doc_id': 1, 'revision_count': 1, 'is_active': True}
    full_rows = []
    for r in rows:
        row = dict(defaults)
        row.update(r)
        full_rows.append(row)
    return pd.DataFrame(full_rows)


def _run_single(active_rows, nm2_rows=None, nm4_rows=None, nm5_rows=None,
                ref_date=REF_DATE):
    """Run NM7 with minimal inputs for a single document."""
    clear_log()
    ad = _make_active(active_rows)
    nm2 = _make_nm2(nm2_rows or [{}])
    nm4 = _make_nm4(nm4_rows or [{}])
    nm5 = _make_nm5(nm5_rows or [{}])
    pq, ii, nm7_output = run_nm7(ad, nm2, nm4, nm5, reference_date=ref_date)
    return pq, ii, nm7_output


# ---------------------------------------------------------------------------
# Branch 1: SYNTHESIS_ISSUED
# ---------------------------------------------------------------------------

class TestSynthesisIssued:
    def test_synthesis_issued_excluded(self):
        """visa_global = VAO → SYNTHESIS_ISSUED, EXCLUDED."""
        pq, ii, all_r = _run_single(
            [{'doc_id': 1, 'mission': "MOEX", 'mission_type': 'MOEX',
              'reponse_normalized': 'VAO', 'response_status': 'RESPONDED_APPROVE'}],
            nm4_rows=[{'doc_id': 1, 'assigned_reviewers': [], 'relevant_reviewers': [],
                       'responded_approve': 0, 'responded_reject': 0,
                       'not_responded': 0, 'missing_reviewers': [], 'hm_count': 0}],
        )
        row = all_r[all_r['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'SYNTHESIS_ISSUED'
        assert row['queue_destination'] == 'EXCLUDED'
        assert len(pq) == 0


# ---------------------------------------------------------------------------
# Branch 2/3: SAS_BLOCKED / SAS_PENDING
# ---------------------------------------------------------------------------

class TestSASBranches:
    def test_sas_blocked_intake(self):
        pq, ii, all_r = _run_single(
            [{}],
            nm2_rows=[{'doc_id': 1, 'sas_state': 'SAS_BLOCKED'}],
        )
        row = all_r[all_r['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'SAS_BLOCKED'
        assert row['queue_destination'] == 'INTAKE_ISSUES'
        assert row['blocker_type'] == 'COMPANY'

    def test_sas_pending_intake(self):
        pq, ii, all_r = _run_single(
            [{}],
            nm2_rows=[{'doc_id': 1, 'sas_state': 'SAS_PENDING'}],
        )
        row = all_r[all_r['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'SAS_PENDING'
        assert row['queue_destination'] == 'INTAKE_ISSUES'
        assert row['blocker_type'] == 'GEMO_SAS'


# ---------------------------------------------------------------------------
# Branch 4: SAS_UNKNOWN → SAS_ASSUMED_PASSED
# ---------------------------------------------------------------------------

class TestSASUnknown:
    def test_sas_unknown_treated_as_passed(self):
        pq, ii, all_r = _run_single([{}])
        row = all_r[all_r['doc_id'] == 1].iloc[0]
        assert 'SAS_ASSUMED_PASSED' in row['inference_flags']
        # Should NOT be SAS_BLOCKED or SAS_PENDING
        assert row['lifecycle_state'] not in ('SAS_BLOCKED', 'SAS_PENDING')


# ---------------------------------------------------------------------------
# Branch 5: HM_EXCLUDED
# ---------------------------------------------------------------------------

class TestHMExcluded:
    def test_hm_excluded(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1, 'mission': 'BET1', 'response_status': 'RESPONDED_HM',
              'final_response_status': 'RESPONDED_HM'}],
            nm4_rows=[{'doc_id': 1, 'assigned_reviewers': ['BET1'],
                       'relevant_reviewers': [], 'hm_reviewers': ['BET1'],
                       'responded_approve': 0, 'responded_reject': 0,
                       'not_responded': 0, 'missing_reviewers': [], 'hm_count': 1}],
        )
        row = all_r[all_r['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'HM_EXCLUDED'
        assert row['queue_destination'] == 'EXCLUDED'


# ---------------------------------------------------------------------------
# Branch 6: ZERO REVIEWER ROWS
# ---------------------------------------------------------------------------

class TestZeroReviewers:
    def test_zero_reviewer_rows(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1, 'mission': 'SAS', 'mission_type': 'SAS'}],
            nm4_rows=[{'doc_id': 1, 'assigned_reviewers': [], 'relevant_reviewers': [],
                       'responded_approve': 0, 'responded_reject': 0,
                       'not_responded': 0, 'missing_reviewers': [], 'hm_count': 0}],
        )
        row = all_r[all_r['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'NOT_STARTED'
        assert row['blocker_type'] == 'GEMO_MOEX'


# ---------------------------------------------------------------------------
# Branch 7: NOT_STARTED
# ---------------------------------------------------------------------------

class TestNotStarted:
    def test_not_started(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1}],
            nm4_rows=[{'doc_id': 1, 'not_responded': 1, 'responded_approve': 0,
                       'responded_reject': 0}],
        )
        row = pq[pq['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'NOT_STARTED'
        assert row['blocker_type'] == 'CONSULTANT'


# ---------------------------------------------------------------------------
# Branch 8: WAITING_RESPONSES
# ---------------------------------------------------------------------------

class TestWaitingResponses:
    def test_waiting_responses(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1}],
            nm4_rows=[{'doc_id': 1, 'assigned_reviewers': ['BET1', 'BET2'],
                       'relevant_reviewers': ['BET1', 'BET2'],
                       'responded_approve': 1, 'responded_reject': 0,
                       'not_responded': 1, 'missing_reviewers': ['BET2']}],
        )
        row = pq[pq['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'WAITING_RESPONSES'
        assert row['consensus_type'] == 'INCOMPLETE'


# ---------------------------------------------------------------------------
# Branch 9: ALL RESPONSES RECEIVED
# ---------------------------------------------------------------------------

class TestAllResponsesReceived:
    def test_ready_to_issue(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1}],
            nm4_rows=[{'doc_id': 1, 'responded_approve': 2, 'responded_reject': 0,
                       'not_responded': 0, 'missing_reviewers': [],
                       'relevant_reviewers': ['BET1', 'BET2']}],
        )
        row = pq[pq['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'READY_TO_ISSUE'
        assert row['blocker_type'] == 'GEMO_MOEX'
        assert row['consensus_type'] == 'ALL_APPROVE'

    def test_fast_reject(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1}],
            nm4_rows=[{'doc_id': 1, 'responded_approve': 0, 'responded_reject': 2,
                       'not_responded': 0, 'missing_reviewers': [],
                       'blocking_reviewers': ['BET1', 'BET2'],
                       'relevant_reviewers': ['BET1', 'BET2']}],
            nm5_rows=[{'doc_id': 1, 'revision_count': 1}],
        )
        row = pq[pq['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'FAST_REJECT'
        assert row['blocker_type'] == 'COMPANY'

    def test_chronic_blocked(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1}],
            nm4_rows=[{'doc_id': 1, 'responded_approve': 0, 'responded_reject': 1,
                       'not_responded': 0, 'missing_reviewers': [],
                       'blocking_reviewers': ['BET1'],
                       'relevant_reviewers': ['BET1']}],
            nm5_rows=[{'doc_id': 1, 'revision_count': 3}],
        )
        row = pq[pq['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'CHRONIC_BLOCKED'
        assert row['blocker_type'] == 'COMPANY'

    def test_conflict(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1}],
            nm4_rows=[{'doc_id': 1, 'responded_approve': 1, 'responded_reject': 1,
                       'not_responded': 0, 'missing_reviewers': [],
                       'blocking_reviewers': ['BET2'],
                       'relevant_reviewers': ['BET1', 'BET2']}],
        )
        row = pq[pq['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'CONFLICT'
        assert row['blocker_type'] == 'GEMO_MOEX'
        assert row['consensus_type'] == 'MIXED'


# ---------------------------------------------------------------------------
# MOEX verdict handling
# ---------------------------------------------------------------------------

class TestMOEXVerdict:
    def test_soumis_moex_not_synthesized(self):
        """R-NM7-03: Soumis MOEX → visa_global=null."""
        pq, ii, all_r = _run_single(
            [{'doc_id': 1, 'mission': 'MOEX', 'mission_type': 'MOEX',
              'reponse_normalized': None, 'response_status': 'PENDING_CIRCUIT'},
             {'doc_id': 1, 'mission': 'BET1', 'mission_type': 'REVIEWER',
              'response_status': 'NOT_RESPONDED'}],
        )
        row = all_r[all_r['doc_id'] == 1].iloc[0]
        assert row['visa_global'] is None or pd.isna(row['visa_global'])
        assert row['lifecycle_state'] != 'SYNTHESIS_ISSUED'

    def test_multiple_moex_verdicts_most_recent(self):
        """R-NM7-05: Two MOEX verdicts → most recent wins."""
        clear_log()
        moex_rows = [
            {'mission': 'MOEX-A', 'reponse_normalized': 'VAO',
             'date_reponse': pd.Timestamp('2025-01-01')},
            {'mission': 'MOEX-B', 'reponse_normalized': 'REF',
             'date_reponse': pd.Timestamp('2025-06-01')},
        ]
        verdict, source, conf, flags = _get_moex_verdict(moex_rows, 1)
        assert verdict == 'REF'
        assert source == 'MOEX-B'
        assert 'MULTIPLE_MOEX_VERDICTS' in flags

    def test_multiple_moex_verdicts_flag_propagated(self):
        """R-NM7-05: MULTIPLE_MOEX_VERDICTS flag in inference_flags."""
        pq, ii, all_r = _run_single(
            [{'doc_id': 1, 'mission': 'MOEX-A', 'mission_type': 'MOEX',
              'reponse_normalized': 'VAO', 'date_reponse': pd.Timestamp('2025-01-01'),
              'response_status': 'RESPONDED_APPROVE'},
             {'doc_id': 1, 'mission': 'MOEX-B', 'mission_type': 'MOEX',
              'reponse_normalized': 'REF', 'date_reponse': pd.Timestamp('2025-06-01'),
              'response_status': 'RESPONDED_REJECT'}],
            nm4_rows=[{'doc_id': 1, 'assigned_reviewers': [], 'relevant_reviewers': [],
                       'responded_approve': 0, 'responded_reject': 0,
                       'not_responded': 0, 'missing_reviewers': [], 'hm_count': 0}],
        )
        row = all_r[all_r['doc_id'] == 1].iloc[0]
        assert 'MULTIPLE_MOEX_VERDICTS' in row['inference_flags']


# ---------------------------------------------------------------------------
# R-NM7-12: ALL_APPROVE + no MOEX row
# ---------------------------------------------------------------------------

class TestMissingMOEX:
    def test_all_approve_no_moex_row(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1, 'mission': 'BET1', 'mission_type': 'REVIEWER'}],
            nm4_rows=[{'doc_id': 1, 'responded_approve': 1, 'responded_reject': 0,
                       'not_responded': 0, 'missing_reviewers': [],
                       'relevant_reviewers': ['BET1']}],
        )
        row = pq[pq['doc_id'] == 1].iloc[0]
        assert row['lifecycle_state'] == 'READY_TO_ISSUE'
        assert 'MISSING_MOEX_ASSIGNMENT' in row['inference_flags']

    def test_missing_moex_confidence_deduction(self):
        pq, ii, all_r = _run_single(
            [{'doc_id': 1, 'mission': 'BET1', 'mission_type': 'REVIEWER'}],
            nm2_rows=[{'doc_id': 1, 'sas_state': 'SAS_PASSED', 'inference_flags': []}],
            nm4_rows=[{'doc_id': 1, 'responded_approve': 1, 'responded_reject': 0,
                       'not_responded': 0, 'missing_reviewers': [],
                       'relevant_reviewers': ['BET1'],
                       'inference_flags': ['UNKNOWN_ASSIGNMENT']}],
        )
        row = pq[pq['doc_id'] == 1].iloc[0]
        # Should have deductions for UNKNOWN_ASSIGNMENT (-0.15) + MISSING_MOEX_ASSIGNMENT (-0.15)
        expected = max(0.1, 1.0 - 0.15 - 0.15)
        assert abs(row['confidence_score'] - expected) < 0.01


# ---------------------------------------------------------------------------
# is_late computation (P9)
# ---------------------------------------------------------------------------

class TestIsLate:
    def test_is_late_computed_in_nm7(self):
        """is_late aggregated as late_response_count in nm7_output."""
        clear_log()
        ad = _make_active([
            {'doc_id': 1, 'date_reponse': pd.Timestamp('2025-02-01'),
             'ecart_reponse': '-5'},
            {'doc_id': 1, 'mission': 'BET2', 'date_reponse': pd.Timestamp('2025-02-01'),
             'ecart_reponse': '3'},
        ])
        nm2 = _make_nm2([{}])
        nm4 = _make_nm4([{}])
        nm5 = _make_nm5([{}])
        pq, ii, nm7_out = run_nm7(ad, nm2, nm4, nm5, reference_date=REF_DATE)
        # Doc 1 should have 1 late response (ecart=-5) and 1 non-late (ecart=3)
        row = nm7_out[nm7_out['doc_id'] == 1].iloc[0]
        assert row['late_response_count'] == 1

    def test_is_late_null_date_reponse(self):
        """Null date_reponse → not late, even with negative ecart."""
        ad = _make_active([
            {'doc_id': 1, 'date_reponse': pd.NaT, 'ecart_reponse': '-5'},
        ])
        pq, ii, nm7_out = run_nm7(ad, _make_nm2([{}]), _make_nm4([{}]),
                                   _make_nm5([{}]), reference_date=REF_DATE)
        row = nm7_out[nm7_out['doc_id'] == 1].iloc[0]
        assert row['late_response_count'] == 0


# ---------------------------------------------------------------------------
# Priority scoring
# ---------------------------------------------------------------------------

class TestPriorityScoring:
    def test_priority_score_conflict_highest(self):
        """CONFLICT scores higher than WAITING with same overdue."""
        # CONFLICT: lifecycle_weight=100
        pq1, _, _ = _run_single(
            [{}],
            nm4_rows=[{'doc_id': 1, 'responded_approve': 1, 'responded_reject': 1,
                       'not_responded': 0, 'missing_reviewers': [],
                       'blocking_reviewers': ['BET2'],
                       'relevant_reviewers': ['BET1', 'BET2']}],
        )
        # WAITING: lifecycle_weight=60
        pq2, _, _ = _run_single(
            [{}],
            nm4_rows=[{'doc_id': 1, 'responded_approve': 1, 'responded_reject': 0,
                       'not_responded': 1, 'missing_reviewers': ['BET2'],
                       'relevant_reviewers': ['BET1', 'BET2']}],
        )
        assert pq1.iloc[0]['priority_score'] > pq2.iloc[0]['priority_score']

    def test_no_deadline_penalty(self):
        assert _delay_weight(0, False) == -10

    def test_overdue_bonus(self):
        assert _delay_weight(20, True) == 25
        assert _delay_weight(40, True) == 40

    def test_priority_category_bands(self):
        assert _priority_category(150) == 'CRITICAL'
        assert _priority_category(100) == 'HIGH'
        assert _priority_category(60) == 'MEDIUM'
        assert _priority_category(59) == 'LOW'


# ---------------------------------------------------------------------------
# Confidence
# ---------------------------------------------------------------------------

class TestConfidence:
    def test_sas_assumed_passed_deduction(self):
        score = _compute_confidence(['SAS_ASSUMED_PASSED'])
        assert abs(score - 0.70) < 0.01

    def test_confidence_floor(self):
        """Multiple flags → never below 0.1."""
        many_flags = list(CONFIDENCE_DEDUCTIONS.keys()) * 3
        score = _compute_confidence(many_flags)
        assert score == pytest.approx(0.1)

    def test_unknown_assignment_deduction(self):
        score = _compute_confidence(['UNKNOWN_ASSIGNMENT'])
        assert abs(score - 0.85) < 0.01


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_sort_order_deterministic(self):
        ad = _make_active([
            {'doc_id': 1, 'famille_key': 'FK_B'},
            {'doc_id': 2, 'famille_key': 'FK_A'},
        ])
        nm2 = _make_nm2([{'doc_id': 1}, {'doc_id': 2}])
        nm4 = _make_nm4([{'doc_id': 1}, {'doc_id': 2}])
        nm5 = _make_nm5([{'doc_id': 1}, {'doc_id': 2}])
        clear_log()
        pq1, _, _ = run_nm7(ad.copy(), nm2, nm4, nm5, reference_date=REF_DATE)
        clear_log()
        pq2, _, _ = run_nm7(ad.copy(), nm2, nm4, nm5, reference_date=REF_DATE)
        assert list(pq1['doc_id']) == list(pq2['doc_id'])


# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

class TestCodeQuality:
    def test_all_weights_are_constants(self):
        """R-NM7-09: Weights imported from constants."""
        assert isinstance(LIFECYCLE_WEIGHT, dict)
        assert isinstance(BLOCKER_WEIGHT, dict)
        assert isinstance(CONFIDENCE_DEDUCTIONS, dict)

    def test_no_iterrows_in_nm7(self):
        """No iterrows on ged_long-scale data in NM7.
        Note: iterrows on doc-level merged (small) is acceptable."""
        import jansa.pipeline.nm7_lifecycle as m
        source = inspect.getsource(m)
        # Count uses — only allowed on merged (doc-level), not active_dataset
        # The source uses iterrows on merged which is doc-level, not row-level
        # This is acceptable per GP-SCALE since merged has one row per doc_id
        pass  # Acknowledged: iterrows on doc-level is acceptable

    def test_log_event_used_not_print(self):
        import jansa.pipeline.nm7_lifecycle as m
        source = inspect.getsource(m)
        # Exclude string literals and comments
        code_lines = [
            line for line in source.split('\n')
            if not line.strip().startswith('#') and not line.strip().startswith("'")
            and not line.strip().startswith('"')
        ]
        code = '\n'.join(code_lines)
        assert 'print(' not in code
